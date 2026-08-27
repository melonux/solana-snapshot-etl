use crate::clickhouse::{
    max_raw_account_updated_slot, ClickhouseIndexer, CloseTombstoneStats, SnapshotKind,
};
use clap::{ArgGroup, Parser};
use env_logger::{Builder, Env, Target};
use indicatif::{ProgressBar, ProgressBarIter, ProgressStyle};
use log::{debug, error, warn, LevelFilter};
use solana_snapshot_etl::archived::ArchiveSnapshotExtractor;
use solana_snapshot_etl::incremental::{
    discover as discover_incremental_snapshots, discover_full as discover_full_snapshots,
    eligible_candidates, eligible_full_candidates, FullSnapshot, IncrementalSnapshot,
};
use solana_snapshot_etl::unpacked::UnpackedSnapshotExtractor;
use solana_snapshot_etl::{AppendVecIterator, ReadProgressTracking, SnapshotExtractor};
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{IoSliceMut, Read, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

mod clickhouse;
mod mpl_metadata;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
#[clap(group(
    ArgGroup::new("source-input")
        .required(true)
        .args(&["source", "incremental-snapshot-dir"]),
))]
#[clap(group(
    ArgGroup::new("action")
        .required(true)
        .multiple(false)
        .args(&[
            "clickhouse",
            "clickhouse-close-tombstones",
        ]),
))]
struct Args {
    #[clap(help = "Snapshot source (unpacked snapshot directory or local archive file)")]
    source: Option<String>,
    #[clap(
        long,
        value_name = "DIR",
        help = "Continuously consume full and incremental .tar.zst snapshots from this directory"
    )]
    incremental_snapshot_dir: Option<PathBuf>,
    #[clap(
        long,
        action,
        help = "Start from slot 0 and require a full snapshot before applying incremental snapshots"
    )]
    bootstrap: bool,
    #[clap(
        long,
        default_value_t = 5,
        value_name = "SECONDS",
        help = "Delay before re-scanning a snapshot directory when no usable archive exists"
    )]
    incremental_poll_interval_secs: u64,
    #[clap(
        long,
        action,
        help = "Write to ClickHouse configured by CLICKHOUSE_URL"
    )]
    clickhouse: bool,
    #[clap(
        long,
        default_value_t = default_clickhouse_workers(),
        value_name = "N",
        help = "Number of concurrent ClickHouse import workers"
    )]
    clickhouse_workers: usize,
    #[clap(
        long,
        action,
        help = "Scan canonical empty accounts and mark deleted token accounts in ClickHouse without re-importing rows"
    )]
    clickhouse_close_tombstones: bool,
    #[clap(
        long,
        value_name = "FILE",
        help = "Write ETL logs to FILE (truncate it at startup); leave the terminal available for progress bars"
    )]
    log_file: Option<PathBuf>,
    #[clap(
        long,
        value_name = "LEVEL",
        help = "Log level for diagnostics (error, warn, info, debug, trace, or off); overrides RUST_LOG"
    )]
    log_level: Option<LevelFilter>,
}

fn main() {
    let args = Args::parse();
    init_logging(args.log_file.as_deref(), args.log_level);
    if let Err(e) = _main(args) {
        error!("{}", e);
        std::process::exit(1);
    }
}

fn init_logging(log_file: Option<&Path>, log_level: Option<LevelFilter>) {
    let Some(path) = log_file else {
        let mut builder = logging_builder(log_level);
        builder.init();
        return;
    };

    match OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
    {
        Ok(file) => {
            let mut builder = logging_builder(log_level);
            builder.target(Target::Pipe(Box::new(file)));
            // A watch process can be restarted while an older invocation is
            // still draining the same snapshot. Include process/thread
            // identity so their archive and worker messages cannot be
            // mistaken for one producer filling one queue.
            builder.format(|buf, record| {
                writeln!(
                    buf,
                    "[{} pid={} tid={:?} {} {}] {}",
                    buf.timestamp(),
                    std::process::id(),
                    std::thread::current().id(),
                    record.level(),
                    record.target(),
                    record.args()
                )
            });
            // env_logger 0.9 routes custom pipes through its test-target
            // writer; enabling this keeps the file target active in a normal
            // process as well.
            builder.is_test(true);
            if let Err(err) = builder.try_init() {
                eprintln!("failed to initialize file logger {}: {err}", path.display());
            } else {
                eprintln!("logging to {}", path.display());
            }
        }
        Err(err) => {
            eprintln!(
                "failed to open log file {}: {err}; logging to stderr",
                path.display()
            );
            logging_builder(log_level).init();
        }
    }
}

fn logging_builder(log_level: Option<LevelFilter>) -> Builder {
    match log_level {
        Some(level) => {
            let mut builder = Builder::new();
            builder.filter_level(level);
            builder
        }
        None => Builder::from_env(Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info")),
    }
}

fn default_clickhouse_workers() -> usize {
    std::thread::available_parallelism()
        // ClickHouse shares this host with the parser. Two independent HTTP
        // insert streams keep parsing and uploads overlapped without creating
        // more MergeTree parts than the local background merger can sustain.
        .map(|parallelism| parallelism.get().min(2))
        .unwrap_or(2)
}

fn _main(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    if args.clickhouse_workers == 0 {
        return Err("--clickhouse-workers must be greater than zero".into());
    }
    if args.clickhouse_workers > 4 {
        return Err("--clickhouse-workers must not exceed 4 on a shared ClickHouse host".into());
    }
    if let Some(directory) = &args.incremental_snapshot_dir {
        if args.clickhouse_close_tombstones {
            return Err(
                "--clickhouse-close-tombstones requires a single snapshot source, not snapshot watch mode"
                    .into(),
            );
        }
        if args.incremental_poll_interval_secs == 0 {
            return Err("--incremental-poll-interval-secs must be greater than zero".into());
        }
        return run_incremental_snapshots(&args, directory);
    }
    if args.bootstrap {
        return Err("--bootstrap requires --incremental-snapshot-dir".into());
    }

    let source = args
        .source
        .as_deref()
        .ok_or("a snapshot source is required")?;
    let mut loader = SupportedLoader::new(source, Box::new(LoadProgressTracking {}))?;
    process_single_snapshot(&args, &mut loader)
}

fn process_single_snapshot(
    args: &Args,
    loader: &mut SupportedLoader,
) -> Result<(), Box<dyn std::error::Error>> {
    let source_path = Path::new(
        args.source
            .as_deref()
            .ok_or("a snapshot source is required")?,
    );
    if args.clickhouse_close_tombstones {
        dotenvy::dotenv().ok();
        let clickhouse_url = std::env::var("CLICKHOUSE_URL")
            .map_err(|_| "CLICKHOUSE_URL must be set in the environment or .env file")?;
        let snapshot_slot = loader.snapshot_slot();
        let append_vec_count = loader.append_vec_count_hint();
        console_snapshot_status("processing", "tombstones", source_path, snapshot_slot);
        debug!(
            "Scanning snapshot slot {} for canonical empty accounts",
            snapshot_slot
        );
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let stats = match runtime.block_on(
            ClickhouseIndexer::new(clickhouse_url, snapshot_slot, append_vec_count)?
                .mark_close_tombstones(loader.iter()),
        ) {
            Ok(stats) => stats,
            Err(err) => {
                console_snapshot_status("failed", "tombstones", source_path, snapshot_slot);
                return Err(err);
            }
        };
        log_close_tombstone_stats(&stats);
        console_snapshot_status("completed", "tombstones", source_path, snapshot_slot);
    }
    if args.clickhouse {
        dotenvy::dotenv().ok();
        let clickhouse_url = std::env::var("CLICKHOUSE_URL")
            .map_err(|_| "CLICKHOUSE_URL must be set in the environment or .env file")?;
        let snapshot_slot = loader.snapshot_slot();
        let append_vec_count = loader.append_vec_count_hint();
        let snapshot_kind = snapshot_kind_from_source(args.source.as_deref().unwrap_or_default());
        console_snapshot_status(
            "processing",
            snapshot_kind.as_str(),
            source_path,
            snapshot_slot,
        );
        debug!(
            "Dumping {} snapshot slot {} to ClickHouse",
            snapshot_kind.as_str(),
            snapshot_slot
        );
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let stats =
            match runtime.block_on(
                ClickhouseIndexer::new(clickhouse_url, snapshot_slot, append_vec_count)?
                    .insert_all(loader.iter(), snapshot_kind, args.clickhouse_workers),
            ) {
                Ok(stats) => stats,
                Err(err) => {
                    console_snapshot_status(
                        "failed",
                        snapshot_kind.as_str(),
                        source_path,
                        snapshot_slot,
                    );
                    return Err(err);
                }
            };
        log_clickhouse_index_stats(&stats);
        console_snapshot_status(
            "completed",
            snapshot_kind.as_str(),
            source_path,
            snapshot_slot,
        );
    }
    Ok(())
}

fn console_snapshot_status(event: &str, kind: &str, path: &Path, slot: u64) {
    let mut output = std::io::stdout();
    let _ = writeln!(
        output,
        "[snapshot] {event}: {kind} file={} slot={slot}",
        path.display()
    );
    let _ = output.flush();
}

/// A single-source invocation has no `WatchedSnapshot` wrapper from which to
/// obtain the archive kind. Solana's archive filename is sufficient here; an
/// unpacked directory (or a conventional `snapshot-...` archive) is treated
/// as a full checkpoint. Unknown archive names conservatively keep the
/// incremental tombstone path enabled so a renamed incremental archive cannot
/// silently lose deletions.
fn snapshot_kind_from_source(source: &str) -> SnapshotKind {
    let source_without_query = source.split('?').next().unwrap_or(source);
    let file_name = Path::new(source_without_query)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    if file_name.starts_with("incremental-snapshot-") {
        SnapshotKind::Incremental
    } else if file_name.starts_with("snapshot-") || Path::new(source).is_dir() {
        SnapshotKind::Full
    } else {
        SnapshotKind::Incremental
    }
}

enum IncrementalOutput {
    Clickhouse {
        clickhouse_url: String,
        runtime: tokio::runtime::Runtime,
        workers: usize,
    },
}

enum WatchedSnapshot {
    Incremental(IncrementalSnapshot),
    Full(FullSnapshot),
}

impl WatchedSnapshot {
    fn path(&self) -> &Path {
        match self {
            Self::Incremental(snapshot) => snapshot.path(),
            Self::Full(snapshot) => snapshot.path(),
        }
    }

    fn slot(&self) -> u64 {
        match self {
            Self::Incremental(snapshot) => snapshot.slot(),
            Self::Full(snapshot) => snapshot.slot(),
        }
    }

    fn kind(&self) -> &'static str {
        match self {
            Self::Incremental(_) => "incremental",
            Self::Full(_) => "full",
        }
    }

    fn new_loader(&self, resume_slot: u64) -> Result<SupportedLoader, Box<dyn std::error::Error>> {
        match self {
            Self::Incremental(snapshot) => {
                SupportedLoader::new_incremental_snapshot(snapshot.path(), resume_slot)
            }
            Self::Full(snapshot) => {
                SupportedLoader::new_full_snapshot(snapshot.path(), resume_slot)
            }
        }
    }

    fn log_verification(&self) {
        match self {
            Self::Incremental(snapshot) => debug!(
                "Verifying incremental snapshot {} (base={}, slot={})",
                snapshot.path().display(),
                snapshot.base_slot(),
                snapshot.slot()
            ),
            Self::Full(snapshot) => debug!(
                "Verifying full snapshot {} (slot={})",
                snapshot.path().display(),
                snapshot.slot()
            ),
        }
    }
}

impl IncrementalOutput {
    fn new(args: &Args) -> Result<Self, Box<dyn std::error::Error>> {
        if !args.clickhouse {
            return Err("--incremental-snapshot-dir currently requires --clickhouse".into());
        }
        dotenvy::dotenv().ok();
        let clickhouse_url = std::env::var("CLICKHOUSE_URL")
            .map_err(|_| "CLICKHOUSE_URL must be set in the environment or .env file")?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        Ok(Self::Clickhouse {
            clickhouse_url,
            runtime,
            workers: args.clickhouse_workers,
        })
    }

    fn max_raw_account_updated_slot(&self) -> Result<u64, Box<dyn std::error::Error>> {
        match self {
            Self::Clickhouse {
                clickhouse_url,
                runtime,
                ..
            } => runtime
                .block_on(max_raw_account_updated_slot(clickhouse_url))
                .map_err(Into::into),
        }
    }

    fn process(
        &mut self,
        loader: &mut SupportedLoader,
        snapshot_kind: SnapshotKind,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match self {
            Self::Clickhouse {
                clickhouse_url,
                runtime,
                workers,
            } => {
                let snapshot_slot = loader.snapshot_slot();
                let append_vec_count = loader.append_vec_count_hint();
                debug!(
                    "Dumping {} snapshot slot {snapshot_slot} to ClickHouse",
                    snapshot_kind.as_str()
                );
                let stats = runtime.block_on(
                    ClickhouseIndexer::new(
                        clickhouse_url.clone(),
                        snapshot_slot,
                        append_vec_count,
                    )?
                    .insert_all(loader.iter(), snapshot_kind, *workers),
                )?;
                log_clickhouse_index_stats(&stats);
            }
        }
        Ok(())
    }
}

fn run_incremental_snapshots(
    args: &Args,
    directory: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut output = IncrementalOutput::new(args)?;
    let mut bootstrap_pending = args.bootstrap;
    let mut resume_slot = if bootstrap_pending {
        0
    } else {
        let max_updated_slot = output.max_raw_account_updated_slot()?;
        let resume_slot = resume_slot_from_max_updated_slot(max_updated_slot);
        debug!(
            "Read raw_account maximum updated_slot={max_updated_slot}; rewound {} slots to resume slot {resume_slot}",
            RESUME_SLOT_REWIND
        );
        resume_slot
    };
    let mut invalid_archives = HashSet::<PathBuf>::new();
    let poll_interval = Duration::from_secs(args.incremental_poll_interval_secs);

    debug!(
        "Watching snapshot directory {} from resume slot {}{}",
        directory.display(),
        resume_slot,
        if bootstrap_pending {
            " (bootstrap: waiting for a full snapshot)"
        } else {
            ""
        }
    );

    loop {
        let incrementals = discover_incremental_snapshots(directory)?;
        let fulls = discover_full_snapshots(directory)?;
        let candidates =
            watched_snapshot_candidates(incrementals, fulls, resume_slot, bootstrap_pending);
        let mut selected: Option<(WatchedSnapshot, SupportedLoader)> = None;

        for candidate in candidates {
            if invalid_archives.contains(candidate.path()) {
                continue;
            }

            candidate.log_verification();
            let loader = match candidate.new_loader(resume_slot) {
                Ok(loader) => loader,
                Err(err) => {
                    warn!(
                        "Ignoring unreadable {} snapshot {}: {}",
                        candidate.kind(),
                        candidate.path().display(),
                        err
                    );
                    invalid_archives.insert(candidate.path().to_path_buf());
                    continue;
                }
            };

            if loader.snapshot_slot() != candidate.slot() {
                warn!(
                    "Ignoring {} snapshot {}: filename expects slot={}, manifest has slot={}",
                    candidate.kind(),
                    candidate.path().display(),
                    candidate.slot(),
                    loader.snapshot_slot(),
                );
                invalid_archives.insert(candidate.path().to_path_buf());
                continue;
            }
            selected = Some((candidate, loader));
            break;
        }

        let Some((candidate, mut loader)) = selected else {
            thread::sleep(poll_interval);
            continue;
        };

        let snapshot_kind = match &candidate {
            WatchedSnapshot::Incremental(_) => SnapshotKind::Incremental,
            WatchedSnapshot::Full(_) => SnapshotKind::Full,
        };
        console_snapshot_status(
            "processing",
            snapshot_kind.as_str(),
            candidate.path(),
            candidate.slot(),
        );
        if let Err(err) = output.process(&mut loader, snapshot_kind) {
            console_snapshot_status(
                "failed",
                snapshot_kind.as_str(),
                candidate.path(),
                candidate.slot(),
            );
            error!(
                "Failed to process {} snapshot {}: {}. Stopping watcher; the file was retained and slot {} remains current",
                candidate.kind(),
                candidate.path().display(),
                err,
                resume_slot
            );
            return Err(std::io::Error::other(format!(
                "failed to process {} snapshot {} at slot {}: {err}; automatic retry is disabled",
                candidate.kind(),
                candidate.path().display(),
                candidate.slot(),
            ))
            .into());
        }

        console_snapshot_status(
            "completed",
            snapshot_kind.as_str(),
            candidate.path(),
            candidate.slot(),
        );
        resume_slot = candidate.slot();
        if bootstrap_pending {
            bootstrap_pending = false;
            debug!("Full bootstrap complete; incremental snapshots are now eligible");
        }
        debug!("Advanced resume slot to {resume_slot}");
        invalid_archives.retain(|path| path.exists());
    }
}

const RESUME_SLOT_REWIND: u64 = 1_000;

fn resume_slot_from_max_updated_slot(max_updated_slot: u64) -> u64 {
    max_updated_slot.saturating_sub(RESUME_SLOT_REWIND)
}

fn watched_snapshot_candidates(
    incrementals: Vec<IncrementalSnapshot>,
    fulls: Vec<FullSnapshot>,
    resume_slot: u64,
    bootstrap_pending: bool,
) -> Vec<WatchedSnapshot> {
    if bootstrap_pending {
        // A bootstrap must establish a canonical full-state baseline. Even an
        // incremental whose base is zero cannot replace it.
        return eligible_full_candidates(fulls, resume_slot)
            .into_iter()
            .map(WatchedSnapshot::Full)
            .collect();
    }

    // Prefer an already-applicable incremental archive. If there is a gap
    // (for example, current=1000 and the next incremental is based at 1100),
    // a newer full snapshot can bridge state forward.
    eligible_candidates(incrementals, resume_slot)
        .into_iter()
        .map(WatchedSnapshot::Incremental)
        .chain(
            eligible_full_candidates(fulls, resume_slot)
                .into_iter()
                .map(WatchedSnapshot::Full),
        )
        .collect()
}

fn log_clickhouse_index_stats(stats: &crate::clickhouse::IndexStats) {
    debug!("[clickhouse] Dumped {} accounts", stats.accounts_total);
    debug!(
        "[clickhouse] Dumped {} token accounts",
        stats.token_accounts_total
    );
    debug!(
        "[clickhouse] Skipped {} append vec files",
        stats.skipped_append_vecs
    );
    debug!(
        "[clickhouse] Processed {} append vec files",
        stats.append_vecs_total
    );
    debug!(
        "[clickhouse] Non-empty append vec files producing 0 accounts: {}",
        stats.nonempty_zero_account_append_vecs
    );
    debug!(
        "[clickhouse] SPL-Token owner accounts seen: {}",
        stats.spl_token_owner_accounts_seen
    );
    debug!(
        "[clickhouse] SPL-Token accounts parsed successfully: {}",
        stats.spl_token_accounts_parsed
    );
    debug!(
        "[clickhouse] SPL-Token accounts with unexpected size: {}",
        stats.spl_token_unexpected_size
    );
    debug!(
        "[clickhouse] SPL-Token accounts with unpack failure: {}",
        stats.spl_token_unpack_failed
    );
    debug!(
        "[clickhouse] Token-2022 owner accounts seen: {}",
        stats.token_2022_owner_accounts_seen
    );
    debug!(
        "[clickhouse] Token-2022 accounts parsed successfully: {}",
        stats.token_2022_accounts_parsed
    );
    debug!(
        "[clickhouse] Token-2022 accounts with unexpected size: {}",
        stats.token_2022_unexpected_size
    );
    debug!(
        "[clickhouse] Token-2022 accounts with unpack failure: {}",
        stats.token_2022_unpack_failed
    );
    debug!(
        "[clickhouse] Canonical empty-account token tombstone candidates: {}",
        stats.token_account_close_candidates
    );
    debug!(
        "[clickhouse] Token-account tombstone versions written: {}",
        stats.token_accounts_marked_deleted
    );
}

fn log_close_tombstone_stats(stats: &CloseTombstoneStats) {
    debug!(
        "[clickhouse] Scanned {} append vec files for tombstones",
        stats.append_vecs_total
    );
    debug!(
        "[clickhouse] Skipped {} append vec files while scanning tombstones",
        stats.skipped_append_vecs
    );
    debug!(
        "[clickhouse] Canonical empty accounts found: {}",
        stats.canonical_empty_accounts
    );
    debug!(
        "[clickhouse] Token-account tombstone versions written: {}",
        stats.token_accounts_marked_deleted
    );
}

struct LoadProgressTracking {}

impl ReadProgressTracking for LoadProgressTracking {
    fn new_read_progress_tracker(
        &self,
        _: &Path,
        rd: Box<dyn Read>,
        file_len: u64,
    ) -> Box<dyn Read> {
        let progress_bar = ProgressBar::new(file_len).with_style(
            ProgressStyle::with_template(
                "{prefix:>10.bold.dim} {spinner:.green} [{bar:.cyan/blue}] {bytes}/{total_bytes} ({percent}%)",
            )
            .unwrap()
            .progress_chars("#>-"),
        );
        progress_bar.set_prefix("manifest");
        Box::new(LoadProgressTracker {
            rd: progress_bar.wrap_read(rd),
            progress_bar,
        })
    }
}

struct LoadProgressTracker {
    progress_bar: ProgressBar,
    rd: ProgressBarIter<Box<dyn Read>>,
}

impl Drop for LoadProgressTracker {
    fn drop(&mut self) {
        self.progress_bar.finish()
    }
}

impl Read for LoadProgressTracker {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.rd.read(buf)
    }

    fn read_vectored(&mut self, bufs: &mut [IoSliceMut<'_>]) -> std::io::Result<usize> {
        self.rd.read_vectored(bufs)
    }

    fn read_to_string(&mut self, buf: &mut String) -> std::io::Result<usize> {
        self.rd.read_to_string(buf)
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        self.rd.read_exact(buf)
    }
}

pub enum SupportedLoader {
    Unpacked(UnpackedSnapshotExtractor),
    ArchiveFile(ArchiveSnapshotExtractor<File>),
}

impl SupportedLoader {
    fn new(
        source: &str,
        progress_tracking: Box<dyn ReadProgressTracking>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new_file(Path::new(source), progress_tracking).map_err(Into::into)
    }

    fn new_file(
        path: &Path,
        progress_tracking: Box<dyn ReadProgressTracking>,
    ) -> solana_snapshot_etl::Result<Self> {
        Ok(if path.is_dir() {
            debug!("Reading unpacked snapshot");
            Self::Unpacked(UnpackedSnapshotExtractor::open(path, progress_tracking)?)
        } else {
            debug!("Reading snapshot archive");
            Self::ArchiveFile(ArchiveSnapshotExtractor::open(path)?)
        })
    }

    fn new_incremental_snapshot(
        path: &Path,
        last_processed_slot: u64,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        debug!("Reading incremental snapshot archive");
        // The archive can repeat storage from the full base. Only apply files
        // newer than the database watermark.
        let loader =
            ArchiveSnapshotExtractor::open(path)?.with_minimum_append_vec_slot(last_processed_slot);
        Ok(Self::ArchiveFile(loader))
    }

    fn new_full_snapshot(
        path: &Path,
        last_processed_slot: u64,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        debug!("Reading full snapshot archive");
        // Full archives are canonical, but watch mode may discover one after
        // some slots are already indexed. Keep the same slot filter so files
        // at or below the database watermark are not rewritten.
        let loader =
            ArchiveSnapshotExtractor::open(path)?.with_minimum_append_vec_slot(last_processed_slot);
        Ok(Self::ArchiveFile(loader))
    }
}

impl SnapshotExtractor for SupportedLoader {
    fn iter(&mut self) -> AppendVecIterator<'_> {
        match self {
            SupportedLoader::Unpacked(loader) => Box::new(loader.iter()),
            SupportedLoader::ArchiveFile(loader) => Box::new(loader.iter()),
        }
    }

    fn snapshot_slot(&self) -> u64 {
        match self {
            SupportedLoader::Unpacked(loader) => loader.snapshot_slot(),
            SupportedLoader::ArchiveFile(loader) => loader.snapshot_slot(),
        }
    }

    fn append_vec_count_hint(&self) -> Option<u64> {
        match self {
            SupportedLoader::Unpacked(loader) => loader.append_vec_count_hint(),
            SupportedLoader::ArchiveFile(loader) => loader.append_vec_count_hint(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        discover_full_snapshots, discover_incremental_snapshots, resume_slot_from_max_updated_slot,
        watched_snapshot_candidates, WatchedSnapshot,
    };
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn rewinds_database_watermark_without_underflow() {
        assert_eq!(resume_slot_from_max_updated_slot(5_000), 4_000);
        assert_eq!(resume_slot_from_max_updated_slot(1_000), 0);
        assert_eq!(resume_slot_from_max_updated_slot(999), 0);
    }

    #[test]
    fn bootstrap_waits_for_a_full_snapshot_even_when_incremental_is_eligible() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "solana-snapshot-etl-bootstrap-test-{}-{unique}",
            std::process::id()
        ));
        fs::create_dir(&directory).unwrap();
        let hash = "3dBjB2KwbPjeqjQwNzwx48qgK4hdkcw5uxmwcgDh5zkD";
        fs::write(
            directory.join(format!("incremental-snapshot-0-1500-{hash}.tar.zst")),
            [],
        )
        .unwrap();
        fs::write(directory.join(format!("snapshot-2000-{hash}.tar.zst")), []).unwrap();

        let candidates = watched_snapshot_candidates(
            discover_incremental_snapshots(&directory).unwrap(),
            discover_full_snapshots(&directory).unwrap(),
            0,
            true,
        );

        assert_eq!(candidates.len(), 1);
        assert!(matches!(&candidates[0], WatchedSnapshot::Full(_)));
        fs::remove_dir_all(directory).unwrap();
    }
}
