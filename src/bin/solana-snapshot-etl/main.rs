use crate::clickhouse::{
    exchange_table_groups, import_full_snapshot_fanout, load_enabled_hot_mints,
    max_raw_account_updated_slot, rebuild_derived_indexes_from_state, reset_table_group,
    set_group_table_merges, snapshot_hot_mints, table_group_identity, validate_clickhouse_schema,
    validate_staging_group, wait_for_group_merges_to_settle, ClickhouseIndexer,
    CloseTombstoneStats, HotMintSet, SnapshotKind, TableGroup, TableGroupIdentity,
};
use clap::{ArgGroup, Parser};
use env_logger::{Builder, Env, Target};
use indicatif::{ProgressBar, ProgressBarIter, ProgressStyle};
use log::{debug, error, info, warn, LevelFilter};
use serde::{Deserialize, Serialize};
use solana_snapshot_etl::archived::ArchiveSnapshotExtractor;
use solana_snapshot_etl::incremental::{
    discover as discover_incremental_snapshots, discover_full as discover_full_snapshots,
    eligible_candidates, eligible_full_candidates, FullSnapshot, IncrementalSnapshot,
};
use solana_snapshot_etl::unpacked::UnpackedSnapshotExtractor;
use solana_snapshot_etl::{AppendVecIterator, ReadProgressTracking, SnapshotExtractor};
use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, IoSliceMut, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

mod clickhouse;
mod mpl_metadata;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
#[clap(group(
    ArgGroup::new("source-input")
        .required(false)
        .args(&["source", "incremental-snapshot-dir"]),
))]
#[clap(group(
    ArgGroup::new("action")
        .required(true)
        .multiple(false)
        .args(&[
            "clickhouse",
            "clickhouse-validate-schema",
            "clickhouse-rebuild-hot",
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
        default_value_t = DEFAULT_RESUME_SLOT_REWIND,
        value_name = "SLOTS",
        help = "Rewind this many slots from raw_account.max(updated_slot) when resuming (default: 1000)"
    )]
    resume_slot_rewind: u64,
    #[clap(
        long,
        default_value_t = 5,
        value_name = "SECONDS",
        help = "Delay before re-scanning after the first snapshot when no usable archive exists"
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
        action,
        help = "Validate the ClickHouse schema and exit without reading a snapshot or writing data"
    )]
    clickhouse_validate_schema: bool,
    #[clap(
        long,
        action,
        help = "Rebuild active L3 wallet balances and token-info from existing direct-write hot state without reading a snapshot"
    )]
    clickhouse_rebuild_hot: bool,
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
    if args.clickhouse_validate_schema {
        if args.source.is_some()
            || args.incremental_snapshot_dir.is_some()
            || args.bootstrap
            || args.clickhouse_close_tombstones
        {
            return Err(
                "--clickhouse-validate-schema is a standalone read-only action and cannot be combined with a snapshot source, --bootstrap, or tombstone mode"
                    .into(),
            );
        }
        validate_clickhouse_prerequisites()?;
        info!("[clickhouse] schema validation completed successfully; no snapshot was read and no data was written");
        return Ok(());
    }
    if args.clickhouse_rebuild_hot {
        if args.source.is_some()
            || args.incremental_snapshot_dir.is_some()
            || args.bootstrap
            || args.clickhouse_close_tombstones
        {
            return Err(
                "--clickhouse-rebuild-hot is a standalone repair action and cannot be combined with a snapshot source, --bootstrap, or tombstone mode"
                    .into(),
            );
        }
        validate_clickhouse_prerequisites()?;
        return rebuild_hot_indexes_only();
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
        validate_clickhouse_prerequisites()?;
        return run_incremental_snapshots(&args, directory);
    }
    if args.bootstrap {
        return Err("--bootstrap requires --incremental-snapshot-dir".into());
    }
    if args.clickhouse || args.clickhouse_close_tombstones {
        validate_clickhouse_prerequisites()?;
    }

    let source = args
        .source
        .as_deref()
        .ok_or("a snapshot source is required")?;
    let mut loader = SupportedLoader::new(source, Box::new(LoadProgressTracking {}))?;
    process_single_snapshot(&args, &mut loader)
}

fn validate_clickhouse_prerequisites() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    let clickhouse_url = std::env::var("CLICKHOUSE_URL")
        .map_err(|_| "CLICKHOUSE_URL must be set in the environment or .env file")?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime
        .block_on(validate_clickhouse_schema(&clickhouse_url))
        .map_err(Into::into)
}

/// Repair only L3/token-info after direct-write L2 has already been imported.
/// This deliberately does not read an archive or truncate raw/L2 tables.
/// Group raw/hot merges remain paused when the repair fails; they are enabled
/// only after the derived rebuild succeeds.
fn rebuild_hot_indexes_only() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    let clickhouse_url = std::env::var("CLICKHOUSE_URL")
        .map_err(|_| "CLICKHOUSE_URL must be set in the environment or .env file")?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    info!(
        "[clickhouse] derived-only repair 启动：保留 active raw/L2 数据，仅重建 L3 与 token-info；不读取快照、不清空表"
    );
    runtime.block_on(set_group_table_merges(
        &clickhouse_url,
        TableGroup::Active,
        false,
    ))?;
    let rebuild_result = runtime.block_on(rebuild_derived_indexes_from_state(
        &clickhouse_url,
        TableGroup::Active,
    ));
    if let Err(err) = rebuild_result {
        warn!(
            "[clickhouse] derived-only repair failed; active MERGE remains paused for retry: {}",
            err
        );
        return Err(err);
    }
    runtime.block_on(set_group_table_merges(
        &clickhouse_url,
        TableGroup::Active,
        true,
    ))?;
    info!("[clickhouse] derived-only repair 成功，active raw+hot MERGE 已恢复");
    Ok(())
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
        let hot_mints = runtime.block_on(load_enabled_hot_mints(&clickhouse_url))?;
        let stats = match runtime.block_on(
            ClickhouseIndexer::new(
                clickhouse_url,
                snapshot_slot,
                append_vec_count,
                TableGroup::Active,
                hot_mints,
            )?
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
        let pause_merges = matches!(snapshot_kind, SnapshotKind::Full);
        if pause_merges {
            runtime.block_on(set_group_table_merges(
                &clickhouse_url,
                TableGroup::Active,
                false,
            ))?;
            runtime.block_on(reset_table_group(&clickhouse_url, TableGroup::Active))?;
            info!(
                "[clickhouse] standalone full snapshot cold load: active group reset and merges paused before INSERT"
            );
        }
        let hot_mints = if matches!(snapshot_kind, SnapshotKind::Full) {
            runtime.block_on(snapshot_hot_mints(&clickhouse_url))?
        } else {
            runtime.block_on(load_enabled_hot_mints(&clickhouse_url))?
        };
        let indexer = ClickhouseIndexer::new(
            clickhouse_url.clone(),
            snapshot_slot,
            append_vec_count,
            TableGroup::Active,
            hot_mints,
        )?;
        let import_result = runtime.block_on(indexer.insert_all(
            loader.iter(),
            snapshot_kind,
            args.clickhouse_workers,
        ));
        let stats = if pause_merges {
            match import_result {
                Ok(stats) => {
                    runtime.block_on(set_group_table_merges(
                        &clickhouse_url,
                        TableGroup::Active,
                        true,
                    ))?;
                    runtime.block_on(wait_for_group_merges_to_settle(
                        &clickhouse_url,
                        TableGroup::Active,
                    ))?;
                    stats
                }
                Err(err) => {
                    warn!(
                        "[clickhouse] full snapshot import failed; leaving raw active MERGE paused for cleanup/rebuild"
                    );
                    console_snapshot_status(
                        "failed",
                        snapshot_kind.as_str(),
                        source_path,
                        snapshot_slot,
                    );
                    return Err(err);
                }
            }
        } else {
            match import_result {
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
        hot_mints: Option<HotMintSet>,
    },
}

enum WatchedSnapshot {
    Incremental(IncrementalSnapshot),
    Full(FullSnapshot),
}

/// A staging full snapshot is built independently of the watcher thread so
/// that the already-serving active path can continue consuming incrementals.
/// The snapshot value is retained for cleanup/retry if the background build
/// fails.
struct StagingBuild {
    snapshot: FullSnapshotWatcherState,
    /// The full import sends its frozen mint set only after all full rows and
    /// derived tables have committed. The watcher durably checkpoints
    /// `full_merging` before acknowledging that the worker may start MERGE.
    full_imported: Option<Receiver<HotMintSet>>,
    full_import_ack: Option<Sender<()>>,
    handle: JoinHandle<Result<(), String>>,
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
        Self::from_config(clickhouse_url, args.clickhouse_workers)
    }

    fn from_config(
        clickhouse_url: String,
        workers: usize,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        Ok(Self::Clickhouse {
            clickhouse_url,
            runtime,
            workers,
            hot_mints: None,
        })
    }

    fn clickhouse_config(&self) -> (String, usize) {
        match self {
            Self::Clickhouse {
                clickhouse_url,
                workers,
                ..
            } => (clickhouse_url.clone(), *workers),
        }
    }

    /// Full builds take a new snapshot of the mutable global configuration;
    /// incrementals reuse this process-local group snapshot. Watch mode
    /// restores that snapshot from its persisted local mint file before this
    /// method is used for an incremental.
    fn hot_mints_for(
        &mut self,
        snapshot_kind: SnapshotKind,
    ) -> Result<HotMintSet, Box<dyn std::error::Error>> {
        match self {
            Self::Clickhouse {
                clickhouse_url,
                runtime,
                hot_mints,
                ..
            } => {
                let mints = if matches!(snapshot_kind, SnapshotKind::Full) {
                    runtime.block_on(snapshot_hot_mints(clickhouse_url))?
                } else if let Some(mints) = hot_mints.as_ref() {
                    std::sync::Arc::clone(mints)
                } else {
                    runtime.block_on(load_enabled_hot_mints(clickhouse_url))?
                };
                *hot_mints = Some(std::sync::Arc::clone(&mints));
                Ok(mints)
            }
        }
    }

    fn set_hot_mints(&mut self, mints: HotMintSet) {
        match self {
            Self::Clickhouse { hot_mints, .. } => *hot_mints = Some(mints),
        }
    }

    fn frozen_hot_mints(&self) -> Result<HotMintSet, Box<dyn std::error::Error>> {
        match self {
            Self::Clickhouse { hot_mints, .. } => hot_mints
                .as_ref()
                .map(std::sync::Arc::clone)
                .ok_or_else(|| "no frozen hot-token set is loaded for this output".into()),
        }
    }

    fn max_raw_account_updated_slot(&self) -> Result<u64, Box<dyn std::error::Error>> {
        match self {
            Self::Clickhouse {
                clickhouse_url,
                runtime,
                ..
            } => runtime
                .block_on(max_raw_account_updated_slot(
                    clickhouse_url,
                    TableGroup::Active,
                ))
                .map_err(Into::into),
        }
    }

    fn load_enabled_hot_mints(&self) -> Result<HotMintSet, Box<dyn std::error::Error>> {
        match self {
            Self::Clickhouse {
                clickhouse_url,
                runtime,
                ..
            } => runtime
                .block_on(load_enabled_hot_mints(clickhouse_url))
                .map_err(Into::into),
        }
    }

    /// Capture the mutable global selection for a newly bound staging full.
    /// The caller persists this set before destructive `_bak` work so a retry
    /// keeps the same generation definition.
    fn snapshot_hot_mints(&self) -> Result<HotMintSet, Box<dyn std::error::Error>> {
        match self {
            Self::Clickhouse {
                clickhouse_url,
                runtime,
                ..
            } => runtime
                .block_on(snapshot_hot_mints(clickhouse_url))
                .map_err(Into::into),
        }
    }

    fn reset_group(&mut self, group: TableGroup) -> Result<(), Box<dyn std::error::Error>> {
        match self {
            Self::Clickhouse {
                clickhouse_url,
                runtime,
                ..
            } => runtime.block_on(reset_table_group(clickhouse_url, group)),
        }
    }

    fn stop_group_merges(&mut self, group: TableGroup) -> Result<(), Box<dyn std::error::Error>> {
        match self {
            Self::Clickhouse {
                clickhouse_url,
                runtime,
                ..
            } => runtime.block_on(set_group_table_merges(clickhouse_url, group, false)),
        }
    }

    fn start_group_merges(&mut self, group: TableGroup) -> Result<(), Box<dyn std::error::Error>> {
        match self {
            Self::Clickhouse {
                clickhouse_url,
                runtime,
                ..
            } => runtime.block_on(set_group_table_merges(clickhouse_url, group, true)),
        }
    }

    fn wait_for_group_merges_to_settle(
        &mut self,
        group: TableGroup,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match self {
            Self::Clickhouse {
                clickhouse_url,
                runtime,
                ..
            } => runtime.block_on(wait_for_group_merges_to_settle(clickhouse_url, group)),
        }
    }

    fn table_group_identity(
        &mut self,
        group: TableGroup,
    ) -> Result<TableGroupIdentity, Box<dyn std::error::Error>> {
        match self {
            Self::Clickhouse {
                clickhouse_url,
                runtime,
                ..
            } => runtime.block_on(table_group_identity(clickhouse_url, group)),
        }
    }

    fn exchange_groups(
        &mut self,
        active_identity: &TableGroupIdentity,
        staging_identity: &TableGroupIdentity,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match self {
            Self::Clickhouse {
                clickhouse_url,
                runtime,
                ..
            } => runtime.block_on(exchange_table_groups(
                clickhouse_url,
                active_identity,
                staging_identity,
            )),
        }
    }

    fn validate_staging_group(
        &mut self,
        frozen_hot_mint_count: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match self {
            Self::Clickhouse {
                clickhouse_url,
                runtime,
                ..
            } => runtime.block_on(validate_staging_group(
                clickhouse_url,
                frozen_hot_mint_count,
            )),
        }
    }

    fn process(
        &mut self,
        loader: &mut SupportedLoader,
        snapshot_kind: SnapshotKind,
        group: TableGroup,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let hot_mints = self.hot_mints_for(snapshot_kind)?;
        match self {
            Self::Clickhouse {
                clickhouse_url,
                runtime,
                workers,
                ..
            } => {
                let snapshot_slot = loader.snapshot_slot();
                let append_vec_count = loader.append_vec_count_hint();
                info!(
                    "[clickhouse] processing {} snapshot slot={} group={} append_vecs={:?}",
                    snapshot_kind.as_str(),
                    snapshot_slot,
                    group.as_str(),
                    append_vec_count
                );
                debug!(
                    "Dumping {} snapshot slot {snapshot_slot} to ClickHouse",
                    snapshot_kind.as_str()
                );
                let stats = runtime.block_on(
                    ClickhouseIndexer::new(
                        clickhouse_url.clone(),
                        snapshot_slot,
                        append_vec_count,
                        group,
                        hot_mints,
                    )?
                    .insert_all(loader.iter(), snapshot_kind, *workers),
                )?;
                log_clickhouse_index_stats(&stats);
                info!(
                    "[clickhouse] completed {} snapshot slot={} group={} accounts={} token_accounts={}",
                    snapshot_kind.as_str(),
                    snapshot_slot,
                    group.as_str(),
                    stats.accounts_total,
                    stats.token_accounts_total
                );
            }
        }
        Ok(())
    }

    /// Import one full archive into staging and the active tail without
    /// opening/decompressing the archive twice.  `self` deliberately keeps
    /// its existing active frozen mint set; `staging_hot_mints` is the new
    /// generation's separately frozen configuration.
    fn process_shared_full_fanout(
        &mut self,
        loader: &mut SupportedLoader,
        active_resume_slot: u64,
        staging_hot_mints: HotMintSet,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let active_hot_mints = self.frozen_hot_mints()?;
        match self {
            Self::Clickhouse {
                clickhouse_url,
                runtime,
                workers,
                ..
            } => {
                let snapshot_slot = loader.snapshot_slot();
                let append_vec_count = loader.append_vec_count_hint();
                info!(
                    "[clickhouse] processing shared full fanout slot={} staging=all active_tail_after_slot={} append_vecs={:?}",
                    snapshot_slot,
                    active_resume_slot,
                    append_vec_count
                );
                let stats = runtime.block_on(import_full_snapshot_fanout(
                    clickhouse_url.clone(),
                    snapshot_slot,
                    append_vec_count,
                    loader.iter(),
                    *workers,
                    active_resume_slot,
                    active_hot_mints,
                    staging_hot_mints,
                ))?;
                log_clickhouse_index_stats(&stats);
                info!(
                    "[clickhouse] completed shared full fanout slot={} staging=full active_tail_after_slot={} accounts={} token_accounts={}",
                    snapshot_slot,
                    active_resume_slot,
                    stats.accounts_total,
                    stats.token_accounts_total
                );
            }
        }
        Ok(())
    }
}

/// A failed staging generation must never take the already-serving active
/// generation down.  Keep the backup raw Merge paused, clear all six backup
/// tables, and let the watcher retry the full snapshot on its next iteration.
/// Cleanup is best-effort here; a later retry will attempt the TRUNCATE again.
fn reset_failed_staging(
    output: &mut IncrementalOutput,
    snapshot: &FullSnapshotWatcherState,
    poll_interval: Duration,
    reason: &str,
) {
    warn!(
        "[switch] staging 全量失败，active 继续服役；清理 _bak 后重试 file={} slot={} reason={}",
        snapshot.path.display(),
        snapshot.slot,
        reason
    );
    if let Err(err) = output.stop_group_merges(TableGroup::Backup) {
        warn!(
            "[switch] staging 失败后再次暂停 _bak raw+hot MERGE 失败：{}",
            err
        );
    }
    match output.reset_group(TableGroup::Backup) {
        Ok(()) => info!("[switch] staging 失败后的 _bak 六张表已清理，等待后重试该全量"),
        Err(err) => warn!(
            "[switch] staging 失败后的 _bak 清理失败（下次重试时会再次清理）：{}",
            err
        ),
    }
    thread::sleep(poll_interval);
}

/// Build a fresh `_bak` generation on a dedicated worker thread. The watcher
/// keeps its own ClickHouse runtime for active work, while this worker owns a
/// separate runtime/client for staging. The two paths intentionally run
/// independently because their tables use separate ClickHouse disks.
fn build_staging_full(
    snapshot: FullSnapshot,
    clickhouse_url: String,
    workers: usize,
    full_imported: Sender<HotMintSet>,
    full_import_ack: Receiver<()>,
) -> Result<(), String> {
    let mut output = IncrementalOutput::from_config(clickhouse_url, workers)
        .map_err(|err| format!("failed to create staging ClickHouse runtime: {err}"))?;
    let candidate = WatchedSnapshot::Full(snapshot);
    let group = TableGroup::Backup;

    output
        .stop_group_merges(group)
        .map_err(|err| format!("failed to pause _bak raw+hot MERGE: {err}"))?;
    output
        .reset_group(group)
        .map_err(|err| format!("failed to reset _bak group: {err}"))?;
    let mut loader = candidate
        .new_loader(0)
        .map_err(|err| format!("failed to create staging full snapshot loader: {err}"))?;
    output
        .process(&mut loader, SnapshotKind::Full, group)
        .map_err(|err| format!("staging full snapshot import/derived refresh failed: {err}"))?;
    let hot_mints = output
        .frozen_hot_mints()
        .map_err(|err| format!("staging full completed without frozen hot-token set: {err}"))?;
    // Do not begin background MERGE until the watcher has atomically written
    // the `full_merging` state and mint-file path. This makes an interruption
    // during the lengthy convergence wait resumable without rebuilding full.
    full_imported.send(hot_mints).map_err(|_| {
        "watcher stopped before it could checkpoint staging full_merging".to_owned()
    })?;
    full_import_ack
        .recv()
        .map_err(|_| "watcher stopped before acknowledging staging full_merging".to_owned())?;
    output
        .start_group_merges(group)
        .map_err(|err| format!("staging full snapshot succeeded but START MERGES failed: {err}"))?;
    output
        .wait_for_group_merges_to_settle(group)
        .map_err(|err| format!("staging raw+hot MERGE settle check failed: {err}"))
}

/// Resume only the post-full Merge convergence phase for `_bak`. This is used
/// after a restart from `full_merging`; it never truncates or re-imports data.
fn wait_for_staging_merges(clickhouse_url: String, workers: usize) -> Result<(), String> {
    let mut output = IncrementalOutput::from_config(clickhouse_url, workers)
        .map_err(|err| format!("failed to create staging ClickHouse runtime: {err}"))?;
    output
        .start_group_merges(TableGroup::Backup)
        .map_err(|err| format!("failed to start _bak raw+hot MERGE: {err}"))?;
    output
        .wait_for_group_merges_to_settle(TableGroup::Backup)
        .map_err(|err| format!("staging raw+hot MERGE settle check failed: {err}"))
}

/// Apply one incremental to the backup group on its own ClickHouse runtime.
/// The watcher uses this helper so active and staging can process the same
/// archive concurrently once the backup generation is ready.
fn process_incremental_in_background(
    snapshot: IncrementalSnapshot,
    clickhouse_url: String,
    workers: usize,
    group: TableGroup,
    resume_slot: u64,
    hot_mints: HotMintSet,
) -> Result<(), String> {
    let mut output = IncrementalOutput::from_config(clickhouse_url, workers).map_err(|err| {
        format!("failed to create {group:?} incremental ClickHouse runtime: {err}")
    })?;
    output.set_hot_mints(hot_mints);
    let candidate = WatchedSnapshot::Incremental(snapshot);
    let mut loader = candidate
        .new_loader(resume_slot)
        .map_err(|err| format!("failed to create {group:?} incremental snapshot loader: {err}"))?;
    output
        .process(&mut loader, SnapshotKind::Incremental, group)
        .map_err(|err| format!("{group:?} incremental import failed: {err}"))
}

fn spawn_staging_incremental(
    snapshot: IncrementalSnapshot,
    clickhouse_url: String,
    workers: usize,
    stage_slot: u64,
    hot_mints: HotMintSet,
) -> Result<JoinHandle<Result<u64, String>>, String> {
    let candidate_slot = snapshot.slot();
    thread::Builder::new()
        .name("staging-incremental".to_owned())
        .spawn(move || {
            process_incremental_in_background(
                snapshot,
                clickhouse_url,
                workers,
                TableGroup::Backup,
                stage_slot,
                hot_mints,
            )
            .map(|_| candidate_slot)
        })
        .map_err(|err| format!("failed to spawn staging incremental worker: {err}"))
}

const WATCHER_STATE_FILENAME: &str = "solana-snapshot-etl-state.json";
const WATCHER_STATE_VERSION: u32 = 5;
const HOT_MINTS_FILE_PREFIX: &str = "solana-snapshot-etl-hot-mints-";
const HOT_MINTS_FILE_SUFFIX: &str = ".txt";

/// The watcher state is deliberately local to the process working directory.
/// ClickHouse holds the data generations, while this file is the durable
/// journal for their full-snapshot generation and completed slot watermarks.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct WatcherState {
    version: u32,
    active: LaneWatcherState,
    staging: LaneWatcherState,
    #[serde(default)]
    cutover: Option<CutoverWatcherState>,
    /// Present only while one full archive is being decoded once and fanned
    /// out into staging plus the active generation's unseen tail.
    #[serde(default)]
    shared_full_load: Option<SharedFullLoadWatcherState>,
}

/// A full snapshot identifies a generation. Its `slot` is deliberately
/// independent from `max_slot`: later incrementals advance only `max_slot`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct FullSnapshotWatcherState {
    path: PathBuf,
    slot: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum LanePhase {
    /// No full snapshot is assigned yet. A retained old `_bak` generation may
    /// still be described by `full_snapshot`/`max_slot`.
    Disabled,
    /// Bootstrap has discarded the previous journal and is waiting to select
    /// its first full archive.
    WaitingForFull,
    /// A fixed full archive is being imported; retry that exact archive after
    /// a restart.
    FullLoading,
    /// The full rows and derived indexes are committed (`max_slot` is set),
    /// but ClickHouse background MERGE has not yet reached the stability
    /// barrier. Restart resumes only MERGE/start-wait, never the full import.
    FullMerging,
    /// Full plus all recorded incrementals are committed and this lane can
    /// accept the next incremental.
    Ready,
    /// `inflight_incremental` was checkpointed before the import started.
    IncrementalLoading,
    /// Staging completed its first incremental and active has stopped taking
    /// new work. The next action is the table-group cutover.
    CutoverPending,
}

impl Default for LanePhase {
    fn default() -> Self {
        Self::Disabled
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum PersistedSnapshotKind {
    Full,
    Incremental,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct InflightSnapshotWatcherState {
    kind: PersistedSnapshotKind,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct LaneWatcherState {
    #[serde(default)]
    phase: LanePhase,
    #[serde(default)]
    full_snapshot: Option<FullSnapshotWatcherState>,
    /// Largest slot whose import finished successfully. `None` means that
    /// this generation has not completed its full import yet.
    #[serde(default)]
    max_slot: Option<u64>,
    #[serde(default)]
    hot_mints_path: Option<PathBuf>,
    #[serde(default)]
    inflight_incremental: Option<InflightSnapshotWatcherState>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct CutoverWatcherState {
    active_tables: TableGroupIdentity,
    staging_tables: TableGroupIdentity,
}

/// Durable identity of the coordinated full import. `active_resume_slot` is
/// captured before the archive starts and must not drift while the active
/// bridge is in flight, otherwise a restart could skip a different tail.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct SharedFullLoadWatcherState {
    snapshot: FullSnapshotWatcherState,
    active_resume_slot: u64,
}

impl LaneWatcherState {
    fn disabled() -> Self {
        Self {
            phase: LanePhase::Disabled,
            full_snapshot: None,
            max_slot: None,
            hot_mints_path: None,
            inflight_incremental: None,
        }
    }

    fn waiting_for_full() -> Self {
        Self {
            phase: LanePhase::WaitingForFull,
            ..Self::disabled()
        }
    }

    fn full_loading(snapshot: FullSnapshotWatcherState) -> Self {
        Self {
            phase: LanePhase::FullLoading,
            full_snapshot: Some(snapshot),
            ..Self::disabled()
        }
    }
}

impl WatcherState {
    fn bootstrap_waiting_for_full() -> Self {
        Self {
            version: WATCHER_STATE_VERSION,
            active: LaneWatcherState::waiting_for_full(),
            staging: LaneWatcherState::disabled(),
            cutover: None,
            shared_full_load: None,
        }
    }

    fn recovered_active_only(max_slot: u64) -> Self {
        Self {
            version: WATCHER_STATE_VERSION,
            active: LaneWatcherState {
                phase: LanePhase::Ready,
                full_snapshot: None,
                max_slot: Some(max_slot),
                hot_mints_path: None,
                inflight_incremental: None,
            },
            staging: LaneWatcherState::disabled(),
            cutover: None,
            shared_full_load: None,
        }
    }
}

/// Version 2 did not retain the active generation's full slot. Keep its
/// committed watermarks and any staging work, but leave that one historical
/// full slot unknown rather than inventing a false generation boundary.
#[derive(Deserialize)]
struct LegacyWatcherStateV2 {
    active_slot: u64,
    #[serde(default)]
    active_hot_mints_path: Option<PathBuf>,
    staging: Option<LegacyStagingWatcherStateV2>,
}

#[derive(Deserialize)]
struct LegacyStagingWatcherStateV2 {
    full_snapshot_path: PathBuf,
    full_snapshot_slot: u64,
    #[serde(default)]
    ready_slot: Option<u64>,
    #[serde(default)]
    hot_mints_path: Option<PathBuf>,
    #[serde(default)]
    first_incremental: Option<LegacyIncrementalWatcherStateV2>,
    #[serde(default)]
    first_incremental_completed: bool,
}

#[derive(Deserialize)]
struct LegacyIncrementalWatcherStateV2 {}

fn migrate_v2_state(legacy: LegacyWatcherStateV2) -> WatcherState {
    let active = LaneWatcherState {
        phase: LanePhase::Ready,
        full_snapshot: None,
        max_slot: Some(legacy.active_slot),
        hot_mints_path: legacy.active_hot_mints_path,
        inflight_incremental: None,
    };
    let staging = match legacy.staging {
        None => LaneWatcherState::disabled(),
        Some(legacy_staging) => {
            let full_snapshot = Some(FullSnapshotWatcherState {
                path: legacy_staging.full_snapshot_path,
                slot: legacy_staging.full_snapshot_slot,
            });
            let inflight_incremental =
                legacy_staging
                    .first_incremental
                    .map(|_| InflightSnapshotWatcherState {
                        kind: PersistedSnapshotKind::Incremental,
                    });
            let phase = if legacy_staging.ready_slot.is_none() {
                LanePhase::FullLoading
            } else if legacy_staging.first_incremental_completed {
                // v2 could have crashed during its six independent exchanges
                // without recording which pairs moved. Do one more staging
                // incremental under v3 instead of risking a blind exchange.
                LanePhase::Ready
            } else if inflight_incremental.is_some() {
                LanePhase::IncrementalLoading
            } else {
                LanePhase::Ready
            };
            LaneWatcherState {
                phase,
                full_snapshot,
                max_slot: legacy_staging.ready_slot,
                hot_mints_path: legacy_staging.hot_mints_path,
                inflight_incremental,
            }
        }
    };
    WatcherState {
        version: WATCHER_STATE_VERSION,
        active,
        staging,
        cutover: None,
        shared_full_load: None,
    }
}

fn watcher_state_path() -> Result<PathBuf, Box<dyn std::error::Error>> {
    Ok(std::env::current_dir()?.join(WATCHER_STATE_FILENAME))
}

fn load_watcher_state(path: &Path) -> Result<Option<WatcherState>, Box<dyn std::error::Error>> {
    let contents = match fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(format!("failed to read watcher state {}: {err}", path.display()).into())
        }
    };
    let value: serde_json::Value = serde_json::from_str(&contents)
        .map_err(|err| format!("failed to parse watcher state {}: {err}", path.display()))?;
    let version = value
        .get("version")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| format!("watcher state {} has no numeric version", path.display()))?;
    let state = if version == WATCHER_STATE_VERSION as u64 {
        serde_json::from_value(value).map_err(|err| {
            format!(
                "failed to parse v{} watcher state {}: {err}",
                WATCHER_STATE_VERSION,
                path.display()
            )
        })?
    } else if version == 4 {
        let mut migrated: WatcherState = serde_json::from_value(value)
            .map_err(|err| format!("failed to parse v4 watcher state {}: {err}", path.display()))?;
        migrated.version = WATCHER_STATE_VERSION;
        persist_watcher_state(path, &migrated)?;
        warn!("[watcher] 已将 v4 状态迁移到 v5；中断增量将按已提交水位重新选择当前可用 archive");
        migrated
    } else if version == 3 {
        let mut migrated: WatcherState = serde_json::from_value(value)
            .map_err(|err| format!("failed to parse v3 watcher state {}: {err}", path.display()))?;
        migrated.version = WATCHER_STATE_VERSION;
        migrated.shared_full_load = None;
        persist_watcher_state(path, &migrated)?;
        warn!("[watcher] 已将 v3 状态迁移到 v5；后续新 full 将使用单次解压的双路分流，中断增量将重新选择当前可用 archive");
        migrated
    } else if version == 2 {
        let migrated = migrate_v2_state(serde_json::from_value(value).map_err(|err| {
            format!("failed to parse v2 watcher state {}: {err}", path.display())
        })?);
        persist_watcher_state(path, &migrated)?;
        warn!(
            "[watcher] 已将 v2 状态迁移到 v5；旧 active 的 full_slot 无法反推，下一次可用 full 会建立新的 staging 代际"
        );
        migrated
    } else {
        return Err(format!(
            "unsupported watcher state version {} in {}; expected {}",
            version,
            path.display(),
            WATCHER_STATE_VERSION
        )
        .into());
    };
    Ok(Some(state))
}

fn persist_watcher_state(
    path: &Path,
    state: &WatcherState,
) -> Result<(), Box<dyn std::error::Error>> {
    let bytes = serde_json::to_vec_pretty(state)
        .map_err(|err| format!("failed to serialize watcher state: {err}"))?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            format!(
                "watcher state path has no UTF-8 file name: {}",
                path.display()
            )
        })?;
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let temporary =
        path.with_file_name(format!(".{file_name}.{}.{}.tmp", std::process::id(), nonce));
    let write_result = (|| -> Result<(), Box<dyn std::error::Error>> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        file.write_all(&bytes)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        File::open(path.parent().unwrap_or_else(|| Path::new(".")))?.sync_all()?;
        Ok(())
    })();
    if write_result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    write_result
}

/// Persist the large frozen mint set independently of the small JSON control
/// record. Rewriting tens of millions of mints after every slot would make
/// the state checkpoint needlessly expensive; this file is written only when
/// a new active or staging generation is built.
fn persist_frozen_hot_mints(
    state_path: &Path,
    hot_mints: &HotMintSet,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let directory = state_path.parent().unwrap_or_else(|| Path::new("."));
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let path = directory.join(format!(
        "{HOT_MINTS_FILE_PREFIX}{}.{}{HOT_MINTS_FILE_SUFFIX}",
        std::process::id(),
        nonce
    ));
    let temporary = directory.join(format!(
        ".solana-snapshot-etl-hot-mints-{}.{}.tmp",
        std::process::id(),
        nonce
    ));
    let write_result = (|| -> Result<(), Box<dyn std::error::Error>> {
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        let mut writer = BufWriter::new(file);
        for mint in hot_mints.iter() {
            writer.write_all(mint.as_bytes())?;
            writer.write_all(b"\n")?;
        }
        writer.flush()?;
        writer.get_ref().sync_all()?;
        fs::rename(&temporary, &path)?;
        File::open(directory)?.sync_all()?;
        Ok(())
    })();
    if write_result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    write_result?;
    info!(
        "[watcher] 已持久化冻结 hot-mint 集合 file={} mint_count={}",
        path.display(),
        hot_mints.len()
    );
    Ok(path)
}

fn is_managed_hot_mints_file(path: &Path, directory: &Path) -> bool {
    path.parent() == Some(directory)
        && path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| {
                name.starts_with(HOT_MINTS_FILE_PREFIX) && name.ends_with(HOT_MINTS_FILE_SUFFIX)
            })
}

/// Remove generated frozen-mint files that are no longer referenced by a
/// working lane.  Do not follow paths from the state file outside its own
/// directory: only files created by `persist_frozen_hot_mints` are managed.
fn cleanup_unused_frozen_hot_mints(
    state_path: &Path,
    state: &WatcherState,
) -> Result<usize, Box<dyn std::error::Error>> {
    let directory = state_path.parent().unwrap_or_else(|| Path::new("."));
    let referenced = [
        (state.active.phase, state.active.hot_mints_path.as_ref()),
        (state.staging.phase, state.staging.hot_mints_path.as_ref()),
    ];
    let mut removed = 0;
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();
        if !is_managed_hot_mints_file(&path, directory)
            || referenced.iter().any(|(phase, candidate)| {
                *phase != LanePhase::Disabled
                    && candidate.is_some_and(|candidate| candidate == &path)
            })
        {
            continue;
        }
        match fs::remove_file(&path) {
            Ok(()) => {
                info!(
                    "[watcher] 已清理不再使用的 hot-mint 文件 file={}",
                    path.display()
                );
                removed += 1;
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                return Err(format!(
                    "failed to remove unused hot-mint file {}: {err}",
                    path.display()
                )
                .into())
            }
        }
    }
    if removed > 0 {
        File::open(directory)?.sync_all()?;
    }
    Ok(removed)
}

fn cleanup_unused_frozen_hot_mints_or_warn(state_path: &Path, state: &WatcherState) {
    if let Err(err) = cleanup_unused_frozen_hot_mints(state_path, state) {
        warn!("[watcher] 清理不再使用的 hot-mint 文件失败：{err}");
    }
}

/// A disabled lane keeps its table-generation watermark for diagnostics, but
/// no longer needs its potentially very large frozen mint file.
fn clear_disabled_hot_mints_paths(state: &mut WatcherState) -> bool {
    let mut changed = false;
    for lane in [&mut state.active, &mut state.staging] {
        if lane.phase == LanePhase::Disabled && lane.hot_mints_path.take().is_some() {
            changed = true;
        }
    }
    changed
}

fn load_frozen_hot_mints(path: &Path) -> Result<HotMintSet, Box<dyn std::error::Error>> {
    let file = File::open(path).map_err(|err| {
        format!(
            "failed to open frozen hot-mint file {}: {err}",
            path.display()
        )
    })?;
    let mut mints = HashSet::new();
    for line in BufReader::new(file).lines() {
        let mint = line.map_err(|err| {
            format!(
                "failed to read frozen hot-mint file {}: {err}",
                path.display()
            )
        })?;
        if mint.is_empty() {
            return Err(format!(
                "frozen hot-mint file {} contains an empty mint",
                path.display()
            )
            .into());
        }
        mints.insert(mint);
    }
    if mints.is_empty() {
        return Err(format!("frozen hot-mint file {} is empty", path.display()).into());
    }
    info!(
        "[watcher] 已恢复冻结 hot-mint 集合 file={} mint_count={}",
        path.display(),
        mints.len()
    );
    Ok(std::sync::Arc::new(mints))
}

fn full_snapshot_state(snapshot: &FullSnapshot) -> FullSnapshotWatcherState {
    FullSnapshotWatcherState {
        path: snapshot.path().to_path_buf(),
        slot: snapshot.slot(),
    }
}

fn find_recorded_full(
    fulls: &[FullSnapshot],
    recorded: &FullSnapshotWatcherState,
) -> Result<FullSnapshot, Box<dyn std::error::Error>> {
    fulls
        .iter()
        .find(|snapshot| snapshot.slot() == recorded.slot && snapshot.path() == recorded.path)
        .cloned()
        .ok_or_else(|| {
            format!(
                "cannot resume full snapshot slot={}: archive {} is missing",
                recorded.slot,
                recorded.path.display()
            )
            .into()
        })
}

fn inflight_snapshot(snapshot: &WatchedSnapshot) -> InflightSnapshotWatcherState {
    let kind = match snapshot {
        WatchedSnapshot::Full(_) => PersistedSnapshotKind::Full,
        WatchedSnapshot::Incremental(_) => PersistedSnapshotKind::Incremental,
    };
    InflightSnapshotWatcherState { kind }
}

/// Pick the current archive with the highest ending slot that can continue
/// from the committed watermark.  Archive names are intentionally not kept in
/// the state journal because an interrupted archive may have been rolled away
/// before the next process starts.
fn newest_eligible_incremental(
    incrementals: &[IncrementalSnapshot],
    committed_slot: u64,
) -> Option<IncrementalSnapshot> {
    eligible_candidates(incrementals.to_vec(), committed_slot)
        .into_iter()
        .next()
}

/// Apply a full archive's new tail or a normal incremental to active. This
/// advances only `max_slot`; `active.full_snapshot` remains the generation.
fn apply_active_incremental(
    output: &mut IncrementalOutput,
    state_path: &Path,
    watcher_state: &mut WatcherState,
    active_slot: &mut u64,
    candidate: &WatchedSnapshot,
) -> Result<bool, Box<dyn std::error::Error>> {
    if candidate.slot() <= *active_slot {
        return Ok(false);
    }
    let mut loader = candidate.new_loader(*active_slot)?;
    let append_vecs = loader.append_vec_count_hint().unwrap_or(0);
    watcher_state.active.phase = LanePhase::IncrementalLoading;
    watcher_state.active.inflight_incremental = Some(inflight_snapshot(candidate));
    persist_watcher_state(state_path, watcher_state)?;
    info!(
        "[watcher] active 开始增量处理 file={} slot={} resume_slot={} append_vecs={}",
        candidate.path().display(),
        candidate.slot(),
        *active_slot,
        append_vecs
    );
    output.process(&mut loader, SnapshotKind::Incremental, TableGroup::Active)?;
    *active_slot = candidate.slot();
    watcher_state.active.max_slot = Some(*active_slot);
    watcher_state.active.phase = LanePhase::Ready;
    watcher_state.active.inflight_incremental = None;
    persist_watcher_state(state_path, watcher_state)?;
    console_snapshot_status(
        "completed",
        SnapshotKind::Incremental.as_str(),
        candidate.path(),
        candidate.slot(),
    );
    Ok(true)
}

fn process_bootstrap_full(
    output: &mut IncrementalOutput,
    state_path: &Path,
    watcher_state: &mut WatcherState,
    snapshot: FullSnapshot,
) -> Result<u64, Box<dyn std::error::Error>> {
    watcher_state.active = LaneWatcherState::full_loading(full_snapshot_state(&snapshot));
    watcher_state.staging = LaneWatcherState::disabled();
    watcher_state.cutover = None;
    // This checkpoint precedes STOP MERGES/TRUNCATE. A plain restart will
    // therefore resume the selected bootstrap full instead of old history.
    persist_watcher_state(state_path, watcher_state)?;
    info!(
        "[switch] bootstrap 绑定 active full_slot={}；清空 active 并从 slot 0 灌入",
        snapshot.slot()
    );
    output.stop_group_merges(TableGroup::Active)?;
    output.reset_group(TableGroup::Active)?;
    let candidate = WatchedSnapshot::Full(snapshot.clone());
    let mut loader = candidate.new_loader(0)?;
    output.process(&mut loader, SnapshotKind::Full, TableGroup::Active)?;
    let hot_mints_path = persist_frozen_hot_mints(state_path, &output.frozen_hot_mints()?)?;
    watcher_state.active.phase = LanePhase::FullMerging;
    watcher_state.active.max_slot = Some(snapshot.slot());
    watcher_state.active.hot_mints_path = Some(hot_mints_path);
    watcher_state.active.inflight_incremental = None;
    persist_watcher_state(state_path, watcher_state)?;
    info!(
        "[watcher] bootstrap 全量已提交：active full_slot={} max_slot={}；开始等待 MERGE 稳定",
        snapshot.slot(),
        snapshot.slot()
    );
    output.start_group_merges(TableGroup::Active)?;
    output.wait_for_group_merges_to_settle(TableGroup::Active)?;
    watcher_state.active.phase = LanePhase::Ready;
    persist_watcher_state(state_path, watcher_state)?;
    info!(
        "[watcher] bootstrap 全量完成：active full_slot={} max_slot={}",
        snapshot.slot(),
        snapshot.slot()
    );
    console_snapshot_status(
        "completed",
        SnapshotKind::Full.as_str(),
        snapshot.path(),
        snapshot.slot(),
    );
    Ok(snapshot.slot())
}

fn spawn_staging_full(
    output: &IncrementalOutput,
    snapshot: FullSnapshot,
) -> Result<StagingBuild, Box<dyn std::error::Error>> {
    let (clickhouse_url, workers) = output.clickhouse_config();
    let snapshot_state = full_snapshot_state(&snapshot);
    let build_snapshot = snapshot.clone();
    let (full_imported_tx, full_imported_rx) = mpsc::channel();
    let (full_import_ack_tx, full_import_ack_rx) = mpsc::channel();
    let handle = thread::Builder::new()
        .name("staging-full".to_owned())
        .spawn(move || {
            build_staging_full(
                build_snapshot,
                clickhouse_url,
                workers,
                full_imported_tx,
                full_import_ack_rx,
            )
        })
        .map_err(|err| format!("failed to spawn staging full worker: {err}"))?;
    Ok(StagingBuild {
        snapshot: snapshot_state,
        full_imported: Some(full_imported_rx),
        full_import_ack: Some(full_import_ack_tx),
        handle,
    })
}

fn spawn_staging_merge_wait(
    output: &IncrementalOutput,
    snapshot: FullSnapshotWatcherState,
) -> Result<StagingBuild, Box<dyn std::error::Error>> {
    let (clickhouse_url, workers) = output.clickhouse_config();
    let handle = thread::Builder::new()
        .name("staging-merge-wait".to_owned())
        .spawn(move || wait_for_staging_merges(clickhouse_url, workers))
        .map_err(|err| format!("failed to spawn staging merge-wait worker: {err}"))?;
    Ok(StagingBuild {
        snapshot,
        full_imported: None,
        full_import_ack: None,
        handle,
    })
}

/// Checkpoint and prepare a newly observed full generation for a single
/// archive read that feeds both physical groups.  This is deliberately done
/// before STOP MERGES/TRUNCATE, so a restart knows that `_bak` must be rebuilt
/// from this exact archive and that active's bridge must use the captured
/// watermark.
fn begin_shared_full_load(
    output: &mut IncrementalOutput,
    state_path: &Path,
    watcher_state: &mut WatcherState,
    snapshot: &FullSnapshot,
) -> Result<HotMintSet, Box<dyn std::error::Error>> {
    if watcher_state.staging.phase != LanePhase::Disabled {
        return Err("cannot begin shared full load while staging is not disabled".into());
    }
    if watcher_state.active.phase != LanePhase::Ready {
        return Err("cannot begin shared full load while active is not ready".into());
    }
    let active_resume_slot = watcher_state
        .active
        .max_slot
        .ok_or("ready active has no committed max_slot")?;
    let staging_hot_mints = output.snapshot_hot_mints()?;
    let staging_hot_mints_path = persist_frozen_hot_mints(state_path, &staging_hot_mints)?;
    let snapshot_state = full_snapshot_state(snapshot);
    let active_candidate = WatchedSnapshot::Full(snapshot.clone());

    watcher_state.staging = LaneWatcherState::full_loading(snapshot_state.clone());
    watcher_state.staging.hot_mints_path = Some(staging_hot_mints_path);
    watcher_state.active.phase = LanePhase::IncrementalLoading;
    watcher_state.active.inflight_incremental = Some(inflight_snapshot(&active_candidate));
    watcher_state.cutover = None;
    watcher_state.shared_full_load = Some(SharedFullLoadWatcherState {
        snapshot: snapshot_state,
        active_resume_slot,
    });
    persist_watcher_state(state_path, watcher_state)?;
    cleanup_unused_frozen_hot_mints_or_warn(state_path, watcher_state);
    info!(
        "[switch] 新 full 到达：绑定 staging full_slot={}，捕获 active max_slot={}；准备停止并清空 _bak 后单次解压分流",
        snapshot.slot(),
        active_resume_slot
    );
    Ok(staging_hot_mints)
}

/// Execute or resume the checkpointed one-stream full fanout.  Both lanes
/// remain in their pre-completion phases until every raw/derived write has
/// committed, so retrying an interrupted attempt replays one deterministic
/// shared stream rather than falling back to two independent decompressions.
fn complete_shared_full_load(
    output: &mut IncrementalOutput,
    state_path: &Path,
    watcher_state: &mut WatcherState,
    snapshot: FullSnapshot,
    staging_hot_mints: HotMintSet,
) -> Result<Option<StagingBuild>, Box<dyn std::error::Error>> {
    let shared = watcher_state
        .shared_full_load
        .clone()
        .ok_or("shared full completion without a shared-full checkpoint")?;
    if watcher_state.staging.phase != LanePhase::FullLoading
        || watcher_state.active.phase != LanePhase::IncrementalLoading
    {
        return Err("shared full checkpoint has incompatible lane phases".into());
    }
    let active_inflight = watcher_state
        .active
        .inflight_incremental
        .as_ref()
        .ok_or("shared full checkpoint has no active inflight archive")?;
    if active_inflight.kind != PersistedSnapshotKind::Full
        || snapshot.path() != shared.snapshot.path
        || snapshot.slot() != shared.snapshot.slot
    {
        return Err("shared full checkpoint does not match the recorded archive".into());
    }
    if watcher_state.active.max_slot != Some(shared.active_resume_slot) {
        return Err(format!(
            "shared full checkpoint expected active max_slot={}, found {:?}",
            shared.active_resume_slot, watcher_state.active.max_slot
        )
        .into());
    }
    let mut loader = WatchedSnapshot::Full(snapshot.clone()).new_loader(0)?;
    output.process_shared_full_fanout(&mut loader, shared.active_resume_slot, staging_hot_mints)?;

    watcher_state.active.max_slot = Some(snapshot.slot());
    watcher_state.active.phase = LanePhase::Ready;
    watcher_state.active.inflight_incremental = None;
    watcher_state.staging.max_slot = Some(snapshot.slot());
    watcher_state.staging.phase = LanePhase::FullMerging;
    watcher_state.staging.inflight_incremental = None;
    watcher_state.shared_full_load = None;
    persist_watcher_state(state_path, watcher_state)?;
    info!(
        "[switch] 单次解压双路导入已提交：active bridge max_slot={}；staging full_slot={} max_slot={}，后台等待 MERGE 稳定",
        snapshot.slot(),
        snapshot.slot(),
        snapshot.slot()
    );

    match spawn_staging_merge_wait(output, shared.snapshot) {
        Ok(build) => Ok(Some(build)),
        Err(err) => {
            // The durable `full_merging` checkpoint is already in place; the
            // normal recovery branch below can safely start the wait worker on
            // the next loop without replaying the full archive.
            warn!(
                "[switch] staging full 已提交但无法立即启动 MERGE 等待线程；将按 full_merging 恢复：{}",
                err
            );
            Ok(None)
        }
    }
}

fn finish_cutover(
    output: &mut IncrementalOutput,
    state_path: &Path,
    watcher_state: &mut WatcherState,
    staging_hot_mints: &HotMintSet,
) -> Result<(), Box<dyn std::error::Error>> {
    if watcher_state.active.phase != LanePhase::CutoverPending
        || watcher_state.staging.phase != LanePhase::CutoverPending
    {
        return Ok(());
    }
    let staging_slot = watcher_state
        .staging
        .max_slot
        .ok_or("cutover pending without a committed staging max_slot")?;
    if watcher_state.cutover.is_none() {
        info!(
            "[switch] staging 已完成首增量 max_slot={}，active 没有进行中的导入；执行 staging 自检",
            staging_slot
        );
        output.validate_staging_group(staging_hot_mints.len())?;
        watcher_state.cutover = Some(CutoverWatcherState {
            active_tables: output.table_group_identity(TableGroup::Active)?,
            staging_tables: output.table_group_identity(TableGroup::Backup)?,
        });
        persist_watcher_state(state_path, watcher_state)?;
    }
    let checkpoint = watcher_state
        .cutover
        .as_ref()
        .ok_or("cutover checkpoint disappeared before exchange")?;
    info!("[switch] staging 自检通过，开始/恢复交换六对 active/_bak 表");
    output.exchange_groups(&checkpoint.active_tables, &checkpoint.staging_tables)?;
    output.set_hot_mints(std::sync::Arc::clone(staging_hot_mints));

    let mut old_active = std::mem::replace(&mut watcher_state.active, LaneWatcherState::disabled());
    let mut promoted = std::mem::replace(&mut watcher_state.staging, LaneWatcherState::disabled());
    promoted.phase = LanePhase::Ready;
    promoted.inflight_incremental = None;
    old_active.phase = LanePhase::Disabled;
    old_active.inflight_incremental = None;
    old_active.hot_mints_path = None;
    watcher_state.active = promoted;
    watcher_state.staging = old_active;
    watcher_state.cutover = None;
    watcher_state.shared_full_load = None;
    persist_watcher_state(state_path, watcher_state)?;
    cleanup_unused_frozen_hot_mints_or_warn(state_path, watcher_state);
    info!(
        "[switch] 表组切换完成：active full_slot={:?} max_slot={:?}；旧 active 保留在 disabled staging，冻结 mint 文件已清理",
        watcher_state.active.full_snapshot.as_ref().map(|snapshot| snapshot.slot),
        watcher_state.active.max_slot
    );
    Ok(())
}

fn newest_full_generation(
    fulls: &[FullSnapshot],
    active_full_slot: Option<u64>,
) -> Option<FullSnapshot> {
    fulls
        .iter()
        .filter(|snapshot| active_full_slot.map_or(true, |slot| snapshot.slot() > slot))
        .max_by(|left, right| {
            left.slot()
                .cmp(&right.slot())
                .then_with(|| right.path().cmp(left.path()))
        })
        .cloned()
}

fn run_incremental_snapshots(
    args: &Args,
    directory: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut output = IncrementalOutput::new(args)?;
    let state_path = watcher_state_path()?;
    let mut watcher_state = if args.bootstrap {
        let existing_max_slot = output.max_raw_account_updated_slot()?;
        let state = WatcherState::bootstrap_waiting_for_full();
        // `--bootstrap` is an explicit reset. Commit that fact before the
        // first destructive ClickHouse action, not after the full succeeds.
        persist_watcher_state(&state_path, &state)?;
        let removed_hot_mints = cleanup_unused_frozen_hot_mints(&state_path, &state)?;
        info!(
            "[watcher] bootstrap 已重置状态文件并清理 {} 个本地 hot-mint 文件：旧 active max_slot={} 将被忽略，等待新的 active full",
            removed_hot_mints,
            existing_max_slot,
        );
        state
    } else {
        match load_watcher_state(&state_path)? {
            Some(state) => state,
            None => {
                let max_updated_slot = output.max_raw_account_updated_slot()?;
                let resume_slot =
                    resume_slot_from_max_updated_slot(max_updated_slot, args.resume_slot_rewind);
                let state = WatcherState::recovered_active_only(resume_slot);
                persist_watcher_state(&state_path, &state)?;
                warn!(
                    "[watcher] 未找到状态文件；active max_slot 从 raw_account 回退初始化为 {}，historical full_slot 未知",
                    resume_slot
                );
                state
            }
        }
    };

    if clear_disabled_hot_mints_paths(&mut watcher_state) {
        persist_watcher_state(&state_path, &watcher_state)?;
    }
    cleanup_unused_frozen_hot_mints_or_warn(&state_path, &watcher_state);

    let mut bootstrap_pending = matches!(
        watcher_state.active.phase,
        LanePhase::WaitingForFull | LanePhase::FullLoading
    );
    let mut active_slot = watcher_state.active.max_slot.unwrap_or(0);
    if !bootstrap_pending {
        let active_hot_mints = match watcher_state.active.hot_mints_path.as_ref() {
            Some(path) => load_frozen_hot_mints(path)?,
            None => {
                let hot_mints = output.load_enabled_hot_mints()?;
                let path = persist_frozen_hot_mints(&state_path, &hot_mints)?;
                watcher_state.active.hot_mints_path = Some(path);
                persist_watcher_state(&state_path, &watcher_state)?;
                hot_mints
            }
        };
        output.set_hot_mints(active_hot_mints);
    }
    let mut staging_hot_mints = match watcher_state.staging.hot_mints_path.as_ref() {
        Some(path) if watcher_state.staging.phase != LanePhase::Disabled => {
            Some(load_frozen_hot_mints(path)?)
        }
        _ => None,
    };
    let mut staging_build: Option<StagingBuild> = None;
    let mut staging_incremental_handle: Option<JoinHandle<Result<u64, String>>> = None;
    let mut waiting_logged = false;
    let mut invalid_archives = HashSet::<PathBuf>::new();
    let poll_interval = Duration::from_secs(args.incremental_poll_interval_secs);
    info!(
        "[watcher] 状态恢复：file={} active phase={:?} full_slot={:?} max_slot={:?}; staging phase={:?} full_slot={:?} max_slot={:?}",
        state_path.display(),
        watcher_state.active.phase,
        watcher_state.active.full_snapshot.as_ref().map(|snapshot| snapshot.slot),
        watcher_state.active.max_slot,
        watcher_state.staging.phase,
        watcher_state.staging.full_snapshot.as_ref().map(|snapshot| snapshot.slot),
        watcher_state.staging.max_slot,
    );

    loop {
        // The staging worker pauses after its full INSERTs and derived-index
        // build. Persist `full_merging` before allowing it to start MERGE, so
        // a stop in the long stability window never replays the full archive.
        if watcher_state.staging.phase == LanePhase::FullLoading {
            if let Some(build) = staging_build.as_mut() {
                if let Some(full_imported) = build.full_imported.as_ref() {
                    match full_imported.try_recv() {
                        Ok(hot_mints) => {
                            let hot_mints_path = persist_frozen_hot_mints(&state_path, &hot_mints)?;
                            watcher_state.staging.phase = LanePhase::FullMerging;
                            watcher_state.staging.max_slot = Some(build.snapshot.slot);
                            watcher_state.staging.hot_mints_path = Some(hot_mints_path);
                            watcher_state.staging.inflight_incremental = None;
                            persist_watcher_state(&state_path, &watcher_state)?;
                            cleanup_unused_frozen_hot_mints_or_warn(&state_path, &watcher_state);
                            staging_hot_mints = Some(hot_mints);
                            build
                                .full_import_ack
                                .take()
                                .ok_or("staging full worker has no merge-start acknowledgement")?
                                .send(())
                                .map_err(|_| {
                                    "staging full worker exited before full_merging acknowledgement"
                                })?;
                            info!(
                                "[switch] staging 全量已提交：full_slot={:?} max_slot={:?}；后台等待 MERGE 稳定",
                                watcher_state
                                    .staging
                                    .full_snapshot
                                    .as_ref()
                                    .map(|full| full.slot),
                                watcher_state.staging.max_slot
                            );
                        }
                        Err(TryRecvError::Empty | TryRecvError::Disconnected) => {}
                    }
                }
            }
        }

        if staging_build
            .as_ref()
            .is_some_and(|build| build.handle.is_finished())
        {
            let build = staging_build.take().expect("staging build checked above");
            let snapshot = build.snapshot;
            match build.handle.join() {
                Ok(Ok(())) => {
                    if watcher_state.staging.phase != LanePhase::FullMerging {
                        return Err("staging full worker finished without a full_merging checkpoint".into());
                    }
                    watcher_state.staging.phase = LanePhase::Ready;
                    persist_watcher_state(&state_path, &watcher_state)?;
                    info!(
                        "[switch] staging MERGE 已稳定：full_slot={:?} max_slot={}；等待首个适用增量",
                        watcher_state
                            .staging
                            .full_snapshot
                            .as_ref()
                            .map(|full| full.slot),
                        snapshot.slot
                    );
                    console_snapshot_status(
                        "completed",
                        SnapshotKind::Full.as_str(),
                        &snapshot.path,
                        snapshot.slot,
                    );
                }
                Ok(Err(err)) if watcher_state.staging.phase == LanePhase::FullMerging => {
                    return Err(format!(
                        "staging full data is committed but MERGE convergence failed; restart will resume only MERGE waiting: {err}"
                    )
                    .into())
                }
                Ok(Err(err)) => {
                    warn!("[switch] staging full 失败，将保留 full_loading 状态并重试：{err}");
                    reset_failed_staging(
                        &mut output,
                        &snapshot,
                        poll_interval,
                        "后台全量导入或二层刷新失败",
                    );
                }
                Err(panic) if watcher_state.staging.phase == LanePhase::FullMerging => {
                    return Err(format!(
                        "staging full data is committed but MERGE worker panicked; restart will resume only MERGE waiting: {panic:?}"
                    )
                    .into())
                }
                Err(panic) => {
                    warn!("[switch] staging full worker 异常退出：{panic:?}");
                    reset_failed_staging(
                        &mut output,
                        &snapshot,
                        poll_interval,
                        "后台构建线程异常退出",
                    );
                }
            }
        }

        if staging_incremental_handle
            .as_ref()
            .is_some_and(|handle| handle.is_finished())
        {
            let handle = staging_incremental_handle
                .take()
                .expect("staging incremental handle checked above");
            match handle.join() {
                Ok(Ok(slot)) => {
                    watcher_state.staging.max_slot = Some(slot);
                    watcher_state.staging.phase = LanePhase::CutoverPending;
                    watcher_state.staging.inflight_incremental = None;
                    // Active imports are synchronous. At this loop boundary
                    // its previous import has committed, so no active IO is
                    // left once new work is stopped here.
                    watcher_state.active.phase = LanePhase::CutoverPending;
                    watcher_state.active.inflight_incremental = None;
                    persist_watcher_state(&state_path, &watcher_state)?;
                    info!(
                        "[switch] staging 首个增量已提交 max_slot={}；active 已静止，准备切换",
                        slot
                    );
                }
                Ok(Err(err)) => {
                    return Err(format!("staging incremental import failed: {err}").into())
                }
                Err(_) => return Err("staging incremental worker panicked".into()),
            }
        }

        if watcher_state.active.phase == LanePhase::CutoverPending
            && watcher_state.staging.phase == LanePhase::CutoverPending
        {
            let hot_mints = staging_hot_mints
                .as_ref()
                .ok_or("cutover pending without staging frozen hot-mint set")?;
            finish_cutover(&mut output, &state_path, &mut watcher_state, hot_mints)?;
            active_slot = watcher_state.active.max_slot.unwrap_or(0);
            staging_hot_mints = None;
            waiting_logged = false;
            continue;
        }

        let incrementals = discover_incremental_snapshots(directory)?;
        let fulls = discover_full_snapshots(directory)?;

        // This takes precedence over the ordinary per-lane recovery paths.
        // The checkpoint means `_bak` and active's bridge are one operation;
        // recovering them separately would reopen and decompress the same full
        // archive twice.
        if let Some(shared) = watcher_state.shared_full_load.clone() {
            if staging_build.is_some() || staging_incremental_handle.is_some() {
                return Err("shared full checkpoint conflicts with a staging worker".into());
            }
            // This preparation is intentionally repeated on every shared
            // attempt. The checkpoint is written before destructive work, so
            // a crash between those operations must still never append the
            // new full generation to retained `_bak` data.
            if let Err(err) = output.stop_group_merges(TableGroup::Backup) {
                warn!(
                    "[switch] 单次解压前暂停 _bak MERGE 失败；保留共享检查点并重试：{}",
                    err
                );
                reset_failed_staging(
                    &mut output,
                    &shared.snapshot,
                    poll_interval,
                    "单次解压前暂停 _bak MERGE 失败",
                );
                continue;
            }
            if let Err(err) = output.reset_group(TableGroup::Backup) {
                warn!(
                    "[switch] 单次解压前清理 _bak 失败；保留共享检查点并重试：{}",
                    err
                );
                reset_failed_staging(
                    &mut output,
                    &shared.snapshot,
                    poll_interval,
                    "单次解压前清理 _bak 失败",
                );
                continue;
            }
            let snapshot = find_recorded_full(&fulls, &shared.snapshot)?;
            let hot_mints = staging_hot_mints
                .as_ref()
                .map(std::sync::Arc::clone)
                .ok_or("shared full checkpoint has no frozen staging hot-mint set")?;
            match complete_shared_full_load(
                &mut output,
                &state_path,
                &mut watcher_state,
                snapshot,
                hot_mints,
            ) {
                Ok(build) => {
                    active_slot = watcher_state.active.max_slot.unwrap_or(0);
                    staging_build = build;
                    waiting_logged = false;
                }
                Err(err) => {
                    warn!(
                        "[switch] 单次解压双路 full 导入失败；保留共享检查点并重试：{}",
                        err
                    );
                    reset_failed_staging(
                        &mut output,
                        &shared.snapshot,
                        poll_interval,
                        "单次解压双路全量导入或派生索引刷新失败",
                    );
                }
            }
            continue;
        }

        if watcher_state.active.phase == LanePhase::FullMerging {
            let full_slot = watcher_state
                .active
                .full_snapshot
                .as_ref()
                .ok_or("active full_merging without full snapshot")?
                .slot;
            output.start_group_merges(TableGroup::Active)?;
            output.wait_for_group_merges_to_settle(TableGroup::Active)?;
            watcher_state.active.phase = LanePhase::Ready;
            persist_watcher_state(&state_path, &watcher_state)?;
            active_slot = watcher_state.active.max_slot.unwrap_or(0);
            bootstrap_pending = false;
            info!(
                "[watcher] 重启恢复：active full_slot={} 的 MERGE 已稳定，继续增量",
                full_slot
            );
            continue;
        }

        if watcher_state.active.phase == LanePhase::FullLoading {
            let recorded = watcher_state
                .active
                .full_snapshot
                .clone()
                .ok_or("active full_loading without full snapshot")?;
            let snapshot = find_recorded_full(&fulls, &recorded)?;
            active_slot =
                process_bootstrap_full(&mut output, &state_path, &mut watcher_state, snapshot)?;
            bootstrap_pending = false;
            waiting_logged = false;
            continue;
        }

        if watcher_state.active.phase == LanePhase::IncrementalLoading {
            let recorded = watcher_state
                .active
                .inflight_incremental
                .clone()
                .ok_or("active incremental_loading without inflight snapshot")?;
            let candidate = match recorded.kind {
                PersistedSnapshotKind::Full => eligible_full_candidates(fulls.clone(), active_slot)
                    .into_iter()
                    .next()
                    .map(WatchedSnapshot::Full),
                PersistedSnapshotKind::Incremental => {
                    newest_eligible_incremental(&incrementals, active_slot)
                        .map(WatchedSnapshot::Incremental)
                }
            };
            let Some(candidate) = candidate else {
                watcher_state.active.phase = LanePhase::Ready;
                watcher_state.active.inflight_incremental = None;
                persist_watcher_state(&state_path, &watcher_state)?;
                info!(
                    "[watcher] 重启恢复：中断增量对应 archive 已滚动，active 从已提交 max_slot={} 重新等待可衔接快照",
                    active_slot
                );
                continue;
            };
            if !apply_active_incremental(
                &mut output,
                &state_path,
                &mut watcher_state,
                &mut active_slot,
                &candidate,
            )? {
                watcher_state.active.phase = LanePhase::Ready;
                watcher_state.active.inflight_incremental = None;
                persist_watcher_state(&state_path, &watcher_state)?;
            }
            info!(
                "[watcher] 重启恢复：active 使用当前可衔接快照完成中断的增量 slot={}",
                candidate.slot()
            );
            continue;
        }

        if staging_build.is_none() && watcher_state.staging.phase == LanePhase::FullLoading {
            let recorded = watcher_state
                .staging
                .full_snapshot
                .clone()
                .ok_or("staging full_loading without full snapshot")?;
            let snapshot = find_recorded_full(&fulls, &recorded)?;
            staging_build = Some(spawn_staging_full(&output, snapshot)?);
            info!(
                "[switch] 重启恢复：继续 staging 固定 full_slot={}",
                recorded.slot
            );
        }

        if staging_build.is_none() && watcher_state.staging.phase == LanePhase::FullMerging {
            let recorded = watcher_state
                .staging
                .full_snapshot
                .clone()
                .ok_or("staging full_merging without full snapshot")?;
            staging_build = Some(spawn_staging_merge_wait(&output, recorded.clone())?);
            info!(
                "[switch] 重启恢复：staging full_slot={} 已提交，仅继续等待 MERGE 稳定",
                recorded.slot
            );
        }

        if staging_incremental_handle.is_none()
            && watcher_state.staging.phase == LanePhase::IncrementalLoading
        {
            let recorded = watcher_state
                .staging
                .inflight_incremental
                .clone()
                .ok_or("staging incremental_loading without inflight snapshot")?;
            let stage_slot = watcher_state
                .staging
                .max_slot
                .ok_or("staging incremental_loading without a committed max_slot")?;
            if recorded.kind != PersistedSnapshotKind::Incremental {
                return Err("staging incremental_loading has a non-incremental marker".into());
            }
            let Some(snapshot) = newest_eligible_incremental(&incrementals, stage_slot) else {
                watcher_state.staging.phase = LanePhase::Ready;
                watcher_state.staging.inflight_incremental = None;
                persist_watcher_state(&state_path, &watcher_state)?;
                info!(
                    "[switch] 重启恢复：中断 staging 增量对应 archive 已滚动，从已提交 max_slot={} 重新等待可衔接快照",
                    stage_slot
                );
                continue;
            };
            let hot_mints = staging_hot_mints
                .as_ref()
                .map(std::sync::Arc::clone)
                .ok_or("staging restart requires frozen hot-mint set")?;
            let (clickhouse_url, workers) = output.clickhouse_config();
            let snapshot_slot = snapshot.slot();
            staging_incremental_handle = Some(spawn_staging_incremental(
                snapshot,
                clickhouse_url,
                workers,
                stage_slot,
                hot_mints,
            )?);
            info!(
                "[switch] 重启恢复：staging 使用当前可衔接的首个增量 slot={}",
                snapshot_slot
            );
        }

        if bootstrap_pending || watcher_state.active.phase == LanePhase::WaitingForFull {
            if let Some(snapshot) = newest_full_generation(&fulls, None) {
                active_slot =
                    process_bootstrap_full(&mut output, &state_path, &mut watcher_state, snapshot)?;
                bootstrap_pending = false;
                waiting_logged = false;
                continue;
            }
        }

        // The generation test is against active.full_slot, not active.max_slot.
        // This intentionally accepts a new full even after active advanced far
        // beyond that full's slot through ordinary incrementals.
        if watcher_state.staging.phase == LanePhase::Disabled && staging_build.is_none() {
            if let Some(snapshot) = newest_full_generation(
                &fulls,
                watcher_state
                    .active
                    .full_snapshot
                    .as_ref()
                    .map(|full| full.slot),
            ) {
                // Both target groups consume this archive through one loader:
                // staging receives the full baseline while active receives
                // only AppendVecs newer than its captured watermark. The
                // shared checkpoint also prevents a restart from falling back
                // to two independent decompressions.
                let hot_mints = begin_shared_full_load(
                    &mut output,
                    &state_path,
                    &mut watcher_state,
                    &snapshot,
                )?;
                staging_hot_mints = Some(hot_mints);
                waiting_logged = false;
                continue;
            }
        }

        let active_candidate = if watcher_state.active.phase == LanePhase::Ready {
            eligible_candidates(incrementals.clone(), active_slot)
                .into_iter()
                .filter(|snapshot| !invalid_archives.contains(snapshot.path()))
                .next()
        } else {
            None
        };
        let staging_candidate = if staging_incremental_handle.is_none()
            && watcher_state.staging.phase == LanePhase::Ready
        {
            let stage_slot = watcher_state
                .staging
                .max_slot
                .ok_or("ready staging without max_slot")?;
            eligible_candidates(incrementals.clone(), stage_slot)
                .into_iter()
                .filter(|snapshot| !invalid_archives.contains(snapshot.path()))
                .next()
        } else {
            None
        };
        let selected = staging_candidate.clone().or(active_candidate.clone());
        if let Some(incremental) = selected {
            let candidate = WatchedSnapshot::Incremental(incremental.clone());
            candidate.log_verification();
            if staging_incremental_handle.is_none()
                && watcher_state.staging.phase == LanePhase::Ready
                && watcher_state.staging.max_slot.is_some_and(|stage_slot| {
                    incremental.slot() > stage_slot && incremental.base_slot() <= stage_slot
                })
            {
                let stage_slot = watcher_state.staging.max_slot.expect("checked above");
                let hot_mints = staging_hot_mints
                    .as_ref()
                    .map(std::sync::Arc::clone)
                    .ok_or("staging incremental selected without frozen hot-mint set")?;
                watcher_state.staging.phase = LanePhase::IncrementalLoading;
                watcher_state.staging.inflight_incremental = Some(inflight_snapshot(&candidate));
                persist_watcher_state(&state_path, &watcher_state)?;
                let (clickhouse_url, workers) = output.clickhouse_config();
                staging_incremental_handle = Some(spawn_staging_incremental(
                    incremental.clone(),
                    clickhouse_url,
                    workers,
                    stage_slot,
                    hot_mints,
                )?);
                info!(
                    "[switch] staging 开始首个增量 slot={}；完成即停止 active 后续任务",
                    incremental.slot()
                );
            }
            if watcher_state.active.phase == LanePhase::Ready
                && incremental.slot() > active_slot
                && incremental.base_slot() <= active_slot
            {
                if let Err(err) = apply_active_incremental(
                    &mut output,
                    &state_path,
                    &mut watcher_state,
                    &mut active_slot,
                    &candidate,
                ) {
                    invalid_archives.insert(candidate.path().to_path_buf());
                    return Err(err);
                }
            }
            waiting_logged = false;
            continue;
        }

        if !waiting_logged {
            info!(
                "[watcher] 暂无可处理快照：active phase={:?} full_slot={:?} max_slot={:?}; staging phase={:?} full_slot={:?} max_slot={:?}；{} 秒后重试",
                watcher_state.active.phase,
                watcher_state.active.full_snapshot.as_ref().map(|snapshot| snapshot.slot),
                watcher_state.active.max_slot,
                watcher_state.staging.phase,
                watcher_state.staging.full_snapshot.as_ref().map(|snapshot| snapshot.slot),
                watcher_state.staging.max_slot,
                args.incremental_poll_interval_secs,
            );
            waiting_logged = true;
        }
        thread::sleep(poll_interval);
        invalid_archives.retain(|path| path.exists());
    }
}

const DEFAULT_RESUME_SLOT_REWIND: u64 = 1_000;

fn resume_slot_from_max_updated_slot(max_updated_slot: u64, rewind: u64) -> u64 {
    max_updated_slot.saturating_sub(rewind)
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
        cleanup_unused_frozen_hot_mints, discover_full_snapshots, discover_incremental_snapshots,
        load_frozen_hot_mints, load_watcher_state, persist_frozen_hot_mints, persist_watcher_state,
        resume_slot_from_max_updated_slot, watched_snapshot_candidates, FullSnapshotWatcherState,
        InflightSnapshotWatcherState, LanePhase, LaneWatcherState, PersistedSnapshotKind,
        SharedFullLoadWatcherState, WatchedSnapshot, WatcherState, WATCHER_STATE_VERSION,
    };
    use std::collections::HashSet;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn rewinds_database_watermark_without_underflow() {
        assert_eq!(resume_slot_from_max_updated_slot(5_000, 1_000), 4_000);
        assert_eq!(resume_slot_from_max_updated_slot(1_000, 1_000), 0);
        assert_eq!(resume_slot_from_max_updated_slot(999, 1_000), 0);
        assert_eq!(resume_slot_from_max_updated_slot(5_000, 2_000), 3_000);
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

    #[test]
    fn watcher_state_round_trips_staging_and_cutover_data() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "solana-snapshot-etl-state-test-{}-{unique}",
            std::process::id()
        ));
        fs::create_dir(&directory).unwrap();
        let path = directory.join("state.json");
        let state = WatcherState {
            version: WATCHER_STATE_VERSION,
            active: LaneWatcherState {
                phase: LanePhase::FullMerging,
                full_snapshot: Some(FullSnapshotWatcherState {
                    path: PathBuf::from("/snapshots/snapshot-443000000.tar.zst"),
                    slot: 443_000_000,
                }),
                max_slot: Some(443_089_098),
                hot_mints_path: Some(PathBuf::from("/state/hot-mints-active.txt")),
                inflight_incremental: None,
            },
            staging: LaneWatcherState {
                phase: LanePhase::IncrementalLoading,
                full_snapshot: Some(FullSnapshotWatcherState {
                    path: PathBuf::from("/snapshots/snapshot-443052286.tar.zst"),
                    slot: 443_052_286,
                }),
                max_slot: Some(443_052_286),
                hot_mints_path: Some(PathBuf::from("/state/hot-mints-staging.txt")),
                inflight_incremental: Some(InflightSnapshotWatcherState {
                    kind: PersistedSnapshotKind::Incremental,
                }),
            },
            cutover: None,
            shared_full_load: None,
        };

        persist_watcher_state(&path, &state).unwrap();
        assert_eq!(load_watcher_state(&path).unwrap(), Some(state));
        let persisted: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(
            persisted["staging"]["inflight_incremental"]
                .as_object()
                .unwrap()
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            ["kind"]
        );

        let hot_mints = Arc::new(HashSet::from(["mint-a".to_owned(), "mint-b".to_owned()]));
        let hot_mints_path = persist_frozen_hot_mints(&path, &hot_mints).unwrap();
        assert_eq!(load_frozen_hot_mints(&hot_mints_path).unwrap(), hot_mints);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn shared_full_checkpoint_round_trips_with_its_captured_active_watermark() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "solana-snapshot-etl-shared-full-state-test-{}-{unique}",
            std::process::id()
        ));
        fs::create_dir(&directory).unwrap();
        let path = directory.join("state.json");
        let shared_snapshot = FullSnapshotWatcherState {
            path: PathBuf::from("/snapshots/snapshot-443200000.tar.zst"),
            slot: 443_200_000,
        };
        let state = WatcherState {
            version: WATCHER_STATE_VERSION,
            active: LaneWatcherState {
                phase: LanePhase::IncrementalLoading,
                full_snapshot: Some(FullSnapshotWatcherState {
                    path: PathBuf::from("/snapshots/snapshot-443000000.tar.zst"),
                    slot: 443_000_000,
                }),
                max_slot: Some(443_150_000),
                hot_mints_path: Some(PathBuf::from("/state/hot-mints-active.txt")),
                inflight_incremental: Some(InflightSnapshotWatcherState {
                    kind: PersistedSnapshotKind::Full,
                }),
            },
            staging: LaneWatcherState {
                phase: LanePhase::FullLoading,
                full_snapshot: Some(shared_snapshot.clone()),
                max_slot: None,
                hot_mints_path: Some(PathBuf::from("/state/hot-mints-staging.txt")),
                inflight_incremental: None,
            },
            cutover: None,
            shared_full_load: Some(SharedFullLoadWatcherState {
                snapshot: shared_snapshot,
                active_resume_slot: 443_150_000,
            }),
        };

        persist_watcher_state(&path, &state).unwrap();
        assert_eq!(load_watcher_state(&path).unwrap(), Some(state));
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn v3_state_migrates_to_v5_without_a_shared_full_checkpoint() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "solana-snapshot-etl-v3-state-test-{}-{unique}",
            std::process::id()
        ));
        fs::create_dir(&directory).unwrap();
        let path = directory.join("state.json");
        fs::write(
            &path,
            r#"{
  "version": 3,
  "active": {
    "phase": "ready",
    "full_snapshot": {
      "path": "/snapshots/snapshot-443000000.tar.zst",
      "slot": 443000000
    },
    "max_slot": 443150000,
    "hot_mints_path": "/state/hot-mints-active.txt",
    "inflight_incremental": null
  },
  "staging": {
    "phase": "disabled",
    "full_snapshot": null,
    "max_slot": null,
    "hot_mints_path": null,
    "inflight_incremental": null
  },
  "cutover": null
}"#,
        )
        .unwrap();

        let state = load_watcher_state(&path).unwrap().unwrap();
        assert_eq!(state.version, WATCHER_STATE_VERSION);
        assert_eq!(state.shared_full_load, None);
        assert_eq!(state.active.max_slot, Some(443_150_000));
        assert!(fs::read_to_string(&path)
            .unwrap()
            .contains("\"version\": 5"));
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn v2_state_migrates_without_inventing_an_active_full_slot() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "solana-snapshot-etl-v2-state-test-{}-{unique}",
            std::process::id()
        ));
        fs::create_dir(&directory).unwrap();
        let path = directory.join("state.json");
        fs::write(
            &path,
            r#"{
  "version": 2,
  "active_slot": 443089098,
  "active_hot_mints_path": "/state/hot-mints-active.txt",
  "staging": {
    "full_snapshot_path": "/snapshots/snapshot-443052286.tar.zst",
    "full_snapshot_slot": 443052286,
    "ready_slot": 443052286,
    "hot_mints_path": "/state/hot-mints-staging.txt",
    "first_incremental": null,
    "first_incremental_completed": false
  },
  "retired_hot_mints_path": null,
  "rollback_deadline_unix_secs": null
}"#,
        )
        .unwrap();

        let state = load_watcher_state(&path).unwrap().unwrap();
        assert_eq!(state.version, WATCHER_STATE_VERSION);
        assert_eq!(state.active.max_slot, Some(443_089_098));
        assert_eq!(state.active.full_snapshot, None);
        assert_eq!(state.staging.phase, LanePhase::Ready);
        assert_eq!(
            state
                .staging
                .full_snapshot
                .as_ref()
                .map(|snapshot| snapshot.slot),
            Some(443_052_286)
        );
        assert!(fs::read_to_string(&path)
            .unwrap()
            .contains("\"version\": 5"));
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn v4_migration_removes_recorded_incremental_archive_details() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "solana-snapshot-etl-v4-state-test-{}-{unique}",
            std::process::id()
        ));
        fs::create_dir(&directory).unwrap();
        let path = directory.join("state.json");
        fs::write(
            &path,
            r#"{
  "version": 4,
  "active": {
    "phase": "incremental_loading",
    "full_snapshot": null,
    "max_slot": 443150000,
    "hot_mints_path": null,
    "inflight_incremental": {
      "kind": "incremental",
      "path": "/snapshots/incremental-snapshot-443150000-443160000.tar.zst",
      "base_slot": 443150000,
      "slot": 443160000
    }
  },
  "staging": {
    "phase": "disabled",
    "full_snapshot": null,
    "max_slot": null,
    "hot_mints_path": null,
    "inflight_incremental": null
  },
  "cutover": null,
  "shared_full_load": null
}"#,
        )
        .unwrap();

        let state = load_watcher_state(&path).unwrap().unwrap();
        assert_eq!(state.version, WATCHER_STATE_VERSION);
        assert_eq!(
            state.active.inflight_incremental,
            Some(InflightSnapshotWatcherState {
                kind: PersistedSnapshotKind::Incremental,
            })
        );
        let persisted: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(
            persisted["active"]["inflight_incremental"]
                .as_object()
                .unwrap()
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            ["kind"]
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn bootstrap_state_cleanup_removes_all_generated_hot_mint_files() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "solana-snapshot-etl-hot-mints-cleanup-test-{}-{unique}",
            std::process::id()
        ));
        fs::create_dir(&directory).unwrap();
        let state_path = directory.join("state.json");
        let hot_mints = Arc::new(HashSet::from(["mint-a".to_owned()]));
        let old_active = persist_frozen_hot_mints(&state_path, &hot_mints).unwrap();
        let old_staging = persist_frozen_hot_mints(&state_path, &hot_mints).unwrap();
        let unrelated = directory.join("keep-me.txt");
        fs::write(&unrelated, []).unwrap();

        let bootstrap = WatcherState::bootstrap_waiting_for_full();
        persist_watcher_state(&state_path, &bootstrap).unwrap();
        assert_eq!(
            cleanup_unused_frozen_hot_mints(&state_path, &bootstrap).unwrap(),
            2
        );
        assert!(!old_active.exists());
        assert!(!old_staging.exists());
        assert!(unrelated.exists());
        fs::remove_dir_all(directory).unwrap();
    }
}
