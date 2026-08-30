use crate::clickhouse::{
    active_group_id, exchange_table_groups, hot_token_version, load_group_hot_mints,
    max_raw_account_updated_slot, rebuild_derived_indexes_from_state, record_index_control,
    reset_table_group, set_group_table_merges, snapshot_group_hot_mints,
    validate_clickhouse_schema, validate_staging_group, wait_for_group_merges_to_settle,
    ClickhouseIndexer, CloseTombstoneStats, HotMintSet, SnapshotKind, TableGroup,
};
use clap::{ArgGroup, Parser};
use env_logger::{Builder, Env, Target};
use indicatif::{ProgressBar, ProgressBarIter, ProgressStyle};
use log::{debug, error, info, warn, LevelFilter};
use solana_snapshot_etl::archived::ArchiveSnapshotExtractor;
use solana_snapshot_etl::incremental::{
    discover as discover_incremental_snapshots, discover_full as discover_full_snapshots,
    eligible_candidates, eligible_full_candidates, FullSnapshot, IncrementalSnapshot,
};
use solana_snapshot_etl::unpacked::UnpackedSnapshotExtractor;
use solana_snapshot_etl::{AppendVecIterator, ReadProgressTracking, SnapshotExtractor};
use std::collections::{HashSet, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{IoSliceMut, Read, Write};
use std::path::{Path, PathBuf};
use std::thread::{self, JoinHandle};
use std::time::Duration;

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
        let hot_mints =
            runtime.block_on(load_group_hot_mints(&clickhouse_url, TableGroup::Active))?;
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
            runtime.block_on(snapshot_group_hot_mints(
                &clickhouse_url,
                TableGroup::Active,
            ))?
        } else {
            runtime.block_on(load_group_hot_mints(&clickhouse_url, TableGroup::Active))?
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
    snapshot: FullSnapshot,
    handle: JoinHandle<Result<HotMintSet, String>>,
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
    /// incrementals reuse this process-local group snapshot. After a watcher
    /// restart the snapshot is restored once from the group's swapped filter
    /// table, never from `hot_token_enabled`.
    fn hot_mints_for(
        &mut self,
        group: TableGroup,
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
                    runtime.block_on(snapshot_group_hot_mints(clickhouse_url, group))?
                } else if let Some(mints) = hot_mints.as_ref() {
                    std::sync::Arc::clone(mints)
                } else {
                    runtime.block_on(load_group_hot_mints(clickhouse_url, group))?
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

    fn exchange_groups(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        match self {
            Self::Clickhouse {
                clickhouse_url,
                runtime,
                ..
            } => runtime.block_on(exchange_table_groups(clickhouse_url)),
        }
    }

    fn validate_staging_group(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        match self {
            Self::Clickhouse {
                clickhouse_url,
                runtime,
                ..
            } => runtime.block_on(validate_staging_group(clickhouse_url)),
        }
    }

    fn hot_token_version(&mut self) -> Result<u64, Box<dyn std::error::Error>> {
        match self {
            Self::Clickhouse {
                clickhouse_url,
                runtime,
                ..
            } => runtime.block_on(hot_token_version(clickhouse_url)),
        }
    }

    fn record_control(
        &mut self,
        active_group: u8,
        ready_slot: u64,
        hot_token_version: u64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match self {
            Self::Clickhouse {
                clickhouse_url,
                runtime,
                ..
            } => runtime.block_on(record_index_control(
                clickhouse_url,
                active_group,
                ready_slot,
                hot_token_version,
            )),
        }
    }

    fn active_group_id(&mut self) -> Result<u8, Box<dyn std::error::Error>> {
        match self {
            Self::Clickhouse {
                clickhouse_url,
                runtime,
                ..
            } => runtime.block_on(active_group_id(clickhouse_url)),
        }
    }

    fn process(
        &mut self,
        loader: &mut SupportedLoader,
        snapshot_kind: SnapshotKind,
        group: TableGroup,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let hot_mints = self.hot_mints_for(group, snapshot_kind)?;
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
}

/// A failed staging generation must never take the already-serving active
/// generation down.  Keep the backup raw Merge paused, clear all seven backup
/// tables, and let the watcher retry the full snapshot on its next iteration.
/// Cleanup is best-effort here; a later retry will attempt the TRUNCATE again.
fn reset_failed_staging(
    output: &mut IncrementalOutput,
    candidate: &WatchedSnapshot,
    poll_interval: Duration,
    reason: &str,
) {
    warn!(
        "[switch] staging 全量失败，active 继续服役；清理 _bak 后重试 file={} slot={} reason={}",
        candidate.path().display(),
        candidate.slot(),
        reason
    );
    if let Err(err) = output.stop_group_merges(TableGroup::Backup) {
        warn!(
            "[switch] staging 失败后再次暂停 _bak raw+hot MERGE 失败：{}",
            err
        );
    }
    match output.reset_group(TableGroup::Backup) {
        Ok(()) => info!("[switch] staging 失败后的 _bak 七张表已清理，等待后重试该全量"),
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
) -> Result<HotMintSet, String> {
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
    output
        .start_group_merges(group)
        .map_err(|err| format!("staging full snapshot succeeded but START MERGES failed: {err}"))?;
    output
        .wait_for_group_merges_to_settle(group)
        .map_err(|err| format!("staging raw+hot MERGE settle check failed: {err}"))?;
    output
        .frozen_hot_mints()
        .map_err(|err| format!("staging full completed without frozen hot-token set: {err}"))
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

/// Atomically promote a staging generation once both paths have reached the
/// same slot.  Returns `true` when a switch was performed.
fn exchange_if_ready(
    output: &mut IncrementalOutput,
    active_group_id: &mut u8,
    active_slot: &mut u64,
    staging_slot: &mut Option<u64>,
    staging_hot_mints: &mut Option<HotMintSet>,
    retired_until: &mut Option<std::time::Instant>,
) -> Result<bool, Box<dyn std::error::Error>> {
    let Some(stage_slot) = *staging_slot else {
        return Ok(false);
    };
    if stage_slot != *active_slot {
        return Ok(false);
    }

    debug!(
        "Active and staging reached slot {}; exchanging table groups",
        stage_slot
    );
    info!(
        "[switch] active 与 _bak 已追平到 slot={}，执行 staging 自检",
        stage_slot
    );
    output.validate_staging_group()?;
    info!("[switch] staging 自检通过，开始交换七对 active/_bak 表");
    output.exchange_groups()?;
    let staged_mints = staging_hot_mints
        .take()
        .ok_or("staging group reached a slot without a frozen hot-token set")?;
    output.set_hot_mints(staged_mints);
    *active_slot = stage_slot;
    *staging_slot = None;
    *active_group_id = if *active_group_id == 1 { 2 } else { 1 };
    let control_hot_version = output.hot_token_version().unwrap_or_default();
    if let Err(err) = output.record_control(*active_group_id, *active_slot, control_hot_version) {
        warn!("Table exchange succeeded but control audit write failed: {err}");
    }
    *retired_until = Some(std::time::Instant::now() + Duration::from_secs(300));
    info!(
        "[switch] 表组切换完成：active_group={} ready_slot={}；旧组进入 5 分钟回滚窗口",
        *active_group_id, *active_slot
    );
    debug!("Table-group switch complete; _bak rollback window is 5 minutes");
    Ok(true)
}

fn run_incremental_snapshots(
    args: &Args,
    directory: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut output = IncrementalOutput::new(args)?;
    let mut bootstrap_pending = args.bootstrap;
    let mut active_slot = if bootstrap_pending {
        let existing_max_slot = output.max_raw_account_updated_slot()?;
        info!(
            "[watcher] bootstrap 启动：现有 active raw_account 最大 slot={} 将被忽略，active 组从 slot 0 等待并导入全量快照",
            existing_max_slot
        );
        0
    } else {
        let max_updated_slot = output.max_raw_account_updated_slot()?;
        let resume_slot =
            resume_slot_from_max_updated_slot(max_updated_slot, args.resume_slot_rewind);
        debug!(
            "Read raw_account maximum updated_slot={max_updated_slot}; rewound {} slots to resume slot {resume_slot}",
            args.resume_slot_rewind
        );
        info!(
            "[watcher] 非 bootstrap 启动：active raw_account 最大 slot={}，回退 {} 个 slot，起始 resume_slot={}",
            max_updated_slot,
            args.resume_slot_rewind,
            resume_slot
        );
        resume_slot
    };
    let mut staging_slot: Option<u64> = None;
    let mut staging_hot_mints: Option<HotMintSet> = None;
    let mut staging_build: Option<StagingBuild> = None;
    let mut staging_incremental_handle: Option<JoinHandle<Result<u64, String>>> = None;
    let mut staging_incremental_queue: VecDeque<IncrementalSnapshot> = VecDeque::new();
    // A failed staging full must be retried even if active has already
    // advanced beyond that full while serving incrementals. Keep the retry
    // candidate separately instead of rewinding the independent active
    // watermark.
    let mut retry_full: Option<FullSnapshot> = None;
    let mut retired_until: Option<std::time::Instant> = None;
    let mut active_group_id: u8 = if bootstrap_pending {
        1
    } else {
        output.active_group_id().unwrap_or(1)
    };
    let mut has_processed_snapshot = false;
    let mut waiting_logged = false;
    let mut invalid_archives = HashSet::<PathBuf>::new();
    let poll_interval = Duration::from_secs(args.incremental_poll_interval_secs);
    info!(
        "[watcher] snapshot directory={} active_slot={} bootstrap={} poll_interval={}s",
        directory.display(),
        active_slot,
        bootstrap_pending,
        args.incremental_poll_interval_secs
    );

    debug!(
        "Watching snapshot directory {} from resume slot {}{}",
        directory.display(),
        active_slot,
        if bootstrap_pending {
            " (bootstrap: requiring a full snapshot)"
        } else {
            ""
        }
    );

    loop {
        // A staging full is built in parallel with the active path.  Harvest
        // its result without blocking; while it is still running, the normal
        // candidate selection below continues to consume active incrementals.
        if staging_build
            .as_ref()
            .is_some_and(|build| build.handle.is_finished())
        {
            let build = staging_build.take().expect("staging build checked above");
            let snapshot = build.snapshot;
            match build.handle.join() {
                Ok(Ok(hot_mints)) => {
                    retry_full = None;
                    staging_slot = Some(snapshot.slot());
                    staging_hot_mints = Some(hot_mints);
                    has_processed_snapshot = true;
                    debug!("Staging full snapshot ready at slot {}", snapshot.slot());
                    info!(
                        "[switch] staging 全量构建完成，staging_slot={}；active 已并行追赶，后续增量将同时更新 active 与 _bak",
                        snapshot.slot()
                    );
                    console_snapshot_status(
                        "completed",
                        SnapshotKind::Full.as_str(),
                        snapshot.path(),
                        snapshot.slot(),
                    );
                }
                Ok(Err(err)) => {
                    retry_full = Some(snapshot.clone());
                    let candidate = WatchedSnapshot::Full(snapshot);
                    warn!("[switch] staging 后台构建失败：{}", err);
                    info!(
                        "[switch] staging 失败：保留 active_slot={} 不变，登记 full slot={} 稍后重试",
                        active_slot,
                        candidate.slot()
                    );
                    reset_failed_staging(
                        &mut output,
                        &candidate,
                        poll_interval,
                        "后台全量导入或二层刷新失败",
                    );
                }
                Err(panic) => {
                    retry_full = Some(snapshot.clone());
                    let candidate = WatchedSnapshot::Full(snapshot);
                    warn!("[switch] staging 后台构建线程异常退出：{:?}", panic);
                    info!(
                        "[switch] staging 线程异常：保留 active_slot={} 不变，登记 full slot={} 稍后重试",
                        active_slot,
                        candidate.slot()
                    );
                    reset_failed_staging(
                        &mut output,
                        &candidate,
                        poll_interval,
                        "后台构建线程异常退出",
                    );
                }
            }
        }

        // A backup incremental is deliberately not joined by the watcher
        // after dispatch.  Harvest its result here when it finishes, while
        // active may have consumed several newer incrementals in the
        // meantime.  Only one backup job is in flight at a time so its slot
        // order remains deterministic.
        if staging_incremental_handle
            .as_ref()
            .is_some_and(|handle| handle.is_finished())
        {
            let handle = staging_incremental_handle
                .take()
                .expect("staging incremental handle checked above");
            match handle.join() {
                Ok(Ok(slot)) => {
                    staging_slot = Some(slot);
                    info!(
                        "[switch] _bak 增量完成：staging_slot={}；active 可继续独立消费后续快照",
                        slot
                    );
                }
                Ok(Err(err)) => {
                    return Err(format!("staging incremental import failed: {err}").into());
                }
                Err(_) => {
                    return Err("staging incremental worker panicked".into());
                }
            }
        }

        // Start the next queued backup archive only after the previous one
        // has committed. This preserves staging slot order without making
        // the active watcher wait for the backup queue to drain.
        if staging_incremental_handle.is_none() {
            if let (Some(stage_slot), Some(snapshot)) =
                (staging_slot, staging_incremental_queue.front().cloned())
            {
                if snapshot.slot() <= stage_slot {
                    staging_incremental_queue.pop_front();
                } else if snapshot.base_slot() > stage_slot {
                    return Err(format!(
                        "staging incremental gap: snapshot slot={} base_slot={} but staging_slot={}",
                        snapshot.slot(),
                        snapshot.base_slot(),
                        stage_slot
                    )
                    .into());
                } else {
                    let snapshot = staging_incremental_queue
                        .pop_front()
                        .expect("staging queue front checked above");
                    let (clickhouse_url, workers) = output.clickhouse_config();
                    let hot_mints = staging_hot_mints
                        .as_ref()
                        .map(std::sync::Arc::clone)
                        .ok_or("staging incremental queued without a frozen hot-token set")?;
                    staging_incremental_handle = Some(spawn_staging_incremental(
                        snapshot.clone(),
                        clickhouse_url,
                        workers,
                        stage_slot,
                        hot_mints,
                    )?);
                }
            }
        }

        // If active consumed the same slot while staging was building, the
        // newly completed backup can be promoted immediately rather than
        // waiting for another incremental archive to arrive.
        if staging_incremental_handle.is_none() && staging_incremental_queue.is_empty() {
            exchange_if_ready(
                &mut output,
                &mut active_group_id,
                &mut active_slot,
                &mut staging_slot,
                &mut staging_hot_mints,
                &mut retired_until,
            )?;
        }

        if let Some(deadline) = retired_until {
            if std::time::Instant::now() >= deadline {
                info!(
                    "[switch] 5 分钟回滚安全窗口已结束，开始清空旧 _bak 表（TRUNCATE ... max_table_size_to_drop=0）"
                );
                output.reset_group(TableGroup::Backup)?;
                info!("[switch] 旧 _bak 表已清空，可用于下一轮 staging 全量构建");
                retired_until = None;
            }
        }
        let incrementals = discover_incremental_snapshots(directory)?;
        let fulls = discover_full_snapshots(directory)?;
        let has_archives = !incrementals.is_empty() || !fulls.is_empty();
        let next_incremental_base = incrementals
            .iter()
            .filter(|snapshot| snapshot.slot() > active_slot)
            .map(|snapshot| snapshot.base_slot())
            .min();
        // Before a new full snapshot is seen, only the active path consumes
        // incrementals. Once the new full is selected, its high-slot tail is
        // first applied to active as an incremental (see the full branch
        // below), so subsequent incrementals can be consumed independently
        // by the two paths using their own watermarks.
        let selected_incremental = if bootstrap_pending {
            None
        } else if staging_slot.is_some() {
            let stage_watermark = staging_slot.expect("staging slot checked above");
            // Advance the lagging path first.  Choosing the furthest archive
            // that is only eligible for staging could skip the active path's
            // bridge archive (for example [1200,1300] followed by
            // [2300,2400]).  Prefer an archive whose base is applicable to
            // active; otherwise let staging make progress independently.
            eligible_candidates(incrementals.clone(), active_slot)
                .into_iter()
                .filter(|snapshot| !invalid_archives.contains(snapshot.path()))
                .next()
                .or_else(|| {
                    if staging_incremental_handle.is_none() && staging_incremental_queue.is_empty()
                    {
                        eligible_candidates(incrementals, stage_watermark)
                            .into_iter()
                            .filter(|snapshot| !invalid_archives.contains(snapshot.path()))
                            .next()
                    } else {
                        None
                    }
                })
                .map(WatchedSnapshot::Incremental)
        } else {
            eligible_candidates(incrementals, active_slot)
                .into_iter()
                .filter(|snapshot| !invalid_archives.contains(snapshot.path()))
                .next()
                .map(WatchedSnapshot::Incremental)
        };
        let selected = selected_incremental.or_else(|| {
            if (bootstrap_pending || (staging_slot.is_none() && staging_build.is_none()))
                && retired_until.is_none()
            {
                retry_full
                    .clone()
                    .filter(|snapshot| !invalid_archives.contains(snapshot.path()))
                    .map(WatchedSnapshot::Full)
                    .or_else(|| {
                        eligible_full_candidates(fulls, active_slot)
                            .into_iter()
                            .filter(|snapshot| !invalid_archives.contains(snapshot.path()))
                            .next()
                            .map(WatchedSnapshot::Full)
                    })
            } else {
                None
            }
        });

        let Some(candidate) = selected else {
            if staging_build.is_some() {
                if !waiting_logged {
                    info!(
                        "[watcher] _bak 全量仍在后台制备，active_slot={}；继续等待可处理的 active 增量或 staging 完成",
                        active_slot
                    );
                    waiting_logged = true;
                }
                thread::sleep(poll_interval);
                continue;
            }
            if !bootstrap_pending && staging_slot.is_none() && retired_until.is_none() {
                if let Some(base_slot) = next_incremental_base {
                    if base_slot > active_slot {
                        return Err(std::io::Error::other(format!(
                            "no suitable snapshot for active slot {active_slot}: next incremental starts at base slot {base_slot} and no bridging full snapshot is available"
                        ))
                        .into());
                    }
                }
            }
            if !has_processed_snapshot {
                let reason = if bootstrap_pending {
                    "bootstrap requires a usable full snapshot"
                } else {
                    "no suitable snapshot can advance the resume slot"
                };
                let inventory = if has_archives {
                    "recognized snapshot files were found, but none is usable"
                } else {
                    "the snapshot directory contains no recognized archives"
                };
                return Err(std::io::Error::other(format!(
                    "{reason}; {inventory}; stopping watcher"
                ))
                .into());
            }
            if !waiting_logged {
                info!(
                    "[watcher] 当前没有可处理的新快照，active_slot={} staging_slot={:?}，等待 {} 秒后重试",
                    active_slot,
                    staging_slot,
                    args.incremental_poll_interval_secs
                );
                waiting_logged = true;
            }
            thread::sleep(poll_interval);
            continue;
        };

        if waiting_logged {
            info!("[watcher] 发现新的可处理快照，继续处理");
            waiting_logged = false;
        }

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
        candidate.log_verification();
        info!(
            "[watcher] 选择 {} 快照 file={} slot={} active_slot={} staging_slot={:?} staging_build={}",
            snapshot_kind.as_str(),
            candidate.path().display(),
            candidate.slot(),
            active_slot,
            staging_slot,
            staging_build.is_some()
        );
        if matches!(candidate, WatchedSnapshot::Full(_)) {
            // A full snapshot starts a completely fresh staging generation.
            let bootstrap_full = bootstrap_pending;
            let group = if bootstrap_full {
                TableGroup::Active
            } else {
                TableGroup::Backup
            };
            info!(
                "[switch] {} 全量到达，准备 {} 组：清空目标组并从 slot 0 冷启动",
                if bootstrap_full {
                    "bootstrap"
                } else {
                    "新一轮"
                },
                group.as_str()
            );
            info!(
                "[clickhouse] {} 全量冷启动：暂停 {} 组 raw+hot 表后台 MERGE，以优先保障 INSERT",
                if bootstrap_full {
                    "bootstrap"
                } else {
                    "staging"
                },
                group.as_str()
            );
            if !bootstrap_full {
                // This generation will freeze its own hot-mint set in the
                // background full build.  Never let a failed/retried build
                // reuse a previous staging set.
                staging_hot_mints = None;
                let staging_snapshot = match &candidate {
                    WatchedSnapshot::Full(snapshot) => snapshot.clone(),
                    WatchedSnapshot::Incremental(_) => unreachable!(),
                };
                let (clickhouse_url, workers) = output.clickhouse_config();
                let snapshot_slot = staging_snapshot.slot();
                let retry_snapshot = staging_snapshot.clone();
                let handle = match thread::Builder::new()
                    .name("staging-full".to_owned())
                    .spawn(move || build_staging_full(staging_snapshot, clickhouse_url, workers))
                {
                    Ok(handle) => handle,
                    Err(err) => {
                        retry_full = Some(retry_snapshot);
                        warn!("[switch] 无法启动 staging 后台构建线程：{}", err);
                        reset_failed_staging(
                            &mut output,
                            &candidate,
                            poll_interval,
                            "无法启动后台构建线程",
                        );
                        continue;
                    }
                };
                staging_build = Some(StagingBuild {
                    snapshot: match candidate {
                        WatchedSnapshot::Full(snapshot) => snapshot,
                        WatchedSnapshot::Incremental(_) => unreachable!(),
                    },
                    handle,
                });
                info!(
                    "[switch] 新一轮全量 slot={} 已转入 _bak 后台制备；active/_bak 使用独立磁盘并行运行，active 先处理该 full 的高 slot 尾段",
                    snapshot_slot
                );

                // The new full is also the only authoritative bridge from
                // the old active watermark to the new incremental chain. Read
                // it with the active watermark as a lower bound and treat
                // the filtered tail as an incremental update. This keeps
                // active moving while staging builds, and makes subsequent
                // incrementals (whose base_slot is the new full slot)
                // eligible without inventing a slot gap.
                let active_candidate = match &staging_build {
                    Some(build) => WatchedSnapshot::Full(build.snapshot.clone()),
                    None => unreachable!("staging build was just installed"),
                };
                let active_resume_slot = active_slot;
                let mut active_loader = active_candidate.new_loader(active_resume_slot)?;
                let active_append_vecs = active_loader.append_vec_count_hint().unwrap_or(0);
                if snapshot_slot <= active_resume_slot {
                    info!(
                        "[switch] active 已达到或超过 full_slot={}，不回退 active_slot={}；仅重试 _bak staging",
                        snapshot_slot,
                        active_resume_slot
                    );
                } else if active_append_vecs == 0 {
                    info!(
                        "[switch] active full 尾段无需写入：full_slot={} active_slot={}，继续等待后续增量",
                        snapshot_slot,
                        active_resume_slot
                    );
                } else {
                    info!(
                        "[switch] active 将新 full 作为增量尾段处理：full_slot={} resume_slot={} append_vecs={}",
                        snapshot_slot,
                        active_resume_slot,
                        active_append_vecs
                    );
                    let process_result = output.process(
                        &mut active_loader,
                        SnapshotKind::Incremental,
                        TableGroup::Active,
                    );
                    process_result?;
                    active_slot = snapshot_slot;
                }
                has_processed_snapshot = true;
                info!(
                    "[switch] active full 尾段处理完成：active_slot={}；_bak 继续后台制备",
                    active_slot
                );
                // `candidate` has been moved into StagingBuild above. The
                // staging result is harvested at the top of the next loop.
                continue;
            }
            if let Err(err) = output.stop_group_merges(group) {
                warn!(
                    "[clickhouse] failed to pause {} raw+hot MERGE: {}",
                    group.as_str(),
                    err
                );
                if bootstrap_full {
                    return Err(err);
                }
                reset_failed_staging(&mut output, &candidate, poll_interval, "STOP MERGES 失败");
                continue;
            }
            if let Err(err) = output.reset_group(group) {
                warn!(
                    "[clickhouse] reset {} group failed: {}",
                    group.as_str(),
                    err
                );
                if bootstrap_full {
                    return Err(err);
                }
                reset_failed_staging(
                    &mut output,
                    &candidate,
                    poll_interval,
                    "清理 staging 表失败",
                );
                continue;
            }
            let mut loader = match candidate.new_loader(0) {
                Ok(loader) => loader,
                Err(err) => {
                    warn!(
                        "[clickhouse] {} full snapshot loader failed: {}",
                        group.as_str(),
                        err
                    );
                    if bootstrap_full {
                        console_snapshot_status(
                            "failed",
                            snapshot_kind.as_str(),
                            candidate.path(),
                            candidate.slot(),
                        );
                        return Err(err);
                    }
                    reset_failed_staging(
                        &mut output,
                        &candidate,
                        poll_interval,
                        "解析 full 快照失败",
                    );
                    continue;
                }
            };
            let process_result = output.process(&mut loader, snapshot_kind, group);
            if let Err(err) = process_result {
                warn!(
                    "[clickhouse] {} full snapshot import/derived refresh failed: {}",
                    group.as_str(),
                    err
                );
                if bootstrap_full {
                    return Err(err);
                }
                reset_failed_staging(
                    &mut output,
                    &candidate,
                    poll_interval,
                    "ClickHouse 导入或二层刷新失败",
                );
                continue;
            }
            if let Err(err) = output.start_group_merges(group) {
                warn!(
                    "[clickhouse] {} full snapshot succeeded but START MERGES failed: {}",
                    group.as_str(),
                    err
                );
                if bootstrap_full {
                    return Err(err);
                }
                reset_failed_staging(&mut output, &candidate, poll_interval, "START MERGES 失败");
                continue;
            }
            output.wait_for_group_merges_to_settle(group)?;
            info!(
                "[clickhouse] {} 组 raw 全量冷启动完成，后台 MERGE 已收敛到增量写入门槛；开始后续增量流程",
                group.as_str()
            );
            let control_hot_version = output.hot_token_version()?;
            if bootstrap_full {
                active_slot = candidate.slot();
                bootstrap_pending = false;
                if let Err(err) =
                    output.record_control(active_group_id, active_slot, control_hot_version)
                {
                    warn!("Failed to record initial hot-index control state: {err}");
                }
                debug!("Full bootstrap complete at slot {}", active_slot);
                info!(
                    "[watcher] bootstrap 全量导入完成，active_slot={}，开始接收增量快照",
                    active_slot
                );
            } else {
                staging_slot = Some(candidate.slot());
                debug!("Staging full snapshot ready at slot {}", candidate.slot());
                info!(
                    "[switch] staging 全量构建完成，staging_slot={}；后续增量将同时更新 active 与 _bak",
                    candidate.slot()
                );
            }
        } else {
            let incremental = match &candidate {
                WatchedSnapshot::Incremental(snapshot) => snapshot,
                WatchedSnapshot::Full(_) => unreachable!(),
            };
            // Active path is always kept current. During staging, the new
            // full's high-slot tail has already been applied to active, so
            // each following incremental can be validated against the active
            // watermark independently from staging's watermark.
            let active_eligible =
                candidate.slot() > active_slot && incremental.base_slot() <= active_slot;
            let mut active_loader = if active_eligible {
                match candidate.new_loader(active_slot) {
                    Ok(loader) => Some(loader),
                    Err(err) => {
                        invalid_archives.insert(candidate.path().to_path_buf());
                        warn!(
                            "Ignoring unreadable incremental snapshot {}: {}",
                            candidate.path().display(),
                            err
                        );
                        continue;
                    }
                }
            } else {
                None
            };

            // Queue the same archive for staging in slot order. It is picked
            // up by the independent worker above, so active never waits for
            // the backup route to finish this snapshot.
            if staging_slot.is_some_and(|stage_slot| {
                candidate.slot() > stage_slot && incremental.base_slot() <= stage_slot
            }) {
                staging_incremental_queue.push_back(incremental.clone());
            }

            let active_result = if let Some(ref mut loader) = active_loader {
                output.process(loader, snapshot_kind, TableGroup::Active)
            } else {
                Ok(())
            };

            if let Err(err) = active_result {
                // Do not leave a detached writer behind if active fails and
                // the process is about to exit. This is only the fatal-error
                // cleanup path; normal processing never waits for staging.
                if let Some(handle) = staging_incremental_handle.take() {
                    match handle.join() {
                        Ok(Ok(_)) => {}
                        Ok(Err(backup_err)) => {
                            warn!(
                                "[switch] active 增量失败，同时运行的 _bak 增量也失败：{}",
                                backup_err
                            )
                        }
                        Err(_) => {
                            warn!("[switch] active 增量失败，同时运行的 _bak 增量线程异常退出")
                        }
                    }
                }
                return Err(err);
            }
            if active_loader.is_some() {
                active_slot = candidate.slot();
            }
        }

        console_snapshot_status(
            "completed",
            snapshot_kind.as_str(),
            candidate.path(),
            candidate.slot(),
        );
        info!(
            "[watcher] {} 快照处理完成：active_slot={} staging_slot={:?}",
            snapshot_kind.as_str(),
            active_slot,
            staging_slot
        );
        has_processed_snapshot = true;
        if staging_incremental_handle.is_none() && staging_incremental_queue.is_empty() {
            exchange_if_ready(
                &mut output,
                &mut active_group_id,
                &mut active_slot,
                &mut staging_slot,
                &mut staging_hot_mints,
                &mut retired_until,
            )?;
        }
        debug!("Advanced active slot to {active_slot}");
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
        discover_full_snapshots, discover_incremental_snapshots, resume_slot_from_max_updated_slot,
        watched_snapshot_candidates, WatchedSnapshot,
    };
    use std::fs;
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
}
