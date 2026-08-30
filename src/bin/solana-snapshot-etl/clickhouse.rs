use borsh::BorshDeserialize;
use clickhouse::inserter::{Inserter, Quantities};
use clickhouse::{Client, Row};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use log::{debug, error, info, warn};
use percent_encoding::percent_decode_str;
use serde::{Deserialize, Serialize};
use solana_sdk::program_pack::Pack;
use solana_sdk::pubkey::Pubkey;
use solana_snapshot_etl::append_vec::{AppendVec, StoredAccountMeta};
use solana_snapshot_etl::{append_vec_accounts, AppendVecIterator};
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use url::Url;

use crate::mpl_metadata;

const DATABASE: &str = "solana";
const ACCOUNT_TABLE: &str = "raw_account";
const TOKEN_MINT_TABLE: &str = "raw_token_mint";
const TOKEN_METADATA_TABLE: &str = "raw_token_metadata";
const RAW_TABLES: [&str; 3] = [ACCOUNT_TABLE, TOKEN_MINT_TABLE, TOKEN_METADATA_TABLE];
const GROUP_HOT_TABLES: [&str; 4] = [
    "hot_token_filter",
    "hot_token_account_state",
    "hot_token_info",
    "hot_wallet_token_balance",
];
const GROUP_MERGE_TABLES: [&str; 7] = [
    ACCOUNT_TABLE,
    TOKEN_MINT_TABLE,
    TOKEN_METADATA_TABLE,
    "hot_token_filter",
    "hot_token_account_state",
    "hot_token_info",
    "hot_wallet_token_balance",
];
// `hot_token_info` is built in ordered mint ranges.  The full source mint and
// metadata tables are sorted by mint, so a bounded range keeps the right side
// of each JOIN small without repeatedly scanning overlapping key ranges.
const HOT_TOKEN_INFO_BATCH_SIZE: u64 = 10_000;
// Keep the high-cardinality wallet aggregation below the server-wide memory
// cap.  ClickHouse spills intermediate GROUP BY / sort state to its temporary
// disk once either limit is reached.
const HOT_BALANCE_EXTERNAL_AGGREGATION_BYTES: &str = "1073741824";
// A cold full import deliberately leaves a bounded number of large parts in
// each group table. Once background merges are restarted, let that backlog
// shrink before accepting the next incremental INSERT: otherwise the merge
// burst and another set of HTTP RowBinary INSERTs compete for the same disk.
// The condition is strict: every active partition must have fewer than this
// many active parts.
pub(crate) const RAW_MERGE_READY_PARTS_PER_PARTITION_LIMIT: u64 = 20;
const RAW_MERGE_READY_POLL_INTERVAL: Duration = Duration::from_secs(10);
const RAW_MERGE_READY_LOG_EVERY_POLLS: u64 = 3;

/// Physical table group used by the dual-buffer importer.  `Active` always
/// maps to the stable query-facing names; `Backup` maps to the `_bak` staging
/// names.  Keeping this choice in one value prevents a raw table from being
/// written to one generation while its derived tables are written to another.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TableGroup {
    Active,
    Backup,
}

impl TableGroup {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Backup => "backup",
        }
    }

    pub(crate) fn suffix(self) -> &'static str {
        match self {
            Self::Active => "",
            Self::Backup => "_bak",
        }
    }

    fn table(self, base: &str) -> String {
        format!("{base}{}", self.suffix())
    }
}

// Larger inserts reduce MergeTree part creation. The exporter also force-commits every open
// RowBinary stream regularly, so sparse derived tables cannot leave an idle chunked request open
// long enough for ClickHouse or a reverse proxy to close it.
// ClickHouse's MergeTree merger is storage-bound. Bigger input parts greatly
// reduce merge amplification. Keep this bounded by bytes so token rows cannot
// grow without limit in memory.
const MAX_BATCH_ROWS: u64 = 1_000_000;
const MAX_BATCH_BYTES: u64 = 256 * 1024 * 1024;
const BATCH_LIMIT_CHECK_INTERVAL: u16 = 1_024;
const FLUSH_CHECK_INTERVAL: u16 = 1_024;
// Never keep a sparse HTTP INSERT request open beyond this age.
// `clickhouse-rs` buffers RowBinary rows locally until roughly 256 KiB is
// available, so sparse derived tables can receive rows continuously while
// sending no bytes on their HTTP body. ClickHouse enforces its 30-second HTTP
// socket receive timeout before request-level settings are parsed, so the
// setting embedded in an INSERT URL cannot extend that deadline. Five seconds
// leaves room for a preceding large-part finalization and the other table
// streams to close. This applies to raw_account as well: Inserter's local
// RowBinary buffer can otherwise leave its HTTP request body idle for too long.
const MAX_OPEN_INSERT_AGE: Duration = Duration::from_secs(5);
// ClickHouse's HTTP receive timeout is commonly 30 seconds. A tar.zst entry
// can take longer than that to decompress before another account reaches a
// worker, so a worker must close an open chunked upload while it waits for the
// archive reader.
const IDLE_INSERT_FLUSH_INTERVAL: Duration = Duration::from_secs(10);
// This setting applies after ClickHouse begins executing the query, but does
// not replace the server's initial 30-second HTTP-body socket timeout. The
// short client-side age limit above protects that initial phase.
const HTTP_RECEIVE_TIMEOUT_SECS: &str = "600";
const HTTP_RECEIVE_TIMEOUT: Duration = Duration::from_secs(600);
// A ClickHouse INSERT is not finished when the client has uploaded the
// RowBinary body: MergeTree still has to build parts before returning HTTP 200.
// The permit
// limits the number of finalizations, but it must not be smaller than the
// number of parser workers: an Inserter has already opened its HTTP request
// while rows are being serialized, so a worker waiting behind a single permit
// can leave that request idle long enough for ClickHouse to close it.  Keep
// this bounded at the worker count (the CLI caps workers at four).
const MAX_INSERT_CONCURRENCY: usize = 4;
// The inserter crate has no end timeout by default, so an overloaded or
// wedged ClickHouse query would otherwise leave a worker blocked forever.
// Keep this generous enough for a 256 MiB part while making failures visible
// and recoverable.
const INSERT_END_TIMEOUT_SECS: u64 = 30 * 60;
// Keep tombstone INSERT/query batches large enough to avoid creating
// thousands of tiny MergeTree parts while bounding each RowBinary request and
// the point-lookup IN list used to recover the previous raw token-account
// pair.
const CLOSE_TOMBSTONE_BATCH_SIZE: usize = 100_000;
// Incremental pair sets can be large (one entry per changed token-account
// owner/mint pair). Keep the query in one round trip without relying on the
// server's small default max_query_size; the values are generated from base58
// public keys and do not contain user-controlled SQL.
const HOT_PAIR_QUERY_MAX_QUERY_SIZE: &str = "134217728";

pub(crate) type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
type TokenPair = (String, String);
pub(crate) type HotMintSet = Arc<HashSet<String>>;

/// Describes how the archive is being applied to ClickHouse.
///
/// Full archives are canonical checkpoints and Agave deliberately excludes
/// tombstone records from them. Incremental archives retain tombstones so that
/// they can delete rows that were present in the full base.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SnapshotKind {
    Full,
    Incremental,
}

impl SnapshotKind {
    fn collect_close_tombstones(self) -> bool {
        matches!(self, Self::Incremental)
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Incremental => "incremental",
        }
    }
}

pub(crate) struct ClickhouseIndexer {
    client: Client,
    connection_url: String,
    group: TableGroup,
    hot_mints: HotMintSet,
    sink: ClickhouseSink,
    snapshot_slot: u64,
    multi_progress: MultiProgress,
    progress: Arc<Progress>,
}

struct Progress {
    append_vecs: ProgressBar,
    accounts: ProgressCounter,
    tokens: ProgressCounter,
    metadata: ProgressCounter,
}

impl Progress {
    /// Estimate remaining time from the complete run average. The built-in
    /// indicatif ETA uses a short recent-rate window, which is unstable because
    /// AppendVec sizes vary considerably.
    fn inc_append_vec(&self) {
        self.append_vecs.inc(1);
        let completed = self.append_vecs.position();
        let Some(total) = self.append_vecs.length() else {
            return;
        };
        if completed == 0 {
            return;
        }

        let remaining = total.saturating_sub(completed);
        let average_seconds = self.append_vecs.elapsed().as_secs_f64() / completed as f64;
        let eta_seconds = average_seconds * remaining as f64;
        self.append_vecs
            .set_message(format!("avg_eta={}", format_duration(eta_seconds)));
    }
}

fn format_duration(seconds: f64) -> String {
    if !seconds.is_finite() || seconds < 0.0 {
        return "unknown".to_owned();
    }

    let total_seconds = seconds.ceil().min(u64::MAX as f64) as u64;
    let hours = total_seconds / 3_600;
    let minutes = (total_seconds % 3_600) / 60;
    let seconds = total_seconds % 60;
    format!("{hours:02}:{minutes:02}:{seconds:02}")
}

pub(crate) struct IndexStats {
    pub(crate) accounts_total: u64,
    pub(crate) token_accounts_total: u64,
    pub(crate) skipped_append_vecs: u64,
    pub(crate) append_vecs_total: u64,
    pub(crate) nonempty_zero_account_append_vecs: u64,
    pub(crate) spl_token_owner_accounts_seen: u64,
    pub(crate) spl_token_accounts_parsed: u64,
    pub(crate) spl_token_unexpected_size: u64,
    pub(crate) spl_token_unpack_failed: u64,
    pub(crate) token_2022_owner_accounts_seen: u64,
    pub(crate) token_2022_accounts_parsed: u64,
    pub(crate) token_2022_unexpected_size: u64,
    pub(crate) token_2022_unpack_failed: u64,
    pub(crate) token_account_close_candidates: u64,
    pub(crate) token_accounts_marked_deleted: u64,
}

pub(crate) struct CloseTombstoneStats {
    pub(crate) append_vecs_total: u64,
    pub(crate) skipped_append_vecs: u64,
    pub(crate) canonical_empty_accounts: u64,
    pub(crate) token_accounts_marked_deleted: u64,
}

#[derive(Row, Serialize)]
struct AccountRow {
    pubkey: String,
    owner: String,
    lamports: u64,
    data_len: u64,
    executable: bool,
    updated_slot: u64,
}

#[derive(Row, Serialize)]
struct TokenAccountRow {
    pubkey: String,
    mint: String,
    owner: String,
    amount: u64,
    delegate: Option<String>,
    delegated_amount: u64,
    state: u8,
    close_authority: Option<String>,
    /// ClickHouse ReplacingMergeTree's is_deleted column: 0 = live, 1 = tombstone.
    is_deleted: u8,
    updated_slot: u64,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct AccountVersion {
    updated_slot: u64,
}

#[derive(Row, Deserialize)]
struct HotTokenAccountPairRow {
    pubkey: String,
    mint: String,
    owner: String,
    updated_slot: u64,
}

#[derive(Row, Deserialize)]
struct ExplainEstimateRow {
    database: String,
    table: String,
    parts: u64,
    rows: u64,
    marks: u64,
}

#[derive(Row, Deserialize)]
struct WalletBalanceAggregateRow {
    mint: String,
    owner: String,
    amount_raw: u64,
}

#[derive(Row, Serialize)]
struct WalletBalanceRow {
    mint: String,
    owner: String,
    amount_raw: u64,
    updated_slot: u64,
}

struct TombstoneWriteResult {
    marked_deleted: u64,
    affected_pairs: HashSet<TokenPair>,
}

#[derive(Row, Serialize)]
struct TokenMintRow {
    mint: String,
    mint_authority: Option<String>,
    supply: u64,
    decimals: u8,
    is_initialized: bool,
    freeze_authority: Option<String>,
    updated_slot: u64,
}

#[derive(Row, Serialize)]
struct TokenMetadataRow {
    mint: String,
    name: String,
    symbol: String,
    uri: String,
    update_authority: String,
    is_mutable: bool,
    token_standard: Option<u8>,
    seller_fee_basis_points: u16,
    creators: Vec<String>,
    updated_slot: u64,
}

impl ClickhouseIndexer {
    pub(crate) fn new(
        connection_url: String,
        snapshot_slot: u64,
        append_vec_count: Option<u64>,
        group: TableGroup,
        hot_mints: HotMintSet,
    ) -> Result<Self> {
        let spinner_style = ProgressStyle::with_template(
            "{prefix:>13.bold.dim} {spinner} rate={per_sec:>13} total={human_pos:>11} elapsed={elapsed_precise} {msg}",
        )?;
        let multi_progress = MultiProgress::new();
        let append_vec_style = ProgressStyle::with_template(
            "{prefix:>13.bold.dim} [{bar:40.cyan/blue}] {pos:>7}/{len:>7} ({percent:>3}%) elapsed={elapsed_precise} {msg}",
        )?;
        let append_vecs = multi_progress.add(match append_vec_count {
            Some(total) => ProgressBar::new(total)
                .with_style(append_vec_style)
                .with_prefix("append_vecs")
                .with_message("avg_eta=calculating"),
            None => ProgressBar::new_spinner()
                .with_style(spinner_style.clone())
                .with_prefix("append_vecs")
                .with_message("avg_eta=unknown"),
        });

        let progress = Arc::new(Progress {
            append_vecs,
            accounts: ProgressCounter::new(
                multi_progress.add(
                    ProgressBar::new_spinner()
                        .with_style(spinner_style.clone())
                        .with_prefix("accs"),
                ),
            ),
            tokens: ProgressCounter::new(
                multi_progress.add(
                    ProgressBar::new_spinner()
                        .with_style(spinner_style.clone())
                        .with_prefix("token_accs"),
                ),
            ),
            metadata: ProgressCounter::new(
                multi_progress.add(
                    ProgressBar::new_spinner()
                        .with_style(spinner_style)
                        .with_prefix("metaplex_accs"),
                ),
            ),
        });

        let client = new_clickhouse_client(&connection_url)?;

        Ok(Self {
            sink: ClickhouseSink::new(&client, "main", None, group),
            client,
            connection_url,
            group,
            hot_mints,
            snapshot_slot,
            multi_progress,
            progress,
        })
    }

    pub(crate) async fn insert_all(
        self,
        iterator: AppendVecIterator<'_>,
        snapshot_kind: SnapshotKind,
        workers: usize,
    ) -> Result<IndexStats> {
        if workers > 1 {
            self.insert_all_parallel(iterator, snapshot_kind, workers)
                .await
        } else {
            self.insert_all_sequential(iterator, snapshot_kind).await
        }
    }

    async fn insert_all_sequential(
        mut self,
        iterator: AppendVecIterator<'_>,
        snapshot_kind: SnapshotKind,
    ) -> Result<IndexStats> {
        let collect_close_tombstones = snapshot_kind.collect_close_tombstones();
        let collect_affected_pairs = matches!(snapshot_kind, SnapshotKind::Incremental);
        let mut worker = Worker {
            sink: &mut self.sink,
            snapshot_slot: self.snapshot_slot,
            progress: Arc::clone(&self.progress),
            collect_close_tombstones,
            spl_token_owner_accounts_seen: 0,
            spl_token_accounts_parsed: 0,
            spl_token_unexpected_size: 0,
            spl_token_unpack_failed: 0,
            token_2022_owner_accounts_seen: 0,
            token_2022_accounts_parsed: 0,
            token_2022_unexpected_size: 0,
            token_2022_unpack_failed: 0,
            closed_token_accounts: HashMap::new(),
            collect_affected_pairs,
            affected_pairs: HashSet::new(),
            hot_mints: Arc::clone(&self.hot_mints),
        };
        let mut skipped_append_vecs = 0;
        let mut append_vecs_total = 0;
        let mut nonempty_zero_account_append_vecs = 0;

        for (append_vec_idx, append_vec) in iterator.enumerate() {
            match append_vec {
                Ok(append_vec) => {
                    append_vecs_total += 1;
                    if worker.on_append_vec_count(append_vec).await? == 0 {
                        nonempty_zero_account_append_vecs += 1;
                    }
                }
                Err(err) => {
                    skipped_append_vecs += 1;
                    warn!(
                        "[clickhouse] Skipping append vec #{}: {}",
                        append_vec_idx, err
                    );
                }
            }
            self.progress.inc_append_vec();
        }

        let spl_token_owner_accounts_seen = worker.spl_token_owner_accounts_seen;
        let spl_token_accounts_parsed = worker.spl_token_accounts_parsed;
        let spl_token_unexpected_size = worker.spl_token_unexpected_size;
        let spl_token_unpack_failed = worker.spl_token_unpack_failed;
        let token_2022_owner_accounts_seen = worker.token_2022_owner_accounts_seen;
        let token_2022_accounts_parsed = worker.token_2022_accounts_parsed;
        let token_2022_unexpected_size = worker.token_2022_unexpected_size;
        let token_2022_unpack_failed = worker.token_2022_unpack_failed;
        let closed_token_accounts = std::mem::take(&mut worker.closed_token_accounts);
        let mut affected_pairs = std::mem::take(&mut worker.affected_pairs);
        let token_account_close_candidates = if collect_close_tombstones {
            closed_token_accounts.len() as u64
        } else {
            0
        };
        drop(worker);

        self.sink.end().await?;
        let tombstone_result = if collect_close_tombstones {
            write_close_token_account_tombstones(&self.client, self.group, &closed_token_accounts)
                .await?
        } else {
            debug!(
                "[clickhouse] Full snapshot: skipped tombstone candidate scan (archive excludes tombstones)"
            );
            TombstoneWriteResult {
                marked_deleted: 0,
                affected_pairs: HashSet::new(),
            }
        };
        affected_pairs.extend(tombstone_result.affected_pairs);
        refresh_hot_indexes(
            &self.client,
            self.group,
            snapshot_kind,
            self.snapshot_slot,
            &affected_pairs,
            matches!(snapshot_kind, SnapshotKind::Full),
        )
        .await?;
        self.progress.append_vecs.finish_with_message("done");
        self.progress.accounts.sync();
        self.progress.tokens.sync();
        self.progress.metadata.sync();
        let _ = &self.multi_progress;

        Ok(IndexStats {
            accounts_total: self.progress.accounts.get(),
            token_accounts_total: self.progress.tokens.get(),
            skipped_append_vecs,
            append_vecs_total,
            nonempty_zero_account_append_vecs,
            spl_token_owner_accounts_seen,
            spl_token_accounts_parsed,
            spl_token_unexpected_size,
            spl_token_unpack_failed,
            token_2022_owner_accounts_seen,
            token_2022_accounts_parsed,
            token_2022_unexpected_size,
            token_2022_unpack_failed,
            token_account_close_candidates,
            token_accounts_marked_deleted: tombstone_result.marked_deleted,
        })
    }

    /// Decode AppendVecs and write ClickHouse rows concurrently.  Archive
    /// decompression itself is necessarily ordered (one tar.zst stream), but
    /// each completed AppendVec is independent.  The old implementation held
    /// the only ClickHouse inserter while parsing every account, leaving the
    /// other CPUs idle.  A bounded queue overlaps stream decompression,
    /// account parsing/base58 encoding, and multiple ClickHouse HTTP inserts.
    async fn insert_all_parallel(
        self,
        iterator: AppendVecIterator<'_>,
        snapshot_kind: SnapshotKind,
        workers: usize,
    ) -> Result<IndexStats> {
        let workers = workers.max(2);
        let collect_close_tombstones = snapshot_kind.collect_close_tombstones();
        let collect_affected_pairs = matches!(snapshot_kind, SnapshotKind::Incremental);
        // Keep at most one queued AppendVec per worker.  AppendVecs are
        // memory-mapped buffers and can be large, so an unbounded or oversized
        // queue would trade the CPU win for avoidable memory pressure.
        let (tx, rx) = crossbeam::channel::bounded::<AppendVec>(workers);
        let mut handles = Vec::with_capacity(workers);
        let insert_concurrency = workers.min(MAX_INSERT_CONCURRENCY);
        let insert_gate = Arc::new(Semaphore::new(insert_concurrency));
        let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        debug!(
            "[clickhouse] Parallel workers={workers}; INSERT finalization concurrency={insert_concurrency}"
        );

        for worker_index in 0..workers {
            let rx = rx.clone();
            let connection_url = self.connection_url.clone();
            let group = self.group;
            let snapshot_slot = self.snapshot_slot;
            let progress = Arc::clone(&self.progress);
            let insert_gate = Arc::clone(&insert_gate);
            let cancelled = Arc::clone(&cancelled);
            let hot_mints = Arc::clone(&self.hot_mints);
            handles.push(thread::spawn(move || {
                debug!("[clickhouse] Worker {worker_index} thread started");
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|err| err.to_string())?;
                runtime.block_on(async move {
                    // Client clones share one Hyper connection pool. Each worker has a
                    // distinct current-thread Tokio runtime, so sharing that pool can
                    // make one worker's request depend on another worker's connection
                    // driver. Give every worker its own transport instead.
                    let client = new_clickhouse_client(&connection_url)
                        .map_err(|err| err.to_string())?;
                    let mut sink = ClickhouseSink::new(
                        &client,
                        format!("worker-{worker_index}"),
                        Some(insert_gate),
                        group,
                    );
                    debug!("[clickhouse] Worker {worker_index} ready; waiting for AppendVec");
                    let mut worker = Worker {
                        sink: &mut sink,
                        snapshot_slot,
                        progress,
                        collect_close_tombstones,
                        spl_token_owner_accounts_seen: 0,
                        spl_token_accounts_parsed: 0,
                        spl_token_unexpected_size: 0,
                        spl_token_unpack_failed: 0,
                        token_2022_owner_accounts_seen: 0,
                        token_2022_accounts_parsed: 0,
                        token_2022_unexpected_size: 0,
                        token_2022_unpack_failed: 0,
                        closed_token_accounts: HashMap::new(),
                        collect_affected_pairs,
                        affected_pairs: HashSet::new(),
                        hot_mints,
                    };
                    let mut append_vecs_total = 0;
                    let mut nonempty_zero_account_append_vecs = 0;
                    loop {
                        if cancelled.load(Ordering::Acquire) {
                            return Err(
                                "parallel import cancelled after another worker failed".to_owned(),
                            );
                        }
                        let append_vec = match rx.recv_timeout(IDLE_INSERT_FLUSH_INTERVAL) {
                            Ok(append_vec) => {
                                debug!(
                                    "[clickhouse] Worker {worker_index} received file=accounts/{}.{} bytes={}",
                                    append_vec.slot(),
                                    append_vec.id(),
                                    append_vec.len(),
                                );
                                append_vec
                            }
                            Err(crossbeam::channel::RecvTimeoutError::Timeout) => {
                                debug!(
                                    "[clickhouse] Worker {worker_index} idle: archive queue empty"
                                );
                                if worker.sink.has_pending_rows() {
                                    debug!(
                                        "[clickhouse] Worker {worker_index} is waiting for archive input; flushing open inserts"
                                    );
                                    if let Err(err) = worker.sink.force_commit_all().await {
                                        error!(
                                            "[clickhouse] Worker {worker_index} failed during idle INSERT flush: {err}"
                                        );
                                        cancelled.store(true, Ordering::Release);
                                        return Err(err.to_string());
                                    }
                                }
                                continue;
                            }
                            Err(crossbeam::channel::RecvTimeoutError::Disconnected) => break,
                        };
                        let append_vec_slot = append_vec.slot();
                        let append_vec_id = append_vec.id();
                        append_vecs_total += 1;
                        debug!(
                            "[clickhouse] Worker {worker_index} parsing file=accounts/{append_vec_slot}.{append_vec_id}"
                        );
                        let process_started = Instant::now();
                        match worker.on_append_vec_count(append_vec).await {
                            Ok(parsed_accounts) => {
                                debug!(
                                    "[clickhouse] Worker {worker_index} finished file=accounts/{append_vec_slot}.{append_vec_id} accounts={parsed_accounts} elapsed={:?}",
                                    process_started.elapsed()
                                );
                                if parsed_accounts == 0 {
                                    nonempty_zero_account_append_vecs += 1;
                                }
                            }
                            Err(err) => {
                                error!(
                                    "[clickhouse] Worker {worker_index} failed parsing/writing file=accounts/{append_vec_slot}.{append_vec_id}: {err}"
                                );
                                cancelled.store(true, Ordering::Release);
                                return Err(err.to_string());
                            }
                        }
                        worker.progress.inc_append_vec();
                    }

                    let spl_token_owner_accounts_seen = worker.spl_token_owner_accounts_seen;
                    let spl_token_accounts_parsed = worker.spl_token_accounts_parsed;
                    let spl_token_unexpected_size = worker.spl_token_unexpected_size;
                    let spl_token_unpack_failed = worker.spl_token_unpack_failed;
                    let token_2022_owner_accounts_seen = worker.token_2022_owner_accounts_seen;
                    let token_2022_accounts_parsed = worker.token_2022_accounts_parsed;
                    let token_2022_unexpected_size = worker.token_2022_unexpected_size;
                    let token_2022_unpack_failed = worker.token_2022_unpack_failed;
                    let closed_token_accounts = std::mem::take(&mut worker.closed_token_accounts);
                    let affected_pairs = std::mem::take(&mut worker.affected_pairs);
                    drop(worker);
                    if cancelled.load(Ordering::Acquire) {
                        return Err(
                            "parallel import cancelled after another worker failed".to_owned(),
                        );
                    }
                    if let Err(err) = sink.end().await {
                        error!("[clickhouse] Worker {worker_index} failed final INSERT flush: {err}");
                        cancelled.store(true, Ordering::Release);
                        return Err(err.to_string());
                    }

                    Ok(ParallelWorkerStats {
                        append_vecs_total,
                        nonempty_zero_account_append_vecs,
                        spl_token_owner_accounts_seen,
                        spl_token_accounts_parsed,
                        spl_token_unexpected_size,
                        spl_token_unpack_failed,
                        token_2022_owner_accounts_seen,
                        token_2022_accounts_parsed,
                        token_2022_unexpected_size,
                        token_2022_unpack_failed,
                        closed_token_accounts,
                        affected_pairs,
                    })
                })
            }));
        }
        drop(rx);

        let mut skipped_append_vecs = 0;
        let mut producer_error: Option<String> = None;
        'producer: for (append_vec_idx, append_vec) in iterator.enumerate() {
            match append_vec {
                Ok(append_vec) => {
                    if cancelled.load(Ordering::Acquire) {
                        producer_error =
                            Some("parallel import cancelled after a worker failure".to_owned());
                        break 'producer;
                    }
                    let slot = append_vec.slot();
                    let id = append_vec.id();
                    let dispatch_started = Instant::now();
                    let pending_before = tx.len();
                    let mut pending = append_vec;
                    loop {
                        if cancelled.load(Ordering::Acquire) {
                            producer_error =
                                Some("parallel import cancelled after a worker failure".to_owned());
                            break 'producer;
                        }
                        match tx.send_timeout(pending, Duration::from_millis(100)) {
                            Ok(()) => break,
                            Err(crossbeam::channel::SendTimeoutError::Timeout(value)) => {
                                pending = value;
                            }
                            Err(crossbeam::channel::SendTimeoutError::Disconnected(_)) => {
                                producer_error = Some(
                                    "parallel ClickHouse worker exited unexpectedly".to_owned(),
                                );
                                break 'producer;
                            }
                        }
                    }
                    let dispatch_elapsed = dispatch_started.elapsed();
                    if dispatch_elapsed >= Duration::from_secs(1) || append_vec_idx % 1_000 == 0 {
                        debug!(
                            "[clickhouse] Dispatch of AppendVec index={append_vec_idx} slot={slot} id={id} queue_before={pending_before} queue_after={} send_elapsed={:?}",
                            tx.len(),
                            dispatch_elapsed
                        );
                    }
                }
                Err(err) => {
                    skipped_append_vecs += 1;
                    warn!(
                        "[clickhouse] Skipping append vec #{}: {}",
                        append_vec_idx, err
                    );
                }
            }
        }
        drop(tx);

        let mut totals = ParallelWorkerStats::default();
        for handle in handles {
            let stats = handle
                .join()
                .map_err(|_| "parallel ClickHouse worker panicked")?
                .map_err(|err| format!("parallel ClickHouse worker failed: {err}"))?;
            totals.merge(stats);
        }

        if let Some(err) = producer_error {
            return Err(err.into());
        }

        let token_account_close_candidates = if collect_close_tombstones {
            totals.closed_token_accounts.len() as u64
        } else {
            0
        };
        let mut affected_pairs = totals.affected_pairs;
        let tombstone_result = if collect_close_tombstones {
            write_close_token_account_tombstones(
                &self.client,
                self.group,
                &totals.closed_token_accounts,
            )
            .await?
        } else {
            debug!(
                "[clickhouse] Full snapshot: skipped tombstone candidate scan (archive excludes tombstones)"
            );
            TombstoneWriteResult {
                marked_deleted: 0,
                affected_pairs: HashSet::new(),
            }
        };
        affected_pairs.extend(tombstone_result.affected_pairs);
        refresh_hot_indexes(
            &self.client,
            self.group,
            snapshot_kind,
            self.snapshot_slot,
            &affected_pairs,
            matches!(snapshot_kind, SnapshotKind::Full),
        )
        .await?;
        self.progress.append_vecs.finish_with_message("done");
        self.progress.accounts.sync();
        self.progress.tokens.sync();
        self.progress.metadata.sync();

        Ok(IndexStats {
            accounts_total: self.progress.accounts.get(),
            token_accounts_total: self.progress.tokens.get(),
            skipped_append_vecs,
            append_vecs_total: totals.append_vecs_total,
            nonempty_zero_account_append_vecs: totals.nonempty_zero_account_append_vecs,
            spl_token_owner_accounts_seen: totals.spl_token_owner_accounts_seen,
            spl_token_accounts_parsed: totals.spl_token_accounts_parsed,
            spl_token_unexpected_size: totals.spl_token_unexpected_size,
            spl_token_unpack_failed: totals.spl_token_unpack_failed,
            token_2022_owner_accounts_seen: totals.token_2022_owner_accounts_seen,
            token_2022_accounts_parsed: totals.token_2022_accounts_parsed,
            token_2022_unexpected_size: totals.token_2022_unexpected_size,
            token_2022_unpack_failed: totals.token_2022_unpack_failed,
            token_account_close_candidates,
            token_accounts_marked_deleted: tombstone_result.marked_deleted,
        })
    }

    /// Scan only canonical empty accounts and append their delete versions.
    /// Unlike `insert_all`, this does not write any raw or parsed snapshot
    /// rows, so it can repair tombstones without re-importing an already
    /// loaded snapshot.
    pub(crate) async fn mark_close_tombstones(
        self,
        iterator: AppendVecIterator<'_>,
    ) -> Result<CloseTombstoneStats> {
        let mut closed_token_accounts = HashMap::new();
        let mut skipped_append_vecs = 0;
        let mut append_vecs_total = 0;
        let mut canonical_empty_accounts = 0;

        for (append_vec_idx, append_vec) in iterator.enumerate() {
            match append_vec {
                Ok(append_vec) => {
                    append_vecs_total += 1;
                    for account in append_vec_accounts(&append_vec) {
                        if is_canonical_empty_account(
                            account.meta.data_len,
                            account.account_meta.lamports,
                            account.account_meta.owner,
                            account.account_meta.executable,
                        ) {
                            canonical_empty_accounts += 1;
                            remember_close_candidate(
                                &mut closed_token_accounts,
                                &account,
                                append_vec.slot(),
                            );
                        }
                    }
                }
                Err(err) => {
                    skipped_append_vecs += 1;
                    warn!(
                        "[clickhouse] Skipping append vec #{} while scanning tombstones: {}",
                        append_vec_idx, err
                    );
                }
            }
            self.progress.inc_append_vec();
        }

        let tombstone_result =
            write_close_token_account_tombstones(&self.client, self.group, &closed_token_accounts)
                .await?;
        if tombstone_result.marked_deleted > 0 {
            refresh_hot_indexes(
                &self.client,
                self.group,
                SnapshotKind::Incremental,
                self.snapshot_slot,
                &tombstone_result.affected_pairs,
                false,
            )
            .await?;
        }
        self.progress.append_vecs.finish_with_message("done");
        self.progress.accounts.sync();
        self.progress.tokens.sync();
        self.progress.metadata.sync();
        let _ = &self.multi_progress;

        Ok(CloseTombstoneStats {
            append_vecs_total,
            skipped_append_vecs,
            canonical_empty_accounts,
            token_accounts_marked_deleted: tombstone_result.marked_deleted,
        })
    }
}

struct ClickhouseConnection {
    endpoint: String,
    user: Option<String>,
    password: Option<String>,
}

fn new_clickhouse_client(connection_url: &str) -> Result<Client> {
    let connection = parse_clickhouse_connection_url(connection_url)?;
    let mut client = Client::default()
        .with_url(connection.endpoint)
        .with_database(DATABASE)
        // This selects HTTP RowBinary instead of RowBinaryWithNamesAndTypes.
        .with_validation(false);

    if let Some(user) = connection.user {
        client = client.with_user(user);
    }
    if let Some(password) = connection.password {
        client = client.with_password(password);
    }

    Ok(client)
}

/// Log ClickHouse's part/mark/row estimate before a deliberately expensive
/// read query. `EXPLAIN ESTIMATE` reflects the physical ranges selected after
/// primary-key pruning, not the final number of rows returned by filters or
/// aggregation. Keep the operation best-effort so an older ClickHouse version
/// without EXPLAIN ESTIMATE does not prevent an otherwise valid repair.
async fn log_query_scan_estimate(client: &Client, label: &str, select_sql: &str) {
    let explain_sql = format!("EXPLAIN ESTIMATE {select_sql}");
    match client
        .query(&explain_sql)
        .with_setting("max_query_size", HOT_PAIR_QUERY_MAX_QUERY_SIZE)
        .fetch_all::<ExplainEstimateRow>()
        .await
    {
        Ok(rows) if rows.is_empty() => {
            info!("[clickhouse] query estimate label={label}: no table ranges selected");
        }
        Ok(rows) => {
            let parts = rows.iter().map(|row| row.parts).sum::<u64>();
            let scanned_rows = rows.iter().map(|row| row.rows).sum::<u64>();
            let marks = rows.iter().map(|row| row.marks).sum::<u64>();
            info!(
                "[clickhouse] query estimate label={} total_parts={} total_marks={} estimated_rows={} tables={}",
                label,
                parts,
                marks,
                scanned_rows,
                rows.len()
            );
            for row in rows {
                info!(
                    "[clickhouse] query estimate label={} table={}.{} parts={} marks={} estimated_rows={}",
                    label,
                    row.database,
                    row.table,
                    row.parts,
                    row.marks,
                    row.rows
                );
            }
        }
        Err(err) => warn!(
            "[clickhouse] query estimate unavailable label={label}; proceeding without it: {err}"
        ),
    }
}

/// Return the high-water mark that the snapshot watcher uses to resume an
/// existing ClickHouse import. `coalesce` also makes an empty raw_account
/// table start from slot zero.
pub(crate) async fn max_raw_account_updated_slot(
    connection_url: &str,
    group: TableGroup,
) -> Result<u64> {
    let table = group.table(ACCOUNT_TABLE);
    new_clickhouse_client(connection_url)?
        .query(&format!(
            "SELECT coalesce(max(updated_slot), toUInt64(0)) FROM {table}"
        ))
        .fetch_one::<u64>()
        .await
        .map_err(Into::into)
}

/// Return the current global hot-token configuration version for control-table
/// audit records. It is never used to alter an already-built table group's
/// frozen mint set.
pub(crate) async fn hot_token_version(connection_url: &str) -> Result<u64> {
    new_clickhouse_client(connection_url)?
        .query("SELECT coalesce(max(version), toUInt64(0)) FROM hot_token_enabled")
        .fetch_one::<u64>()
        .await
        .map_err(Into::into)
}

/// Freeze the global hot-token selection into one physical table group. This
/// is called only while building a new full generation; active incrementals
/// receive the resulting in-memory set and never query `hot_token_enabled`.
pub(crate) async fn snapshot_group_hot_mints(
    connection_url: &str,
    group: TableGroup,
) -> Result<HotMintSet> {
    let client = new_clickhouse_client(connection_url)?;
    let table = group.table("hot_token_filter");
    client
        .query(&format!(
            "TRUNCATE TABLE {table} SETTINGS max_table_size_to_drop = 0"
        ))
        .execute()
        .await
        .map_err(|err| format!("failed to reset frozen hot-token filter {table}: {err}"))?;
    client
        .query(&format!(
            "INSERT INTO {table} (mint) SELECT mint FROM hot_token_enabled"
        ))
        .execute()
        .await
        .map_err(|err| format!("failed to snapshot hot_token_enabled into {table}: {err}"))?;
    load_group_hot_mints_with_client(&client, group).await
}

/// Restore the frozen mint set for an already-built active/staging group. It
/// is used only on process start (or by an independently spawned worker), not
/// for each incremental archive in the long-running active watcher.
pub(crate) async fn load_group_hot_mints(
    connection_url: &str,
    group: TableGroup,
) -> Result<HotMintSet> {
    let client = new_clickhouse_client(connection_url)?;
    load_group_hot_mints_with_client(&client, group).await
}

async fn load_group_hot_mints_with_client(
    client: &Client,
    group: TableGroup,
) -> Result<HotMintSet> {
    let table = group.table("hot_token_filter");
    let rows = client
        .query(&format!("SELECT mint FROM {table}"))
        .fetch_all::<HotMintRow>()
        .await
        .map_err(|err| format!("failed to load frozen hot-token filter {table}: {err}"))?;
    if rows.is_empty() {
        return Err(format!(
            "frozen hot-token filter {table} is empty; build this table group from a full snapshot first"
        )
        .into());
    }
    let mints = rows.into_iter().map(|row| row.mint).collect::<HashSet<_>>();
    info!(
        "[clickhouse] loaded frozen hot-token filter group={} mint_count={}",
        group.as_str(),
        mints.len()
    );
    Ok(Arc::new(mints))
}

pub(crate) async fn reset_table_group(connection_url: &str, group: TableGroup) -> Result<()> {
    let client = new_clickhouse_client(connection_url)?;
    for base in RAW_TABLES.into_iter().chain(GROUP_HOT_TABLES) {
        let table = group.table(base);
        client
            .query(&format!(
                "TRUNCATE TABLE {table} SETTINGS max_table_size_to_drop = 0"
            ))
            .execute()
            .await
            .map_err(|err| format!("failed to truncate {table}: {err}"))?;
    }
    Ok(())
}

/// Enable or disable ClickHouse's background merges for all seven tables in
/// one physical table group. Full snapshot loading is append-heavy and does
/// not need background ReplacingMergeTree deduplication while rows are
/// arriving; deferring raw and derived-table merges keeps CPU/IO available
/// for INSERTs. The caller re-enables merges only after a successful cold load
/// and derived refresh. On failure the process exits (bootstrap) or cleans
/// the staging group (backup) with merges stopped.
pub(crate) async fn set_group_table_merges(
    connection_url: &str,
    group: TableGroup,
    enabled: bool,
) -> Result<()> {
    let client = new_clickhouse_client(connection_url)?;
    let action = if enabled { "START" } else { "STOP" };
    info!(
        "[clickhouse] SYSTEM {action} MERGES for raw+hot tables group={} table_count={}",
        group.as_str(),
        GROUP_MERGE_TABLES.len()
    );
    let mut first_error = None;
    for base in GROUP_MERGE_TABLES {
        let table = group.table(base);
        if let Err(err) = client
            .query(&format!("SYSTEM {action} MERGES {table}"))
            .execute()
            .await
        {
            let message = format!(
                "failed to SYSTEM {action} MERGES for {table} (group={}): {err}",
                group.as_str()
            );
            warn!("[clickhouse] {message}");
            if first_error.is_none() {
                first_error = Some(message);
            }
        }
    }
    match first_error {
        Some(err) => Err(err.into()),
        None => Ok(()),
    }
}

#[derive(Debug, Row, Deserialize)]
struct RawMergePartCount {
    partition: String,
    parts_count: u64,
    total_rows: u64,
    total_size: String,
}

fn raw_merge_backlog_is_ready(parts: &[RawMergePartCount]) -> bool {
    parts
        .iter()
        .all(|part| part.parts_count < RAW_MERGE_READY_PARTS_PER_PARTITION_LIMIT)
}

fn raw_merge_backlog_summary(table: &str, parts: &[RawMergePartCount]) -> String {
    if parts.is_empty() {
        return format!("{table}[no active parts]");
    }

    let partitions = parts
        .iter()
        .map(|part| {
            format!(
                "partition={} parts={} rows={} size={}",
                part.partition, part.parts_count, part.total_rows, part.total_size
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("{table}[{partitions}]")
}

async fn raw_merge_part_counts(client: &Client, table: &str) -> Result<Vec<RawMergePartCount>> {
    client
        .query(&format!(
            "SELECT partition, count() AS parts_count, sum(rows) AS total_rows, \
             formatReadableSize(sum(bytes_on_disk)) AS total_size \
             FROM system.parts \
             WHERE database = {database} AND table = {table_name} AND active = 1 \
             GROUP BY partition \
             ORDER BY parts_count DESC, partition",
            database = sql_string_literal(DATABASE),
            table_name = sql_string_literal(table),
        ))
        .fetch_all::<RawMergePartCount>()
        .await
        .map_err(|err| format!("failed to read active parts for {table}: {err}").into())
}

/// After a successful full import starts raw and derived-table Merge again,
/// wait until every partition in every group table has a modest active-part
/// backlog. This is a write barrier for the next incremental snapshot, not a
/// request for a single fully merged part; keeping fewer than 20 parts per
/// partition still gives ClickHouse room to finish the remaining background
/// work naturally.
///
/// The probe retries transient ClickHouse failures.  At this point the full
/// generation is already valid and MERGE has already been resumed, so a brief
/// system.parts read failure must not cause the staging generation to be
/// cleared and re-imported.
pub(crate) async fn wait_for_group_merges_to_settle(
    connection_url: &str,
    group: TableGroup,
) -> Result<()> {
    let client = new_clickhouse_client(connection_url)?;
    let tables = GROUP_MERGE_TABLES
        .iter()
        .map(|base| group.table(base))
        .collect::<Vec<_>>();
    let started_at = Instant::now();
    let mut polls = 0_u64;

    info!(
        "[clickhouse] group MERGE 已恢复；等待 group={} 的每张 raw+hot 表每个 partition 活跃分片数 < {}，期间不派发该组后续增量 INSERT",
        group.as_str(),
        RAW_MERGE_READY_PARTS_PER_PARTITION_LIMIT
    );

    loop {
        let mut summaries = Vec::with_capacity(tables.len());
        let mut ready = true;
        let mut probe_error = None;

        for table in &tables {
            match raw_merge_part_counts(&client, table).await {
                Ok(parts) => {
                    ready &= raw_merge_backlog_is_ready(&parts);
                    summaries.push(raw_merge_backlog_summary(table, &parts));
                }
                Err(err) => {
                    probe_error = Some(err);
                    break;
                }
            }
        }

        if let Some(err) = probe_error {
            warn!(
                "[clickhouse] group MERGE 收敛检查失败 group={}；全量数据保持有效、不会清理，{} 秒后重试：{}",
                group.as_str(),
                RAW_MERGE_READY_POLL_INTERVAL.as_secs(),
                err
            );
            tokio::time::sleep(RAW_MERGE_READY_POLL_INTERVAL).await;
            continue;
        }

        let elapsed_secs = started_at.elapsed().as_secs();
        if ready {
            info!(
                "[clickhouse] group MERGE 收敛完成 group={} elapsed={}s condition=all_tables_all_partitions_parts<{}；允许该组后续增量 INSERT：{}",
                group.as_str(),
                elapsed_secs,
                RAW_MERGE_READY_PARTS_PER_PARTITION_LIMIT,
                summaries.join("; ")
            );
            return Ok(());
        }

        if polls % RAW_MERGE_READY_LOG_EVERY_POLLS == 0 {
            info!(
                "[clickhouse] 等待 group MERGE 收敛 group={} elapsed={}s condition=all_tables_all_partitions_parts<{}；{} 秒后复查：{}",
                group.as_str(),
                elapsed_secs,
                RAW_MERGE_READY_PARTS_PER_PARTITION_LIMIT,
                RAW_MERGE_READY_POLL_INTERVAL.as_secs(),
                summaries.join("; ")
            );
        }
        polls += 1;
        tokio::time::sleep(RAW_MERGE_READY_POLL_INTERVAL).await;
    }
}

/// Refresh the derived serving indexes after the parser has directly inserted
/// hot token-account versions into L2. The hot-mint filter is frozen for the
/// lifetime of a table group, so incrementals only recompute their affected
/// L3 pairs and never consult the global hot-token configuration.
pub(crate) async fn refresh_hot_indexes(
    client: &Client,
    group: TableGroup,
    snapshot_kind: SnapshotKind,
    snapshot_slot: u64,
    affected_pairs: &HashSet<TokenPair>,
    state_is_full_baseline: bool,
) -> Result<()> {
    debug!(
        "[clickhouse] refreshing hot indexes group={} kind={}",
        group.as_str(),
        snapshot_kind.as_str(),
    );
    let state = group.table("hot_token_account_state");
    let info = group.table("hot_token_info");
    let balance = group.table("hot_wallet_token_balance");
    let filter = group.table("hot_token_filter");
    let enabled_hot_tokens = client
        .query(&format!("SELECT count() FROM {filter}"))
        .fetch_one::<u64>()
        .await
        .map_err(|err| format!("failed to count {filter}: {err}"))?;
    info!(
        "[clickhouse] hot state committed; starting derived-index refresh group={} kind={} frozen_hot_tokens={}",
        group.as_str(),
        snapshot_kind.as_str(),
        enabled_hot_tokens
    );
    if matches!(snapshot_kind, SnapshotKind::Incremental) {
        refresh_wallet_balance_incremental(client, &state, &balance, affected_pairs, snapshot_slot)
            .await?;
    } else {
        // Build the ReplacingMergeTree serving table in a temporary clone and
        // exchange it into place, so full refreshes never expose an empty table
        // to readers. The table is keyed by (mint, owner); newer updated_slot
        // rows supersede older balance versions during background merges (or
        // via FINAL/argMax for exact reads).
        let state_source = if state_is_full_baseline {
            state.to_owned()
        } else {
            format!("{state} FINAL")
        };
        let balance_query = format!(
            "SELECT mint, owner, sum(amount) AS amount_raw, max(updated_slot) AS updated_slot \
             FROM {state_source} \
             WHERE is_deleted = 0 AND state != 0 \
             GROUP BY mint, owner \
             HAVING amount_raw > 0"
        );
        rebuild_wallet_balance_table(client, &balance, &balance_query).await?;
    }

    let info_rows = if matches!(snapshot_kind, SnapshotKind::Full) {
        info!(
            "[clickhouse] rebuilding hot_token_info from {} in ordered mint batches size={HOT_TOKEN_INFO_BATCH_SIZE}",
            if state_is_full_baseline {
                "full raw baseline without FINAL"
            } else {
                "existing hot-only raw tables with FINAL"
            }
        );
        rebuild_token_info_table(client, group, &info, state_is_full_baseline).await?;
        client
            .query(&format!("SELECT count() FROM {info}"))
            .fetch_one::<u64>()
            .await
            .map_err(|err| format!("failed to count {info}: {err}"))?
            .to_string()
    } else {
        info!(
            "[clickhouse] incremental hot-index refresh: hot_token_info is static for this table group; skip rebuild"
        );
        "unchanged".to_owned()
    };
    let state_rows = client
        .query(&format!("SELECT count() FROM {state}"))
        .fetch_one::<u64>()
        .await
        .map_err(|err| format!("failed to count {state}: {err}"))?;
    let balance_rows = client
        .query(&format!("SELECT count() FROM {balance}"))
        .fetch_one::<u64>()
        .await
        .map_err(|err| format!("failed to count {balance}: {err}"))?;
    info!(
        "[clickhouse] hot indexes refreshed group={} kind={} enabled_hot_tokens={} state_rows={} wallet_rows={} token_info_rows={}",
        group.as_str(),
        snapshot_kind.as_str(),
        enabled_hot_tokens,
        state_rows,
        balance_rows,
        info_rows
    );
    Ok(())
}

/// Rebuild the L3 balance and token display cache from an already imported
/// direct-write hot state. This cannot reconstruct L2: a failed/corrupt L2
/// needs a fresh full table-group build.
pub(crate) async fn rebuild_derived_indexes_from_state(
    connection_url: &str,
    group: TableGroup,
) -> Result<()> {
    let client = new_clickhouse_client(connection_url)?;
    info!(
        "[clickhouse] rebuilding L3/token-info from existing hot state group={} without snapshot import",
        group.as_str()
    );
    refresh_hot_indexes(
        &client,
        group,
        SnapshotKind::Full,
        0,
        &HashSet::new(),
        false,
    )
    .await
}

/// Build the wallet serving table with external aggregation enabled. L2 is
/// ordered by token-account pubkey rather than wallet/mint, so an exact
/// wallet aggregation can have a large number of live groups. Spilling keeps
/// the query bounded by disk rather than the server-wide RAM cap.
async fn rebuild_wallet_balance_table(
    client: &Client,
    target: &str,
    select_sql: &str,
) -> Result<()> {
    let nonce = format!(
        "{}_{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    );
    let temporary = format!("{target}__build_{nonce}");
    client
        .query(&format!("CREATE TABLE {temporary} AS {target}"))
        .execute()
        .await
        .map_err(|err| format!("failed to create temporary table {temporary}: {err}"))?;
    let result = async {
        client
            .query(&format!("INSERT INTO {temporary} {select_sql}"))
            .with_setting("max_threads", "1")
            .with_setting("max_final_threads", "1")
            .with_setting(
                "max_bytes_before_external_group_by",
                HOT_BALANCE_EXTERNAL_AGGREGATION_BYTES,
            )
            .with_setting(
                "max_bytes_before_external_sort",
                HOT_BALANCE_EXTERNAL_AGGREGATION_BYTES,
            )
            .execute()
            .await
            .map_err(|err| format!("failed to populate temporary table {temporary}: {err}"))?;
        client
            .query(&format!("EXCHANGE TABLES {target} AND {temporary}"))
            .execute()
            .await
            .map_err(|err| format!("failed to exchange rebuilt table {target}: {err}"))?;
        client
            .query(&format!(
                "DROP TABLE {temporary} SETTINGS max_table_size_to_drop = 0"
            ))
            .execute()
            .await
            .map_err(|err| format!("failed to drop old table {temporary}: {err}"))?;
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;
    if result.is_err() {
        let _ = client
            .query(&format!(
                "DROP TABLE IF EXISTS {temporary} SETTINGS max_table_size_to_drop = 0"
            ))
            .execute()
            .await;
    }
    result
}

/// Recompute only wallet/mint pairs touched by an incremental snapshot. The
/// aggregate must still read every current token account belonging to each
/// touched pair; aggregating only the changed pubkeys would lose balances held
/// by their unchanged sibling accounts. Zero rows are retained so a pair that
/// lost its final live account supersedes the previous positive balance.
async fn refresh_wallet_balance_incremental(
    client: &Client,
    state: &str,
    target: &str,
    affected_pairs: &HashSet<TokenPair>,
    snapshot_slot: u64,
) -> Result<()> {
    let started = Instant::now();
    if affected_pairs.is_empty() {
        info!("[clickhouse] incremental wallet refresh: no affected wallet/mint pairs; skip");
        return Ok(());
    }

    // Affected pairs are collected only when the parser writes a row into the
    // frozen group's hot-state table, or when a hot-state tombstone is
    // recovered by pubkey. They are therefore already hot; do not re-check the
    // mutable global hot-token view during an active incremental.
    let mut pairs = affected_pairs.iter().cloned().collect::<Vec<_>>();
    pairs.sort_unstable();

    let pair_literals = pairs
        .iter()
        .map(|(mint, owner)| {
            format!(
                "({}, {})",
                sql_string_literal(mint),
                sql_string_literal(owner)
            )
        })
        .collect::<Vec<_>>();
    let aggregate_sql = format!(
        "SELECT mint, owner, sum(amount) AS amount_raw \
         FROM {state} FINAL \
         WHERE is_deleted = 0 AND state != 0 \
           AND (mint, owner) IN ({}) \
         GROUP BY mint, owner",
        pair_literals.join(", ")
    );
    log_query_scan_estimate(
        client,
        "incremental_wallet_pair_aggregation",
        &aggregate_sql,
    )
    .await;
    let aggregate_rows = client
        .query(&aggregate_sql)
        .with_setting("max_query_size", HOT_PAIR_QUERY_MAX_QUERY_SIZE)
        .with_setting(
            "max_bytes_before_external_group_by",
            HOT_BALANCE_EXTERNAL_AGGREGATION_BYTES,
        )
        .with_setting(
            "max_bytes_before_external_sort",
            HOT_BALANCE_EXTERNAL_AGGREGATION_BYTES,
        )
        .fetch_all::<WalletBalanceAggregateRow>()
        .await
        .map_err(|err| format!("failed to aggregate affected wallet/mint pairs: {err}"))?;
    let aggregates = aggregate_rows
        .into_iter()
        .map(|row| ((row.mint, row.owner), row.amount_raw))
        .collect::<HashMap<_, _>>();

    let mut insert = new_inserter::<WalletBalanceRow>(client, target);
    for (mint, owner) in &pairs {
        insert
            .write(&WalletBalanceRow {
                mint: mint.clone(),
                owner: owner.clone(),
                amount_raw: aggregates
                    .get(&(mint.clone(), owner.clone()))
                    .copied()
                    .unwrap_or(0),
                updated_slot: snapshot_slot,
            })
            .await
            .map_err(|err| format!("failed to write incremental wallet balance: {err}"))?;
    }
    insert
        .end()
        .await
        .map_err(|err| format!("failed to commit incremental wallet balance: {err}"))?;
    info!(
        "[clickhouse] incremental wallet refresh complete affected_pairs={} aggregate_rows={} snapshot_slot={} elapsed={:?}",
        pairs.len(),
        aggregates.len(),
        snapshot_slot,
        started.elapsed()
    );
    Ok(())
}

#[derive(Row, Deserialize, Serialize)]
struct HotMintRow {
    mint: String,
}

/// Rebuild token display information from the group's frozen mint filter and
/// the corresponding hot-only raw mint/metadata tables. The filter is read
/// in sorted batches so each JOIN remains bounded; the temporary table is
/// exchanged only once all batches have succeeded.
async fn rebuild_token_info_table(
    client: &Client,
    group: TableGroup,
    target: &str,
    raw_is_full_baseline: bool,
) -> Result<()> {
    let nonce = format!(
        "{}_{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    );
    let temporary = format!("{target}__build_{nonce}");
    client
        .query(&format!("CREATE TABLE {temporary} AS {target}"))
        .execute()
        .await
        .map_err(|err| format!("failed to create temporary table {temporary}: {err}"))?;

    let mint_table = group.table(TOKEN_MINT_TABLE);
    let metadata_table = group.table(TOKEN_METADATA_TABLE);
    let filter_table = group.table("hot_token_filter");
    // A cold full load has one canonical source row per mint, so it avoids
    // FINAL. The manual derived-table rebuild can run after incrementals, in
    // which case the hot-only raw tables may have multiple versions and need
    // FINAL for a deterministic display cache.
    let mint_source = if raw_is_full_baseline {
        mint_table
    } else {
        format!("{mint_table} FINAL")
    };
    let metadata_source = if raw_is_full_baseline {
        metadata_table
    } else {
        format!("{metadata_table} FINAL")
    };

    let result = async {
        let mut previous_last_mint: Option<String> = None;
        let mut batch_count = 0_u64;
        let mut inserted_tokens = 0_u64;
        loop {
            let resume_predicate = previous_last_mint.as_ref().map_or_else(String::new, |mint| {
                format!("WHERE mint > {}", sql_string_literal(mint))
            });
            let mint_batch_sql = format!(
                "SELECT mint FROM {filter_table} {resume_predicate} ORDER BY mint LIMIT {HOT_TOKEN_INFO_BATCH_SIZE}"
            );
            let mints = client
                .query(&mint_batch_sql)
                .fetch_all::<HotMintRow>()
                .await
                .map_err(|err| format!("failed to read hot-token info batch: {err}"))?;
            let Some(first) = mints.first() else {
                break;
            };
            let last = mints
                .last()
                .expect("nonempty hot-token batch has a last mint");
            let lower = sql_string_literal(&first.mint);
            let upper = sql_string_literal(&last.mint);
            let range = format!("mint >= {lower} AND mint <= {upper}");
            let insert_sql = format!(
                "INSERT INTO {temporary} \
                 SELECT h.mint, ifNull(m.decimals, 0), ifNull(m.supply, 0), ifNull(md.name, ''), ifNull(md.symbol, ''), ifNull(md.uri, ''), md.token_standard, \
                        ifNull(m.updated_slot, 0), ifNull(md.updated_slot, 0), greatest(ifNull(m.updated_slot, 0), ifNull(md.updated_slot, 0)) \
                 FROM (SELECT mint FROM {filter_table} WHERE {range}) AS h \
                 LEFT ANY JOIN (SELECT mint, decimals, supply, updated_slot FROM {mint_source} WHERE {range}) AS m USING (mint) \
                 LEFT ANY JOIN (SELECT mint, name, symbol, uri, token_standard, updated_slot FROM {metadata_source} WHERE {range}) AS md USING (mint)"
            );
            client
                .query(&insert_sql)
                .with_setting("max_threads", "1")
                .execute()
                .await
                .map_err(|err| {
                    format!(
                        "failed to populate temporary table {temporary} in hot-token info batch {} (range {} .. {}): {err}",
                        batch_count + 1,
                        first.mint,
                        last.mint
                    )
                })?;
            previous_last_mint = Some(last.mint.clone());
            batch_count += 1;
            inserted_tokens += mints.len() as u64;
            if batch_count == 1 || batch_count % 25 == 0 {
                info!(
                    "[clickhouse] hot_token_info build group={} batches={} tokens={} last_mint={}",
                    group.as_str(),
                    batch_count,
                    inserted_tokens,
                    last.mint
                );
            }
        }
        info!(
            "[clickhouse] hot_token_info temporary build complete group={} batches={} tokens={}",
            group.as_str(),
            batch_count,
            inserted_tokens
        );
        client
            .query(&format!("EXCHANGE TABLES {target} AND {temporary}"))
            .execute()
            .await
            .map_err(|err| format!("failed to exchange rebuilt table {target}: {err}"))?;
        client
            .query(&format!(
                "DROP TABLE {temporary} SETTINGS max_table_size_to_drop = 0"
            ))
            .execute()
            .await
            .map_err(|err| format!("failed to drop old table {temporary}: {err}"))?;
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;
    if result.is_err() {
        let _ = client
            .query(&format!(
                "DROP TABLE IF EXISTS {temporary} SETTINGS max_table_size_to_drop = 0"
            ))
            .execute()
            .await;
    }
    result
}

pub(crate) async fn exchange_table_groups(connection_url: &str) -> Result<()> {
    let client = new_clickhouse_client(connection_url)?;
    let pairs = [
        ("raw_account", "raw_account_bak"),
        ("raw_token_mint", "raw_token_mint_bak"),
        ("raw_token_metadata", "raw_token_metadata_bak"),
        ("hot_token_filter", "hot_token_filter_bak"),
        ("hot_token_account_state", "hot_token_account_state_bak"),
        ("hot_token_info", "hot_token_info_bak"),
        ("hot_wallet_token_balance", "hot_wallet_token_balance_bak"),
    ];
    for (left, right) in pairs {
        client
            .query(&format!("EXCHANGE TABLES {left} AND {right}"))
            .execute()
            .await
            .map_err(|err| format!("failed to exchange {left} and {right}: {err}"))?;
    }
    Ok(())
}

/// Validate that the staging generation contains a self-consistent frozen
/// mint set and token-info cache before it becomes the serving group.
///
/// The active and staging groups deliberately may use different frozen mint
/// sets, because a new full snapshot is the only point at which configuration
/// changes take effect. Comparing their L2/L3 row counts or balances would
/// therefore reject a valid switch.
pub(crate) async fn validate_staging_group(connection_url: &str) -> Result<()> {
    let client = new_clickhouse_client(connection_url)?;
    let metric = async |table: &str, expression: &str| -> Result<u64> {
        client
            .query(&format!("SELECT {expression} FROM {table}"))
            .fetch_one::<u64>()
            .await
            .map_err(Into::into)
    };

    let filter_rows = metric("hot_token_filter_bak", "count()").await?;
    if filter_rows == 0 {
        return Err("staging validation failed: hot_token_filter_bak is empty".into());
    }
    let info_rows = metric("hot_token_info_bak", "count()").await?;
    if info_rows != filter_rows {
        return Err(format!(
            "staging validation failed: hot_token_info_bak rows={info_rows}, but frozen filter rows={filter_rows}"
        )
        .into());
    }

    let state_rows = metric("hot_token_account_state_bak", "count()").await?;
    let balance_rows = metric("hot_wallet_token_balance_bak", "count()").await?;
    info!(
        "[clickhouse] staging generation validation passed frozen_hot_tokens={} token_info_rows={} state_rows={} wallet_rows={}",
        filter_rows, info_rows, state_rows, balance_rows
    );
    Ok(())
}

pub(crate) async fn record_index_control(
    connection_url: &str,
    active_group: u8,
    ready_slot: u64,
    hot_token_version: u64,
) -> Result<()> {
    let client = new_clickhouse_client(connection_url)?;
    let generation = client
        .query("SELECT coalesce(max(generation), toUInt64(0)) + 1 FROM hot_index_control FINAL")
        .fetch_one::<u64>()
        .await
        .map_err(|err| format!("failed to read hot_index_control generation: {err}"))?;
    let sql = format!(
        "INSERT INTO hot_index_control (control_key, active_group, generation, ready_slot, hot_token_version) VALUES ('default', {active_group}, {generation}, {ready_slot}, {hot_token_version})"
    );
    client
        .query(&sql)
        .execute()
        .await
        .map_err(|err| format!("failed to record hot_index_control: {err}"))?;
    Ok(())
}

pub(crate) async fn active_group_id(connection_url: &str) -> Result<u8> {
    new_clickhouse_client(connection_url)?
        .query(
            "SELECT if(count() = 0, toUInt8(1), argMax(active_group, generation)) FROM hot_index_control FINAL",
        )
        .fetch_one::<u8>()
        .await
        .map_err(Into::into)
}

#[derive(Row, Deserialize)]
struct SystemTableRow {
    name: String,
    engine: String,
    sorting_key: String,
    engine_full: String,
    create_table_query: String,
}

#[derive(Row, Deserialize)]
struct SystemColumnRow {
    table: String,
    name: String,
    r#type: String,
}

#[derive(Row, Deserialize)]
struct SystemProjectionRow {
    table: String,
    name: String,
    sorting_key: String,
}

/// Validate the schema required by the dual-buffer importer before any
/// snapshot bytes are read.  The active tables use their stable names and the
/// staging tables use the `_bak` suffix; both groups must have identical
/// schemas because either group can become active after EXCHANGE TABLES.
pub(crate) async fn validate_clickhouse_schema(connection_url: &str) -> Result<()> {
    let client = new_clickhouse_client(connection_url)?;
    let required_tables = required_table_names();
    let table_list = sql_string_list(&required_tables);
    let table_rows = client
        .query(&format!(
            "SELECT name, engine, sorting_key, engine_full, create_table_query FROM system.tables WHERE database = '{DATABASE}' AND name IN ({table_list})"
        ))
        .fetch_all::<SystemTableRow>()
        .await
        .map_err(|err| format!("ClickHouse schema check failed while reading system.tables: {err}"))?;

    let mut table_definitions = HashMap::with_capacity(table_rows.len());
    for row in table_rows {
        table_definitions.insert(row.name.clone(), row);
    }

    let mut errors = Vec::new();
    for spec in required_table_specs() {
        match table_definitions.get(spec.name) {
            None => errors.push(format!("missing table solana.{}", spec.name)),
            Some(definition) => {
                if definition.engine != spec.engine {
                    errors.push(format!(
                        "table solana.{} uses engine {}, expected {}",
                        spec.name, definition.engine, spec.engine
                    ));
                }
                if !spec.engine_full_prefix.is_empty()
                    && !definition.engine_full.starts_with(spec.engine_full_prefix)
                {
                    errors.push(format!(
                        "table solana.{} uses engine definition {}, expected prefix {}",
                        spec.name, definition.engine_full, spec.engine_full_prefix
                    ));
                }
                if !spec.sorting_key.is_empty()
                    && normalize_sorting_key(&definition.sorting_key)
                        != normalize_sorting_key(spec.sorting_key)
                {
                    errors.push(format!(
                        "table solana.{} uses sorting key {}, expected {}",
                        spec.name, definition.sorting_key, spec.sorting_key
                    ));
                }
                if requires_rebuild_projection_mode(spec.name)
                    && !table_has_rebuild_projection_mode(&definition.create_table_query)
                {
                    errors.push(format!(
                        "table solana.{} must set deduplicate_merge_projection_mode = 'rebuild'",
                        spec.name
                    ));
                }
            }
        }
    }

    let column_rows = client
        .query(&format!(
            "SELECT table, name, type FROM system.columns WHERE database = '{DATABASE}' AND table IN ({table_list})"
        ))
        .fetch_all::<SystemColumnRow>()
        .await
        .map_err(|err| format!("ClickHouse schema check failed while reading system.columns: {err}"))?;
    let columns = column_rows
        .into_iter()
        .map(|row| ((row.table, row.name), row.r#type))
        .collect::<HashMap<_, _>>();

    for (table, column, expected_type) in required_columns() {
        let actual_table = table.to_owned();
        let Some(actual_type) = columns.get(&(actual_table.clone(), column.to_owned())) else {
            errors.push(format!("missing column solana.{actual_table}.{column}"));
            continue;
        };
        if !actual_type_matches(actual_type, expected_type) {
            errors.push(format!(
                "column solana.{actual_table}.{column} uses type {actual_type}, expected {expected_type}"
            ));
        }
    }

    let projection_rows = client
        .query(&format!(
            // `system.projections.sorting_key` is Nullable/String-like across
            // ClickHouse versions.  Force a non-null String in the result so
            // clickhouse-rs RowBinary decoding remains stable.
            "SELECT table, name, toString(sorting_key) AS sorting_key FROM system.projections WHERE database = '{DATABASE}' AND table IN ({table_list})"
        ))
        .fetch_all::<SystemProjectionRow>()
        .await
        .map_err(|err| {
            format!(
                "ClickHouse schema check failed while reading system.projections: {err}"
            )
        })?;
    let projections = projection_rows
        .into_iter()
        .map(|row| ((row.table, row.name), row.sorting_key))
        .collect::<HashMap<_, _>>();
    for table in ["hot_wallet_token_balance", "hot_wallet_token_balance_bak"] {
        for (projection, expected_sorting_key) in [
            ("proj_by_mint_amount", "mint, amount_raw, owner"),
            ("proj_by_owner", "owner, mint"),
        ] {
            let key = (table.to_owned(), projection.to_owned());
            match projections.get(&key) {
                None => errors.push(format!("missing projection solana.{table}.{projection}")),
                Some(actual_sorting_key)
                    if normalize_sorting_key(actual_sorting_key)
                        != normalize_sorting_key(expected_sorting_key) =>
                {
                    errors.push(format!(
                        "projection solana.{table}.{projection} uses sorting key {actual_sorting_key}, expected {expected_sorting_key}"
                    ));
                }
                Some(_) => {}
            }
        }
    }

    if errors.is_empty() {
        info!(
            "[clickhouse] schema validation passed: {} tables, required columns, wallet projections, and projection merge settings",
            required_tables.len()
        );
        debug!(
            "[clickhouse] Schema validation passed: {} tables, required columns, wallet projections, and projection merge settings",
            required_tables.len()
        );
        Ok(())
    } else {
        errors.sort_unstable();
        Err(format!(
            "ClickHouse schema validation failed; refusing to process snapshots:\n{}",
            errors
                .into_iter()
                .map(|error| format!("  - {error}"))
                .collect::<Vec<_>>()
                .join("\n")
        )
        .into())
    }
}

fn required_table_names() -> Vec<String> {
    required_table_specs()
        .into_iter()
        .map(|spec| spec.name.to_owned())
        .collect()
}

#[derive(Clone, Copy)]
struct RequiredTableSpec {
    name: &'static str,
    engine: &'static str,
    engine_full_prefix: &'static str,
    sorting_key: &'static str,
}

fn requires_rebuild_projection_mode(table: &str) -> bool {
    matches!(
        table,
        "hot_wallet_token_balance" | "hot_wallet_token_balance_bak"
    )
}

fn table_has_rebuild_projection_mode(create_table_query: &str) -> bool {
    let normalized = create_table_query
        .to_ascii_lowercase()
        .chars()
        .filter(|ch| !ch.is_ascii_whitespace())
        .collect::<String>();
    normalized.contains("deduplicate_merge_projection_mode='rebuild'")
}

fn normalize_sorting_key(value: &str) -> String {
    value
        .chars()
        .filter(|ch| {
            !ch.is_ascii_whitespace()
                && *ch != '('
                && *ch != ')'
                && *ch != '['
                && *ch != ']'
                && *ch != '\''
                && *ch != '"'
        })
        .collect()
}

fn required_table_specs() -> Vec<RequiredTableSpec> {
    vec![
        RequiredTableSpec {
            name: "raw_account",
            engine: "ReplacingMergeTree",
            engine_full_prefix: "ReplacingMergeTree(updated_slot)",
            sorting_key: "owner, pubkey",
        },
        RequiredTableSpec {
            name: "raw_token_mint",
            engine: "ReplacingMergeTree",
            engine_full_prefix: "ReplacingMergeTree(updated_slot)",
            sorting_key: "mint",
        },
        RequiredTableSpec {
            name: "raw_token_metadata",
            engine: "ReplacingMergeTree",
            engine_full_prefix: "ReplacingMergeTree(updated_slot)",
            sorting_key: "mint",
        },
        RequiredTableSpec {
            name: "raw_account_bak",
            engine: "ReplacingMergeTree",
            engine_full_prefix: "ReplacingMergeTree(updated_slot)",
            sorting_key: "owner, pubkey",
        },
        RequiredTableSpec {
            name: "raw_token_mint_bak",
            engine: "ReplacingMergeTree",
            engine_full_prefix: "ReplacingMergeTree(updated_slot)",
            sorting_key: "mint",
        },
        RequiredTableSpec {
            name: "raw_token_metadata_bak",
            engine: "ReplacingMergeTree",
            engine_full_prefix: "ReplacingMergeTree(updated_slot)",
            sorting_key: "mint",
        },
        RequiredTableSpec {
            name: "hot_token",
            engine: "ReplacingMergeTree",
            engine_full_prefix: "ReplacingMergeTree(version)",
            sorting_key: "mint",
        },
        RequiredTableSpec {
            name: "hot_token_enabled",
            engine: "View",
            // ClickHouse reports an empty engine_full for a regular View.
            engine_full_prefix: "",
            sorting_key: "",
        },
        RequiredTableSpec {
            name: "hot_token_filter",
            engine: "MergeTree",
            engine_full_prefix: "",
            sorting_key: "mint",
        },
        RequiredTableSpec {
            name: "hot_token_filter_bak",
            engine: "MergeTree",
            engine_full_prefix: "",
            sorting_key: "mint",
        },
        RequiredTableSpec {
            name: "hot_index_control",
            engine: "ReplacingMergeTree",
            engine_full_prefix: "ReplacingMergeTree(generation)",
            sorting_key: "control_key",
        },
        RequiredTableSpec {
            name: "hot_token_account_state",
            engine: "ReplacingMergeTree",
            engine_full_prefix: "ReplacingMergeTree(updated_slot, is_deleted)",
            sorting_key: "pubkey",
        },
        RequiredTableSpec {
            name: "hot_token_account_state_bak",
            engine: "ReplacingMergeTree",
            engine_full_prefix: "ReplacingMergeTree(updated_slot, is_deleted)",
            sorting_key: "pubkey",
        },
        RequiredTableSpec {
            name: "hot_wallet_token_balance",
            engine: "ReplacingMergeTree",
            engine_full_prefix: "ReplacingMergeTree(updated_slot)",
            sorting_key: "mint, owner",
        },
        RequiredTableSpec {
            name: "hot_wallet_token_balance_bak",
            engine: "ReplacingMergeTree",
            engine_full_prefix: "ReplacingMergeTree(updated_slot)",
            sorting_key: "mint, owner",
        },
        RequiredTableSpec {
            name: "hot_token_info",
            engine: "ReplacingMergeTree",
            engine_full_prefix: "ReplacingMergeTree(updated_slot)",
            sorting_key: "mint",
        },
        RequiredTableSpec {
            name: "hot_token_info_bak",
            engine: "ReplacingMergeTree",
            engine_full_prefix: "ReplacingMergeTree(updated_slot)",
            sorting_key: "mint",
        },
    ]
}

fn required_columns() -> Vec<(&'static str, &'static str, &'static str)> {
    const RAW_ACCOUNT: &[(&str, &str)] = &[
        ("pubkey", "String"),
        ("owner", "LowCardinality(String)"),
        ("lamports", "UInt64"),
        ("data_len", "UInt64"),
        ("executable", "Bool"),
        ("updated_slot", "UInt64"),
    ];
    const RAW_TOKEN_MINT: &[(&str, &str)] = &[
        ("mint", "String"),
        ("mint_authority", "Nullable(String)"),
        ("supply", "UInt64"),
        ("decimals", "UInt8"),
        ("is_initialized", "Bool"),
        ("freeze_authority", "Nullable(String)"),
        ("updated_slot", "UInt64"),
    ];
    const RAW_TOKEN_METADATA: &[(&str, &str)] = &[
        ("mint", "String"),
        ("name", "String"),
        ("symbol", "String"),
        ("uri", "String"),
        ("update_authority", "LowCardinality(String)"),
        ("is_mutable", "Bool"),
        ("token_standard", "Nullable(UInt8)"),
        ("seller_fee_basis_points", "UInt16"),
        ("creators", "Array(String)"),
        ("updated_slot", "UInt64"),
    ];
    const HOT_TOKEN: &[(&str, &str)] = &[
        ("mint", "String"),
        ("enable", "UInt8"),
        ("version", "UInt64"),
    ];
    const HOT_INDEX_CONTROL: &[(&str, &str)] = &[
        ("control_key", "LowCardinality(String)"),
        ("active_group", "UInt8"),
        ("generation", "UInt64"),
        ("ready_slot", "UInt64"),
        ("hot_token_version", "UInt64"),
        ("updated_at", "DateTime64"),
    ];
    const HOT_TOKEN_ACCOUNT_STATE: &[(&str, &str)] = &[
        ("pubkey", "String"),
        ("mint", "String"),
        ("owner", "String"),
        ("amount", "UInt64"),
        ("delegate", "Nullable(String)"),
        ("delegated_amount", "UInt64"),
        ("state", "Enum8"),
        ("close_authority", "Nullable(String)"),
        ("is_deleted", "UInt8"),
        ("updated_slot", "UInt64"),
    ];
    const HOT_WALLET_TOKEN_BALANCE: &[(&str, &str)] = &[
        ("mint", "String"),
        ("owner", "String"),
        ("amount_raw", "UInt64"),
        ("updated_slot", "UInt64"),
    ];
    const HOT_TOKEN_INFO: &[(&str, &str)] = &[
        ("mint", "String"),
        ("decimals", "UInt8"),
        ("supply_raw", "UInt64"),
        ("name", "String"),
        ("symbol", "String"),
        ("uri", "String"),
        ("token_standard", "Nullable(UInt8)"),
        ("mint_updated_slot", "UInt64"),
        ("metadata_updated_slot", "UInt64"),
        ("updated_slot", "UInt64"),
    ];

    let mut required = Vec::new();
    for table in ["raw_account", "raw_account_bak"] {
        required.extend(
            RAW_ACCOUNT
                .iter()
                .map(|(column, expected)| (table, *column, *expected)),
        );
    }
    for table in ["raw_token_mint", "raw_token_mint_bak"] {
        required.extend(
            RAW_TOKEN_MINT
                .iter()
                .map(|(column, expected)| (table, *column, *expected)),
        );
    }
    for table in ["raw_token_metadata", "raw_token_metadata_bak"] {
        required.extend(
            RAW_TOKEN_METADATA
                .iter()
                .map(|(column, expected)| (table, *column, *expected)),
        );
    }
    required.extend(
        HOT_TOKEN
            .iter()
            .map(|(column, expected)| ("hot_token", *column, *expected)),
    );
    required.extend(
        HOT_INDEX_CONTROL
            .iter()
            .map(|(column, expected)| ("hot_index_control", *column, *expected)),
    );
    for table in ["hot_token_account_state", "hot_token_account_state_bak"] {
        required.extend(
            HOT_TOKEN_ACCOUNT_STATE
                .iter()
                .map(|(column, expected)| (table, *column, *expected)),
        );
    }
    for table in ["hot_token_filter", "hot_token_filter_bak"] {
        required.push((table, "mint", "String"));
    }
    for table in ["hot_wallet_token_balance", "hot_wallet_token_balance_bak"] {
        required.extend(
            HOT_WALLET_TOKEN_BALANCE
                .iter()
                .map(|(column, expected)| (table, *column, *expected)),
        );
    }
    for table in ["hot_token_info", "hot_token_info_bak"] {
        required.extend(
            HOT_TOKEN_INFO
                .iter()
                .map(|(column, expected)| (table, *column, *expected)),
        );
    }
    required.extend([
        ("hot_token_enabled", "mint", "String"),
        ("hot_token_enabled", "version", "UInt64"),
    ]);
    required
}

fn actual_type_matches(actual: &str, expected: &str) -> bool {
    if expected == "Enum8" {
        actual.starts_with("Enum8")
    } else if expected == "DateTime64" {
        actual.starts_with("DateTime64")
    } else {
        actual == expected
    }
}

fn sql_string_list(values: &[String]) -> String {
    values
        .iter()
        .map(|value| sql_string_literal(value))
        .collect::<Vec<_>>()
        .join(", ")
}

fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn parse_clickhouse_connection_url(connection_url: &str) -> Result<ClickhouseConnection> {
    let mut endpoint = Url::parse(connection_url)?;
    let has_userinfo = !endpoint.username().is_empty() || endpoint.password().is_some();
    let user = (!endpoint.username().is_empty())
        .then(|| decode_url_component(endpoint.username()))
        .transpose()?;
    let password = endpoint.password().map(decode_url_component).transpose()?;

    if has_userinfo {
        endpoint.set_username("").map_err(|_| {
            std::io::Error::other("CLICKHOUSE_URL has invalid username information")
        })?;
        endpoint.set_password(None).map_err(|_| {
            std::io::Error::other("CLICKHOUSE_URL has invalid password information")
        })?;
    }

    Ok(ClickhouseConnection {
        endpoint: endpoint.to_string(),
        user,
        password,
    })
}

fn decode_url_component(value: &str) -> Result<String> {
    Ok(percent_decode_str(value).decode_utf8()?.into_owned())
}

struct ClickhouseSink {
    worker_name: String,
    insert_gate: Option<Arc<Semaphore>>,
    account_table: String,
    token_account_table: String,
    token_mint_table: String,
    token_metadata_table: String,
    account: Inserter<AccountRow>,
    token_account: Inserter<TokenAccountRow>,
    token_mint: Inserter<TokenMintRow>,
    token_metadata: Inserter<TokenMetadataRow>,
    account_rows_since_commit_check: u16,
    token_account_rows_since_commit_check: u16,
    token_mint_rows_since_commit_check: u16,
    token_metadata_rows_since_commit_check: u16,
    flush_check_counter: u16,
    account_opened_at: Option<Instant>,
    token_account_opened_at: Option<Instant>,
    token_mint_opened_at: Option<Instant>,
    token_metadata_opened_at: Option<Instant>,
}

impl ClickhouseSink {
    fn new(
        client: &Client,
        worker_name: impl Into<String>,
        insert_gate: Option<Arc<Semaphore>>,
        group: TableGroup,
    ) -> Self {
        let account_table = group.table(ACCOUNT_TABLE);
        let token_account_table = group.table("hot_token_account_state");
        let token_mint_table = group.table(TOKEN_MINT_TABLE);
        let token_metadata_table = group.table(TOKEN_METADATA_TABLE);
        Self {
            worker_name: worker_name.into(),
            insert_gate,
            account: new_inserter(client, &account_table),
            token_account: new_inserter(client, &token_account_table),
            token_mint: new_inserter(client, &token_mint_table),
            token_metadata: new_inserter(client, &token_metadata_table),
            account_table,
            token_account_table,
            token_mint_table,
            token_metadata_table,
            account_rows_since_commit_check: 0,
            token_account_rows_since_commit_check: 0,
            token_mint_rows_since_commit_check: 0,
            token_metadata_rows_since_commit_check: 0,
            flush_check_counter: 0,
            account_opened_at: None,
            token_account_opened_at: None,
            token_mint_opened_at: None,
            token_metadata_opened_at: None,
        }
    }

    async fn write_account(&mut self, row: &AccountRow) -> Result<()> {
        if self.account.pending().rows == 0 {
            debug!(
                "[clickhouse] {} writing table={} phase=start",
                self.worker_name, self.account_table
            );
            self.account_opened_at = Some(Instant::now());
        }
        self.account.write(row).await?;
        check_batch_limit(
            &self.worker_name,
            &self.account_table,
            &mut self.account,
            &mut self.account_rows_since_commit_check,
            self.insert_gate.as_ref(),
        )
        .await?;
        self.clear_open_timestamp_if_empty();
        Ok(())
    }

    async fn write_token_account(&mut self, row: &TokenAccountRow) -> Result<()> {
        if self.token_account.pending().rows == 0 {
            debug!(
                "[clickhouse] {} writing table={} phase=start",
                self.worker_name, self.token_account_table
            );
            self.token_account_opened_at = Some(Instant::now());
        }
        self.token_account.write(row).await?;
        check_batch_limit(
            &self.worker_name,
            &self.token_account_table,
            &mut self.token_account,
            &mut self.token_account_rows_since_commit_check,
            self.insert_gate.as_ref(),
        )
        .await?;
        self.clear_open_timestamp_if_empty();
        Ok(())
    }

    async fn write_token_mint(&mut self, row: &TokenMintRow) -> Result<()> {
        if self.token_mint.pending().rows == 0 {
            debug!(
                "[clickhouse] {} writing table={} phase=start",
                self.worker_name, self.token_mint_table
            );
            self.token_mint_opened_at = Some(Instant::now());
        }
        self.token_mint.write(row).await?;
        check_batch_limit(
            &self.worker_name,
            &self.token_mint_table,
            &mut self.token_mint,
            &mut self.token_mint_rows_since_commit_check,
            self.insert_gate.as_ref(),
        )
        .await?;
        self.clear_open_timestamp_if_empty();
        Ok(())
    }

    async fn write_token_metadata(&mut self, row: &TokenMetadataRow) -> Result<()> {
        if self.token_metadata.pending().rows == 0 {
            debug!(
                "[clickhouse] {} writing table={} phase=start",
                self.worker_name, self.token_metadata_table
            );
            self.token_metadata_opened_at = Some(Instant::now());
        }
        self.token_metadata.write(row).await?;
        check_batch_limit(
            &self.worker_name,
            &self.token_metadata_table,
            &mut self.token_metadata,
            &mut self.token_metadata_rows_since_commit_check,
            self.insert_gate.as_ref(),
        )
        .await?;
        self.clear_open_timestamp_if_empty();
        Ok(())
    }

    async fn maybe_force_commit(&mut self) -> Result<()> {
        self.flush_check_counter += 1;
        if self.flush_check_counter != FLUSH_CHECK_INTERVAL {
            return Ok(());
        }
        self.flush_check_counter = 0;

        let now = Instant::now();
        force_aged_inserter(
            &self.worker_name,
            &self.account_table,
            &mut self.account,
            &mut self.account_opened_at,
            now,
            self.insert_gate.as_ref(),
        )
        .await?;
        force_aged_inserter(
            &self.worker_name,
            &self.token_account_table,
            &mut self.token_account,
            &mut self.token_account_opened_at,
            now,
            self.insert_gate.as_ref(),
        )
        .await?;
        force_aged_inserter(
            &self.worker_name,
            &self.token_mint_table,
            &mut self.token_mint,
            &mut self.token_mint_opened_at,
            now,
            self.insert_gate.as_ref(),
        )
        .await?;
        force_aged_inserter(
            &self.worker_name,
            &self.token_metadata_table,
            &mut self.token_metadata,
            &mut self.token_metadata_opened_at,
            now,
            self.insert_gate.as_ref(),
        )
        .await?;
        Ok(())
    }

    async fn force_commit_all(&mut self) -> Result<()> {
        if !self.has_pending_rows() {
            return Ok(());
        }
        let started = Instant::now();
        debug!(
            "[clickhouse] {} INSERT flush begin: raw_account={} rows, hot_token_account_state={} rows, raw_token_mint={} rows, raw_token_metadata={} rows",
            self.worker_name,
            self.account.pending().rows,
            self.token_account.pending().rows,
            self.token_mint.pending().rows,
            self.token_metadata.pending().rows,
        );
        // Finish one table at a time here.  This path is used when the archive
        // queue is empty, so there is no parsing work to overlap.  Starting
        // four finalizations together would only create a burst of storage
        // work at exactly the point where the producer is already starved.
        let gate = self.insert_gate.as_ref();
        let account = force_commit_with_gate(
            gate,
            &self.worker_name,
            &self.account_table,
            &mut self.account,
        )
        .await?;
        let token_account = force_commit_with_gate(
            gate,
            &self.worker_name,
            &self.token_account_table,
            &mut self.token_account,
        )
        .await?;
        let token_mint = force_commit_with_gate(
            gate,
            &self.worker_name,
            &self.token_mint_table,
            &mut self.token_mint,
        )
        .await?;
        let token_metadata = force_commit_with_gate(
            gate,
            &self.worker_name,
            &self.token_metadata_table,
            &mut self.token_metadata,
        )
        .await?;
        log_insert_commit(
            &self.worker_name,
            &self.account_table,
            "idle flush",
            &account,
        );
        log_insert_commit(
            &self.worker_name,
            &self.token_account_table,
            "idle flush",
            &token_account,
        );
        log_insert_commit(
            &self.worker_name,
            &self.token_mint_table,
            "idle flush",
            &token_mint,
        );
        log_insert_commit(
            &self.worker_name,
            &self.token_metadata_table,
            "idle flush",
            &token_metadata,
        );
        self.account_opened_at = None;
        self.token_account_opened_at = None;
        self.token_mint_opened_at = None;
        self.token_metadata_opened_at = None;
        if started.elapsed() >= Duration::from_secs(5) {
            warn!(
                "[clickhouse] Flushing open inserts took {:?}",
                started.elapsed()
            );
        }
        Ok(())
    }

    fn clear_open_timestamp_if_empty(&mut self) {
        if self.account.pending().rows == 0 {
            self.account_opened_at = None;
        }
        if self.token_account.pending().rows == 0 {
            self.token_account_opened_at = None;
        }
        if self.token_mint.pending().rows == 0 {
            self.token_mint_opened_at = None;
        }
        if self.token_metadata.pending().rows == 0 {
            self.token_metadata_opened_at = None;
        }
    }

    fn has_pending_rows(&self) -> bool {
        self.account.pending().rows > 0
            || self.token_account.pending().rows > 0
            || self.token_mint.pending().rows > 0
            || self.token_metadata.pending().rows > 0
    }

    async fn end(self) -> Result<()> {
        let worker_name = self.worker_name;
        debug!("[clickhouse] {worker_name} final INSERT flush begin");
        let gate = self.insert_gate.as_ref();
        let account = end_with_gate(gate, &worker_name, &self.account_table, self.account).await?;
        let token_account = end_with_gate(
            gate,
            &worker_name,
            &self.token_account_table,
            self.token_account,
        )
        .await?;
        let token_mint =
            end_with_gate(gate, &worker_name, &self.token_mint_table, self.token_mint).await?;
        let token_metadata = end_with_gate(
            gate,
            &worker_name,
            &self.token_metadata_table,
            self.token_metadata,
        )
        .await?;
        log_insert_commit(&worker_name, &self.account_table, "final flush", &account);
        log_insert_commit(
            &worker_name,
            &self.token_account_table,
            "final flush",
            &token_account,
        );
        log_insert_commit(
            &worker_name,
            &self.token_mint_table,
            "final flush",
            &token_mint,
        );
        log_insert_commit(
            &worker_name,
            &self.token_metadata_table,
            "final flush",
            &token_metadata,
        );
        Ok(())
    }
}

async fn acquire_insert_permit(
    gate: Option<&Arc<Semaphore>>,
    worker_name: &str,
    table: &str,
) -> Result<Option<OwnedSemaphorePermit>> {
    let Some(gate) = gate else {
        return Ok(None);
    };

    let started = Instant::now();
    debug!("[clickhouse] {worker_name} INSERT table={table} waiting for finalization slot");
    let permit = Arc::clone(gate).acquire_owned().await?;
    let waited = started.elapsed();
    if waited >= Duration::from_secs(1) {
        debug!(
            "[clickhouse] {worker_name} INSERT table={table} acquired finalization slot after {waited:?}"
        );
    }
    Ok(Some(permit))
}

async fn force_commit_with_gate<T: Row>(
    gate: Option<&Arc<Semaphore>>,
    worker_name: &str,
    table: &str,
    inserter: &mut Inserter<T>,
) -> Result<Quantities> {
    let pending = inserter.pending().clone();
    if pending.rows == 0 {
        return Ok(Quantities::ZERO);
    }
    let _permit = acquire_insert_permit(gate, worker_name, table).await?;
    let started = Instant::now();
    let result = inserter.force_commit().await;
    if let Err(err) = &result {
        error!(
            "[clickhouse] {worker_name} INSERT table={table} finalization failed after {:?}: {err}; debug={err:?}; pending_rows={} pending_bytes={}",
            started.elapsed(),
            pending.rows,
            pending.bytes,
        );
    }
    if started.elapsed() >= Duration::from_secs(5) {
        debug!(
            "[clickhouse] {worker_name} INSERT table={table} finalization elapsed={:?}",
            started.elapsed()
        );
    }
    result.map_err(|err| {
        std::io::Error::other(format!(
            "{worker_name} INSERT table={table} finalization failed: {err}"
        ))
        .into()
    })
}

async fn end_with_gate<T: Row>(
    gate: Option<&Arc<Semaphore>>,
    worker_name: &str,
    table: &str,
    inserter: Inserter<T>,
) -> Result<Quantities> {
    let pending = inserter.pending().clone();
    if pending.rows == 0 {
        return Ok(Quantities::ZERO);
    }
    let _permit = acquire_insert_permit(gate, worker_name, table).await?;
    let started = Instant::now();
    let result = inserter.end().await;
    if let Err(err) = &result {
        error!(
            "[clickhouse] {worker_name} INSERT table={table} finalization failed after {:?}: {err}; debug={err:?}; pending_rows={} pending_bytes={}",
            started.elapsed(),
            pending.rows,
            pending.bytes,
        );
    }
    if started.elapsed() >= Duration::from_secs(5) {
        debug!(
            "[clickhouse] {worker_name} INSERT table={table} finalization elapsed={:?}",
            started.elapsed()
        );
    }
    result.map_err(|err| {
        std::io::Error::other(format!(
            "{worker_name} INSERT table={table} finalization failed: {err}"
        ))
        .into()
    })
}

async fn force_aged_inserter<T: Row>(
    worker_name: &str,
    table: &str,
    inserter: &mut Inserter<T>,
    opened_at: &mut Option<Instant>,
    now: Instant,
    gate: Option<&Arc<Semaphore>>,
) -> Result<()> {
    if inserter.pending().rows == 0 {
        *opened_at = None;
        return Ok(());
    }

    if opened_at
        .map(|started| now.duration_since(started) >= MAX_OPEN_INSERT_AGE)
        .unwrap_or(false)
    {
        let started = Instant::now();
        debug!(
            "[clickhouse] {worker_name} INSERT table={table} age flush begin rows={} age={:?}",
            inserter.pending().rows,
            opened_at.expect("opened timestamp must exist").elapsed(),
        );
        let quantities = force_commit_with_gate(gate, worker_name, table, inserter).await?;
        log_insert_commit(worker_name, table, "age flush", &quantities);
        *opened_at = None;
        if started.elapsed() >= Duration::from_secs(5) {
            warn!(
                "[clickhouse] Flushing aged {table} insert took {:?}",
                started.elapsed()
            );
        }
    }
    Ok(())
}

fn log_insert_commit(worker_name: &str, table: &str, phase: &str, quantities: &Quantities) {
    if quantities.transactions > 0 {
        debug!(
            "[clickhouse] {worker_name} writing table={table} phase={phase} rows={} bytes={} transactions={}",
            quantities.rows, quantities.bytes, quantities.transactions,
        );
    }
}

fn new_inserter<T: Row>(client: &Client, table: &str) -> Inserter<T> {
    client
        .inserter(table)
        .with_setting("http_receive_timeout", HTTP_RECEIVE_TIMEOUT_SECS)
        .with_timeouts(
            Some(HTTP_RECEIVE_TIMEOUT),
            Some(Duration::from_secs(INSERT_END_TIMEOUT_SECS)),
        )
        .with_max_rows(MAX_BATCH_ROWS)
        .with_max_bytes(MAX_BATCH_BYTES)
}

async fn check_batch_limit<T: Row>(
    worker_name: &str,
    table: &str,
    inserter: &mut Inserter<T>,
    rows_since_commit_check: &mut u16,
    gate: Option<&Arc<Semaphore>>,
) -> Result<()> {
    *rows_since_commit_check += 1;
    if *rows_since_commit_check == BATCH_LIMIT_CHECK_INTERVAL {
        let pending = inserter.pending();
        let limit_reached = pending.rows >= MAX_BATCH_ROWS || pending.bytes >= MAX_BATCH_BYTES;
        if limit_reached {
            debug!(
                "[clickhouse] {worker_name} INSERT table={table} batch flush begin rows={} bytes={}",
                pending.rows, pending.bytes
            );
        }
        if limit_reached {
            let started = Instant::now();
            let quantities = force_commit_with_gate(gate, worker_name, table, inserter).await?;
            log_insert_commit(worker_name, table, "batch threshold", &quantities);
            debug!(
                "[clickhouse] {worker_name} INSERT table={table} batch flush completed elapsed={:?}",
                started.elapsed()
            );
        }
        *rows_since_commit_check = 0;
    }
    Ok(())
}

struct Worker<'a> {
    sink: &'a mut ClickhouseSink,
    snapshot_slot: u64,
    progress: Arc<Progress>,
    collect_close_tombstones: bool,
    spl_token_owner_accounts_seen: u64,
    spl_token_accounts_parsed: u64,
    spl_token_unexpected_size: u64,
    spl_token_unpack_failed: u64,
    token_2022_owner_accounts_seen: u64,
    token_2022_accounts_parsed: u64,
    token_2022_unexpected_size: u64,
    token_2022_unpack_failed: u64,
    closed_token_accounts: HashMap<String, AccountVersion>,
    collect_affected_pairs: bool,
    affected_pairs: HashSet<TokenPair>,
    hot_mints: HotMintSet,
}

#[derive(Default)]
struct ParallelWorkerStats {
    append_vecs_total: u64,
    nonempty_zero_account_append_vecs: u64,
    spl_token_owner_accounts_seen: u64,
    spl_token_accounts_parsed: u64,
    spl_token_unexpected_size: u64,
    spl_token_unpack_failed: u64,
    token_2022_owner_accounts_seen: u64,
    token_2022_accounts_parsed: u64,
    token_2022_unexpected_size: u64,
    token_2022_unpack_failed: u64,
    closed_token_accounts: HashMap<String, AccountVersion>,
    affected_pairs: HashSet<TokenPair>,
}

impl ParallelWorkerStats {
    fn merge(&mut self, other: Self) {
        self.append_vecs_total += other.append_vecs_total;
        self.nonempty_zero_account_append_vecs += other.nonempty_zero_account_append_vecs;
        self.spl_token_owner_accounts_seen += other.spl_token_owner_accounts_seen;
        self.spl_token_accounts_parsed += other.spl_token_accounts_parsed;
        self.spl_token_unexpected_size += other.spl_token_unexpected_size;
        self.spl_token_unpack_failed += other.spl_token_unpack_failed;
        self.token_2022_owner_accounts_seen += other.token_2022_owner_accounts_seen;
        self.token_2022_accounts_parsed += other.token_2022_accounts_parsed;
        self.token_2022_unexpected_size += other.token_2022_unexpected_size;
        self.token_2022_unpack_failed += other.token_2022_unpack_failed;
        for (pubkey, version) in other.closed_token_accounts {
            self.closed_token_accounts
                .entry(pubkey)
                .and_modify(|current| {
                    if version > *current {
                        *current = version;
                    }
                })
                .or_insert(version);
        }
        self.affected_pairs.extend(other.affected_pairs);
    }
}

impl<'a> Worker<'a> {
    async fn on_append_vec_count(&mut self, append_vec: AppendVec) -> Result<u64> {
        let append_vec_len = append_vec.len();
        let account_slot = append_vec.slot();
        let append_vec = append_vec;
        let mut parsed_accounts = 0;

        for account in append_vec_accounts(&append_vec) {
            self.insert_account(&account, account_slot).await?;
            parsed_accounts += 1;
        }

        if append_vec_len > 0 && parsed_accounts == 0 {
            warn!(
                "[clickhouse] Non-empty append vec produced 0 accounts (len={})",
                append_vec_len
            );
        }

        Ok(parsed_accounts)
    }

    async fn insert_account(
        &mut self,
        account: &StoredAccountMeta<'_>,
        account_slot: u64,
    ) -> Result<()> {
        // Do this before any write can synchronously finalize a full
        // raw_account batch. A large raw-account finalization can take several
        // seconds; flushing aged sparse streams first keeps every chunked HTTP
        // body below ClickHouse's fixed 30-second receive timeout.
        self.sink.maybe_force_commit().await?;

        self.sink
            .write_account(&AccountRow {
                pubkey: pubkey_string(account.meta.pubkey),
                owner: pubkey_string(account.account_meta.owner),
                lamports: account.account_meta.lamports,
                data_len: account.meta.data_len,
                executable: account.account_meta.executable,
                updated_slot: self.snapshot_slot,
            })
            .await?;

        // AccountsDb normalizes a zero-lamport account before writing it to an
        // AppendVec.  This check must therefore happen before dispatching by
        // owner: a closed token account has the default owner and no data left.
        if self.collect_close_tombstones
            && is_canonical_empty_account(
                account.meta.data_len,
                account.account_meta.lamports,
                account.account_meta.owner,
                account.account_meta.executable,
            )
        {
            self.record_close_candidate(account, account_slot);
        }

        if account.account_meta.owner == spl_token::id() {
            self.spl_token_owner_accounts_seen += 1;
            self.insert_spl_token(account, account_slot).await?;
        } else if account.account_meta.owner == *token_2022_program_id() {
            self.token_2022_owner_accounts_seen += 1;
            self.insert_token_2022(account, account_slot).await?;
        }

        if account.account_meta.owner == mpl_metadata::id() {
            self.insert_token_metadata(account).await?;
        }

        self.progress.accounts.inc();
        Ok(())
    }

    async fn insert_spl_token(
        &mut self,
        account: &StoredAccountMeta<'_>,
        account_slot: u64,
    ) -> Result<()> {
        match account.meta.data_len as usize {
            spl_token::state::Account::LEN => {
                match spl_token::state::Account::unpack(account.data) {
                    Ok(token_account) => {
                        let mint = pubkey_string(token_account.mint);
                        let owner = pubkey_string(token_account.owner);
                        if self.hot_mints.contains(&mint) {
                            self.remember_affected_pair(&mint, &owner);
                            self.sink
                                .write_token_account(&TokenAccountRow {
                                    pubkey: pubkey_string(account.meta.pubkey),
                                    mint,
                                    owner,
                                    amount: token_account.amount,
                                    delegate: token_account.delegate.map(pubkey_string).into(),
                                    delegated_amount: token_account.delegated_amount,
                                    state: token_account.state as u8,
                                    close_authority: token_account
                                        .close_authority
                                        .map(pubkey_string)
                                        .into(),
                                    is_deleted: 0,
                                    updated_slot: account_slot,
                                })
                                .await?;
                        }
                        self.spl_token_accounts_parsed += 1;
                        self.progress.tokens.inc();
                    }
                    Err(_) => {
                        self.spl_token_unpack_failed += 1;
                        self.record_close_candidate(account, account_slot);
                    }
                }
            }
            spl_token::state::Mint::LEN => match spl_token::state::Mint::unpack(account.data) {
                Ok(token_mint) => {
                    let mint = pubkey_string(account.meta.pubkey);
                    if self.hot_mints.contains(&mint) {
                        self.sink
                            .write_token_mint(&TokenMintRow {
                                mint,
                                mint_authority: token_mint.mint_authority.map(pubkey_string).into(),
                                supply: token_mint.supply,
                                decimals: token_mint.decimals,
                                is_initialized: token_mint.is_initialized,
                                freeze_authority: token_mint
                                    .freeze_authority
                                    .map(pubkey_string)
                                    .into(),
                                updated_slot: self.snapshot_slot,
                            })
                            .await?;
                    }
                    self.spl_token_accounts_parsed += 1;
                    self.progress.tokens.inc();
                }
                Err(_) => self.spl_token_unpack_failed += 1,
            },
            _ => self.spl_token_unexpected_size += 1,
        }

        Ok(())
    }

    async fn insert_token_2022(
        &mut self,
        account: &StoredAccountMeta<'_>,
        account_slot: u64,
    ) -> Result<()> {
        match account.meta.data_len as usize {
            spl_token_2022::state::Account::LEN => {
                match spl_token_2022::state::Account::unpack(account.data) {
                    Ok(token_account) => {
                        let mint = pubkey_string(token_account.mint);
                        let owner = pubkey_string(token_account.owner);
                        if self.hot_mints.contains(&mint) {
                            self.remember_affected_pair(&mint, &owner);
                            self.sink
                                .write_token_account(&TokenAccountRow {
                                    pubkey: pubkey_string(account.meta.pubkey),
                                    mint,
                                    owner,
                                    amount: token_account.amount,
                                    delegate: token_account.delegate.map(pubkey_string).into(),
                                    delegated_amount: token_account.delegated_amount,
                                    state: token_account.state as u8,
                                    close_authority: token_account
                                        .close_authority
                                        .map(pubkey_string)
                                        .into(),
                                    is_deleted: 0,
                                    updated_slot: account_slot,
                                })
                                .await?;
                        }
                        self.token_2022_accounts_parsed += 1;
                        self.progress.tokens.inc();
                    }
                    Err(_) => {
                        self.token_2022_unpack_failed += 1;
                        self.record_close_candidate(account, account_slot);
                    }
                }
            }
            spl_token_2022::state::Mint::LEN => {
                match spl_token_2022::state::Mint::unpack(account.data) {
                    Ok(token_mint) => {
                        let mint = pubkey_string(account.meta.pubkey);
                        if self.hot_mints.contains(&mint) {
                            self.sink
                                .write_token_mint(&TokenMintRow {
                                    mint,
                                    mint_authority: token_mint
                                        .mint_authority
                                        .map(pubkey_string)
                                        .into(),
                                    supply: token_mint.supply,
                                    decimals: token_mint.decimals,
                                    is_initialized: token_mint.is_initialized,
                                    freeze_authority: token_mint
                                        .freeze_authority
                                        .map(pubkey_string)
                                        .into(),
                                    updated_slot: self.snapshot_slot,
                                })
                                .await?;
                        }
                        self.token_2022_accounts_parsed += 1;
                        self.progress.tokens.inc();
                    }
                    Err(_) => self.token_2022_unpack_failed += 1,
                }
            }
            _ => self.token_2022_unexpected_size += 1,
        }

        Ok(())
    }

    fn remember_affected_pair(&mut self, mint: &str, owner: &str) {
        if self.collect_affected_pairs {
            self.affected_pairs
                .insert((mint.to_owned(), owner.to_owned()));
        }
    }

    async fn insert_token_metadata(&mut self, account: &StoredAccountMeta<'_>) -> Result<()> {
        if account.data.is_empty() {
            return Ok(());
        }

        let mut data = account.data;
        let account_key = match mpl_metadata::AccountKey::deserialize(&mut data) {
            Ok(account_key) => account_key,
            Err(_) => return Ok(()),
        };
        if !matches!(account_key, mpl_metadata::AccountKey::MetadataV1) {
            return Ok(());
        }

        let metadata = match mpl_metadata::Metadata::deserialize(&mut data) {
            Ok(metadata) => metadata,
            Err(err) => {
                warn!(
                    "Skipping invalid token-metadata v1 metadata account {}: {}",
                    account.meta.pubkey, err
                );
                return Ok(());
            }
        };
        let metadata_ext = mpl_metadata::MetadataExt::deserialize(&mut data).ok();
        let metadata_ext_v1_2 = metadata_ext
            .as_ref()
            .and_then(|_| mpl_metadata::MetadataExtV1_2::deserialize(&mut data).ok());

        let mint = pubkey_string(metadata.mint);
        if !self.hot_mints.contains(&mint) {
            return Ok(());
        }

        self.sink
            .write_token_metadata(&TokenMetadataRow {
                mint,
                name: metadata.data.name,
                symbol: metadata.data.symbol,
                uri: metadata.data.uri,
                update_authority: pubkey_string(metadata.update_authority),
                is_mutable: metadata.is_mutable,
                token_standard: metadata_ext_v1_2.and_then(|metadata| metadata.token_standard),
                seller_fee_basis_points: metadata.data.seller_fee_basis_points,
                creators: metadata
                    .data
                    .creators
                    .unwrap_or_default()
                    .into_iter()
                    .map(|creator| pubkey_string(creator.address))
                    .collect(),
                updated_slot: self.snapshot_slot,
            })
            .await?;
        self.progress.metadata.inc();
        Ok(())
    }

    /// AccountsDb canonicalizes a closed account to an empty account, so the
    /// old mint/owner are not present in the archive. Keep the pubkey and the
    /// newest slot; the tombstone writer appends a delete version directly
    /// after all regular rows have committed.
    fn record_close_candidate(&mut self, account: &StoredAccountMeta<'_>, account_slot: u64) {
        if self.collect_close_tombstones
            && is_close_tombstone_candidate(account.account_meta.lamports)
        {
            remember_close_candidate(&mut self.closed_token_accounts, account, account_slot);
        }
    }
}

fn remember_close_candidate(
    candidates: &mut HashMap<String, AccountVersion>,
    account: &StoredAccountMeta<'_>,
    account_slot: u64,
) {
    let candidate = AccountVersion {
        updated_slot: account_slot,
    };
    let pubkey = pubkey_string(account.meta.pubkey);
    candidates
        .entry(pubkey)
        .and_modify(|current| {
            if candidate > *current {
                *current = candidate;
            }
        })
        .or_insert(candidate);
}

fn is_close_tombstone_candidate(lamports: u64) -> bool {
    lamports == 0
}

fn is_canonical_empty_account(
    data_len: u64,
    lamports: u64,
    owner: Pubkey,
    executable: bool,
) -> bool {
    data_len == 0 && lamports == 0 && owner == Pubkey::default() && !executable
}

async fn write_close_token_account_tombstones(
    client: &Client,
    group: TableGroup,
    closed_token_accounts: &HashMap<String, AccountVersion>,
) -> Result<TombstoneWriteResult> {
    let mut pubkeys = closed_token_accounts.keys().collect::<Vec<_>>();
    if pubkeys.is_empty() {
        return Ok(TombstoneWriteResult {
            marked_deleted: 0,
            affected_pairs: HashSet::new(),
        });
    }

    // HashMap iteration order is intentionally random. Sorting makes batches
    // deterministic, which is useful for diagnostics and reproducible imports.
    pubkeys.sort_unstable();
    let batch_count = pubkeys.len().div_ceil(CLOSE_TOMBSTONE_BATCH_SIZE);
    let state_table = group.table("hot_token_account_state");
    let mut tombstone_insert: Inserter<TokenAccountRow> = new_inserter(client, &state_table);
    let mut marked_deleted = 0;
    let mut affected_pairs = HashSet::new();

    for (batch_idx, pubkeys) in pubkeys.chunks(CLOSE_TOMBSTONE_BATCH_SIZE).enumerate() {
        debug!(
            "[clickhouse] Writing tombstone batch {}/{} ({} pubkeys)",
            batch_idx + 1,
            batch_count,
            pubkeys.len()
        );

        let pubkey_literals = pubkeys
            .iter()
            .map(|pubkey| sql_string_literal(pubkey))
            .collect::<Vec<_>>();
        let previous_rows_sql = format!(
            "SELECT pubkey, mint, owner, updated_slot FROM {state_table} FINAL \
             WHERE is_deleted = 0 AND pubkey IN ({})",
            pubkey_literals.join(", ")
        );
        let previous_rows = client
            .query(&previous_rows_sql)
            .with_setting("max_query_size", HOT_PAIR_QUERY_MAX_QUERY_SIZE)
            .fetch_all::<HotTokenAccountPairRow>()
            .await
            .map_err(|err| {
                format!("failed to recover old hot token-account pairs for tombstones: {err}")
            })?;
        let previous_pairs = previous_rows
            .into_iter()
            .filter(|row| !row.mint.is_empty() && !row.owner.is_empty())
            .map(|row| (row.pubkey.clone(), row))
            .collect::<HashMap<_, _>>();

        for pubkey in pubkeys {
            let candidate = closed_token_accounts
                .get(*pubkey)
                .ok_or_else(|| format!("missing tombstone candidate for {pubkey}"))?;
            let Some(previous) = previous_pairs.get(*pubkey) else {
                // A canonical empty account is not necessarily a hot SPL
                // token account. It has no place in this direct-write L2.
                continue;
            };
            if previous.updated_slot >= candidate.updated_slot {
                // A later live version in this archive already supersedes the
                // candidate empty record; inserting an older tombstone would
                // be useless and create an avoidable physical row.
                continue;
            }
            let mint = previous.mint.clone();
            let owner = previous.owner.clone();
            affected_pairs.insert((mint.clone(), owner.clone()));
            tombstone_insert
                .write(&TokenAccountRow {
                    pubkey: (*pubkey).clone(),
                    // The archive no longer contains the closed account's
                    // token payload. Recover it from the current hot-state
                    // row before appending its delete version.
                    mint,
                    owner,
                    amount: 0,
                    delegate: None,
                    delegated_amount: 0,
                    state: 0,
                    close_authority: None,
                    is_deleted: 1,
                    updated_slot: candidate.updated_slot,
                })
                .await?;
            marked_deleted += 1;
        }

        tombstone_insert.force_commit().await.map_err(|err| {
            format!(
                "hot-state tombstone insert failed for batch {}/{}: {}",
                batch_idx + 1,
                batch_count,
                err
            )
        })?;
        debug!(
            "[clickhouse] Tombstone batch {}/{} inserted {} delete versions",
            batch_idx + 1,
            batch_count,
            pubkeys.len()
        );
    }

    tombstone_insert
        .end()
        .await
        .map_err(|err| format!("final hot-state tombstone insert failed: {}", err))?;
    Ok(TombstoneWriteResult {
        marked_deleted,
        affected_pairs,
    })
}

fn pubkey_string(pubkey: Pubkey) -> String {
    pubkey.to_string()
}

fn token_2022_program_id() -> &'static Pubkey {
    static TOKEN_2022_ID: OnceLock<Pubkey> = OnceLock::new();
    TOKEN_2022_ID.get_or_init(|| {
        Pubkey::from_str("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb")
            .expect("Token-2022 program id must be a valid public key")
    })
}

struct ProgressCounter {
    count: AtomicU64,
    progress_bar: ProgressBar,
}

impl ProgressCounter {
    fn new(progress_bar: ProgressBar) -> Self {
        Self {
            count: AtomicU64::new(0),
            progress_bar,
        }
    }

    fn inc(&self) {
        let count = self.count.fetch_add(1, Ordering::Relaxed) + 1;
        // Updating an indicatif progress bar takes a mutex and may redraw the
        // terminal.  Doing that for every account can become a measurable part
        // of a 100M+ row import, so keep the exact atomic counter but redraw in
        // small chunks.  The final value remains available through `get()`.
        if count % 4_096 == 0 {
            self.progress_bar.inc(4_096);
        }
    }

    fn get(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    fn sync(&self) {
        self.progress_bar.set_position(self.get());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_columns_match_clickhouse_schema() {
        assert_eq!(
            <AccountRow as Row>::COLUMN_NAMES,
            [
                "pubkey",
                "owner",
                "lamports",
                "data_len",
                "executable",
                "updated_slot",
            ]
        );
        assert_eq!(
            <TokenAccountRow as Row>::COLUMN_NAMES,
            [
                "pubkey",
                "mint",
                "owner",
                "amount",
                "delegate",
                "delegated_amount",
                "state",
                "close_authority",
                "is_deleted",
                "updated_slot",
            ]
        );
        assert_eq!(
            <TokenMintRow as Row>::COLUMN_NAMES,
            [
                "mint",
                "mint_authority",
                "supply",
                "decimals",
                "is_initialized",
                "freeze_authority",
                "updated_slot"
            ]
        );
        assert_eq!(
            <TokenMetadataRow as Row>::COLUMN_NAMES,
            [
                "mint",
                "name",
                "symbol",
                "uri",
                "update_authority",
                "is_mutable",
                "token_standard",
                "seller_fee_basis_points",
                "creators",
                "updated_slot"
            ]
        );
        assert_eq!(
            <WalletBalanceRow as Row>::COLUMN_NAMES,
            ["mint", "owner", "amount_raw", "updated_slot"]
        );
    }

    #[test]
    fn parses_basic_auth_from_clickhouse_url() {
        let connection = parse_clickhouse_connection_url(
            "http://user%40name:pass%3Aword@clickhouse.example:8123",
        )
        .unwrap();

        assert_eq!(connection.endpoint, "http://clickhouse.example:8123/");
        assert_eq!(connection.user.as_deref(), Some("user@name"));
        assert_eq!(connection.password.as_deref(), Some("pass:word"));
    }

    #[test]
    fn zeroed_token_account_is_a_close_tombstone_candidate() {
        let zeroed_data = vec![0; spl_token::state::Account::LEN];

        assert!(spl_token::state::Account::unpack(&zeroed_data).is_err());
        assert!(is_close_tombstone_candidate(0));
        assert!(!is_close_tombstone_candidate(1));
    }

    #[test]
    fn canonical_empty_account_is_a_close_tombstone_candidate() {
        assert!(is_canonical_empty_account(0, 0, Pubkey::default(), false));
        assert!(!is_canonical_empty_account(
            165,
            0,
            Pubkey::default(),
            false
        ));
        assert!(!is_canonical_empty_account(0, 0, spl_token::id(), false));
        assert!(!is_canonical_empty_account(0, 0, Pubkey::default(), true));
    }

    #[test]
    fn account_version_orders_by_slot() {
        assert!(AccountVersion { updated_slot: 2 } > AccountVersion { updated_slot: 1 });
    }

    #[test]
    fn formats_average_eta_as_hours_minutes_seconds() {
        assert_eq!(format_duration(0.0), "00:00:00");
        assert_eq!(format_duration(3_661.1), "01:01:02");
        assert_eq!(format_duration(f64::NAN), "unknown");
    }

    #[test]
    fn only_incremental_archives_collect_close_tombstones() {
        assert!(!SnapshotKind::Full.collect_close_tombstones());
        assert!(SnapshotKind::Incremental.collect_close_tombstones());
    }

    #[test]
    fn normalizes_clickhouse_projection_sorting_key_formats() {
        assert_eq!(
            normalize_sorting_key("['mint','amount_raw','owner']"),
            normalize_sorting_key("mint, amount_raw, owner")
        );
        assert_eq!(
            normalize_sorting_key("(owner, mint)"),
            normalize_sorting_key("owner, mint")
        );
    }

    #[test]
    fn raw_merge_backlog_requires_strictly_fewer_than_twenty_parts() {
        let below_limit = vec![RawMergePartCount {
            partition: "all".to_owned(),
            parts_count: RAW_MERGE_READY_PARTS_PER_PARTITION_LIMIT - 1,
            total_rows: 1,
            total_size: "1 B".to_owned(),
        }];
        assert!(raw_merge_backlog_is_ready(&below_limit));

        let at_limit = vec![RawMergePartCount {
            partition: "all".to_owned(),
            parts_count: RAW_MERGE_READY_PARTS_PER_PARTITION_LIMIT,
            total_rows: 1,
            total_size: "1 B".to_owned(),
        }];
        assert!(!raw_merge_backlog_is_ready(&at_limit));
    }

    #[test]
    fn tombstone_rows_keep_the_recovered_hot_pair() {
        let row = TokenAccountRow {
            pubkey: "candidate".to_owned(),
            mint: "mint".to_owned(),
            owner: "owner".to_owned(),
            amount: 0,
            delegate: None,
            delegated_amount: 0,
            state: 0,
            close_authority: None,
            is_deleted: 1,
            updated_slot: 42,
        };
        assert_eq!(row.is_deleted, 1);
        assert_eq!(row.updated_slot, 42);
        assert_eq!(row.mint, "mint");
        assert_eq!(row.owner, "owner");
    }

    #[test]
    fn direct_write_schema_has_a_frozen_filter_but_no_raw_token_account_table() {
        let names = required_table_names();
        assert!(names.iter().any(|name| name == "hot_token_filter"));
        assert!(names.iter().any(|name| name == "hot_token_filter_bak"));
        assert!(!names.iter().any(|name| name == "raw_token_account"));
        assert!(!names.iter().any(|name| name == "raw_token_account_bak"));
    }
}
