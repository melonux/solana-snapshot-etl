use borsh::BorshDeserialize;
use clickhouse::inserter::{Inserter, Quantities};
use clickhouse::{Client, Row};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use log::{debug, error, warn};
use percent_encoding::percent_decode_str;
use serde::{Deserialize, Serialize};
use solana_sdk::program_pack::Pack;
use solana_sdk::pubkey::Pubkey;
use solana_snapshot_etl::append_vec::{AppendVec, StoredAccountMeta};
use solana_snapshot_etl::{append_vec_accounts, AppendVecIterator};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread;
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use url::Url;

use crate::mpl_metadata;

const DATABASE: &str = "solana";
const ACCOUNT_TABLE: &str = "raw_account";
const TOKEN_ACCOUNT_TABLE: &str = "raw_token_account";
const TOKEN_MINT_TABLE: &str = "raw_token_mint";
const TOKEN_METADATA_TABLE: &str = "raw_token_metadata";

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
// Keep tombstone INSERT batches large enough to avoid creating thousands of
// tiny MergeTree parts, while bounding each RowBinary request and its
// finalization memory. (This used to be a 2,000-row query batch because the
// pubkey IN list had to fit max_query_size; tombstones are now INSERT-only.)
const CLOSE_TOMBSTONE_BATCH_SIZE: usize = 100_000;

pub(crate) type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

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
            sink: ClickhouseSink::new(&client, "main", None),
            client,
            connection_url,
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
        let token_account_close_candidates = if collect_close_tombstones {
            closed_token_accounts.len() as u64
        } else {
            0
        };
        drop(worker);

        self.sink.end().await?;
        let token_accounts_marked_deleted = if collect_close_tombstones {
            write_close_token_account_tombstones(&self.client, &closed_token_accounts).await?
        } else {
            debug!(
                "[clickhouse] Full snapshot: skipped tombstone candidate scan (archive excludes tombstones)"
            );
            0
        };
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
            token_accounts_marked_deleted,
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
            let snapshot_slot = self.snapshot_slot;
            let progress = Arc::clone(&self.progress);
            let insert_gate = Arc::clone(&insert_gate);
            let cancelled = Arc::clone(&cancelled);
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
        let token_accounts_marked_deleted = if collect_close_tombstones {
            write_close_token_account_tombstones(&self.client, &totals.closed_token_accounts)
                .await?
        } else {
            debug!(
                "[clickhouse] Full snapshot: skipped tombstone candidate scan (archive excludes tombstones)"
            );
            0
        };
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
            token_accounts_marked_deleted,
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

        let token_accounts_marked_deleted =
            write_close_token_account_tombstones(&self.client, &closed_token_accounts).await?;
        self.progress.append_vecs.finish_with_message("done");
        self.progress.accounts.sync();
        self.progress.tokens.sync();
        self.progress.metadata.sync();
        let _ = &self.multi_progress;

        Ok(CloseTombstoneStats {
            append_vecs_total,
            skipped_append_vecs,
            canonical_empty_accounts,
            token_accounts_marked_deleted,
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

/// Return the high-water mark that the snapshot watcher uses to resume an
/// existing ClickHouse import. `coalesce` also makes an empty raw_account
/// table start from slot zero.
pub(crate) async fn max_raw_account_updated_slot(connection_url: &str) -> Result<u64> {
    new_clickhouse_client(connection_url)?
        .query("SELECT coalesce(max(updated_slot), toUInt64(0)) FROM raw_account")
        .fetch_one::<u64>()
        .await
        .map_err(Into::into)
}

#[derive(Row, Deserialize)]
struct SystemTableRow {
    name: String,
    engine: String,
    sorting_key: String,
    engine_full: String,
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
            "SELECT name, engine, sorting_key, engine_full FROM system.tables WHERE database = '{DATABASE}' AND name IN ({table_list})"
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
                if !spec.sorting_key.is_empty() && definition.sorting_key != spec.sorting_key {
                    errors.push(format!(
                        "table solana.{} uses sorting key {}, expected {}",
                        spec.name, definition.sorting_key, spec.sorting_key
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
            "SELECT table, name FROM system.projections WHERE database = '{DATABASE}' AND table IN ({table_list})"
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
        .map(|row| (row.table, row.name))
        .collect::<std::collections::HashSet<_>>();
    for table in ["hot_wallet_token_balance", "hot_wallet_token_balance_bak"] {
        if !projections.contains(&(table.to_owned(), "proj_by_owner".to_owned())) {
            errors.push(format!("missing projection solana.{table}.proj_by_owner"));
        }
    }

    if errors.is_empty() {
        debug!(
            "[clickhouse] Schema validation passed: {} tables, required columns, and wallet projections",
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

fn required_table_specs() -> Vec<RequiredTableSpec> {
    vec![
        RequiredTableSpec {
            name: "raw_account",
            engine: "ReplacingMergeTree",
            engine_full_prefix: "ReplacingMergeTree(updated_slot)",
            sorting_key: "owner, pubkey",
        },
        RequiredTableSpec {
            name: "raw_token_account",
            engine: "ReplacingMergeTree",
            engine_full_prefix: "ReplacingMergeTree(updated_slot, is_deleted)",
            sorting_key: "pubkey",
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
            name: "raw_token_account_bak",
            engine: "ReplacingMergeTree",
            engine_full_prefix: "ReplacingMergeTree(updated_slot, is_deleted)",
            sorting_key: "pubkey",
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
            engine: "MergeTree",
            engine_full_prefix: "MergeTree",
            sorting_key: "mint, amount_raw, owner",
        },
        RequiredTableSpec {
            name: "hot_wallet_token_balance_bak",
            engine: "MergeTree",
            engine_full_prefix: "MergeTree",
            sorting_key: "mint, amount_raw, owner",
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
    const RAW_TOKEN_ACCOUNT: &[(&str, &str)] = &[
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
        ("state", "Enum8"),
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
    for table in ["raw_token_account", "raw_token_account_bak"] {
        required.extend(
            RAW_TOKEN_ACCOUNT
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
        .map(|value| format!("'{}'", value.replace('\'', "''")))
        .collect::<Vec<_>>()
        .join(", ")
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
    ) -> Self {
        Self {
            worker_name: worker_name.into(),
            insert_gate,
            account: new_inserter(client, ACCOUNT_TABLE),
            token_account: new_inserter(client, TOKEN_ACCOUNT_TABLE),
            token_mint: new_inserter(client, TOKEN_MINT_TABLE),
            token_metadata: new_inserter(client, TOKEN_METADATA_TABLE),
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
                self.worker_name, ACCOUNT_TABLE
            );
            self.account_opened_at = Some(Instant::now());
        }
        self.account.write(row).await?;
        check_batch_limit(
            &self.worker_name,
            ACCOUNT_TABLE,
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
                self.worker_name, TOKEN_ACCOUNT_TABLE
            );
            self.token_account_opened_at = Some(Instant::now());
        }
        self.token_account.write(row).await?;
        check_batch_limit(
            &self.worker_name,
            TOKEN_ACCOUNT_TABLE,
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
                self.worker_name, TOKEN_MINT_TABLE
            );
            self.token_mint_opened_at = Some(Instant::now());
        }
        self.token_mint.write(row).await?;
        check_batch_limit(
            &self.worker_name,
            TOKEN_MINT_TABLE,
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
                self.worker_name, TOKEN_METADATA_TABLE
            );
            self.token_metadata_opened_at = Some(Instant::now());
        }
        self.token_metadata.write(row).await?;
        check_batch_limit(
            &self.worker_name,
            TOKEN_METADATA_TABLE,
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
            ACCOUNT_TABLE,
            &mut self.account,
            &mut self.account_opened_at,
            now,
            self.insert_gate.as_ref(),
        )
        .await?;
        force_aged_inserter(
            &self.worker_name,
            TOKEN_ACCOUNT_TABLE,
            &mut self.token_account,
            &mut self.token_account_opened_at,
            now,
            self.insert_gate.as_ref(),
        )
        .await?;
        force_aged_inserter(
            &self.worker_name,
            TOKEN_MINT_TABLE,
            &mut self.token_mint,
            &mut self.token_mint_opened_at,
            now,
            self.insert_gate.as_ref(),
        )
        .await?;
        force_aged_inserter(
            &self.worker_name,
            TOKEN_METADATA_TABLE,
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
            "[clickhouse] {} INSERT flush begin: raw_account={} rows, raw_token_account={} rows, raw_token_mint={} rows, raw_token_metadata={} rows",
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
        let account =
            force_commit_with_gate(gate, &self.worker_name, ACCOUNT_TABLE, &mut self.account)
                .await?;
        let token_account = force_commit_with_gate(
            gate,
            &self.worker_name,
            TOKEN_ACCOUNT_TABLE,
            &mut self.token_account,
        )
        .await?;
        let token_mint = force_commit_with_gate(
            gate,
            &self.worker_name,
            TOKEN_MINT_TABLE,
            &mut self.token_mint,
        )
        .await?;
        let token_metadata = force_commit_with_gate(
            gate,
            &self.worker_name,
            TOKEN_METADATA_TABLE,
            &mut self.token_metadata,
        )
        .await?;
        log_insert_commit(&self.worker_name, ACCOUNT_TABLE, "idle flush", &account);
        log_insert_commit(
            &self.worker_name,
            TOKEN_ACCOUNT_TABLE,
            "idle flush",
            &token_account,
        );
        log_insert_commit(
            &self.worker_name,
            TOKEN_MINT_TABLE,
            "idle flush",
            &token_mint,
        );
        log_insert_commit(
            &self.worker_name,
            TOKEN_METADATA_TABLE,
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
        let account = end_with_gate(gate, &worker_name, ACCOUNT_TABLE, self.account).await?;
        let token_account =
            end_with_gate(gate, &worker_name, TOKEN_ACCOUNT_TABLE, self.token_account).await?;
        let token_mint =
            end_with_gate(gate, &worker_name, TOKEN_MINT_TABLE, self.token_mint).await?;
        let token_metadata = end_with_gate(
            gate,
            &worker_name,
            TOKEN_METADATA_TABLE,
            self.token_metadata,
        )
        .await?;
        log_insert_commit(&worker_name, ACCOUNT_TABLE, "final flush", &account);
        log_insert_commit(
            &worker_name,
            TOKEN_ACCOUNT_TABLE,
            "final flush",
            &token_account,
        );
        log_insert_commit(&worker_name, TOKEN_MINT_TABLE, "final flush", &token_mint);
        log_insert_commit(
            &worker_name,
            TOKEN_METADATA_TABLE,
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
                        self.sink
                            .write_token_account(&TokenAccountRow {
                                pubkey: pubkey_string(account.meta.pubkey),
                                mint: pubkey_string(token_account.mint),
                                owner: pubkey_string(token_account.owner),
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
                    self.sink
                        .write_token_mint(&TokenMintRow {
                            mint: pubkey_string(account.meta.pubkey),
                            mint_authority: token_mint.mint_authority.map(pubkey_string).into(),
                            supply: token_mint.supply,
                            decimals: token_mint.decimals,
                            is_initialized: token_mint.is_initialized,
                            freeze_authority: token_mint.freeze_authority.map(pubkey_string).into(),
                            updated_slot: self.snapshot_slot,
                        })
                        .await?;
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
                        self.sink
                            .write_token_account(&TokenAccountRow {
                                pubkey: pubkey_string(account.meta.pubkey),
                                mint: pubkey_string(token_account.mint),
                                owner: pubkey_string(token_account.owner),
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
                        self.sink
                            .write_token_mint(&TokenMintRow {
                                mint: pubkey_string(account.meta.pubkey),
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

        self.sink
            .write_token_metadata(&TokenMetadataRow {
                mint: pubkey_string(metadata.mint),
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
    closed_token_accounts: &HashMap<String, AccountVersion>,
) -> Result<u64> {
    let mut pubkeys = closed_token_accounts.keys().collect::<Vec<_>>();
    if pubkeys.is_empty() {
        return Ok(0);
    }

    // HashMap iteration order is intentionally random. Sorting makes batches
    // deterministic, which is useful for diagnostics and reproducible imports.
    pubkeys.sort_unstable();
    let batch_count = pubkeys.len().div_ceil(CLOSE_TOMBSTONE_BATCH_SIZE);
    let mut tombstone_insert: Inserter<TokenAccountRow> = new_inserter(client, TOKEN_ACCOUNT_TABLE);
    let mut marked_deleted = 0;

    for (batch_idx, pubkeys) in pubkeys.chunks(CLOSE_TOMBSTONE_BATCH_SIZE).enumerate() {
        debug!(
            "[clickhouse] Writing tombstone batch {}/{} ({} pubkeys)",
            batch_idx + 1,
            batch_count,
            pubkeys.len()
        );

        for pubkey in pubkeys {
            let candidate = closed_token_accounts
                .get(*pubkey)
                .ok_or_else(|| format!("missing tombstone candidate for {pubkey}"))?;
            tombstone_insert
                .write(&TokenAccountRow {
                    pubkey: (*pubkey).clone(),
                    // Canonical empty accounts do not retain token metadata.
                    // ReplacingMergeTree uses pubkey + updated_slot +
                    // is_deleted for the state transition; these fields are
                    // deliberately neutral values for the delete version.
                    mint: String::new(),
                    owner: String::new(),
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
                "tombstone insert failed for batch {}/{}: {}",
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
        .map_err(|err| format!("final tombstone insert failed: {}", err))?;
    Ok(marked_deleted)
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
    fn tombstone_rows_use_delete_version_and_neutral_fields() {
        let row = TokenAccountRow {
            pubkey: "candidate".to_owned(),
            mint: String::new(),
            owner: String::new(),
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
        assert!(row.mint.is_empty());
        assert!(row.owner.is_empty());
    }
}
