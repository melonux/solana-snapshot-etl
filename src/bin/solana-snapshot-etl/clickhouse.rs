use borsh::BorshDeserialize;
use clickhouse::inserter::Inserter;
use clickhouse::{Client, Row};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use log::{info, warn};
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
use url::Url;

use crate::mpl_metadata;

const DATABASE: &str = "solana";
const ACCOUNT_TABLE: &str = "raw_account";
const TOKEN_ACCOUNT_TABLE: &str = "raw_token_account";
const TOKEN_MINT_TABLE: &str = "raw_token_mint";
const TOKEN_METADATA_TABLE: &str = "raw_token_metadata";

// AccountsDb stores every zero-lamport account as a canonical empty account
// (data_len=0, default owner), so a CloseAccount no longer looks like a token
// account when it is read from a snapshot.  Keep those pubkeys and look up
// their previous L1 identity after the streamed inserts have committed.
const CLOSE_TOKEN_ACCOUNT_LIVE_ROWS_QUERY: &str = r#"
SELECT ?fields
FROM raw_token_account FINAL
WHERE is_deleted = 0
  AND pubkey IN ?
"#;

// Larger inserts reduce MergeTree part creation. The exporter also force-commits every open
// RowBinary stream regularly, so sparse derived tables cannot leave an idle chunked request open
// long enough for ClickHouse or a reverse proxy to close it.
const MAX_BATCH_ROWS: u64 = 250_000;
const MAX_BATCH_BYTES: u64 = 64 * 1024 * 1024;
const BATCH_LIMIT_CHECK_INTERVAL: u16 = 1_024;
const FLUSH_CHECK_INTERVAL: u16 = 1_024;
const MAX_OPEN_INSERT_AGE: Duration = Duration::from_secs(15);
// Query `.bind()` values are rendered into the SQL text by clickhouse-rs. Keep
// the IN list well below ClickHouse's default max_query_size (256 KiB); a
// 10,000-pubkey batch can exceed it before the server starts executing the
// query.
const CLOSE_TOMBSTONE_BATCH_SIZE: usize = 2_000;

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

#[derive(Row, Serialize, Deserialize)]
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
            sink: ClickhouseSink::new(&client),
            client,
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
            return self
                .insert_all_parallel(iterator, snapshot_kind, workers)
                .await;
        }

        self.insert_all_sequential(iterator, snapshot_kind).await
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
            info!(
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

        for _ in 0..workers {
            let rx = rx.clone();
            let client = self.client.clone();
            let snapshot_slot = self.snapshot_slot;
            let progress = Arc::clone(&self.progress);
            handles.push(thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|err| err.to_string())?;
                runtime.block_on(async move {
                    let mut sink = ClickhouseSink::new(&client);
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
                    while let Ok(append_vec) = rx.recv() {
                        append_vecs_total += 1;
                        match worker.on_append_vec_count(append_vec).await {
                            Ok(0) => nonempty_zero_account_append_vecs += 1,
                            Ok(_) => {}
                            Err(err) => {
                                // Drain the queue before returning so the
                                // producer cannot deadlock if a worker fails.
                                while rx.recv().is_ok() {}
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
                    sink.end().await.map_err(|err| err.to_string())?;

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
        for (append_vec_idx, append_vec) in iterator.enumerate() {
            match append_vec {
                Ok(append_vec) => tx
                    .send(append_vec)
                    .map_err(|_| "parallel ClickHouse worker exited unexpectedly")?,
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

        let token_account_close_candidates = if collect_close_tombstones {
            totals.closed_token_accounts.len() as u64
        } else {
            0
        };
        let token_accounts_marked_deleted = if collect_close_tombstones {
            write_close_token_account_tombstones(&self.client, &totals.closed_token_accounts)
                .await?
        } else {
            info!(
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

    /// Scan only canonical empty accounts and mark matching previously-live
    /// token accounts as deleted. Unlike `insert_all`, this does not write any
    /// raw or parsed snapshot rows, so it can repair tombstones without
    /// re-importing an already loaded snapshot.
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
    account: Inserter<AccountRow>,
    token_account: Inserter<TokenAccountRow>,
    token_mint: Inserter<TokenMintRow>,
    token_metadata: Inserter<TokenMetadataRow>,
    account_rows_since_commit_check: u16,
    token_account_rows_since_commit_check: u16,
    token_mint_rows_since_commit_check: u16,
    token_metadata_rows_since_commit_check: u16,
    flush_check_counter: u16,
    last_force_commit: Instant,
}

impl ClickhouseSink {
    fn new(client: &Client) -> Self {
        Self {
            account: new_inserter(client, ACCOUNT_TABLE),
            token_account: new_inserter(client, TOKEN_ACCOUNT_TABLE),
            token_mint: new_inserter(client, TOKEN_MINT_TABLE),
            token_metadata: new_inserter(client, TOKEN_METADATA_TABLE),
            account_rows_since_commit_check: 0,
            token_account_rows_since_commit_check: 0,
            token_mint_rows_since_commit_check: 0,
            token_metadata_rows_since_commit_check: 0,
            flush_check_counter: 0,
            last_force_commit: Instant::now(),
        }
    }

    async fn write_account(&mut self, row: &AccountRow) -> Result<()> {
        self.account.write(row).await?;
        check_batch_limit(&mut self.account, &mut self.account_rows_since_commit_check).await
    }

    async fn write_token_account(&mut self, row: &TokenAccountRow) -> Result<()> {
        self.token_account.write(row).await?;
        check_batch_limit(
            &mut self.token_account,
            &mut self.token_account_rows_since_commit_check,
        )
        .await
    }

    async fn write_token_mint(&mut self, row: &TokenMintRow) -> Result<()> {
        self.token_mint.write(row).await?;
        check_batch_limit(
            &mut self.token_mint,
            &mut self.token_mint_rows_since_commit_check,
        )
        .await
    }

    async fn write_token_metadata(&mut self, row: &TokenMetadataRow) -> Result<()> {
        self.token_metadata.write(row).await?;
        check_batch_limit(
            &mut self.token_metadata,
            &mut self.token_metadata_rows_since_commit_check,
        )
        .await
    }

    async fn maybe_force_commit(&mut self) -> Result<()> {
        self.flush_check_counter += 1;
        if self.flush_check_counter != FLUSH_CHECK_INTERVAL {
            return Ok(());
        }
        self.flush_check_counter = 0;

        if self.last_force_commit.elapsed() >= MAX_OPEN_INSERT_AGE {
            self.force_commit_all().await?;
        }
        Ok(())
    }

    async fn force_commit_all(&mut self) -> Result<()> {
        self.account.force_commit().await?;
        self.token_account.force_commit().await?;
        self.token_mint.force_commit().await?;
        self.token_metadata.force_commit().await?;
        self.last_force_commit = Instant::now();
        Ok(())
    }

    async fn end(self) -> Result<()> {
        self.account.end().await?;
        self.token_account.end().await?;
        self.token_mint.end().await?;
        self.token_metadata.end().await?;
        Ok(())
    }
}

fn new_inserter<T: Row>(client: &Client, table: &str) -> Inserter<T> {
    client
        .inserter(table)
        .with_max_rows(MAX_BATCH_ROWS)
        .with_max_bytes(MAX_BATCH_BYTES)
}

async fn check_batch_limit<T: Row>(
    inserter: &mut Inserter<T>,
    rows_since_commit_check: &mut u16,
) -> Result<()> {
    *rows_since_commit_check += 1;
    if *rows_since_commit_check == BATCH_LIMIT_CHECK_INTERVAL {
        inserter.commit().await?;
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
        self.sink.maybe_force_commit().await?;
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

    /// The old mint and holder are unavailable from a canonical empty snapshot
    /// record, but remain available in the previous L1 row by pubkey.
    ///
    /// Zero-lamport malformed/uninitialized accounts can also reach this path.
    /// They are harmless: the later server-side lookup only creates a tombstone
    /// when the pubkey already has a live raw_token_account row.
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
    let pubkeys = closed_token_accounts.keys().collect::<Vec<_>>();
    if pubkeys.is_empty() {
        return Ok(0);
    }

    let batch_count = pubkeys.len().div_ceil(CLOSE_TOMBSTONE_BATCH_SIZE);
    let mut tombstone_insert: Inserter<TokenAccountRow> = new_inserter(client, TOKEN_ACCOUNT_TABLE);
    let mut marked_deleted = 0;

    for (batch_idx, pubkeys) in pubkeys.chunks(CLOSE_TOMBSTONE_BATCH_SIZE).enumerate() {
        info!(
            "[clickhouse] Checking tombstone candidates batch {}/{} ({} pubkeys)",
            batch_idx + 1,
            batch_count,
            pubkeys.len()
        );

        let mut live_rows = client
            .query(CLOSE_TOKEN_ACCOUNT_LIVE_ROWS_QUERY)
            .bind(pubkeys)
            .fetch_all::<TokenAccountRow>()
            .await
            .map_err(|err| {
                format!(
                    "tombstone candidate lookup failed for batch {}/{}: {}",
                    batch_idx + 1,
                    batch_count,
                    err
                )
            })?;

        for row in &mut live_rows {
            let candidate = closed_token_accounts
                .get(&row.pubkey)
                .ok_or_else(|| format!("missing tombstone candidate for {}", row.pubkey))?;
            let live_version = AccountVersion {
                updated_slot: row.updated_slot,
            };
            if *candidate <= live_version {
                warn!(
                    "[clickhouse] Skipping stale tombstone candidate: pubkey={} candidate_slot={} live_slot={}",
                    row.pubkey,
                    candidate.updated_slot,
                    live_version.updated_slot,
                );
                continue;
            }
            info!(
                "[clickhouse] Marking token account deleted: pubkey={} updated_slot={}",
                row.pubkey, candidate.updated_slot,
            );
            row.amount = 0;
            row.delegated_amount = 0;
            row.is_deleted = 1;
            row.updated_slot = candidate.updated_slot;
            tombstone_insert.write(row).await?;
            marked_deleted += 1;
        }

        if !live_rows.is_empty() {
            tombstone_insert.force_commit().await.map_err(|err| {
                format!(
                    "tombstone insert failed for batch {}/{}: {}",
                    batch_idx + 1,
                    batch_count,
                    err
                )
            })?;
        }
        info!(
            "[clickhouse] Tombstone candidate batch {}/{} matched {} live token accounts",
            batch_idx + 1,
            batch_count,
            live_rows.len()
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
    fn close_tombstone_lookup_query_selects_live_token_accounts() {
        assert!(CLOSE_TOKEN_ACCOUNT_LIVE_ROWS_QUERY.contains("FROM raw_token_account FINAL"));
        assert!(CLOSE_TOKEN_ACCOUNT_LIVE_ROWS_QUERY.contains("is_deleted"));
        assert!(CLOSE_TOKEN_ACCOUNT_LIVE_ROWS_QUERY.contains("pubkey IN ?"));
    }

    #[test]
    fn close_tombstone_batch_stays_below_default_clickhouse_query_limit() {
        let pubkeys = vec!["A".repeat(44); CLOSE_TOMBSTONE_BATCH_SIZE];
        let query = Client::default()
            .query(CLOSE_TOKEN_ACCOUNT_LIVE_ROWS_QUERY)
            .bind(&pubkeys);

        assert!(format!("{}", query.sql_display()).len() < 256 * 1024);
    }
}
