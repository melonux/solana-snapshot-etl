use borsh::BorshDeserialize;
use clickhouse::inserter::Inserter;
use clickhouse::{Client, Row};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use log::warn;
use percent_encoding::percent_decode_str;
use serde::Serialize;
use solana_sdk::program_pack::Pack;
use solana_sdk::pubkey::Pubkey;
use solana_snapshot_etl::append_vec::{AppendVec, StoredAccountMeta};
use solana_snapshot_etl::{append_vec_iter, AppendVecIterator};
use std::rc::Rc;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use url::Url;

use crate::mpl_metadata;

const DATABASE: &str = "solana";
const ACCOUNT_TABLE: &str = "account";
const TOKEN_ACCOUNT_TABLE: &str = "raw_token_account";
const TOKEN_MINT_TABLE: &str = "raw_token_mint";
const TOKEN_METADATA_TABLE: &str = "raw_token_metadata";

// Larger inserts reduce MergeTree part creation while RowBinary is streamed in 256 KiB chunks by
// the client, so these limits do not retain the complete batch in process memory.
const MAX_BATCH_ROWS: u64 = 250_000;
const MAX_BATCH_BYTES: u64 = 64 * 1024 * 1024;
const COMMIT_CHECK_INTERVAL: u16 = 1_024;

pub(crate) type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub(crate) struct ClickhouseIndexer {
    sink: ClickhouseSink,
    snapshot_slot: u64,
    multi_progress: MultiProgress,
    progress: Arc<Progress>,
}

struct Progress {
    accounts: ProgressCounter,
    tokens: ProgressCounter,
    metadata: ProgressCounter,
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
    updated_slot: u64,
}

#[derive(Row, Serialize)]
struct TokenMintRow {
    mint: String,
    supply: u64,
    decimals: u8,
    is_initialized: bool,
    updated_slot: u64,
}

#[derive(Row, Serialize)]
struct TokenMetadataRow {
    mint: String,
    name: String,
    symbol: String,
    uri: String,
    is_mutable: bool,
    updated_slot: u64,
}

impl ClickhouseIndexer {
    pub(crate) fn new(connection_url: String, snapshot_slot: u64) -> Result<Self> {
        let spinner_style = ProgressStyle::with_template(
            "{prefix:>13.bold.dim} {spinner} rate={per_sec:>13} total={human_pos:>11}",
        )?;
        let multi_progress = MultiProgress::new();
        let progress = Arc::new(Progress {
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

        Ok(Self {
            sink: ClickhouseSink::new(&new_clickhouse_client(&connection_url)?),
            snapshot_slot,
            multi_progress,
            progress,
        })
    }

    pub(crate) async fn insert_all(
        mut self,
        iterator: AppendVecIterator<'_>,
    ) -> Result<IndexStats> {
        let mut worker = Worker {
            sink: &mut self.sink,
            snapshot_slot: self.snapshot_slot,
            progress: Arc::clone(&self.progress),
            spl_token_owner_accounts_seen: 0,
            spl_token_accounts_parsed: 0,
            spl_token_unexpected_size: 0,
            spl_token_unpack_failed: 0,
            token_2022_owner_accounts_seen: 0,
            token_2022_accounts_parsed: 0,
            token_2022_unexpected_size: 0,
            token_2022_unpack_failed: 0,
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
        }

        let spl_token_owner_accounts_seen = worker.spl_token_owner_accounts_seen;
        let spl_token_accounts_parsed = worker.spl_token_accounts_parsed;
        let spl_token_unexpected_size = worker.spl_token_unexpected_size;
        let spl_token_unpack_failed = worker.spl_token_unpack_failed;
        let token_2022_owner_accounts_seen = worker.token_2022_owner_accounts_seen;
        let token_2022_accounts_parsed = worker.token_2022_accounts_parsed;
        let token_2022_unexpected_size = worker.token_2022_unexpected_size;
        let token_2022_unpack_failed = worker.token_2022_unpack_failed;
        drop(worker);

        self.sink.end().await?;
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
    if *rows_since_commit_check == COMMIT_CHECK_INTERVAL {
        inserter.commit().await?;
        *rows_since_commit_check = 0;
    }
    Ok(())
}

struct Worker<'a> {
    sink: &'a mut ClickhouseSink,
    snapshot_slot: u64,
    progress: Arc<Progress>,
    spl_token_owner_accounts_seen: u64,
    spl_token_accounts_parsed: u64,
    spl_token_unexpected_size: u64,
    spl_token_unpack_failed: u64,
    token_2022_owner_accounts_seen: u64,
    token_2022_accounts_parsed: u64,
    token_2022_unexpected_size: u64,
    token_2022_unpack_failed: u64,
}

impl<'a> Worker<'a> {
    async fn on_append_vec_count(&mut self, append_vec: AppendVec) -> Result<u64> {
        let append_vec_len = append_vec.len();
        let append_vec = Rc::new(append_vec);
        let mut parsed_accounts = 0;

        for account in append_vec_iter(Rc::clone(&append_vec)) {
            self.insert_account(&account.access().ok_or("invalid account access")?)
                .await?;
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

    async fn insert_account(&mut self, account: &StoredAccountMeta<'_>) -> Result<()> {
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

        if account.account_meta.owner == spl_token::id() {
            self.spl_token_owner_accounts_seen += 1;
            self.insert_spl_token(account).await?;
        } else if account.account_meta.owner == *token_2022_program_id() {
            self.token_2022_owner_accounts_seen += 1;
            self.insert_token_2022(account).await?;
        }

        if account.account_meta.owner == mpl_metadata::id() {
            self.insert_token_metadata(account).await?;
        }

        self.progress.accounts.inc();
        Ok(())
    }

    async fn insert_spl_token(&mut self, account: &StoredAccountMeta<'_>) -> Result<()> {
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
                                updated_slot: self.snapshot_slot,
                            })
                            .await?;
                        self.spl_token_accounts_parsed += 1;
                        self.progress.tokens.inc();
                    }
                    Err(_) => self.spl_token_unpack_failed += 1,
                }
            }
            spl_token::state::Mint::LEN => match spl_token::state::Mint::unpack(account.data) {
                Ok(token_mint) => {
                    self.sink
                        .write_token_mint(&TokenMintRow {
                            mint: pubkey_string(account.meta.pubkey),
                            supply: token_mint.supply,
                            decimals: token_mint.decimals,
                            is_initialized: token_mint.is_initialized,
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

    async fn insert_token_2022(&mut self, account: &StoredAccountMeta<'_>) -> Result<()> {
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
                                updated_slot: self.snapshot_slot,
                            })
                            .await?;
                        self.token_2022_accounts_parsed += 1;
                        self.progress.tokens.inc();
                    }
                    Err(_) => self.token_2022_unpack_failed += 1,
                }
            }
            spl_token_2022::state::Mint::LEN => {
                match spl_token_2022::state::Mint::unpack(account.data) {
                    Ok(token_mint) => {
                        self.sink
                            .write_token_mint(&TokenMintRow {
                                mint: pubkey_string(account.meta.pubkey),
                                supply: token_mint.supply,
                                decimals: token_mint.decimals,
                                is_initialized: token_mint.is_initialized,
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

        let metadata = mpl_metadata::Metadata::deserialize(&mut data).map_err(|err| {
            format!(
                "Invalid token-metadata v1 metadata account {}: {}",
                account.meta.pubkey, err
            )
        })?;
        self.sink
            .write_token_metadata(&TokenMetadataRow {
                mint: pubkey_string(metadata.mint),
                name: metadata.data.name,
                symbol: metadata.data.symbol,
                uri: metadata.data.uri,
                is_mutable: metadata.is_mutable,
                updated_slot: self.snapshot_slot,
            })
            .await?;
        self.progress.metadata.inc();
        Ok(())
    }
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
        self.count.fetch_add(1, Ordering::Relaxed);
        self.progress_bar.inc(1);
    }

    fn get(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
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
                "updated_slot",
            ]
        );
        assert_eq!(
            <TokenMintRow as Row>::COLUMN_NAMES,
            [
                "mint",
                "supply",
                "decimals",
                "is_initialized",
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
                "is_mutable",
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
}
