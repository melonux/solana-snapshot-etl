use crate::clickhouse::ClickhouseIndexer;
use crate::csv::CsvDumper;
use crate::geyser::GeyserDumper;
use crate::geyser_plugin::load_plugin;
use crate::programs::ProgramDumper;
use crate::sqlite::SqliteIndexer;
use clap::{ArgGroup, Parser};
use indicatif::{ProgressBar, ProgressBarIter, ProgressStyle};
use log::{error, info, warn};
use reqwest::blocking::Response;
use solana_snapshot_etl::archived::ArchiveSnapshotExtractor;
use solana_snapshot_etl::parallel::AppendVecConsumer;
use solana_snapshot_etl::unpacked::UnpackedSnapshotExtractor;
use solana_snapshot_etl::{AppendVecIterator, ReadProgressTracking, SnapshotExtractor};
use std::fs::{File, OpenOptions};
use std::io::{stdout, IoSliceMut, Read, Write};
use std::path::{Path, PathBuf};

mod clickhouse;
mod csv;
mod geyser;
mod geyser_plugin;
mod mpl_metadata;
mod programs;
mod sqlite;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
#[clap(group(
    ArgGroup::new("action")
        .required(true)
        .args(&["csv", "geyser", "sqlite-out", "clickhouse", "programs-out"]),
))]
struct Args {
    #[clap(help = "Snapshot source (unpacked snapshot, archive file, or HTTP link)")]
    source: String,
    #[clap(long, action, help = "Write CSV to stdout")]
    csv: bool,
    #[clap(long, help = "Export to new SQLite3 DB at this path")]
    sqlite_out: Option<String>,
    #[clap(
        long,
        action,
        help = "Write to ClickHouse configured by CLICKHOUSE_URL"
    )]
    clickhouse: bool,
    #[clap(long, help = "SQLite3 cache size in MB")]
    sqlite_cache_size: Option<i64>,
    #[clap(long, action, help = "Index token program data")]
    tokens: bool,
    #[clap(long, help = "Load Geyser plugin from given config file")]
    geyser: Option<String>,
    #[clap(long, help = "Write programs tar stream")]
    programs_out: Option<String>,
}

fn main() {
    env_logger::init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"),
    );
    if let Err(e) = _main() {
        error!("{}", e);
        std::process::exit(1);
    }
}

fn _main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let mut loader = SupportedLoader::new(&args.source, Box::new(LoadProgressTracking {}))?;
    if args.csv {
        info!("Dumping to CSV");
        let mut writer = CsvDumper::new();
        let mut skipped_append_vecs = 0u64;
        for (append_vec_idx, append_vec) in loader.iter().enumerate() {
            match append_vec {
                Ok(append_vec) => writer.dump_append_vec(append_vec),
                Err(err) => {
                    skipped_append_vecs += 1;
                    warn!("[csv] Skipping append vec #{}: {}", append_vec_idx, err);
                }
            }
        }
        drop(writer);
        info!("[csv] Skipped {} append vec files", skipped_append_vecs);
        println!("Done!");
    }
    if let Some(geyser_config_path) = args.geyser {
        info!("Dumping to Geyser plugin: {}", &geyser_config_path);
        let plugin = unsafe { load_plugin(&geyser_config_path)? };
        assert!(
            plugin.account_data_notifications_enabled(),
            "Geyser plugin does not accept account data notifications"
        );
        let mut dumper = GeyserDumper::new(plugin);
        let mut skipped_append_vecs = 0u64;
        for (append_vec_idx, append_vec) in loader.iter().enumerate() {
            match append_vec {
                Ok(append_vec) => dumper.on_append_vec(append_vec)?,
                Err(err) => {
                    skipped_append_vecs += 1;
                    warn!("[geyser] Skipping append vec #{}: {}", append_vec_idx, err);
                }
            }
        }
        drop(dumper);
        info!("[geyser] Skipped {} append vec files", skipped_append_vecs);
        println!("Done!");
    }
    if args.clickhouse {
        dotenvy::dotenv().ok();
        let clickhouse_url = std::env::var("CLICKHOUSE_URL")
            .map_err(|_| "CLICKHOUSE_URL must be set in the environment or .env file")?;
        let snapshot_slot = loader.snapshot_slot();
        let append_vec_count = loader.append_vec_count_hint();
        info!("Dumping snapshot slot {} to ClickHouse", snapshot_slot);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let stats = runtime.block_on(
            ClickhouseIndexer::new(clickhouse_url, snapshot_slot, append_vec_count)?
                .insert_all(loader.iter()),
        )?;
        log_clickhouse_index_stats(&stats);
    }
    if let Some(sqlite_out_path) = args.sqlite_out {
        info!("Dumping to SQLite3: {}", &sqlite_out_path);
        let db_path = PathBuf::from(sqlite_out_path);
        if db_path.exists() {
            return Err("Refusing to overwrite database that already exists".into());
        }

        let mut indexer = SqliteIndexer::new(db_path)?;
        if let Some(cache_size) = args.sqlite_cache_size {
            indexer.set_cache_size(cache_size)?;
        }
        let stats = indexer.insert_all(loader.iter())?;

        info!("Done!");
        info!("Dumped {} accounts", stats.accounts_total);
        info!("Dumped {} token accounts", stats.token_accounts_total);
        info!("Skipped {} append vec files", stats.skipped_append_vecs);
        info!("Processed {} append vec files", stats.append_vecs_total);
        info!(
            "Non-empty append vec files producing 0 accounts: {}",
            stats.nonempty_zero_account_append_vecs
        );
        info!(
            "SPL-Token owner accounts seen: {}",
            stats.spl_token_owner_accounts_seen
        );
        info!(
            "SPL-Token accounts parsed successfully: {}",
            stats.spl_token_accounts_parsed
        );
        info!(
            "SPL-Token accounts with unexpected size: {}",
            stats.spl_token_unexpected_size
        );
        info!(
            "SPL-Token accounts with unpack failure: {}",
            stats.spl_token_unpack_failed
        );
        info!(
            "Token-2022 owner accounts seen (currently not decoded): {}",
            stats.token_2022_owner_accounts_seen
        );
        info!(
            "Token-2022 accounts parsed successfully: {}",
            stats.token_2022_accounts_parsed
        );
        info!(
            "Token-2022 accounts with unexpected size: {}",
            stats.token_2022_unexpected_size
        );
        info!(
            "Token-2022 accounts with unpack failure: {}",
            stats.token_2022_unpack_failed
        );
    }
    if let Some(programs) = args.programs_out {
        info!("Dumping program accounts to {}", &programs);
        let writer: Box<dyn Write> = if programs == "-" {
            Box::new(stdout())
        } else {
            Box::new(
                OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(programs)?,
            )
        };
        let mut dumper = ProgramDumper::new(writer);
        let mut skipped_append_vecs = 0u64;
        for (append_vec_idx, append_vec) in loader.iter().enumerate() {
            match append_vec {
                Ok(append_vec) => dumper.on_append_vec(append_vec)?,
                Err(err) => {
                    skipped_append_vecs += 1;
                    warn!(
                        "[programs] Skipping append vec #{}: {}",
                        append_vec_idx, err
                    );
                }
            }
        }
        drop(dumper);
        info!(
            "[programs] Skipped {} append vec files",
            skipped_append_vecs
        );
        info!("Done!");
    }
    Ok(())
}

fn log_clickhouse_index_stats(stats: &crate::clickhouse::IndexStats) {
    info!("[clickhouse] Dumped {} accounts", stats.accounts_total);
    info!(
        "[clickhouse] Dumped {} token accounts",
        stats.token_accounts_total
    );
    info!(
        "[clickhouse] Skipped {} append vec files",
        stats.skipped_append_vecs
    );
    info!(
        "[clickhouse] Processed {} append vec files",
        stats.append_vecs_total
    );
    info!(
        "[clickhouse] Non-empty append vec files producing 0 accounts: {}",
        stats.nonempty_zero_account_append_vecs
    );
    info!(
        "[clickhouse] SPL-Token owner accounts seen: {}",
        stats.spl_token_owner_accounts_seen
    );
    info!(
        "[clickhouse] SPL-Token accounts parsed successfully: {}",
        stats.spl_token_accounts_parsed
    );
    info!(
        "[clickhouse] SPL-Token accounts with unexpected size: {}",
        stats.spl_token_unexpected_size
    );
    info!(
        "[clickhouse] SPL-Token accounts with unpack failure: {}",
        stats.spl_token_unpack_failed
    );
    info!(
        "[clickhouse] Token-2022 owner accounts seen: {}",
        stats.token_2022_owner_accounts_seen
    );
    info!(
        "[clickhouse] Token-2022 accounts parsed successfully: {}",
        stats.token_2022_accounts_parsed
    );
    info!(
        "[clickhouse] Token-2022 accounts with unexpected size: {}",
        stats.token_2022_unexpected_size
    );
    info!(
        "[clickhouse] Token-2022 accounts with unpack failure: {}",
        stats.token_2022_unpack_failed
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
    ArchiveDownload(ArchiveSnapshotExtractor<Response>),
}

impl SupportedLoader {
    fn new(
        source: &str,
        progress_tracking: Box<dyn ReadProgressTracking>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        if source.starts_with("http://") || source.starts_with("https://") {
            Self::new_download(source)
        } else {
            Self::new_file(source.as_ref(), progress_tracking).map_err(Into::into)
        }
    }

    fn new_download(url: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let resp = reqwest::blocking::get(url)?;
        let loader = ArchiveSnapshotExtractor::from_reader(resp)?;
        info!("Streaming snapshot from HTTP");
        Ok(Self::ArchiveDownload(loader))
    }

    fn new_file(
        path: &Path,
        progress_tracking: Box<dyn ReadProgressTracking>,
    ) -> solana_snapshot_etl::Result<Self> {
        Ok(if path.is_dir() {
            info!("Reading unpacked snapshot");
            Self::Unpacked(UnpackedSnapshotExtractor::open(path, progress_tracking)?)
        } else {
            info!("Reading snapshot archive");
            Self::ArchiveFile(ArchiveSnapshotExtractor::open(path)?)
        })
    }
}

impl SnapshotExtractor for SupportedLoader {
    fn iter(&mut self) -> AppendVecIterator<'_> {
        match self {
            SupportedLoader::Unpacked(loader) => Box::new(loader.iter()),
            SupportedLoader::ArchiveFile(loader) => Box::new(loader.iter()),
            SupportedLoader::ArchiveDownload(loader) => Box::new(loader.iter()),
        }
    }

    fn snapshot_slot(&self) -> u64 {
        match self {
            SupportedLoader::Unpacked(loader) => loader.snapshot_slot(),
            SupportedLoader::ArchiveFile(loader) => loader.snapshot_slot(),
            SupportedLoader::ArchiveDownload(loader) => loader.snapshot_slot(),
        }
    }

    fn append_vec_count_hint(&self) -> Option<u64> {
        match self {
            SupportedLoader::Unpacked(loader) => loader.append_vec_count_hint(),
            SupportedLoader::ArchiveFile(loader) => loader.append_vec_count_hint(),
            SupportedLoader::ArchiveDownload(loader) => loader.append_vec_count_hint(),
        }
    }
}
