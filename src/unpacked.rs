use crate::{
    deserialize_from, parse_append_vec_name, AccountsDbFields, AppendVec, AppendVecIterator,
    DeserializableVersionedBank, ReadProgressTracking, Result, SerializableAccountStorageEntry,
    SnapshotError, SnapshotExtractor, SNAPSHOTS_DIR,
};
use log::{debug, warn};
use solana_runtime::snapshot_utils::SNAPSHOT_STATUS_CACHE_FILENAME;
use std::fs::OpenOptions;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Instant;

/// Extracts account data from snapshots that were unarchived to a file system.
pub struct UnpackedSnapshotExtractor {
    root: PathBuf,
    snapshot_slot: u64,
    accounts_db_fields: AccountsDbFields<SerializableAccountStorageEntry>,
}

impl SnapshotExtractor for UnpackedSnapshotExtractor {
    fn iter(&mut self) -> AppendVecIterator<'_> {
        self.unboxed_iter()
    }

    fn snapshot_slot(&self) -> u64 {
        self.snapshot_slot
    }

    fn append_vec_count_hint(&self) -> Option<u64> {
        self.root.join("accounts").read_dir().ok().map(|entries| {
            entries
                .filter_map(|entry| entry.ok())
                .filter(|entry| parse_append_vec_name(&entry.file_name()).is_some())
                .count() as u64
        })
    }
}

impl UnpackedSnapshotExtractor {
    pub fn open(path: &Path, progress_tracking: Box<dyn ReadProgressTracking>) -> Result<Self> {
        let snapshots_dir = path.join(SNAPSHOTS_DIR);
        let status_cache = snapshots_dir.join(SNAPSHOT_STATUS_CACHE_FILENAME);
        if !status_cache.is_file() {
            return Err(SnapshotError::NoStatusCache);
        }

        let latest_snapshot_slot = snapshots_dir
            .read_dir()?
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| u64::from_str(&entry.file_name().to_string_lossy()).ok())
            .max()
            .ok_or(SnapshotError::NoSnapshotManifest)?;

        let snapshot_file_path = snapshots_dir
            .join(latest_snapshot_slot.to_string())
            .join(latest_snapshot_slot.to_string());
        if !snapshot_file_path.is_file() {
            return Err(SnapshotError::NoSnapshotManifest);
        }

        debug!("Opening snapshot manifest: {:?}", snapshot_file_path);
        let snapshot_file = OpenOptions::new().read(true).open(&snapshot_file_path)?;
        let snapshot_file_len = snapshot_file.metadata()?.len();

        let snapshot_file = progress_tracking.new_read_progress_tracker(
            &snapshot_file_path,
            Box::new(snapshot_file),
            snapshot_file_len,
        );
        let mut snapshot_file = BufReader::new(snapshot_file);

        let pre_unpack = Instant::now();
        let versioned_bank: DeserializableVersionedBank = deserialize_from(&mut snapshot_file)?;
        let snapshot_slot = versioned_bank.slot;
        drop(versioned_bank);
        let versioned_bank_post_time = Instant::now();

        let accounts_db_fields: AccountsDbFields<SerializableAccountStorageEntry> =
            deserialize_from(&mut snapshot_file)?;
        let accounts_db_fields_post_time = Instant::now();
        drop(snapshot_file);

        debug!(
            "Read bank fields in {:?}",
            versioned_bank_post_time - pre_unpack
        );
        debug!(
            "Read accounts DB fields in {:?}",
            accounts_db_fields_post_time - versioned_bank_post_time
        );

        Ok(UnpackedSnapshotExtractor {
            root: path.to_path_buf(),
            snapshot_slot,
            accounts_db_fields,
        })
    }

    pub fn unboxed_iter(&self) -> AppendVecIterator<'_> {
        match self.iter_streams() {
            Ok(iter) => Box::new(iter),
            Err(err) => Box::new(std::iter::once(Err(err))),
        }
    }

    fn iter_streams(&self) -> Result<impl Iterator<Item = Result<AppendVec>> + '_> {
        let accounts_dir = self.root.join("accounts");
        let warn_accounts_dir = accounts_dir.clone();
        let parsed_files = accounts_dir
            .read_dir()?
            .filter_map(|f| f.ok())
            .filter_map(move |f| {
                let name = f.file_name();
                let parsed = parse_append_vec_name(&name);
                if parsed.is_none() {
                    warn!(
                        "Skipping non-appendvec file in accounts dir: {}",
                        warn_accounts_dir.join(&name).display()
                    );
                }
                parsed.map(|(slot, version)| (slot, version, accounts_dir.join(name)))
            })
            .collect::<Vec<_>>();

        let total_files = parsed_files.len();
        debug!("Found {} appendvec files to process", total_files);

        Ok(parsed_files
            .into_iter()
            .enumerate()
            .map(move |(idx, (slot, version, path))| {
                let processed = idx + 1;
                if total_files > 0 {
                    let percent = (processed as f64 * 100.0) / total_files as f64;
                    debug!(
                        "AppendVec progress: {}/{} ({:.2}%) file={}",
                        processed,
                        total_files,
                        percent,
                        path.display()
                    );
                }
                self.open_append_vec(slot, version, &path)
            }))
    }

    fn open_append_vec(&self, slot: u64, id: u64, path: &Path) -> Result<AppendVec> {
        let known_vecs = self
            .accounts_db_fields
            .0
            .get(&slot)
            .map(|v| &v[..])
            .unwrap_or(&[]);
        let known_vec = known_vecs.iter().find(|entry| entry.id == (id as usize));
        let known_vec = match known_vec {
            None => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "appendvec {} (slot={}, id={}) does not exist in snapshot manifest",
                        path.display(),
                        slot,
                        id
                    ),
                )
                .into())
            }
            Some(v) => v,
        };
        let archive_len = path.metadata()?.len();
        let valid_len = known_vec.accounts_current_len as u64;
        debug!(
            "[unpacked] Opening file={} slot={} id={} file_len={} MiB valid_len={} MiB unused_tail={} MiB",
            path.display(),
            slot,
            id,
            archive_len / (1024 * 1024),
            valid_len / (1024 * 1024),
            archive_len.saturating_sub(valid_len) / (1024 * 1024),
        );

        Ok(
            AppendVec::new_from_file(path, known_vec.accounts_current_len, slot, id).map_err(
                |e| {
                    std::io::Error::new(
                        e.kind(),
                        format!(
                        "failed to open/parse appendvec {} (slot={}, id={}, expected_len={}): {}",
                        path.display(),
                        slot,
                        id,
                        known_vec.accounts_current_len,
                        e
                    ),
                    )
                },
            )?,
        )
    }
}
