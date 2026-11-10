use crate::{
    deserialize_from, parse_append_vec_name, AccountsDbFields, AppendVec, AppendVecIterator,
    DeserializableVersionedBank, Result, SerializableAccountStorageEntry, SnapshotError,
    SnapshotExtractor,
};
use log::debug;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Component, Path};
use std::pin::Pin;
use std::time::{Duration, Instant};
use tar::{Archive, Entries, Entry};

// A single storage file can be many GiB. While it is decompressed, archive
// progress cannot advance because no complete AppendVec is available yet.
// Log each file explicitly so a long interval with a static progress bar is
// distinguishable from a stuck importer.
const APPEND_VEC_READ_HEARTBEAT: Duration = Duration::from_secs(5);

struct AppendVecReadProgress<'a, R> {
    reader: &'a mut R,
    slot: u64,
    id: u64,
    expected_len: u64,
    started: Instant,
    last_log: Instant,
    bytes_read: u64,
}

impl<'a, R: Read> AppendVecReadProgress<'a, R> {
    fn new(reader: &'a mut R, slot: u64, id: u64, expected_len: u64) -> Self {
        let now = Instant::now();
        Self {
            reader,
            slot,
            id,
            expected_len,
            started: now,
            last_log: now,
            bytes_read: 0,
        }
    }

    fn log_progress(&mut self) {
        let elapsed = self.started.elapsed();
        if self.last_log.elapsed() < APPEND_VEC_READ_HEARTBEAT {
            return;
        }
        self.last_log = Instant::now();
        let rate_mib = self.bytes_read as f64 / elapsed.as_secs_f64() / (1024.0 * 1024.0);
        debug!(
            "Reading AppendVec slot={} id={} progress={}/{} MiB rate={:.1} MiB/s",
            self.slot,
            self.id,
            self.bytes_read / (1024 * 1024),
            self.expected_len / (1024 * 1024),
            rate_mib,
        );
    }
}

impl<R: Read> Read for AppendVecReadProgress<'_, R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let read = self.reader.read(buffer)?;
        self.bytes_read += read as u64;
        self.log_progress();
        Ok(read)
    }
}

/// Extracts account data from a .tar.zst stream.
pub struct ArchiveSnapshotExtractor<Source>
where
    Source: Read + Unpin + 'static,
{
    snapshot_slot: u64,
    minimum_append_vec_slot: Option<u64>,
    accounts_db_fields: AccountsDbFields<SerializableAccountStorageEntry>,
    _archive: Pin<Box<Archive<zstd::Decoder<'static, BufReader<Source>>>>>,
    entries: Option<Entries<'static, zstd::Decoder<'static, BufReader<Source>>>>,
}

impl<Source> SnapshotExtractor for ArchiveSnapshotExtractor<Source>
where
    Source: Read + Unpin + 'static,
{
    fn iter(&mut self) -> AppendVecIterator<'_> {
        Box::new(self.unboxed_iter())
    }

    fn snapshot_slot(&self) -> u64 {
        self.snapshot_slot
    }

    fn append_vec_count_hint(&self) -> Option<u64> {
        // Recent Agave snapshots deliberately leave the storage list in the
        // bank snapshot empty. The archive entries themselves are the list.
        if self.accounts_db_fields.0.is_empty() {
            return None;
        }
        Some(
            self.accounts_db_fields
                .0
                .iter()
                .filter(|(slot, _)| {
                    should_process_append_vec_slot(self.minimum_append_vec_slot, **slot)
                })
                .map(|(_, entries)| entries.len() as u64)
                .sum(),
        )
    }
}

impl<Source> ArchiveSnapshotExtractor<Source>
where
    Source: Read + Unpin + 'static,
{
    pub fn from_reader(source: Source) -> Result<Self> {
        let tar_stream = zstd::stream::read::Decoder::new(source)?;
        let mut archive = Box::pin(Archive::new(tar_stream));

        // This is safe as long as we guarantee that entries never gets accessed past drop.
        let archive_static = unsafe { &mut *((&mut *archive) as *mut Archive<_>) };
        let mut entries = archive_static.entries()?;

        // Search for snapshot manifest.
        let mut snapshot_file: Option<Entry<_>> = None;
        for entry in entries.by_ref() {
            let entry = entry?;
            let path = entry.path()?;
            if Self::is_snapshot_manifest_file(&path) {
                snapshot_file = Some(entry);
                break;
            } else if Self::is_appendvec_file(&path) {
                // TODO Support archives where AppendVecs precede snapshot manifests
                return Err(SnapshotError::UnexpectedAppendVec);
            }
        }
        let snapshot_file = snapshot_file.ok_or(SnapshotError::NoSnapshotManifest)?;
        //let snapshot_file_len = snapshot_file.size();
        let snapshot_file_path = snapshot_file.path()?.as_ref().to_path_buf();

        debug!("Opening snapshot manifest: {:?}", &snapshot_file_path);
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

        Ok(ArchiveSnapshotExtractor {
            snapshot_slot,
            minimum_append_vec_slot: None,
            _archive: archive,
            accounts_db_fields,
            entries: Some(entries),
        })
    }

    fn unboxed_iter(&mut self) -> impl Iterator<Item = Result<AppendVec>> + '_ {
        let mut entries = self.entries.take().into_iter().flatten();
        std::iter::from_fn(move || loop {
            // `tar::Entries::next()` must consume any unread bytes from the
            // previous entry before it can read the next header. With a
            // non-seekable zstd stream this may decompress a large unused
            // AppendVec tail, so time this step separately from valid-data
            // copying in `process_entry`.
            let advance_started = Instant::now();
            let mut entry = match entries.next()? {
                Ok(x) => x,
                Err(e) => return Some(Err(e.into())),
            };
            let path = match entry.path() {
                Ok(x) => x,
                Err(e) => return Some(Err(e.into())),
            };
            let advance_elapsed = advance_started.elapsed();
            if advance_elapsed >= Duration::from_secs(5) {
                debug!(
                    "Advanced tar stream to {:?} in {:?} (may include skipped entry padding)",
                    path, advance_elapsed
                );
            }
            let (slot, id) = match path.file_name().and_then(parse_append_vec_name) {
                Some(value) => value,
                None => continue,
            };
            if !should_process_append_vec_slot(self.minimum_append_vec_slot, slot) {
                continue;
            }
            return Some(self.process_entry(&mut entry, slot, id));
        })
    }

    /// Skip AppendVec files from slots that have already been applied by the
    /// current database watermark. This is used for both incremental archives
    /// and a newer full archive discovered by snapshot-watch mode.
    pub fn with_minimum_append_vec_slot(mut self, slot: u64) -> Self {
        self.minimum_append_vec_slot = Some(slot);
        self
    }

    fn process_entry(
        &self,
        entry: &mut Entry<'static, zstd::Decoder<'static, BufReader<Source>>>,
        slot: u64,
        id: u64,
    ) -> Result<AppendVec> {
        let known_vecs = self
            .accounts_db_fields
            .0
            .get(&slot)
            .map(|v| &v[..])
            .unwrap_or(&[]);
        let known_vec = known_vecs.iter().find(|entry| entry.id == (id as usize));
        let entry_name = entry.path()?.to_string_lossy().into_owned();
        let archive_len = entry.size();
        let expected_len = match known_vec {
            Some(known_vec) => known_vec.accounts_current_len as u64,
            None if self.accounts_db_fields.0.is_empty() => archive_len,
            None => return Err(SnapshotError::UnexpectedAppendVec),
        };
        let valid_len = usize::try_from(expected_len).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "AppendVec length exceeds usize",
            )
        })?;
        let unused_tail = archive_len.saturating_sub(expected_len);
        let started = Instant::now();
        debug!(
            "[archive] Reading file={entry_name} slot={slot} id={id} archive_len={} bytes ({:.2} MiB) valid_len={} bytes ({:.2} MiB) unused_tail={} bytes ({:.2} MiB)",
            archive_len,
            archive_len as f64 / (1024.0 * 1024.0),
            expected_len,
            expected_len as f64 / (1024.0 * 1024.0),
            unused_tail,
            unused_tail as f64 / (1024.0 * 1024.0),
        );
        let mut progress_reader = AppendVecReadProgress::new(entry, slot, id, expected_len);
        let append_vec = AppendVec::new_from_reader(&mut progress_reader, valid_len, slot, id)?;
        let elapsed = started.elapsed();
        debug!(
            "[archive] Decompressed file={entry_name} slot={slot} id={id} valid_len={} bytes ({:.2} MiB) archive_len={} bytes ({:.2} MiB) unused_tail={} bytes ({:.2} MiB) elapsed={:?}",
            append_vec.len(),
            append_vec.len() as f64 / (1024.0 * 1024.0),
            archive_len,
            archive_len as f64 / (1024.0 * 1024.0),
            unused_tail,
            unused_tail as f64 / (1024.0 * 1024.0),
            elapsed
        );
        Ok(append_vec)
    }

    fn is_snapshot_manifest_file(path: &Path) -> bool {
        let mut components = path.components();
        if components.next() != Some(Component::Normal("snapshots".as_ref())) {
            return false;
        }
        let slot_number_str_1 = match components.next() {
            Some(Component::Normal(slot)) => slot,
            _ => return false,
        };
        // Check if slot number file is valid u64.
        if slot_number_str_1
            .to_str()
            .and_then(|s| s.parse::<u64>().ok())
            .is_none()
        {
            return false;
        }
        let slot_number_str_2 = match components.next() {
            Some(Component::Normal(slot)) => slot,
            _ => return false,
        };
        components.next().is_none() && slot_number_str_1 == slot_number_str_2
    }

    fn is_appendvec_file(path: &Path) -> bool {
        let mut components = path.components();
        if components.next() != Some(Component::Normal("accounts".as_ref())) {
            return false;
        }
        let name = match components.next() {
            Some(Component::Normal(c)) => c,
            _ => return false,
        };
        components.next().is_none() && parse_append_vec_name(name).is_some()
    }
}

fn should_process_append_vec_slot(minimum_append_vec_slot: Option<u64>, slot: u64) -> bool {
    minimum_append_vec_slot.map_or(true, |minimum_slot| slot > minimum_slot)
}

impl ArchiveSnapshotExtractor<File> {
    pub fn open(path: &Path) -> Result<Self> {
        Self::from_reader(File::open(path)?)
    }
}

#[cfg(test)]
mod tests {
    use super::should_process_append_vec_slot;

    #[test]
    #[ignore = "requires a local snapshot archive"]
    fn parses_sample_of_modern_snapshot_append_vecs() {
        use super::ArchiveSnapshotExtractor;
        use crate::SnapshotExtractor;
        #[cfg(feature = "standalone")]
        use solana_program::program_pack::Pack;
        use solana_sdk::pubkey::Pubkey;
        use std::{path::Path, str::FromStr};

        let path = std::env::var("SNAPSHOT_PROBE_PATH").expect("set SNAPSHOT_PROBE_PATH");
        let mut loader = ArchiveSnapshotExtractor::open(Path::new(&path)).unwrap();
        assert_eq!(loader.append_vec_count_hint(), None);
        let spl_token = Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").unwrap();
        let token_2022 = Pubkey::from_str("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb").unwrap();
        let mut files = 0;
        let mut accounts = 0u64;
        let mut token_accounts = 0u64;
        let mut token_mints = 0u64;
        let mut token_2022_accounts = 0u64;
        let mut bytes = 0u64;
        let mut largest_file = 0usize;
        #[cfg(feature = "standalone")]
        let mut token_unpack_ok = 0u64;
        #[cfg(feature = "standalone")]
        let mut token_unpack_failed = 0u64;
        #[cfg(feature = "standalone")]
        let mut mint_unpack_failed = 0u64;
        #[cfg(feature = "standalone")]
        let mut token_2022_unpack_ok = 0u64;
        #[cfg(feature = "standalone")]
        let mut token_2022_unpack_failed = 0u64;
        for append_vec in loader.iter().take(100) {
            let append_vec = append_vec.unwrap();
            let mut offset = 0;
            let mut file_accounts = 0;
            while offset < append_vec.len() {
                let (account, next) = append_vec.get_account(offset).unwrap_or_else(|| {
                    panic!(
                        "invalid record in slot={} id={} at offset={offset} of {}",
                        append_vec.slot(),
                        append_vec.id(),
                        append_vec.len()
                    )
                });
                assert!(next > offset && next <= append_vec.len());
                if account.account_meta.owner == spl_token && account.data.len() == 165 {
                    token_accounts += 1;
                    #[cfg(feature = "standalone")]
                    if spl_token::state::Account::unpack(account.data).is_ok() {
                        token_unpack_ok += 1;
                    } else {
                        token_unpack_failed += 1;
                    }
                }
                if account.account_meta.owner == spl_token && account.data.len() == 82 {
                    token_mints += 1;
                    #[cfg(feature = "standalone")]
                    if spl_token::state::Mint::unpack(account.data).is_err() {
                        mint_unpack_failed += 1;
                    }
                }
                if account.account_meta.owner == token_2022 {
                    token_2022_accounts += 1;
                    #[cfg(feature = "standalone")]
                    if spl_token_2022::extension::StateWithExtensions::<
                        spl_token_2022::state::Account,
                    >::unpack(account.data)
                    .is_ok()
                        || spl_token_2022::extension::StateWithExtensions::<
                            spl_token_2022::state::Mint,
                        >::unpack(account.data)
                        .is_ok()
                    {
                        token_2022_unpack_ok += 1;
                    } else {
                        token_2022_unpack_failed += 1;
                    }
                }
                file_accounts += 1;
                offset = next;
            }
            if append_vec.len() >= 1024 * 1024 {
                eprintln!(
                    "large file: slot={} id={} bytes={} accounts={file_accounts}",
                    append_vec.slot(),
                    append_vec.id(),
                    append_vec.len()
                );
            }
            bytes += append_vec.len() as u64;
            largest_file = largest_file.max(append_vec.len());
            accounts += file_accounts;
            files += 1;
        }
        eprintln!("sample: files={files} bytes={bytes} largest_file={largest_file} accounts={accounts} spl_token_accounts={token_accounts} spl_token_mints={token_mints} token_2022_owner={token_2022_accounts}");
        #[cfg(feature = "standalone")]
        eprintln!("SPL Token unpack: ok={token_unpack_ok} failed={token_unpack_failed}");
        #[cfg(feature = "standalone")]
        eprintln!(
            "SPL Token mint unpack: ok={} failed={mint_unpack_failed}",
            token_mints - mint_unpack_failed
        );
        #[cfg(feature = "standalone")]
        eprintln!(
            "Token-2022 base unpack: ok={token_2022_unpack_ok} failed={token_2022_unpack_failed}"
        );
        assert_eq!(files, 100);
        assert!(accounts > 0);
        assert!(token_accounts > 0);
        #[cfg(feature = "standalone")]
        assert!(token_unpack_ok > 0);
    }

    #[test]
    fn skips_append_vecs_from_already_processed_slots() {
        assert!(!should_process_append_vec_slot(Some(1_000), 999));
        assert!(!should_process_append_vec_slot(Some(1_000), 1_000));
        assert!(should_process_append_vec_slot(Some(1_000), 1_001));
        assert!(should_process_append_vec_slot(None, 1));
    }
}
