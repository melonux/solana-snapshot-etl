//! Discovery and selection of incremental snapshot archives.

use solana_runtime::snapshot_archive_info::{
    IncrementalSnapshotArchiveInfo, SnapshotArchiveInfoGetter,
};
use solana_sdk::clock::Slot;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

/// Metadata encoded in an `incremental-snapshot-*.tar.zst` filename.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IncrementalSnapshot {
    path: PathBuf,
    base_slot: Slot,
    slot: Slot,
}

impl IncrementalSnapshot {
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn base_slot(&self) -> Slot {
        self.base_slot
    }

    pub fn slot(&self) -> Slot {
        self.slot
    }

    fn from_path(path: PathBuf) -> Option<Self> {
        // This mode intentionally accepts only the format requested by the CLI.  The Solana
        // parser below still validates the complete filename and parses its base58 hash.
        if !path.to_string_lossy().ends_with(".tar.zst") {
            return None;
        }

        let info = IncrementalSnapshotArchiveInfo::new_from_path(path).ok()?;
        Some(Self {
            path: info.path().clone(),
            base_slot: info.base_slot(),
            slot: info.slot(),
        })
    }
}

/// Return all complete, parseable incremental `.tar.zst` archives in `directory`.
///
/// Files that do not use the Solana incremental-snapshot naming convention are ignored.
pub fn discover(directory: &Path) -> io::Result<Vec<IncrementalSnapshot>> {
    let snapshots = fs::read_dir(directory)?
        .filter_map(|entry| match entry {
            Ok(entry) => Some(entry),
            Err(_) => None,
        })
        .filter_map(|entry| match entry.file_type() {
            Ok(file_type) if file_type.is_file() => Some(entry.path()),
            _ => None,
        })
        .filter_map(IncrementalSnapshot::from_path)
        .collect::<Vec<_>>();
    Ok(snapshots)
}

/// Eligible archives sorted by preference: the archive ending at the highest new slot comes
/// first.  A stable pathname tie-breaker makes retries deterministic.
pub fn eligible_candidates(
    snapshots: Vec<IncrementalSnapshot>,
    last_processed_slot: Slot,
) -> Vec<IncrementalSnapshot> {
    let mut candidates = snapshots
        .into_iter()
        .filter(|snapshot| {
            snapshot.base_slot() <= last_processed_slot && snapshot.slot() > last_processed_slot
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        right
            .slot()
            .cmp(&left.slot())
            .then_with(|| left.path().cmp(right.path()))
    });
    candidates
}

/// Remove archives that cannot add data beyond `last_processed_slot`.
///
/// Only archives whose names parse as supported incremental `.tar.zst` archives are removed.
pub fn remove_processed(directory: &Path, last_processed_slot: Slot) -> io::Result<Vec<PathBuf>> {
    let mut removed = Vec::new();
    for snapshot in discover(directory)? {
        if snapshot.slot() <= last_processed_slot {
            match fs::remove_file(snapshot.path()) {
                Ok(()) => removed.push(snapshot.path().to_path_buf()),
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Err(err) => return Err(err),
            }
        }
    }
    Ok(removed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn snapshot(base_slot: Slot, slot: Slot) -> IncrementalSnapshot {
        let path = PathBuf::from(format!(
            "incremental-snapshot-{base_slot}-{slot}-3dBjB2KwbPjeqjQwNzwx48qgK4hdkcw5uxmwcgDh5zkD.tar.zst"
        ));
        IncrementalSnapshot::from_path(path).expect("test snapshot filename must parse")
    }

    #[test]
    fn chooses_the_furthest_eligible_snapshot() {
        let snapshots = vec![
            snapshot(900, 1500),
            snapshot(950, 1550),
            snapshot(950, 2000),
            snapshot(1100, 2500),
        ];

        let candidates = eligible_candidates(snapshots, 1000);
        assert_eq!(candidates[0].base_slot(), 950);
        assert_eq!(candidates[0].slot(), 2000);
        assert_eq!(candidates.len(), 3);
    }

    #[test]
    fn previously_future_snapshot_becomes_eligible_after_progress() {
        let candidates = eligible_candidates(vec![snapshot(1100, 2500)], 2000);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].slot(), 2500);
    }

    #[test]
    fn removes_only_archives_that_cannot_add_new_slots() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "solana-snapshot-etl-incremental-test-{}-{}",
            std::process::id(),
            unique
        ));
        fs::create_dir(&directory).unwrap();

        let stale = directory.join(
            "incremental-snapshot-900-1500-3dBjB2KwbPjeqjQwNzwx48qgK4hdkcw5uxmwcgDh5zkD.tar.zst",
        );
        let selected = directory.join(
            "incremental-snapshot-950-2000-3dBjB2KwbPjeqjQwNzwx48qgK4hdkcw5uxmwcgDh5zkD.tar.zst",
        );
        let future = directory.join(
            "incremental-snapshot-1100-2500-3dBjB2KwbPjeqjQwNzwx48qgK4hdkcw5uxmwcgDh5zkD.tar.zst",
        );
        fs::write(&stale, []).unwrap();
        fs::write(&selected, []).unwrap();
        fs::write(&future, []).unwrap();

        let removed = remove_processed(&directory, 2000).unwrap();
        assert_eq!(removed.len(), 2);
        assert!(!stale.exists());
        assert!(!selected.exists());
        assert!(future.exists());

        fs::remove_dir_all(directory).unwrap();
    }
}
