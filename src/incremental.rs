//! Discovery and selection of full and incremental snapshot archives.

use solana_runtime::snapshot_archive_info::{
    FullSnapshotArchiveInfo, IncrementalSnapshotArchiveInfo, SnapshotArchiveInfoGetter,
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

/// Metadata encoded in a full `snapshot-*.tar.zst` filename.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FullSnapshot {
    path: PathBuf,
    slot: Slot,
}

impl FullSnapshot {
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn slot(&self) -> Slot {
        self.slot
    }

    fn from_path(path: PathBuf) -> Option<Self> {
        // ArchiveSnapshotExtractor reads zstd streams, so accept only the archive
        // format that the importer can consume.
        if !path.to_string_lossy().ends_with(".tar.zst") {
            return None;
        }

        let info = FullSnapshotArchiveInfo::new_from_path(path).ok()?;
        Some(Self {
            path: info.path().clone(),
            slot: info.slot(),
        })
    }
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

/// Return all complete, parseable full `.tar.zst` archives in `directory`.
///
/// Files that do not use the Solana full-snapshot naming convention are ignored.
pub fn discover_full(directory: &Path) -> io::Result<Vec<FullSnapshot>> {
    let snapshots = fs::read_dir(directory)?
        .filter_map(|entry| match entry {
            Ok(entry) => Some(entry),
            Err(_) => None,
        })
        .filter_map(|entry| match entry.file_type() {
            Ok(file_type) if file_type.is_file() => Some(entry.path()),
            _ => None,
        })
        .filter_map(FullSnapshot::from_path)
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

/// Full snapshots that can authoritatively advance the current state, sorted
/// with the furthest new slot first.  Full snapshots are considered only after
/// no usable incremental snapshot can be applied.
pub fn eligible_full_candidates(
    snapshots: Vec<FullSnapshot>,
    last_processed_slot: Slot,
) -> Vec<FullSnapshot> {
    let mut candidates = snapshots
        .into_iter()
        .filter(|snapshot| snapshot.slot() > last_processed_slot)
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        right
            .slot()
            .cmp(&left.slot())
            .then_with(|| left.path().cmp(right.path()))
    });
    candidates
}

/// Remove full or incremental archives that cannot add data beyond `last_processed_slot`.
///
/// Only archives whose names parse as supported `.tar.zst` archives are removed.
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
    for snapshot in discover_full(directory)? {
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

    fn full_snapshot(slot: Slot) -> FullSnapshot {
        let path = PathBuf::from(format!(
            "snapshot-{slot}-3dBjB2KwbPjeqjQwNzwx48qgK4hdkcw5uxmwcgDh5zkD.tar.zst"
        ));
        FullSnapshot::from_path(path).expect("test snapshot filename must parse")
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
    fn chooses_the_furthest_full_snapshot_that_advances_state() {
        let candidates = eligible_full_candidates(
            vec![
                full_snapshot(1_000),
                full_snapshot(1_100),
                full_snapshot(1_500),
            ],
            1_000,
        );

        assert_eq!(
            candidates
                .iter()
                .map(FullSnapshot::slot)
                .collect::<Vec<_>>(),
            [1_500, 1_100]
        );
    }

    #[test]
    fn full_snapshot_bridges_an_incremental_base_slot_gap() {
        let incremental = snapshot(1_100, 2_000);

        assert!(eligible_candidates(vec![incremental.clone()], 1_000).is_empty());
        assert_eq!(
            eligible_full_candidates(vec![full_snapshot(1_100)], 1_000)[0].slot(),
            1_100
        );
        assert_eq!(
            eligible_candidates(vec![incremental], 1_100)[0].slot(),
            2_000
        );
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
        let full_stale =
            directory.join("snapshot-1500-3dBjB2KwbPjeqjQwNzwx48qgK4hdkcw5uxmwcgDh5zkD.tar.zst");
        let full_future =
            directory.join("snapshot-2500-3dBjB2KwbPjeqjQwNzwx48qgK4hdkcw5uxmwcgDh5zkD.tar.zst");
        fs::write(&stale, []).unwrap();
        fs::write(&selected, []).unwrap();
        fs::write(&future, []).unwrap();
        fs::write(&full_stale, []).unwrap();
        fs::write(&full_future, []).unwrap();

        let removed = remove_processed(&directory, 2000).unwrap();
        assert_eq!(removed.len(), 3);
        assert!(!stale.exists());
        assert!(!selected.exists());
        assert!(!full_stale.exists());
        assert!(future.exists());
        assert!(full_future.exists());

        fs::remove_dir_all(directory).unwrap();
    }
}
