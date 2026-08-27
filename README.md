# Solana Snapshot ETL 📸

[![crates.io](https://img.shields.io/crates/v/solana-snapshot-etl?style=flat-square&logo=rust&color=blue)](https://crates.io/crates/solana-snapshot-etl)
[![docs.rs](https://img.shields.io/badge/docs.rs-solana--snapshot--etl-blue?style=flat-square&logo=docs.rs)](https://docs.rs/solana-snapshot-etl)
[![license](https://img.shields.io/badge/license-Apache--2.0-blue?style=flat-square)](#license)

**`solana-snapshot-etl` efficiently extracts all accounts in a snapshot** to load them into an external system.

## Project Status

This repository is a maintained fork of the original project by terorie:

- Original upstream (archived): https://github.com/riptl/solana-snapshot-etl
- This maintained fork: https://github.com/melonux/solana-snapshot-etl

The goal of this fork is to keep the ETL workflow usable with modern Solana/Agave snapshots and provide practical operational diagnostics.

### Key changes in this fork

- Updated AppendVec account layout parsing for modern snapshots.
- Added Token-2022 account decoding support (account, mint, multisig) in ClickHouse output.
- Added parser diagnostics and compatibility counters in ClickHouse summary logs.
- Added unpacked snapshot progress logging with total files and percentage processed.
- Added parallel AppendVec parsing and ClickHouse insertion.

## Motivation

Solana nodes periodically backup their account database into a `.tar.zst` "snapshot" stream.
If you run a node yourself, you've probably seen a snapshot file such as this one already:

```
snapshot-139240745-D17vR2iksG5RoLMfTX7i5NwSsr4VpbybuX1eqzesQfu2.tar.zst
```

A full snapshot file contains a copy of all accounts at a specific slot state (in this case slot `139240745`).

Historical accounts data is relevant to blockchain analytics use-cases and event tracing.
Despite archives being readily available, the ecosystem was missing an easy-to-use tool to access snapshot data.

## Building

```shell
cargo install --git https://github.com/melonux/solana-snapshot-etl --features=standalone --bins
```

## Usage

The ETL tool reads local snapshot archives or already-unpacked snapshot directories
and loads them into ClickHouse.

The basic command-line usage is as follows:

```
USAGE:
    solana-snapshot-etl [OPTIONS] <LOAD_FLAGS> <SOURCE>

    # or continuously consume incremental archives
    solana-snapshot-etl [OPTIONS] <LOAD_FLAGS> \
      --incremental-snapshot-dir <DIR>
```

### Sources

Extract from a local snapshot file:

```shell
solana-snapshot-etl /path/to/snapshot-*.tar.zst ...
```

Extract from an unpacked snapshot:

```shell
# Example unarchive command
tar -I zstd -xvf snapshot-*.tar.zst ./unpacked_snapshot/

solana-snapshot-etl ./unpacked_snapshot/
```

#### Snapshot watch directory

To continuously apply full and incremental snapshots, provide the directory. By default, the
watcher reads `max(updated_slot)` from `solana.raw_account`, rewinds 1,000 slots (never below
zero), and resumes from that slot. Change the rewind with `--resume-slot-rewind <SLOTS>`. The
producer should publish completed archives with an atomic rename. This mode currently writes to
ClickHouse only.

```shell
solana-snapshot-etl \
  --incremental-snapshot-dir /path/to/incremental-snapshots \
  --clickhouse
```

For a new database, add `--bootstrap`. It starts at slot 0 and requires a usable full snapshot; no
incremental snapshot is applied until that full snapshot has been imported.

```shell
solana-snapshot-etl \
  --incremental-snapshot-dir /path/to/incremental-snapshots \
  --bootstrap \
  --clickhouse
```

The importer recognizes both full files named
`snapshot-<slot>-<accounts-hash>.tar.zst` and incremental files named
`incremental-snapshot-<base-slot>-<slot>-<accounts-hash>.tar.zst`.

Outside bootstrap, each round first chooses an eligible incremental with the largest ending slot
(`base-slot <= resume-slot < slot`). If no incremental can be applied, it uses the newest full
snapshot beyond the current slot. This lets a full snapshot bridge a missing incremental base: with
current slot `1000`, incremental `[1100, 2000]`, and full snapshot `1100`, the importer loads the
full snapshot first, then applies the incremental. In bootstrap mode, only full snapshots are
eligible until one completes successfully.

While processing either archive type, it skips `accounts/<slot>.<id>` entries at slots at or below
the resume slot. Thus files at lower slots are ignored directly, while a full snapshot contributes
only account changes after the resume slot. Full archives use a fast ClickHouse path: Agave excludes
tombstones from full archives, so the importer does not perform any extra close-candidate pass for
them. Incremental archives retain the tombstone path because it is needed to delete token accounts
from the full base. Canonical empty accounts are appended directly as `is_deleted = 1` versions;
the importer does not issue a `raw_token_account FINAL` lookup. After a successful write, the
resume slot advances and the directory is scanned again. At startup, if no suitable archive can
advance the initial resume slot (including a slot gap with no bridging full snapshot), the watcher
reports the problem and exits. Once at least one archive has completed, it waits five seconds by
default when the next archive is not yet available; change this with
`--incremental-poll-interval-secs`. If an archive fails during ClickHouse processing, the watcher
reports the error and exits non-zero instead of retrying the same file (which could otherwise
duplicate a partial import).

### ClickHouse

Create the tables in [`docs/clickhouse_schema.md`](docs/clickhouse_schema.md) first, then put the
HTTP endpoint in a local `.env` file:

```shell
CLICKHOUSE_URL=http://user:password@clickhouse.example:8123
# Percent-encode URL-reserved characters in username or password, for example @ as %40.
```

Run the importer with `--clickhouse`:

```shell
solana-snapshot-etl snapshot-139240745-*.tar.zst --clickhouse
```

By default logs are written to stderr. Pass `--log-file` to write timestamped ETL logs to a file;
the file is truncated when the process starts, and the terminal remains available for progress bars:

```shell
./run.sh --log-file /tmp/solana-snapshot-etl.log
```

Detailed archive, worker, and ClickHouse diagnostics use the `debug` level. The default `info`
level keeps those messages out of the log; enable them explicitly with `--log-level debug` (or use
`--log-level trace` for the most verbose logger output). If `--log-level` is omitted, the existing
`RUST_LOG` setting is honored. The console prints a processing and completed line for every
full/incremental snapshot file.

At `debug` level, archive diagnostics include each AppendVec path, physical archive size, valid
size, and unused tail in exact bytes and MiB. ClickHouse diagnostics identify the worker, file
being processed, and each INSERT batch's table, row count, and byte count. File logs also include
the process ID and thread ID, which helps
identify messages if more than one invocation is running. The per-AppendVec messages are intentionally verbose, so use
the file logger while diagnosing and disable or revert them for a normal high-throughput run.

Rows are parsed and written directly to ClickHouse with HTTP `RowBinary`. Inserts are streamed and
committed per table at a 1,000,000-row or 256 MiB threshold. Every open HTTP request is also
committed within 15 seconds, even if rows are still arriving: sparse RowBinary rows can remain in
the client's 256 KiB buffer and otherwise leave ClickHouse with no body bytes for its default
30-second socket timeout. An idle worker flushes immediately as well. Each upload also requests
ClickHouse's `http_receive_timeout=600`, but the 15-second client-side limit keeps the importer
safe even if the server profile does not honor that per-query override.

Workers parse and upload concurrently. The expensive server-side INSERT finalization (MergeTree
part/projection creation after the RowBinary body is closed) is bounded by the worker count (at most
four). This avoids leaving already-uploaded HTTP requests idle behind a single finalization slot,
which can make ClickHouse close the request before it is finalized. When an input worker is idle,
its four table requests are finalized sequentially to avoid an unnecessary burst. The log records
when a worker waits for and acquires a finalization slot, plus the finalization time; the client-side
end timeout is 30 minutes so a pathological ClickHouse query fails visibly instead of leaving a
worker blocked forever. If a worker encounters an INSERT error, the producer is cancelled
immediately; it no longer silently drains the remaining AppendVec queue.

ClickHouse imports use two workers by default (the bundled `run.sh` currently opts into four; set
`CLICKHOUSE_WORKERS=2` to use the safer shared-host default). The tar.zst stream is read in order, but completed
AppendVecs are dispatched to independent parsers and ClickHouse inserters so decompression, base58
encoding, and server-side inserts overlap. Tune this for the ClickHouse host with
`--clickhouse-workers N` (or `CLICKHOUSE_WORKERS` when using `run.sh`). On a host that also runs
ClickHouse, start with `2` and do not exceed `4`; more streams can cause MergeTree part merges to
consume all disk I/O. Set it to `1` for the single-threaded path when diagnosing a problematic
server.

The importer leaves ClickHouse MergeTree background merges enabled for both full and incremental
snapshots; merge state does not change when switching between snapshot types.

If a snapshot was already imported but the CloseAccount tombstone pass failed, run only that pass:

```shell
solana-snapshot-etl snapshot-139240745-*.tar.zst --clickhouse-close-tombstones
```

This scans the snapshot's canonical empty accounts and appends `raw_token_account` rows with
`is_deleted = 1`; it does not re-insert raw or parsed account rows. Since a canonical empty
account does not contain the previous mint/owner, those fields are neutral empty values in the
tombstone row.
