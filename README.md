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
- Added Token-2022 account decoding support (account, mint, multisig) in SQLite output.
- Added parser diagnostics and compatibility counters in SQLite summary logs.
- Added unpacked snapshot progress logging with total files and percentage processed.
- Clarified CSV behavior (writes to stdout).

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

The ETL tool can extract snapshots from a variety of streaming sources
and load them into one of the supported storage backends.

The basic command-line usage is as follows:

```
USAGE:
    solana-snapshot-etl [OPTIONS] <LOAD_FLAGS> <SOURCE>

    # or continuously consume incremental archives
    solana-snapshot-etl [OPTIONS] <LOAD_FLAGS> \
      --incremental-snapshot-dir <DIR> --last-processed-slot <SLOT>
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

Stream snapshot from HTTP source or S3 bucket:

```shell
solana-snapshot-etl 'https://my-solana-node.bdnodes.net/snapshot.tar.zst?auth=xxx' ...
```

#### Snapshot watch directory

To continuously apply full and incremental snapshots to an already indexed slot, provide the
directory and the highest slot already processed. The producer should publish completed archives
with an atomic rename. This mode currently writes to ClickHouse only.

```shell
solana-snapshot-etl \
  --incremental-snapshot-dir /path/to/incremental-snapshots \
  --last-processed-slot 441050694 \
  --clickhouse
```

The importer recognizes both full files named
`snapshot-<slot>-<accounts-hash>.tar.zst` and incremental files named
`incremental-snapshot-<base-slot>-<slot>-<accounts-hash>.tar.zst`.

In each round it first chooses an eligible incremental with the largest ending slot
(`base-slot <= last-processed-slot < slot`). If no incremental can be applied, it uses the newest
full snapshot beyond the current slot. This lets a full snapshot bridge a missing incremental base:
with current slot `1000`, incremental `[1100, 2000]`, and full snapshot `1100`, the importer loads
the full snapshot first, then applies the incremental.

While processing either archive type, it skips `accounts/<slot>.<id>` entries at slots already
processed. Thus a full snapshot only contributes the account changes after the current slot, and
CloseAccount records follow the same tombstone path as in an incremental archive. After a
successful write, the current slot advances, all recognized full and incremental archives ending
at or below it are deleted, and the directory is scanned again. If no usable archive is available,
it waits five seconds by default; change this with `--incremental-poll-interval-secs`.

### Targets

#### SQLite3 (recommended)

The fastest way to access snapshot data is the SQLite3 load mechanism.

The resulting SQLite database file can be loaded using any SQLite client library.

```shell
solana-snapshot-etl snapshot-139240745-*.tar.zst --sqlite-out snapshot.db
```

The resulting SQLite database contains the following tables.

- `account`
- `token_account` (SPL Token Program and Token-2022 Program)
- `token_mint` (SPL Token Program and Token-2022 Program)
- `token_multisig` (SPL Token Program and Token-2022 Program)
- `token_metadata` (MPL Metadata Program)

#### ClickHouse

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

Rows are parsed and written directly to ClickHouse with HTTP `RowBinary`. Inserts are streamed and
committed per table at a 250,000-row or 64 MiB threshold; no SQLite database is created.

If a snapshot was already imported but the CloseAccount tombstone pass failed, run only that pass:

```shell
solana-snapshot-etl snapshot-139240745-*.tar.zst --clickhouse-close-tombstones
```

This scans the snapshot's canonical empty accounts and updates matching existing
`raw_token_account` rows with `is_deleted = 1`; it does not re-insert raw or parsed account
rows.

#### CSV

The CSV target writes records to stdout. Redirect stdout to save into a file.

```shell
solana-snapshot-etl snapshot-139240745-*.tar.zst --csv > snapshot.csv
```

#### Geyser plugin

Much like `solana-validator`, this tool can write account updates to Geyser plugins.

```shell
solana-snapshot-etl snapshot-139240745-*.tar.zst --geyser plugin-config.json
```

For more info, consult Solana's docs: https://docs.solana.com/developing/plugins/geyser-plugins

#### Dump programs

The `--programs-out` flag exports all Solana programs (in ELF format).

```shell
solana-snapshot-etl snapshot-139240745-*.tar.zst --programs-out programs.tar
```

or to extract in place

```shell
solana-snapshot-etl snapshot-139240745-*.tar.zst --programs-out - | tar -xv
```
