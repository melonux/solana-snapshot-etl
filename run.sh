#!/bin/bash
set -euo pipefail

# AppendVec decompression remains ordered, while account parsing and
# ClickHouse RowBinary uploads run in parallel workers.  Override this for a
# faster ClickHouse host, e.g. CLICKHOUSE_WORKERS=3 ./run.sh.  Two workers is
# the safe default when ClickHouse shares the host: more upload streams can
# overwhelm MergeTree background merges.
clickhouse_workers="${CLICKHOUSE_WORKERS:-4}"

# Keep one watcher from accidentally importing the same snapshot stream twice.
# The lock is released automatically when this process exits.
lock_file="${SNAPSHOT_ETL_LOCK_FILE:-/tmp/solana-snapshot-etl-watch.lock}"
exec 9>"$lock_file"
if ! flock -n 9; then
  echo "another solana-snapshot-etl watcher is already running (lock: $lock_file)" >&2
  exit 1
fi

# Repair only the derived hot tables after a raw full snapshot has already
# been imported.  This mode must not inherit the watch directory or
# --bootstrap flags below: it reads active raw tables in place and exits.
# Keep it behind the same lock so it cannot race a running watcher.
for arg in "$@"; do
  if [[ "$arg" == "--clickhouse-rebuild-hot" ]]; then
    exec target/release/solana-snapshot-etl \
      --clickhouse-rebuild-hot \
      --log-file ./solana-snapshot-etl.log \
      "$@"
  fi
done

exec target/release/solana-snapshot-etl \
  --incremental-snapshot-dir /data-static/solana/snapshot \
  --clickhouse \
  --clickhouse-workers "$clickhouse_workers" \
  --log-file ./solana-snapshot-etl.log \
  "$@"
