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

exec target/release/solana-snapshot-etl \
  --incremental-snapshot-dir /data-static/solana/snapshot \
  --clickhouse \
  --clickhouse-workers "$clickhouse_workers" \
  --log-file ./solana-snapshot-etl.log \
  --bootstrap \
  "$@"
