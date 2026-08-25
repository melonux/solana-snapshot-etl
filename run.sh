#!/bin/bash
set -euo pipefail

# AppendVec decompression remains ordered, while account parsing and
# ClickHouse RowBinary uploads run in parallel workers.  Override this for a
# slower/faster ClickHouse host, e.g. CLICKHOUSE_WORKERS=2 ./run.sh.


target/release/solana-snapshot-etl \
  --incremental-snapshot-dir /data/sl \
  --last-processed-slot 0 \
  --clickhouse \
  --clickhouse-workers 4
