#!/bin/bash

target/release/solana-snapshot-etl \
  --incremental-snapshot-dir /data-static/solana/snapshot \
  --last-processed-slot 0 \
  --clickhouse

