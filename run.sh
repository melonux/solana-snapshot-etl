#!/bin/bash

target/release/solana-snapshot-etl \
  --incremental-snapshot-dir /data/solana/snapshot \
  --last-processed-slot 441050694 \
  --clickhouse

