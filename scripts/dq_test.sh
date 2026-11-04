#!/usr/bin/env bash
# Run one on_dq_fail scenario on Databricks (loads the sample file with 2 bad rows).
#   bash scripts/dq_test.sh drop | warn | fail
# Needs: databricks CLI logged in, `databricks bundle deploy` already done.
set -u
case "${1:-}" in drop|warn|fail) ;; *) echo "usage: $0 drop|warn|fail"; exit 2 ;; esac
land="dbfs:/Volumes/spec_driven/landing/raw/dq_$1"
databricks fs mkdir "$land"
databricks fs cp --overwrite sample_data/run1/transactions_001.csv "$land/"
databricks bundle run "dq_test_$1"
echo "job exit code: $?   (expected: non-zero for fail, 0 for drop/warn)"
