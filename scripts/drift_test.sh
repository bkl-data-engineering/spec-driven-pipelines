#!/usr/bin/env bash
# Run one schema-drift scenario end to end on Databricks.
#   scripts/drift_test.sh add_columns | rescue | fail
# Needs: databricks CLI logged in, `databricks bundle deploy` already done.
set -u
case "${1:-}" in
  add_columns) d=drift_addcols ;;
  rescue)      d=drift_rescue ;;
  fail)        d=drift_fail ;;
  *) echo "usage: $0 add_columns|rescue|fail"; exit 2 ;;
esac
job="drift_test_$1"
land="dbfs:/Volumes/spec_driven/landing/raw/$d"

databricks fs mkdir "$land"
databricks fs cp --overwrite sample_data/run1/transactions_001.csv "$land/"
echo "== run 1: clean file =="
databricks bundle run "$job" || { echo "run 1 failed - stop here and check the error"; exit 1; }

databricks fs cp --overwrite sample_data/run2_drift/transactions_002.csv "$land/"
echo "== run 2: file with extra column device_type =="
databricks bundle run "$job"
echo "run 2 exit code: $?   (expected: 0 for add_columns/rescue, non-zero for fail)"
