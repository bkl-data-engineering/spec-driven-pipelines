#!/usr/bin/env bash
# Run the fleet_trips (JSON Lines) pipeline on Databricks: clean file, then a file with an extra key.
#   bash scripts/fleet_test.sh
# Needs: databricks CLI logged in, `databricks bundle deploy` already done.
set -u
land="dbfs:/Volumes/spec_driven/landing/raw/fleet_trips"
databricks fs mkdir "$land"
databricks fs cp --overwrite sample_data/fleet/trips_001.jsonl "$land/"
echo "== run 1: clean file (10 records, 4 bad) =="
databricks bundle run fleet_trips || { echo "run 1 failed - stop here and check the error"; exit 1; }

databricks fs cp --overwrite sample_data/fleet/trips_002_drift.jsonl "$land/"
echo "== run 2: file with an extra key (driver_score) =="
databricks bundle run fleet_trips
echo "run 2 exit code: $?   (expected: 0 - the file is quarantined, the run still succeeds)"
