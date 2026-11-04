# GENERATED from fleet_trips.yaml (sha256 71a383d0092b). DO NOT EDIT - change the spec and regenerate.
# Bronze ingestion for fleet_trips: land the source as-is (all strings)
# plus audit columns. Logic lives in runtime/bronze.py, shared by every pipeline.
import pathlib
import sys


def _bundle_root() -> str:
    """Find the repo root (the folder that holds runtime/).

    Databricks serverless exec()s this file inside a kernel, so __file__ is NOT
    defined there. The job passes the deployed bundle path as argument 1; the
    other lookups cover running the file directly (local, classic compute)."""
    starts = []
    if len(sys.argv) > 1:
        starts.append(pathlib.Path(sys.argv[1]))
    if "__file__" in globals():
        starts.append(pathlib.Path(__file__).resolve().parents[2])
    starts.append(pathlib.Path.cwd())
    for start in starts:
        for folder in (start, *start.parents):
            if (folder / "runtime").is_dir():
                return str(folder)
    raise RuntimeError(f"cannot find the repo root (folder with runtime/); tried {[str(s) for s in starts]}")


sys.path.insert(0, _bundle_root())

from runtime.bronze import run_bronze

CONFIG = {'pipeline': 'fleet_trips',
 'format': 'json',
 'path': '/Volumes/spec_driven/landing/raw/fleet_trips/',
 'ingest': 'autoloader',
 'options': {},
 'on_schema_drift': 'quarantine',
 'expected_columns': ['trip_id',
                      'vehicle_id',
                      'driver_id',
                      'vehicle_type',
                      'start_time',
                      'end_time',
                      'distance_km',
                      'fuel_used_l',
                      'avg_speed_kph',
                      'max_speed_kph',
                      'harsh_braking_events',
                      'idle_minutes',
                      'status',
                      'region'],
 'table': 'spec_driven.bronze.fleet_trips',
 'quarantine_table': 'spec_driven.bronze.fleet_trips_schema_errors',
 'load_type': 'incremental',
 'audit_columns': ['_run_id', '_source_file', '_ingestion_ts', '_load_type', '_record_hash'],
 'alert_on_drift': True}

if __name__ == "__main__":
    run_bronze(CONFIG)
