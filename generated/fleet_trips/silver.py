# GENERATED from fleet_trips.yaml (sha256 71a383d0092b). DO NOT EDIT - change the spec and regenerate.
# Silver transform for fleet_trips: typing, cleaning, DQ, dedupe.
# Logic lives in runtime/silver.py; this file is the reviewable part.
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

from pyspark.sql import functions as F

from runtime.silver import run_silver

CONFIG = {'pipeline': 'fleet_trips',
 'source_table': 'spec_driven.bronze.fleet_trips',
 'table': 'spec_driven.silver.fleet_trips',
 'keys': ['trip_id'],
 'order_by': '_ingestion_ts',
 'on_dq_fail': 'quarantine',
 'quarantine_table': 'spec_driven.silver.fleet_trips_dq_errors',
 'checkpoint': '/Volumes/spec_driven/landing/raw/fleet_trips_state/silver_checkpoint'}

# (silver column, SQL expression). try_cast => a bad value becomes NULL, never a crash.
COLUMNS = [
    ('trip_id', 'try_cast((`trip_id`) AS string)'),
    ('_record_hash', 'try_cast((_record_hash) AS string)'),
    ('vehicle_id', 'try_cast((`vehicle_id`) AS string)'),
    ('driver_id', 'try_cast((`driver_id`) AS string)'),
    ('vehicle_type', 'try_cast((`vehicle_type`) AS string)'),
    ('start_ts', 'try_cast((try_to_timestamp(start_time)) AS timestamp)'),
    ('end_ts', 'try_cast((try_to_timestamp(end_time)) AS timestamp)'),
    ('duration_min', 'try_cast(((unix_timestamp(try_to_timestamp(end_time)) - unix_timestamp(try_to_timestamp(start_time))) / 60) AS decimal(8,1))'),
    ('distance_km', 'try_cast((`distance_km`) AS decimal(8,2))'),
    ('fuel_used_l', 'try_cast((`fuel_used_l`) AS decimal(8,2))'),
    ('avg_speed_kph', 'try_cast((`avg_speed_kph`) AS decimal(6,2))'),
    ('max_speed_kph', 'try_cast((`max_speed_kph`) AS decimal(6,2))'),
    ('harsh_braking_events', 'try_cast((`harsh_braking_events`) AS int)'),
    ('idle_minutes', 'try_cast((`idle_minutes`) AS int)'),
    ('status', 'try_cast((`status`) AS string)'),
    ('region', 'try_cast((`region`) AS string)'),
]


def checks():
    """(column, rule, condition). A row PASSES when the condition is true."""
    return [
        ('trip_id', 'not_null', F.col('trip_id').isNotNull()),
        ('vehicle_id', 'not_null', F.col('vehicle_id').isNotNull()),
        ('vehicle_id', 'regex', (F.col('vehicle_id').isNull() | (F.col('vehicle_id').rlike('^VH-\\d{4}$')))),
        ('vehicle_type', 'in_set', (F.col('vehicle_type').isNull() | (F.col('vehicle_type').isin(['sedan', 'van', 'truck'])))),
        ('start_ts', 'not_null', F.col('start_ts').isNotNull()),
        ('end_ts', 'not_null', F.col('end_ts').isNotNull()),
        ('distance_km', 'range', (F.col('distance_km').isNull() | ((F.col('distance_km') >= 0.0) & (F.col('distance_km') <= 2000.0)))),
        ('fuel_used_l', 'range', (F.col('fuel_used_l').isNull() | ((F.col('fuel_used_l') >= 0.0)))),
        ('avg_speed_kph', 'range', (F.col('avg_speed_kph').isNull() | ((F.col('avg_speed_kph') >= 0.0) & (F.col('avg_speed_kph') <= 200.0)))),
        ('max_speed_kph', 'range', (F.col('max_speed_kph').isNull() | ((F.col('max_speed_kph') >= 0.0) & (F.col('max_speed_kph') <= 250.0)))),
        ('harsh_braking_events', 'range', (F.col('harsh_braking_events').isNull() | ((F.col('harsh_braking_events') >= 0.0)))),
        ('idle_minutes', 'range', (F.col('idle_minutes').isNull() | ((F.col('idle_minutes') >= 0.0)))),
        ('status', 'in_set', (F.col('status').isNull() | (F.col('status').isin(['completed', 'aborted'])))),
    ]


if __name__ == "__main__":
    run_silver(CONFIG, COLUMNS, checks())
