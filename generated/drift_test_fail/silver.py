# GENERATED from drift_test_fail.yaml (sha256 945eb0c33af7). DO NOT EDIT - change the spec and regenerate.
# Silver transform for drift_test_fail: typing, cleaning, DQ, dedupe.
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

CONFIG = {'pipeline': 'drift_test_fail',
 'source_table': 'spec_driven.bronze.drift_fail',
 'table': 'spec_driven.silver.drift_fail',
 'keys': ['transaction_id'],
 'order_by': '_ingestion_ts',
 'on_dq_fail': 'quarantine',
 'quarantine_table': 'spec_driven.silver.drift_fail_dq_errors',
 'checkpoint': '/Volumes/spec_driven/landing/raw/drift_fail_state/silver_checkpoint'}

# (silver column, SQL expression). try_cast => a bad value becomes NULL, never a crash.
COLUMNS = [
    ('transaction_id', 'try_cast((`transaction_id`) AS string)'),
    ('_record_hash', 'try_cast((_record_hash) AS string)'),
    ('user', 'try_cast((`user`) AS int)'),
    ('card', 'try_cast((`card`) AS int)'),
    ('event_ts', "try_cast((try_to_timestamp(concat(year,'-',lpad(month,2,'0'),'-',lpad(day,2,'0'),' ',time), 'yyyy-MM-dd HH:mm:ss')) AS timestamp)"),
    ('amount', "try_cast((regexp_replace(amount,'[$,]','')) AS decimal(18,2))"),
    ('use_chip', 'try_cast((`use_chip`) AS string)'),
    ('merchant_name', 'try_cast((`merchant_name`) AS string)'),
    ('merchant_city', 'try_cast((`merchant_city`) AS string)'),
    ('merchant_state', 'try_cast((`merchant_state`) AS string)'),
    ('zip', 'try_cast((`zip`) AS string)'),
    ('mcc', 'try_cast((`mcc`) AS string)'),
    ('errors', 'try_cast((`errors`) AS string)'),
    ('is_fraud', "try_cast((CASE WHEN lower(is_fraud)='yes' THEN true WHEN lower(is_fraud)='no' THEN false END) AS boolean)"),
]


def checks():
    """(column, rule, condition). A row PASSES when the condition is true."""
    return [
        ('transaction_id', 'not_null', F.col('transaction_id').isNotNull()),
        ('user', 'not_null', F.col('user').isNotNull()),
        ('event_ts', 'not_null', F.col('event_ts').isNotNull()),
        ('amount', 'not_null', F.col('amount').isNotNull()),
        ('mcc', 'regex', (F.col('mcc').isNull() | (F.col('mcc').rlike('^\\d{4}$')))),
        ('is_fraud', 'not_null', F.col('is_fraud').isNotNull()),
    ]


if __name__ == "__main__":
    run_silver(CONFIG, COLUMNS, checks())
