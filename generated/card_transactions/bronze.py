# GENERATED from card_transactions.yaml (sha256 2ebe907925f5). DO NOT EDIT - change the spec and regenerate.
# Bronze ingestion for card_transactions: land the source as-is (all strings)
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

CONFIG = {'pipeline': 'card_transactions',
 'format': 'csv',
 'path': '/Volumes/spec_driven/landing/raw/transactions/',
 'ingest': 'autoloader',
 'options': {'header': 'true'},
 'on_schema_drift': 'quarantine',
 'expected_columns': ['transaction_id',
                      'user',
                      'card',
                      'year',
                      'month',
                      'day',
                      'time',
                      'amount',
                      'use_chip',
                      'merchant_name',
                      'merchant_city',
                      'merchant_state',
                      'zip',
                      'mcc',
                      'errors',
                      'is_fraud'],
 'table': 'spec_driven.bronze.transactions',
 'quarantine_table': 'spec_driven.bronze.transactions_schema_errors',
 'load_type': 'incremental',
 'audit_columns': ['_run_id', '_source_file', '_ingestion_ts', '_load_type', '_record_hash'],
 'alert_on_drift': True}

if __name__ == "__main__":
    run_bronze(CONFIG)
