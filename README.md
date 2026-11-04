# spec-driven-pipelines

Describe a pipeline in a YAML spec; generate the bronze/silver code and the Databricks job from it.

```
specs/x.yaml ──validate──► generate ──► generated/x/{bronze.py, silver.py, job.yml}
                                              │ thin entry points call ▼
                                        runtime/ (shared, tested library)
```

## What a spec controls
| Section | Controls |
|---|---|
| `source` | file format (`csv` or `json` = JSON Lines), landing path, expected-schema file, **schema-drift strategy** |
| `bronze` | target table, load type, audit columns (`_run_id`, `_source_file`, `_ingestion_ts`, `_load_type`, `_record_hash`) |
| `silver` | column types and SQL expressions, DQ checks, dedupe keys, **what to do with failing rows** |
| `alerts`, `schedule` | notification events, job schedule |

## Schema drift (`source.on_schema_drift`)
| Strategy | Extra columns | Missing columns |
|---|---|---|
| `add_columns` | loaded; bronze table gains the columns | run fails |
| `rescue` | loaded; extras land in `_rescued_data` | run fails |
| `quarantine` | file's rows go to `bronze.quarantine_table` as JSON, file moved aside, alert | same |
| `fail` | run fails | run fails |

Missing columns are never tolerated silently (a wrong file would load as NULLs).

For **JSON Lines** sources the "columns" are the top-level keys, taken as the union over every record in a file. A key absent from some records loads as `NULL`; a key absent from every record counts as missing. A file that is not JSON Lines (for example a pretty-printed array) is "unreadable" and is never tolerated by `add_columns` or `rescue`. Quarantined JSON files keep their original lines unchanged in the side table.

## Silver data quality (`silver.on_dq_fail`)
`drop` | `warn` | `fail` | `quarantine` (failing rows kept as raw JSON + reason codes in `silver.quarantine_table`).
Casts use `try_cast`, so bad values become NULL and are caught by `not_null`, never crash the run.

## Commands
```
uv sync
uv run python -m unittest discover -s tests -t .
uv run python -m generator.cli generate specs/card_transactions.yaml
uv run python -m generator.cli generate specs/card_transactions.yaml --check   # CI: fails if generated code is stale
```
The Spark tests (`tests/test_silver_spark.py`) run automatically when `pyspark` is installed (it is in the dev group, so `uv sync` installs it and CI runs them; they need Java 17). Without Java they error locally.

## Running on Databricks (Free Edition)
1. Run `setup/00_create_objects.sql` once in the SQL editor (catalog `spec_driven`, schemas, volume `landing.raw`).
2. Install the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install), then `databricks auth login --host <workspace URL>` (the `https://dbc-...cloud.databricks.com` address, not the numeric workspace ID).
3. Set the same URL as `host:` in `databricks.yml`, then:
   `databricks bundle validate && databricks bundle deploy`
4. Upload the first sample file and run:
   ```
   databricks fs mkdir dbfs:/Volumes/spec_driven/landing/raw/transactions
   databricks fs cp sample_data/run1/transactions_001.csv dbfs:/Volumes/spec_driven/landing/raw/transactions/
   databricks bundle run card_transactions
   ```
   Expected: bronze 5 rows, silver 3 rows, `silver.transactions_dq_errors` 2 rows (`t0004` bad amount, `t0005` bad `is_fraud`).
5. Drift test: copy `sample_data/run2_drift/transactions_002.csv` to the same folder and run again. Expected: an `[ALERT:schema_drift]` line in the job log, 1 row in `bronze.transactions_schema_errors`, the file moved to `.../transactions_quarantined/`, and bronze/silver counts unchanged.

The job runs on serverless compute: `job.yml` declares an `environments` block, and the scripts find the repo root from the bundle path the job passes in (serverless has no `__file__`).

## Testing the drift strategies
Each strategy has its own spec (`specs/drift_test_<strategy>.yaml`) with a separate landing folder and tables, so scenarios never share state. After `databricks bundle deploy`:
```
bash scripts/drift_test.sh add_columns    # or: rescue | fail
```
The script loads the clean sample file, runs the job, adds the drift file (extra column `device_type`) and runs again.

| Strategy | Run 2 result | Bronze | Silver |
|---|---|---|---|
| `add_columns` | success after one retry (Auto Loader stops once on the new column) | 6 rows, new `device_type` column | 4 rows |
| `rescue` | success | 6 rows, extra value in `_rescued_data` | 4 rows |
| `fail` | job fails with `SchemaDriftError`; the drift file stays in the landing folder | 5 rows | 3 rows |

`tests/test_specs_isolated.py` fails if two specs share a pipeline name, landing folder or table.

## Testing the DQ policies
Same pattern: `specs/dq_test_<policy>.yaml`, each with its own landing folder and tables. After `databricks bundle deploy`:
```
bash scripts/dq_test.sh drop    # or: warn | fail
```
It loads the sample file with two bad rows (`t0004` bad amount, `t0005` bad `is_fraud`) and runs the job.

| Policy | Job result | Bronze | Silver |
|---|---|---|---|
| `quarantine` | success | 5 rows | 3 rows; the 2 bad rows go to `silver.quarantine_table` with reason codes |
| `drop` | success | 5 rows | 3 rows; bad rows discarded |
| `warn` | success; `[DQ-WARN]` line in the silver task output | 5 rows | 5 rows; bad values are `NULL` |
| `fail` | job fails with `DataQualityError`; no silver table is created | 5 rows | none |

## Second pipeline: fleet trips (JSON Lines)
`specs/fleet_trips.yaml` is a different feed on the same runtime: JSON Lines, other columns and types, a derived `duration_min`, and DQ using all four rule types (`not_null`, `range`, `in_set`, `regex`). Silver needed no code changes for the new format. After `databricks bundle deploy`:
```
bash scripts/fleet_test.sh
```
| Run | File | Bronze | Silver | Side tables |
|---|---|---|---|---|
| 1 | `sample_data/fleet/trips_001.jsonl` (10 records, 4 bad) | 10 | 6 | `silver.fleet_trips_dq_errors`: 4 rows (`distance_km:range`, `vehicle_type:in_set`, `trip_id:not_null`, `vehicle_id:regex`) |
| 2 | `sample_data/fleet/trips_002_drift.jsonl` (extra key `driver_score`) | 10 | 6 | `bronze.fleet_trips_schema_errors`: 2 rows; file moved to `.../fleet_trips_quarantined/`; `[ALERT:schema_drift]` logged |

## Status
CI runs the unit tests (including the Spark tests) and the generated-code check on every push.

**Verified on Databricks Free Edition (serverless):** the full bronze to silver flow, all four `on_schema_drift` strategies (`quarantine`, `add_columns`, `rescue`, `fail`), all four `on_dq_fail` policies (`quarantine`, `drop`, `warn`, `fail`), and both source formats (CSV card transactions, JSON Lines fleet trips).

Known limits: CSV and flat JSON Lines only (no Parquet, no nested JSON); alerts are log-only; alerts are log-only; across runs the latest-arriving record per key wins (`order_by` applies within a batch); tolerated additive drift is not alerted.
