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
| `source` | file format, landing path, expected-schema file, **schema-drift strategy** |
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
Optional Spark tests: `uv add --dev pyspark` (needs Java), then `uv run python -m unittest tests.test_silver_spark -v`.

## Running on Databricks (Free Edition)
1. Create catalog `spec_driven`; schemas `bronze`, `silver`, `landing`; volume `landing.raw`.
2. Put CSVs in `/Volumes/spec_driven/landing/raw/transactions/`.
3. Set your workspace host in `databricks.yml`, then `databricks bundle validate && databricks bundle deploy && databricks bundle run card_transactions`.

## Status
Tested locally: spec validation, generator, drift logic, DQ-expression rendering.
**Not yet run on Databricks:** `runtime/bronze.py` (Auto Loader, quarantine move), `runtime/silver.py` (streaming MERGE), the bundle config. Expect a fix or two on first run.

Known limits: CSV only; alerts are log-only; across runs the latest-arriving record per key wins (`order_by` applies within a batch); tolerated additive drift is not alerted.
