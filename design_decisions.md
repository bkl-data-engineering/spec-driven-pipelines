# spec-driven-pipelines: design decisions (as of 2026-10-31)

Standalone repo `spec-driven-pipelines`; demo feed = card transactions on a new Databricks Free Edition catalog `spec_driven`.

## Architecture
spec YAML -> Pydantic validation -> Jinja generator -> `generated/<pipeline>/{bronze.py, silver.py, job.yml}`.
Generated files are thin; shared logic lives in `runtime/` (hybrid of generated code and a shared library).
CI: `generate --check` fails if committed generated code differs from what the spec produces.
Each spec owns its landing folder and tables (enforced by tests/test_specs_isolated.py); Auto Loader state and the silver checkpoint live in `<landing path>_state/`.

## Data
- Landing: Unity Catalog volume `/Volumes/spec_driven/landing/raw/transactions/` (CSV).
- Bronze: all strings, plus audit columns `_run_id, _source_file, _ingestion_ts, _load_type, _record_hash`. Source has `transaction_id` (typed string).
- Silver: typing via `try_cast` (ANSI-safe), derived `event_ts` from year/month/day/time (HH:MM:SS), `amount` strips `$`, `is_fraud` Yes/No -> boolean (anything else -> NULL -> DQ fail). Dedupe key `transaction_id`.

## Schema drift (source.on_schema_drift)
- add_columns / rescue: tolerate EXTRA columns; quarantine / fail react to any drift.
- MISSING columns are never tolerated silently (fail, or quarantine under quarantine).
- Quarantine = rows stored as JSON in a side table with reason + file columns, file moved out of landing folder, alert.

## Silver DQ (silver.on_dq_fail): drop | warn | fail | quarantine
Checks: not_null, range, in_set, regex. range/in_set/regex ignore NULLs; not_null is the only NULL check.
Silver is incremental (stream from bronze, availableNow) with MERGE on key; latest-arriving record wins across runs.

## Status (2026-10-31)
CI green. VERIFIED on Databricks Free Edition (serverless), counts confirmed by queries:
- card_transactions (quarantine/quarantine): run 1 bronze 5, silver 3, silver dq_errors 2 (t0004 amount, t0005 is_fraud); run 2 drift file quarantined (schema_errors 1 row, file moved, alert logged, counts unchanged).
- Drift strategies add_columns, rescue, fail: each tested with its own spec (drift_test_*), run via `bash scripts/drift_test.sh <strategy>`; all behaved as predicted (add_columns/rescue: bronze 6, silver 4; fail: job fails with SchemaDriftError, bronze 5, silver 3).
- DQ policies drop, warn, fail: specs dq_test_* and `scripts/dq_test.sh` built and delivered as dq-tests.patch (applies on top of drift-tests.patch) but NOT yet run on Databricks. Predictions on run1 sample (2 bad rows): drop -> silver 3; warn -> silver 5 with NULLs in failing columns plus a [DQ-WARN] line in the silver task output; fail -> silver task fails with DataQualityError, no silver table created, bronze 5.
Limits: CSV only; alerts log-only (each alert prints twice in the job log: logger + print); tolerated additive drift not alerted.

## Fix log
- 2026-10-29 CI (commit 76dd359): `uv sync` installs the dev group, so pyspark 4.2 ran the Spark tests in CI (they skip locally). Both failed with CANNOT_DETERMINE_TYPE because `errors` is None in every test row. Fix: explicit all-string schema in tests/test_silver_spark.py. The "STALE silver.py" log line was expected output from test_check_detects_hand_edits (now silenced). CI pins Java 17. Landed in 324b289. Still to do: commit uv.lock.
  - Gotcha: macOS blocked the terminal from reading ~/Downloads ("Operation not permitted"), so the first unzip silently did nothing. Move files into the repo via Finder, or grant the terminal Files and Folders access.
- 2026-10-30 pre-deploy changes: silver registers `runtime` with cloudpickle by value (serverless foreachBatch); silver skips if bronze table is missing; bronze quarantine backticks column names; databricks.yml sync.exclude for sample_data/ and tests/; added setup/00_create_objects.sql and sample_data/ (run1: 5 rows; run2_drift: extra `device_type`).
- 2026-10-30 first deploy errors, both observed on Databricks:
  1. `bundle validate`: serverless tasks need an environment. Fix: `environments` block (environment_version "2") plus `environment_key` on each task in generator/templates/job.yml.j2.
  2. bronze run: `NameError: __file__` because serverless exec()s the script in a kernel. Fix: shared generator/templates/_bootstrap.py.j2 finds the repo root from sys.argv[1] (job passes `${workspace.file_path}`), then `__file__`, then cwd walk-up.
- Whether the cloudpickle-by-value change in silver was actually needed is untested; silver ran fine either way.
- Still open: shutil.move on a Volume is copy+delete (not atomic); the file did move correctly in the test run.

## Next
1. Apply dq-tests.patch, deploy, run `bash scripts/dq_test.sh drop|warn|fail`, compare with predictions above; then update README (Status: all drift and DQ options verified).
2. Second pipeline from a different spec to prove the runtime is not card-specific.
3. Upload this design-decisions doc to the GitHub repo (e.g. docs/design-decisions.md).
4. Commit uv.lock; real email/Slack alerts (dedupe the double print); json/parquet sources; Streamlit/LLM spec-authoring UI.