# spec-driven-pipelines: design decisions (as of 2026-11-04)

Standalone repo `spec-driven-pipelines`; demo feeds = card transactions (CSV) and fleet trips (JSON Lines) on a new Databricks Free Edition catalog `spec_driven`.

## Architecture
spec YAML -> Pydantic validation -> Jinja generator -> `generated/<pipeline>/{bronze.py, silver.py, job.yml}`.
Generated files are thin; shared logic lives in `runtime/` (hybrid of generated code and a shared library).
CI: `generate --check` fails if committed generated code differs from what the spec produces.
Each spec owns its landing folder and tables (enforced by tests/test_specs_isolated.py); Auto Loader state and the silver checkpoint live in `<landing path>_state/`.

## Data
- Landing: Unity Catalog volume `/Volumes/spec_driven/landing/raw/<feed>/`.
- card_transactions: CSV. Silver: typing via `try_cast` (ANSI-safe), derived `event_ts` from year/month/day/time, `amount` strips `$`, `is_fraud` Yes/No -> boolean. Dedupe key `transaction_id`.
- fleet_trips: JSON Lines, flat records, all values stringified in bronze (`inferColumnTypes=false`). Silver: timestamps via `try_to_timestamp`, derived `duration_min`, decimals/ints, DQ uses all four rule types (not_null, range, in_set, regex). Dedupe key `trip_id`. Sample trips_001.jsonl = 10 records, 4 bad (one per rule type); trips_002_drift.jsonl = 2 records, one with extra key `driver_score`.
- Bronze: all strings, plus audit columns `_run_id, _source_file, _ingestion_ts, _load_type, _record_hash`.

## Schema drift (source.on_schema_drift)
- add_columns / rescue: tolerate EXTRA columns; quarantine / fail react to any drift.
- MISSING columns are never tolerated silently (fail, or quarantine under quarantine).
- Quarantine = rows stored in a side table with reason + file columns, file moved out of landing folder, alert. CSV: rows as JSON. JSON Lines: original lines verbatim.
- JSON drift check (runtime/drift.py): the keys are the union over ALL records of a file (a key absent from some records is NULL, not drift; absent from every record is "missing"). Files that are not JSON Lines (e.g. a pretty-printed array) are "unreadable": always FAIL, or quarantine under quarantine; never tolerated by add_columns/rescue. Cost: the whole file is read in Python on the driver during preflight.

## Silver DQ (silver.on_dq_fail): drop | warn | fail | quarantine
Checks: not_null, range, in_set, regex. range/in_set/regex ignore NULLs; not_null is the only NULL check.
Silver is incremental (stream from bronze, availableNow) with MERGE on key; latest-arriving record wins across runs.
Silver code did not change for the JSON feed (the shared runtime is format-agnostic past bronze).

## Status (2026-11-04)
CI green. VERIFIED on Databricks Free Edition (serverless), confirmed by table queries:
- card_transactions (quarantine/quarantine): run 1 bronze 5, silver 3, silver dq_errors 2; run 2 drift file quarantined (schema_errors 1 row, file moved, alert logged, counts unchanged).
- Drift strategies add_columns, rescue, fail: specs drift_test_*, `bash scripts/drift_test.sh <strategy>`; add_columns/rescue bronze 6, silver 4; fail: SchemaDriftError, bronze 5, silver 3.
- DQ policies drop, warn, fail: specs dq_test_*, `bash scripts/dq_test.sh <policy>`; all matched predictions.
- fleet_trips (JSON Lines, quarantine/quarantine), `bash scripts/fleet_test.sh`: validation queries matched predictions (run 1: bronze 10, silver 6, dq_errors 4 with one reason per rule type; run 2: extra key `driver_score` quarantined, schema_errors 2 raw lines, file moved, counts unchanged). The earlier unverified risks (number stringification, trailing-Z timestamps, duration_min SQL) did not cause failures. Second pipeline needed changes only in runtime drift/bronze (JSON support), none in silver or the generator logic.
README and this doc updated to match; repo synced through the fleet-json patch by the user.
Limits: CSV and flat JSON Lines only (parquet accepted by the spec but rejected at run time; nested JSON untested); alerts log-only (each alert prints twice: logger + print); tolerated additive drift not alerted.

## Fix log
- 2026-10-29 CI (commit 76dd359): `uv sync` installs the dev group, so pyspark ran the Spark tests in CI (they skip locally). Both failed with CANNOT_DETERMINE_TYPE because `errors` is None in every test row. Fix: explicit all-string schema in tests. CI pins Java 17. Landed in 324b289. Still to do: commit uv.lock.
  - Gotcha: macOS blocked the terminal from reading ~/Downloads ("Operation not permitted"), so the first unzip silently did nothing. Move files into the repo via Finder, or grant the terminal Files and Folders access.
- 2026-10-30 pre-deploy changes: silver registers `runtime` with cloudpickle by value (serverless foreachBatch); silver skips if bronze table is missing; bronze quarantine backticks column names; databricks.yml sync.exclude for sample_data/ and tests/; added setup/00_create_objects.sql and sample_data/.
- 2026-10-30 first deploy errors, both observed on Databricks:
  1. `bundle validate`: serverless tasks need an environment. Fix: `environments` block (environment_version "2") plus `environment_key` on each task in generator/templates/job.yml.j2.
  2. bronze run: `NameError: __file__` because serverless exec()s the script in a kernel. Fix: shared generator/templates/_bootstrap.py.j2 finds the repo root from sys.argv[1] (job passes `${workspace.file_path}`), then `__file__`, then cwd walk-up.
- Whether the cloudpickle-by-value change in silver was actually needed is untested; silver ran fine either way.
- Still open: shutil.move on a Volume is copy+delete (not atomic); the file did move correctly in the test runs.

## Next
1. Commit uv.lock; confirm CI is green with the new Spark test file (tests/test_silver_fleet_spark.py ran for the first time there).
2. Real email/Slack alerts (dedupe the double print); parquet source; nested JSON.
3. Streamlit/LLM spec-authoring UI (the FDE-style piece): an LLM drafts a spec from a sample file, a human reviews it, the generator builds the pipeline.