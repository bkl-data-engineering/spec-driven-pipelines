"""Silver transform, shared by every generated pipeline.

NOTE: not yet run on Spark. split_batch() is the testable core
(tests/test_silver_spark.py runs it if pyspark is installed locally);
verify the whole module on Databricks first.

Incremental: streams from the bronze table (trigger availableNow), so each run
processes only bronze rows it has not seen. Across runs the latest-arriving
record for a key wins; order_by only decides between duplicates inside one batch.
"""
from __future__ import annotations

import uuid


class DataQualityError(RuntimeError):
    pass


def split_batch(df, columns, checks):
    """Type the batch and attach the reasons each row failed.

    columns: [(silver_name, sql_expression)]
    checks:  [(column, rule, condition)]   condition True = row passes
    Returns a DataFrame with the original bronze row in `_bronze` (kept so a
    quarantined record is the raw data, not the half-typed version),
    the typed columns, and `_dq_reasons` (array<string>, empty = clean).
    """
    from pyspark.sql import functions as F

    typed = df.select(
        F.struct(*[F.col(f"`{c}`") for c in df.columns]).alias("_bronze"),
        *[F.expr(sql).alias(name) for name, sql in columns],
    )
    if checks:
        failed = [
            F.when(~F.coalesce(cond, F.lit(False)), F.lit(f"{col}:{rule}"))
            for col, rule, cond in checks
        ]
        reasons = F.filter(F.array(*failed), lambda x: x.isNotNull())
    else:
        reasons = F.array().cast("array<string>")
    return typed.withColumn("_dq_reasons", reasons)


def _dedupe(df, keys, order_by):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    if not order_by:
        return df.dropDuplicates(keys)
    w = Window.partitionBy(*keys).orderBy(F.col(f"_bronze.`{order_by}`").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def process_batch(cfg, columns, checks, run_id, batch_df, batch_id) -> None:
    from pyspark.sql import functions as F

    spark = batch_df.sparkSession
    typed = split_batch(batch_df, columns, checks)
    bad = typed.filter(F.size("_dq_reasons") > 0)
    policy = cfg["on_dq_fail"]

    if policy == "fail":
        n = bad.count()
        if n:
            raise DataQualityError(f"{cfg['pipeline']}: {n} rows failed DQ in batch {batch_id}")
        good = typed
    elif policy == "quarantine":
        (bad.select(
            F.to_json("_bronze").alias("_raw_record"),
            F.concat_ws(",", "_dq_reasons").alias("_reasons"),
            F.lit(run_id).alias("_run_id"),
            F.current_timestamp().alias("_detected_ts"),
        ).write.mode("append").saveAsTable(cfg["quarantine_table"]))
        good = typed.filter(F.size("_dq_reasons") == 0)
    elif policy == "drop":
        good = typed.filter(F.size("_dq_reasons") == 0)
    else:  # warn: keep everything, just report
        print(f"[DQ-WARN] {cfg['pipeline']}: {bad.count()} rows failed checks in batch {batch_id}")
        good = typed

    names = [n for n, _ in columns]
    final = _dedupe(good, cfg["keys"], cfg["order_by"]).select(*[f"`{n}`" for n in names])

    if not spark.catalog.tableExists(cfg["table"]):
        final.limit(0).write.saveAsTable(cfg["table"])
    view = f"_sdp_batch_{uuid.uuid4().hex[:8]}"
    final.createOrReplaceTempView(view)
    on = " AND ".join(f"t.`{k}` <=> s.`{k}`" for k in cfg["keys"])
    spark.sql(
        f"MERGE INTO {cfg['table']} t USING {view} s ON {on} "
        "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
    )


def _ship_runtime_with_closures() -> None:
    """On serverless (Spark Connect) the foreachBatch function runs in a separate
    Python process that does NOT have the bundle root on sys.path, so pickling
    process_batch by reference fails there with `ModuleNotFoundError: runtime`.
    Pickling the runtime package by value sends the code itself instead."""
    import runtime

    for mod in ("pyspark.cloudpickle", "cloudpickle"):
        try:
            cp = __import__(mod, fromlist=["register_pickle_by_value"])
            cp.register_pickle_by_value(runtime)
        except (ImportError, AttributeError):
            pass


def run_silver(cfg: dict, columns, checks) -> None:
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    if not spark.catalog.tableExists(cfg["source_table"]):
        # Empty landing folder, or every file was quarantined: nothing to do yet.
        print(f"[silver] {cfg['source_table']} does not exist yet; skipping")
        return
    _ship_runtime_with_closures()
    run_id = str(uuid.uuid4())
    (
        spark.readStream.table(cfg["source_table"])
        .writeStream.foreachBatch(lambda df, bid: process_batch(cfg, columns, checks, run_id, df, bid))
        .option("checkpointLocation", cfg["checkpoint"])
        .trigger(availableNow=True)
        .start()
        .awaitTermination()
    )
