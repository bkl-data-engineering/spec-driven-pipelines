"""Runs the fleet_trips Silver data-quality logic on a local Spark session (JSON feed, so
typing, range, in_set and regex checks). Skipped if pyspark is not installed."""
import importlib.util
import unittest
from pathlib import Path

try:
    import pyspark  # noqa: F401
    HAVE_SPARK = True
except ImportError:
    HAVE_SPARK = False

ROOT = Path(__file__).resolve().parent.parent
BRONZE_COLS = ["trip_id", "vehicle_id", "driver_id", "vehicle_type", "start_time", "end_time", "distance_km",
               "fuel_used_l", "avg_speed_kph", "max_speed_kph", "harsh_braking_events", "idle_minutes",
               "status", "region", "_run_id", "_source_file", "_ingestion_ts", "_load_type", "_record_hash"]
# Bronze is all strings; explicit schema because Spark cannot infer a type for an all-None column.
BRONZE_SCHEMA = ", ".join(f"`{c}` string" for c in BRONZE_COLS)


def row(driver_id, **over):
    base = dict(trip_id="T1", vehicle_id="VH-1001", driver_id=driver_id, vehicle_type="van",
                start_time="2026-03-09T13:00:00Z", end_time="2026-03-09T13:45:00Z", distance_km="32.5",
                fuel_used_l="4.1", avg_speed_kph="43.3", max_speed_kph="88", harsh_braking_events="1",
                idle_minutes="6", status="completed", region="northeast",
                _run_id="r", _source_file="f.jsonl", _ingestion_ts="2026-03-09 00:00:00",
                _load_type="incremental", _record_hash="h1")
    base.update(over)
    return tuple(base[c] for c in BRONZE_COLS)


@unittest.skipUnless(HAVE_SPARK, "pyspark not installed")
class TestFleetSplitBatch(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        cls.spark = SparkSession.builder.master("local[1]").appName("sdp-fleet-test").getOrCreate()
        spec = importlib.util.spec_from_file_location("gen_fleet_silver", ROOT / "generated/fleet_trips/silver.py")
        cls.gen = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.gen)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def run_split(self, *rows):
        from runtime.silver import split_batch
        df = self.spark.createDataFrame(list(rows), BRONZE_SCHEMA)
        out = split_batch(df, self.gen.COLUMNS, self.gen.checks())
        return {r["driver_id"]: r for r in out.collect()}      # driver_id labels each test row

    def test_clean_row_is_typed_and_passes(self):
        r = self.run_split(row("ok"))["ok"]
        self.assertEqual(list(r["_dq_reasons"]), [])
        self.assertEqual(str(r["distance_km"]), "32.50")
        self.assertEqual(str(r["duration_min"]), "45.0")
        self.assertEqual(r["harsh_braking_events"], 1)

    def test_each_rule_type_fails_with_its_own_reason(self):
        res = self.run_split(
            row("range", distance_km="-5.2"),
            row("in_set", vehicle_type="spaceship"),
            row("not_null", trip_id=None),
            row("regex", vehicle_id="V-12"),
        )
        self.assertEqual(list(res["range"]["_dq_reasons"]), ["distance_km:range"])
        self.assertEqual(list(res["in_set"]["_dq_reasons"]), ["vehicle_type:in_set"])
        self.assertEqual(list(res["not_null"]["_dq_reasons"]), ["trip_id:not_null"])
        self.assertEqual(list(res["regex"]["_dq_reasons"]), ["vehicle_id:regex"])

    def test_unparseable_values_become_reasons_not_crashes(self):
        res = self.run_split(row("badtime", start_time="yesterday"), row("badnum", distance_km="lots"))
        self.assertIn("start_ts:not_null", res["badtime"]["_dq_reasons"])
        self.assertEqual(list(res["badnum"]["_dq_reasons"]), [])    # NULL passes range: only not_null judges NULLs


if __name__ == "__main__":
    unittest.main()
