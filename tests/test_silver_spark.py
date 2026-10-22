"""Runs the Silver data-quality logic on a local Spark session.
Skipped automatically if pyspark is not installed. To run:
    uv add --dev pyspark     (needs Java 11/17 on the machine)
    uv run python -m unittest tests.test_silver_spark -v
"""
import importlib.util
import unittest
from pathlib import Path

try:
    import pyspark  # noqa: F401
    HAVE_SPARK = True
except ImportError:
    HAVE_SPARK = False

ROOT = Path(__file__).resolve().parent.parent
BRONZE_COLS = ["transaction_id", "user", "card", "year", "month", "day", "time", "amount", "use_chip",
               "merchant_name", "merchant_city", "merchant_state", "zip", "mcc", "errors", "is_fraud",
               "_run_id", "_source_file", "_ingestion_ts", "_load_type", "_record_hash"]


def row(**over):
    base = dict(transaction_id="t1", user="7", card="1", year="2024", month="3", day="9", time="14:05:59",
                amount="$134.09", use_chip="Swipe Transaction", merchant_name="m", merchant_city="Boston",
                merchant_state="MA", zip="02110", mcc="5411", errors=None, is_fraud="No",
                _run_id="r", _source_file="f.csv", _ingestion_ts="2024-03-09 00:00:00",
                _load_type="incremental", _record_hash="h1")
    base.update(over)
    return tuple(base[c] for c in BRONZE_COLS)


@unittest.skipUnless(HAVE_SPARK, "pyspark not installed")
class TestSplitBatch(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        cls.spark = SparkSession.builder.master("local[1]").appName("sdp-test").getOrCreate()
        spec = importlib.util.spec_from_file_location("gen_silver", ROOT / "generated/card_transactions/silver.py")
        cls.gen = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.gen)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def run_split(self, *rows):
        from runtime.silver import split_batch
        df = self.spark.createDataFrame(list(rows), BRONZE_COLS)
        out = split_batch(df, self.gen.COLUMNS, self.gen.checks())
        return {r["transaction_id"]: r for r in out.collect()}

    def test_clean_row_is_typed_and_passes(self):
        r = self.run_split(row())["t1"]
        self.assertEqual(list(r["_dq_reasons"]), [])
        self.assertEqual(str(r["amount"]), "134.09")
        self.assertIs(r["is_fraud"], False)
        self.assertEqual(r["event_ts"].hour, 14)

    def test_bad_values_fail_with_reasons_instead_of_crashing(self):
        res = self.run_split(
            row(transaction_id="a", amount="abc"),
            row(transaction_id="b", is_fraud="maybe"),
            row(transaction_id="c", time="14:05"),       # wrong time format -> NULL event_ts
            row(transaction_id="d", mcc="12"),
        )
        self.assertIn("amount:not_null", res["a"]["_dq_reasons"])
        self.assertIn("is_fraud:not_null", res["b"]["_dq_reasons"])
        self.assertIn("event_ts:not_null", res["c"]["_dq_reasons"])
        self.assertIn("mcc:regex", res["d"]["_dq_reasons"])


if __name__ == "__main__":
    unittest.main()
