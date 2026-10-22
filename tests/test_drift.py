import tempfile
import unittest
from pathlib import Path

from runtime.drift import Action, compare, decide, read_csv_header, scan_folder

EXPECTED = ["transaction_id", "user", "amount"]


class TestCompare(unittest.TestCase):
    def test_identical(self):
        self.assertFalse(compare(EXPECTED, ["transaction_id", "user", "amount"]).has_drift)

    def test_order_case_and_whitespace_ignored(self):
        self.assertFalse(compare(EXPECTED, [" Amount", "USER ", "Transaction_ID"]).has_drift)

    def test_extra_and_missing(self):
        r = compare(EXPECTED, ["transaction_id", "user", "channel"])
        self.assertEqual(r.extra, ("channel",))
        self.assertEqual(r.missing, ("amount",))


class TestDecide(unittest.TestCase):
    extra = compare(EXPECTED, EXPECTED + ["channel"])
    missing = compare(EXPECTED, ["transaction_id", "user"])
    none = compare(EXPECTED, EXPECTED)

    def test_no_drift_always_proceeds(self):
        for s in ("add_columns", "rescue", "quarantine", "fail"):
            self.assertEqual(decide(self.none, s).action, Action.PROCEED)

    def test_extra_columns_per_strategy(self):
        expect = {"add_columns": Action.PROCEED, "rescue": Action.PROCEED,
                  "quarantine": Action.QUARANTINE, "fail": Action.FAIL}
        for s, action in expect.items():
            self.assertEqual(decide(self.extra, s).action, action, s)

    def test_missing_columns_never_tolerated(self):
        expect = {"add_columns": Action.FAIL, "rescue": Action.FAIL,
                  "quarantine": Action.QUARANTINE, "fail": Action.FAIL}
        for s, action in expect.items():
            self.assertEqual(decide(self.missing, s).action, action, s)

    def test_unknown_strategy(self):
        with self.assertRaises(ValueError):
            decide(self.extra, "yolo")


class TestScan(unittest.TestCase):
    def test_scan_finds_only_drifted_files_and_skips_state_dirs(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            (root / "good.csv").write_text("transaction_id,user,amount\n1,2,3\n")
            (root / "bad.csv").write_text("transaction_id,user,amount,channel\n1,2,3,web\n")
            (root / "bom.csv").write_bytes(b"\xef\xbb\xbftransaction_id,user,amount\n1,2,3\n")
            (root / "_state").mkdir()
            (root / "_state" / "ignored.csv").write_text("x,y\n1,2\n")
            self.assertEqual(read_csv_header(str(root / "bom.csv"))[0], "transaction_id")
            found = scan_folder(str(root), EXPECTED, "quarantine")
            self.assertEqual([Path(p).name for p, _, _ in found], ["bad.csv"])
            self.assertEqual(found[0][2].action, Action.QUARANTINE)


if __name__ == "__main__":
    unittest.main()
