import tempfile
import unittest
from pathlib import Path

from runtime.drift import Action, compare, decide, read_csv_header, read_json_keys, scan_folder

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


class TestJsonKeys(unittest.TestCase):
    def write(self, d, name, text):
        p = Path(d) / name
        p.write_text(text, encoding="utf-8")
        return str(p)

    def test_keys_are_the_union_over_all_records(self):
        with tempfile.TemporaryDirectory() as d:
            p = self.write(d, "a.jsonl", '{"a": 1, "b": 2}\n\n{"a": 3, "c": 4}\n')
            self.assertEqual(read_json_keys(p), ["a", "b", "c"])

    def test_bom_is_stripped(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "bom.jsonl"
            p.write_bytes(b'\xef\xbb\xbf{"a": 1}\n')
            self.assertEqual(read_json_keys(str(p)), ["a"])

    def test_not_json_lines_is_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            for text in ('[{"a": 1}]\n', '[\n  {"a": 1}\n]\n', '{"a": 1}\nnot json\n', '3\n'):
                with self.assertRaises(ValueError, msg=text):
                    read_json_keys(self.write(d, "bad.jsonl", text))


class TestScanJson(unittest.TestCase):
    EXPECTED_J = ["trip_id", "vehicle_id"]

    def test_scan_json_finds_extra_missing_and_unreadable(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            (root / "good.jsonl").write_text('{"trip_id": "1", "vehicle_id": "v"}\n')
            (root / "sparse.json").write_text('{"trip_id": "1"}\n{"vehicle_id": "v"}\n')   # key absent per record, not per file
            (root / "extra.ndjson").write_text('{"trip_id": "1", "vehicle_id": "v", "score": 9}\n')
            (root / "missing.jsonl").write_text('{"trip_id": "1"}\n')
            (root / "array.jsonl").write_text('[{"trip_id": "1", "vehicle_id": "v"}]\n')
            (root / "ignored.csv").write_text("x,y\n1,2\n")
            (root / "_state").mkdir()
            (root / "_state" / "ignored.jsonl").write_text('{"zzz": 1}\n')
            found = {Path(p).name: (r, dec) for p, r, dec in scan_folder(str(root), self.EXPECTED_J, "quarantine", "json")}
            self.assertEqual(sorted(found), ["array.jsonl", "extra.ndjson", "missing.jsonl"])
            self.assertEqual(found["extra.ndjson"][0].extra, ("score",))
            self.assertEqual(found["missing.jsonl"][0].missing, ("vehicle_id",))
            self.assertIn("expected JSON Lines", found["array.jsonl"][0].unreadable)
            for _, dec in found.values():
                self.assertEqual(dec.action, Action.QUARANTINE)

    def test_unreadable_files_are_never_tolerated(self):
        with tempfile.TemporaryDirectory() as d:
            (Path(d) / "array.jsonl").write_text('[{"trip_id": "1"}]\n')
            expect = {"add_columns": Action.FAIL, "rescue": Action.FAIL,
                      "quarantine": Action.QUARANTINE, "fail": Action.FAIL}
            for strategy, action in expect.items():
                (_, _, dec), = scan_folder(d, self.EXPECTED_J, strategy, "json")
                self.assertEqual(dec.action, action, strategy)


if __name__ == "__main__":
    unittest.main()
