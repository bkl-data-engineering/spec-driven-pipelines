"""The second pipeline (JSON Lines fleet trips): its spec, schema file and generated code agree."""
import ast
import unittest

import yaml

from generator.render import ROOT, render_all
from spec_schema.pipeline_spec import load_spec

SPEC = ROOT / "specs" / "fleet_trips.yaml"
SCHEMA = ROOT / "schemas" / "fleet_trips_v1.yaml"
SAMPLES = ROOT / "sample_data" / "fleet"


class TestFleetSpec(unittest.TestCase):
    def setUp(self):
        self.spec = load_spec(SPEC)
        self.files = render_all(self.spec, SPEC)

    def test_is_json_and_generated_python_is_valid(self):
        self.assertEqual(self.spec.source.format, "json")
        self.assertIn("'format': 'json'", self.files["bronze.py"])
        for name in ("bronze.py", "silver.py"):
            ast.parse(self.files[name])

    def test_uses_every_rule_type(self):
        rules = {k.rule for c in self.spec.silver.columns for k in c.checks}
        self.assertEqual(rules, {"not_null", "range", "in_set", "regex"})

    def test_sample_records_use_exactly_the_schema_keys(self):
        import json
        from runtime.drift import compare, read_json_keys

        expected = [c["name"] for c in yaml.safe_load(SCHEMA.read_text())["columns"]]
        self.assertFalse(compare(expected, read_json_keys(str(SAMPLES / "trips_001.jsonl"))).has_drift)
        drift = compare(expected, read_json_keys(str(SAMPLES / "trips_002_drift.jsonl")))
        self.assertEqual((drift.extra, drift.missing), (("driver_score",), ()))
        rows = [json.loads(line) for line in (SAMPLES / "trips_001.jsonl").read_text().splitlines()]
        self.assertEqual(len(rows), 10)

    def test_silver_keys_exist_and_job_is_wired(self):
        job = yaml.safe_load(self.files["job.yml"])["resources"]["jobs"]["fleet_trips"]
        self.assertEqual([t["task_key"] for t in job["tasks"]], ["bronze", "silver"])


if __name__ == "__main__":
    unittest.main()
