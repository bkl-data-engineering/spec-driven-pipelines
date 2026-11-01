"""Every spec must own its landing folder and tables. Two specs sharing one would
silently corrupt each other's Auto Loader state and tables (the drift tests rely on this)."""
import unittest
from collections import Counter
from pathlib import Path

from spec_schema.pipeline_spec import load_spec

SPECS = sorted((Path(__file__).resolve().parent.parent / "specs").glob("*.yaml"))


class TestSpecsIsolated(unittest.TestCase):
    def setUp(self):
        self.specs = [load_spec(p) for p in SPECS]

    def assertUnique(self, label, values):
        dupes = [v for v, n in Counter(values).items() if n > 1]
        self.assertEqual(dupes, [], f"{label} shared by more than one spec: {dupes}")

    def test_pipeline_names_unique(self):
        self.assertUnique("pipeline name", [s.pipeline for s in self.specs])

    def test_landing_paths_unique(self):
        self.assertUnique("landing path", [s.source.path.rstrip("/") for s in self.specs])

    def test_tables_unique(self):
        tables = []
        for s in self.specs:
            tables += [s.bronze.table, s.silver.table, s.bronze.quarantine_table, s.silver.quarantine_table]
        self.assertUnique("table", [t for t in tables if t])


if __name__ == "__main__":
    unittest.main()
