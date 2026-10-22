import copy
import unittest
from pathlib import Path

import yaml
from pydantic import ValidationError

from spec_schema.pipeline_spec import PipelineSpec, load_spec

SPEC = Path(__file__).parent.parent / "specs" / "card_transactions.yaml"


def raw():
    return copy.deepcopy(yaml.safe_load(SPEC.read_text()))


class TestSpec(unittest.TestCase):
    def test_real_spec_is_valid(self):
        spec = load_spec(SPEC)
        self.assertEqual(spec.bronze.table, "spec_driven.bronze.transactions")
        self.assertEqual(len(spec.bronze.audit_columns), 5)

    def test_typo_in_key_is_rejected(self):
        d = raw(); d["source"]["on_schema_drfit"] = "fail"
        with self.assertRaises(ValidationError):
            PipelineSpec.model_validate(d)

    def test_quarantine_drift_needs_a_table(self):
        d = raw(); del d["bronze"]["quarantine_table"]
        with self.assertRaisesRegex(ValidationError, "bronze.quarantine_table"):
            PipelineSpec.model_validate(d)

    def test_dq_quarantine_needs_a_table(self):
        d = raw(); del d["silver"]["quarantine_table"]
        with self.assertRaisesRegex(ValidationError, "silver.quarantine_table"):
            PipelineSpec.model_validate(d)

    def test_key_must_be_a_silver_column(self):
        d = raw(); d["silver"]["keys"] = ["nope"]
        with self.assertRaisesRegex(ValidationError, "keys not defined"):
            PipelineSpec.model_validate(d)

    def test_table_name_must_be_three_part(self):
        d = raw(); d["bronze"]["table"] = "transactions"
        with self.assertRaisesRegex(ValidationError, "catalog.schema.table"):
            PipelineSpec.model_validate(d)

    def test_range_check_needs_bounds(self):
        d = raw(); d["silver"]["columns"][3]["checks"] = [{"rule": "range"}]
        with self.assertRaisesRegex(ValidationError, "min and/or max"):
            PipelineSpec.model_validate(d)

    def test_unknown_audit_column_rejected(self):
        d = raw(); d["bronze"]["audit_columns"] = ["_made_up"]
        with self.assertRaisesRegex(ValidationError, "unknown audit"):
            PipelineSpec.model_validate(d)


if __name__ == "__main__":
    unittest.main()
