import ast
import contextlib
import io
import unittest
from pathlib import Path

import yaml

from generator.cli import main
from generator.render import ROOT, check_code, render_all, silver_select
from spec_schema.pipeline_spec import Check, SilverColumn, load_spec

SPEC = ROOT / "specs" / "card_transactions.yaml"
OUT = ROOT / "generated" / "card_transactions"


class TestRender(unittest.TestCase):
    def setUp(self):
        self.spec = load_spec(SPEC)
        self.files = render_all(self.spec, SPEC)

    def test_python_outputs_are_valid_python(self):
        for name in ("bronze.py", "silver.py"):
            ast.parse(self.files[name])               # raises SyntaxError if broken

    def test_job_yaml_has_bronze_then_silver(self):
        job = yaml.safe_load(self.files["job.yml"])["resources"]["jobs"]["card_transactions"]
        keys = [t["task_key"] for t in job["tasks"]]
        self.assertEqual(keys, ["bronze", "silver"])
        self.assertEqual(job["tasks"][1]["depends_on"], [{"task_key": "bronze"}])
        self.assertIn("schedule", job)

    def test_output_is_deterministic(self):
        self.assertEqual(self.files, render_all(self.spec, SPEC))

    def test_default_expr_backticks_reserved_names(self):
        sql = silver_select(SilverColumn(name="user", type="int"))
        self.assertEqual(sql, "try_cast((`user`) AS int)")

    def test_check_rendering(self):
        self.assertEqual(check_code("a", Check(rule="not_null")), "F.col('a').isNotNull()")
        self.assertIn(">= 0.0", check_code("a", Check(rule="range", min=0)))
        self.assertIn("isin(['x', 'y'])", check_code("a", Check(rule="in_set", values=["x", "y"])))
        # range/in_set/regex let NULLs through; not_null is the only rule that judges them
        self.assertIn("isNull()", check_code("a", Check(rule="range", min=0)))


class TestCheckMode(unittest.TestCase):
    def test_check_detects_hand_edits(self):
        # Silence the CLI's prints: an expected "STALE" line in the CI log looks like a real failure.
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(main(["generate", str(SPEC)]), 0)
            self.assertEqual(main(["generate", str(SPEC), "--check"]), 0)
            target = OUT / "silver.py"
            original = target.read_text()
            try:
                target.write_text(original + "\n# sneaky manual edit\n")
                self.assertEqual(main(["generate", str(SPEC), "--check"]), 1)
            finally:
                target.write_text(original)


if __name__ == "__main__":
    unittest.main()


class TestWiring(unittest.TestCase):
    def setUp(self):
        self.files = render_all(load_spec(SPEC), SPEC)

    def test_scripts_bootstrap_repo_root(self):
        for name in ("bronze.py", "silver.py"):
            self.assertIn("sys.path.insert(0", self.files[name])

    def test_silver_gets_checkpoint_beside_landing_path(self):
        self.assertIn("transactions_state/silver_checkpoint", self.files["silver.py"])

    def test_only_bronze_task_retries(self):
        tasks = yaml.safe_load(self.files["job.yml"])["resources"]["jobs"]["card_transactions"]["tasks"]
        self.assertEqual([t.get("max_retries") for t in tasks], [1, None])


class TestServerless(unittest.TestCase):
    def test_every_task_uses_the_serverless_environment(self):
        files = render_all(load_spec(SPEC), SPEC)
        job = yaml.safe_load(files["job.yml"])["resources"]["jobs"]["card_transactions"]
        env_keys = {e["environment_key"] for e in job["environments"]}
        for t in job["tasks"]:
            self.assertIn(t["environment_key"], env_keys)


class TestDatabricksBootstrap(unittest.TestCase):
    """Serverless exec()s the script, so __file__ is undefined. The first Databricks run failed on this."""

    def setUp(self):
        self.files = render_all(load_spec(SPEC), SPEC)

    def test_job_passes_bundle_path_to_every_task(self):
        tasks = yaml.safe_load(self.files["job.yml"])["resources"]["jobs"]["card_transactions"]["tasks"]
        for t in tasks:
            self.assertEqual(t["spark_python_task"]["parameters"], ["${workspace.file_path}"])

    def test_scripts_run_without___file__(self):
        import os, sys
        for name in ("bronze.py", "silver.py"):
            marker = "sys.path.insert(0, _bundle_root())"
            src = self.files[name].split(marker)[0] + marker          # the bootstrap part only
            argv, path = sys.argv, list(sys.path)
            try:
                sys.argv = ["x", str(ROOT)]
                exec(compile(src, name, "exec"), {})                # no __file__ in these globals
                self.assertEqual(sys.path[0], str(ROOT))
                sys.argv, cwd = ["x"], os.getcwd()
                os.chdir(ROOT / "runtime")                          # fallback: walk up from cwd
                try:
                    exec(compile(src, name, "exec"), {})
                finally:
                    os.chdir(cwd)
                self.assertEqual(sys.path[0], str(ROOT))
            finally:
                sys.argv, sys.path[:] = argv, path
