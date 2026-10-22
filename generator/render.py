"""Turn a validated PipelineSpec into generated files.

Design rule: all decisions live here, in Python. Templates only lay text out.
Output is deterministic (no timestamps) so CI can regenerate and diff.
"""
from __future__ import annotations

import hashlib
import pprint
from pathlib import Path

import yaml
from jinja2 import Environment, FileSystemLoader, StrictUndefined

from spec_schema.pipeline_spec import Check, PipelineSpec, SilverColumn

ROOT = Path(__file__).resolve().parent.parent
TEMPLATES = Path(__file__).resolve().parent / "templates"

# Output file -> template that produces it.
OUTPUTS = {"bronze.py": "bronze.py.j2", "silver.py": "silver.py.j2", "job.yml": "job.yml.j2"}

# How the spec's schedule words map to a Databricks (quartz) cron.
CRON = {"hourly": "0 0 * * * ?", "daily": "0 0 6 * * ?"}


def _lit(value) -> str:
    """Python source for a literal. repr() handles all quoting/escaping safely."""
    return repr(value)


def check_code(column: str, check: Check) -> str:
    """PySpark source for one DQ rule. True means the row PASSES.

    range / in_set / regex ignore NULLs: whether a value may be missing is
    the job of not_null alone. That way one bad value produces one reason.
    """
    col = f"F.col({_lit(column)})"
    if check.rule == "not_null":
        return f"{col}.isNotNull()"
    if check.rule == "range":
        parts = []
        if check.min is not None:
            parts.append(f"({col} >= {check.min!r})")
        if check.max is not None:
            parts.append(f"({col} <= {check.max!r})")
        cond = " & ".join(parts)
    elif check.rule == "in_set":
        cond = f"{col}.isin({check.values!r})"
    else:  # regex
        cond = f"{col}.rlike({_lit(check.pattern)})"
    return f"({col}.isNull() | ({cond}))"


def silver_select(col: SilverColumn) -> str:
    """SQL for one silver column. try_cast turns bad values into NULL instead of
    raising (Databricks serverless runs in ANSI mode). Default expression is the
    bronze column itself, back-ticked because names like `user` are SQL keywords."""
    expr = col.expr or f"`{col.name}`"
    return f"try_cast(({expr}) AS {col.type})"


def build_context(spec: PipelineSpec, spec_path: Path) -> dict:
    schema_path = ROOT / spec.source.schema_file
    if not schema_path.exists():
        raise FileNotFoundError(f"schema_file not found: {spec.source.schema_file}")
    expected = yaml.safe_load(schema_path.read_text())["columns"]

    spec_text = spec_path.read_text() + schema_path.read_text()
    header = (
        f"GENERATED from {spec_path.name} (sha256 {hashlib.sha256(spec_text.encode()).hexdigest()[:12]}). "
        "DO NOT EDIT - change the spec and regenerate."
    )

    silver_cols = [{"name": c.name, "sql": silver_select(c)} for c in spec.silver.columns]
    checks = [
        {"column": c.name, "rule": k.rule, "code": check_code(c.name, k)}
        for c in spec.silver.columns for k in c.checks
    ]

    cfg = lambda d: pprint.pformat(d, sort_dicts=False, width=100)  # noqa: E731
    return {
        "header": header,
        "spec": spec,
        "bronze_config": cfg({
            "pipeline": spec.pipeline,
            "format": spec.source.format,
            "path": spec.source.path,
            "ingest": spec.source.ingest,
            "options": spec.source.options,
            "on_schema_drift": spec.source.on_schema_drift.value,
            "expected_columns": [c["name"] for c in expected],
            "table": spec.bronze.table,
            "quarantine_table": spec.bronze.quarantine_table,
            "load_type": spec.bronze.load_type,
            "audit_columns": spec.bronze.audit_columns,
            "alert_on_drift": "schema_drift" in spec.alerts.events,
        }),
        "silver_config": cfg({
            "pipeline": spec.pipeline,
            "source_table": spec.bronze.table,
            "table": spec.silver.table,
            "keys": spec.silver.keys,
            "order_by": spec.silver.order_by,
            "on_dq_fail": spec.silver.on_dq_fail.value,
            "quarantine_table": spec.silver.quarantine_table,
            "checkpoint": spec.source.path.rstrip("/") + "_state/silver_checkpoint",
        }),
        "silver_cols": silver_cols,
        "checks": checks,
        "cron": CRON.get(spec.schedule),
    }


def make_env() -> Environment:
    env = Environment(
        loader=FileSystemLoader(TEMPLATES),
        undefined=StrictUndefined,   # a typo in a template is an error, not blank text
        trim_blocks=True, lstrip_blocks=True, keep_trailing_newline=True,
    )
    env.filters["py"] = _lit
    return env


def render_all(spec: PipelineSpec, spec_path: Path) -> dict[str, str]:
    """Return {filename: content} for every generated file."""
    ctx = build_context(spec, spec_path)
    env = make_env()
    return {name: env.get_template(tpl).render(**ctx) for name, tpl in OUTPUTS.items()}
