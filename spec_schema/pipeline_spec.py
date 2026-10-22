"""The contract for a pipeline spec.

A spec is a YAML file a person writes. This module defines every field it may
contain and rejects anything invalid *before* any code is generated.
"""
from __future__ import annotations

import re
from enum import Enum
from pathlib import Path
from typing import Literal, Optional

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

# Three-part Unity Catalog name: catalog.schema.table
TABLE_NAME = re.compile(r"^[a-z0-9_]+\.[a-z0-9_]+\.[a-z0-9_]+$")


class Strict(BaseModel):
    """Base class: unknown keys in a spec are errors, not silently ignored.
    This catches typos like `on_schema_drfit`."""
    model_config = ConfigDict(extra="forbid")


# --------------------------------------------------------------------- source
class SchemaDrift(str, Enum):
    ADD_COLUMNS = "add_columns"   # new columns are added to the bronze table
    RESCUE = "rescue"             # unexpected fields go into _rescued_data
    QUARANTINE = "quarantine"     # mismatching file goes to a side table + alert
    FAIL = "fail"                 # stop the run


class Source(Strict):
    format: Literal["csv", "json", "parquet"]
    path: str                                  # e.g. s3://bucket/landing/transactions/
    ingest: Literal["autoloader", "batch"] = "autoloader"
    schema_file: str                           # expected columns, see schemas/
    on_schema_drift: SchemaDrift = SchemaDrift.RESCUE
    options: dict[str, str] = Field(default_factory=dict)   # e.g. header: "true"


# --------------------------------------------------------------------- bronze
# Every bronze table carries these; the runtime fills them in.
DEFAULT_AUDIT = ["_run_id", "_source_file", "_ingestion_ts", "_load_type", "_record_hash"]


class Bronze(Strict):
    table: str
    load_type: Literal["full", "incremental", "backfill"] = "incremental"
    audit_columns: list[str] = Field(default_factory=lambda: list(DEFAULT_AUDIT))
    quarantine_table: Optional[str] = None     # required if drift mode is quarantine

    @model_validator(mode="after")
    def _check(self):
        if not TABLE_NAME.match(self.table):
            raise ValueError(f"bronze.table must be catalog.schema.table, got {self.table!r}")
        unknown = set(self.audit_columns) - set(DEFAULT_AUDIT)
        if unknown:
            raise ValueError(f"unknown audit columns: {sorted(unknown)}")
        return self


# --------------------------------------------------------------------- silver
class Check(Strict):
    """One data-quality rule on one column."""
    rule: Literal["not_null", "range", "in_set", "regex"]
    min: Optional[float] = None
    max: Optional[float] = None
    values: Optional[list[str]] = None
    pattern: Optional[str] = None

    @model_validator(mode="after")
    def _params(self):
        if self.rule == "range" and self.min is None and self.max is None:
            raise ValueError("range check needs min and/or max")
        if self.rule == "in_set" and not self.values:
            raise ValueError("in_set check needs values")
        if self.rule == "regex":
            if not self.pattern:
                raise ValueError("regex check needs pattern")
            re.compile(self.pattern)           # fail early on a bad pattern
        return self


class SilverColumn(Strict):
    name: str
    type: str                                  # Spark SQL type, e.g. decimal(18,2)
    expr: Optional[str] = None                 # Spark SQL expression; default = cast(name)
    checks: list[Check] = Field(default_factory=list)


class OnDqFail(str, Enum):
    DROP = "drop"
    WARN = "warn"
    FAIL = "fail"
    QUARANTINE = "quarantine"


class Silver(Strict):
    table: str
    keys: list[str] = Field(min_length=1)      # business key used for dedupe
    order_by: Optional[str] = None             # latest row wins on duplicate keys
    columns: list[SilverColumn] = Field(min_length=1)
    on_dq_fail: OnDqFail = OnDqFail.QUARANTINE
    quarantine_table: Optional[str] = None

    @model_validator(mode="after")
    def _check(self):
        if not TABLE_NAME.match(self.table):
            raise ValueError(f"silver.table must be catalog.schema.table, got {self.table!r}")
        names = [c.name for c in self.columns]
        if len(names) != len(set(names)):
            raise ValueError("duplicate silver column names")
        missing = set(self.keys) - set(names)
        if missing:
            raise ValueError(f"keys not defined as silver columns: {sorted(missing)}")
        if self.on_dq_fail == OnDqFail.QUARANTINE and not self.quarantine_table:
            raise ValueError("on_dq_fail=quarantine requires silver.quarantine_table")
        return self


# ------------------------------------------------------------------- pipeline
class Alerts(Strict):
    events: list[Literal["schema_drift", "dq_threshold_breach", "run_failure"]] = Field(default_factory=list)
    channel: Literal["email", "slack"] = "email"
    target: Optional[str] = None


class PipelineSpec(Strict):
    pipeline: str
    owner: str
    source: Source
    bronze: Bronze
    silver: Silver
    alerts: Alerts = Field(default_factory=Alerts)
    schedule: Literal["continuous", "hourly", "daily", "manual"] = "daily"

    @model_validator(mode="after")
    def _cross(self):
        # Cross-section rule: a drift strategy of "quarantine" needs somewhere to put the rows.
        if self.source.on_schema_drift == SchemaDrift.QUARANTINE and not self.bronze.quarantine_table:
            raise ValueError("on_schema_drift=quarantine requires bronze.quarantine_table")
        return self


def load_spec(path: str | Path) -> PipelineSpec:
    with open(path) as f:
        return PipelineSpec.model_validate(yaml.safe_load(f))
