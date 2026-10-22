"""Schema-drift detection. Pure Python: no Spark, so it is fully unit-testable.

Policy (the one decision worth confirming):
  * EXTRA columns   = additive drift. add_columns / rescue tolerate it.
  * MISSING columns = breaking drift. Never tolerated silently: a file that lost
    columns (or is simply the wrong file) would load as rows of NULLs.
  * quarantine / fail react to ANY drift.
"""
from __future__ import annotations

import csv
import os
from dataclasses import dataclass
from enum import Enum
from typing import Iterator


def _norm(name: str) -> str:
    return name.strip().lower()


@dataclass(frozen=True)
class DriftReport:
    extra: tuple[str, ...]      # in the file, not in the expected schema
    missing: tuple[str, ...]    # expected, but absent from the file

    @property
    def has_drift(self) -> bool:
        return bool(self.extra or self.missing)


def compare(expected: list[str], actual: list[str]) -> DriftReport:
    """Compare by name only, ignoring case, whitespace and column order."""
    exp = {_norm(c): c for c in expected}
    act = {_norm(c): c.strip() for c in actual}
    return DriftReport(
        extra=tuple(act[k] for k in act if k not in exp),
        missing=tuple(exp[k] for k in exp if k not in act),
    )


class Action(str, Enum):
    PROCEED = "proceed"
    QUARANTINE = "quarantine"
    FAIL = "fail"


@dataclass(frozen=True)
class Decision:
    action: Action
    reason: str


def describe(report: DriftReport) -> str:
    parts = []
    if report.extra:
        parts.append(f"new columns: {list(report.extra)}")
    if report.missing:
        parts.append(f"missing columns: {list(report.missing)}")
    return "; ".join(parts)


def decide(report: DriftReport, strategy: str) -> Decision:
    if not report.has_drift:
        return Decision(Action.PROCEED, "schema matches")
    why = describe(report)
    if strategy == "fail":
        return Decision(Action.FAIL, why)
    if strategy == "quarantine":
        return Decision(Action.QUARANTINE, why)
    if strategy in ("add_columns", "rescue"):
        if report.missing:
            return Decision(Action.FAIL, f"{why} (missing columns are never tolerated)")
        return Decision(Action.PROCEED, why)     # additive only; Auto Loader absorbs it
    raise ValueError(f"unknown on_schema_drift strategy: {strategy!r}")


def read_csv_header(path: str, **csv_options) -> list[str]:
    """First row of a CSV. utf-8-sig strips the BOM that Excel exports add."""
    with open(path, newline="", encoding="utf-8-sig") as f:
        return next(csv.reader(f, **csv_options), [])


def iter_csv_files(folder: str) -> Iterator[str]:
    """CSVs under folder, recursively, skipping hidden / underscore directories
    (Auto Loader keeps its own state in such folders)."""
    for root, dirs, files in os.walk(folder):
        dirs[:] = sorted(d for d in dirs if not d.startswith((".", "_")))
        for name in sorted(files):
            if name.lower().endswith(".csv") and not name.startswith((".", "_")):
                yield os.path.join(root, name)


def scan_folder(folder: str, expected: list[str], strategy: str) -> list[tuple[str, DriftReport, Decision]]:
    """(file, report, decision) for every CSV whose decision is not PROCEED-with-no-drift."""
    results = []
    for path in iter_csv_files(folder):
        report = compare(expected, read_csv_header(path))
        if report.has_drift:
            results.append((path, report, decide(report, strategy)))
    return results
