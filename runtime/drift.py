"""Schema-drift detection. Pure Python: no Spark, so it is fully unit-testable.

Policy (the one decision worth confirming):
  * EXTRA columns   = additive drift. add_columns / rescue tolerate it.
  * MISSING columns = breaking drift. Never tolerated silently: a file that lost
    columns (or is simply the wrong file) would load as rows of NULLs.
  * quarantine / fail react to ANY drift.
"""
from __future__ import annotations

import csv
import json
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
    unreadable: str = ""        # set when the file can't be parsed in its declared format

    @property
    def has_drift(self) -> bool:
        return bool(self.extra or self.missing or self.unreadable)


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
    if report.unreadable:
        parts.append(f"unreadable file: {report.unreadable}")
    if report.extra:
        parts.append(f"new columns: {list(report.extra)}")
    if report.missing:
        parts.append(f"missing columns: {list(report.missing)}")
    return "; ".join(parts)


def decide(report: DriftReport, strategy: str) -> Decision:
    if not report.has_drift:
        return Decision(Action.PROCEED, "schema matches")
    why = describe(report)
    if report.unreadable and strategy not in ("fail", "quarantine"):
        return Decision(Action.FAIL, f"{why} (unreadable files are never tolerated)")
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


def read_json_keys(path: str) -> list[str]:
    """Top-level keys of a JSON Lines file: the union over ALL records, in first-seen order.

    Unlike a CSV there is no header, and one record may omit a key another has, so the
    whole file is scanned. A key missing from some records is not drift (it loads as
    NULL); a key missing from every record is. Raises ValueError if the file is not
    JSON Lines (one JSON object per line), e.g. a pretty-printed array."""
    keys: dict[str, None] = {}
    with open(path, encoding="utf-8-sig") as f:
        for n, line in enumerate(f, start=1):
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as e:
                raise ValueError(f"line {n} is not valid JSON ({e.msg}); expected JSON Lines") from e
            if not isinstance(record, dict):
                raise ValueError(f"line {n} is not a JSON object; expected JSON Lines")
            keys.update(dict.fromkeys(record))
    return list(keys)


# File extensions per source format. Everything else in the folder is ignored.
EXTENSIONS = {"csv": (".csv",), "json": (".json", ".jsonl", ".ndjson")}


def iter_files(folder: str, fmt: str = "csv") -> Iterator[str]:
    """Files of the given format under folder, recursively, skipping hidden /
    underscore directories (Auto Loader keeps its own state in such folders)."""
    exts = EXTENSIONS[fmt]
    for root, dirs, files in os.walk(folder):
        dirs[:] = sorted(d for d in dirs if not d.startswith((".", "_")))
        for name in sorted(files):
            if name.lower().endswith(exts) and not name.startswith((".", "_")):
                yield os.path.join(root, name)


def iter_csv_files(folder: str) -> Iterator[str]:
    return iter_files(folder, "csv")


def inspect_file(path: str, expected: list[str], fmt: str = "csv") -> DriftReport:
    """Compare one file's columns (CSV header / JSON keys) with the expected schema."""
    if fmt == "json":
        try:
            actual = read_json_keys(path)
        except ValueError as e:
            return DriftReport(extra=(), missing=(), unreadable=str(e))
    else:
        actual = read_csv_header(path)
    return compare(expected, actual)


def scan_folder(folder: str, expected: list[str], strategy: str, fmt: str = "csv") -> list[tuple[str, DriftReport, Decision]]:
    """(file, report, decision) for every file that shows drift."""
    results = []
    for path in iter_files(folder, fmt):
        report = inspect_file(path, expected, fmt)
        if report.has_drift:
            results.append((path, report, decide(report, strategy)))
    return results
