"""
data_quality.py

Real automated QC checks run against the real processed data in the SQLite
warehouse, mirroring the `quality_control` task (`dq-runner --checks
completeness,schema,range,duplicates`) in dags/etl_pipeline_dag.py.

Each check returns a real pass/fail outcome with a real failing-row count,
not a hand-typed placeholder.
"""
from dataclasses import dataclass
from typing import List
import pandas as pd

from .warehouse import query, TABLE_NAME

KNOWN_EVENT_TYPES = {
    "page_view",
    "add_to_cart",
    "purchase",
    "signup",
    "refund",
}


@dataclass
class CheckResult:
    name: str
    category: str
    passed: bool
    failing_rows: int
    threshold: int
    detail: str


def _completeness_user_id() -> CheckResult:
    df = query(f"SELECT COUNT(*) AS n FROM {TABLE_NAME} WHERE user_id IS NULL OR user_id = ''")
    n = int(df["n"].iloc[0])
    threshold = 0
    return CheckResult(
        "completeness_user_id_not_null", "completeness", n <= threshold, n, threshold,
        f"{n} row(s) with null/empty user_id",
    )


def _schema_event_type() -> CheckResult:
    df = query(f"SELECT DISTINCT event_type FROM {TABLE_NAME}")
    bad_values = sorted(set(df["event_type"]) - KNOWN_EVENT_TYPES)
    full = query(f"SELECT event_type FROM {TABLE_NAME}")
    n = int((~full["event_type"].isin(KNOWN_EVENT_TYPES)).sum())
    threshold = 0
    return CheckResult(
        "schema_event_type_known_values", "schema", n <= threshold, n, threshold,
        f"{n} row(s) with event_type outside known set {sorted(bad_values)}",
    )


def _range_session_duration() -> CheckResult:
    df = query(
        f"SELECT COUNT(*) AS n FROM {TABLE_NAME} "
        f"WHERE session_duration_sec IS NOT NULL AND session_duration_sec < 0"
    )
    n = int(df["n"].iloc[0])
    threshold = 0
    return CheckResult(
        "range_session_duration_non_negative", "range", n <= threshold, n, threshold,
        f"{n} row(s) with negative session_duration_sec",
    )


def _range_order_value() -> CheckResult:
    # Flag implausible order values (> $10,000) as an outlier/range violation
    df = query(
        f"SELECT COUNT(*) AS n FROM {TABLE_NAME} "
        f"WHERE order_value IS NOT NULL AND order_value > 10000"
    )
    n = int(df["n"].iloc[0])
    threshold = 0
    return CheckResult(
        "range_order_value_plausible", "range", n <= threshold, n, threshold,
        f"{n} row(s) with order_value > $10,000 (implausible outlier)",
    )


def _duplicate_event_id() -> CheckResult:
    df = query(
        f"SELECT COUNT(*) AS n FROM ("
        f"  SELECT event_id, COUNT(*) AS c FROM {TABLE_NAME} "
        f"  GROUP BY event_id HAVING c > 1"
        f")"
    )
    n = int(df["n"].iloc[0])
    threshold = 0
    return CheckResult(
        "duplicate_event_id", "duplicates", n <= threshold, n, threshold,
        f"{n} duplicate event_id group(s) remaining after dedup",
    )


def run_all_checks() -> List[CheckResult]:
    return [
        _completeness_user_id(),
        _schema_event_type(),
        _range_session_duration(),
        _range_order_value(),
        _duplicate_event_id(),
    ]
