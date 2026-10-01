"""
test_pipeline.py

Real end-to-end sanity tests for local_pipeline/: runs the actual generate
-> transform -> load -> QC flow and checks that:
  1. The pipeline runs end to end and produces expected row-shape results.
  2. QC genuinely catches an injected bad record (not a check that always
     trivially passes).
"""
import os
import sqlite3
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from local_pipeline.generate_raw_data import generate
from local_pipeline.etl_spark_equivalent import transform
from local_pipeline.warehouse import load, query, TABLE_NAME
from local_pipeline.data_quality import run_all_checks
from local_pipeline.data_mining import run_all as run_mining


@pytest.fixture(scope="module")
def pipeline_run(tmp_path_factory):
    raw_df = generate(n_rows=20_000, seed=7)
    clean_df, stats = transform(raw_df)
    db_path = str(tmp_path_factory.mktemp("wh") / "test_warehouse.db")
    loaded_rows = load(clean_df, db_path=db_path)
    return {
        "raw_df": raw_df,
        "clean_df": clean_df,
        "stats": stats,
        "db_path": db_path,
        "loaded_rows": loaded_rows,
    }


def test_generate_produces_rows():
    df = generate(n_rows=5_000, seed=1)
    assert len(df) >= 5_000  # dedup-sample append makes it slightly larger
    assert "user_id" in df.columns
    assert df["user_id"].isna().sum() > 0  # injected nulls are really there


def test_transform_removes_exact_duplicates(pipeline_run):
    stats = pipeline_run["stats"]
    assert stats["duplicate_rows_removed"] > 0
    assert stats["output_rows"] == stats["input_rows"] - stats["duplicate_rows_removed"]


def test_transform_clips_negative_durations(pipeline_run):
    clean_df = pipeline_run["clean_df"]
    assert (clean_df["session_duration_sec"].dropna() >= 0).all()


def test_load_into_warehouse_real_row_count(pipeline_run):
    assert pipeline_run["loaded_rows"] == len(pipeline_run["clean_df"])
    assert os.path.exists(pipeline_run["db_path"])
    con = sqlite3.connect(pipeline_run["db_path"])
    n = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
    con.close()
    assert n == pipeline_run["loaded_rows"]


def test_quality_control_runs_and_returns_results(pipeline_run, monkeypatch):
    import local_pipeline.data_quality as dq

    monkeypatch.setattr(dq, "query", lambda sql: query(sql, db_path=pipeline_run["db_path"]))
    checks = dq.run_all_checks()
    assert len(checks) == 5
    assert all(c.failing_rows >= 0 for c in checks)


def test_quality_control_catches_injected_bad_record(tmp_path):
    """Build a tiny warehouse with one deliberately bad record and confirm
    the completeness check actually fails on it (not a check that always
    silently passes)."""
    import local_pipeline.data_quality as dq

    good_df = pd.DataFrame(
        {
            "event_id": [1, 2, 3],
            "user_id": ["u_a", "u_b", None],  # one injected bad record: null user_id
            "event_ts": ["2026-08-01", "2026-08-01", "2026-08-01"],
            "region": ["us-east", "us-east", "us-east"],
            "event_type": ["page_view", "page_view", "page_view"],
            "device_type": ["mobile", "mobile", "mobile"],
            "session_duration_sec": [10.0, 20.0, 30.0],
            "order_value": [None, None, None],
            "is_monetized_event": [0, 0, 0],
            "event_date": ["2026-08-01"] * 3,
            "session_duration_bucket": ["10-60s"] * 3,
        }
    )
    db_path = str(tmp_path / "bad_warehouse.db")
    con = sqlite3.connect(db_path)
    good_df.to_sql(TABLE_NAME, con, index=False)
    con.close()

    # call directly against the injected db by monkeypatching the module-level query
    orig_query = dq.query
    try:
        dq.query = lambda sql: query(sql, db_path=db_path)
        result = dq._completeness_user_id()
    finally:
        dq.query = orig_query

    assert result.passed is False
    assert result.failing_rows == 1


def test_data_mining_runs_end_to_end(pipeline_run, monkeypatch):
    import local_pipeline.data_mining as dm

    monkeypatch.setattr(dm, "query", lambda sql: query(sql, db_path=pipeline_run["db_path"]))
    summary = dm.run_all()
    assert "top_correlations" in summary
    assert "top_frequent_value_combinations" in summary
    assert len(summary["top_frequent_value_combinations"]) > 0
