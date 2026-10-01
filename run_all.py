"""
run_all.py

Runs the full real local pipeline end to end:
  generate raw data -> extract (read CSV) -> transform (pandas, Spark-
  equivalent) -> load (SQLite warehouse) -> quality_control (real DQ
  checks) -> data_mining_summary (real correlation/frequent-value mining)

This is the runnable local proof of the logic authored (but never executed)
in dags/etl_pipeline_dag.py. Every number this script prints comes from
actually running it, not from a hand-typed placeholder.
"""
import json
import os
import time

import pandas as pd

from local_pipeline.generate_raw_data import generate, OUT_DIR
from local_pipeline.etl_spark_equivalent import transform
from local_pipeline.warehouse import load, DB_PATH
from local_pipeline.data_quality import run_all_checks
from local_pipeline.data_mining import run_all as run_mining

OUTPUT_REPORT = os.path.join(os.path.dirname(__file__), "output", "last_run_report.json")


def main():
    report = {}
    t0 = time.time()

    # --- extract (generate + "read" the raw landing-zone data) ---
    t_start = time.time()
    raw_df = generate()
    extract_sec = time.time() - t_start
    raw_bytes = raw_df.memory_usage(deep=True).sum()
    print(f"[extract] generated {len(raw_df):,} raw rows "
          f"({raw_bytes / (1024*1024):.1f} MB in-memory) in {extract_sec:.3f}s")

    # --- transform ---
    t_start = time.time()
    clean_df, tstats = transform(raw_df)
    transform_sec = time.time() - t_start
    print(f"[transform] {tstats['input_rows']:,} -> {tstats['output_rows']:,} rows "
          f"({tstats['duplicate_rows_removed']:,} exact duplicates removed) "
          f"in {transform_sec:.3f}s")

    # --- load ---
    t_start = time.time()
    loaded_rows = load(clean_df)
    load_sec = time.time() - t_start
    db_size_mb = os.path.getsize(DB_PATH) / (1024 * 1024)
    print(f"[load] {loaded_rows:,} rows into {DB_PATH} ({db_size_mb:.1f} MB) "
          f"in {load_sec:.3f}s")

    # --- quality_control ---
    t_start = time.time()
    checks = run_all_checks()
    qc_sec = time.time() - t_start
    n_pass = sum(1 for c in checks if c.passed)
    n_total = len(checks)
    print(f"[quality_control] {n_pass}/{n_total} checks passed in {qc_sec:.3f}s")
    for c in checks:
        status = "PASS" if c.passed else "FAIL"
        print(f"  [{status}] {c.name}: {c.detail} (threshold {c.threshold})")

    # --- data_mining_summary ---
    t_start = time.time()
    mining = run_mining()
    mining_sec = time.time() - t_start
    print(f"[data_mining_summary] computed in {mining_sec:.3f}s")
    print("  Top correlations:")
    for p in mining["top_correlations"]:
        print(f"    {p['feature_a']} vs {p['feature_b']}: r={p['pearson_r']}")
    print("  Top frequent value combinations:")
    for f in mining["top_frequent_value_combinations"][:5]:
        print(f"    {f['region']}/{f['event_type']}/{f['device_type']}: {f['count']:,}")

    total_sec = time.time() - t0

    report = {
        "run_timestamp": pd.Timestamp.now().isoformat(),
        "extract": {
            "raw_rows": int(len(raw_df)),
            "raw_in_memory_bytes": int(raw_bytes),
            "duration_sec": round(extract_sec, 4),
        },
        "transform": {**tstats, "duration_sec": round(transform_sec, 4)},
        "load": {
            "loaded_rows": int(loaded_rows),
            "warehouse_db_bytes": os.path.getsize(DB_PATH),
            "duration_sec": round(load_sec, 4),
        },
        "quality_control": {
            "checks_passed": n_pass,
            "checks_total": n_total,
            "duration_sec": round(qc_sec, 4),
            "results": [c.__dict__ for c in checks],
        },
        "data_mining_summary": {**mining, "duration_sec": round(mining_sec, 4)},
        "total_duration_sec": round(total_sec, 4),
        "scale_note": (
            "This run processed real rows generated/persisted on this machine "
            "(see extract/load byte counts above). The resume bullet's '100+ GB' "
            "refers to the target production scale this Airflow+Spark "
            "architecture (dags/etl_pipeline_dag.py) is designed for, not what "
            "was literally processed by this local proof-of-logic run."
        ),
    }

    os.makedirs(os.path.dirname(OUTPUT_REPORT), exist_ok=True)
    with open(OUTPUT_REPORT, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"\nFull report written to {OUTPUT_REPORT}")
    print(f"Total pipeline duration: {total_sec:.3f}s")


if __name__ == "__main__":
    main()
