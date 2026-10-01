"""
etl_spark_equivalent.py

pandas stand-in for the Spark transform job described in
dags/etl_pipeline_dag.py (etl_transform_job.py submitted via
SparkSubmitOperator). Performs the SAME transformation logic a Spark job
would: schema/type normalization, deduplication, null handling for
non-critical fields, and derived-column computation — just executed with
pandas on a disk-safe local sample instead of Spark on a cluster.
"""
import pandas as pd
import numpy as np


def transform(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # 1. Deduplicate on full row content (Spark: .dropDuplicates())
    before = len(df)
    df = df.drop_duplicates()
    dedup_removed = before - len(df)

    # 2. Normalize types (Spark: explicit schema casting)
    df["event_ts"] = pd.to_datetime(df["event_ts"], errors="coerce")
    df["session_duration_sec"] = pd.to_numeric(
        df["session_duration_sec"], errors="coerce"
    )
    df["order_value"] = pd.to_numeric(df["order_value"], errors="coerce")

    # 3. Schema normalization: fold any event_type not in the known set into
    # 'UNKNOWN_EVENT' rather than silently dropping (Spark: when/otherwise)
    known_types = {"page_view", "add_to_cart", "purchase", "signup", "refund"}
    df["event_type"] = df["event_type"].where(
        df["event_type"].isin(known_types), other="UNKNOWN_EVENT"
    )

    # 4. Clip clearly-invalid session durations (negative) to null rather than
    # silently keeping bad data (Spark: when(col < 0, None).otherwise(col))
    df.loc[df["session_duration_sec"] < 0, "session_duration_sec"] = np.nan

    # 5. Derived column: is_monetized_event (purchase or refund)
    df["is_monetized_event"] = df["event_type"].isin(["purchase", "refund"])

    # 6. Derived column: event_date (partition key downstream, mirrors Spark
    # writing out date-partitioned parquet)
    df["event_date"] = df["event_ts"].dt.date.astype(str)

    # 7. Derived column: session_duration_bucket for downstream mining
    bins = [-np.inf, 10, 60, 300, 1800, np.inf]
    labels = ["<10s", "10-60s", "1-5min", "5-30min", "30min+"]
    df["session_duration_bucket"] = pd.cut(
        df["session_duration_sec"], bins=bins, labels=labels
    )

    stats = {
        "input_rows": before,
        "duplicate_rows_removed": dedup_removed,
        "output_rows": len(df),
    }
    return df, stats
