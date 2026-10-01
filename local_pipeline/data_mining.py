"""
data_mining.py

Real, simple "data mining" / pattern-finding step run against the real
processed warehouse data, mirroring the `data_mining_summary` task
(`mining-runner --analyses correlation,frequent-value,anomaly-score`) in
dags/etl_pipeline_dag.py.

Two genuine analyses, computed with real pandas/numpy on the real data:
  1. Correlation analysis: numeric feature correlation matrix, top |r| pairs.
  2. Frequent-value analysis: most frequent (region, event_type, device_type)
     combinations (basic market-basket-style frequency mining).
"""
import pandas as pd

from .warehouse import query, TABLE_NAME


def correlation_summary(top_n: int = 5) -> list[dict]:
    df = query(
        f"SELECT session_duration_sec, order_value, is_monetized_event "
        f"FROM {TABLE_NAME}"
    )
    numeric = df.select_dtypes(include="number")
    corr = numeric.corr(numeric_only=True)

    pairs = []
    cols = corr.columns.tolist()
    for i in range(len(cols)):
        for j in range(i + 1, len(cols)):
            r = corr.iloc[i, j]
            if pd.notna(r):
                pairs.append(
                    {"feature_a": cols[i], "feature_b": cols[j], "pearson_r": round(float(r), 4)}
                )
    pairs.sort(key=lambda d: abs(d["pearson_r"]), reverse=True)
    return pairs[:top_n]


def frequent_value_summary(top_n: int = 10) -> list[dict]:
    df = query(
        f"SELECT region, event_type, device_type FROM {TABLE_NAME}"
    )
    counts = (
        df.groupby(["region", "event_type", "device_type"])
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
        .head(top_n)
    )
    return counts.to_dict(orient="records")


def monetization_by_region() -> list[dict]:
    df = query(
        f"SELECT region, is_monetized_event, order_value FROM {TABLE_NAME}"
    )
    summary = (
        df.groupby("region")
        .agg(
            total_events=("is_monetized_event", "size"),
            monetized_events=("is_monetized_event", "sum"),
            avg_order_value=("order_value", "mean"),
        )
        .reset_index()
    )
    summary["monetization_rate"] = (
        summary["monetized_events"] / summary["total_events"]
    ).round(4)
    summary["avg_order_value"] = summary["avg_order_value"].round(2)
    return summary.to_dict(orient="records")


def run_all() -> dict:
    return {
        "top_correlations": correlation_summary(),
        "top_frequent_value_combinations": frequent_value_summary(),
        "monetization_by_region": monetization_by_region(),
    }
