"""
warehouse.py

Loads the transformed events DataFrame into a real local SQLite database,
standing in for the production warehouse table (`analytics.fact_events`)
targeted by the `load` task in dags/etl_pipeline_dag.py. SQLite is used
(stdlib, zero extra disk footprint) rather than DuckDB to keep the
dependency list minimal and disk usage as low as possible on this machine.
"""
import os
import sqlite3
import pandas as pd

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "warehouse.db")
TABLE_NAME = "fact_events"


def load(df: pd.DataFrame, db_path: str = DB_PATH, table_name: str = TABLE_NAME) -> int:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    # event_date / session_duration_bucket are category/str; ensure sqlite-friendly dtypes
    out = df.copy()
    out["session_duration_bucket"] = out["session_duration_bucket"].astype(str)
    out["event_ts"] = out["event_ts"].astype(str)
    out["is_monetized_event"] = out["is_monetized_event"].astype(int)

    con = sqlite3.connect(db_path)
    try:
        out.to_sql(table_name, con, if_exists="replace", index=False)
        con.commit()
        row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    finally:
        con.close()
    return row_count


def query(sql: str, db_path: str = DB_PATH) -> pd.DataFrame:
    con = sqlite3.connect(db_path)
    try:
        return pd.read_sql_query(sql, con)
    finally:
        con.close()
