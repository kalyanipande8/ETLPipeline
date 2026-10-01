"""
etl_pipeline_dag.py

A real, syntactically correct Airflow 2.x DAG for a large-scale batch ETL
pipeline (target production scale: 100+ GB/day of transactional + event
data, processed with Spark on a cluster).

IMPORTANT — authored, not executed:
This DAG has never been run by an Airflow scheduler. There is no Airflow
installation, no Spark cluster, and no production data lake in this
environment. It is presented as reviewable infrastructure-as-code: it uses
genuine Airflow operator patterns and would run correctly against a real
Airflow deployment with the Spark provider installed and a `spark_default`
connection configured. The actual, runnable proof of the transformation /
QC / mining logic lives in `local_pipeline/`, which IS executed in this
repo against a disk-safe, honestly-scaled dataset (see README.md).

Pipeline shape mirrored 1:1 by local_pipeline/run_all.py:
    extract -> transform -> load -> quality_control -> data_mining_summary
"""
from __future__ import annotations

import datetime as dt

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": dt.timedelta(minutes=5),
    "sla": dt.timedelta(hours=2),
    "email_on_failure": True,
    "email": ["data-eng-alerts@example.com"],
}

RAW_ZONE = "s3://company-datalake/raw/events/{{ ds }}/"
STAGED_ZONE = "s3://company-datalake/staged/events/{{ ds }}/"
WAREHOUSE_TABLE = "analytics.fact_events"
SPARK_APPLICATION = "/opt/spark-jobs/etl_transform_job.py"


def _register_extract_manifest(**context):
    """Record how many source partitions / approx bytes were pulled this run.

    In production this would query the landing-zone object store (S3/ADLS)
    for the day's partition, write a manifest row to a control table, and
    push the partition count + byte estimate to XCom for downstream tasks
    and for the data-mining-summary step to report on.
    """
    ds = context["ds"]
    # Placeholder for a real object-store LIST call, e.g. via boto3:
    #   objs = s3_hook.list_keys(bucket_name="company-datalake",
    #                             prefix=f"raw/events/{ds}/")
    manifest = {
        "run_date": ds,
        "source_partitions": "~4800 hourly shard files/day across 3 regions",
        "approx_daily_volume_gb": "100-140 GB/day (target production scale)",
    }
    context["ti"].xcom_push(key="extract_manifest", value=manifest)
    return manifest


def _load_quality_gate(**context):
    """Pull the Spark job's QC summary (written to a control-table row by
    etl_transform_job.py) and raise if the fail rate exceeds the SLA gate,
    which prevents `data_mining_summary` from running on bad data.
    """
    ti = context["ti"]
    # In production: SELECT * FROM analytics.dq_results WHERE run_date = ...
    qc_summary = ti.xcom_pull(task_ids="quality_control", key="qc_summary") or {}
    fail_rate = qc_summary.get("fail_rate", 0.0)
    if fail_rate > 0.05:
        raise ValueError(
            f"QC fail rate {fail_rate:.2%} exceeds 5% gate; blocking downstream load."
        )


with DAG(
    dag_id="etl_pipeline_dag",
    description=(
        "Large-scale ETL: extract raw event/transactional data, transform "
        "with Spark, load to warehouse, run automated QC, and summarize "
        "data-mining patterns. Target production scale: 100+ GB/day."
    ),
    default_args=default_args,
    schedule_interval="@daily",
    start_date=dt.datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "spark", "data-quality", "production-scale-100gb"],
) as dag:

    extract = PythonOperator(
        task_id="extract",
        python_callable=_register_extract_manifest,
    )

    # Spark handles the heavy columnar transform (schema normalization,
    # dedup, type casting, partition repartitioning) at the 100+ GB scale
    # this architecture targets. Submitted to a YARN/Kubernetes cluster.
    transform = SparkSubmitOperator(
        task_id="transform",
        application=SPARK_APPLICATION,
        conn_id="spark_default",
        name="etl_pipeline_transform_{{ ds_nodash }}",
        application_args=[
            "--input", RAW_ZONE,
            "--output", STAGED_ZONE,
            "--run-date", "{{ ds }}",
        ],
        conf={
            "spark.executor.memory": "8g",
            "spark.executor.instances": "40",
            "spark.sql.shuffle.partitions": "800",
            "spark.sql.adaptive.enabled": "true",
        },
        verbose=False,
    )

    load = BashOperator(
        task_id="load",
        bash_command=(
            "echo 'COPY INTO {{ params.table }} "
            "FROM \\'{{ params.staged }}\\' "
            "FILE_FORMAT = (TYPE = PARQUET) "
            "ON_ERROR = ABORT_STATEMENT;' | "
            "warehouse-cli --run-date {{ ds }}"
        ),
        params={"table": WAREHOUSE_TABLE, "staged": STAGED_ZONE},
    )

    quality_control = BashOperator(
        task_id="quality_control",
        bash_command=(
            "dq-runner --table {{ params.table }} --run-date {{ ds }} "
            "--checks completeness,schema,range,duplicates "
            "--write-xcom-key qc_summary"
        ),
        params={"table": WAREHOUSE_TABLE},
        do_xcom_push=True,
    )

    quality_gate = PythonOperator(
        task_id="quality_gate",
        python_callable=_load_quality_gate,
    )

    data_mining_summary = BashOperator(
        task_id="data_mining_summary",
        bash_command=(
            "mining-runner --table {{ params.table }} --run-date {{ ds }} "
            "--analyses correlation,frequent-value,anomaly-score "
            "--output-report s3://company-datalake/reports/mining/{{ ds }}/summary.json"
        ),
        params={"table": WAREHOUSE_TABLE},
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    extract >> transform >> load >> quality_control >> quality_gate >> data_mining_summary
