# ETL Pipeline Automation

Airflow DAG design + Spark-pattern ETL for "Data Pipeline & ETL Automation |
Big Data — Designed scalable ETL workflows with Airflow and Spark; processed
100+ GB datasets with automated quality control and data mining," plus a
real, runnable local pandas/SQLite pipeline that executes the same
transformation, QC, and data-mining logic to prove it's correct.

## What's real vs. authored infrastructure-as-code (read this first)

This project has no Airflow deployment and no Spark cluster — nor should it:
standing one up for a portfolio project would either cost money or produce
nothing but a toy install. Disk space on this machine was also critically
low when this was built (~19 GB free), which further ruled out installing
Airflow/Spark or generating anything close to 100 GB of data. So, like this
repo's siblings (`CMC-DataPipeline-AIReady`, `HPC-Container-Orchestration-System`),
it's built and presented honestly in two parts:

1. **`dags/etl_pipeline_dag.py` — real, syntactically correct, reviewed
   code, authored but never executed against a live deployment.**
   - It is a real Airflow 2.x DAG using genuine operator patterns
     (`PythonOperator`, `BashOperator`, `SparkSubmitOperator`, SLA/retry
     config, XCom-based quality gating) orchestrating
     `extract -> transform (Spark) -> load -> quality_control -> quality_gate
     -> data_mining_summary`.
   - It would run correctly on a real Airflow instance with the Spark
     provider installed and a `spark_default` connection configured.
     **It has never been run by an Airflow scheduler in this
     environment** — there is no Airflow here. Verified only with
     `python3 -m py_compile` (passes) since there's no Airflow package
     installed to import-check it against.
   - Treat it as reviewable infra-as-code, not evidence of a production
     deployment.

2. **`local_pipeline/` — a real, runnable local pipeline**, not a live
   Airflow/Spark deployment. It is genuine Python that:
   - generates a synthetic raw "events" dataset with the same column
     shapes/dtypes and the same classes of real-world defects (nulls,
     out-of-range values, duplicates, an invalid category value) that a
     production ingest pipeline has to handle
     (`local_pipeline/generate_raw_data.py`),
   - transforms it with pandas standing in for the Spark transform job —
     dedup, type normalization, schema normalization, range clipping,
     derived columns — the *same logic* `etl_transform_job.py` would run on
     a cluster (`local_pipeline/etl_spark_equivalent.py`),
   - loads the result into a real local SQLite database standing in for
     the production warehouse table (`local_pipeline/warehouse.py`),
   - runs real completeness / schema / range / duplicate-detection data
     quality checks with real SQL against the real loaded data
     (`local_pipeline/data_quality.py`) — the runnable stand-in for the
     DAG's `quality_control` task,
   - runs a real data-mining step — correlation analysis and frequent-value
     (basic market-basket-style) analysis — over the real processed data
     (`local_pipeline/data_mining.py`) — the runnable stand-in for
     `data_mining_summary`.

   **Every number in the Results section below was produced by actually
   running `python3 run_all.py` and `pytest -q`** in this repository on
   2026-10-01 — nothing is hand-typed or invented.

## Scale honesty: "100+ GB" vs. what actually ran here

The resume bullet's "processed 100+ GB datasets" describes the **target
production scale** this Airflow + Spark architecture is designed for (e.g.
`SparkSubmitOperator` config requests 40 executors × 8 GB, consistent with a
cluster-scale multi-hundred-GB/day workload, and the DAG's extract step
comments document an assumed ~100-140 GB/day source volume).

What was **literally, actually processed** by the runnable `local_pipeline/`
in this repo is far smaller, by design — disk-safe and fast enough to run
and test on a laptop with ~19 GB free:

- **402,000 raw rows generated** (~112.8 MB in-memory as a pandas
  DataFrame), reduced to **400,000 rows after deduplication**.
- The resulting SQLite warehouse (`data/warehouse.db`) is **38.4 MB**.

This is an honest, disk-safe proof of the *transformation, QC, and mining
logic* — not a claim that 100 GB was processed on this machine. The
`dags/etl_pipeline_dag.py` architecture (Spark executors, partitioned S3
zones, `COPY INTO` bulk warehouse load) is what would actually scale this
same logic to the 100+ GB production target.

## Pipeline stages (identical shape in both the DAG and the local pipeline)

```
extract -> transform -> load -> quality_control -> data_mining_summary
```

| Stage | `dags/etl_pipeline_dag.py` (authored) | `local_pipeline/` (real, run here) |
|---|---|---|
| extract | List/manifest raw S3 partitions | Generate synthetic raw events CSV-shaped DataFrame |
| transform | `SparkSubmitOperator` running a PySpark job | `etl_spark_equivalent.py` — same logic in pandas |
| load | `COPY INTO` a Snowflake/warehouse table | `warehouse.py` — load into local SQLite |
| quality_control | `dq-runner` (completeness/schema/range/duplicates) | `data_quality.py` — same 4 check categories, real SQL |
| data_mining_summary | `mining-runner` (correlation/frequent-value/anomaly) | `data_mining.py` — real correlation + frequent-value analysis |

## How to run

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

python3 run_all.py     # runs the full local pipeline end to end
pytest -q              # runs the test suite
```

## Results (real, from an actual run on 2026-10-01)

```
$ python3 run_all.py
[extract] generated 402,000 raw rows (112.8 MB in-memory) in 0.881s
[transform] 402,000 -> 400,000 rows (2,000 exact duplicates removed) in 1.709s
[load] 400,000 rows into data/warehouse.db (38.4 MB) in 2.180s
[quality_control] 2/5 checks passed in 1.117s
  [FAIL] completeness_user_id_not_null: 4800 row(s) with null/empty user_id (threshold 0)
  [FAIL] schema_event_type_known_values: 50 row(s) with event_type outside known set ['UNKNOWN_EVENT'] (threshold 0)
  [PASS] range_session_duration_non_negative: 0 row(s) with negative session_duration_sec (threshold 0)
  [FAIL] range_order_value_plausible: 136 row(s) with order_value > $10,000 (implausible outlier) (threshold 0)
  [PASS] duplicate_event_id: 0 duplicate event_id group(s) remaining after dedup (threshold 0)
[data_mining_summary] computed in 2.079s
  Top correlations:
    session_duration_sec vs order_value: r=0.0019
    session_duration_sec vs is_monetized_event: r=0.001
    order_value vs is_monetized_event: r=0.0006
  Top frequent value combinations:
    us-east/page_view/mobile: 39,444
    us-west/page_view/mobile: 33,071
    eu-west/page_view/mobile: 26,496
    eu-central/page_view/mobile: 19,863
    us-east/page_view/desktop: 19,732

Total pipeline duration: 8.745s
```

```
$ pytest -q
.......                                                                  [100%]
7 passed in 2.09s
```

The QC fail/pass split is not cosmetic: `generate_raw_data.py` deliberately
injects real nulls (1.2% of `user_id`), a real invalid category
(`UNKNOWN_EVENT`, 50 rows), real negative durations (clipped by the
transform step, so that check passes), and real implausible order-value
outliers — so the three FAIL results above are genuine QC failures against
genuinely dirty data, not vacuous checks that always pass. One test
(`test_quality_control_catches_injected_bad_record`) specifically builds a
3-row warehouse with one injected null `user_id` and asserts the
completeness check fails with exactly 1 failing row, to prove the QC logic
actually detects bad data rather than always returning green.

Row/byte counts above are reproducible: `generate_raw_data.py` uses a fixed
random seed (42), so rerunning `python3 run_all.py` regenerates the same
402,000 raw rows and the same defect counts every time.

## Project layout

```
dags/etl_pipeline_dag.py       Authored-not-executed Airflow 2.x DAG
local_pipeline/                Real, runnable pandas + SQLite pipeline (the proof)
  generate_raw_data.py           Synthetic raw "events" data generator (w/ injected defects)
  etl_spark_equivalent.py        pandas stand-in for the Spark transform job
  warehouse.py                   Loads into a local SQLite warehouse (stand-in for prod DW)
  data_quality.py                Real completeness/schema/range/duplicate QC checks
  data_mining.py                 Real correlation + frequent-value pattern mining
run_all.py                     Runs the full local pipeline end to end, writes output/last_run_report.json
tests/test_pipeline.py         Real end-to-end tests incl. QC-catches-bad-record sanity check
requirements.txt
.gitignore
```
