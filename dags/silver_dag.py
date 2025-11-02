# dags/silver_transformation_pipeline.py
from __future__ import annotations


import json
import sys
import subprocess
from datetime import timedelta,datetime
import json, os, shlex, shutil, subprocess
from datetime import datetime, timedelta


from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils import timezone
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from clickhouse_driver import Client as CHClient


# ---------- Config ----------
LAG_DAYS = 0



# Paths inside the Airflow containers
DBT_PROJECT_DIR = Variable.get("DBT_PROJECT_DIR", default_var="/opt/airflow/dbt_project")
DBT_PROFILES_DIR = Variable.get("DBT_PROFILES_DIR", default_var="/opt/airflow/dbt_project")

def _get_ch():
    from airflow.hooks.base import BaseHook
    from clickhouse_driver import Client as CHClient
    conn = BaseHook.get_connection("clickhouse_default")
    return CHClient(
        host=conn.host or "clickhouse-server",
        port=conn.port or 9000,
        user=conn.login or "default",
        password=conn.password or "",
        database=(conn.schema or "default"),
        settings=(conn.extra_dejson.get("settings", {}) if isinstance(conn.extra_dejson.get("settings", {}), dict) else {}),
    )


def _ensure_bronze_prereqs():
    ch = _get_ch()
    # DBs
    ch.execute("CREATE DATABASE IF NOT EXISTS bronze")
    ch.execute("CREATE DATABASE IF NOT EXISTS silver")


    # merchants — create with the columns your silver model references
    ch.execute("""
        CREATE TABLE IF NOT EXISTS bronze.merchants (
            `id` Nullable(String),
            `acquirer_id` Nullable(String),
            `name` Nullable(String),
            `state` Nullable(String),
            `created_at` Nullable(String),
            `updated_at` Nullable(String),
            `_snapshot_date` Nullable(String),
            `_source_file` Nullable(String)
        )
        ENGINE = MergeTree
        ORDER BY tuple()
    """)


    # payments — used by silver_payments
    ch.execute("""
        CREATE TABLE IF NOT EXISTS bronze.payments (
            `id` Nullable(String),
            `merchant_id` Nullable(String),
            `acquirer_id` Nullable(String),
            `state` Nullable(String),
            `card_type` Nullable(String),
            `reference` Nullable(String),
            `order_reference` Nullable(String),
            `details` Nullable(String),
            `created_at` Nullable(String),
            `updated_at` Nullable(String),
            `_snapshot_date` Nullable(String),
            `_source_file` Nullable(String)
        )
        ENGINE = MergeTree
        ORDER BY tuple()
    """)

def _ensure_silver_db():
    conn = BaseHook.get_connection("clickhouse_default")
    ch = CHClient(
        host=conn.host or "clickhouse-server",
        port=conn.port or 9000,
        user=conn.login or "default",
        password=conn.password or "",
        database=(conn.schema or "default"),
        settings=(conn.extra_dejson.get("settings", {}) if isinstance(conn.extra_dejson.get("settings", {}), dict) else {}),
    )
    ch.execute("CREATE DATABASE IF NOT EXISTS silver")


def _compute_ds_lagged(ds: str, lag_days: int) -> str:
    dt = datetime.strptime(ds, "%Y-%m-%d")
    return (dt - timedelta(days=lag_days)).strftime("%Y-%m-%d")


def _resolve_dbt_exec() -> list:
    """Return argv prefix to run dbt regardless of version/install location."""
    # 1) Prefer the dbt binary if on PATH (works for both 1.5 and 1.8)
    dbt_path = shutil.which("dbt")
    if dbt_path:
        return [dbt_path]
    # 2) Try 1.8+ module path
    try:
        subprocess.run(["python", "-c", "import dbt.cli.main"], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return ["python", "-m", "dbt.cli.main"]
    except Exception:
        pass
    # 3) Try 1.5 module path
    try:
        subprocess.run(["python", "-c", "import dbt"], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return ["python", "-m", "dbt"]
    except Exception:
        pass
    raise RuntimeError("dbt CLI not found. Ensure dbt-core is installed and on PATH.")


def _run_dbt(select: str, ds: str, full_refresh: bool = False, pass_ds_lagged: bool = True):
    exec_argv = _resolve_dbt_exec()
    proj = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt_project")
    prof = os.environ.get("DBT_PROFILES_DIR", "/opt/airflow/dbt_project")


    args = exec_argv + [
        "--no-use-colors", "run",
        "--project-dir", proj,
        "--profiles-dir", prof,
        "--select", select,
    ]
    if full_refresh:
        args.append("--full-refresh")


    if pass_ds_lagged:
        ds_lagged = _compute_ds_lagged(ds, LAG_DAYS)
        args += ["--vars", json.dumps({"ds_lagged": ds_lagged})]


    proc = subprocess.run(args, text=True, capture_output=True)
    if proc.returncode != 0:
        raise RuntimeError(f"dbt failed ({proc.returncode}).\nSTDOUT:\n{proc.stdout}\n\nSTDERR:\n{proc.stderr}")


with DAG(
    dag_id="silver_transformation_pipeline",
    description="Build ClickHouse Silver models from Bronze using dbt (29-day lag incrementals).",
    start_date=timezone.datetime(2025, 10, 1),
    schedule_interval="@daily",
    catchup=True,
    tags=["dbt", "silver", "clickhouse"],
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
    },
    max_active_runs=1,
) as dag:
    ensure_bronze = PythonOperator(
        task_id="ensure_bronze_prereqs",
        python_callable=_ensure_bronze_prereqs,
    )


    dbt_run_linktx = PythonOperator(
        task_id="dbt_run_linktx",
        python_callable=_run_dbt,
        op_kwargs={
            "select": "silver_link_transactions",
            "ds": "{{ ds }}",                 # <-- pass Airflow's ds into your helper
            "full_refresh": False,
            "pass_ds_lagged": True,           # <-- your helper will add --vars {"ds_lagged": ...}
        },
    )

    dbt_run_payments = PythonOperator(
        task_id="dbt_run_payments",
        python_callable=lambda ds, **_: _run_dbt("silver_payments", ds=ds, full_refresh=False, pass_ds_lagged=True),
    )
    dbt_run_merchants = PythonOperator(
        task_id="dbt_run_merchants",
        python_callable=lambda ds, **_: _run_dbt("silver_merchants", ds=ds, full_refresh=False, pass_ds_lagged=True),
    )

    ensure_bronze >> [dbt_run_merchants, dbt_run_payments, dbt_run_linktx]



















