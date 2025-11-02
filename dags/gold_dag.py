# dags/gold_dag.py
from __future__ import annotations


import os
import shutil
import subprocess
from datetime import timedelta


from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils import timezone




# --- Paths inside the Airflow container (mounted via docker-compose) ---
DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt_project")
DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR", "/opt/airflow/dbt_project")




def _resolve_dbt_exec() -> list[str]:
    """
    Return argv prefix to run dbt, regardless of installation layout.
    Tries:
      1) 'dbt' binary on PATH
      2) dbt 1.8+ module path
      3) dbt 1.5 module path
    """
    dbt_path = shutil.which("dbt")
    if dbt_path:
        return [dbt_path]


    # Try 1.8+ module path
    try:
        subprocess.run(
            ["python", "-c", "import dbt.cli.main"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            text=True,
        )
        return ["python", "-m", "dbt.cli.main"]
    except Exception:
        pass


    # Fallback to 1.5 module path
    return ["python", "-m", "dbt"]




def _run_dbt(select_expr: str, full_refresh: bool = False) -> None:
    """
    Run `dbt run` with the given selection expression.
    Raises RuntimeError on non-zero exit so Airflow marks task as failed.
    """
    args = _resolve_dbt_exec() + [
        "--no-use-colors",
        "run",
        "--project-dir", DBT_PROJECT_DIR,
        "--profiles-dir", DBT_PROFILES_DIR,
        "--select", select_expr,
    ]
    if full_refresh:
        args.append("--full-refresh")


    proc = subprocess.run(args, text=True, capture_output=True)


    # Always print stdout/stderr to task logs for easier debugging
    print("==== DBT STDOUT ====")
    print(proc.stdout)
    print("==== DBT STDERR ====")
    print(proc.stderr)


    if proc.returncode != 0:
        raise RuntimeError(
            f"dbt failed ({proc.returncode}).\nSTDOUT:\n{proc.stdout}\n\nSTDERR:\n{proc.stderr}"
        )




with DAG(
    dag_id="gold_dimensions_pipeline",
    description="Build ClickHouse GOLD dimension models with dbt.",
    start_date=timezone.datetime(2025, 10, 1),
    schedule_interval="@daily",
    catchup=True,
    tags=["dbt", "gold", "clickhouse"],
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    max_active_runs=1,
) as dag:


    # One task per dimension (explicit model names = SQL file names without .sql)
    dbt_dim_date = PythonOperator(
        task_id="dbt_dim_date",
        python_callable=lambda **_: _run_dbt("dim_date"),
    )


    dbt_dim_payment_method = PythonOperator(
        task_id="dbt_dim_payment_method",
        python_callable=lambda **_: _run_dbt("dim_payment_method"),
    )


    dbt_dim_payment_state = PythonOperator(
        task_id="dbt_dim_payment_state",
        python_callable=lambda **_: _run_dbt("dim_payment_state"),
    )


    dbt_dim_merchants = PythonOperator(
        task_id="dbt_dim_merchants",
        python_callable=lambda **_: _run_dbt("dim_merchants"),
    )


    # Optional: run dim_date first, then the rest in parallel (good for FK availability)
    dbt_dim_date >> [dbt_dim_payment_method, dbt_dim_payment_state, dbt_dim_merchants]
