import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils import timezone

# -----------------------------
# Configuration
# -----------------------------
DBT_PROJECT_DIR = "/dbt"  # inside the 'dbt' container
CH_CONTAINER = "clickhouse-server"
DBT_CONTAINER = "dbt"
LAG_DAYS = 29
DBT_PROFILES_DIR = "/dbt"
# Jinja expression -> 29-day-lag date (YYYY-MM-DD)
DS_LAGGED = "{{ (execution_date - macros.timedelta(days=" + str(LAG_DAYS) + ")).strftime('%Y-%m-%d') }}"

with DAG(
    dag_id="silver_transformation_pipeline",
    description="Build ClickHouse Silver models from Bronze using dbt (29-day lag incrementals).",
    start_date=timezone.datetime(2025, 10, 30),
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
) as dag:

    # 1) Ensure target database exists
    create_silver_db = BashOperator(
        task_id="create_silver_db",
        bash_command=(
            f'docker exec -i {CH_CONTAINER} '
            'clickhouse-client -q "CREATE DATABASE IF NOT EXISTS silver;"'
        ),
    )

    # 2) dbt deps (install packages)
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=(
            f"docker exec -i {DBT_CONTAINER} bash -lc '"
            f"cd {DBT_PROJECT_DIR} && "
            "dbt --no-use-colors deps --profiles-dir /dbt"
            "'"
        ),
    )

    # 3) Build merchants (full-refresh table)
    dbt_run_merchants = BashOperator(
        task_id="dbt_run_merchants",
        bash_command=(
            f"docker exec -i {DBT_CONTAINER} bash -lc '"
            f"cd {DBT_PROJECT_DIR} && "
            "dbt --no-use-colors run "
            "--profiles-dir /dbt "
            "--select silver_merchants "
            "--full-refresh"
            "'"
        ),
    )

    # 4) Build incremental models (payments + link_transactions) with ds_lagged
    # Build JSON once via Airflow Jinja and pass as env
    t_dbt_run_incrementals = BashOperator(
        task_id="dbt_run_incrementals",
        bash_command=r"""
    docker exec -i dbt bash -lc '
    set -e
    cd /dbt

    # Compute lagged date from Airflow-rendered {{ ds }} without heredocs
    DBT_LAGGED=$(python -c "from datetime import datetime, timedelta; \
    import sys; print((datetime.strptime(\"{{ ds }}\", \"%Y-%m-%d\") - timedelta(days=29)).strftime(\"%Y-%m-%d\"))")

    DBT_VARS="{\"ds_lagged\":\"${DBT_LAGGED}\"}"
    echo "dbt vars => ${DBT_VARS}"

    dbt --no-use-colors run \
        --profiles-dir /dbt \
        --select silver_payments silver_link_transactions \
        --vars "${DBT_VARS}"
    '
    """,
    )



    # Order: make DB -> deps -> merchants -> incrementals
    create_silver_db >> dbt_deps >> dbt_run_merchants >> t_dbt_run_incrementals