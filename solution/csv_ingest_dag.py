import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.exceptions import AirflowFailException
import pandas as pd
import sqlalchemy as sa
from pathlib import Path
import json
import logging
import os

# ----------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------
# Resolve the data directory to the repository's sample_data by default, so the DAG
# runs both locally and inside Airflow. Can be overridden with CSV_DATA_DIR env var.
_default_sample_dir_candidates = [
    # If the DAG is placed under /opt/airflow/dags/solution/csv_ingest_dag.py,
    # the sample_data is typically a sibling of the solution folder.
    (Path(__file__).resolve().parents[1] / "sample_data"),
    # Common fallback if sample_data is placed directly under dags
    (Path(__file__).resolve().parents[2] / "sample_data") if len(Path(__file__).resolve().parents) > 2 else None,
    # Docker-compose style default mount
    Path("/opt/airflow/dags/sample_data"),
    # Legacy/default location
    Path("/opt/airflow/data"),
]

_env_dir = os.getenv("CSV_DATA_DIR")
if _env_dir:
    DATA_DIR = Path(_env_dir)
else:
    DATA_DIR = next((p for p in _default_sample_dir_candidates if p and p.exists()), _default_sample_dir_candidates[-1])

DB_URL = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"

# Expected columns
MERCHANTS_SCHEMA = ["id", "acquirer_id", "name", "state", "created_at", "updated_at"]
LINK_TX_SCHEMA = ["id", "created_at", "updated_at", "state", "linkpay_reference", "payment_details"]
PAYMENTS_SCHEMA = ["id", "created_at", "updated_at", "merchant_id", "acquirer_id", "reference", "order_reference", "state", "card_type", "details"]

# ----------------------------------------------------------------------
# Helper: Quality Checks
# ----------------------------------------------------------------------
def validate_and_clean(df, schema, table_name):
    # 1. Schema check
    missing_cols = set(schema) - set(df.columns)
    if missing_cols:
        raise AirflowFailException(f"Missing columns in {table_name}: {missing_cols}")

    # 2. Null check on critical fields
    critical = [col for col in ["id"] if col in df.columns]
    if df[critical].isnull().any().any():
        raise AirflowFailException(f"Null values in critical columns of {table_name}")

    # 3. Duplicate ID check
    if "id" in df.columns and df["id"].duplicated().any():
        dupes = df[df["id"].duplicated()]["id"].tolist()
        raise AirflowFailException(f"Duplicate IDs in {table_name}: {dupes[:5]}...")

    # 4. Clean empty rows (common in your payments.csv)
    df = df.dropna(how="all")

    # 5. Parse JSON fields safely
    if "payment_details" in df.columns:
        def safe_json(x):
            if pd.isna(x): return {}
            try:
                return json.loads(x) if isinstance(x, str) else x
            except:
                logging.warning(f"Invalid JSON in payment_details: {x}")
                return {}
        df["payment_details"] = df["payment_details"].apply(safe_json)

    if "details" in df.columns:
        def safe_json(x):
            if pd.isna(x): return {}
            try:
                return json.loads(x) if isinstance(x, str) else x
            except:
                logging.warning(f"Invalid JSON in details: {x}")
                return {}
        df["details"] = df["details"].apply(safe_json)

    return df

# ----------------------------------------------------------------------
# Task: Ingest Merchants (Static-ish, daily incremental)
# ----------------------------------------------------------------------
def ingest_merchants(**context):
    ds_nodash = context["ds_nodash"]
    file_pattern = f"merchants-{ds_nodash}.csv"
    file_path = DATA_DIR / file_pattern

    if not file_path.exists():
        logging.info(f"No merchants file for {ds_nodash}, skipping.")
        return

    df = pd.read_csv(file_path)
    df = validate_and_clean(df, MERCHANTS_SCHEMA, "merchants")

    # Parse timestamps
    for col in ["created_at", "updated_at"]:
        df[col] = pd.to_datetime(df[col], utc=True)

    engine = sa.create_engine(DB_URL)

    # Create table
    with engine.begin() as conn:
        conn.execute(sa.text("""
            CREATE TABLE IF NOT EXISTS dim_merchant (
                id INTEGER PRIMARY KEY,
                acquirer_id INTEGER,
                name TEXT,
                state TEXT,
                created_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ
            );
        """))

    # UPSERT
    tmp_table = "dim_merchant_tmp"
    df.to_sql(tmp_table, engine, if_exists="replace", index=False)

    with engine.begin() as conn:
        conn.execute(sa.text(f"""
            INSERT INTO dim_merchant
            SELECT * FROM {tmp_table}
            ON CONFLICT (id) DO UPDATE SET
                acquirer_id = EXCLUDED.acquirer_id,
                name = EXCLUDED.name,
                state = EXCLUDED.state,
                created_at = EXCLUDED.created_at,
                updated_at = EXCLUDED.updated_at;
            DROP TABLE {tmp_table};
        """))

    logging.info(f"Ingested {len(df)} merchants from {file_path.name}")

# ----------------------------------------------------------------------
# Task: Ingest Link Transactions
# ----------------------------------------------------------------------
def ingest_link_transactions(**context):
    ds_nodash = context["ds_nodash"]
    file_pattern = f"link_transactions-{ds_nodash}.csv"
    file_path = DATA_DIR / file_pattern

    if not file_path.exists():
        raise AirflowFailException(f"Link transactions file not found: {file_path}")

    df = pd.read_csv(file_path)
    df = validate_and_clean(df, LINK_TX_SCHEMA, "link_transactions")

    # Parse timestamps
    for col in ["created_at", "updated_at"]:
        df[col] = pd.to_datetime(df[col], utc=True)

    engine = sa.create_engine(DB_URL)

    with engine.begin() as conn:
        conn.execute(sa.text("""
            CREATE TABLE IF NOT EXISTS fact_link_transaction (
                id BIGINT PRIMARY KEY,
                created_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ,
                state TEXT,
                linkpay_reference TEXT,
                payment_details JSONB
            );
        """))

    tmp_table = "fact_link_transaction_tmp"
    df.to_sql(tmp_table, engine, if_exists="replace", index=False)

    with engine.begin() as conn:
        conn.execute(sa.text(f"""
            INSERT INTO fact_link_transaction
            SELECT id, created_at, updated_at, state, linkpay_reference, payment_details::jsonb
            FROM {tmp_table}
            ON CONFLICT (id) DO UPDATE SET
                created_at = EXCLUDED.created_at,
                updated_at = EXCLUDED.updated_at,
                state = EXCLUDED.state,
                linkpay_reference = EXCLUDED.linkpay_reference,
                payment_details = EXCLUDED.payment_details;
            DROP TABLE {tmp_table};
        """))

    logging.info(f"Ingested {len(df)} link transactions")

# ----------------------------------------------------------------------
# Task: Ingest Payments
# ----------------------------------------------------------------------
def ingest_payments(**context):
    ds_nodash = context["ds_nodash"]
    file_pattern = f"payments-{ds_nodash}.csv"
    file_path = DATA_DIR / file_pattern

    if not file_path.exists():
        raise AirflowFailException(f"Payments file not found: {file_path}")

    df = pd.read_csv(file_path)
    df = validate_and_clean(df, PAYMENTS_SCHEMA, "payments")

    # Parse timestamps
    for col in ["created_at", "updated_at"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")

    engine = sa.create_engine(DB_URL)

    with engine.begin() as conn:
        conn.execute(sa.text("""
            CREATE TABLE IF NOT EXISTS fact_payment (
                id BIGINT PRIMARY KEY,
                created_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ,
                merchant_id INTEGER,
                acquirer_id INTEGER,
                reference TEXT,
                order_reference TEXT,
                state TEXT,
                card_type TEXT,
                details JSONB
            );
        """))

    tmp_table = "fact_payment_tmp"
    df.to_sql(tmp_table, engine, if_exists="replace", index=False)

    with engine.begin() as conn:
        conn.execute(sa.text(f"""
            INSERT INTO fact_payment
            SELECT id, created_at, updated_at, merchant_id, acquirer_id,
                   reference, order_reference, state, card_type, details::jsonb
            FROM {tmp_table}
            ON CONFLICT (id) DO UPDATE SET
                created_at = EXCLUDED.created_at,
                updated_at = EXCLUDED.updated_at,
                merchant_id = EXCLUDED.merchant_id,
                acquirer_id = EXCLUDED.acquirer_id,
                reference = EXCLUDED.reference,
                order_reference = EXCLUDED.order_reference,
                state = EXCLUDED.state,
                card_type = EXCLUDED.card_type,
                details = EXCLUDED.details;
            DROP TABLE {tmp_table};
        """))

    logging.info(f"Ingested {len(df)} payments")

# ----------------------------------------------------------------------
# DAG Definition
# ----------------------------------------------------------------------
with DAG(
    dag_id="payment_platform_ingest",
    # schedule="@daily",
    schedule_interval="*/1 * * * *",  # every minute
    catchup=False,
    start_date=pendulum.datetime(2025, 10, 1, tz="UTC"),
    # catchup=True,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=5),
    },
    tags=["payments", "merchants", "link_tx", "csv"],
) as dag:

    # --- Sensors ---
    wait_merchants = FileSensor(
        task_id="wait_for_merchants",
        filepath=str(DATA_DIR / "merchants-{{ ds_nodash }}.csv"),
        fs_conn_id=None,
        poke_interval=60,
        timeout=600,
        mode="poke",
    )

    wait_link_tx = FileSensor(
        task_id="wait_for_link_transactions",
        filepath=str(DATA_DIR / "link_transactions-{{ ds_nodash }}.csv"),
        poke_interval=60,
        timeout=600,
        mode="poke",
    )

    wait_payments = FileSensor(
        task_id="wait_for_payments",
        filepath=str(DATA_DIR / "payments-{{ ds_nodash }}.csv"),
        poke_interval=60,
        timeout=600,
        mode="poke",
    )

    # --- Tasks ---
    load_merchants = PythonOperator(
        task_id="load_merchants",
        python_callable=ingest_merchants,
    )

    load_link_tx = PythonOperator(
        task_id="load_link_transactions",
        python_callable=ingest_link_transactions,
    )

    load_payments = PythonOperator(
        task_id="load_payments",
        python_callable=ingest_payments,
    )

    # --- Dependencies ---
    wait_merchants >> load_merchants
    wait_link_tx >> load_link_tx
    wait_payments >> load_payments

    # Optional: all must complete
    [load_merchants, load_link_tx, load_payments]