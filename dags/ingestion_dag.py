# dags/ingestion_dag.py
#
# Single DAG orchestrating all ingestion steps.
# - CSV -> Mongo for link_transactions via pipelines.mongo_ingestion
# - Mongo -> ClickHouse bronze via pipelines.mongo_to_clickhouse
# - CSV -> ClickHouse bronze for merchants via pipelines.merchants_to_clickhouse
# - CSV -> ClickHouse bronze for payments via pipelines.payments_to_clickhouse
#
# Python 3.8 compatible (uses typing.List, avoids list[str])


import logging
from typing import List


from airflow import DAG
from airflow.operators.python import PythonOperator  # (not used directly now, but fine to keep)
from airflow.utils import timezone


# Reusable tasks
from pipelines.mongo_ingestion import (
    LinkTxIngestConfig,
    build_linktx_to_mongo_task,
)
from pipelines.mongo_to_clickhouse import (
    LinkTxMongoToCHConfig,
    build_mongo_to_ch_task,
)
from pipelines.merchants_to_clickhouse import (
    MerchantsToCHConfig,
    build_merchants_to_ch_task,
)
from pipelines.payments_to_clickhouse import (
    PaymentsToCHConfig,
    build_payments_to_ch_task,
)


# -----------------------------
# Global Config
# -----------------------------
BASE_DATA_DIR = "/tmp/data"
LAG_DAYS = 0


# Mongo
MONGO_CONN_ID = "mongo_default"
MONGO_DATABASE = "projectdb"
MONGO_COLLECTION_RAW = "link_transactions_raw"


# ClickHouse Bronze
CH_CONN_ID = "clickhouse_default"
BRONZE_DB = "bronze"


# Bronze: folders & prefixes
MERCHANTS_SUBFOLDER = "merchants"   # daily lagged snapshot file: merchants-YYYYMMDD.csv
MERCHANTS_TABLE = "merchants"


PAYMENTS_SUBFOLDER = "payments"     # 29-day lag
PAYMENTS_TABLE = "payments"


# link_transactions shape (used by Mongo -> CH)
LINK_TX_COLUMNS: List[str] = [
    "id",
    "created_at",
    "updated_at",
    "state",
    "linkpay_reference",
    "payment_details",  # JSON-stringified for CH bronze
]


# Mongo -> CH streaming
MONGO_FETCH_BATCH = 10_000


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# -----------------------------
# DAG definition
# -----------------------------
with DAG(
    dag_id="ingestion_pipeline",
    description=(
        "Merchants(daily lag) & payments(29d lag) -> ClickHouse bronze; "
        "link_transactions(29d lag) CSV -> Mongo; Mongo -> CH bronze."
    ),
    start_date=timezone.datetime(2025, 10, 1),
    schedule_interval="@daily",
    catchup=True,
    tags=["bronze", "clickhouse", "mongodb", "ingestion"],
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 1,
        "email_on_failure": False,
        "email_on_retry": False,
    },
) as dag:


    # Merchants (CSV -> CH bronze) — reusable module
    cfg_merchants = MerchantsToCHConfig(
        ch_conn_id=CH_CONN_ID,
        ch_database=BRONZE_DB,
        ch_table=MERCHANTS_TABLE,
        base_data_dir=BASE_DATA_DIR,
        subfolder=MERCHANTS_SUBFOLDER,
        file_prefix="merchants",
        lag_days=LAG_DAYS,
        task_id="load_merchants_bronze",
    )
    t_merchants_bronze = build_merchants_to_ch_task(dag=dag, cfg=cfg_merchants)


    # Payments (CSV -> CH bronze) — reusable module
    cfg_payments = PaymentsToCHConfig(
        ch_conn_id=CH_CONN_ID,
        ch_database=BRONZE_DB,
        ch_table="payments",
        base_data_dir=BASE_DATA_DIR,
        subfolder="payments",
        file_prefix="payments",
        lag_days=LAG_DAYS,
        task_id="load_payments_bronze",
    )

    t_payments_bronze = build_payments_to_ch_task(dag=dag, cfg=cfg_payments)


    # Link Transactions (CSV -> Mongo) — reusable module
    cfg_linktx_csv_to_mongo = LinkTxIngestConfig(
        mongo_conn_id=MONGO_CONN_ID,
        database=MONGO_DATABASE,
        collection=MONGO_COLLECTION_RAW,
        base_data_dir=BASE_DATA_DIR,
        subfolder="link_transactions",
        file_prefix="link_transactions",
        lag_days=LAG_DAYS,
        task_id="load_link_transactions_to_mongo",
    )
    t_linktx_mongo = build_linktx_to_mongo_task(dag=dag, cfg=cfg_linktx_csv_to_mongo)


    # Link Transactions (Mongo -> CH bronze) — reusable module
    cfg_mongo_to_ch = LinkTxMongoToCHConfig(
        mongo_conn_id=MONGO_CONN_ID,
        database=MONGO_DATABASE,
        collection=MONGO_COLLECTION_RAW,
        ch_conn_id=CH_CONN_ID,
        ch_database=BRONZE_DB,
        ch_table="link_transactions",
        columns=LINK_TX_COLUMNS,
        fetch_batch=MONGO_FETCH_BATCH,
        lag_days=LAG_DAYS,
        task_id="load_link_transactions_bronze_from_mongo",
        add_metadata=True,
    )
    t_linktx_mongo_to_ch = build_mongo_to_ch_task(dag=dag, cfg=cfg_mongo_to_ch)


    # Merchants & payments run independently; Mongo CSV -> Mongo -> CH must be ordered
    [t_merchants_bronze, t_payments_bronze, (t_linktx_mongo >> t_linktx_mongo_to_ch)]
