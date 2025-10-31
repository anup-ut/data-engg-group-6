# dags/pipelines/mongo_to_clickhouse.py
#
# Reusable task factory: MongoDB -> ClickHouse (bronze) for link_transactions.
# - Lazy third-party imports so Airflow can parse the DAG without those libs installed.
# - Python 3.8 compatible typing (no list[str]).


from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional




@dataclass
class LinkTxMongoToCHConfig:
    # Mongo side
    mongo_conn_id: str = "mongo_default"
    database: str = "projectdb"
    collection: str = "link_transactions_raw"


    # ClickHouse side
    ch_conn_id: str = "clickhouse_default"
    ch_database: str = "bronze"
    ch_table: str = "link_transactions"


    # Data shape & batching
    columns: List[str] = field(default_factory=lambda: [
        "id",
        "created_at",
        "updated_at",
        "state",
        "linkpay_reference",
        "payment_details",  # JSON/dict will be stringified
    ])
    fetch_batch: int = 10_000


    # Windowing (same semantics as your CSV→Mongo job)
    lag_days: int = 29


    # Airflow task id
    task_id: str = "load_link_transactions_bronze_from_mongo"


    # Bronze table metadata
    add_metadata: bool = True
    metadata_cols: dict = field(default_factory=lambda: {
        "_ingested_at": "DateTime DEFAULT now()",
        "_source_file": "String",  # we'll put "mongo:<collection>"
    })




def build_mongo_to_ch_task(dag, cfg: LinkTxMongoToCHConfig):
    """Create a PythonOperator that streams Mongo -> ClickHouse bronze."""
    from airflow.operators.python import PythonOperator


    def _callable(**context):
        # Lazy imports (Airflow parser-friendly)
        import json
        import logging
        from datetime import datetime, timedelta


        import pendulum
        import pandas as pd
        from airflow.hooks.base import BaseHook
        from bson import ObjectId
        from pymongo import MongoClient
        from clickhouse_driver import Client as CHClient


        # ---------- helpers (local) ----------
        def _object_to_string(val):
            if val is None:
                return None
            if isinstance(val, ObjectId):
                return str(val)
            if isinstance(val, (dict, list)):
                try:
                    return json.dumps(val, ensure_ascii=False)
                except Exception:
                    return str(val)
            if isinstance(val, datetime):
                # keep ISO; silver will cast as needed
                return val.isoformat()
            return str(val)


        def _ensure_database(ch: CHClient, db: str):
            ch.execute(f"CREATE DATABASE IF NOT EXISTS {db}")


        def _ensure_table_from_df(ch: CHClient, db: str, table: str, df: pd.DataFrame):
            cols = []
            for c in df.columns:
                safe = c.replace("`", "")
                cols.append(f"`{safe}` Nullable(String)")
            if cfg.add_metadata:
                for k, v in cfg.metadata_cols.items():
                    cols.append(f"`{k}` {v}")
            ddl = f"""
            CREATE TABLE IF NOT EXISTS {db}.{table} (
              {", ".join(cols)}
            )
            ENGINE = MergeTree
            ORDER BY tuple()
            """
            ch.execute(ddl)


        def _insert_df(ch: CHClient, db: str, table: str, df: pd.DataFrame, source_label: str):
            # add metadata columns if needed
            if cfg.add_metadata and "_source_file" in cfg.metadata_cols:
                if "_source_file" not in df.columns:
                    df["_source_file"] = source_label
                else:
                    df["_source_file"] = source_label


            cols = list(df.columns)
            if not len(df):
                return
            data = [tuple(row.get(c) for c in cols) for _, row in df.iterrows()]
            col_list = ", ".join(f"`{c.replace('`','')}`" for c in cols)
            ch.execute(f"INSERT INTO {db}.{table} ({col_list}) VALUES", data)


        def _ensure_columns(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
            for c in cols:
                if c not in df.columns:
                    df[c] = None
            return df[cols]


        # ---------- window selection (29-day lag by default) ----------
        ds = context["ds"]
        dagrun_conf = (context.get("dag_run") or {}).conf or {}
        full_load = bool(dagrun_conf.get("full_load", False))


        if full_load:
            created_at_filter = None
            window_label = "ALL"
        else:
            target_day = pendulum.parse(ds).subtract(days=cfg.lag_days)
            override = dagrun_conf.get("override_ds_nodash")
            if override:
                target_day = pendulum.from_format(override, "YYYYMMDD")
            start_dt = datetime(target_day.year, target_day.month, target_day.day, 0, 0, 0)
            end_dt = start_dt + timedelta(days=1)
            created_at_filter = {"created_at": {"$gte": start_dt, "$lt": end_dt}}
            window_label = f"{start_dt}..{end_dt}"


        logging.info(f"[mongo->ch] Query window: {window_label}")


        # ---------- connections ----------
        # Mongo
        m_conn = BaseHook.get_connection(cfg.mongo_conn_id)
        m_uri = m_conn.get_uri()
        mclient = MongoClient(m_uri, connect=False)


        # ClickHouse
        ch_conn = BaseHook.get_connection(cfg.ch_conn_id)
        ch_host = ch_conn.host or "clickhouse-server"
        ch_port = ch_conn.port or 9000
        ch_user = ch_conn.login or "default"
        ch_pw = ch_conn.password or ""
        ch_db_default = ch_conn.schema or "default"
        # NOTE: we still create/use cfg.ch_database for bronze
        ch = CHClient(host=ch_host, port=ch_port, user=ch_user, password=ch_pw, database=ch_db_default)


        try:
            # query mongo
            coll = mclient[cfg.database][cfg.collection]
            projection = {k: 1 for k in cfg.columns}
            cursor = coll.find(created_at_filter or {}, projection=projection, batch_size=cfg.fetch_batch)


            # ensure bronze db and table
            _ensure_database(ch, cfg.ch_database)


            total = 0
            batch = []
            for doc in cursor:
                row = {}
                for key in cfg.columns:
                    val = doc.get(key)
                    if key == "payment_details" and isinstance(val, (dict, list)):
                        # stringify; keep structure for silver parsing later
                        row[key] = json.dumps(val, ensure_ascii=False)
                    else:
                        row[key] = _object_to_string(val)
                batch.append(row)


                if len(batch) >= cfg.fetch_batch:
                    df = pd.DataFrame(batch)
                    df = _ensure_columns(df, cfg.columns)
                    _ensure_table_from_df(ch, cfg.ch_database, cfg.ch_table, df)
                    _insert_df(ch, cfg.ch_database, cfg.ch_table, df, f"mongo:{cfg.collection}")
                    total += len(df)
                    batch = []


            if batch:
                df = pd.DataFrame(batch)
                df = _ensure_columns(df, cfg.columns)
                _ensure_table_from_df(ch, cfg.ch_database, cfg.ch_table, df)
                _insert_df(ch, cfg.ch_database, cfg.ch_table, df, f"mongo:{cfg.collection}")
                total += len(df)


            logging.info(f"[mongo->ch] Inserted {total} rows into {cfg.ch_database}.{cfg.ch_table}.")
            if total == 0:
                logging.info("[mongo->ch] No matching docs found for given filter.")
        finally:
            mclient.close()


    return PythonOperator(
        task_id=cfg.task_id,
        python_callable=_callable,
        dag=dag,
    )
