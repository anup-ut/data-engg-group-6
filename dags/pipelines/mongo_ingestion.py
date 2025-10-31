# dags/pipelines/mongo_ingestion.py

from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Dict, Any

# Note: no third-party imports at module import time.

@dataclass
class LinkTxIngestConfig:
    mongo_conn_id: str = "mongo_default"
    database: str = "projectdb"
    collection: str = "link_transactions_raw"
    base_data_dir: str = "/tmp/data"
    subfolder: str = "link_transactions"
    file_prefix: str = "link_transactions"
    lag_days: int = 29
    task_id: str = "load_link_transactions_to_mongo"

def build_linktx_to_mongo_task(dag, cfg: LinkTxIngestConfig):
    """Create a PythonOperator that ingests CSV -> Mongo (29-day lag)."""
    from airflow.operators.python import PythonOperator

    def _callable(**context):
        # Lazy imports here so the DAG can be parsed without these libs
        import os
        import json
        import logging
        import pendulum
        import pandas as pd
        from datetime import datetime, timedelta
        from airflow.hooks.base import BaseHook
        from pymongo import MongoClient

        def _parse_json_or_none(x: str):
            if x is None or not isinstance(x, str):
                return None
            s = x.strip()
            if not s:
                return None
            try:
                return json.loads(s)
            except Exception:
                return None

        # --- compute target day (29-day lag, override via conf) ---
        ds = context["ds"]
        dagrun_conf = (context.get("dag_run") or {}).conf or {}
        target = pendulum.parse(ds).subtract(days=cfg.lag_days).format("YYYYMMDD")
        override = dagrun_conf.get("override_ds_nodash")
        if override:
            target = override

        # --- locate CSV ---
        csv_dir = os.path.join(cfg.base_data_dir, cfg.subfolder)
        csv_path = os.path.join(csv_dir, f"{cfg.file_prefix}-{target}.csv")

        logging.info(f"[mongo/link_tx] Target CSV: {csv_path}")
        try:
            listing = sorted(os.listdir(csv_dir))
            logging.info(f"[mongo/link_tx] Directory listing: {listing}")
        except Exception as e:
            logging.warning(f"[mongo/link_tx] Could not list {csv_dir}: {e}")

        if not os.path.exists(csv_path):
            logging.warning(f"[mongo/link_tx] File not found, skipping.")
            return

        # --- read & normalize ---
        df = pd.read_csv(csv_path)
        if df.empty:
            logging.warning("[mongo/link_tx] CSV empty, skipping.")
            return

        required_cols = ["id", "created_at", "updated_at", "state", "linkpay_reference", "payment_details"]

        # parse payment_details JSON-ish
        if "payment_details" not in df.columns:
            df["payment_details"] = None
        df = df.rename(columns={"payment_details": "_pd_tmp"})
        df["_pd_tmp"] = df["_pd_tmp"].apply(_parse_json_or_none)
        df = df.rename(columns={"_pd_tmp": "payment_details"})

        # ensure required columns exist
        for c in required_cols:
            if c not in df.columns:
                df[c] = None
        df = df[required_cols]

        # normalize timestamps: UTC-naive ISO strings
        for col in ("created_at", "updated_at"):
            if col in df.columns:
                s = pd.to_datetime(df[col], errors="coerce", utc=True)
                df[col] = s.dt.tz_localize(None)

        # Replace NaN with None
        df = df.where(df.notna(), None)

        docs = df.to_dict(orient="records")
        if not docs:
            logging.info("[mongo/link_tx] No documents after transform, skipping.")
            return

        # --- connect to Mongo & insert ---
        conn = BaseHook.get_connection(cfg.mongo_conn_id)
        # If using SRV URI in conn, BaseHook.get_uri() handles it too.
        mongo_uri = conn.get_uri()
        client = MongoClient(mongo_uri, connect=False)
        try:
            coll = client[cfg.database][cfg.collection]
            res = coll.insert_many(docs, ordered=False)
            logging.info(f"[mongo/link_tx] Inserted {len(res.inserted_ids)} docs into {cfg.collection}.")
        finally:
            client.close()
            logging.info("[mongo/link_tx] MongoDB connection closed.")

    return PythonOperator(
        task_id=cfg.task_id,
        python_callable=_callable,
        dag=dag,
    )
