# dags/pipelines/mongo_to_clickhouse.py
#
# MongoDB -> ClickHouse bronze for link_transactions
# - Filters by Mongo `_snapshot_date` (Date) to match CSV->Mongo loader
# - Idempotent: CH table PARTITION BY _snapshot_date (Date); DROP PARTITION before insert
# - Schema drift safe and robust string/None coercion


from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Dict


@dataclass
class LinkTxMongoToCHConfig:
    # Mongo
    mongo_conn_id: str = "mongo_default"
    database: str = "projectdb"
    collection: str = "link_transactions_raw"


    # ClickHouse
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
        "payment_details",  # dict/list will be stringified
    ])
    fetch_batch: int = 10_000


    # Windowing
    lag_days: int = 29


    # Airflow task id
    task_id: str = "load_link_transactions_bronze_from_mongo"


    # Bronze metadata & partitioning
    add_metadata: bool = True
    snapshot_col: str = "_snapshot_date"  # ClickHouse Date partition; read from Mongo field of same name
    metadata_cols: Dict[str, str] = field(default_factory=lambda: {
        "_ingested_at": "DateTime DEFAULT now()",
        "_source_file": "String",  # we'll set "mongo:<collection>:YYYY-MM-DD"
    })


def build_mongo_to_ch_task(dag, cfg: LinkTxMongoToCHConfig):
    from airflow.operators.python import PythonOperator


    def _callable(**context):
        # Lazy imports
        import json
        import logging
        from datetime import date
        import pendulum
        import pandas as pd
        from airflow.hooks.base import BaseHook
        from pymongo import MongoClient
        from clickhouse_driver import Client as CHClient
        from bson import ObjectId
        from datetime import datetime


        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


        # ---------- target day (by _snapshot_date) ----------
        ds = context["ds"]
        dagrun = context.get("dag_run")
        conf = dagrun.conf if dagrun else {}


        target_dt = pendulum.parse(ds).subtract(days=cfg.lag_days).date()
        if conf.get("override_ds_nodash"):
            target_dt = pendulum.from_format(conf["override_ds_nodash"], "YYYYMMDD").date()


        snap_date_py = date(target_dt.year, target_dt.month, target_dt.day)  # Python date
        snap_date_str = snap_date_py.isoformat()


        # Full load?
        full_load = bool(conf.get("full_load", False))


        # ---------- connections ----------
        # Mongo
        m_conn = BaseHook.get_connection(cfg.mongo_conn_id)
        mclient = MongoClient(m_conn.get_uri(), connect=False)
        mcoll = mclient[cfg.database][cfg.collection]


        # CH
        ch_conn = BaseHook.get_connection(cfg.ch_conn_id)
        ch = CHClient(
            host=ch_conn.host or "clickhouse-server",
            port=ch_conn.port or 9000,
            user=ch_conn.login or "default",
            password=ch_conn.password or "",
            database=(ch_conn.schema or "default"),
            settings=(ch_conn.extra_dejson.get("settings", {}) if isinstance(ch_conn.extra_dejson.get("settings", {}), dict) else {}),
        )


        # ---------- helpers ----------
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
            if isinstance(val, (datetime,)):
                # keep ISO; silver can cast later
                return val.isoformat()
            return str(val)


        def _ensure_db_and_table_base():
            ch.execute(f"CREATE DATABASE IF NOT EXISTS {cfg.ch_database}")
            # Base table with only metadata + snapshot date; CSV/Mongo fields added on demand
            ddl_cols = []
            if cfg.add_metadata:
                for k, v in cfg.metadata_cols.items():
                    ddl_cols.append(f"`{k}` {v}")
            ddl_cols.append(f"`{cfg.snapshot_col}` Date")
            ddl = f"""
            CREATE TABLE IF NOT EXISTS {cfg.ch_database}.{cfg.ch_table} (
                {", ".join(ddl_cols)}
            )
            ENGINE = MergeTree
            PARTITION BY `{cfg.snapshot_col}`
            ORDER BY tuple()
            """
            ch.execute(ddl)


        def _add_missing_columns(cols: List[str]):
            existing = {r[0] for r in ch.execute(f"DESCRIBE TABLE {cfg.ch_database}.{cfg.ch_table}")}
            reserved = set(cfg.metadata_cols.keys()) if cfg.add_metadata else set()
            reserved.add(cfg.snapshot_col)
            for c in cols:
                safe = c.replace("`", "")
                if safe in existing or safe in reserved:
                    continue
                ch.execute(
                    f"ALTER TABLE {cfg.ch_database}.{cfg.ch_table} "
                    f"ADD COLUMN IF NOT EXISTS `{safe}` Nullable(String)"
                )


        def _drop_partition_for_day():
            try:
                ch.execute(
                    f"ALTER TABLE {cfg.ch_database}.{cfg.ch_table} DROP PARTITION %(p)s",
                    {"p": snap_date_str},
                )
                logging.info(f"[mongo->ch] Dropped partition for {cfg.snapshot_col}={snap_date_str}.")
            except Exception as e:
                logging.info(f"[mongo->ch] DROP PARTITION (likely none yet): {e}")


        def _insert_rows(rows: List[dict], source_label: str):
            if not rows:
                return
            df = pd.DataFrame(rows)


            # Ensure all configured columns exist (order) + we’ll add snapshot later
            for c in cfg.columns:
                if c not in df.columns:
                    df[c] = None
            df = df[cfg.columns]


            # Coerce every value to str/None
            for c in df.columns:
                df[c] = df[c].map(lambda v: None if v is None else str(v))


            # Metadata + snapshot
            if cfg.add_metadata:
                df["_source_file"] = source_label
            df[cfg.snapshot_col] = snap_date_str  # string in DF; we pass Python date at emit time


            # Add any missing columns in CH (drift)
            _add_missing_columns(list(df.columns))


            # Build data and insert (coerce snapshot col to Python date)
            cols = list(df.columns)
            snap_col = cfg.snapshot_col


            def coerce_val(col_name, v):
                if col_name == snap_col:
                    return snap_date_py
                return None if v is None else str(v)


            data = [
                tuple(coerce_val(col, val) for col, val in zip(cols, row))
                for row in df.itertuples(index=False, name=None)
            ]
            col_list = ", ".join(f"`{c.replace('`','')}`" for c in cols)
            ch.execute(f"INSERT INTO {cfg.ch_database}.{cfg.ch_table} ({col_list}) VALUES", data)


        try:
            # Ensure DB/table regardless of data presence
            _ensure_db_and_table_base()


            # Build Mongo query
            if full_load:
                query = {}
                source_label = f"mongo:{cfg.collection}:ALL"
            else:
                # filter by _snapshot_date (Mongo Date) equals the target day
                start = datetime(snap_date_py.year, snap_date_py.month, snap_date_py.day)
                end = datetime(snap_date_py.year, snap_date_py.month, snap_date_py.day, 23, 59, 59, 999000)
                query = {cfg.snapshot_col: {"$gte": start, "$lte": end}}
                source_label = f"mongo:{cfg.collection}:{snap_date_str}"


            logging.info(f"[mongo->ch] Mongo query: {query}")


            # Idempotency in CH: drop the day's partition (or whole table partition) first (only for non-full-load);
            # for full_load you may want a different strategy (e.g., truncate then reload), but we keep insert-only.
            if not full_load:
                _drop_partition_for_day()


            # Stream from Mongo in batches
            total = 0
            batch = []
            for doc in mcoll.find(query, projection={k: 1 for k in cfg.columns}, batch_size=cfg.fetch_batch):
                row = {}
                for key in cfg.columns:
                    v = doc.get(key)
                    if key == "payment_details" and isinstance(v, (dict, list)):
                        row[key] = json.dumps(v, ensure_ascii=False)
                    else:
                        row[key] = _object_to_string(v)
                batch.append(row)


                if len(batch) >= cfg.fetch_batch:
                    _insert_rows(batch, source_label)
                    total += len(batch)
                    batch = []


            if batch:
                _insert_rows(batch, source_label)
                total += len(batch)


            logging.info(f"[mongo->ch] Inserted {total} rows into {cfg.ch_database}.{cfg.ch_table}.")
            if total == 0:
                logging.info("[mongo->ch] No matching docs found for given filter.")
        finally:
            try:
                mclient.close()
            except Exception:
                pass


    return PythonOperator(
        task_id=cfg.task_id,
        python_callable=_callable,
        dag=dag,
    )
