# dags/pipelines/merchants_to_clickhouse.py
from __future__ import annotations


from dataclasses import dataclass
from typing import Dict


@dataclass
class MerchantsToCHConfig:
    # ClickHouse connection (Airflow Conn)
    ch_conn_id: str = "clickhouse_default"
    ch_database: str = "bronze"
    ch_table: str = "merchants"


    # Files
    base_data_dir: str = "/tmp/data"
    subfolder: str = "merchants"
    file_prefix: str = "merchants"   # merchants-YYYYMMDD.csv


    # Scheduling / lag
    lag_days: int = 29                # daily lag snapshot
    task_id: str = "load_merchants_bronze"


    # Metadata
    add_metadata: bool = True
    snapshot_col: str = "_snapshot_date"  # ClickHouse Date
    metadata_cols: Dict[str, str] = None  # filled in __post_init__


    # Back-compat no-op (ignored if passed)
    delete_strategy: str = "partition_drop"


    def __post_init__(self):
        if self.metadata_cols is None:
            self.metadata_cols = {
                "_ingested_at": "DateTime DEFAULT now()",
                "_source_file": "String",
            }




def build_merchants_to_ch_task(dag, cfg: MerchantsToCHConfig):
    """Create a PythonOperator that ingests merchants CSV -> ClickHouse bronze (daily lag)."""
    from airflow.operators.python import PythonOperator


    def _callable(**context):
        # Lazy imports to keep DAG parsing light
        import os
        import logging
        from datetime import date
        import pendulum
        import pandas as pd
        from airflow.hooks.base import BaseHook
        from clickhouse_driver import Client as CHClient


        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


        # ---------- compute target file ----------
        ds = context["ds"]
        dagrun = context.get("dag_run")
        dagrun_conf = dagrun.conf if dagrun else {}


        lagged_date = pendulum.parse(ds).subtract(days=cfg.lag_days).date()
        override = dagrun_conf.get("override_ds_nodash")
        if override:
            lagged_date = pendulum.from_format(override, "YYYYMMDD").date()


        lagged_nodash = lagged_date.strftime("%Y%m%d")
        snap_date_py = date(lagged_date.year, lagged_date.month, lagged_date.day)


        folder = os.path.join(cfg.base_data_dir, cfg.subfolder)
        csv_name = f"{cfg.file_prefix}-{lagged_nodash}.csv"
        csv_path = os.path.join(folder, csv_name)
        logging.info(f"[merchants->ch] Target CSV: {csv_path}")


        try:
            listing = sorted(os.listdir(folder))
            logging.info(f"[merchants->ch] Folder listing: {listing[:20]}{' ...' if len(listing) > 20 else ''}")
        except Exception as e:
            logging.warning(f"[merchants->ch] Could not list {folder}: {e}")


        # ---------- ClickHouse connect ----------
        conn = BaseHook.get_connection(cfg.ch_conn_id)
        ch = CHClient(
            host=conn.host or "clickhouse-server",
            port=conn.port or 9000,
            user=conn.login or "default",
            password=conn.password or "",
            database=(conn.schema or "default"),
            settings=(conn.extra_dejson.get("settings", {}) if isinstance(conn.extra_dejson.get("settings", {}), dict) else {}),
        )


        # Ensure database
        ch.execute(f"CREATE DATABASE IF NOT EXISTS {cfg.ch_database}")


        # Ensure base table exists even if today we have no file
        # Base DDL: only metadata + snapshot date; CSV columns are added later on-demand.
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


        # If no file for this day: we ensured the table and exit gracefully
        if not os.path.exists(csv_path):
            logging.warning("[merchants->ch] File not found. Table ensured. Skipping insert.")
            return


        # ---------- read CSV (no NA inference; str/None only) ----------
        df = pd.read_csv(
            csv_path,
            dtype=str,
            keep_default_na=False,
            na_values=[],
            na_filter=False
        ).replace("", None)


        if df.empty:
            logging.warning("[merchants->ch] CSV empty. Skipping.")
            return


        # Enforce metadata/snapshot for lineage and partition
        if cfg.add_metadata:
            df["_source_file"] = csv_name
        df[cfg.snapshot_col] = snap_date_py.isoformat()  # string in DF; we pass a date at insert


        # ---------- add any missing CSV columns to table (schema drift safe) ----------
        existing_cols = {row[0] for row in ch.execute(f"DESCRIBE TABLE {cfg.ch_database}.{cfg.ch_table}")}
        reserved = set(cfg.metadata_cols.keys()) if cfg.add_metadata else set()
        reserved.add(cfg.snapshot_col)


        for c in df.columns:
            safe = c.replace("`", "")
            if safe in existing_cols:
                continue
            if cfg.add_metadata and safe in cfg.metadata_cols:
                ch.execute(
                    f"ALTER TABLE {cfg.ch_database}.{cfg.ch_table} "
                    f"ADD COLUMN IF NOT EXISTS `{safe}` {cfg.metadata_cols[safe]}"
                )
            elif safe == cfg.snapshot_col:
                # already defined as Date in base DDL
                continue
            else:
                ch.execute(
                    f"ALTER TABLE {cfg.ch_database}.{cfg.ch_table} "
                    f"ADD COLUMN IF NOT EXISTS `{safe}` Nullable(String)"
                )


        # ---------- hard idempotency: drop the day’s partition ----------
        try:
            ch.execute(
                f"ALTER TABLE {cfg.ch_database}.{cfg.ch_table} DROP PARTITION %(p)s",
                {"p": snap_date_py.isoformat()},
            )
            logging.info(f"[merchants->ch] Dropped partition for {cfg.snapshot_col}={snap_date_py.isoformat()}.")
        except Exception as e:
            logging.info(f"[merchants->ch] DROP PARTITION (likely none yet): {e}")


        # ---------- insert ----------
        columns = list(df.columns)
        if cfg.snapshot_col not in columns:
            columns.append(cfg.snapshot_col)
            df[cfg.snapshot_col] = snap_date_py.isoformat()


        snap_col = cfg.snapshot_col


        def coerce_val(col_name, v):
            if col_name == snap_col:
                return snap_date_py  # Python date for CH Date column
            if v is None:
                return None
            return str(v)


        data = [
            tuple(coerce_val(col, val) for col, val in zip(columns, row))
            for row in df[columns].itertuples(index=False, name=None)
        ]


        if data:
            col_list = ", ".join(f"`{c.replace('`','')}`" for c in columns)
            ch.execute(
                f"INSERT INTO {cfg.ch_database}.{cfg.ch_table} ({col_list}) VALUES",
                data,
            )
            logging.info(
                f"[merchants->ch] Inserted {len(df)} rows into {cfg.ch_database}.{cfg.ch_table} for snapshot {snap_date_py}."
            )
        else:
            logging.info("[merchants->ch] Nothing to insert after transform.")


    return PythonOperator(
        task_id=cfg.task_id,
        python_callable=_callable,
        dag=dag,
    )
