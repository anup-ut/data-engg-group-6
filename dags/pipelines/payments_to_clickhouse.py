# dags/pipelines/payments_to_clickhouse.py
#
# Reusable task: CSV -> ClickHouse bronze for payments (29-day lag by default)
# Idempotency: partitioned by _snapshot_date (Date) and the partition is dropped
# before insert, guaranteeing no duplicates for that day.
#
# Python 3.8 compatible (parse-safe: heavy imports are inside the callable)


from __future__ import annotations
from dataclasses import dataclass
from typing import Dict


@dataclass
class PaymentsToCHConfig:
    # ClickHouse target
    ch_conn_id: str = "clickhouse_default"
    ch_database: str = "bronze"
    ch_table: str = "payments"


    # Input CSV location and naming
    base_data_dir: str = "/tmp/data"
    subfolder: str = "payments"
    file_prefix: str = "payments"


    # Lag and task identity
    lag_days: int = 29
    task_id: str = "load_payments_bronze"


    # Metadata / partitioning
    add_metadata: bool = True
    add_snapshot_date: bool = True
    snapshot_col: str = "_snapshot_date"  # stored as ClickHouse Date
    metadata_cols: Dict[str, str] = None  # filled in __post_init__


    # Back-compat: accepts but ignored
    delete_strategy: str = "partition_drop"


    def __post_init__(self):
        if self.metadata_cols is None:
            self.metadata_cols = {
                "_ingested_at": "DateTime DEFAULT now()",
                "_source_file": "String",
            }


def build_payments_to_ch_task(dag, cfg: PaymentsToCHConfig):
    """Return a PythonOperator that ingests lagged payments CSV -> ClickHouse bronze."""
    from airflow.operators.python import PythonOperator


    def _callable(**context):
        import os
        import logging
        from datetime import date
        import pendulum
        import pandas as pd
        from airflow.hooks.base import BaseHook
        from clickhouse_driver import Client as CHClient


        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


        # --- compute lagged day (override via dagrun conf) ---
        ds = context["ds"]  # 'YYYY-MM-DD'
        dagrun = context.get("dag_run")
        dagrun_conf = dagrun.conf if dagrun else {}
        lagged_date = pendulum.parse(ds).subtract(days=cfg.lag_days).date()
        override = dagrun_conf.get("override_ds_nodash")
        if override:
            lagged_date = pendulum.from_format(override, "YYYYMMDD").date()


        lagged_nodash = lagged_date.strftime("%Y%m%d")
        snap_date_py = date(lagged_date.year, lagged_date.month, lagged_date.day)  # Python date for CH


        # --- locate CSV ---
        folder = os.path.join(cfg.base_data_dir, cfg.subfolder)
        csv_name = f"{cfg.file_prefix}-{lagged_nodash}.csv"
        csv_path = os.path.join(folder, csv_name)
        logging.info(f"[payments->ch] Target CSV: {csv_path}")


        try:
            listing = sorted(os.listdir(folder))
            logging.info(f"[payments->ch] Folder listing: {listing[:20]}{' ...' if len(listing) > 20 else ''}")
        except Exception as e:
            logging.warning(f"[payments->ch] Could not list {folder}: {e}")


        if not os.path.exists(csv_path):
            logging.warning("[payments->ch] File not found. Skipping.")
            return


        # --- read CSV: disable NA parsing so no float NaNs leak into strings ---
        df = pd.read_csv(
            csv_path,
            dtype=str,
            keep_default_na=False,   # don't convert things like 'NA' to NaN
            na_values=[],            # nothing is NA
            na_filter=False          # don't detect NA at all
        )
        # Normalize empties to NULLs
        df = df.replace("", None)


        if df.empty:
            logging.warning("[payments->ch] CSV empty. Skipping.")
            return


        # --- metadata & snapshot date ---
        if cfg.add_metadata:
            df["_source_file"] = csv_name


        if cfg.add_snapshot_date:
            # keep a string for DataFrame, but we will pass a Python date at insert time
            df[cfg.snapshot_col] = snap_date_py.isoformat()


        # --- ClickHouse: connect ---
        conn = BaseHook.get_connection(cfg.ch_conn_id)
        host = conn.host or "clickhouse-server"
        port = conn.port or 9000
        user = conn.login or "default"
        password = conn.password or ""
        database = conn.schema or "default"
        extra = conn.extra_dejson or {}
        settings = extra.get("settings", {}) if isinstance(extra.get("settings", {}), dict) else {}


        ch = CHClient(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            settings=settings,
        )


        # --- Ensure database ---
        ch.execute(f"CREATE DATABASE IF NOT EXISTS {cfg.ch_database}")


        # --- Ensure table (partitioned by _snapshot_date Date) ---
        reserved = set(cfg.metadata_cols.keys()) if cfg.add_metadata else set()
        if cfg.add_snapshot_date:
            reserved.add(cfg.snapshot_col)


        csv_cols = [c.replace("`", "") for c in df.columns if c.replace("`", "") not in reserved]


        ddl_cols = []
        for c in csv_cols:
            ddl_cols.append(f"`{c}` Nullable(String)")


        if cfg.add_metadata:
            for k, v in cfg.metadata_cols.items():
                ddl_cols.append(f"`{k}` {v}")


        if cfg.add_snapshot_date:
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


        # --- Schema drift: add any missing CSV columns as Nullable(String) ---
        existing_cols = {row[0] for row in ch.execute(f"DESCRIBE TABLE {cfg.ch_database}.{cfg.ch_table}")}
        for c in df.columns:
            safe = c.replace("`", "")
            if safe in existing_cols:
                continue
            if cfg.add_snapshot_date and safe == cfg.snapshot_col:
                continue
            if cfg.add_metadata and safe in cfg.metadata_cols:
                ch.execute(
                    f"ALTER TABLE {cfg.ch_database}.{cfg.ch_table} "
                    f"ADD COLUMN IF NOT EXISTS `{safe}` {cfg.metadata_cols[safe]}"
                )
            else:
                ch.execute(
                    f"ALTER TABLE {cfg.ch_database}.{cfg.ch_table} "
                    f"ADD COLUMN IF NOT EXISTS `{safe}` Nullable(String)"
                )


        # --- Hard idempotency: drop the day's partition before insert ---
        if cfg.add_snapshot_date:
            try:
                ch.execute(
                    f"ALTER TABLE {cfg.ch_database}.{cfg.ch_table} DROP PARTITION %(p)s",
                    {"p": snap_date_py.isoformat()},
                )
                logging.info(f"[payments->ch] Dropped partition for {cfg.snapshot_col}={snap_date_py.isoformat()}.")
            except Exception as e:
                logging.info(f"[payments->ch] DROP PARTITION (likely none yet): {e}")


        # --- Insert rows (guarantee every cell is str or None; snapshot date as date) ---
        columns = list(df.columns)
        if cfg.add_snapshot_date and cfg.snapshot_col not in columns:
            columns.append(cfg.snapshot_col)
            df[cfg.snapshot_col] = snap_date_py.isoformat()


        # Build rows; coerce every value to str or None; pass Python date for snapshot column
        snap_col = cfg.snapshot_col if cfg.add_snapshot_date else None


        def coerce_val(col_name, v):
            # ClickHouse driver needs strings for String columns and None for NULLs.
            if col_name == snap_col:
                return snap_date_py  # Python date for CH Date column
            if v is None:
                return None
            # Some CSVs might contain literal 'NaN' strings; keep as 'NaN' (string), not float
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
            logging.info(f"[payments->ch] Inserted {len(df)} rows into {cfg.ch_database}.{cfg.ch_table}.")
        else:
            logging.info("[payments->ch] Nothing to insert after transform.")


    return PythonOperator(
        task_id=cfg.task_id,
        python_callable=_callable,
        dag=dag,
    )
