# dags/pipelines/payments_to_clickhouse.py
#
# Reusable task: CSV -> ClickHouse bronze for payments (29-day lag by default)
# Idempotency:
#   - delete_strategy = "source_file" | "snapshot_date" | "both"
#   - adds _source_file and _snapshot_date (if enabled) and deletes matching rows before insert
#
# Python 3.8 compatible


from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Any


# No heavy imports at module import time (Airflow parse-safe)


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


    # Metadata behavior
    add_metadata: bool = True
    add_snapshot_date: bool = True
    snapshot_col: str = "_snapshot_date"
    metadata_cols: Dict[str, str] = None  # filled in __post_init__


    # Idempotency strategy: "source_file" | "snapshot_date" | "both"
    delete_strategy: str = "source_file"


    def __post_init__(self):
        if self.metadata_cols is None:
            self.metadata_cols = {
                "_ingested_at": "DateTime DEFAULT now()",
                "_source_file": "String",
            }
        # normalize delete strategy
        if self.delete_strategy not in ("source_file", "snapshot_date", "both"):
            self.delete_strategy = "source_file"




def build_payments_to_ch_task(dag, cfg: PaymentsToCHConfig):
    """Return a PythonOperator that ingests lagged payments CSV -> ClickHouse bronze."""
    from airflow.operators.python import PythonOperator


    def _callable(**context):
        # Lazy imports so the DAG parses even if deps aren't installed in the scheduler
        import os
        import logging
        import pendulum
        import pandas as pd
        from airflow.hooks.base import BaseHook
        from clickhouse_driver import Client as CHClient


        # --- compute lagged day (override via dagrun conf) ---
        ds = context["ds"]
        dagrun_conf = (context.get("dag_run") or {}).conf or {}
        lagged = pendulum.parse(ds).subtract(days=cfg.lag_days).format("YYYYMMDD")
        override = dagrun_conf.get("override_ds_nodash")
        if override:
            lagged = override


        # --- locate CSV ---
        folder = os.path.join(cfg.base_data_dir, cfg.subfolder)
        csv_name = f"{cfg.file_prefix}-{lagged}.csv"
        csv_path = os.path.join(folder, csv_name)
        logging.info(f"[payments->ch] Target CSV: {csv_path}")


        try:
            listing = sorted(os.listdir(folder))
            logging.info(f"[payments->ch] Folder listing: {listing}")
        except Exception as e:
            logging.warning(f"[payments->ch] Could not list {folder}: {e}")


        if not os.path.exists(csv_path):
            logging.warning("[payments->ch] File not found. Skipping.")
            return


        # --- read CSV as raw strings; empty -> None ---
        df = pd.read_csv(csv_path, dtype=str, keep_default_na=False, na_values=[])
        # replace empty strings with None
        df = df.where(df.applymap(lambda x: x != ""), None)


        if df.empty:
            logging.warning("[payments->ch] CSV empty. Skipping.")
            return


        # --- metadata & snapshot date ---
        if cfg.add_metadata:
            if "_source_file" not in df.columns:
                df["_source_file"] = csv_name
            else:
                # force consistent value for idempotency
                df["_source_file"] = csv_name


        snap_date = f"{lagged[0:4]}-{lagged[4:6]}-{lagged[6:8]}"
        if cfg.add_snapshot_date:
            df[cfg.snapshot_col] = snap_date


        # --- ClickHouse: connect, ensure DB/table, idempotent upsert-by-file/window ---
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


        # Ensure database
        ch.execute(f"CREATE DATABASE IF NOT EXISTS {cfg.ch_database}")


        # Ensure table: all columns as Nullable(String) + metadata DDL types
        cols = []
        for col in df.columns:
            safe = col.replace("`", "")
            if safe in cfg.metadata_cols:
                cols.append(f"`{safe}` {cfg.metadata_cols[safe]}")
            elif cfg.add_snapshot_date and safe == cfg.snapshot_col:
                cols.append(f"`{safe}` Nullable(String)")
            else:
                cols.append(f"`{safe}` Nullable(String)")


        ddl = f"""
        CREATE TABLE IF NOT EXISTS {cfg.ch_database}.{cfg.ch_table} (
          {", ".join(cols)}
        )
        ENGINE = MergeTree
        ORDER BY tuple()
        """
        ch.execute(ddl)


        # Idempotency deletes
        # 1) By _source_file
        if cfg.delete_strategy in ("source_file", "both") and cfg.add_metadata:
            try:
                ch.execute(
                    f"ALTER TABLE {cfg.ch_database}.{cfg.ch_table} DELETE WHERE `_source_file` = %(src)s",
                    {"src": csv_name},
                )
                logging.info(f"[payments->ch] Deleted existing rows for _source_file={csv_name}.")
            except Exception as e:
                logging.warning(f"[payments->ch] DELETE by _source_file failed (non-fatal): {e}")


        # 2) By snapshot date
        if cfg.delete_strategy in ("snapshot_date", "both") and cfg.add_snapshot_date:
            try:
                ch.execute(
                    f"ALTER TABLE {cfg.ch_database}.{cfg.ch_table} DELETE WHERE `{cfg.snapshot_col}` = %(d)s",
                    {"d": snap_date},
                )
                logging.info(f"[payments->ch] Deleted existing rows for {cfg.snapshot_col}={snap_date}.")
            except Exception as e:
                logging.warning(f"[payments->ch] DELETE by {cfg.snapshot_col} failed (non-fatal): {e}")


        # Insert
        columns = list(df.columns)
        data = [tuple(row.get(c) for c in columns) for _, row in df.iterrows()]
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
