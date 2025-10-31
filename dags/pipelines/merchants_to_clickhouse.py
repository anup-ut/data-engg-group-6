# dags/pipelines/merchants_to_clickhouse.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


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

    # Metadata behavior
    add_metadata: bool = True         # always recommended for bronze
    # Note: we always add/overwrite _source_file & _snapshot_date in this task


def build_merchants_to_ch_task(dag, cfg: MerchantsToCHConfig):
    """Create a PythonOperator that ingests merchants CSV -> ClickHouse bronze (daily lag)."""
    from airflow.operators.python import PythonOperator

    def _callable(**context):
        # Lazy imports to keep DAG parsing light
        import os
        import logging
        import pendulum
        import pandas as pd
        from airflow.hooks.base import BaseHook
        from clickhouse_driver import Client as CHClient

        # ------------------ helpers (scoped) ------------------
        def _get_ch_client() -> CHClient:
            conn = BaseHook.get_connection(cfg.ch_conn_id)
            host = conn.host or "clickhouse-server"
            port = conn.port or 9000
            user = conn.login or "default"
            password = conn.password or ""
            database = conn.schema or "default"
            extra = conn.extra_dejson or {}
            settings = extra.get("settings", {}) if isinstance(extra.get("settings", {}), dict) else {}
            return CHClient(host=host, port=port, user=user, password=password, database=database, settings=settings)

        def _ensure_database(client: CHClient, db_name: str):
            client.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")

        def _ensure_table_from_df(client: CHClient, db: str, table: str, df: pd.DataFrame):
            # all columns as Nullable(String) + metadata columns if requested
            cols = []
            for col in df.columns:
                safe = col.replace("`", "")
                cols.append(f"`{safe}` Nullable(String)")
            ddl = f"""
            CREATE TABLE IF NOT EXISTS {db}.{table} (
              {", ".join(cols)}
            )
            ENGINE = MergeTree
            ORDER BY tuple()
            """
            client.execute(ddl)

        def _read_csv_as_strings(path: str) -> pd.DataFrame:
            # bronze stays raw: all string, empty -> None
            df = pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[])
            df = df.where(df.applymap(lambda x: x != ""), None)
            return df

        def _insert_df(client: CHClient, db: str, table: str, df: pd.DataFrame, src_name: str):
            # ensure metadata columns visible in table
            if "_source_file" not in df.columns:
                df["_source_file"] = src_name
            else:
                df["_source_file"] = src_name
            cols = list(df.columns)
            if not len(df):
                return
            data = [tuple(row.get(c) for c in cols) for _, row in df.iterrows()]
            col_list = ", ".join(f"`{c.replace('`','')}`" for c in cols)
            client.execute(f"INSERT INTO {db}.{table} ({col_list}) VALUES", data)

        # ------------------ compute target file ------------------
        ds = context["ds"]
        dagrun_conf = (context.get("dag_run") or {}).conf or {}
        lagged = pendulum.parse(ds).subtract(days=cfg.lag_days).format("YYYYMMDD")
        override = dagrun_conf.get("override_ds_nodash")
        if override:
            lagged = override

        folder = os.path.join(cfg.base_data_dir, cfg.subfolder)
        csv_name = f"{cfg.file_prefix}-{lagged}.csv"
        csv_path = os.path.join(folder, csv_name)
        logging.info(f"[merchants->ch] Target CSV: {csv_path}")

        try:
            listing = sorted(os.listdir(folder))
            logging.info(f"[merchants->ch] Folder listing: {listing}")
        except Exception as e:
            logging.warning(f"[merchants->ch] Could not list {folder}: {e}")

        if not os.path.exists(csv_path):
            logging.warning("[merchants->ch] File not found. Skipping.")
            return

        df = _read_csv_as_strings(csv_path)
        if df.empty:
            logging.warning("[merchants->ch] CSV empty. Skipping.")
            return

        # Derive snapshot date (as YYYY-MM-DD string) and enforce metadata
        snap_date = f"{lagged[0:4]}-{lagged[4:6]}-{lagged[6:8]}"
        df["_source_file"] = csv_name  # force for idempotency
        df["_snapshot_date"] = snap_date

        ch = _get_ch_client()
        _ensure_database(ch, cfg.ch_database)
        _ensure_table_from_df(ch, cfg.ch_database, cfg.ch_table, df)

        # Idempotency for re-runs: delete any prior rows for this file
        try:
            ch.execute(
                f"ALTER TABLE {cfg.ch_database}.{cfg.ch_table} DELETE WHERE `_source_file` = %(src)s",
                {"src": csv_name},
            )
        except Exception as e:
            logging.warning(f"[merchants->ch] DELETE by _source_file failed (non-fatal): {e}")

        _insert_df(ch, cfg.ch_database, cfg.ch_table, df, csv_name)
        logging.info(
            f"[merchants->ch] Inserted {len(df)} rows into {cfg.ch_database}.{cfg.ch_table} for snapshot {snap_date}."
        )

    return PythonOperator(
        task_id=cfg.task_id,
        python_callable=_callable,
        dag=dag,
    )

