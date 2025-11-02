# dags/pipelines/mongo_ingestion.py


from __future__ import annotations
from dataclasses import dataclass


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
    """CSV -> Mongo (29-day lag). Idempotent per _source_file (delete then insert)."""
    from airflow.operators.python import PythonOperator


    def _callable(**context):
        # Lazy imports so DAG parse stays light
        import os, json, logging, pendulum
        import pandas as pd
        from datetime import datetime, timezone
        from airflow.hooks.base import BaseHook
        from pymongo import MongoClient


        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


        # ---------- target logical day ----------
        ds = context["ds"]  # 'YYYY-MM-DD'
        dagrun = context.get("dag_run")
        conf = dagrun.conf if dagrun else {}


        lagged_dt = pendulum.parse(ds).subtract(days=cfg.lag_days).date()
        if conf.get("override_ds_nodash"):
            lagged_dt = pendulum.from_format(conf["override_ds_nodash"], "YYYYMMDD").date()


        lagged_nodash = lagged_dt.strftime("%Y%m%d")
        snap_date_iso = lagged_dt.isoformat()           # 'YYYY-MM-DD'
        snap_date_mongo = datetime(lagged_dt.year, lagged_dt.month, lagged_dt.day, tzinfo=timezone.utc)


        # ---------- locate CSV ----------
        folder = os.path.join(cfg.base_data_dir, cfg.subfolder)
        csv_name = f"{cfg.file_prefix}-{lagged_nodash}.csv"
        csv_path = os.path.join(folder, csv_name)


        logging.info(f"[mongo/link_tx] Target CSV: {csv_path}")
        try:
            listing = sorted(os.listdir(folder))
            logging.info(f"[mongo/link_tx] Directory listing: {listing[:20]}{' ...' if len(listing) > 20 else ''}")
        except Exception as e:
            logging.warning(f"[mongo/link_tx] Could not list {folder}: {e}")


        # ---------- connect Mongo & ensure indexes (even if file missing) ----------
        conn = BaseHook.get_connection(cfg.mongo_conn_id)
        client = MongoClient(conn.get_uri(), connect=False)
        try:
            coll = client[cfg.database][cfg.collection]
            # Helpful indexes for fast idempotent delete / time filtering
            coll.create_index([("_source_file", 1)], name="by_source_file", background=True)
            coll.create_index([("_snapshot_date", 1)], name="by_snapshot_date", background=True)
            # Optional (commented): prevent dupes within the same file if you ever upsert
            # coll.create_index([("_source_file", 1), ("id", 1)], name="uniq_file_id", unique=True, background=True)


            if not os.path.exists(csv_path):
                logging.warning("[mongo/link_tx] File not found; indexes ensured; skipping.")
                return


            # ---------- read CSV robustly (strings only; no NaN floats) ----------
            df = pd.read_csv(
                csv_path,
                dtype=str,
                keep_default_na=False,
                na_values=[],
                na_filter=False
            ).replace("", None)


            if df.empty:
                logging.warning("[mongo/link_tx] CSV empty; skipping.")
                return


            # ---------- normalize schema ----------
            required_cols = ["id", "created_at", "updated_at", "state", "linkpay_reference", "payment_details"]
            for c in required_cols:
                if c not in df.columns:
                    df[c] = None


            # parse payment_details JSON
            def _parse_json_or_none(x):
                if x is None or not isinstance(x, str):
                    return None
                s = x.strip()
                if not s:
                    return None
                try:
                    return json.loads(s)
                except Exception:
                    return None


            df["payment_details"] = df["payment_details"].apply(_parse_json_or_none)


            # normalize timestamps to UTC-naive ISO strings (or keep None)
            for col in ("created_at", "updated_at"):
                if col in df.columns:
                    ts = pd.to_datetime(df[col], errors="coerce", utc=True)
                    df[col] = ts.dt.strftime("%Y-%m-%dT%H:%M:%S").where(ts.notna(), None)


            # lineage & snapshot metadata
            df["_source_file"] = csv_name
            df["_snapshot_date"] = snap_date_mongo  # Mongo Date type (UTC midnight)


            # ---------- idempotency: delete-by-source-file, then insert ----------
            del_res = coll.delete_many({"_source_file": csv_name})
            logging.info(f"[mongo/link_tx] Deleted {del_res.deleted_count} docs for _source_file={csv_name} (idempotency).")


            docs = df.to_dict(orient="records")
            if not docs:
                logging.info("[mongo/link_tx] No documents after transform; nothing to insert.")
                return


            ins_res = coll.insert_many(docs, ordered=False)
            logging.info(f"[mongo/link_tx] Inserted {len(ins_res.inserted_ids)} docs into {cfg.collection} for {snap_date_iso}.")


        finally:
            try:
                client.close()
            except Exception:
                pass
            logging.info("[mongo/link_tx] MongoDB connection closed.")


    return PythonOperator(
        task_id=cfg.task_id,
        python_callable=_callable,
        dag=dag,
    )


