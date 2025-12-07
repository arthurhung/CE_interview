# airflow/dags/event_log_pipeline_dag.py

from datetime import datetime, timedelta
from pathlib import Path
import sys
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email  # 需在 Airflow 中設定好 SMTP

# ------------------------------------------------------------
# Logger 設定
# ------------------------------------------------------------
logger = logging.getLogger("event_log_pipeline")
logger.setLevel(logging.INFO)


# ------------------------------------------------------------
# 專案路徑設定：讓 Airflow 找到 src/*
# ------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]  # CE_interview/
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config.settings import DATA_ROOT, RAW_ROOT, CLEAN_ROOT, PARQUET_ENGINE, VALID_MODELS, FEATURE_ROOT
from src.ingestion.ingest import EventLogRawIngestor
from src.cleaning.clean import EventLogCleaner
from src.feature.feature import EventLogFeatureEngineer


# ------------------------------------------------------------
# 失敗通知 callback（Email / log）
# ------------------------------------------------------------
def notify_failure(context):
    """
    DAG / Task 任一失敗時觸發：
    - 寫 log
    - 用 Airflow send_email 發通知（如果有 SMTP）
    """
    dag_id = context["dag"].dag_id
    task_instance = context["task_instance"]
    task_id = task_instance.task_id
    execution_date = context.get("execution_date")
    log_url = task_instance.log_url

    msg = f"[Airflow][FAIL] DAG={dag_id}, Task={task_id}, " f"Execution={execution_date}, Log={log_url}"
    logger.error(msg)

    # 如果沒設定 SMTP，這段就算失敗也不影響主流程
    try:
        send_email(
            to=["arthur@example.com"],
            subject=f"[Airflow] DAG {dag_id} Task {task_id} Failed",
            html_content=f"""
            <h3>Airflow Task Failed</h3>
            <ul>
              <li><b>DAG</b>: {dag_id}</li>
              <li><b>Task</b>: {task_id}</li>
              <li><b>Execution date</b>: {execution_date}</li>
              <li><b>Log</b>: <a href="{log_url}">Log Link</a></li>
            </ul>
            """,
        )
    except Exception as e:
        logger.warning(f"send_email failed: {e}")


# ------------------------------------------------------------
# Airflow DAG 設定（含 retry / backoff / alert）
# ------------------------------------------------------------
default_args = {
    "owner": "arthur",
    "depends_on_past": False,
    "retries": 2,  # 每個 task 失敗時最多 retry 2 次
    "retry_delay": timedelta(minutes=3),  # 第一次重試延遲
    "retry_exponential_backoff": True,  # 指數退避：3m → 6m → ...
    "max_retry_delay": timedelta(minutes=30),
    "on_failure_callback": notify_failure,  # 任一 task 失敗就觸發通知
    # 如需開 email_on_failure（搭配 Airflow 全域 email 設定）
    # "email": ["you@example.com"],
    # "email_on_failure": True,
}

with DAG(
    dag_id="event_log_pipeline",
    default_args=default_args,
    description="Ingest → Clean → Feature pipeline for laptop event logs",
    schedule_interval=None,  # 先手動觸發，之後要排程再改，如 '0 2 * * *'
    start_date=datetime(2024, 5, 1),
    catchup=False,
    tags=["event_log", "data_pipeline"],
) as dag:

    # ---------------- Ingest Task ---------------- #
    def ingest_raw():
        """
        Step 1: 從 data/20xx-xx-xx/*.parquet
                轉存到 fake S3 raw zone: s3/raw/event_log/week=.../pn=...
        """
        logger.info(f"[INGEST] start | DATA_ROOT={DATA_ROOT}, RAW_ROOT={RAW_ROOT}, " f"VALID_MODELS={VALID_MODELS}")
        saver = EventLogRawIngestor(
            src_root=DATA_ROOT,
            dst_root=RAW_ROOT,
            valid_models=VALID_MODELS,
        )
        saver.run()
        logger.info("[INGEST] done")

    ingest_task = PythonOperator(
        task_id="ingest_raw_data",
        python_callable=ingest_raw,
        # 可以在這裡覆蓋 default_args 的 retry 設定（如果需要）
        # retries=3,
    )

    # ---------------- Clean Task ---------------- #
    def clean_event_log():
        """
        Step 2: 從 raw zone 讀 parquet
                清洗 ev / valid_timestamp / event_key
                去重後寫入 clean zone: s3/clean/event_log/...
        """
        logger.info(f"[CLEAN] start | RAW_ROOT={RAW_ROOT}, CLEAN_ROOT={CLEAN_ROOT}, " f"ENGINE={PARQUET_ENGINE}")
        cleaner = EventLogCleaner(
            raw_root=RAW_ROOT,
            clean_root=CLEAN_ROOT,
            parquet_engine=PARQUET_ENGINE,
        )
        cleaner.run()
        logger.info("[CLEAN] done")

    clean_task = PythonOperator(
        task_id="clean_event_log",
        python_callable=clean_event_log,
    )

    # ---------------- Feature Task ---------------- #
    def build_features():
        """
        Step 3: 從 clean zone 讀資料
                建 device-level 特徵、error 時序資料
                寫入 feature zone: s3/feature/event_log/...
        """
        logger.info(
            f"[FEATURE] start | CLEAN_ROOT={CLEAN_ROOT}, FEATURE_ROOT={FEATURE_ROOT}, " f"ENGINE={PARQUET_ENGINE}"
        )
        engineer = EventLogFeatureEngineer(
            clean_root=CLEAN_ROOT,
            feature_root=FEATURE_ROOT,
            parquet_engine=PARQUET_ENGINE,
        )
        engineer.run()
        logger.info("[FEATURE] done")

    feature_task = PythonOperator(
        task_id="build_features",
        python_callable=build_features,
    )

    # 設定 DAG 依賴關係：Ingest → Clean → Feature
    ingest_task >> clean_task >> feature_task
