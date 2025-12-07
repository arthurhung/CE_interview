
# Airflow — Event Log Pipeline DAG


這個資料夾包含：

- Airflow DAG 定義：`dags/event_log_pipeline_dag.py`
- Dockerfile 與 docker-compose，用來啟動本地的 Airflow + Postgres

DAG 串起整條流程：

```text
Ingestion → Cleaning → Feature Engineering
```



## DAG 說明

* DAG ID：`event_log_pipeline`
* 任務順序：
  1. `ingest_raw_data`
     * 呼叫 `EventLogRawSaver`
     * `data/YYYY-MM-DD/*.parquet` → `s3/raw/event_log/...`
  2. `clean_event_log`
     * 呼叫 `EventLogCleaner`
     * 產生 `valid_timestamp` / `event_key`，並去重 → `s3/clean/event_log/...`
  3. `build_features`
     * 呼叫 `EventLogFeatureEngineer`
     * 產生 `device_features.parquet` / `error_events.parquet`


## 啟動方式（本地 Docker）

在專案根目錄 `CE_interview/` 底下：

<pre class="overflow-visible!" data-start="5056" data-end="5176"><div class="contain-inline-size rounded-2xl corner-superellipse/1.1 relative bg-token-sidebar-surface-primary"><div class="sticky top-9"><div class="absolute end-0 bottom-0 flex h-9 items-center pe-2"><div class="bg-token-bg-elevated-secondary text-token-text-secondary flex items-center gap-4 rounded-sm px-2 font-sans text-xs"></div></div></div><div class="overflow-y-auto p-4" dir="ltr"><code class="whitespace-pre! language-bash"><span><span>cd</span><span> airflow

</span><span># 建 image（包含 Airflow + 專案程式碼）</span><span>
docker compose build

</span><span># 啟動 Airflow + Postgres</span><span>
docker compose up -d</span></span></code></div></div></pre>


啟動後：

* 打開瀏覽器：[http://localhost:8080]()
* 預設帳號密碼（依 compose 設定，例如）：`admin / admin`
* 在 Airflow UI 中可以看到 `event_log_pipeline` DAG

---

## 手動觸發 DAG(先手動觸發，之後有排程需求可在Dag中改)

1. 進入 Airflow Web UI
2. 找到 `event_log_pipeline`
3. 點選 **Trigger DAG**

   Airflow 會依序執行：

   * Ingestion → Cleaning → Feature

## 注意事項

* Airflow 容器會把整個專案掛載到：`/opt/airflow/app`
* DAG 內透過 `PROJECT_ROOT = Path(__file__).resolve().parents[2]` 找到專案根目錄
* 所有路徑設定集中在：`src/config/settings.py`
* ```
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
  ```
