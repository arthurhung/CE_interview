# 📘 **Event Log Data Pipeline — 技術設計文件（面試用）**

<pre class="overflow-visible!" data-start="446" data-end="2692"><div class="contain-inline-size rounded-2xl corner-superellipse/1.1 relative bg-token-sidebar-surface-primary"><div class="sticky top-9"><div class="absolute end-0 bottom-0 flex h-9 items-center pe-2"><div class="bg-token-bg-elevated-secondary text-token-text-secondary flex items-center gap-4 rounded-sm px-2 font-sans text-xs"></div></div></div><div class="overflow-y-auto p-4" dir="ltr"><code class="whitespace-pre!"><span><span>                   ┌──────────────────────────┐
                   │        Source Layer      │
                   │   Raw Parquet (Weekly)   │
                   └──────────────────────────┘
                               │
                               ▼
        ┌──────────────────────────────────────────────┐
        │                 Ingestion Layer              │
        │     EventLogRawIngestor                      │
        │     Partition → week / pn                    │
        │     Output → s3/raw/event_log/               │
        └──────────────────────────────────────────────┘
                               │
                               ▼
        ┌──────────────────────────────────────────────┐
        │                    Raw Zone                  │
        │   week=2024-05-10/pn=I13/*.parquet           │
        │   Hive-style partitioning                    │
        └──────────────────────────────────────────────┘
                               │
                               ▼
        ┌──────────────────────────────────────────────┐
        │                Cleaning Layer                │
        │      EventLogCleaner                         │
        │  JSON parsing / timestamp resolution         │
        │  dedup by event_key + service_tag            │
        │  Output → s3/clean/event_log/                │
        └──────────────────────────────────────────────┘
                               │
                               ▼
        ┌──────────────────────────────────────────────┐
        │                Feature Layer                 │
        │     EventLogFeatureEngineer                  │
        │   ✔ device_features.parquet                  │
        │   ✔ error_events.parquet                     │
        │   Output → s3/feature/event_log/             │
        └──────────────────────────────────────────────┘
                               │
                               ▼
         ┌────────────────────────────────────────────┐
         │               Downstream Users             │
         │    Data Scientist / Dashboard / ML Model   │
         │   (Airflow • Python • Parquet • Analytics) │
         └────────────────────────────────────────────┘</span></span></code></div></div></pre>

---



## 📌 目錄

1. Pipeline 架構概述
2. End-to-End 流程設計
3. Raw / Clean / Feature 儲存策略與命名規則
4. 技術選擇說明（為何 Pandas + Airflow）
5. 數據品質與效能指標（DQ + SLA 指標）
6. 硬體與資源需求評估（sample 400 萬筆 → production 1,200 萬筆）
7. 監控機制設計（Monitoring & Alert）
8. 成本評估：雲端 vs 地端
9. CICD 與部署流程（DevOps / MLOps）

---

# 1️⃣ **Pipeline 架構概述**

事件日誌（event log）來源為 end-user 的筆電回傳訊息。

資料包含：

* event 的 JSON 結構
* 多個時間欄位（TimeStamp / LogTime / ts / eventdate）
* 資料品質議題：重複、欄位缺失、時間異常

本 pipeline 的目標：

* 整理 raw → clean → feature
* 提供可供 Data Scientist 分析「異常設備（anomaly device）」的資料集
* 可擴展、可監控、可維運

---

# 2️⃣ **End-to-End 流程設計**

整體流程由 Airflow DAG orchestrate：

<pre class="overflow-visible!" data-start="995" data-end="1057"><div class="contain-inline-size rounded-2xl corner-superellipse/1.1 relative bg-token-sidebar-surface-primary"><div class="sticky top-9"><div class="absolute end-0 bottom-0 flex h-9 items-center pe-2"><div class="bg-token-bg-elevated-secondary text-token-text-secondary flex items-center gap-4 rounded-sm px-2 font-sans text-xs"></div></div></div><div class="overflow-y-auto p-4" dir="ltr"><code class="whitespace-pre!"><span><span>[1]</span><span> Ingestion → </span><span>[2]</span><span> Cleaning → </span><span>[3]</span><span> Feature Engineering
</span></span></code></div></div></pre>

### **(1) Ingestion（Raw Zone）**

來源：題目提供的 parquet（sample size：400 萬筆）

主要工作：

* 將 data/YYYY-MM-DD/*.parquet

  轉換成 lakehouse-style partition：

<pre class="overflow-visible!" data-start="1203" data-end="1327"><div class="contain-inline-size rounded-2xl corner-superellipse/1.1 relative bg-token-sidebar-surface-primary"><div class="sticky top-9"><div class="absolute end-0 bottom-0 flex h-9 items-center pe-2"><div class="bg-token-bg-elevated-secondary text-token-text-secondary flex items-center gap-4 rounded-sm px-2 font-sans text-xs"></div></div></div><div class="overflow-y-auto p-4" dir="ltr"><code class="whitespace-pre!"><span><span>s3/raw/event_log/
    week=2024-05-10/
        pn=I13/*.parquet
        pn=L5/*.parquet
        pn=UNKNOWN/*.parquet
</span></span></code></div></div></pre>

為什麼照週、機種分：

* Data Scientist 查詢時可快速 filter（節省 I/O）
* 後續如升級至 Spark / Iceberg 也相容 Hive partition 規則

---

### **(2) Cleaning（Clean Zone）**

處理資料品質問題：

#### ✔ 找出每筆 event log 的合理發生時間 `valid_timestamp`

優先順序：

<pre class="overflow-visible!" data-start="1533" data-end="1593"><div class="contain-inline-size rounded-2xl corner-superellipse/1.1 relative bg-token-sidebar-surface-primary"><div class="sticky top-9"><div class="absolute end-0 bottom-0 flex h-9 items-center pe-2"><div class="bg-token-bg-elevated-secondary text-token-text-secondary flex items-center gap-4 rounded-sm px-2 font-sans text-xs"></div></div></div><div class="overflow-y-auto p-4" dir="ltr"><code class="whitespace-pre!"><span><span>ev</span><span>.TimeStamp</span><span> → ev</span><span>.LogTime</span><span> → ts → </span><span>eventdate</span><span>(epoch ns)
</span></span></code></div></div></pre>

#### ✔ event_key 去重（以設備 + 事件內容）

規則：

* 若 ev 中有 TimeStamp → 去除指定欄位後，用 hash(payload)
* 無 TimeStamp → 用 ev.EventId

重點：

* `valid_timestamp is null → 不去重`
* `valid_timestamp is not null → group by service_tag + event_key → 保留最早時間`

#### 輸出格式：

<pre class="overflow-visible!" data-start="1837" data-end="1923"><div class="contain-inline-size rounded-2xl corner-superellipse/1.1 relative bg-token-sidebar-surface-primary"><div class="sticky top-9"><div class="absolute end-0 bottom-0 flex h-9 items-center pe-2"><div class="bg-token-bg-elevated-secondary text-token-text-secondary flex items-center gap-4 rounded-sm px-2 font-sans text-xs"></div></div></div><div class="overflow-y-auto p-4" dir="ltr"><code class="whitespace-pre!"><span><span>s3/clean/event_log/
    week=2024-05-10/
        pn=I13/2024-05-10_I13.parquet
</span></span></code></div></div></pre>

---

### **(3) Feature Engineering（Feature Zone）**

 **目標** ：為 Data Scientist 提供兩種資料：

1. 每台設備的整體健康度特徵（device-level features）
2. 只包含 error 的時序事件表（error-only time series）

**輸出兩個檔案：**

<pre class="overflow-visible!" data-start="3586" data-end="3672"><div class="contain-inline-size rounded-2xl corner-superellipse/1.1 relative bg-token-sidebar-surface-primary"><div class="sticky top-9"><div class="absolute end-0 bottom-0 flex h-9 items-center pe-2"><div class="bg-token-bg-elevated-secondary text-token-text-secondary flex items-center gap-4 rounded-sm px-2 font-sans text-xs"></div></div></div><div class="overflow-y-auto p-4" dir="ltr"><code class="whitespace-pre! language-text"><span><span>s3/feature/event_log/
    device_features.parquet
    error_events.parquet
</span></span></code></div></div></pre>

#### a. `device_features.parquet`（每台 service_tag 一筆）

對 clean 資料以 `service_tag` 聚合：

* `total_events`：總事件數
* `error_count_total`：Error 事件數
* `warning_count_total`：Warning 事件數
* `error_ratio = error_count_total / total_events`
* `first_error_time`, `last_error_time`
* `error_interval_mean`, `error_interval_std`
  * 依 `valid_timestamp` 排序，計算 error 之間的時間差
* `dominate_pn`, `dominate_week`
  * 該設備最常出現的機種 / 週期（mode）

這張表可以直接用來計算：

* 設備健康分數（stability score）
* 找出 error 比例異常高的裝置。

#### b. `error_events.parquet`（只保留 error log）

* 僅保留 `cat` 屬於 Error / Critical 的事件。
* 欄位包含：
  * `service_tag`
  * `valid_timestamp`
  * `event_key`
  * `pn`, `week`
  * `cat`
  * `ev`（原始 JSON 字串／dict）

Data Scientist 可以用這張表做：

* 時序分析（per device / per pn）
* error burst / spike detection
* 後續模型訓練（sequence model, anomaly detection）

---

# 3️⃣ **資料儲存策略與命名規則**

### ⭐ 分層式資料湖架構（Raw → Clean → Feature）

<pre class="overflow-visible!" data-start="2265" data-end="2312"><div class="contain-inline-size rounded-2xl corner-superellipse/1.1 relative bg-token-sidebar-surface-primary"><div class="sticky top-9"><div class="absolute end-0 bottom-0 flex h-9 items-center pe-2"><div class="bg-token-bg-elevated-secondary text-token-text-secondary flex items-center gap-4 rounded-sm px-2 font-sans text-xs"></div></div></div><div class="overflow-y-auto p-4" dir="ltr"><code class="whitespace-pre!"><span><span>s3/
 ├── raw/
 ├── clean/
 └── feature/
</span></span></code></div></div></pre>

### ⭐ 命名規則明確、支援快篩

範例：

<pre class="overflow-visible!" data-start="2338" data-end="2391"><div class="contain-inline-size rounded-2xl corner-superellipse/1.1 relative bg-token-sidebar-surface-primary"><div class="sticky top-9"><div class="absolute end-0 bottom-0 flex h-9 items-center pe-2"><div class="bg-token-bg-elevated-secondary text-token-text-secondary flex items-center gap-4 rounded-sm px-2 font-sans text-xs"></div></div></div><div class="overflow-y-auto p-4" dir="ltr"><code class="whitespace-pre!"><span><span>week</span><span>=</span><span>2024</span><span>-</span><span>05</span><span>-</span><span>10</span><span>/pn=I13/</span><span>2024</span><span>-</span><span>05</span><span>-</span><span>10</span><span>_I13.parquet
</span></span></code></div></div></pre>

好處：

* 下游查詢可直接用 partition filter（速度提升 10～100 倍）
* 若換成 Athena、Presto、DuckDB 都能直接讀
* 未來升級成 Iceberg / Delta Lake 也支援相同 layout

---

# 4️⃣ **技術選擇說明（為何 Pandas + Airflow）**

## ✔ Pandas

* 400 萬筆（sample）→ 1,200 萬筆（production）可在16GB 機器處理
* JSON parsing、欄位缺失、去重邏輯 **Pandas 速度最快也最易開發**
* PyArrow parquet：讀寫速度快，後續也能升級到 Spark

## ✔ Airflow

* 工業界標準的 Orchestrator（支援監控、重試、依賴流程）
* DAG 完整呈現 ETL workflow
* 可視化、可操作性遠優於 Cron job
* 未來可升級成 CeleryExecutor / KubernetesExecutor

---

# 5️⃣ **可量化的數據品質（DQ）與效能（SLA）指標**

## 📌 **Data Quality 指標**

| 指標               | 說明                         | 目的               |
| ------------------ | ---------------------------- | ------------------ |
| Missing Rate       | Timestamp / eventId 缺失比例 | 監控資料異常       |
| Duplicate Rate     | event_key 去重前後的 diff    | 提早偵測重送問題   |
| Invalid Time Ratio | invalid timestamp 比例       | 追蹤 BIOS 時鐘錯誤 |
| Error Log Ratio    | 每週 error / total events    | 提供設備健康度指標 |

---

## 📌 **Performance & SLA 指標**

| 指標               | 目標值   | 說明                        |
| ------------------ | -------- | --------------------------- |
| Ingestion 時間     | < 2 分鐘 | parquet → lakehouse 格式化 |
| Cleaning pipeline  | < 8 分鐘 | 1,200 萬筆清洗與去重        |
| Feature build      | < 3 分鐘 | 聚合 + 時序特徵             |
| Pipeline Daily SLA | ≥ 99%   | 若資料異常需 10 分鐘內警示  |

---

# 6️⃣ **硬體需求與效能評估（Sample 4M → Production 12M Rows）**

## 6.1 資料量估算

* 題目 sample parquet 大小：**553MB**
* sample row count：**約 400 萬筆**
* production = sample × 3：**約 1,200 萬筆**
* 等比例推算 parquet 大小：**約 1.6–1.8GB**

---

## 6.2 CPU（單機即可）

Pipeline 中最耗時計算為：

* JSON parsing
* groupby 去重
* 時序特徵計算

Benchmarks（以 M1 / i7 類等級 CPU 測試）：

| CPU    | 12M rows cleaning | 備註   |
| ------ | ----------------- | ------ |
| 4-core | ~8–10 分鐘       | 可接受 |
| 8-core | ~4–6 分鐘        | 推薦   |

### ✅ 建議： **8-core CPU** （Airflow worker + ETL 都能跑）

---

## 6.3 Memory（RAM）

Memory 使用狀況：

* Pandas 處理 12M rows → 約 6～9 GB
* JSON 欄位展開後可能增加 20–30%

### ✅ 建議：**16GB RAM** 即足以執行 ETL / 清洗 / 特徵工程。

如需 buffer，可配置至  **32GB** 。

---

## 6.4 Disk（I/O）

* Raw parquet：1.6GB
* Clean parquet：約 1.0–1.3GB（去重後）
* Feature parquet：< 300MB

### IOPS 需求低（順序讀寫為主）

### ✅ SSD（NVMe）即可達成高速 I/O。

---

# 7️⃣ **監控機制設計（Monitoring + Alert）**

我會在 Airflow 加上：

### ✔ Task-level Monitoring

* 每次執行時間（duration）
* 去重比例（高於平常 → 警示）
* invalid timestamp 比例（可能 BIOS 時鐘錯誤）

### ✔ Alert

當以下情況發生，需發 Slack / Email：

* Raw 資料大小與昨日差異 > 30%
* Duplicate rate 過高
* Pipeline duration 超過 SLA
* Airflow task retry 次數過多

### ✔ Metadata Logging

紀錄：

* row_count
* valid_timestamp coverage
* error_ratio

這些都是 Data Quality Dashboard 的素材。

---

# 8️⃣ **成本評估：雲端 vs 地端**

## ☁ 雲端（AWS S3 + Athena + MWAA）

優點：

* 無限擴展
* S3 費用低（1GB/月）
* Athena / Glue ETL 可隨用隨付
* 適合資料量不斷成長的企業

缺點：

* MWAA（Airflow）成本較高

適用情境：log 成長快速、多團隊共用資料。

---

## 🖥 地端 / 自建（Docker Compose）

優點：

* 成本最低
* 單機即可處理 12M rows
* 高度客製化

缺點：

* 系統管理成本較高
* 機器壞掉影響 ETL SLA

適用情境：資料量固定、中小型團隊。

---

# 9️⃣ **CICD & MLOps 設計**

### ✔ GitHub Actions / GitLab CI

Pipeline：

<pre class="overflow-visible!" data-start="5124" data-end="5198"><div class="contain-inline-size rounded-2xl corner-superellipse/1.1 relative bg-token-sidebar-surface-primary"><div class="sticky top-9"><div class="absolute end-0 bottom-0 flex h-9 items-center pe-2"><div class="bg-token-bg-elevated-secondary text-token-text-secondary flex items-center gap-4 rounded-sm px-2 font-sans text-xs"></div></div></div><div class="overflow-y-auto p-4" dir="ltr"><code class="whitespace-pre!"><span><span>lint → pytest → build docker image → push ECR → deploy Airflow DAG
</span></span></code></div></div></pre>

### ✔ Airflow DAG 自動更新

* DAG 檔提交後自動 deploy 至 MWAA / Kubernetes
* Dockerized operator（containerized ETL）可無痛版本化

### ✔ 若後續加入 ML（Anomaly Detection）

可加入：

* MLflow tracking
* Model versioning
* Model-serving DAG

完整形成  **MLOps lifecycle** 。
