# CE_interview


# Event Log Data Pipeline

收集與分析 end user 筆電的 event log（開機、BIOS 更新、當機…），透過 Data Pipeline + Feature Engineering，協助 Data Scientist 進行異常偵測與設備健康度分析。

---

## 1. 專案目標與資料說明

**專案目的**

* 從 OEM 端收集每台筆電的 event log
* 透過資料清洗、去重與特徵工程，輸出「可直接被 Data Scientist 使用」的分析資料
* 支援後續做：
  * 異常筆電偵測（Anomaly Detection）
  * 設備穩定性評估
  * Error pattern / burst 分析

**資料特性**

* 來源：OEM Portal / Agent 上報
* 期間：兩週 sample data（`weeks = ['2024-05-10', '2024-05-17']`）
* 機種：`L5`, `I13`, `DG5` 三種，外加未知機種
* 主 key：`service_tag`（每台筆電唯一 ID）
* 主要欄位：
  * `eventdate`, `ts`：時間欄位（其中一個為 epoch 型態）
  * `ev`：內含 JSON 的字串，包含 `TimeStamp`, `LogTime`, `EventId`, `Cat` (Error/Warning/Info...) 等
* 資料品質問題：
  * 不同週會重送重複資料
  * 時間欄位不一致：`TimeStamp`, `LogTime`, `ts`, `eventdate` 可能缺漏或格式不一

---

## 2. 架構與 End-to-End 流程設計

整體分成四層：**Raw → Clean → Feature → Analysis**

<pre class="overflow-visible!" data-start="884" data-end="1857"><div class="contain-inline-size rounded-2xl corner-superellipse/1.1 relative bg-token-sidebar-surface-primary"><div class="sticky top-9"><div class="absolute end-0 bottom-0 flex h-9 items-center pe-2"><div class="bg-token-bg-elevated-secondary text-token-text-secondary flex items-center gap-4 rounded-sm px-2 font-sans text-xs"></div></div></div><div class="overflow-y-auto p-4" dir="ltr"><code class="whitespace-pre! language-text"><span><span>        +----------------+
        |   OEM Portal   |
        +--------+-------+
                 |
                 v
        (local mount / fake S3)
        data/2024-05-10/*.parquet
        data/2024-05-17/*.parquet

                 |
                 |  Ingestion (EventLogRawSaver)
                 v
s3/raw/event_log/week=YYYY-MM-DD/pn=PN_VALUE/*.parquet
        - 只做基本欄位補齊 + partition
        - 保留原始 log，不做破壞性變更

                 |
                 |  Cleaning (EventLogCleaner)
                 v
s3/clean/event_log/week=YYYY-MM-DD/pn=PN_VALUE/*.parquet
        - 解析 ev JSON
        - 計算 valid_timestamp
        - 產生 event_key
        - 依 service_tag + event_key 去重

                 |
                 |  Feature Engineering (EventLogFeatureEngineer)
                 v
s3/feature/event_log/
   ├─ device_features.parquet   (每台筆電一筆)
   └─ error_events.parquet      (error 級別時序資料)

                 |
                 v
        Data Scientist 分析 / 模型開發
</span></span></code></div></div></pre>

**Orchestration**

使用 Airflow DAG `event_log_pipeline` 串起整個流程：

1. `ingest_raw_data` → 讀 `data/`，寫入 raw zone
2. `clean_event_log` → 讀 raw，寫入 clean zone
3. `build_features` → 讀 clean，寫入 feature zone

---

## 3. 儲存策略、命名規則與分層架構

### 3.1 Fake S3 / Data Lake Layout

為了模擬未來上雲（如 S3 / Data Lakehouse）的型態，我採用  **S3-style 分區目錄** ：

#### Raw Layer

<pre class="overflow-visible!" data-start="2196" data-end="2485"><div class="contain-inline-size rounded-2xl corner-superellipse/1.1 relative bg-token-sidebar-surface-primary"><div class="sticky top-9"><div class="absolute end-0 bottom-0 flex h-9 items-center pe-2"><div class="bg-token-bg-elevated-secondary text-token-text-secondary flex items-center gap-4 rounded-sm px-2 font-sans text-xs"></div></div></div><div class="overflow-y-auto p-4" dir="ltr"><code class="whitespace-pre! language-text"><span><span>s3/raw/event_log/
  └─ week=2024-05-10/
      ├─ pn=I13/
      │   ├─ 2024-05-10_I13.parquet
      │   ├─ 2024-05-10_I13_1.parquet
      ├─ pn=L5/
      │   └─ 2024-05-10_L5.parquet
      └─ pn=UNKNOWN/
          └─ 2024-05-10_UNKNOWN.parquet
  └─ week=2024-05-17/
      └─ ...
</span></span></code></div></div></pre>

**設計理由**

* `week=...`、`pn=...` 採 **key=value** 命名：
  * 和 Hive / Spark / Trino 等常見查詢引擎兼容
  * Query 時可以直接用 partition pruning（例如：只讀某週 / 某機種）
* `pn` 未知或不在 `VALID_MODELS = {I13, L5, DG5}` 內 → 落在 `pn=UNKNOWN`：
  * 讓資料不會消失
  * 後續可針對 UNKNOWN 進行追蹤與修補
* 檔案格式：**Parquet**
  * 壓縮好（節省儲存空間）
  * columnar 格式對後續分析友善
  * 與 Pandas / Spark / DuckDB 完全相容

#### Clean Layer

<pre class="overflow-visible!" data-start="2842" data-end="2995"><div class="contain-inline-size rounded-2xl corner-superellipse/1.1 relative bg-token-sidebar-surface-primary"><div class="sticky top-9"><div class="absolute end-0 bottom-0 flex h-9 items-center pe-2"><div class="bg-token-bg-elevated-secondary text-token-text-secondary flex items-center gap-4 rounded-sm px-2 font-sans text-xs"></div></div></div><div class="overflow-y-auto p-4" dir="ltr"><code class="whitespace-pre! language-text"><span><span>s3/clean/event_log/
  └─ week=2024-05-10/
      ├─ pn=I13/2024-05-10_I13.parquet
      ├─ pn=L5/2024-05-10_L5.parquet
      └─ pn=UNKNOWN/...
</span></span></code></div></div></pre>

* 保留與 raw 相同 partition key（`week`, `pn`），方便交叉比對 row count
* 新增欄位：
  * `valid_timestamp`
  * `event_key`
  * `cat`（從 ev JSON 抽出）

#### Feature Layer

<pre class="overflow-visible!" data-start="3146" data-end="3285"><div class="contain-inline-size rounded-2xl corner-superellipse/1.1 relative bg-token-sidebar-surface-primary"><div class="sticky top-9"><div class="absolute end-0 bottom-0 flex h-9 items-center pe-2"><div class="bg-token-bg-elevated-secondary text-token-text-secondary flex items-center gap-4 rounded-sm px-2 font-sans text-xs"></div></div></div><div class="overflow-y-auto p-4" dir="ltr"><code class="whitespace-pre! language-text"><span><span>s3/feature/event_log/
  ├─ device_features.parquet    # 每台 service_tag 一筆
  └─ error_events.parquet       # 只保留 error log 的時序資料
</span></span></code></div></div></pre>

* feature 資料量已大幅縮減，暫不再做 partition
* 若資料持續增加，可再分成：
  * `week=.../device_features.parquet`
  * `week=.../error_events.parquet`

---

## 4. 各階段實作與技術選擇

### 4.1 Ingestion：EventLogRawSaver

 **位置** ：`src/ingestion/ingest.py`

 **類別** ：`EventLogRawSaver`

**邏輯**

1. 掃描 `DATA_ROOT` 底下的週目錄，例如：
   <pre class="overflow-visible!" data-start="3574" data-end="3646"><div class="contain-inline-size rounded-2xl corner-superellipse/1.1 relative bg-token-sidebar-surface-primary"><div class="sticky top-9"><div class="absolute end-0 bottom-0 flex h-9 items-center pe-2"><div class="bg-token-bg-elevated-secondary text-token-text-secondary flex items-center gap-4 rounded-sm px-2 font-sans text-xs"></div></div></div><div class="overflow-y-auto p-4" dir="ltr"><code class="whitespace-pre! language-text"><span><span>data/2024-05-10/*.parquet
   data/2024-05-17/*.parquet
   </span></span></code></div></div></pre>
2. 讀入每週全部 Parquet，補上 `week` 欄位。
3. 清洗 `pn` 欄位：
   * 缺失或不在 `{I13, L5, DG5}` 內 → 設為 `"UNKNOWN"`
4. 依 `week`, `pn` 分組，寫回：
   <pre class="overflow-visible!" data-start="3769" data-end="3827"><div class="contain-inline-size rounded-2xl corner-superellipse/1.1 relative bg-token-sidebar-surface-primary"><div class="sticky top-9"><div class="absolute end-0 bottom-0 flex h-9 items-center pe-2"><div class="bg-token-bg-elevated-secondary text-token-text-secondary flex items-center gap-4 rounded-sm px-2 font-sans text-xs"></div></div></div><div class="overflow-y-auto p-4" dir="ltr"><code class="whitespace-pre! language-text"><span><span>s3/raw/event_log/week={week}/pn={pn}/...
   </span></span></code></div></div></pre>
5. 若同一 `(week, pn)` 多次執行（重跑），自動在檔名加 `_1`, `_2` 避免覆蓋。

**技術選擇理由**

* 用  **PyArrow / Pandas** ：
  * 開發速度快、容易 debug
  * 對於目前等級的資料量（數百萬列）足夠
* Partition 設計提前與日後 Spark / BigQuery 的使用習慣對齊，便於未來擴展。

---

### 4.2 Cleaning：EventLogCleaner

 **位置** ：`src/cleaning/clean.py`

 **類別** ：`EventLogCleaner`

#### 4.2.1 解析 `ev` JSON

* 使用 `orjson` / `json` 將 `ev` 轉成 dict
* 抽出欄位：
  * `TimeStamp`
  * `LogTime`
  * `EventId` / `eventID`
  * `Cat`（記錄 error / warning / info）

#### 4.2.2 計算 `valid_timestamp`

時間欄位優先順序：

1. `ev.TimeStamp`
2. `ev.LogTime`
3. `ts`
4. `eventdate`（epoch，需除以 1e9 再轉成秒）

實作：

* 用 `pd.to_datetime(..., errors="coerce")` 將各欄位轉成 datetime
* 按優先順序用 `fillna` 合併：
  <pre class="overflow-visible!" data-start="4489" data-end="4570"><div class="contain-inline-size rounded-2xl corner-superellipse/1.1 relative bg-token-sidebar-surface-primary"><div class="sticky top-9"><div class="absolute end-0 bottom-0 flex h-9 items-center pe-2"><div class="bg-token-bg-elevated-secondary text-token-text-secondary flex items-center gap-4 rounded-sm px-2 font-sans text-xs"></div></div></div><div class="overflow-y-auto p-4" dir="ltr"><code class="whitespace-pre! language-python"><span><span>df[</span><span>"valid_timestamp"</span><span>] = ts1.fillna(ts2).fillna(ts3).fillna(ts4)
  </span></span></code></div></div></pre>

#### 4.2.3 產生 `event_key`

題目規則：

* 情況 1：`ev` 中有 `TimeStamp`
  * 將 `ev` dict 中  **扣除以下欄位** ：
    <pre class="overflow-visible!" data-start="4667" data-end="4919"><div class="contain-inline-size rounded-2xl corner-superellipse/1.1 relative bg-token-sidebar-surface-primary"><div class="sticky top-9"><div class="absolute end-0 bottom-0 flex h-9 items-center pe-2"><div class="bg-token-bg-elevated-secondary text-token-text-secondary flex items-center gap-4 rounded-sm px-2 font-sans text-xs"></div></div></div><div class="overflow-y-auto p-4" dir="ltr"><code class="whitespace-pre! language-python"><span><span>DROP_KEYS = {
        </span><span>"LogTime"</span><span>, </span><span>"PluginVersion"</span><span>, </span><span>"SOSVersion"</span><span>, </span><span>"Sosstatus"</span><span>,
        </span><span>"IsSOSEnabled"</span><span>, </span><span>"IsSOSSupportedByBIOS"</span><span>, </span><span>"EventType"</span><span>,
        </span><span>"AppSessionID"</span><span>, </span><span>"Imagestatus"</span><span>, </span><span>"AgentVersion"</span><span>,
        </span><span>"Wrestatus"</span><span>, </span><span>"EventId"</span><span>,
    }
    </span></span></code></div></div></pre>
  * 將剩下的 payload 序列化（JSON sorted keys），做 `SHA256`，得到 `event_key`
* 情況 2：`ev` 中 **沒有** `TimeStamp`
  * 直接使用 `EventId` 或 `eventID` 做為 `event_key`

這樣設計的好處：

* 對有 `TimeStamp` 的事件，以「內容 hash」為 key，即使多次重送，也會被視為同一事件。
* 無 `TimeStamp` 的事件仍可透過 `EventId` 去重。

#### 4.2.4 去重規則

* `valid_timestamp`  **為 NaT** ：不去重 → 全部保留（表示無法判斷時間先後）
* 其餘資料：
  * 以 `(service_tag, event_key)` 分組
  * 排序 `valid_timestamp`
  * 保留最早的一筆

實作概念：

<pre class="overflow-visible!" data-start="5328" data-end="5686"><div class="contain-inline-size rounded-2xl corner-superellipse/1.1 relative bg-token-sidebar-surface-primary"><div class="sticky top-9"><div class="absolute end-0 bottom-0 flex h-9 items-center pe-2"><div class="bg-token-bg-elevated-secondary text-token-text-secondary flex items-center gap-4 rounded-sm px-2 font-sans text-xs"></div></div></div><div class="overflow-y-auto p-4" dir="ltr"><code class="whitespace-pre! language-python"><span><span>mask_valid = df[</span><span>"valid_timestamp"</span><span>].notna()
df_has_ts = df[mask_valid].copy()
df_no_ts  = df[~mask_valid].copy()

df_has_ts = (
    df_has_ts
    .sort_values([</span><span>"service_tag"</span><span>, </span><span>"event_key"</span><span>, </span><span>"valid_timestamp"</span><span>])
    .drop_duplicates(subset=[</span><span>"service_tag"</span><span>, </span><span>"event_key"</span><span>], keep=</span><span>"first"</span><span>)
)

df_clean = pd.concat([df_has_ts, df_no_ts], ignore_index=</span><span>True</span><span>)
</span></span></code></div></div></pre>

最後依 `week`, `pn` 再次分組寫回 clean zone。

---

### 4.3 Feature Engineering：EventLogFeatureEngineer

 **位置** ：`src/feature/feature.py`

 **類別** ：`EventLogFeatureEngineer`

目標：對應題目中：

> Data Scientist 需要有發生 error 的 log，進行時序與次數的分析。
>
> Data Scientist 會依據分析需求，增加所需特徵（架構設計需保留彈性）。

#### 4.3.1 基本欄位與 flag

從 clean 資料表增加：

* `is_error`：`cat ∈ {"Error", "Critical"}`
* `is_warning`：`cat == "Warning"`

#### 4.3.2 Device-level 聚合特徵（每台筆電一筆）

`device_features.parquet` 包含：

* count 類：
  * `error_count_total`
  * `warning_count_total`
  * `total_events`
  * `error_ratio = error_count_total / total_events`
* 時間範圍：
  * `first_error_time`
  * `last_error_time`
* error 間隔特徵：
  * `error_interval_mean`（平均 error 之間間隔秒數）
  * `error_interval_std`（間隔標準差，代表穩定度）
* 識別欄位：
  * `service_tag`
  * `week`（出現次數最多的 week）
  * `pn`（出現次數最多的 pn）

給 Data Scientist 直接做：

* error 豐富度 vs 穩定度分析
* ranking / scoring（例如設計 stability_score）

#### 4.3.3 Error 時序資料

`error_events.parquet`：

* 只保留 `is_error == True` 的列
* 欄位包含：
  * `service_tag`
  * `valid_timestamp`
  * `cat`
  * `event_key`
  * `pn`
  * `week`
  * `ev`（完整 payload，方便 DS 做進一步特徵）

Data Scientist 可以直接使用這張表進行：

* time-series analysis
* error burst detection
* per-device 時序模型建置

---

### 4.4 Airflow Orchestration

 **位置** ：`airflow/dags/event_log_pipeline_dag.py`

 **DAG ID** ：`event_log_pipeline`

Task 列表：

1. `ingest_raw_data`
   * 呼叫 `EventLogRawSaver`
   * 負責從 `data/` 匯入 raw zone
2. `clean_event_log`
   * 呼叫 `EventLogCleaner`
   * 執行 ev 解析、時間計算、去重
3. `build_features`
   * 呼叫 `EventLogFeatureEngineer`
   * 生成 `device_features.parquet` 與 `error_events.parquet`

DAG 特點：

* 使用 `PythonOperator`，方便復用已寫好的 class
* Default args：
  * `retries=2`, 指數退避
  * `on_failure_callback` 設定通知（如 email / log）
* 所有路徑由 `src/config/settings.py` 管理：
  * `DATA_ROOT`, `RAW_ROOT`, `CLEAN_ROOT`, `VALID_MODELS`, `PARQUET_ENGINE`

---

## 5. 定義與追蹤指標（效能與資料品質）

### 5.1 Pipeline 效能指標

在 Airflow / log 中可追蹤：

1. **各 stage 執行時間**
   * `t_ingest`, `t_clean`, `t_feature`（秒）
   * 透過 Airflow task duration 直接觀察
2. **吞吐量**
   * `rows_ingested / t_ingest`
   * `rows_cleaned / t_clean`
   * `rows_feature / t_feature`
3. **排程延遲**
   * 「最新 `valid_timestamp` 與 pipeline 執行時間的差距」
   * e.g. `now() - max(valid_timestamp)`，用來量化資料延遲

### 5.2 資料品質指標（Data Quality）

1. **Row count consistency**
   * `raw_rows`, `clean_rows`, `feature_rows`
   * `duplication_rate = 1 - clean_rows/raw_rows`
   * 用來確認去重是否合理（異常高 / 異常低都要注意）
2. **Time completeness**
   * `% rows with non-null valid_timestamp`
   * 若某一週突然下降，代表來源時間欄位有問題
3. **Key 完整性**
   * `% rows with non-null event_key`
4. **Error/Warning 分佈**
   * `error_ratio`, `warning_ratio` per service_tag / per pn
   * 若某機種突然 error ratio 飆高，可能是產品品質或 agent 問題
5. **分區分佈**
   * 每個 `week`, `pn` 的 row count
   * 監控 UNKNOWN bucket 的比例，避免 meta 資訊（機種）長期遺失

這些指標可以：

* 在每次 pipeline 執行結尾寫入一張「DQ summary table」
* 或透過額外一個 Airflow task 將統計寫到 log / DB 供監控

---
