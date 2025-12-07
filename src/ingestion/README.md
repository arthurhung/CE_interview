# Ingestion — Event Log Raw Ingestor

這個模組負責把題目提供的原始 parquet 檔，整理成 data lake 風格的 raw zone：

- Input：`data/YYYY-MM-DD/*.parquet`
- Output：`s3/raw/event_log/week=YYYY-MM-DD/pn=<MODEL>/*.parquet`

> 「pn」為筆電機種（I13 / L5 / DG5），未知機種會被歸類到 `UNKNOWN`。

---

## 結構說明

- `ingest.py`
  - EventLogRawIngestor：主要的 ingestion 類別
  - 會讀取 `src/config/settings.py` 內的：
    - `DATA_ROOT`
    - `RAW_ROOT`
    - `VALID_MODELS`

---

## 使用方式

### 直接用 Python script 執行

在專案根目錄（`CE_interview/`）下：

```bash
cd src

python -m ingestion.ingest
```


## 輸出目錄範例

<pre class="overflow-visible!" data-start="1013" data-end="1249"><div class="contain-inline-size rounded-2xl corner-superellipse/1.1 relative bg-token-sidebar-surface-primary"><div class="sticky top-9"><div class="absolute end-0 bottom-0 flex h-9 items-center pe-2"><div class="bg-token-bg-elevated-secondary text-token-text-secondary flex items-center gap-4 rounded-sm px-2 font-sans text-xs"></div></div></div><div class="overflow-y-auto p-4" dir="ltr"><code class="whitespace-pre! language-text"><span><span>s3/raw/event_log/
  ├─ week=2024-05-10/
  │   ├─ pn=I13/2024-05-10_I13.parquet
  │   ├─ pn=L5/2024-05-10_L5.parquet
  │   └─ pn=UNKNOWN/2024-05-10_UNKNOWN.parquet
  └─ week=2024-05-17/
      ├─ pn=I13/...
      └─ pn=DG5/...</span></span></code></div></div></pre>
