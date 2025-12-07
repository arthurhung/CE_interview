# Cleaning— Event Log Cleaner

### 執行 Cleaning

<pre class="overflow-visible!" data-start="1831" data-end="1875"><div class="contain-inline-size rounded-2xl corner-superellipse/1.1 relative bg-token-sidebar-surface-primary"><div class="sticky top-9"><div class="absolute end-0 bottom-0 flex h-9 items-center pe-2"><div class="bg-token-bg-elevated-secondary text-token-text-secondary flex items-center gap-4 rounded-sm px-2 font-sans text-xs"></div></div></div><div class="overflow-y-auto p-4" dir="ltr"><code class="whitespace-pre! language-bash"><span><span>cd</span><span> src

python -m cleaning.clean</span></span></code></div></div></pre>


## 清洗邏輯摘要

* `valid_timestamp` 優先序：
  <pre class="overflow-visible!" data-start="2191" data-end="2259"><div class="contain-inline-size rounded-2xl corner-superellipse/1.1 relative bg-token-sidebar-surface-primary"><div class="sticky top-9"><div class="absolute end-0 bottom-0 flex h-9 items-center pe-2"><div class="bg-token-bg-elevated-secondary text-token-text-secondary flex items-center gap-4 rounded-sm px-2 font-sans text-xs"></div></div></div><div class="overflow-y-auto p-4" dir="ltr"><code class="whitespace-pre! language-text"><span><span>ev.TimeStamp → ev.LogTime → ts → eventdate(epoch ns)
  </span></span></code></div></div></pre>
* `event_key`：
  * 有 `TimeStamp`：去除題目指定欄位後，對 payload 做 SHA256
  * 無 `TimeStamp`：使用 `EventId`/`eventID`
* 去重：
  * `valid_timestamp` 為 NaT：不去重
  * 否則以 `(service_tag, event_key)` 分組，保留最早的 `valid_timestamp`

---

## 輸出目錄範例

<pre class="overflow-visible!" data-start="2481" data-end="2671"><div class="contain-inline-size rounded-2xl corner-superellipse/1.1 relative bg-token-sidebar-surface-primary"><div class="sticky top-9"><div class="absolute end-0 bottom-0 flex h-9 items-center pe-2"><div class="bg-token-bg-elevated-secondary text-token-text-secondary flex items-center gap-4 rounded-sm px-2 font-sans text-xs"></div></div></div><div class="overflow-y-auto p-4" dir="ltr"><code class="whitespace-pre! language-text"><span><span>s3/clean/event_log/
  ├─ week=2024-05-10/
  │   ├─ pn=I13/2024-05-10_I13.parquet
  │   └─ pn=L5/2024-05-10_L5.parquet
  └─ week=2024-05-17/
      └─ pn=DG5/2024-05-17_DG5.parquet</span></span></code></div></div></pre>
