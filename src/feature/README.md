# Feature— EventLog Feature Engineer

### 執行 Feature Engineering

<pre class="overflow-visible!" data-start="3329" data-end="3374"><div class="contain-inline-size rounded-2xl corner-superellipse/1.1 relative bg-token-sidebar-surface-primary"><div class="sticky top-9"><div class="absolute end-0 bottom-0 flex h-9 items-center pe-2"><div class="bg-token-bg-elevated-secondary text-token-text-secondary flex items-center gap-4 rounded-sm px-2 font-sans text-xs"></div></div></div><div class="overflow-y-auto p-4" dir="ltr"><code class="whitespace-pre! language-bash"><span><span>cd</span><span> src

python -m feature.feature</span></span></code></div></div></pre>


## 輸出目錄範例

<pre class="overflow-visible!" data-start="3822" data-end="3910"><div class="contain-inline-size rounded-2xl corner-superellipse/1.1 relative bg-token-sidebar-surface-primary"><div class="sticky top-9"><div class="absolute end-0 bottom-0 flex h-9 items-center pe-2"><div class="bg-token-bg-elevated-secondary text-token-text-secondary flex items-center gap-4 rounded-sm px-2 font-sans text-xs"></div></div></div><div class="overflow-y-auto p-4" dir="ltr"><code class="whitespace-pre! language-text"><span><span>s3/feature/event_log/
  ├─ device_features.parquet
  └─ error_events.parquet
</span></span></code></div></div></pre>

* `device_features.parquet`
  * `service_tag`
  * `total_events`, `error_count_total`, `warning_count_total`
  * `error_ratio`
  * `first_error_time`, `last_error_time`
  * `error_interval_mean`, `error_interval_std`
  * `dominate_pn`, `dominate_week` 等欄位
* `error_events.parquet`
  * 僅包含 `cat` 為 Error/Critical 的 event
  * 保留 `service_tag`, `valid_timestamp`, `event_key`, `pn`, `week`, `cat`, `ev`
