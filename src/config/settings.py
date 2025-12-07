DATA_ROOT = "../data"
RAW_ROOT = "../s3/raw/event_log"
CLEAN_ROOT = "../s3/clean/event_log"
VALID_MODELS = {"I13", "L5", "DG5"}
PARQUET_ENGINE = "fastparquet"
DROP_KEYS = {
    "LogTime",
    "PluginVersion",
    "SOSVersion",
    "Sosstatus",
    "IsSOSEnabled",
    "IsSOSSupportedByBIOS",
    "EventType",
    "AppSessionID",
    "Imagestatus",
    "AgentVersion",
    "Wrestatus",
    "EventId",
}
NEED_COLS = ["service_tag", "eventdate", "ts", "ev"]
