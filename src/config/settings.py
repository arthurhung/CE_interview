from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROJECT_ROOT = PROJECT_ROOT.resolve()

DATA_ROOT = str(PROJECT_ROOT / "data")
RAW_ROOT = str(PROJECT_ROOT / "s3" / "raw" / "event_log")
CLEAN_ROOT = str(PROJECT_ROOT / "s3" / "clean" / "event_log")
FEATURE_ROOT = str(PROJECT_ROOT / "s3" / "feature" / "event_log")

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
