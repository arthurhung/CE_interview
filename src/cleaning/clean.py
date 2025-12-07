import glob
from pathlib import Path
import os, sys
import pandas as pd
import orjson
import hashlib
import json

ROOT = Path(__file__).resolve().parents[2]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.config.settings import RAW_ROOT, CLEAN_ROOT, PARQUET_ENGINE, DROP_KEYS, NEED_COLS


class EventLogCleaner:

    def __init__(
        self,
        raw_root: str = RAW_ROOT,
        clean_root: str = CLEAN_ROOT,
        parquet_engine: str = PARQUET_ENGINE,
    ):
        self.raw_root = raw_root
        self.clean_root = clean_root
        self.parquet_engine = parquet_engine

        # 去除欄位：照題目規則
        self.drop_keys = DROP_KEYS

        # raw 裡一定要有的欄位
        self.need_cols = NEED_COLS

    # ------------------- public API ------------------- #
    def run(self) -> pd.DataFrame:
        """主要入口：從 raw 讀 → 清洗 → 去重 → 寫到 clean，回傳 df_clean"""
        df = self.load_raw()
        df = self.parse_ev_columns(df)
        df = self.build_valid_timestamp(df)
        df = self.build_event_key(df)
        df_clean = self.deduplicate(df)
        self.save_clean(df_clean)
        return df_clean

    # ------------------- steps ------------------- #
    def load_raw(self) -> pd.DataFrame:
        """從 raw_root 讀取 parquet，並從路徑帶入 week / pn"""
        files = glob.glob(f"{self.raw_root}/**/*.parquet", recursive=True)
        print("Found raw parquet files:", len(files))

        dfs = []
        for f in files:
            p = Path(f)

            # 路徑中解析 week, pn
            week = None
            pn = None
            for part in p.parts:
                if part.startswith("week="):
                    week = part.split("=", 1)[1]
                if part.startswith("pn="):
                    pn = part.split("=", 1)[1]

            if week is None or pn is None:
                raise ValueError(f"Cannot parse week/pn from path: {f}")

            df_tmp = pd.read_parquet(f, engine=self.parquet_engine)

            for c in self.need_cols:
                if c not in df_tmp.columns:
                    df_tmp[c] = pd.NA

            df_tmp["week"] = week
            df_tmp["pn"] = pn

            dfs.append(df_tmp[self.need_cols + ["week", "pn"]])

        df = pd.concat(dfs, ignore_index=True)
        print("Loaded rows:", len(df))
        return df

    def parse_ev_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """解析 ev JSON，抽 TimeStamp / LogTime / EventId / Cat"""

        def parse_ev(s):
            if not isinstance(s, str):
                return {}
            try:
                return orjson.loads(s)
            except Exception:
                return {}

        ev_dict = df["ev"].astype("string").map(parse_ev)

        df["ev_TimeStamp"] = ev_dict.map(lambda d: d.get("TimeStamp"))
        df["ev_LogTime"] = ev_dict.map(lambda d: d.get("LogTime"))
        df["ev_EventId"] = ev_dict.map(lambda d: d.get("EventId") or d.get("eventID"))
        df["cat"] = ev_dict.map(lambda d: d.get("Cat"))
        df["_ev_dict"] = ev_dict  # 暫存給後面 build_event_key 用

        return df

    def build_valid_timestamp(self, df: pd.DataFrame) -> pd.DataFrame:
        """依優先順序組出 valid_timestamp"""

        def to_ts(x):
            return pd.to_datetime(x, errors="coerce")

        ts1 = to_ts(df["ev_TimeStamp"])
        ts2 = to_ts(df["ev_LogTime"])
        ts3 = to_ts(df["ts"])

        event_numeric = pd.to_numeric(df["eventdate"], errors="coerce")
        ts4 = pd.to_datetime(
            event_numeric / 1_000_000_000,
            unit="s",
            errors="coerce",
        )

        df["valid_timestamp"] = ts1.fillna(ts2).fillna(ts3).fillna(ts4)
        return df

    def _build_event_key_single(self, d: dict) -> str | None:
        """單筆的 event_key 邏輯"""
        if not d:
            return None

        # 情況2：沒有 TimeStamp → 用 eventID
        if not d.get("TimeStamp"):
            return d.get("EventId") or d.get("eventID")

        # 情況1：有 TimeStamp → 去掉指定欄位後 hash
        payload = {k: v for k, v in d.items() if k not in self.drop_keys}
        s = json.dumps(payload, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(s.encode("utf-8")).hexdigest()

    def build_event_key(self, df: pd.DataFrame) -> pd.DataFrame:
        """為每列產生 event_key"""
        df["event_key"] = df["_ev_dict"].map(self._build_event_key_single)
        # 用完就可以丟掉，避免佔記憶體
        df = df.drop(columns=["_ev_dict"])
        return df

    def deduplicate(self, df: pd.DataFrame) -> pd.DataFrame:
        """依題目規則去重"""
        mask_valid = df["valid_timestamp"].notna()
        df_has_ts = df[mask_valid].copy()
        df_no_ts = df[~mask_valid].copy()

        df_has_ts = df_has_ts.sort_values(["service_tag", "event_key", "valid_timestamp"]).drop_duplicates(
            subset=["service_tag", "event_key"], keep="first"
        )

        df_clean = pd.concat([df_has_ts, df_no_ts], ignore_index=True)
        print("Before cleaning:", len(df))
        print("After cleaning:", len(df_clean))
        return df_clean

    def save_clean(self, df_clean: pd.DataFrame) -> None:
        """依 week, pn partition 寫回 clean_root"""
        for (week, pn), g in df_clean.groupby(["week", "pn"]):
            out_dir = f"{self.clean_root}/week={week}/pn={pn}"
            os.makedirs(out_dir, exist_ok=True)

            filename = f"{week}_{pn}.parquet"
            out_path = f"{out_dir}/{filename}"

            g.to_parquet(out_path, engine=self.parquet_engine, index=False)
            print("Saved:", out_path)


# ------------------------------------------------------------
# script 入口：給你 local 跑，也給 Airflow / CLI 用
# ------------------------------------------------------------
if __name__ == "__main__":
    cleaner = EventLogCleaner(
        raw_root="s3/raw/event_log",
        clean_root="s3/clean/event_log",
        parquet_engine="fastparquet",
    )
    cleaner.run()
