# src/feature/feature.py

import glob
import os
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd


class EventLogFeatureEngineer:
    """
    從 clean 區讀取 event log，產出給 Data Scientist 使用的特徵：

    1. device-level 聚合特徵（每台筆電一筆）：
       - error_count_total
       - warning_count_total
       - total_events
       - error_ratio
       - first_error_time
       - last_error_time
       - error_interval_mean
       - error_interval_std
       - pn, week 等維度欄位

    2. error_events（error 級別的時序資料）：
       - 只保留 is_error == True 的列
       - 搭配 valid_timestamp，可直接做 time series 分析
    """

    def __init__(
        self,
        clean_root: str | Path = "s3/clean/event_log",
        feature_root: str | Path = "s3/feature/event_log",
        parquet_engine: str = "fastparquet",
    ) -> None:
        self.clean_root = Path(clean_root)
        self.feature_root = Path(feature_root)
        self.parquet_engine = parquet_engine

    # -------------------- public API -------------------- #
    def run(self) -> None:
        """
        主要入口：
        - 從 clean_root 讀取 parquet
        - 計算特徵
        - 寫出到 feature_root
        """
        df_clean = self.load_clean()
        df_clean = self.add_error_flags(df_clean)

        # 產出兩種輸出：
        # 1. device_features：每台 device 一筆
        # 2. error_events：error 級別時序資料
        device_features = self.build_device_features(df_clean)
        error_events = self.build_error_events(df_clean)

        self.save_features(device_features, error_events)

    # -------------------- internal steps -------------------- #
    def load_clean(self) -> pd.DataFrame:
        """
        從 clean_root 底下讀取所有 parquet，合併成一個 df_clean。
        預期結構：
            s3/clean/event_log/week=YYYY-MM-DD/pn=MODEL/*.parquet
        """
        pattern = str(self.clean_root / "**" / "*.parquet")
        files = glob.glob(pattern, recursive=True)

        if not files:
            raise FileNotFoundError(f"No parquet files found under {self.clean_root}")

        dfs = []
        for f in files:
            df_tmp = pd.read_parquet(f, engine=self.parquet_engine)
            dfs.append(df_tmp)

        df_clean = pd.concat(dfs, ignore_index=True)
        print(f"[FeatureEngineer] Loaded clean rows: {len(df_clean)}")

        # 確保必要欄位存在
        required_cols = {"service_tag", "valid_timestamp", "cat"}
        missing = required_cols - set(df_clean.columns)
        if missing:
            raise ValueError(f"Missing required columns in clean data: {missing}")

        # 轉成 datetime（保險）
        df_clean["valid_timestamp"] = pd.to_datetime(df_clean["valid_timestamp"], errors="coerce")

        return df_clean

    def add_error_flags(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        依 cat 欄位標記 is_error / is_warning。
        規則可以依實際資料調整。
        """
        cat_series = df["cat"].astype("string")

        df["is_error"] = cat_series.isin(["Err", "Error", "Critical"])
        df["is_warning"] = cat_series.eq("Warning")

        return df

    def build_device_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        每台 device 一筆，計算聚合特徵：
        - error_count_total
        - warning_count_total
        - total_events
        - error_ratio
        - first_error_time
        - last_error_time
        - error_interval_mean
        - error_interval_std
        - week（取最常出現的那個）/ pn（取最常出現的那個）
        """

        # 基礎 group：每台 machine 的全部 rows
        grp = df.groupby("service_tag", as_index=True)

        # 次數相關
        error_count = grp["is_error"].sum()
        warning_count = grp["is_warning"].sum()
        total_events = grp["cat"].count()

        # 比例
        error_ratio = (error_count / total_events).fillna(0.0)

        # 時間範圍（第一個 error / 最後一個 error）
        df_error = df[df["is_error"]].copy()
        if len(df_error) > 0:
            grp_error = df_error.groupby("service_tag")
            first_error_time = grp_error["valid_timestamp"].min()
            last_error_time = grp_error["valid_timestamp"].max()
        else:
            first_error_time = pd.Series([], dtype="datetime64[ns]")
            last_error_time = pd.Series([], dtype="datetime64[ns]")

        # error interval（平均間隔 / 標準差）
        df_error = df_error.sort_values(["service_tag", "valid_timestamp"])
        df_error["prev_time"] = df_error.groupby("service_tag")["valid_timestamp"].shift(1)
        df_error["interval_sec"] = (df_error["valid_timestamp"] - df_error["prev_time"]).dt.total_seconds()

        # 移除第一筆 NaN interval
        df_interval = df_error.dropna(subset=["interval_sec"])
        if len(df_interval) > 0:
            grp_int = df_interval.groupby("service_tag")
            interval_mean = grp_int["interval_sec"].mean()
            interval_std = grp_int["interval_sec"].std()
        else:
            interval_mean = pd.Series([], dtype="float64")
            interval_std = pd.Series([], dtype="float64")

        # week / pn 用出現次數最多的那個（mode）
        def _mode(series: pd.Series) -> Optional[str]:
            if series.empty:
                return None
            mode_vals = series.mode(dropna=True)
            if len(mode_vals) == 0:
                return None
            return mode_vals.iloc[0]

        week_mode = grp["week"].agg(_mode) if "week" in df.columns else None
        pn_mode = grp["pn"].agg(_mode) if "pn" in df.columns else None

        # 組合成一張 feature table
        feat = pd.DataFrame(
            {
                "service_tag": error_count.index,
                "error_count_total": error_count.values,
                "warning_count_total": warning_count.reindex(error_count.index).fillna(0).values,
                "total_events": total_events.reindex(error_count.index).fillna(0).values,
                "error_ratio": error_ratio.reindex(error_count.index).fillna(0).values,
            }
        ).set_index("service_tag")

        feat["first_error_time"] = first_error_time.reindex(feat.index)
        feat["last_error_time"] = last_error_time.reindex(feat.index)
        feat["error_interval_mean"] = interval_mean.reindex(feat.index)
        feat["error_interval_std"] = interval_std.reindex(feat.index)

        if week_mode is not None:
            feat["week"] = week_mode.reindex(feat.index)
        if pn_mode is not None:
            feat["pn"] = pn_mode.reindex(feat.index)

        feat.reset_index(inplace=True)
        print(f"[FeatureEngineer] Built device_features rows: {len(feat)}")
        return feat

    def build_error_events(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        專門給 DS 拿來做 time series 的 error-only log：
        - 僅保留 is_error == True
        - 僅保留與分析相關欄位（service_tag, valid_timestamp, cat, event_key, pn, week...）
        """
        cols = []
        for c in [
            "service_tag",
            "valid_timestamp",
            "cat",
            "event_key",
            "pn",
            "week",
            "ev",
        ]:
            if c in df.columns:
                cols.append(c)

        df_error = df[df["is_error"]].copy()
        df_error = df_error[cols].sort_values(["service_tag", "valid_timestamp"], na_position="last")
        print(f"[FeatureEngineer] Built error_events rows: {len(df_error)}")
        return df_error

    def save_features(self, device_features: pd.DataFrame, error_events: pd.DataFrame):
        """
        將特徵寫到 feature_root，結構示意：

        s3/feature/event_log/
          ├── device_features.parquet
          └── error_events.parquet

        如果你想依 week 分區，也可以改成：
          week=YYYY-MM-DD/device_features.parquet
        """
        self.feature_root.mkdir(parents=True, exist_ok=True)

        # 1) 整體一張 device_features
        dev_path = self.feature_root / "device_features.parquet"
        device_features.to_parquet(dev_path, engine=self.parquet_engine, index=False)
        print(f"[FeatureEngineer] Saved device_features -> {dev_path}")

        # 2) 整體一張 error_events
        err_path = self.feature_root / "error_events.parquet"
        error_events.to_parquet(err_path, engine=self.parquet_engine, index=False)
        print(f"[FeatureEngineer] Saved error_events -> {err_path}")


if __name__ == "__main__":
    engineer = EventLogFeatureEngineer(
        clean_root="s3/clean/event_log",
        feature_root="s3/feature/event_log",
        parquet_engine="fastparquet",
    )
    engineer.run()
