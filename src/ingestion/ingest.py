import sys
from pathlib import Path
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
from typing import Iterable, Set, Optional

ROOT = Path(__file__).resolve().parents[2]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.config.settings import DATA_ROOT, RAW_ROOT


class EventLogRawIngestor:
    """
    將 data/2024-05-10/*.parquet 這種原始檔
    重新切分成 fake S3 結構：
      s3/raw/event_log/week=2024-05-10/pn=I13/*.parquet
    """

    def __init__(
        self,
        src_root: str | Path = DATA_ROOT,
        dst_root: str | Path = RAW_ROOT,
        valid_models: Optional[Iterable[str]] = None,
    ) -> None:
        self.src_root = Path(src_root)
        self.dst_root = Path(dst_root)
        self.valid_models: Set[str] = set(valid_models or {"I13", "L5", "DG5"})

    # -------------------- public API -------------------- #
    def run(self) -> None:
        """主要入口：逐週讀取並依 pn 分流寫到 raw zone"""
        for week_dir in self.src_root.iterdir():
            if not week_dir.is_dir():
                continue
            self._process_week_dir(week_dir)

    # -------------------- internal methods -------------------- #
    def _process_week_dir(self, week_dir: Path) -> None:
        week = week_dir.name  # e.g. "2024-05-10"
        print(f"processing week = {week}")

        files = list(week_dir.glob("*.parquet"))
        if not files:
            print(f"  no parquet files under {week_dir}, skip")
            return

        # 一週所有 parquet 合併讀入
        table = pq.read_table([str(f) for f in files])
        pdf = table.to_pandas()

        # 加上 week 欄位
        pdf["week"] = week

        # 處理 pn 欄位
        self._normalize_pn(pdf)

        # 依 pn 分組寫出
        for pn_value, gdf in pdf.groupby("pn"):
            self._write_partition(week, pn_value, gdf)

        print(f"done: {week}")

    def _normalize_pn(self, pdf: pd.DataFrame) -> None:
        """整理 pn 欄位，不合法者改成 UNKNOWN"""
        if "pn" not in pdf.columns:
            pdf["pn"] = "UNKNOWN"
            return

        pdf["pn"] = pdf["pn"].astype("string").str.strip()
        pdf["pn"] = pdf["pn"].where(pdf["pn"].isin(self.valid_models), other="UNKNOWN")

    def _write_partition(self, week: str, pn_value: str, gdf: pd.DataFrame) -> None:
        """
        將單一 week + pn 的資料寫成一個 parquet：
        路徑：s3/raw/event_log/week=YYYY-MM-DD/pn=MODEL/YYY-MM-DD_MODEL[_i].parquet
        """
        dest_dir = self.dst_root / f"week={week}" / f"pn={pn_value}"
        dest_dir.mkdir(parents=True, exist_ok=True)

        base_name = f"{week}_{pn_value}"
        dest_path = dest_dir / f"{base_name}.parquet"

        # 若同名檔已存在，自動加 _1, _2, ...
        i = 1
        while dest_path.exists():
            dest_path = dest_dir / f"{base_name}_{i}.parquet"
            i += 1

        out_table = pa.Table.from_pandas(gdf, preserve_index=False)
        pq.write_table(out_table, dest_path)

        print(f"  wrote pn={pn_value} -> {dest_path}")


if __name__ == "__main__":
    saver = EventLogRawIngestor(
        src_root=DATA_ROOT,
        dst_root=RAW_ROOT,
        valid_models={"I13", "L5", "DG5"},
    )
    saver.run()
