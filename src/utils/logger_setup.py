import logging
import os
from logging.handlers import TimedRotatingFileHandler


def get_logger(
    name: str = "ce_logger",
    log_dir: str = "logs",
    level: int = logging.INFO,
) -> logging.Logger:
    """
    建立一個同時輸出到 console + 檔案的 logger。
    檔案使用每日輪替，放在 logs/ 底下。
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # 若已經設定過 handler，就直接回傳（避免重複加入）
    if logger.handlers:
        return logger

    # 確保 logs 目錄存在
    os.makedirs(log_dir, exist_ok=True)

    # -------- formatter --------
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # -------- Console Handler --------
    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # -------- File Handler（按天輪替）--------
    log_path = os.path.join(log_dir, "crawler.log")
    fh = TimedRotatingFileHandler(
        log_path,
        when="midnight",  # 每天 00:00 輪替
        interval=1,
        backupCount=7,  # 保留 7 天
        encoding="utf-8",
    )
    fh.setLevel(level)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    logger.info(f"Logger initialized. Log file: {log_path}")
    return logger
