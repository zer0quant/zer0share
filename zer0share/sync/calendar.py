import pandas as pd
from loguru import logger
from pathlib import Path

import zer0share.dateutil as dateutil
from zer0share.storage import read_trade_cal, write_trade_cal
from zer0share.sync import SyncRuntime
from zer0share.sync._jobs import SyncJob

EXCHANGES = ["SSE", "SZSE"]
ALL_EXCHANGES = ["SSE", "SZSE", "CFFEX", "DCE", "SHFE", "CZCE", "INE", "GFEX"]
TRADE_CAL_FIRST_DATE = "19900101"


def _merge_trade_cal(existing: pd.DataFrame, fetched: pd.DataFrame) -> pd.DataFrame:
    if existing.empty:
        return fetched
    if fetched.empty:
        return existing
    return (
        pd.concat([existing, fetched], ignore_index=True)
        .drop_duplicates(subset=["exchange", "cal_date"], keep="last")
        .sort_values(["exchange", "cal_date"])
        .reset_index(drop=True)
    )


class TradeCalSyncJob(SyncJob):
    table_name = "trade_cal"
    supports_date_range = False

    def __init__(self, fetch=None, data_dir: Path | None = None):
        self._fetch = fetch
        self._data_dir = data_dir

    def run(self, rt: SyncRuntime, start_date=None, end_date=None) -> None:
        today = rt.calendar.today()
        end = today[:4] + "1231"
        max_dates = []
        try:
            for exchange in ALL_EXCHANGES:
                existing = read_trade_cal(self._data_dir, exchange)
                last = str(existing["cal_date"].max()) if not existing.empty else None
                start = dateutil.add_days(last, 1) if last else TRADE_CAL_FIRST_DATE
                if start <= end:
                    fetched = self._fetch(exchange, start, end)
                    df = _merge_trade_cal(existing, fetched)
                    write_trade_cal(self._data_dir, exchange, df)
                    logger.info(f"trade_cal {exchange} 写入完成: 新增 {len(fetched)} 条, 共 {len(df)} 条")
                else:
                    df = existing
                    logger.info(f"trade_cal {exchange} 已覆盖到 {last}，无需同步")
                if not df.empty:
                    max_dates.append(str(df["cal_date"].max()))
            rt.calendar.load_from_parquet(self._data_dir, ALL_EXCHANGES)
            if max_dates:
                rt.meta.update_last_date("trade_cal", min(max_dates))
            latest = min(max_dates) if max_dates else end
            logger.info("trade_cal 全部同步完成")
            rt.notifier.send_success(f"trade_cal 同步完成\n已覆盖至：{latest}")
        except Exception as e:
            logger.error(f"trade_cal 同步失败: {e}")
            rt.notifier.send(f"trade_cal 同步失败\n{e}")
            raise


def build_jobs(cfg, fetcher) -> list[SyncJob]:
    return [
        TradeCalSyncJob(fetch=fetcher.fetch_trade_cal, data_dir=cfg.data_dir),
    ]
