import time

import pandas as pd
from loguru import logger

import zer0share.dateutil as dateutil
from zer0share.sources import DataSources
from zer0share.storage import DailyPartitionStore, SnapshotStore
from zer0share.sync._jobs import SyncJob, _format_duration


BASIC_TABLE_NAME = "ricequant_basic"
MINUTE_TABLE_NAME = "ricequant_stock_minute"
ETF_BASIC_TABLE_NAME = "ricequant_etf_basic"
ETF_MINUTE_TABLE_NAME = "ricequant_etf_minute"


def _chunks(items: list[str], size: int):
    for start in range(0, len(items), size):
        yield items[start : start + size]


class RiceQuantBasicSyncJob(SyncJob):
    table_name = BASIC_TABLE_NAME
    supports_date_range = False

    def __init__(self, cfg, fetcher):
        self.cfg = cfg
        self.fetcher = fetcher
        self.store = SnapshotStore(cfg.data_dir / "ricequant" / "basic" / "data.parquet")

    def run(self, rt, start_date: str | None = None, end_date: str | None = None) -> None:
        df = self.fetcher.fetch_basic()
        self.store.write(df)
        today = rt.calendar.today()
        rt.meta.update_last_date(BASIC_TABLE_NAME, today)
        rt.notifier.send_success(f"{BASIC_TABLE_NAME} 同步完成\n日期：{today}｜{len(df)} 行")


class RiceQuantStockMinuteSyncJob(SyncJob):
    table_name = MINUTE_TABLE_NAME
    supports_date_range = True

    def __init__(self, cfg, fetcher):
        self.cfg = cfg
        self.fetcher = fetcher
        self.store = DailyPartitionStore(cfg.data_dir / "ricequant" / "stock_minute")

    def _load_order_book_ids(self) -> list[str]:
        df = SnapshotStore(self.cfg.data_dir / "ricequant" / "basic" / "data.parquet").read()
        if df.empty:
            raise FileNotFoundError(
                "ricequant basic data not found; run `python main.py sync --table ricequant_basic` first"
            )
        if "status" in df.columns:
            df = df[df["status"] == "Active"]
        return sorted(df["order_book_id"].dropna().astype(str).tolist())

    def run(self, rt, start_date: str | None = None, end_date: str | None = None) -> None:
        today = rt.calendar.today()
        if start_date is None:
            last = rt.meta.get_last_date(MINUTE_TABLE_NAME)
            start = dateutil.add_days(last, 1) if last is not None else today
            end = today
        else:
            start = start_date
            end = end_date if end_date is not None else today
        if start > end:
            raise ValueError(f"start_date {start} is after end_date {end}")

        trading_days = rt.calendar.get_trading_days("SSE", start, end)
        if not trading_days and rt.meta.get_last_date("trade_cal") is None:
            raise RuntimeError("No trading days found. Run `sync --table trade_cal` first.")

        order_book_ids = self._load_order_book_ids()
        batch_size = max(1, int(self.cfg.ricequant.stock_minute.batch_size))
        current_meta = rt.meta.get_last_date(MINUTE_TABLE_NAME)
        for trade_date in trading_days:
            if self.store.exists(trade_date):
                if current_meta is None or trade_date > current_meta:
                    rt.meta.update_last_date(MINUTE_TABLE_NAME, trade_date)
                    current_meta = trade_date
                continue

            frames = []
            failures = []
            started = time.monotonic()
            for batch in _chunks(order_book_ids, batch_size):
                try:
                    df = self.fetcher.fetch_stock_minute(
                        batch,
                        trade_date,
                        trade_date,
                        self.cfg.ricequant.stock_minute.adjust_type,
                        self.cfg.ricequant.stock_minute.skip_suspended,
                    )
                except Exception as exc:
                    failures.extend((order_book_id, str(exc)) for order_book_id in batch)
                    logger.warning(
                        f"{MINUTE_TABLE_NAME}: batch {batch[0]}..{batch[-1]} failed on {trade_date}: {exc}"
                    )
                    continue
                if df is not None and not df.empty:
                    frames.append(df)
                time.sleep(self.cfg.ricequant.stock_minute.request_sleep_seconds)

            if not frames:
                raise RuntimeError(
                    f"all RiceQuant stock minute fetches failed for {trade_date}; failures={failures[:5]}"
                )

            combined = pd.concat(frames, ignore_index=True)
            self.store.write(trade_date, combined)
            rt.meta.update_last_date(MINUTE_TABLE_NAME, trade_date)
            current_meta = trade_date
            elapsed = _format_duration(time.monotonic() - started)
            message = (
                f"{MINUTE_TABLE_NAME} 同步完成\n"
                f"日期：{trade_date}｜写入 {len(combined)} 行｜失败 {len(failures)}｜耗时 {elapsed}"
            )
            if failures:
                message += "\n失败样例：" + "; ".join(f"{code}: {err}" for code, err in failures[:5])
            if failures:
                rt.notifier.send(message)
            else:
                rt.notifier.send_success(message)


class RiceQuantETFBasicSyncJob(SyncJob):
    table_name = ETF_BASIC_TABLE_NAME
    supports_date_range = False

    def __init__(self, cfg, fetcher):
        self.cfg = cfg
        self.fetcher = fetcher
        self.store = SnapshotStore(cfg.data_dir / "ricequant" / "etf_basic" / "data.parquet")

    def run(self, rt, start_date: str | None = None, end_date: str | None = None) -> None:
        df = self.fetcher.fetch_etf_basic()
        self.store.write(df)
        today = rt.calendar.today()
        rt.meta.update_last_date(ETF_BASIC_TABLE_NAME, today)
        rt.notifier.send_success(f"{ETF_BASIC_TABLE_NAME} 同步完成\n日期：{today}｜{len(df)} 行")


class RiceQuantETFMinuteSyncJob(SyncJob):
    table_name = ETF_MINUTE_TABLE_NAME
    supports_date_range = True

    def __init__(self, cfg, fetcher):
        self.cfg = cfg
        self.fetcher = fetcher
        self.store = DailyPartitionStore(cfg.data_dir / "ricequant" / "etf_minute")

    def _load_order_book_ids(self) -> list[str]:
        df = SnapshotStore(self.cfg.data_dir / "ricequant" / "etf_basic" / "data.parquet").read()
        if df.empty:
            raise FileNotFoundError(
                "ricequant etf_basic data not found; run `python main.py sync --table ricequant_etf_basic` first"
            )
        if "status" in df.columns:
            df = df[df["status"] == "Active"]
        return sorted(df["order_book_id"].dropna().astype(str).tolist())

    def run(self, rt, start_date: str | None = None, end_date: str | None = None) -> None:
        today = rt.calendar.today()
        if start_date is None:
            last = rt.meta.get_last_date(ETF_MINUTE_TABLE_NAME)
            start = dateutil.add_days(last, 1) if last is not None else today
            end = today
        else:
            start = start_date
            end = end_date if end_date is not None else today
        if start > end:
            raise ValueError(f"start_date {start} is after end_date {end}")

        trading_days = rt.calendar.get_trading_days("SSE", start, end)
        if not trading_days and rt.meta.get_last_date("trade_cal") is None:
            raise RuntimeError("No trading days found. Run `sync --table trade_cal` first.")

        order_book_ids = self._load_order_book_ids()
        batch_size = max(1, int(self.cfg.ricequant.etf_minute.batch_size))
        request_sleep_seconds = self.cfg.ricequant.etf_minute.request_sleep_seconds
        adjust_type = self.cfg.ricequant.etf_minute.adjust_type
        skip_suspended = self.cfg.ricequant.etf_minute.skip_suspended
        current_meta = rt.meta.get_last_date(ETF_MINUTE_TABLE_NAME)

        logger.info(f"{ETF_MINUTE_TABLE_NAME}: start sync {start} ~ {end}, total {len(trading_days)} trading days")

        for trade_date in trading_days:
            if self.store.exists(trade_date):
                if current_meta is None or trade_date > current_meta:
                    rt.meta.update_last_date(ETF_MINUTE_TABLE_NAME, trade_date)
                    current_meta = trade_date
                logger.info(f"{ETF_MINUTE_TABLE_NAME}: {trade_date} already exists, skipped")
                continue

            frames = []
            failures = []
            started = time.monotonic()
            logger.info(f"{ETF_MINUTE_TABLE_NAME}: fetching {trade_date}, {len(order_book_ids)} ETFs in {len(list(_chunks(order_book_ids, batch_size)))} batches")

            for batch in _chunks(order_book_ids, batch_size):
                try:
                    df = self.fetcher.fetch_etf_minute(
                        batch,
                        trade_date,
                        trade_date,
                        adjust_type,
                        skip_suspended,
                    )
                except Exception as exc:
                    failures.extend((order_book_id, str(exc)) for order_book_id in batch)
                    logger.warning(
                        f"{ETF_MINUTE_TABLE_NAME}: batch {batch[0]}..{batch[-1]} failed on {trade_date}: {exc}"
                    )
                    continue
                if df is not None and not df.empty:
                    frames.append(df)
                time.sleep(request_sleep_seconds)

            if not frames:
                raise RuntimeError(
                    f"all RiceQuant ETF minute fetches failed for {trade_date}; failures={failures[:5]}"
                )

            combined = pd.concat(frames, ignore_index=True)
            self.store.write(trade_date, combined)
            rt.meta.update_last_date(ETF_MINUTE_TABLE_NAME, trade_date)
            current_meta = trade_date
            elapsed = _format_duration(time.monotonic() - started)
            message = (
                f"{ETF_MINUTE_TABLE_NAME} 同步完成\n"
                f"日期：{trade_date}｜写入 {len(combined)} 行｜失败 {len(failures)}｜耗时 {elapsed}"
            )
            logger.info(f"{ETF_MINUTE_TABLE_NAME}: {trade_date} done, wrote {len(combined)} rows, {len(failures)} failures, elapsed {elapsed}")
            if failures:
                message += "\n失败样例：" + "; ".join(f"{code}: {err}" for code, err in failures[:5])
                logger.warning(f"{ETF_MINUTE_TABLE_NAME}: {trade_date} failures: {failures[:5]}")
            if failures:
                rt.notifier.send(message)
            else:
                rt.notifier.send_success(message)


def build_jobs(cfg, sources: DataSources):
    if not cfg.ricequant.enabled:
        return []
    if sources.ricequant is None:
        raise RuntimeError("RiceQuant is enabled but RiceQuantFetcher is not configured")
    jobs = [
        RiceQuantBasicSyncJob(cfg, sources.ricequant),
        RiceQuantStockMinuteSyncJob(cfg, sources.ricequant),
    ]
    if hasattr(cfg.ricequant, "etf_minute") and cfg.ricequant.etf_minute.enabled:
        jobs.extend([
            RiceQuantETFBasicSyncJob(cfg, sources.ricequant),
            RiceQuantETFMinuteSyncJob(cfg, sources.ricequant),
        ])
    return jobs
