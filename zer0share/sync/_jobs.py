"""
sync/_jobs.py — Abstract and concrete sync job implementations.

SyncJob          ABC with table_name, supports_date_range, abstract run()
DailySyncJob     Daily-partitioned table sync (loops over trading days)
SnapshotSyncJob  Single-file snapshot table sync
"""
import time
import datetime as dt
from abc import ABC, abstractmethod
from typing import Callable

import pandas as pd
from loguru import logger

import zer0share.dateutil as dateutil
from zer0share.query.repository import DailyTableSpec, TableSpec
from zer0share.storage import DailyPartitionStore, SnapshotStore
from zer0share.sync import SyncRuntime

PROGRESS_INTERVAL = 50
_RETRY_DELAYS = (5, 15, 45)  # seconds between retries


def _format_duration(seconds: float) -> str:
    total_seconds = max(0, int(seconds))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def _estimate_eta(elapsed_seconds: float, processed: int, total: int) -> str:
    if processed <= 0 or total <= processed:
        return "00:00:00"
    seconds_per_item = elapsed_seconds / processed
    remaining = total - processed
    return _format_duration(seconds_per_item * remaining)


def _date_range_days(start: str, end: str) -> list[str]:
    start_date = dateutil.parse_date(start)
    end_date = dateutil.parse_date(end)
    days = []
    current = start_date
    while current <= end_date:
        days.append(current.strftime("%Y%m%d"))
        current += dt.timedelta(days=1)
    return days


class SyncJob(ABC):
    table_name: str
    supports_date_range: bool

    @abstractmethod
    def run(self, rt: SyncRuntime, start_date: str | None = None, end_date: str | None = None) -> None:
        ...


class DailySyncJob(SyncJob):
    def __init__(
        self,
        table_name: str,
        spec: DailyTableSpec,
        fetch: Callable[[str], pd.DataFrame],
        store: DailyPartitionStore,
        write_empty: bool = False,
        exchange: str = "SSE",
        supports_date_range: bool = True,
        period: str = "day",
    ):
        self.table_name = table_name
        self.spec = spec
        self.fetch = fetch
        self.store = store
        self.write_empty = write_empty
        self.exchange = exchange
        self.supports_date_range = supports_date_range
        self.period = period

    def run(
        self,
        rt: SyncRuntime,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> None:
        today = rt.calendar.today()

        if start_date is None:
            last = rt.meta.get_last_date(self.spec.name)
            start = dateutil.add_days(last, 1) if last is not None else self.spec.first_date
            end = today
            if start > end:
                logger.info(f"{self.spec.name}: 已是最新 (last={last})")
                return
        else:
            start = start_date
            end = end_date if end_date is not None else today
            if start > end:
                raise ValueError(
                    f"start_date {start} is after end_date {end}"
                )

        trade_cal_loaded = rt.meta.get_last_date("trade_cal") is not None
        if self.period == "week":
            trading_days = rt.calendar.get_week_end_trading_days(self.exchange, start, end)
        elif self.period == "month":
            trading_days = rt.calendar.get_month_end_trading_days(self.exchange, start, end)
        else:
            trading_days = rt.calendar.get_trading_days(self.exchange, start, end)

        if not trading_days:
            if not trade_cal_loaded:
                raise RuntimeError(
                    f"No trading days found for {self.exchange} between {start} and {end}. "
                    "Trade calendar may not be loaded. Run `sync --table trade_cal` first."
                )
            return

        logger.info(
            f"{self.spec.name}: start {start} ~ {end}, "
            f"trading_days={len(trading_days)}"
        )

        success = 0
        total_rows = 0
        empty = 0
        skipped_existing = 0
        current_meta = rt.meta.get_last_date(self.spec.name)
        started_at = time.monotonic()

        for i, trade_date in enumerate(trading_days):
            if self.store.exists(trade_date):
                skipped_existing += 1
                continue

            last_exc: Exception | None = None
            for attempt, delay in enumerate((*_RETRY_DELAYS, None), start=1):
                try:
                    df = self.fetch(trade_date)
                    last_exc = None
                    break
                except Exception as exc:
                    last_exc = exc
                    if delay is None:
                        break
                    logger.warning(
                        f"{self.spec.name}: fetch failed on {trade_date} "
                        f"(attempt {attempt}), retry in {delay}s: {exc}"
                    )
                    time.sleep(delay)
            if last_exc is not None:
                logger.error(f"{self.spec.name}: fetch failed on {trade_date} after {attempt} attempts: {last_exc}")
                rt.notifier.send(
                    f"{self.spec.name} 同步失败\n"
                    f"日期：{trade_date}｜{last_exc}"
                )
                raise last_exc

            time.sleep(0.2)

            if df is not None and not df.empty:
                self.store.write(trade_date, df)
                success += 1
                total_rows += len(df)
            elif self.write_empty:
                self.store.write(trade_date, df if df is not None else pd.DataFrame())
                empty += 1
            else:
                empty += 1

            if current_meta is None or trade_date > current_meta:
                rt.meta.update_last_date(self.spec.name, trade_date)
                current_meta = trade_date

            if (i + 1) % PROGRESS_INTERVAL == 0:
                processed = i + 1
                total = len(trading_days)
                elapsed = time.monotonic() - started_at
                percent = processed / total * 100 if total else 100.0
                logger.info(
                    f"{self.spec.name}: progress {processed}/{total} ({percent:.1f}%) "
                    f"success={success} empty={empty} skipped={skipped_existing} "
                    f"elapsed={_format_duration(elapsed)} "
                    f"eta={_estimate_eta(elapsed, processed, total)}"
                )

        total = len(trading_days)
        elapsed = time.monotonic() - started_at
        logger.info(
            f"{self.spec.name}: done total={total} "
            f"success={success} empty={empty} skipped={skipped_existing} "
            f"elapsed={_format_duration(elapsed)}"
        )
        date_range = (
            f"{trading_days[0]} ~ {trading_days[-1]}" if len(trading_days) > 1
            else trading_days[0]
        )
        rt.notifier.send(
            f"{self.spec.name} 同步完成\n"
            f"日期：{date_range}\n"
            f"写入 {success} 天 / {total_rows} 条记录｜空 {empty}｜已存在 {skipped_existing}｜耗时 {_format_duration(elapsed)}"
        )


class CalendarDateSyncJob(SyncJob):
    def __init__(
        self,
        table_name: str,
        spec: DailyTableSpec,
        fetch: Callable[[str], pd.DataFrame],
        store: DailyPartitionStore,
        write_empty: bool = True,
        supports_date_range: bool = True,
    ):
        self.table_name = table_name
        self.spec = spec
        self.fetch = fetch
        self.store = store
        self.write_empty = write_empty
        self.supports_date_range = supports_date_range

    def run(
        self,
        rt: SyncRuntime,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> None:
        today = rt.calendar.today()
        last = rt.meta.get_last_date(self.spec.name)

        if start_date is None:
            start = dateutil.add_days(last, 1) if last is not None else self.spec.first_date
            end = today
            if start > end:
                logger.info(f"{self.spec.name}: 已是最新 (last={last})")
                return
        else:
            start = start_date
            end = end_date if end_date is not None else today
            if start > end:
                raise ValueError(f"start_date {start} is after end_date {end}")

        days = _date_range_days(start, end)
        logger.info(f"{self.spec.name}: start {start} ~ {end}, days={len(days)}")

        success = 0
        total_rows = 0
        empty = 0
        skipped_existing = 0
        current_meta = last
        started_at = time.monotonic()

        for i, ann_date in enumerate(days):
            if self.store.exists(ann_date):
                skipped_existing += 1
                if current_meta is None or ann_date > current_meta:
                    rt.meta.update_last_date(self.spec.name, ann_date)
                    current_meta = ann_date
                continue

            last_exc: Exception | None = None
            for attempt, delay in enumerate((*_RETRY_DELAYS, None), start=1):
                try:
                    df = self.fetch(ann_date)
                    last_exc = None
                    break
                except Exception as exc:
                    last_exc = exc
                    if delay is None:
                        break
                    logger.warning(
                        f"{self.spec.name}: fetch failed on {ann_date} "
                        f"(attempt {attempt}), retry in {delay}s: {exc}"
                    )
                    time.sleep(delay)
            if last_exc is not None:
                logger.error(f"{self.spec.name}: fetch failed on {ann_date} after {attempt} attempts: {last_exc}")
                rt.notifier.send(
                    f"{self.spec.name} 同步失败\n"
                    f"日期：{ann_date}｜{last_exc}"
                )
                raise last_exc

            time.sleep(0.2)

            if df is not None and not df.empty:
                self.store.write(ann_date, df)
                success += 1
                total_rows += len(df)
            elif self.write_empty:
                self.store.write(ann_date, df if df is not None else pd.DataFrame())
                empty += 1
            else:
                empty += 1

            if current_meta is None or ann_date > current_meta:
                rt.meta.update_last_date(self.spec.name, ann_date)
                current_meta = ann_date

            if (i + 1) % PROGRESS_INTERVAL == 0:
                processed = i + 1
                elapsed = time.monotonic() - started_at
                percent = processed / len(days) * 100 if days else 100.0
                logger.info(
                    f"{self.spec.name}: progress {processed}/{len(days)} ({percent:.1f}%) "
                    f"success={success} empty={empty} skipped={skipped_existing} "
                    f"elapsed={_format_duration(elapsed)} "
                    f"eta={_estimate_eta(elapsed, processed, len(days))}"
                )

        elapsed = time.monotonic() - started_at
        logger.info(
            f"{self.spec.name}: done total={len(days)} "
            f"success={success} empty={empty} skipped={skipped_existing} "
            f"elapsed={_format_duration(elapsed)}"
        )
        date_range = (
            f"{days[0]} ~ {days[-1]}" if len(days) > 1
            else days[0]
        )
        rt.notifier.send(
            f"{self.spec.name} 同步完成\n"
            f"日期：{date_range}\n"
            f"写入 {success} 天 / {total_rows} 条记录｜空 {empty}｜已存在 {skipped_existing}｜耗时 {_format_duration(elapsed)}"
        )


class TickerSyncJob(SyncJob):
    """Sync a daily-partitioned table by iterating over tickers (ETF codes).

    Used when the API only supports per-ticker date-range queries
    (relay/proxy mode) rather than per-date bulk queries.
    """

    def __init__(
        self,
        table_name: str,
        spec: DailyTableSpec,
        fetch_range: Callable[[str, str, str], pd.DataFrame],
        store: DailyPartitionStore,
        get_tickers: Callable[[], list[str]],
        ticker_sleep: float = 0.35,
        supports_date_range: bool = True,
    ):
        self.table_name = table_name
        self.spec = spec
        self.fetch_range = fetch_range
        self.store = store
        self.get_tickers = get_tickers
        self.ticker_sleep = ticker_sleep
        self.supports_date_range = supports_date_range

    def run(
        self,
        rt: SyncRuntime,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> None:
        today = rt.calendar.today()
        if start_date is None:
            last = rt.meta.get_last_date(self.spec.name)
            start = dateutil.add_days(last, 1) if last is not None else self.spec.first_date
            end = today
            if start > end:
                logger.info(f"{self.spec.name}: 已是最新 (last={last})")
                return
        else:
            start = start_date
            end = end_date if end_date is not None else today
            if start > end:
                raise ValueError(f"start_date {start} is after end_date {end}")

        trading_days = rt.calendar.get_trading_days("SSE", start, end)
        if not trading_days:
            return

        logger.info(
            f"{self.spec.name} (ticker mode): start {start} ~ {end}, "
            f"trading_days={len(trading_days)}"
        )

        tickers = self.get_tickers()
        logger.info(f"{self.spec.name}: {len(tickers)} tickers to fetch")

        all_frames: list[pd.DataFrame] = []
        ticker_ok = ticker_empty = 0
        started_at = time.monotonic()

        for i, tc in enumerate(tickers):
            try:
                df = self.fetch_range(tc, start, end)
                if df is not None and not df.empty:
                    all_frames.append(df)
                    ticker_ok += 1
                else:
                    ticker_empty += 1
            except Exception as exc:
                logger.warning(f"{self.spec.name}: {tc} fetch failed: {exc}")
                ticker_empty += 1

            if self.ticker_sleep > 0:
                time.sleep(self.ticker_sleep)

            if (i + 1) % min(100, max(1, len(tickers) // 10)) == 0:
                rows = sum(len(f) for f in all_frames)
                elapsed = time.monotonic() - started_at
                logger.info(
                    f"{self.spec.name}: ticker progress {i+1}/{len(tickers)} "
                    f"ok={ticker_ok} empty={ticker_empty} rows={rows} "
                    f"elapsed={_format_duration(elapsed)}"
                )

        elapsed = time.monotonic() - started_at
        if not all_frames:
            logger.info(f"{self.spec.name}: no data fetched, elapsed={_format_duration(elapsed)}")
            rt.notifier.send(f"{self.spec.name} 同步完成 (ticker mode)\n无数据")
            return

        combined = pd.concat(all_frames, ignore_index=True)
        if "trade_date" in combined.columns:
            combined["_date"] = combined["trade_date"].astype(str).str.replace("-", "")
            combined = combined.drop_duplicates(subset=["ts_code", "trade_date"])

            written = 0
            for date_val, grp in combined.groupby("_date", sort=True):
                grp = grp.drop(columns=["_date"], errors="ignore")
                self.store.write(date_val, grp)
                written += 1

            logger.info(
                f"{self.spec.name}: done total={len(tickers)} tickers, "
                f"written={written} partitions, {len(combined)} rows, "
                f"elapsed={_format_duration(elapsed)}"
            )
            rt.notifier.send(
                f"{self.spec.name} 同步完成 (ticker mode)\n"
                f"区间：{start} ~ {end}｜写入 {written} 天 / {len(combined)} 条"
            )
        else:
            logger.error(f"{self.spec.name}: no trade_date column in fetched data")


class SnapshotSyncJob(SyncJob):
    def __init__(
        self,
        table_name: str,
        spec: TableSpec,
        fetch: Callable[[], pd.DataFrame],
        store: SnapshotStore,
        skip_non_trading: bool = True,
        exchange: str = "SSE",
        supports_date_range: bool = False,
    ):
        self.table_name = table_name
        self.spec = spec
        self.fetch = fetch
        self.store = store
        self.skip_non_trading = skip_non_trading
        self.exchange = exchange
        self.supports_date_range = supports_date_range

    def run(
        self,
        rt: SyncRuntime,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> None:
        if self.skip_non_trading and rt.calendar.skip_if_not_trading(self.exchange):
            return

        today = rt.calendar.today()

        try:
            df = self.fetch()
        except Exception as exc:
            logger.error(f"{self.spec.name}: fetch failed: {exc}")
            rt.notifier.send(f"{self.spec.name} 同步失败\n{exc}")
            raise

        self.store.write(df)
        rt.meta.update_last_date(self.spec.name, today)
        logger.info(f"{self.spec.name}: snapshot written ({len(df)} rows)")
        rt.notifier.send(
            f"{self.spec.name} 同步完成\n"
            f"日期：{today}｜{len(df)} 行"
        )
