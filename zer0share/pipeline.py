from datetime import date, timedelta
import time

import pandas as pd
from loguru import logger

from zer0share.config import Config
from zer0share.fetcher import TushareFetcher, INDEX_DAILY_CODES
from zer0share.notifier import Notifier
from zer0share.storage import (
    MetaStore,
    adj_factor_partition_exists,
    daily_partition_exists,
    daily_kline_partition_exists,
    index_weight_partition_exists,
    read_trade_cal,
    write_adj_factor,
    write_basic,
    write_ci_member,
    write_daily_partition,
    write_daily_kline,
    write_index_weight,
    write_sw_classify,
    write_sw_member,
    write_trade_cal,
)


FIRST_DATE = date(2016, 1, 1)
TRADE_CAL_FIRST_DATE = date(1990, 1, 1)
PROGRESS_INTERVAL = 50
EXCHANGES = ["SSE", "SZSE"]
INDEX_CODES = ["399300.SZ", "000905.SH", "000852.SH"]


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


def _should_log_progress(processed: int, total: int) -> bool:
    return processed == total or processed % PROGRESS_INTERVAL == 0


def _log_daily_progress(
    table_name: str,
    processed: int,
    total: int,
    trade_date: date,
    success: int,
    empty: int,
    skipped_existing: int,
) -> None:
    percent = processed / total * 100
    logger.info(
        f"{table_name} 同步进度: {processed}/{total} ({percent:.1f}%), "
        f"当前日期 {trade_date}, "
        f"成功 {success} 天, 空数据 {empty} 天, 跳过已存在 {skipped_existing} 天"
    )


def _month_ranges(start: date, end: date) -> list[tuple[date, date]]:
    ranges = []
    current = date(start.year, start.month, 1)
    while current <= end:
        next_month = (
            date(current.year + 1, 1, 1)
            if current.month == 12
            else date(current.year, current.month + 1, 1)
        )
        month_start = max(start, current)
        month_end = min(end, next_month - timedelta(days=1))
        ranges.append((month_start, month_end))
        current = next_month
    return ranges


def _index_weight_meta_key(index_code: str) -> str:
    return f"index_weight:{index_code}"


class Pipeline:
    def __init__(self, cfg: Config, fetcher: TushareFetcher, notifier: Notifier):
        self._cfg = cfg
        self._fetcher = fetcher
        self._notifier = notifier
        self._meta = MetaStore(cfg.db_path)

    def sync_basic(self) -> None:
        today = date.today()
        try:
            df = self._fetcher.fetch_basic()
            write_basic(self._cfg.data_dir, df)
            self._meta.update_last_date("basic", today)
            logger.info(f"basic 同步完成: {len(df)} 条")
        except Exception as e:
            logger.error(f"basic 同步失败: {e}")
            self._notifier.send(f"basic 同步失败: {e}")
            raise

    def sync_trade_cal(self) -> None:
        try:
            end = date(date.today().year, 12, 31)
            max_dates: list[date] = []
            for exchange in EXCHANGES:
                existing = read_trade_cal(self._cfg.data_dir, exchange)
                last = (
                    existing["cal_date"].max()
                    if not existing.empty
                    else None
                )
                start = (last + timedelta(days=1)) if last else TRADE_CAL_FIRST_DATE

                if start <= end:
                    fetched = self._fetcher.fetch_trade_cal(exchange, start, end)
                    df = _merge_trade_cal(existing, fetched)
                    write_trade_cal(self._cfg.data_dir, exchange, df)
                    logger.info(
                        f"trade_cal {exchange} 写入完成: 新增 {len(fetched)} 条, "
                        f"共 {len(df)} 条"
                    )
                else:
                    df = existing
                    logger.info(f"trade_cal {exchange} 已覆盖到 {last}，无需同步")

                if not df.empty:
                    max_dates.append(df["cal_date"].max())

            self._meta.load_trade_cal_from_parquet(self._cfg.data_dir, EXCHANGES)
            if max_dates:
                self._meta.update_last_date("trade_cal", min(max_dates))
            logger.info("trade_cal 全部同步完成")
        except Exception as e:
            logger.error(f"trade_cal 同步失败: {e}")
            self._notifier.send(f"trade_cal 同步失败: {e}")
            raise

    def sync_daily_kline(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> None:
        today = date.today()
        last = self._meta.get_last_date("daily_kline")

        if start_date is None:
            start = (last + timedelta(days=1)) if last else FIRST_DATE
            end = today
        else:
            start = start_date
            end = end_date or today

        if start_date is None and start > end:
            logger.info("daily_kline 已是最新，无需同步")
            return

        if start > end:
            raise ValueError("start_date must be on or before end_date")

        trading_days = self._meta.get_trading_days("SSE", start, end)
        if not trading_days and self._meta.get_last_date("trade_cal") is None:
            raise RuntimeError(
                "DuckDB 中无 SSE trade_cal 数据，请先运行 "
                "python main.py sync --table trade_cal"
            )

        if not trading_days:
            logger.info("指定范围内无交易日，无需同步")
            return

        success = 0
        empty = 0
        skipped_existing = 0
        frontier = last

        logger.info(
            f"daily_kline 同步开始: {start} ~ {end}, 共 {len(trading_days)} 个交易日"
        )
        for processed, trade_date in enumerate(trading_days, start=1):
            if daily_kline_partition_exists(self._cfg.data_dir, trade_date):
                skipped_existing += 1
                if _should_log_progress(processed, len(trading_days)):
                    _log_daily_progress(
                        "daily_kline",
                        processed,
                        len(trading_days),
                        trade_date,
                        success,
                        empty,
                        skipped_existing,
                    )
                continue
            try:
                df = self._fetcher.fetch_daily_kline(trade_date)
                time.sleep(0.2)
                if not df.empty:
                    write_daily_kline(self._cfg.data_dir, trade_date, df)
                    if frontier is None or trade_date > frontier:
                        self._meta.update_last_date("daily_kline", trade_date)
                        frontier = trade_date
                    success += 1
                else:
                    empty += 1
            except Exception as e:
                logger.error(f"daily_kline {trade_date} 同步失败: {e}")
                self._notifier.send(f"daily_kline {trade_date} 同步失败: {e}")
                raise
            if _should_log_progress(processed, len(trading_days)):
                _log_daily_progress(
                    "daily_kline",
                    processed,
                    len(trading_days),
                    trade_date,
                    success,
                    empty,
                    skipped_existing,
                )

        msg = (
            f"daily_kline 同步完成: 成功 {success} 天, "
            f"空数据 {empty} 天, 跳过已存在 {skipped_existing} 天, "
            f"共 {len(trading_days)} 个交易日"
        )
        logger.info(msg)
        self._notifier.send(msg)

    def sync_adj_factor(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> None:
        today = date.today()
        last = self._meta.get_last_date("adj_factor")

        if start_date is None:
            start = (last + timedelta(days=1)) if last else FIRST_DATE
            end = today
        else:
            start = start_date
            end = end_date or today

        if start_date is None and start > end:
            logger.info("adj_factor 已是最新，无需同步")
            return

        if start > end:
            raise ValueError("start_date must be on or before end_date")

        trading_days = self._meta.get_trading_days("SSE", start, end)
        if not trading_days and self._meta.get_last_date("trade_cal") is None:
            raise RuntimeError(
                "DuckDB 中无 SSE trade_cal 数据，请先运行 "
                "python main.py sync --table trade_cal"
            )

        if not trading_days:
            logger.info("指定范围内无交易日，无需同步")
            return

        success = 0
        empty = 0
        skipped_existing = 0
        frontier = last

        logger.info(
            f"adj_factor 同步开始: {start} ~ {end}, 共 {len(trading_days)} 个交易日"
        )
        for processed, trade_date in enumerate(trading_days, start=1):
            if adj_factor_partition_exists(self._cfg.data_dir, trade_date):
                skipped_existing += 1
                if _should_log_progress(processed, len(trading_days)):
                    _log_daily_progress(
                        "adj_factor",
                        processed,
                        len(trading_days),
                        trade_date,
                        success,
                        empty,
                        skipped_existing,
                    )
                continue
            try:
                df = self._fetcher.fetch_adj_factor(trade_date)
                time.sleep(0.2)
                if not df.empty:
                    write_adj_factor(self._cfg.data_dir, trade_date, df)
                    if frontier is None or trade_date > frontier:
                        self._meta.update_last_date("adj_factor", trade_date)
                        frontier = trade_date
                    success += 1
                else:
                    empty += 1
            except Exception as e:
                logger.error(f"adj_factor {trade_date} 同步失败: {e}")
                self._notifier.send(f"adj_factor {trade_date} 同步失败: {e}")
                raise
            if _should_log_progress(processed, len(trading_days)):
                _log_daily_progress(
                    "adj_factor",
                    processed,
                    len(trading_days),
                    trade_date,
                    success,
                    empty,
                    skipped_existing,
                )

        msg = (
            f"adj_factor 同步完成: 成功 {success} 天, "
            f"空数据 {empty} 天, 跳过已存在 {skipped_existing} 天, "
            f"共 {len(trading_days)} 个交易日"
        )
        logger.info(msg)
        self._notifier.send(msg)

    def sync_daily_basic(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> None:
        self._sync_daily_partitioned(
            table_name="daily_basic",
            fetch=self._fetcher.fetch_daily_basic,
            start_date=start_date,
            end_date=end_date,
        )

    def sync_stock_st(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> None:
        self._sync_daily_partitioned(
            table_name="stock_st",
            fetch=self._fetcher.fetch_stock_st,
            start_date=start_date,
            end_date=end_date,
            write_empty=True,
        )

    def sync_suspend_d(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> None:
        self._sync_daily_partitioned(
            table_name="suspend_d",
            fetch=self._fetcher.fetch_suspend_d,
            start_date=start_date,
            end_date=end_date,
            write_empty=True,
        )

    def sync_stk_limit(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> None:
        self._sync_daily_partitioned(
            table_name="stk_limit",
            fetch=self._fetcher.fetch_stk_limit,
            start_date=start_date,
            end_date=end_date,
        )

    def sync_index_weight(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> None:
        today = date.today()
        end = end_date or today
        if start_date is not None and start_date > end:
            raise ValueError("start_date must be on or before end_date")

        success = 0
        skipped_existing = 0
        empty_months = 0
        requests = 0
        coverage_dates: list[date] = []
        for index_code in INDEX_CODES:
            meta_key = _index_weight_meta_key(index_code)
            last = self._meta.get_last_date(meta_key)
            start = start_date or ((last + timedelta(days=1)) if last else FIRST_DATE)
            if start > end:
                logger.info(f"index_weight {index_code} 已覆盖到 {last}，无需同步")
                if last is not None:
                    coverage_dates.append(last)
                continue

            month_ranges = _month_ranges(start, end)
            logger.info(
                f"index_weight {index_code} 同步开始: {start} ~ {end}, "
                f"共 {len(month_ranges)} 个月度窗口"
            )
            try:
                for processed, (month_start, month_end) in enumerate(
                    month_ranges, start=1
                ):
                    df = self._fetcher.fetch_index_weight(
                        index_code, month_start, month_end
                    )
                    requests += 1
                    time.sleep(0.2)
                    if df.empty:
                        empty_months += 1
                    else:
                        for trade_date, part in df.groupby("trade_date"):
                            if index_weight_partition_exists(
                                self._cfg.data_dir, index_code, trade_date
                            ):
                                skipped_existing += 1
                                continue
                            write_index_weight(
                                self._cfg.data_dir, index_code, trade_date, part
                            )
                            success += 1

                    if _should_log_progress(processed, len(month_ranges)):
                        percent = processed / len(month_ranges) * 100
                        logger.info(
                            f"index_weight {index_code} 同步进度: "
                            f"{processed}/{len(month_ranges)} ({percent:.1f}%), "
                            f"当前窗口 {month_start} ~ {month_end}, "
                            f"成功 {success} 个分区, 空窗口 {empty_months} 个, "
                            f"跳过已存在 {skipped_existing} 个分区"
                        )

                frontier = max(last, end) if last is not None else end
                self._meta.update_last_date(meta_key, frontier)
                coverage_dates.append(frontier)
            except Exception as e:
                logger.error(f"index_weight {index_code} 同步失败: {e}")
                self._notifier.send(f"index_weight {index_code} 同步失败: {e}")
                raise

        if coverage_dates:
            self._meta.update_last_date("index_weight", min(coverage_dates))

        msg = (
            f"index_weight 同步完成: 成功 {success} 个分区, "
            f"空窗口 {empty_months} 个, 跳过已存在 {skipped_existing} 个分区, "
            f"请求 {requests} 次"
        )
        logger.info(msg)
        self._notifier.send(msg)

    def sync_index_daily(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> None:
        today = date.today()
        last = self._meta.get_last_date("index_daily")

        if start_date is None:
            start = (last + timedelta(days=1)) if last else FIRST_DATE
            end = today
        else:
            start = start_date
            end = end_date or today

        if start_date is None and start > end:
            logger.info("index_daily 已是最新，无需同步")
            return
        if start > end:
            raise ValueError("start_date must be on or before end_date")

        logger.info(
            f"index_daily 同步开始: {start} ~ {end}, 共 {len(INDEX_DAILY_CODES)} 个指数"
        )
        all_frames = []
        for ts_code in INDEX_DAILY_CODES:
            try:
                df = self._fetcher.fetch_index_daily(ts_code, start, end)
                time.sleep(0.2)
                if not df.empty:
                    all_frames.append(df)
            except Exception as e:
                logger.error(f"index_daily {ts_code} 拉取失败: {e}")
                self._notifier.send(f"index_daily {ts_code} 拉取失败: {e}")
                continue

        if not all_frames:
            msg = "index_daily 无数据，跳过"
            logger.info(msg)
            self._notifier.send(msg)
            return

        combined = pd.concat(all_frames, ignore_index=True)
        success = 0
        skipped_existing = 0
        frontier = last

        for trade_date, part in combined.groupby("trade_date"):
            if daily_partition_exists(self._cfg.data_dir, "index_daily", trade_date):
                skipped_existing += 1
                continue
            write_daily_partition(
                self._cfg.data_dir, "index_daily", trade_date, part.reset_index(drop=True)
            )
            if frontier is None or trade_date > frontier:
                self._meta.update_last_date("index_daily", trade_date)
                frontier = trade_date
            success += 1

        msg = (
            f"index_daily 同步完成: 成功 {success} 天, "
            f"跳过已存在 {skipped_existing} 天, 共 {len(INDEX_DAILY_CODES)} 个指数"
        )
        logger.info(msg)
        self._notifier.send(msg)

    def sync_industry(self) -> None:
        today = date.today()
        try:
            df = self._fetcher.fetch_sw_classify()
            write_sw_classify(self._cfg.data_dir, df)
            self._meta.update_last_date("sw_classify", today)
            logger.info(f"sw_classify 同步完成: {len(df)} 条")

            df = self._fetcher.fetch_sw_member()
            write_sw_member(self._cfg.data_dir, df)
            self._meta.update_last_date("sw_member", today)
            logger.info(f"sw_member 同步完成: {len(df)} 条")
        except Exception as e:
            logger.error(f"industry 同步失败: {e}")
            self._notifier.send(f"industry 同步失败: {e}")
            raise

    def sync_ci_member(self) -> None:
        today = date.today()
        try:
            df = self._fetcher.fetch_ci_member()
            write_ci_member(self._cfg.data_dir, df)
            self._meta.update_last_date("ci_member", today)
            logger.info(f"ci_member 同步完成: {len(df)} 条")
        except Exception as e:
            logger.error(f"ci_member 同步失败: {e}")
            self._notifier.send(f"ci_member 同步失败: {e}")
            raise

    def _sync_daily_partitioned(
        self,
        table_name: str,
        fetch,
        start_date: date | None,
        end_date: date | None,
        write_empty: bool = False,
    ) -> None:
        today = date.today()
        last = self._meta.get_last_date(table_name)
        if start_date is None:
            start = (last + timedelta(days=1)) if last else FIRST_DATE
            end = today
        else:
            start = start_date
            end = end_date or today

        if start_date is None and start > end:
            logger.info(f"{table_name} 已是最新，无需同步")
            return
        if start > end:
            raise ValueError("start_date must be on or before end_date")

        trading_days = self._meta.get_trading_days("SSE", start, end)
        if not trading_days and self._meta.get_last_date("trade_cal") is None:
            raise RuntimeError(
                "DuckDB 中无 SSE trade_cal 数据，请先运行 "
                "python main.py sync --table trade_cal"
            )
        if not trading_days:
            logger.info("指定范围内无交易日，无需同步")
            return

        success = 0
        empty = 0
        skipped_existing = 0
        frontier = last
        logger.info(
            f"{table_name} 同步开始: {start} ~ {end}, 共 {len(trading_days)} 个交易日"
        )
        for processed, trade_date in enumerate(trading_days, start=1):
            if daily_partition_exists(self._cfg.data_dir, table_name, trade_date):
                skipped_existing += 1
                if _should_log_progress(processed, len(trading_days)):
                    _log_daily_progress(
                        table_name,
                        processed,
                        len(trading_days),
                        trade_date,
                        success,
                        empty,
                        skipped_existing,
                    )
                continue
            try:
                df = fetch(trade_date)
                time.sleep(0.2)
                if not df.empty or write_empty:
                    write_daily_partition(self._cfg.data_dir, table_name, trade_date, df)
                    if frontier is None or trade_date > frontier:
                        self._meta.update_last_date(table_name, trade_date)
                        frontier = trade_date
                    if df.empty:
                        empty += 1
                    else:
                        success += 1
                else:
                    empty += 1
            except Exception as e:
                logger.error(f"{table_name} {trade_date} 同步失败: {e}")
                self._notifier.send(f"{table_name} {trade_date} 同步失败: {e}")
                raise
            if _should_log_progress(processed, len(trading_days)):
                _log_daily_progress(
                    table_name,
                    processed,
                    len(trading_days),
                    trade_date,
                    success,
                    empty,
                    skipped_existing,
                )

        msg = (
            f"{table_name} 同步完成: 成功 {success} 天, "
            f"空数据 {empty} 天, 跳过已存在 {skipped_existing} 天, "
            f"共 {len(trading_days)} 个交易日"
        )
        logger.info(msg)
        self._notifier.send(msg)

    def close(self) -> None:
        self._meta.close()

    def __enter__(self) -> "Pipeline":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self.close()
        return False
