import time

import pandas as pd
from loguru import logger

import zer0share.dateutil as dateutil
from zer0share.catalog import IDX_ANNS_SPEC, INDEX_DAILY_SPEC, INDEX_WEIGHT_SPEC
from zer0share.sources.tushare import INDEX_DAILY_CODES
from zer0share.storage import DailyPartitionStore, IndexWeightStore
from zer0share.sync import SyncRuntime
from zer0share.sync._jobs import CalendarDateSyncJob, SyncJob

INDEX_CODES = ["399300.SZ", "000905.SH", "000852.SH"]


def _index_weight_meta_key(index_code: str) -> str:
    return f"index_weight:{index_code}"


class IndexWeightSyncJob(SyncJob):
    table_name = "index_weight"
    supports_date_range = True

    def __init__(self, fetch, store: IndexWeightStore):
        self._fetch = fetch
        self._store = store

    def run(self, rt: SyncRuntime, start_date=None, end_date=None) -> None:
        today = rt.calendar.today()
        end = end_date or today
        if start_date is not None and start_date > end:
            raise ValueError("start_date must be on or before end_date")

        success = empty_months = skipped_existing = requests = 0
        coverage_dates = []

        for index_code in INDEX_CODES:
            meta_key = _index_weight_meta_key(index_code)
            last = rt.meta.get_last_date(meta_key)
            start = start_date or (dateutil.add_days(last, 1) if last else INDEX_WEIGHT_SPEC.first_date)
            if start > end:
                logger.info(f"index_weight {index_code} 已覆盖到 {last}，无需同步")
                if last is not None:
                    coverage_dates.append(last)
                continue

            ranges = dateutil.month_ranges(start, end)
            logger.info(f"index_weight {index_code} 同步开始: {start} ~ {end}, 共 {len(ranges)} 个月度窗口")
            try:
                for processed, (month_start, month_end) in enumerate(ranges, start=1):
                    df = self._fetch(index_code, month_start, month_end)
                    requests += 1
                    time.sleep(0.2)
                    if df.empty:
                        empty_months += 1
                    else:
                        for trade_date_value, part in df.groupby("trade_date"):
                            trade_date = str(trade_date_value)
                            if self._store.exists(index_code, trade_date):
                                skipped_existing += 1
                                continue
                            self._store.write(index_code, trade_date, part)
                            success += 1
                    if processed == len(ranges) or processed % 50 == 0:
                        percent = processed / len(ranges) * 100
                        logger.info(
                            f"index_weight {index_code} 进度: {processed}/{len(ranges)} ({percent:.1f}%), "
                            f"成功 {success}, 空 {empty_months}, 跳过 {skipped_existing}"
                        )
                frontier = max(last, end) if last is not None else end
                rt.meta.update_last_date(meta_key, frontier)
                coverage_dates.append(frontier)
            except Exception as e:
                logger.error(f"index_weight {index_code} 同步失败: {e}")
                rt.notifier.send(f"index_weight 同步失败\n指数：{index_code}｜{e}")
                raise

        if coverage_dates:
            rt.meta.update_last_date("index_weight", min(coverage_dates))

        msg = (
            f"index_weight 同步完成\n"
            f"日期：{end}\n"
            f"写入 {success} 天｜空窗口 {empty_months}｜已存在 {skipped_existing}"
        )
        logger.info(msg)
        rt.notifier.send_success(msg)


class IndexDailySyncJob(SyncJob):
    table_name = "index_daily"
    supports_date_range = True

    def __init__(self, fetch, store: DailyPartitionStore):
        self._fetch = fetch
        self._store = store

    def run(self, rt: SyncRuntime, start_date=None, end_date=None) -> None:
        today = rt.calendar.today()
        last = rt.meta.get_last_date("index_daily")

        if start_date is None:
            start = dateutil.add_days(last, 1) if last else INDEX_DAILY_SPEC.first_date
            end = today
            if start > end:
                logger.info("index_daily 已是最新，无需同步")
                return
        else:
            start = start_date
            end = end_date or today
            if start > end:
                raise ValueError("start_date must be on or before end_date")

        logger.info(f"index_daily 同步开始: {start} ~ {end}, {len(INDEX_DAILY_CODES)} 个指数")
        all_frames = []
        for ts_code in INDEX_DAILY_CODES:
            try:
                df = self._fetch(ts_code, start, end)
                time.sleep(0.2)
                if not df.empty:
                    all_frames.append(df)
            except Exception as e:
                logger.error(f"index_daily {ts_code} 拉取失败: {e}")
                rt.notifier.send(f"index_daily 拉取失败\n指数：{ts_code}｜{e}")

        if not all_frames:
            msg = "index_daily 无数据，跳过"
            logger.info(msg)
            rt.notifier.send_success(msg)
            return

        combined = pd.concat(all_frames, ignore_index=True)
        success = skipped_existing = total_rows = 0
        frontier = last
        first_written = last_written = None

        for trade_date_value, part in combined.groupby("trade_date"):
            trade_date = str(trade_date_value)
            if self._store.exists(trade_date):
                skipped_existing += 1
                continue
            self._store.write(trade_date, part.reset_index(drop=True))
            if frontier is None or trade_date > frontier:
                rt.meta.update_last_date("index_daily", trade_date)
                frontier = trade_date
            total_rows += len(part)
            if first_written is None:
                first_written = trade_date
            last_written = trade_date
            success += 1

        date_range = (
            f"{first_written} ~ {last_written}" if success > 1
            else (first_written or end)
        )
        msg = (
            f"index_daily 同步完成\n"
            f"日期：{date_range}\n"
            f"写入 {success} 天 / {total_rows} 条记录｜已存在 {skipped_existing}"
        )
        logger.info(msg)
        rt.notifier.send_success(msg)


def build_jobs(cfg, fetcher) -> list[SyncJob]:
    d = cfg.data_dir
    return [
        IndexWeightSyncJob(
            fetch=fetcher.fetch_index_weight,
            store=IndexWeightStore(d / "index" / "index_weight"),
        ),
        IndexDailySyncJob(
            fetch=fetcher.fetch_index_daily,
            store=DailyPartitionStore(d / "index" / "index_daily"),
        ),
        CalendarDateSyncJob(
            table_name=IDX_ANNS_SPEC.name,
            spec=IDX_ANNS_SPEC,
            fetch=fetcher.fetch_idx_anns,
            store=DailyPartitionStore(d / "index" / "idx_anns"),
            write_empty=True,
        ),
    ]
