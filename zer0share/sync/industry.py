from loguru import logger
from zer0share.storage import SnapshotStore, DailyPartitionStore
from zer0share.sync import SyncRuntime
from zer0share.sync._jobs import SnapshotSyncJob, SyncJob
from zer0share.catalog import SW_CLASSIFY_SPEC, SW_MEMBER_SPEC, SW_DAILY_SPEC, CI_MEMBER_SPEC
import zer0share.dateutil as dateutil
import pandas as pd
import time

# SW2021 L1 industry codes (31 industries)
SW_L1_CODES = [
    '801010.SI', '801030.SI', '801040.SI', '801050.SI', '801080.SI', '801880.SI',
    '801110.SI', '801120.SI', '801130.SI', '801140.SI', '801150.SI', '801160.SI',
    '801170.SI', '801180.SI', '801200.SI', '801210.SI', '801780.SI', '801790.SI',
    '801230.SI', '801710.SI', '801720.SI', '801730.SI', '801890.SI', '801740.SI',
    '801750.SI', '801760.SI', '801770.SI', '801950.SI', '801960.SI', '801970.SI',
    '801980.SI',
]


class IndustrySyncJob(SyncJob):
    table_name = "industry"
    supports_date_range = False

    def __init__(self, fetch_classify, fetch_member, store_classify: SnapshotStore, store_member: SnapshotStore):
        self._fetch_classify = fetch_classify
        self._fetch_member = fetch_member
        self._store_classify = store_classify
        self._store_member = store_member

    def run(self, rt: SyncRuntime, start_date=None, end_date=None) -> None:
        if rt.calendar.skip_if_not_trading("SSE"):
            return
        today = rt.calendar.today()
        try:
            df_classify = self._fetch_classify()
            self._store_classify.write(df_classify)
            rt.meta.update_last_date("sw_classify", today)
            logger.info(f"sw_classify 同步完成: {len(df_classify)} 条")

            df_member = self._fetch_member()
            self._store_member.write(df_member)
            rt.meta.update_last_date("sw_member", today)
            logger.info(f"sw_member 同步完成: {len(df_member)} 条")

            rt.notifier.send_success(
                f"industry 同步完成\n"
                f"日期：{today}｜分类 {len(df_classify)} 条｜成分 {len(df_member)} 条"
            )
        except Exception as e:
            logger.error(f"industry 同步失败: {e}")
            rt.notifier.send(f"industry 同步失败\n{e}")
            raise


class SwDailySyncJob(SyncJob):
    table_name = "sw_daily"
    supports_date_range = True

    def __init__(self, fetch, store: DailyPartitionStore):
        self._fetch = fetch
        self._store = store

    def run(self, rt: SyncRuntime, start_date=None, end_date=None) -> None:
        today = rt.calendar.today()
        last = rt.meta.get_last_date("sw_daily")

        if start_date is None:
            start = dateutil.add_days(last, 1) if last else SW_DAILY_SPEC.first_date
            end = today
            if start > end:
                logger.info("sw_daily 已是最新，无需同步")
                return
        else:
            start = start_date
            end = end_date or today
            if start > end:
                raise ValueError("start_date must be on or before end_date")

        # Get all trading days in the date range
        trading_days = rt.calendar.get_trading_days("SSE", start, end)
        if not trading_days:
            logger.info(f"sw_daily {start} ~ {end} 无交易日")
            return

        logger.info(f"sw_daily 同步开始: {start} ~ {end}, {len(trading_days)} 个交易日")

        success = skipped_existing = total_rows = 0
        first_written = last_written = None
        failed_days = []

        for trade_date in trading_days:
            if self._store.exists(trade_date):
                skipped_existing += 1
                continue

            try:
                # Fetch all industries for this date at once
                df = self._fetch(ts_code=None, start_date=trade_date, end_date=trade_date)
                time.sleep(0.3)

                if df.empty:
                    logger.warning(f"sw_daily {trade_date} 无数据")
                    failed_days.append(trade_date)
                    continue

                self._store.write(trade_date, df.reset_index(drop=True))
                rt.meta.update_last_date("sw_daily", trade_date)
                total_rows += len(df)
                if first_written is None:
                    first_written = trade_date
                last_written = trade_date
                success += 1

                if success % 10 == 0:
                    logger.info(f"sw_daily 进度: {success}/{len(trading_days)} 天")

            except Exception as e:
                logger.error(f"sw_daily {trade_date} 拉取失败: {e}")
                failed_days.append(trade_date)

        date_range = (
            f"{first_written} ~ {last_written}" if success > 1
            else (first_written or end)
        )
        msg = (
            f"sw_daily 同步完成\n"
            f"日期：{date_range}\n"
            f"写入 {success} 天 / {total_rows} 条记录｜已存在 {skipped_existing}"
        )
        if failed_days:
            msg += f"\n失败天数: {len(failed_days)}"
        logger.info(msg)
        if failed_days:
            rt.notifier.send(msg)
        else:
            rt.notifier.send_success(msg)


def build_jobs(cfg, fetcher) -> list[SyncJob]:
    d = cfg.data_dir
    return [
        IndustrySyncJob(
            fetch_classify=fetcher.fetch_sw_classify,
            fetch_member=fetcher.fetch_sw_member,
            store_classify=SnapshotStore(d / "stock" / "industry" / "sw_classify" / "data.parquet"),
            store_member=SnapshotStore(d / "stock" / "industry" / "sw_member" / "data.parquet"),
        ),
        SwDailySyncJob(
            fetch=fetcher.fetch_sw_daily,
            store=DailyPartitionStore(d / "stock" / "industry" / "sw_daily"),
        ),
        SnapshotSyncJob(
            table_name=CI_MEMBER_SPEC.name,
            spec=CI_MEMBER_SPEC,
            fetch=fetcher.fetch_ci_member,
            store=SnapshotStore(d / "stock" / "industry" / "ci_member" / "data.parquet"),
        ),
    ]
