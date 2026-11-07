import time
import pandas as pd
from loguru import logger

from zer0share.sources.tushare import OPTIONS_EXCHANGES
from zer0share.storage import DailyPartitionStore, SnapshotStore
from zer0share.sync import SyncRuntime
from zer0share.sync._jobs import DailySyncJob, SyncJob
from zer0share.catalog import OPT_BASIC_SPEC, OPT_DAILY_SPEC


class OptBasicSyncJob(SyncJob):
    table_name = "opt_basic"
    supports_date_range = False

    def __init__(self, fetch, store: SnapshotStore):
        self._fetch = fetch
        self._store = store

    def run(self, rt: SyncRuntime, start_date=None, end_date=None) -> None:
        today = rt.calendar.today()
        all_frames = []
        try:
            for exchange in OPTIONS_EXCHANGES:
                df = self._fetch(exchange)
                time.sleep(0.2)
                if not df.empty:
                    all_frames.append(df)
            combined = pd.concat(all_frames, ignore_index=True) if all_frames else pd.DataFrame()
            self._store.write(combined)
            rt.meta.update_last_date("opt_basic", today)
            logger.info(f"opt_basic 同步完成: {len(combined)} 条")
            rt.notifier.send_success(f"opt_basic 同步完成\n日期：{today}｜{len(combined)} 条记录")
        except Exception as e:
            logger.error(f"opt_basic 同步失败: {e}")
            rt.notifier.send(f"opt_basic 同步失败\n{e}")
            raise


def build_jobs(cfg, fetcher) -> list[SyncJob]:
    od = cfg.data_dir / "options"
    return [
        OptBasicSyncJob(
            fetch=fetcher.fetch_opt_basic,
            store=SnapshotStore(od / "opt_basic" / "data.parquet"),
        ),
        DailySyncJob(
            table_name=OPT_DAILY_SPEC.name, spec=OPT_DAILY_SPEC,
            fetch=fetcher.fetch_opt_daily, store=DailyPartitionStore(od / "opt_daily"),
        ),
    ]
