from zer0share.config import Config
from zer0share.notifier import Notifier
from zer0share.sources import DataSources
from zer0share.storage import MetaStore
from zer0share.sync import SyncRuntime
from zer0share.sync._jobs import SyncJob
from zer0share.trading_calendar import TradingCalendar


class Pipeline:
    def __init__(self, cfg: Config, sources: DataSources, notifier: Notifier, ticker_mode: bool = False):
        meta = MetaStore(cfg.db_path)
        calendar = TradingCalendar(meta)
        self._runtime = SyncRuntime(calendar=calendar, notifier=notifier, meta=meta)
        self._registry: dict[str, SyncJob] = {}
        self._build_registry(cfg, sources, ticker_mode=ticker_mode)

    def _build_registry(self, cfg: Config, sources: DataSources, ticker_mode: bool = False) -> None:
        from zer0share.sync import calendar, stock, index, industry, futures, options, ricequant, etf
        for module in [calendar, stock, index, industry, futures, options]:
            for job in module.build_jobs(cfg, sources.tushare):
                self._registry[job.table_name] = job
        for job in etf.build_jobs(cfg, sources.tushare, ticker_mode=ticker_mode):
            self._registry[job.table_name] = job
        for job in ricequant.build_jobs(cfg, sources):
            self._registry[job.table_name] = job

    def run(self, table_name: str, start_date: str | None = None, end_date: str | None = None) -> None:
        if table_name not in self._registry:
            raise ValueError(f"未知表: {table_name}")
        self._registry[table_name].run(self._runtime, start_date, end_date)

    def run_all(self, start_date: str | None = None, end_date: str | None = None) -> None:
        for job in self._registry.values():
            job.run(self._runtime, start_date, end_date)

    @property
    def registry(self) -> dict[str, SyncJob]:
        return self._registry

    def close(self) -> None:
        self._runtime.meta.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
        return False
