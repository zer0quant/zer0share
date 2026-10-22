from zer0share.config import Config
from zer0share.notifier import Notifier
from zer0share.sources import DataSources
from zer0share.storage import MetaStore
from zer0share.sync import SyncRuntime
from zer0share.sync._jobs import SyncJob
from zer0share.trading_calendar import TradingCalendar


class Pipeline:
    def __init__(self, cfg: Config, sources: DataSources, notifier: Notifier):
        try:
            meta = MetaStore(cfg.db_path)
        except Exception as exc:  # noqa: BLE001
            # DuckDB 单写锁: 同时跑两个 sync 进程时第二个在这里炸,
            # 裸 traceback 很难看出"是别的 sync 还在跑"
            msg = str(exc)
            if "lock" in msg.lower() or "Conflicting" in msg:
                raise RuntimeError(
                    f"MetaStore 数据库被占用 ({cfg.db_path})。\n"
                    "另一个 sync 进程正在运行 —— DuckDB 只允许单写连接。\n"
                    "请等它结束, 或用 `ps -eo pid,cmd | grep zer0share` 找到并"
                    "结束旧进程后重试。"
                ) from exc
            raise
        calendar = TradingCalendar(meta)
        self._runtime = SyncRuntime(calendar=calendar, notifier=notifier, meta=meta)
        self._registry: dict[str, SyncJob] = {}
        self._build_registry(cfg, sources)

    def _build_registry(self, cfg: Config, sources: DataSources) -> None:
        from zer0share.sync import calendar, stock, index, industry, futures, options, ricequant, etf
        from zer0share.sync._batch_jobs import build_batch_jobs, load_codes
        for module in [calendar, stock, index, industry, futures, options, etf]:
            for job in module.build_jobs(cfg, sources.tushare):
                self._registry[job.table_name] = job
        for job in ricequant.build_jobs(cfg, sources):
            self._registry[job.table_name] = job
        # 吸收 bulk 后的批量取数表: 原 bulk.build_tables 注册项, 现已改为
        # BatchSyncJob, 与 job 引擎共用同一套 run/游标/通知/日期区间语义。
        # 逐只拉取的表需要 ts_code 清单, 从 stock/basic 快照读取。
        codes = load_codes(cfg.data_dir)
        for job in build_batch_jobs(cfg, sources.tushare, codes):
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
