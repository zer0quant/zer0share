"""
sync_runner.py — 统一 `sync` 命令的执行层。

所有表都是 zer0share.pipeline.Pipeline 里的 SyncJob (逐日型 DailySyncJob,
或吸收 bulk 能力后的批量型 BatchSyncJob), 由同一套 run/游标/重试/通知驱动。
原先按"bulk / job 两个引擎"分流的逻辑已随 bulk.py 删除而取消。
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from types import SimpleNamespace

from loguru import logger

from zer0share import sync_plan as SP


class UnknownTable(ValueError):
    """表名在引擎 registry 中不存在。

    最常见原因是 config 里对应数据源 enabled=false (如 [ricequant]),
    build_jobs 返回空; 其次是表名拼错。CLI 层转成 UsageError, 给用户
    可操作的提示而不是裸 traceback。
    """


class DateRangeNotSupported(ValueError):
    """表不支持 --start-date/--end-date (快照表 / 多交易所表)。"""


@dataclass
class SyncReport:
    """一次统一 sync 的执行结果。"""
    done: list[str] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.failed

    def summary(self) -> str:
        parts = [f"完成 {len(self.done)} 张"]
        if self.skipped:
            parts.append(f"跳过 {len(self.skipped)} 张")
        if self.failed:
            parts.append(f"失败 {len(self.failed)} 张")
        return "｜".join(parts)


def run_job_tables(cfg, tables: list[str], *, start_date: str | None = None,
                   end_date: str | None = None,
                   dry_run: bool = False,
                   strict: bool = False,
                   pipeline_factory=None) -> tuple[list[str], list[str]]:
    """跑 job 引擎 (zer0share.sync) 负责的表。返回 (成功, 失败)。

    pipeline_factory: 可选的 pipeline 工厂 (默认自建)。CLI 传入 zer0share.cli
    的 _make_pipeline, 这样测试可以 patch 该入口, 也让 job 引擎与 CLI 共用
    同一套配置/数据源构造逻辑 ( RiceQuant 开关、notifier 等)。

    dry_run: Pipeline.run() 原生不支持 dry-run (job 里直接 fetch + 写盘),
    所以在 runner 层拦下: 只打印「将要执行哪些表的哪个日期区间」, 不建
    pipeline、不联网、不写盘。与 bulk 引擎的 --dry-run 语义保持一致。
    """
    done: list[str] = []
    failed: list[str] = []

    if dry_run:
        span = f"{start_date or '自动起点'} ~ {end_date or '自动终点'}"
        logger.info(f"  [dry-run] job 引擎 {len(tables)} 张表, 区间 {span}")
        # 与真实执行保持同一套前置校验, 否则 --dry-run 会漏掉错误, 用户到
        # 真跑时才发现:
        #   1) 快照表给日期区间必须报错;
        #   2) 表名在 job registry 里必须真实存在 (例如 RiceQuant 在 config
        #      关闭时 build_jobs 返回空, 若不校验会谎报"将执行 N 张")。
        # 这里只读 job 的静态属性, 因此用轻量方式构造 registry —— 不开
        # MetaStore/DuckDB, 也不联网。
        reg = _lightweight_registry(cfg)
        missing = [t for t in tables if t not in reg]
        if missing:
            if strict:
                raise UnknownTable(_missing_table_hint(missing, reg))
            # 非 strict: 剔除并提示。通常是 config 中数据源 enabled=false
            # (如 [ricequant]), --all/--topic 展开时不该为它中断整批。
            logger.info(f"⏭ 跳过未注册表 {missing}: "
                        f"{_missing_table_hint(missing, reg)}")
            tables = [t for t in tables if t in reg]
            if not tables:
                logger.info("  [dry-run] job 引擎无可执行表")
                return [], failed
        if start_date is not None or end_date is not None:
            _check_date_range_support(reg, tables, start_date, end_date)
        for t in tables:
            logger.info(f"  [dry-run]   将执行: {t}")
        return list(tables), failed

    if pipeline_factory is not None:
        with pipeline_factory() as pipeline:
            return _run_job_tables_with(pipeline, tables,
                                         start_date=start_date, end_date=end_date,
                                         strict=strict)

    from zer0share.pipeline import Pipeline
    from zer0share.sources import DataSources, RiceQuantFetcher, TushareFetcher
    from zer0share.notifier import NotifierProtocol, build_notifier

    sources = DataSources(
        tushare=TushareFetcher(cfg.tushare_token, cfg.tushare_http_url),
        ricequant=(
            RiceQuantFetcher(
                username=cfg.ricequant.username,
                password=cfg.ricequant.password,
                license_key=cfg.ricequant.license_key,
            )
            if cfg.ricequant.enabled
            else None
        ),
    )
    notifier: NotifierProtocol = build_notifier(cfg.notifier)
    with Pipeline(cfg, sources, notifier) as pipeline:
        return _run_job_tables_with(pipeline, tables,
                                     start_date=start_date, end_date=end_date)


def _missing_table_hint(missing: list[str], registry) -> str:
    """表名不在 registry 时给出可操作提示。

    最常见原因是 config 里对应的数据源 enabled=false (如 [ricequant]),
    build_jobs 直接返回空; 其次是表名拼错。两种都要说清楚, 否则用户只看到
    "找不到表" 不知道该改哪里。
    """
    known = ", ".join(sorted(registry)) or "(空)"
    if all(t.startswith("ricequant") for t in missing):
        return (
            f"表 {missing} 在 job registry 中不存在: "
            f"config [ricequant] enabled = false 时 build_jobs 返回空。"
            f"如需同步这些表, 先在 config/settings.toml 的 [ricequant] "
            f"填入账号并设 enabled = true。"
        )
    return (
        f"表 {missing} 在 job registry 中不存在, 已注册的表: {known}。"
        f"请检查表名拼写, 或该数据源是否在 config 中 enabled = false。"
    )


def _lightweight_registry(cfg) -> dict:
    """不开 MetaStore/DuckDB 的轻量 job registry。

    只需要 job 的静态属性 (table_name / supports_date_range), 因此
    build_jobs(cfg, fetcher) + build_batch_jobs(cfg, fetcher) 就够了 ——
    传一个真实 fetcher (build_jobs 需要它来绑 fetch 回调), 但 dry-run
    路径下我们只读 job 的静态属性, 不会真正调用这些回调。
    用于 --dry-run 时的前置校验, 避免为读一个属性去抢 DuckDB 锁。
    """
    from zer0share.sources import DataSources, TushareFetcher
    from zer0share.sync import (calendar, stock, index, industry, futures,
                                options, etf, _batch_jobs)

    src = DataSources(
        tushare=TushareFetcher(cfg.tushare_token, cfg.tushare_http_url),
        ricequant=None,
    )
    reg: dict = {}
    for module in [calendar, stock, index, industry, futures, options, etf]:
        try:
            for job in module.build_jobs(cfg, src.tushare):
                reg[job.table_name] = job
        except Exception:  # noqa: BLE001 — 单个模块建不了不影响其他表
            continue
    try:
        codes = _batch_jobs.load_codes(cfg.data_dir)
        for job in _batch_jobs.build_batch_jobs(cfg, src.tushare, codes):
            reg[job.table_name] = job
    except Exception:  # noqa: BLE001
        pass
    return reg


def _check_date_range_support(registry, tables: list[str],
                              start_date: str | None,
                              end_date: str | None) -> None:
    """对给出日期区间但不支持区间的表提前抛 DateRangeNotSupported。

    在 dry-run 与真实执行两条路径上都要跑, 保证报错时机一致。
    """
    if start_date is None and end_date is None:
        return
    for t in tables:
        job = registry.get(t)
        if job is not None and not job.supports_date_range:
            raise DateRangeNotSupported(
                f"{t} 不支持日期区间 "
                "(date range options are only supported for daily partitioned tables)"
            )


def _run_job_tables_with(pipeline, tables: list[str], *,
                         start_date: str | None = None,
                         end_date: str | None = None,
                         strict: bool = False) -> tuple[list[str], list[str]]:
    """在给定 pipeline 上逐表执行 job 引擎的任务。

    不支持日期区间的表 (快照表 / 多交易所表) 在给出区间时报错 —— 早期版本
    直接传进 pipeline, 结果由 job 内部抛 ValueError, CLI 层拿到的 exit code
    与提示信息都不明确。这里提前拦下, 由 CLI 转成 UsageError。
    """
    done: list[str] = []
    failed: list[str] = []
    registry = pipeline.registry
    _check_date_range_support(registry, tables, start_date, end_date)
    # 未注册的表直接报错, 而不是静默跳过: --dry-run 已经提前拦过一轮,
    # 走到这里说明是运行期 registry 变了 (如 config 被改), 静默跳过会让
    # 调用方以为同步成功, 实际一张表都没写。
    missing = [t for t in tables if t not in registry]
    if missing:
        if strict:
            raise UnknownTable(_missing_table_hint(missing, registry))
        logger.info(f"⏭ 跳过未注册表 {missing}: "
                    f"{_missing_table_hint(missing, registry)}")
        tables = [t for t in tables if t in registry]
        if not tables:
            return [], []
    for t in tables:
        try:
            pipeline.run(t, start_date=start_date, end_date=end_date)
            done.append(t)
        except Exception as exc:  # noqa: BLE001 — 单表失败不应中断整批
            logger.info(f"❌ {t}: {str(exc)[:160]}")
            failed.append(t)
    return done, failed


def partition_by_engine(tables: list[str]) -> tuple[list[str], list[str]]:
    """把表清单按取数方式分成 (批量取数表, 逐日/快照表)。

    所有表都走同一个 Pipeline; 这个划分只用于日志分组与测试断言,
    不再代表"不同的引擎" —— 原 bulk 引擎的能力已吸收进 BatchSyncJob。
    """
    batch_tables, job_tables = [], []
    for t in tables:
        canonical = SP.normalize_table(t)
        if canonical in SP.BULK_TABLES:
            batch_tables.append(canonical)
        else:
            job_tables.append(canonical)
    return batch_tables, job_tables


def run_sync(cfg, tables: list[str], *, start_date: str | None = None,
             end_date: str | None = None, dry_run: bool = False,
             budget_seconds: float | None = None,
             include_missing: bool = False,
             strict: bool = False,
             pipeline_factory=None) -> SyncReport:
    """执行统一 sync。

    strict=False (默认): 表清单里若有表在 job registry 中不存在 (通常是
      config 中对应数据源 enabled=false, 如 [ricequant]), 直接剔除并提示,
      而不是报错中断 —— --all / --topic 展开的场景下, 关掉的数据源本就不该
      同步, 报错会让整条命令不可用。
    strict=True: 用户显式 --table 指定了表, 不存在就是配置/拼写问题, 必须报错。

    budget_seconds: 批量取数表的整体时间预算。超预算后剩余表不再启动,
      记入 skipped (DEFERRED), 不算失败 —— 下次 sync 继续补。原 bulk 引擎的
      780 秒预算语义, 吸收后保留在 job 侧。
    """
    report = SyncReport()
    canonical = [SP.normalize_table(t) for t in tables]
    batch_tables, job_tables = partition_by_engine(canonical)

    logger.info(f"统一 sync: 共 {len(canonical)} 张表 "
                f"(批量取数 {len(batch_tables)} / 逐日快照 {len(job_tables)})")

    if job_tables:
        logger.info(f"→ job 引擎: {', '.join(job_tables)}")
        j_done, j_failed = run_job_tables(cfg, job_tables, start_date=start_date,
                                          end_date=end_date, dry_run=dry_run,
                                          strict=strict,
                                          pipeline_factory=pipeline_factory)
        report.done.extend(j_done)
        report.failed.extend(j_failed)

    if batch_tables:
        started = time.time()
        for name in batch_tables:
            if (budget_seconds is not None and not dry_run
                    and time.time() - started >= budget_seconds):
                logger.info(f"⏭ {name}: DEFERRED(budget)")
                report.skipped.append(name)
                continue
            b_done, b_failed = run_job_tables(cfg, [name], start_date=start_date,
                                              end_date=end_date, dry_run=dry_run,
                                              strict=strict,
                                              pipeline_factory=pipeline_factory)
            report.done.extend(b_done)
            report.failed.extend(b_failed)

    logger.info(f"统一 sync 结束: {report.summary()}")
    return report
