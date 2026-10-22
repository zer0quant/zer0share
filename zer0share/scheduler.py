from pathlib import Path
import datetime as dt

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from loguru import logger

from zer0share.config import load_config
from zer0share.logging import init_logger
from zer0share.notifier import build_notifier
from zer0share.pipeline import Pipeline
from zer0share.quality.models import QualityRunOptions
from zer0share.quality.reporter import QualityReporter, format_summary
from zer0share.quality.runner import QualityRunner
from zer0share.quality.targets import QUALITY_TARGETS
from zer0share.sources import DataSources, RiceQuantFetcher, TushareFetcher


def _quality_tables_for_sync(table_name: str, markets: list[str]) -> tuple[str, ...]:
    target = QUALITY_TARGETS.get(table_name)
    if target is None or target.market not in markets:
        return ()
    return (target.table,)


def run_scheduled_table(cfg, sources, notifier, table_name: str) -> None:
    with Pipeline(cfg, sources, notifier) as pipeline:
        pipeline.run(table_name)

    quality_cfg = cfg.quality
    if not quality_cfg.enabled:
        return

    tables = _quality_tables_for_sync(table_name, quality_cfg.markets)
    if not tables:
        return

    try:
        options = QualityRunOptions(
            mode="daily",
            tables=tables,
            date=dt.datetime.now().strftime("%Y%m%d"),
        )
        report = QualityRunner(cfg.data_dir).run(options)
        output_dir = QualityReporter(Path("reports") / "quality").write(report)
        notify_on = set(quality_cfg.notify_on)
        should_notify = (
            ("fail" in notify_on and report.fail_count > 0)
            or ("warn" in notify_on and report.warn_count > 0)
        )
        if should_notify:
            notifier.send(f"数据质检发现问题\n{format_summary(report)}\n报告：{output_dir}")
    except Exception as exc:
        logger.error(f"quality check failed after {table_name}: {exc}")


def start_scheduler(config_path: str = "config/settings.toml") -> None:
    cfg = load_config(Path(config_path))
    init_logger(cfg.log_path)

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
    notifier = build_notifier(cfg.notifier)

    def run_table(table_name: str) -> None:
        run_scheduled_table(cfg, sources, notifier, table_name)

    scheduler = BlockingScheduler()
    for table_name, time_str in cfg.schedule.items():
        hour, minute = (int(x) for x in time_str.split(":"))
        scheduler.add_job(
            lambda t=table_name: run_table(t),
            CronTrigger(hour=hour, minute=minute),
            id=table_name,
        )
    table_count = len(cfg.schedule)
    logger.info(f"调度器启动: {table_count} 个表已调度")
    notifier.send(
        f"调度器已启动\n已调度：{table_count} 个表\n配置：{config_path}"
    )
    scheduler.start()
