from pathlib import Path

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from loguru import logger

from zer0share.config import load_config
from zer0share.fetcher import TushareFetcher
from zer0share.logging import init_logger
from zer0share.notifier import Notifier
from zer0share.pipeline import Pipeline


def start_scheduler(config_path: str = "config/settings.toml") -> None:
    cfg = load_config(Path(config_path))
    init_logger(cfg.log_path)

    fetcher = TushareFetcher(cfg.tushare_token)
    notifier = Notifier(cfg.wecom_webhook_url, cfg.notifier_enabled)

    with Pipeline(cfg, fetcher, notifier) as pipeline:
        scheduler = BlockingScheduler()
        scheduler.add_job(
            pipeline.sync_daily_kline,
            CronTrigger(
                hour=cfg.scheduler_daily_kline_hour,
                minute=cfg.scheduler_daily_kline_minute,
            ),
            id="daily_kline",
        )
        scheduler.add_job(
            pipeline.sync_index_daily,
            CronTrigger(
                hour=cfg.scheduler_daily_kline_hour,
                minute=cfg.scheduler_daily_kline_minute,
            ),
            id="index_daily",
        )
        scheduler.add_job(
            pipeline.sync_basic,
            CronTrigger(hour=cfg.scheduler_basic_hour),
            id="basic",
        )
        scheduler.add_job(
            pipeline.sync_adj_factor,
            CronTrigger(
                hour=cfg.scheduler_adj_factor_hour,
                minute=cfg.scheduler_adj_factor_minute,
            ),
            id="adj_factor",
        )
        futures_tables = [
            ("fut_basic", pipeline.sync_fut_basic, 0),
            ("fut_daily", pipeline.sync_fut_daily, 10),
            ("fut_holding", pipeline.sync_fut_holding, 20),
            ("fut_wsr", pipeline.sync_fut_wsr, 30),
            ("fut_settle", pipeline.sync_fut_settle, 40),
            ("fut_mapping", pipeline.sync_fut_mapping, 50),
        ]
        for job_id, func, offset in futures_tables:
            scheduler.add_job(
                func,
                CronTrigger(
                    hour=cfg.scheduler_futures_hour,
                    minute=cfg.scheduler_futures_start_minute + offset,
                ),
                id=job_id,
            )
        logger.info(
            f"调度器启动: daily_kline + index_daily 每天 "
            f"{cfg.scheduler_daily_kline_hour}:{cfg.scheduler_daily_kline_minute:02d}, "
            f"adj_factor 每天 "
            f"{cfg.scheduler_adj_factor_hour}:{cfg.scheduler_adj_factor_minute:02d}, "
            f"basic 每天 {cfg.scheduler_basic_hour}:00, "
            f"futures 每天 {cfg.scheduler_futures_hour}:{cfg.scheduler_futures_start_minute:02d}+"
        )
        scheduler.start()
