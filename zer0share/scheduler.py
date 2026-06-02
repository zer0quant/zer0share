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
            ("ft_limit", pipeline.sync_ft_limit, 60),
            ("fut_weekly", pipeline.sync_fut_weekly, 70),
            ("fut_monthly", pipeline.sync_fut_monthly, 80),
            ("fut_index_daily", pipeline.sync_fut_index_daily, 90),
            ("fut_weekly_detail", pipeline.sync_fut_weekly_detail, 100),
        ]
        for job_id, func, offset in futures_tables:
            total_min = cfg.scheduler_futures_start_minute + offset
            job_hour = cfg.scheduler_futures_hour + total_min // 60
            job_minute = total_min % 60
            scheduler.add_job(
                func,
                CronTrigger(
                    hour=job_hour,
                    minute=job_minute,
                ),
                id=job_id,
            )
        options_tables = [
            ("opt_basic", pipeline.sync_opt_basic, 110),
            ("opt_daily", pipeline.sync_opt_daily, 120),
        ]
        for job_id, func, offset in options_tables:
            total_min = cfg.scheduler_futures_start_minute + offset
            job_hour = cfg.scheduler_futures_hour + total_min // 60
            job_minute = total_min % 60
            scheduler.add_job(
                func,
                CronTrigger(hour=job_hour, minute=job_minute),
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
