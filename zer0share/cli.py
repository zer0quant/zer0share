from datetime import datetime
from pathlib import Path

import click
from loguru import logger

from zer0share.config import load_config
from zer0share.fetcher import TushareFetcher
from zer0share.logging import init_logger
from zer0share.notifier import Notifier
from zer0share.pipeline import Pipeline
from zer0share.storage import MetaStore
from zer0share.universe import build_universes, build_universes_range


def _make_pipeline(config_path: str = "config/settings.toml") -> Pipeline:
    cfg = load_config(Path(config_path))
    init_logger(cfg.log_path)
    fetcher = TushareFetcher(cfg.tushare_token)
    notifier = Notifier(cfg.wecom_webhook_url, cfg.notifier_enabled)
    return Pipeline(cfg, fetcher, notifier)


@click.group()
def cli():
    pass


SYNC_TABLES = [
    "daily_kline",
    "basic",
    "trade_cal",
    "adj_factor",
    "daily_basic",
    "stock_st",
    "suspend_d",
    "stk_limit",
    "index_weight",
    "index_daily",
    "industry",
    "ci_member",
]


@cli.command()
@click.option(
    "--table",
    type=click.Choice(SYNC_TABLES),
    default=None,
)
@click.option("--all", "sync_all", is_flag=True, default=False)
@click.option("--start-date", type=click.DateTime(formats=["%Y-%m-%d"]), default=None)
@click.option("--end-date", type=click.DateTime(formats=["%Y-%m-%d"]), default=None)
def sync(
    table: str | None,
    sync_all: bool,
    start_date: datetime | None,
    end_date: datetime | None,
) -> None:
    """同步数据。"""
    if end_date is not None and start_date is None:
        raise click.UsageError("--end-date requires --start-date")
    range_tables = {
        "daily_kline",
        "adj_factor",
        "daily_basic",
        "stock_st",
        "suspend_d",
        "stk_limit",
        "index_weight",
        "index_daily",
    }
    if (start_date is not None or end_date is not None) and table not in range_tables:
        raise click.UsageError("date range options are only supported for daily partitioned tables")

    parsed_start_date = start_date.date() if start_date is not None else None
    parsed_end_date = end_date.date() if end_date is not None else None
    if (
        parsed_start_date is not None
        and parsed_end_date is not None
        and parsed_end_date < parsed_start_date
    ):
        raise click.UsageError("--end-date must be on or after --start-date")

    with _make_pipeline() as pipeline:
        if sync_all or table == "trade_cal":
            pipeline.sync_trade_cal()
        if sync_all or table == "basic":
            pipeline.sync_basic()
        if sync_all or table == "daily_kline":
            pipeline.sync_daily_kline(
                start_date=parsed_start_date,
                end_date=parsed_end_date,
            )
        if sync_all or table == "adj_factor":
            pipeline.sync_adj_factor(
                start_date=parsed_start_date,
                end_date=parsed_end_date,
            )
        if sync_all or table == "daily_basic":
            pipeline.sync_daily_basic(
                start_date=parsed_start_date,
                end_date=parsed_end_date,
            )
        if sync_all or table == "stock_st":
            pipeline.sync_stock_st(
                start_date=parsed_start_date,
                end_date=parsed_end_date,
            )
        if sync_all or table == "suspend_d":
            pipeline.sync_suspend_d(
                start_date=parsed_start_date,
                end_date=parsed_end_date,
            )
        if sync_all or table == "stk_limit":
            pipeline.sync_stk_limit(
                start_date=parsed_start_date,
                end_date=parsed_end_date,
            )
        if sync_all or table == "index_weight":
            pipeline.sync_index_weight(
                start_date=parsed_start_date,
                end_date=parsed_end_date,
            )
        if sync_all or table == "index_daily":
            pipeline.sync_index_daily(
                start_date=parsed_start_date,
                end_date=parsed_end_date,
            )
        if sync_all or table == "industry":
            pipeline.sync_industry()
        if sync_all or table == "ci_member":
            pipeline.sync_ci_member()


@cli.command("build-universe")
@click.option("--date", "trade_date", type=click.DateTime(formats=["%Y-%m-%d", "%Y%m%d"]), default=None)
@click.option("--start-date", type=click.DateTime(formats=["%Y-%m-%d", "%Y%m%d"]), default=None)
@click.option("--end-date", type=click.DateTime(formats=["%Y-%m-%d", "%Y%m%d"]), default=None)
def build_universe_cmd(
    trade_date: datetime | None,
    start_date: datetime | None,
    end_date: datetime | None,
) -> None:
    """构建股票池。"""
    if trade_date is not None and (start_date is not None or end_date is not None):
        raise click.UsageError("--date cannot be used with --start-date or --end-date")

    cfg = load_config(Path("config/settings.toml"))
    init_logger(cfg.log_path)
    if trade_date is not None:
        counts = build_universes(cfg.data_dir, trade_date.date())
        for name, count in counts.items():
            click.echo(f"{name}: {count}")
        return

    summary = build_universes_range(
        cfg.data_dir,
        start_date=start_date.date() if start_date is not None else None,
        end_date=end_date.date() if end_date is not None else None,
    )
    click.echo(
        f"range: {summary['start_date']} ~ {summary['end_date']}, "
        f"trading_days: {summary['trading_days']}, "
        f"built: {summary['built_days']}, skipped: {summary['skipped_days']}"
    )
    for name, count in summary["counts"].items():
        click.echo(f"{name}: {count}")


@cli.command()
def status() -> None:
    """显示各表最后更新时间。"""
    cfg = load_config(Path("config/settings.toml"))
    with MetaStore(cfg.db_path) as store:
        for table in SYNC_TABLES:
            last = store.get_last_date(table)
            click.echo(f"{table}: {last or '从未同步'}")


@cli.command("scheduler")
@click.argument("action", type=click.Choice(["start"]))
def scheduler_cmd(action: str) -> None:
    """启动定时调度。"""
    from zer0share.scheduler import start_scheduler

    start_scheduler()
