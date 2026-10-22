import datetime as dt
from pathlib import Path

import click

from zer0share.config import load_config
from zer0share.logging import init_logger
from zer0share.news import NEWS_APIS, NEWS_DB, NEWS_RAW, NewsStore, NewsSyncer, RawArchive, TushareHttpClient
from zer0share.notifier import build_notifier
from zer0share.pipeline import Pipeline
from zer0share.quality.models import QualityRunOptions
from zer0share.quality.reporter import QualityReporter, format_summary
from zer0share.sync_runner import (
    DateRangeNotSupported,
    UnknownTable,
    run_sync,
)
from zer0share.quality.runner import QualityRunner
from zer0share.quality.targets import select_targets
from zer0share.sources import DataSources, RiceQuantFetcher, TushareFetcher
from zer0share.storage import MetaStore
from zer0share import sync_plan as SP
from zer0share.universe import build_universes, build_universes_range


# 批量取数表的默认时间预算 (秒)。原 bulk 引擎的 DEFAULT_BUDGET_SECONDS
# 已随 bulk.py 删除, 这里保留同一数值与语义: 超出预算后剩余表不再启动
# (本次运行视为 DEFERRED, 不算失败), 下次 sync 继续补。
DEFAULT_BUDGET_SECONDS = 780.0


def bulk_budget_default() -> float:
    """批量取数表的默认时间预算 (秒)。"""
    return DEFAULT_BUDGET_SECONDS


def _validate_date(ctx, param, value):
    if value is None:
        return None
    try:
        dt.datetime.strptime(value, "%Y%m%d")
        return value
    except ValueError:
        raise click.BadParameter("格式应为 YYYYMMDD，例如 20240102")


def _parse_date(s: str):
    return dt.datetime.strptime(s, "%Y%m%d").date()


def _make_pipeline(config_path: str = "config/settings.toml") -> Pipeline:
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
    return Pipeline(cfg, sources, notifier)


@click.group()
def cli():
    pass


FUTURES_TABLES = [
    "fut_basic",
    "fut_daily",
    "fut_holding",
    "fut_wsr",
    "fut_settle",
    "fut_mapping",
    "ft_limit",
    "fut_weekly",
    "fut_monthly",
    "fut_index_daily",
    "fut_weekly_detail",
]

OPTIONS_TABLES = [
    "opt_basic",
    "opt_daily",
]

ETF_TABLES = [
    "fund_daily",
    "fund_adj",
    "etf_share_size",
    "etf_sh_cons",
    "etf_basic",
    "etf_index",
]

STOCK_TABLES = [
    "basic",
    "trade_cal",
    "daily_kline",
    "adj_factor",
    "daily_basic",
    "stock_st",
    "suspend_d",
    "stk_limit",
    "share_float",
    "block_trade",
    "margin",
    "stk_holdertrade",
    "index_daily",
    "index_weight",
    "idx_anns",
    "industry",
    "sw_daily",
    "ci_member",
]

RICEQUANT_TABLES = [
    "ricequant_basic",
    "ricequant_stock_minute",
    "ricequant_etf_basic",
    "ricequant_etf_minute",
]

SYNC_TABLES = [
    *STOCK_TABLES,
    *FUTURES_TABLES,
    *OPTIONS_TABLES,
    *ETF_TABLES,
    *RICEQUANT_TABLES,
]


@cli.command()
@click.option(
    "--table",
    "tables",
    multiple=True,
    help="指定表 (可重复, 也可逗号分隔)。表名用 catalog 规范名, 如 daily_kline / basic",
)
@click.option("--tables", "tables_csv", default="",
              help="逗号分隔表名, 与 --table 等价 (两者取并集)")
@click.option("--topic", default="",
              help=f"按主题同步, 逗号分隔; all=全部主题。可选: {','.join(SP.ALL_TOPICS)}。"
                   "与 --table 同时给出时取并集")
@click.option("--all", "sync_all", is_flag=True, default=False, help="同步全部表")
@click.option("--stock", "sync_stock", is_flag=True, default=False,
              help="[兼容] 股票主题 (= --topic quote)")
@click.option("--futures", "sync_futures", is_flag=True, default=False,
              help="[兼容] 期货主题 (= --topic futures)")
@click.option("--options", "sync_options", is_flag=True, default=False,
              help="[兼容] 期权主题 (= --topic options)")
@click.option("--etf", "sync_etf", is_flag=True, default=False,
              help="[兼容] ETF 主题 (= --topic etf)")
@click.option("--ricequant", "sync_ricequant", is_flag=True, default=False,
              help="[兼容] RiceQuant 主题 (= --topic ricequant)")
@click.option("--start-date", default=None, callback=_validate_date)
@click.option("--end-date", default=None, callback=_validate_date)
@click.option("--end", "end_alias", default=None, callback=_validate_date,
              help="截止日期 (与 --end-date 等价)")
@click.option("--dry-run", is_flag=True, default=False,
              help="只打印计划, 不发网络请求、不写文件")
@click.option("--include-missing", is_flag=True, default=False,
              help="也回补本地从未建成的表 (全历史, 极慢)")
@click.option("--budget-seconds", type=float, default=None,
              help=f"时间预算秒数 (默认 {bulk_budget_default():.0f})")
def sync(
    tables: tuple[str, ...],
    tables_csv: str,
    topic: str,
    sync_all: bool,
    sync_stock: bool,
    sync_futures: bool,
    sync_options: bool,
    sync_etf: bool,
    sync_ricequant: bool,
    start_date: str | None,
    end_date: str | None,
    end_alias: str | None,
    dry_run: bool,
    include_missing: bool,
    budget_seconds: float | None,
) -> None:
    """同步数据 (统一入口)。

    所有表都是 pipeline 里的 SyncJob, 由同一套 run/游标/重试/通知驱动:
      * 逐日型 (DailySyncJob) — 按交易日历逐日拉取
      * 批量型 (BatchSyncJob) — 多日/多股一次拉取后合并写入分区
        (吸收原 bulk 引擎的取数能力, 不再是独立引擎)

    统一写 catalog 规范布局 (时序表 date=YYYYMMDD/data.parquet,
    静态表 data.parquet), 不再有单文件与分区并存的旧格式。

    例:
      zer0share sync --all
      zer0share sync --topic quote,index
      zer0share sync --table daily_kline --table basic
      zer0share sync --tables daily_kline,daily_basic
      zer0share sync --all --dry-run
    """
    if end_alias and end_date and end_alias != end_date:
        raise click.UsageError("--end 与 --end-date 不一致")
    end_date = end_date or end_alias

    if end_date is not None and start_date is None:
        raise click.UsageError("--end-date requires --start-date")
    if start_date is not None and end_date is not None and end_date < start_date:
        raise click.UsageError("--end-date must be on or after --start-date")

    # 收集表名: --table (可重复) + --tables (逗号) + 主题开关
    wanted: list[str] = []
    for t in tables:
        wanted.extend(x for x in t.split(",") if x.strip())
    wanted.extend(x for x in tables_csv.split(",") if x.strip())

    legacy_flags = {
        "stock": sync_stock,
        "futures": sync_futures,
        "options": sync_options,
        "etf": sync_etf,
        "ricequant": sync_ricequant,
    }
    topics: list[str] = []
    for t in topic.split(","):
        t = t.strip()
        if not t:
            continue
        if t.lower() == "all":
            topics.extend(SP.ALL_TOPICS)
            continue
        resolved = SP.resolve_topic(t)
        if resolved is None:
            raise click.UsageError(
                f"未知主题: {t}｜可用主题: {','.join(SP.ALL_TOPICS)}"
            )
        topics.append(resolved)
    for topic_name in dict.fromkeys(topics):
        wanted.extend(SP.tables_for_topic(topic_name))

    # 旧开关保持原语义: 各自的历史表清单 (不是新的功能主题)
    for flag_name, enabled in legacy_flags.items():
        if enabled:
            wanted.extend(SP.LEGACY_FLAG_TABLES[flag_name])

    if sync_all:
        wanted.extend(SP.all_table_names())

    if not wanted:
        raise click.UsageError(
            "需要指定 --table/--tables/--topic/--all "
            "(或 --stock/--futures/--options/--etf/--ricequant)"
        )

    # 用户显式点名了表 → strict。批量展开 (--all/--topic/--stock...) → 非
    # strict: 关掉的数据源 (config enabled=false) 只是跳过, 不该让整条命令
    # 失败; 只有用户明确要某张表却拿不到时, 才是配置/拼写问题, 必须报错。
    named_table = bool(tables or tables_csv.strip())
    strict = named_table and not (sync_all or topics or
                                  any(legacy_flags.values()))

    # 归一表名 + 去重
    canonical: list[str] = []
    for t in wanted:
        name = SP.normalize_table(t)
        if name not in canonical:
            canonical.append(name)

    unknown = [t for t in canonical if t not in SP.PLANS]
    if unknown:
        raise click.UsageError(
            f"未知表: {','.join(unknown)}｜用 'zer0share sync --help' 查看主题清单"
        )

    cfg = load_config(Path("config/settings.toml"))
    init_logger(cfg.log_path)

    try:
        report = run_sync(
            cfg,
            canonical,
            start_date=start_date,
            end_date=end_date,
            dry_run=dry_run,
            budget_seconds=budget_seconds,
            include_missing=include_missing,
            strict=strict,
            pipeline_factory=_make_pipeline,
        )
    except (DateRangeNotSupported, UnknownTable) as exc:
        raise click.UsageError(str(exc)) from exc
    click.echo(f"sync 完成: {report.summary()}")
    if report.failed:
        raise click.exceptions.Exit(1)


@cli.group()
def quality():
    """数据质检。"""


@quality.command("check")
@click.option("--table", "table_name", default=None)
@click.option(
    "--market",
    type=click.Choice(["stock", "index", "etf", "futures", "options"]),
    default=None,
)
@click.option("--all", "all_targets", is_flag=True, default=False)
@click.option(
    "--mode",
    type=click.Choice(["full", "daily"]),
    default="daily",
    show_default=True,
)
@click.option("--start-date", default=None, callback=_validate_date)
@click.option("--end-date", default=None, callback=_validate_date)
@click.option("--date", "single_date", default=None, callback=_validate_date)
def quality_check(
    table_name: str | None,
    market: str | None,
    all_targets: bool,
    mode: str,
    start_date: str | None,
    end_date: str | None,
    single_date: str | None,
) -> None:
    """检查本地 Parquet 数据质量。"""
    if mode == "full" and (start_date is None or end_date is None):
        raise click.UsageError("full mode requires --start-date and --end-date")
    if mode == "daily" and (start_date is not None or end_date is not None):
        raise click.UsageError("daily mode uses --date, not --start-date/--end-date")
    if mode == "full" and single_date is not None:
        raise click.UsageError("full mode does not use --date")

    try:
        targets = select_targets(table=table_name, market=market, all_targets=all_targets)
    except ValueError as exc:
        raise click.UsageError(str(exc)) from exc

    cfg = load_config(Path("config/settings.toml"))
    options = QualityRunOptions(
        mode=mode,
        tables=tuple(target.table for target in targets),
        start_date=start_date,
        end_date=end_date,
        date=single_date,
    )
    report = QualityRunner(cfg.data_dir).run(options)
    QualityReporter(Path("reports") / "quality").write(report)
    click.echo(format_summary(report))
    if report.fail_count > 0:
        raise click.exceptions.Exit(1)


@cli.command("build-universe")
@click.option("--date", "trade_date", default=None, callback=_validate_date)
@click.option("--start-date", default=None, callback=_validate_date)
@click.option("--end-date", default=None, callback=_validate_date)
def build_universe_cmd(
    trade_date: str | None,
    start_date: str | None,
    end_date: str | None,
) -> None:
    """构建股票池。"""
    if trade_date is not None and (start_date is not None or end_date is not None):
        raise click.UsageError("--date cannot be used with --start-date or --end-date")

    cfg = load_config(Path("config/settings.toml"))
    init_logger(cfg.log_path)
    if trade_date is not None:
        counts = build_universes(cfg.data_dir, _parse_date(trade_date))
        for name, count in counts.items():
            click.echo(f"{name}: {count}")
        return

    summary = build_universes_range(
        cfg.data_dir,
        start_date=_parse_date(start_date) if start_date is not None else None,
        end_date=_parse_date(end_date) if end_date is not None else None,
    )
    click.echo(
        f"range: {summary['start_date']} ~ {summary['end_date']}, "
        f"trading_days: {summary['trading_days']}, "
        f"built: {summary['built_days']}, skipped: {summary['skipped_days']}"
    )
    for name, count in summary["counts"].items():
        click.echo(f"{name}: {count}")


@cli.group("news")
def news_group():
    """新闻数据（原始 JSONL 归档 + SQLite 主库）。"""


@news_group.command("sync")
@click.option("--api", "api_name", type=click.Choice(sorted(NEWS_APIS)), default="major_news")
@click.option("--src", "sources", multiple=True, help="只同步指定来源，可重复；默认全部来源")
@click.option("--start-date", required=True, callback=_validate_date)
@click.option("--end-date", default=None, callback=_validate_date, help="默认今天")
@click.option("--with-content/--no-content", default=None, help="是否存正文；默认取配置 tushare_news.with_content")
@click.option("--refresh", is_flag=True, default=False, help="忽略断点，重拉已完成的日期")
def news_sync(
    api_name: str,
    sources: tuple[str, ...],
    start_date: str,
    end_date: str | None,
    with_content: bool | None,
    refresh: bool,
) -> None:
    """同步新闻：python main.py news sync --api major_news --start-date 20250324"""
    spec = NEWS_APIS[api_name]
    unknown = set(sources) - set(spec.sources)
    if unknown:
        raise click.UsageError(f"{api_name} 不支持的来源：{', '.join(unknown)}；可选：{', '.join(spec.sources)}")
    end_date = end_date or dt.date.today().strftime("%Y%m%d")
    if end_date < start_date:
        raise click.UsageError("--end-date must be on or after --start-date")

    cfg = load_config(Path("config/settings.toml"))
    init_logger(cfg.log_path)
    client = TushareHttpClient(cfg.news.token, cfg.news.http_url, cfg.news.request_sleep_seconds)
    store = NewsStore(cfg.data_dir.joinpath(*NEWS_DB))
    try:
        summary = NewsSyncer(client, store, RawArchive(cfg.data_dir.joinpath(*NEWS_RAW))).sync(
            spec,
            start_date,
            end_date,
            sources=list(sources) or None,
            with_content=cfg.news.with_content if with_content is None else with_content,
            refresh=refresh,
        )
    finally:
        client.close()
        store.close()
    click.echo(f"{api_name}：{summary}")


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
