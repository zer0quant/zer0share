from zer0share.catalog import (
    ADJ_FACTOR_SPEC, BASIC_SPEC, DAILY_BASIC_SPEC, DAILY_KLINE_SPEC,
    STK_HOLDERTRADE_SPEC, STK_LIMIT_SPEC, STOCK_ST_SPEC, SUSPEND_D_SPEC,
    SHARE_FLOAT_SPEC, BLOCK_TRADE_SPEC, MARGIN_SPEC,
)
from zer0share.storage import DailyPartitionStore, SnapshotStore
from zer0share.sync._jobs import CalendarDateSyncJob, DailySyncJob, SnapshotSyncJob, SyncJob


def _part_dir(d, spec) -> DailyPartitionStore:
    """分区目录一律由 spec.path_parts 派生 (catalog 是唯一权威)。

    早前这里硬编码 d/"stock"/"stk_limit" 之类的字面量, 与 catalog 声明、
    bulk 写入位置三方各写一份 — 一旦某处改名 (如 stk_limit → stk_limit_dir)
    就会出现「sync 写 A 目录、query 读 B 目录、bulk 写 C 目录」的静默分裂。
    """
    root = d
    for part in spec.path_parts:
        root = root / part
    return DailyPartitionStore(root)


def build_jobs(cfg, fetcher) -> list[SyncJob]:
    d = cfg.data_dir
    return [
        SnapshotSyncJob(
            table_name=BASIC_SPEC.name, spec=BASIC_SPEC,
            fetch=fetcher.fetch_basic,
            store=SnapshotStore(d / "stock" / "basic" / "data.parquet"),
            skip_non_trading=False,
        ),
        DailySyncJob(
            table_name=DAILY_KLINE_SPEC.name, spec=DAILY_KLINE_SPEC,
            fetch=fetcher.fetch_daily_kline,
            store=_part_dir(d, DAILY_KLINE_SPEC),
        ),
        DailySyncJob(
            table_name=ADJ_FACTOR_SPEC.name, spec=ADJ_FACTOR_SPEC,
            fetch=fetcher.fetch_adj_factor,
            store=_part_dir(d, ADJ_FACTOR_SPEC),
        ),
        DailySyncJob(
            table_name=DAILY_BASIC_SPEC.name, spec=DAILY_BASIC_SPEC,
            fetch=fetcher.fetch_daily_basic,
            store=_part_dir(d, DAILY_BASIC_SPEC),
        ),
        DailySyncJob(
            table_name=STOCK_ST_SPEC.name, spec=STOCK_ST_SPEC,
            fetch=fetcher.fetch_stock_st,
            store=_part_dir(d, STOCK_ST_SPEC),
            write_empty=True,
        ),
        DailySyncJob(
            table_name=SUSPEND_D_SPEC.name, spec=SUSPEND_D_SPEC,
            fetch=fetcher.fetch_suspend_d,
            store=_part_dir(d, SUSPEND_D_SPEC),
            write_empty=True,
        ),
        DailySyncJob(
            table_name=STK_LIMIT_SPEC.name, spec=STK_LIMIT_SPEC,
            fetch=fetcher.fetch_stk_limit,
            store=_part_dir(d, STK_LIMIT_SPEC),
        ),
        DailySyncJob(
            table_name=SHARE_FLOAT_SPEC.name, spec=SHARE_FLOAT_SPEC,
            fetch=fetcher.fetch_share_float,
            store=_part_dir(d, SHARE_FLOAT_SPEC),
            write_empty=True,
        ),
        DailySyncJob(
            table_name=BLOCK_TRADE_SPEC.name, spec=BLOCK_TRADE_SPEC,
            fetch=fetcher.fetch_block_trade,
            store=_part_dir(d, BLOCK_TRADE_SPEC),
            write_empty=True,
        ),
        DailySyncJob(
            table_name=MARGIN_SPEC.name, spec=MARGIN_SPEC,
            fetch=fetcher.fetch_margin,
            store=_part_dir(d, MARGIN_SPEC),
            write_empty=True,
        ),
        # 公告日含周末，按自然日逐日拉
        CalendarDateSyncJob(
            table_name=STK_HOLDERTRADE_SPEC.name, spec=STK_HOLDERTRADE_SPEC,
            fetch=fetcher.fetch_stk_holdertrade,
            store=_part_dir(d, STK_HOLDERTRADE_SPEC),
            write_empty=True,
        ),
    ]
