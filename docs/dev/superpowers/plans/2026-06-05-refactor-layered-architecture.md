# Layered Architecture Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor zer0share from two God classes (pipeline.py / api.py ~1000 lines each) into a layered architecture where sync/ and query/ packages organize logic by domain, and pipeline.py / api.py become thin facades.

**Architecture:** Data flows through four layers — schema (column definitions) → fetcher (Tushare I/O) → storage (Parquet I/O) → sync/ (fetch+store orchestration) and query/ (read+filter). pipeline.py and api.py are entry-point facades only. Each sync domain function takes a `SyncContext` dataclass; each query domain function takes a `QueryContext` dataclass. No class inheritance.

**Tech Stack:** Python 3.11+, pandas, duckdb, pyarrow, loguru, uv for test running.

---

## File Map

**Created:**
- `zer0share/schema.py` — all `*_COLS` column definitions (moved from fetcher.py)
- `zer0share/sync/__init__.py` — `SyncContext` dataclass
- `zer0share/sync/_helpers.py` — constants, `sync_daily_partitioned`, trading-day helpers, date/time utilities
- `zer0share/sync/calendar.py` — `sync_trade_cal`
- `zer0share/sync/equities.py` — `sync_basic`, `sync_daily_kline`, `sync_adj_factor`, `sync_daily_basic`, `sync_stock_st`, `sync_suspend_d`, `sync_stk_limit`, `sync_index_weight`, `sync_index_daily`
- `zer0share/sync/industry.py` — `sync_industry`, `sync_ci_member`
- `zer0share/sync/futures.py` — all `sync_fut_*` and `sync_ft_limit`
- `zer0share/sync/options.py` — `sync_opt_basic`, `sync_opt_daily`
- `zer0share/query/__init__.py` — `QueryContext` dataclass
- `zer0share/query/_helpers.py` — `query_daily_partitioned`, `parse_date`, `parse_fields`, `parse_is_open`, `format_date_columns`
- `zer0share/query/calendar.py` — `trade_cal`
- `zer0share/query/equities.py` — `stock_basic`, `daily`, `adj_factor`, `daily_basic`, `stock_st`, `suspend_d`, `stk_limit`, `index_weight`, `index_daily`, `pro_bar`
- `zer0share/query/industry.py` — `index_classify`, `index_member_all`, `ci_index_member`
- `zer0share/query/futures.py` — all `fut_*` and `ft_limit` query functions
- `zer0share/query/options.py` — `opt_basic`, `opt_daily`

**Replaced (complete rewrite):**
- `zer0share/pipeline.py` — thin Pipeline facade, delegates all methods to sync/*
- `zer0share/api.py` — thin LocalPro facade, delegates all methods to query/*

**Modified:**
- `zer0share/fetcher.py` — remove `*_COLS` definitions, import from schema
- `tests/test_pipeline.py` — update imports and patch paths
- `tests/test_api.py` — update imports (if any reference fetcher.*_COLS)

**Unchanged:**
- `zer0share/storage.py`, `zer0share/config.py`, `zer0share/notifier.py`, `zer0share/logging.py`, `zer0share/universe.py`, `zer0share/fetcher.py` (except column definitions), `zer0share/cli.py`, `zer0share/scheduler.py`, all other test files.

---

## Task 1: Create schema.py and update imports

**Files:**
- Create: `zer0share/schema.py`
- Modify: `zer0share/fetcher.py`
- Modify: `zer0share/api.py`

- [ ] **Step 1: Verify baseline**

```bash
uv run pytest --tb=short -q
```

Expected: `258 passed`

- [ ] **Step 2: Create schema.py**

Create `zer0share/schema.py` with all column definitions extracted from `fetcher.py`. Check `fetcher.py` for every `*_COLS` list and the `INDEX_DAILY_CODES` list (which is NOT a column definition and stays in fetcher.py — only move the `*_COLS` lists and nothing else):

```python
BASIC_COLS = [
    "ts_code", "symbol", "name", "area", "industry", "fullname", "enname",
    "cnspell", "market", "exchange", "curr_type", "list_status",
    "list_date", "delist_date", "is_hs", "act_name", "act_ent_type",
]
DAILY_COLS = [
    "ts_code", "trade_date", "open", "high", "low",
    "close", "pre_close", "change", "pct_chg", "vol", "amount",
]
TRADE_CAL_COLS = ["exchange", "cal_date", "is_open", "pretrade_date"]
ADJ_FACTOR_COLS = ["ts_code", "trade_date", "adj_factor"]
DAILY_BASIC_COLS = [
    "ts_code", "trade_date", "close", "turnover_rate", "turnover_rate_f",
    "volume_ratio", "pe", "pe_ttm", "pb", "ps", "ps_ttm", "dv_ratio",
    "dv_ttm", "total_share", "float_share", "free_share", "total_mv", "circ_mv",
]
STOCK_ST_COLS = ["ts_code", "name", "trade_date", "type", "type_name"]
SUSPEND_D_COLS = ["ts_code", "trade_date", "suspend_timing", "suspend_type"]
STK_LIMIT_COLS = ["trade_date", "ts_code", "pre_close", "up_limit", "down_limit"]
INDEX_WEIGHT_COLS = ["index_code", "con_code", "trade_date", "weight"]
INDEX_DAILY_COLS = [
    "ts_code", "trade_date", "open", "high", "low",
    "close", "pre_close", "change", "pct_chg", "vol", "amount",
]
SW_CLASSIFY_COLS = [
    "index_code", "industry_name", "level", "parent_code",
    "industry_code", "is_pub", "src",
]
SW_MEMBER_COLS = [
    "l1_code", "l1_name", "l2_code", "l2_name", "l3_code", "l3_name",
    "ts_code", "name", "in_date", "out_date", "is_new",
]
CI_MEMBER_COLS = [
    "l1_code", "l1_name", "l2_code", "l2_name", "l3_code", "l3_name",
    "ts_code", "name", "in_date", "out_date", "is_new",
]
OPT_BASIC_COLS = [
    "ts_code", "symbol", "exchange", "name", "per_unit", "opt_code",
    "opt_type", "call_put", "exercise_type", "exercise_price", "s_month",
    "maturity_date", "list_price", "list_date", "delist_date",
    "last_edate", "last_ddate", "quote_unit", "min_price_chg",
]
OPT_DAILY_COLS = [
    "ts_code", "trade_date", "exchange",
    "pre_settle", "pre_close", "open", "high", "low", "close",
    "settle", "vol", "amount", "oi",
]
FUT_BASIC_COLS = [
    "ts_code", "symbol", "exchange", "name", "fut_code", "multiplier",
    "trade_unit", "per_unit", "quote_unit", "quote_unit_desc", "d_mode_desc",
    "list_date", "delist_date", "d_month", "last_ddate", "trade_time_desc",
]
FUT_DAILY_COLS = [
    "ts_code", "trade_date", "pre_close", "pre_settle", "open", "high",
    "low", "close", "settle", "change1", "change2", "vol", "amount",
    "oi", "oi_chg", "delv_settle",
]
FUT_HOLDING_COLS = [
    "trade_date", "symbol", "broker", "vol", "vol_chg",
    "long_hld", "long_chg", "short_hld", "short_chg", "exchange",
]
FUT_WSR_COLS = [
    "trade_date", "symbol", "warehouse", "province", "wh_type",
    "wh_unit", "wh_qty", "wh_chg", "exchange",
]
FUT_SETTLE_COLS = [
    "ts_code", "trade_date", "pre_close", "pre_settle_price",
    "open", "high", "low", "close", "settle_price",
    "zd1_chg", "zd2_chg", "oi", "oi_chg", "vol", "exchange",
]
FUT_MAPPING_COLS = ["ts_code", "trade_date", "mapping_ts_code"]
FT_LIMIT_COLS = [
    "ts_code", "trade_date", "pre_close", "up_limit", "down_limit", "exchange",
]
FUT_WEEKLY_COLS = [
    "ts_code", "trade_date", "pre_close", "pre_settle",
    "open", "high", "low", "close", "settle",
    "vol", "amount", "oi", "exchange",
]
FUT_MONTHLY_COLS = [
    "ts_code", "trade_date", "pre_close", "pre_settle",
    "open", "high", "low", "close", "settle",
    "vol", "amount", "oi", "exchange",
]
FUT_INDEX_DAILY_COLS = [
    "ts_code", "trade_date", "open", "high", "low", "close",
    "settle", "vol", "amount", "exchange",
]
FUT_WEEKLY_DETAIL_COLS = [
    "date", "exchange", "prd", "long_party_name", "long_position",
    "long_change", "short_party_name", "short_position", "short_change",
    "total_position", "net_position",
]
```

- [ ] **Step 3: Update fetcher.py**

Open `zer0share/fetcher.py`. Replace all `*_COLS = [...]` definitions with a single import at the top. Keep `INDEX_DAILY_CODES`, `FUTURES_EXCHANGES`, `OPTIONS_EXCHANGES` as they are (not column definitions):

```python
from zer0share.schema import (
    BASIC_COLS, DAILY_COLS, TRADE_CAL_COLS, ADJ_FACTOR_COLS, DAILY_BASIC_COLS,
    STOCK_ST_COLS, SUSPEND_D_COLS, STK_LIMIT_COLS, INDEX_WEIGHT_COLS,
    INDEX_DAILY_COLS, SW_CLASSIFY_COLS, SW_MEMBER_COLS, CI_MEMBER_COLS,
    OPT_BASIC_COLS, OPT_DAILY_COLS, FUT_BASIC_COLS, FUT_DAILY_COLS,
    FUT_HOLDING_COLS, FUT_WSR_COLS, FUT_SETTLE_COLS, FUT_MAPPING_COLS,
    FT_LIMIT_COLS, FUT_WEEKLY_COLS, FUT_MONTHLY_COLS, FUT_INDEX_DAILY_COLS,
    FUT_WEEKLY_DETAIL_COLS,
)
```

- [ ] **Step 4: Update api.py imports**

In `zer0share/api.py`, find the large `from zer0share.fetcher import ...` block at the top and replace it with an import from schema instead. The import currently pulls in all `*_COLS` names from fetcher; redirect them to schema:

```python
from zer0share.schema import (
    ADJ_FACTOR_COLS, BASIC_COLS, CI_MEMBER_COLS, DAILY_BASIC_COLS, DAILY_COLS,
    INDEX_DAILY_COLS, INDEX_WEIGHT_COLS, STOCK_ST_COLS, STK_LIMIT_COLS,
    SUSPEND_D_COLS, SW_CLASSIFY_COLS, SW_MEMBER_COLS, TRADE_CAL_COLS,
    FUT_BASIC_COLS, FUT_DAILY_COLS, FUT_HOLDING_COLS, FUT_WSR_COLS,
    FUT_SETTLE_COLS, FUT_MAPPING_COLS, FT_LIMIT_COLS, FUT_WEEKLY_COLS,
    FUT_MONTHLY_COLS, FUT_INDEX_DAILY_COLS, FUT_WEEKLY_DETAIL_COLS,
    OPT_BASIC_COLS, OPT_DAILY_COLS,
)
```

- [ ] **Step 5: Run tests**

```bash
uv run pytest --tb=short -q
```

Expected: `258 passed`

- [ ] **Step 6: Commit**

```bash
git add zer0share/schema.py zer0share/fetcher.py zer0share/api.py
git commit -m "refactor: extract column definitions to schema.py"
```

---

## Task 2: Create sync/ skeleton

**Files:**
- Create: `zer0share/sync/__init__.py`
- Create: `zer0share/sync/_helpers.py`

- [ ] **Step 1: Create sync/__init__.py**

```python
from dataclasses import dataclass

from zer0share.config import Config
from zer0share.fetcher import TushareFetcher
from zer0share.notifier import Notifier
from zer0share.storage import MetaStore


@dataclass
class SyncContext:
    cfg: Config
    fetcher: TushareFetcher
    notifier: Notifier
    meta: MetaStore
```

- [ ] **Step 2: Create sync/_helpers.py**

This file contains all constants, utility functions, and the core `sync_daily_partitioned` engine, extracted from `pipeline.py`. Patch targets for tests: `zer0share.sync._helpers.date`, `zer0share.sync._helpers.time`, `zer0share.sync._helpers.FIRST_DATE`.

```python
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Callable

from loguru import logger

from zer0share.storage import daily_partition_exists, write_daily_partition
from zer0share.sync import SyncContext


FIRST_DATE = date(2016, 1, 1)
TRADE_CAL_FIRST_DATE = date(1990, 1, 1)
PROGRESS_INTERVAL = 50
EXCHANGES = ["SSE", "SZSE"]
INDEX_CODES = ["399300.SZ", "000905.SH", "000852.SH"]
ALL_EXCHANGES = ["SSE", "SZSE", "CFFEX", "DCE", "SHFE", "CZCE", "INE", "GFEX"]


def parse_tushare_date(value) -> date:
    import datetime as dt
    if isinstance(value, dt.date):
        return value
    import pandas as pd
    return pd.to_datetime(value, format="%Y%m%d").date()


def should_log_progress(processed: int, total: int) -> bool:
    return processed == total or processed % PROGRESS_INTERVAL == 0


def log_daily_progress(
    table_name: str,
    processed: int,
    total: int,
    trade_date: date,
    success: int,
    empty: int,
    skipped_existing: int,
) -> None:
    percent = processed / total * 100
    logger.info(
        f"{table_name} 同步进度: {processed}/{total} ({percent:.1f}%), "
        f"当前日期 {trade_date}, "
        f"成功 {success} 天, 空数据 {empty} 天, 跳过已存在 {skipped_existing} 天"
    )


def month_ranges(start: date, end: date) -> list[tuple[date, date]]:
    ranges = []
    current = date(start.year, start.month, 1)
    while current <= end:
        next_month = (
            date(current.year + 1, 1, 1)
            if current.month == 12
            else date(current.year, current.month + 1, 1)
        )
        month_start = max(start, current)
        month_end = min(end, next_month - timedelta(days=1))
        ranges.append((month_start, month_end))
        current = next_month
    return ranges


def week_ranges(start: date, end: date) -> list[tuple[str, date]]:
    weeks = []
    seen: set = set()
    current = start
    while current <= end:
        iso_year, iso_week, _ = current.isocalendar()
        week_key = (iso_year, iso_week)
        if week_key not in seen:
            seen.add(week_key)
            week_num = f"{iso_year}{iso_week:02d}"
            monday = current - timedelta(days=current.weekday())
            weeks.append((week_num, monday))
        current += timedelta(days=7)
    return weeks


def index_weight_meta_key(index_code: str) -> str:
    return f"index_weight:{index_code}"


def ensure_trade_cal_loaded(ctx: SyncContext) -> None:
    from zer0share.sync import calendar as cal_module
    if ctx.meta.get_last_date("trade_cal") is None:
        cal_module.sync_trade_cal(ctx)


def skip_if_not_trading(ctx: SyncContext, exchange: str) -> bool:
    ensure_trade_cal_loaded(ctx)
    today = date.today()
    if not ctx.meta.is_trading_day(exchange, today):
        logger.info(f"今日 {today} 非交易日，跳过同步")
        return True
    return False


def sync_daily_partitioned(
    ctx: SyncContext,
    table_name: str,
    fetch: Callable,
    start_date: date | None,
    end_date: date | None,
    write_empty: bool = False,
    data_dir: Path | None = None,
    exchange: str = "SSE",
) -> None:
    base_dir = data_dir or ctx.cfg.data_dir
    today = date.today()
    last = ctx.meta.get_last_date(table_name)
    if start_date is None:
        start = (last + timedelta(days=1)) if last else FIRST_DATE
        end = today
    else:
        start = start_date
        end = end_date or today

    if start_date is None and start > end:
        logger.info(f"{table_name} 已是最新，无需同步")
        return
    if start > end:
        raise ValueError("start_date must be on or before end_date")

    trading_days = ctx.meta.get_trading_days(exchange, start, end)
    if not trading_days and ctx.meta.get_last_date("trade_cal") is None:
        raise RuntimeError(
            f"DuckDB 中无 {exchange} trade_cal 数据，请先运行 "
            "python main.py sync --table trade_cal"
        )
    if not trading_days:
        logger.info("指定范围内无交易日，无需同步")
        return

    success = 0
    empty = 0
    skipped_existing = 0
    frontier = last
    logger.info(
        f"{table_name} 同步开始: {start} ~ {end}, 共 {len(trading_days)} 个交易日"
    )
    for processed, trade_date in enumerate(trading_days, start=1):
        if daily_partition_exists(base_dir, table_name, trade_date):
            skipped_existing += 1
            if should_log_progress(processed, len(trading_days)):
                log_daily_progress(
                    table_name, processed, len(trading_days), trade_date,
                    success, empty, skipped_existing,
                )
            continue
        try:
            df = fetch(trade_date)
            time.sleep(0.2)
            if not df.empty or write_empty:
                write_daily_partition(base_dir, table_name, trade_date, df)
                if frontier is None or trade_date > frontier:
                    ctx.meta.update_last_date(table_name, trade_date)
                    frontier = trade_date
                if df.empty:
                    empty += 1
                else:
                    success += 1
            else:
                empty += 1
        except Exception as e:
            logger.error(f"{table_name} {trade_date} 同步失败: {e}")
            ctx.notifier.send(f"{table_name} {trade_date} 同步失败: {e}")
            raise
        if should_log_progress(processed, len(trading_days)):
            log_daily_progress(
                table_name, processed, len(trading_days), trade_date,
                success, empty, skipped_existing,
            )

    msg = (
        f"{table_name} 同步完成: 成功 {success} 天, "
        f"空数据 {empty} 天, 跳过已存在 {skipped_existing} 天, "
        f"共 {len(trading_days)} 个交易日"
    )
    logger.info(msg)
    ctx.notifier.send(msg)
```

- [ ] **Step 3: Run tests** (pipeline.py untouched, new files not yet imported by anything)

```bash
uv run pytest --tb=short -q
```

Expected: `258 passed`

- [ ] **Step 4: Commit**

```bash
git add zer0share/sync/
git commit -m "refactor: add sync/ package skeleton with SyncContext and helpers"
```

---

## Task 3: Create sync/ domain modules

**Files:**
- Create: `zer0share/sync/calendar.py`
- Create: `zer0share/sync/equities.py`
- Create: `zer0share/sync/industry.py`
- Create: `zer0share/sync/futures.py`
- Create: `zer0share/sync/options.py`

These modules contain sync logic moved from `pipeline.py`. The transformation rule for every method: replace `self._cfg` → `ctx.cfg`, `self._fetcher` → `ctx.fetcher`, `self._notifier` → `ctx.notifier`, `self._meta` → `ctx.meta`. Remove `self` parameter, add `ctx: SyncContext` as first parameter.

- [ ] **Step 1: Create sync/calendar.py**

```python
import pandas as pd
from datetime import date, timedelta
from loguru import logger

from zer0share.storage import read_trade_cal, write_trade_cal
from zer0share.sync import SyncContext
from zer0share.sync._helpers import (
    ALL_EXCHANGES, TRADE_CAL_FIRST_DATE, parse_tushare_date,
)


def _merge_trade_cal(existing: pd.DataFrame, fetched: pd.DataFrame) -> pd.DataFrame:
    if existing.empty:
        return fetched
    if fetched.empty:
        return existing
    return (
        pd.concat([existing, fetched], ignore_index=True)
        .drop_duplicates(subset=["exchange", "cal_date"], keep="last")
        .sort_values(["exchange", "cal_date"])
        .reset_index(drop=True)
    )


def sync_trade_cal(ctx: SyncContext) -> None:
    try:
        end = date(date.today().year, 12, 31)
        max_dates: list[date] = []
        for exchange in ALL_EXCHANGES:
            existing = read_trade_cal(ctx.cfg.data_dir, exchange)
            last = (
                parse_tushare_date(existing["cal_date"].max())
                if not existing.empty
                else None
            )
            start = (last + timedelta(days=1)) if last else TRADE_CAL_FIRST_DATE

            if start <= end:
                fetched = ctx.fetcher.fetch_trade_cal(exchange, start, end)
                df = _merge_trade_cal(existing, fetched)
                write_trade_cal(ctx.cfg.data_dir, exchange, df)
                logger.info(
                    f"trade_cal {exchange} 写入完成: 新增 {len(fetched)} 条, "
                    f"共 {len(df)} 条"
                )
            else:
                df = existing
                logger.info(f"trade_cal {exchange} 已覆盖到 {last}，无需同步")

            if not df.empty:
                max_dates.append(parse_tushare_date(df["cal_date"].max()))

        ctx.meta.load_trade_cal_from_parquet(ctx.cfg.data_dir, ALL_EXCHANGES)
        if max_dates:
            ctx.meta.update_last_date("trade_cal", min(max_dates))
        logger.info("trade_cal 全部同步完成")
    except Exception as e:
        logger.error(f"trade_cal 同步失败: {e}")
        ctx.notifier.send(f"trade_cal 同步失败: {e}")
        raise
```

- [ ] **Step 2: Create sync/equities.py**

```python
import time
from datetime import date, timedelta
from loguru import logger

import pandas as pd

from zer0share.storage import (
    daily_partition_exists, write_basic, write_adj_factor, write_daily_kline,
    write_daily_partition, write_index_weight, index_weight_partition_exists,
    adj_factor_partition_exists, daily_kline_partition_exists,
)
from zer0share.sync import SyncContext
from zer0share.sync._helpers import (
    FIRST_DATE, INDEX_CODES, index_weight_meta_key, log_daily_progress,
    month_ranges, parse_tushare_date, should_log_progress, skip_if_not_trading,
    sync_daily_partitioned,
)
from zer0share.fetcher import INDEX_DAILY_CODES


def sync_basic(ctx: SyncContext) -> None:
    if skip_if_not_trading(ctx, "SSE"):
        return
    today = date.today()
    try:
        df = ctx.fetcher.fetch_basic()
        write_basic(ctx.cfg.data_dir, df)
        ctx.meta.update_last_date("basic", today)
        logger.info(f"basic 同步完成: {len(df)} 条")
    except Exception as e:
        logger.error(f"basic 同步失败: {e}")
        ctx.notifier.send(f"basic 同步失败: {e}")
        raise


def sync_daily_kline(ctx: SyncContext, start_date: date | None = None, end_date: date | None = None) -> None:
    sync_daily_partitioned(ctx, "daily_kline", ctx.fetcher.fetch_daily_kline, start_date, end_date)


def sync_adj_factor(ctx: SyncContext, start_date: date | None = None, end_date: date | None = None) -> None:
    sync_daily_partitioned(ctx, "adj_factor", ctx.fetcher.fetch_adj_factor, start_date, end_date)


def sync_daily_basic(ctx: SyncContext, start_date: date | None = None, end_date: date | None = None) -> None:
    sync_daily_partitioned(ctx, "daily_basic", ctx.fetcher.fetch_daily_basic, start_date, end_date)


def sync_stock_st(ctx: SyncContext, start_date: date | None = None, end_date: date | None = None) -> None:
    sync_daily_partitioned(ctx, "stock_st", ctx.fetcher.fetch_stock_st, start_date, end_date, write_empty=True)


def sync_suspend_d(ctx: SyncContext, start_date: date | None = None, end_date: date | None = None) -> None:
    sync_daily_partitioned(ctx, "suspend_d", ctx.fetcher.fetch_suspend_d, start_date, end_date, write_empty=True)


def sync_stk_limit(ctx: SyncContext, start_date: date | None = None, end_date: date | None = None) -> None:
    sync_daily_partitioned(ctx, "stk_limit", ctx.fetcher.fetch_stk_limit, start_date, end_date)


def sync_index_weight(ctx: SyncContext, start_date: date | None = None, end_date: date | None = None) -> None:
    today = date.today()
    end = end_date or today
    if start_date is not None and start_date > end:
        raise ValueError("start_date must be on or before end_date")

    success = 0
    skipped_existing = 0
    empty_months = 0
    requests = 0
    coverage_dates: list[date] = []
    for index_code in INDEX_CODES:
        meta_key = index_weight_meta_key(index_code)
        last = ctx.meta.get_last_date(meta_key)
        start = start_date or ((last + timedelta(days=1)) if last else FIRST_DATE)
        if start > end:
            logger.info(f"index_weight {index_code} 已覆盖到 {last}，无需同步")
            if last is not None:
                coverage_dates.append(last)
            continue

        ranges = month_ranges(start, end)
        logger.info(
            f"index_weight {index_code} 同步开始: {start} ~ {end}, "
            f"共 {len(ranges)} 个月度窗口"
        )
        try:
            for processed, (month_start, month_end) in enumerate(ranges, start=1):
                df = ctx.fetcher.fetch_index_weight(index_code, month_start, month_end)
                requests += 1
                time.sleep(0.2)
                if df.empty:
                    empty_months += 1
                else:
                    for trade_date_value, part in df.groupby("trade_date"):
                        trade_date = parse_tushare_date(trade_date_value)
                        if index_weight_partition_exists(ctx.cfg.data_dir, index_code, trade_date):
                            skipped_existing += 1
                            continue
                        write_index_weight(ctx.cfg.data_dir, index_code, trade_date, part)
                        success += 1

                if should_log_progress(processed, len(ranges)):
                    percent = processed / len(ranges) * 100
                    logger.info(
                        f"index_weight {index_code} 同步进度: "
                        f"{processed}/{len(ranges)} ({percent:.1f}%), "
                        f"当前窗口 {month_start} ~ {month_end}, "
                        f"成功 {success} 个分区, 空窗口 {empty_months} 个, "
                        f"跳过已存在 {skipped_existing} 个分区"
                    )

            frontier = max(last, end) if last is not None else end
            ctx.meta.update_last_date(meta_key, frontier)
            coverage_dates.append(frontier)
        except Exception as e:
            logger.error(f"index_weight {index_code} 同步失败: {e}")
            ctx.notifier.send(f"index_weight {index_code} 同步失败: {e}")
            raise

    if coverage_dates:
        ctx.meta.update_last_date("index_weight", min(coverage_dates))

    msg = (
        f"index_weight 同步完成: 成功 {success} 个分区, "
        f"空窗口 {empty_months} 个, 跳过已存在 {skipped_existing} 个分区, "
        f"请求 {requests} 次"
    )
    logger.info(msg)
    ctx.notifier.send(msg)


def sync_index_daily(ctx: SyncContext, start_date: date | None = None, end_date: date | None = None) -> None:
    today = date.today()
    last = ctx.meta.get_last_date("index_daily")

    if start_date is None:
        start = (last + timedelta(days=1)) if last else FIRST_DATE
        end = today
    else:
        start = start_date
        end = end_date or today

    if start_date is None and start > end:
        logger.info("index_daily 已是最新，无需同步")
        return
    if start > end:
        raise ValueError("start_date must be on or before end_date")

    logger.info(f"index_daily 同步开始: {start} ~ {end}, 共 {len(INDEX_DAILY_CODES)} 个指数")
    all_frames = []
    for ts_code in INDEX_DAILY_CODES:
        try:
            df = ctx.fetcher.fetch_index_daily(ts_code, start, end)
            time.sleep(0.2)
            if not df.empty:
                all_frames.append(df)
        except Exception as e:
            logger.error(f"index_daily {ts_code} 拉取失败: {e}")
            ctx.notifier.send(f"index_daily {ts_code} 拉取失败: {e}")
            continue

    if not all_frames:
        msg = "index_daily 无数据，跳过"
        logger.info(msg)
        ctx.notifier.send(msg)
        return

    combined = pd.concat(all_frames, ignore_index=True)
    success = 0
    skipped_existing = 0
    frontier = last

    for trade_date_value, part in combined.groupby("trade_date"):
        trade_date = parse_tushare_date(trade_date_value)
        if daily_partition_exists(ctx.cfg.data_dir, "index_daily", trade_date):
            skipped_existing += 1
            continue
        write_daily_partition(
            ctx.cfg.data_dir, "index_daily", trade_date, part.reset_index(drop=True)
        )
        if frontier is None or trade_date > frontier:
            ctx.meta.update_last_date("index_daily", trade_date)
            frontier = trade_date
        success += 1

    msg = (
        f"index_daily 同步完成: 成功 {success} 天, "
        f"跳过已存在 {skipped_existing} 天, 共 {len(INDEX_DAILY_CODES)} 个指数"
    )
    logger.info(msg)
    ctx.notifier.send(msg)
```

- [ ] **Step 3: Create sync/industry.py**

```python
from datetime import date
from loguru import logger

from zer0share.storage import write_sw_classify, write_sw_member, write_ci_member
from zer0share.sync import SyncContext
from zer0share.sync._helpers import skip_if_not_trading


def sync_industry(ctx: SyncContext) -> None:
    if skip_if_not_trading(ctx, "SSE"):
        return
    today = date.today()
    try:
        df = ctx.fetcher.fetch_sw_classify()
        write_sw_classify(ctx.cfg.data_dir, df)
        ctx.meta.update_last_date("sw_classify", today)
        logger.info(f"sw_classify 同步完成: {len(df)} 条")

        df = ctx.fetcher.fetch_sw_member()
        write_sw_member(ctx.cfg.data_dir, df)
        ctx.meta.update_last_date("sw_member", today)
        logger.info(f"sw_member 同步完成: {len(df)} 条")
    except Exception as e:
        logger.error(f"industry 同步失败: {e}")
        ctx.notifier.send(f"industry 同步失败: {e}")
        raise


def sync_ci_member(ctx: SyncContext) -> None:
    if skip_if_not_trading(ctx, "SSE"):
        return
    today = date.today()
    try:
        df = ctx.fetcher.fetch_ci_member()
        write_ci_member(ctx.cfg.data_dir, df)
        ctx.meta.update_last_date("ci_member", today)
        logger.info(f"ci_member 同步完成: {len(df)} 条")
    except Exception as e:
        logger.error(f"ci_member 同步失败: {e}")
        ctx.notifier.send(f"ci_member 同步失败: {e}")
        raise
```

- [ ] **Step 4: Create sync/futures.py**

```python
import time
from datetime import date, timedelta
from loguru import logger

import pandas as pd

from zer0share.storage import daily_partition_exists, write_daily_partition
from zer0share.sync import SyncContext
from zer0share.sync._helpers import (
    FIRST_DATE, parse_tushare_date, skip_if_not_trading, sync_daily_partitioned,
    week_ranges,
)
from zer0share.fetcher import FUTURES_EXCHANGES


def sync_fut_basic(ctx: SyncContext) -> None:
    if skip_if_not_trading(ctx, "SSE"):
        return
    today = date.today()
    futures_dir = ctx.cfg.data_dir / "futures"
    all_frames = []
    try:
        for exchange in FUTURES_EXCHANGES:
            for fut_type in ("1", "2"):
                df = ctx.fetcher.fetch_fut_basic(exchange, fut_type)
                time.sleep(0.2)
                if not df.empty:
                    all_frames.append(df)
        combined = pd.concat(all_frames, ignore_index=True) if all_frames else pd.DataFrame()
        write_daily_partition(futures_dir, "fut_basic", today, combined)
        ctx.meta.update_last_date("fut_basic", today)
        logger.info(f"fut_basic 同步完成: {len(combined)} 条")
    except Exception as e:
        logger.error(f"fut_basic 同步失败: {e}")
        ctx.notifier.send(f"fut_basic 同步失败: {e}")
        raise


def sync_fut_daily(ctx: SyncContext, start_date: date | None = None, end_date: date | None = None) -> None:
    sync_daily_partitioned(
        ctx, "fut_daily", ctx.fetcher.fetch_fut_daily, start_date, end_date,
        data_dir=ctx.cfg.data_dir / "futures",
    )


def sync_fut_holding(ctx: SyncContext, start_date: date | None = None, end_date: date | None = None) -> None:
    sync_daily_partitioned(
        ctx, "fut_holding", ctx.fetcher.fetch_fut_holding, start_date, end_date,
        data_dir=ctx.cfg.data_dir / "futures",
    )


def sync_fut_wsr(ctx: SyncContext, start_date: date | None = None, end_date: date | None = None) -> None:
    sync_daily_partitioned(
        ctx, "fut_wsr", ctx.fetcher.fetch_fut_wsr, start_date, end_date,
        data_dir=ctx.cfg.data_dir / "futures",
    )


def sync_fut_settle(ctx: SyncContext, start_date: date | None = None, end_date: date | None = None) -> None:
    sync_daily_partitioned(
        ctx, "fut_settle", ctx.fetcher.fetch_fut_settle, start_date, end_date,
        data_dir=ctx.cfg.data_dir / "futures",
    )


def sync_fut_mapping(ctx: SyncContext, start_date: date | None = None, end_date: date | None = None) -> None:
    sync_daily_partitioned(
        ctx, "fut_mapping", ctx.fetcher.fetch_fut_mapping, start_date, end_date,
        data_dir=ctx.cfg.data_dir / "futures",
    )


def sync_ft_limit(ctx: SyncContext, start_date: date | None = None, end_date: date | None = None) -> None:
    sync_daily_partitioned(
        ctx, "ft_limit", ctx.fetcher.fetch_ft_limit, start_date, end_date,
        data_dir=ctx.cfg.data_dir / "futures",
    )


def sync_fut_weekly(ctx: SyncContext, start_date: date | None = None, end_date: date | None = None) -> None:
    sync_daily_partitioned(
        ctx, "fut_weekly", ctx.fetcher.fetch_fut_weekly, start_date, end_date,
        data_dir=ctx.cfg.data_dir / "futures",
    )


def sync_fut_monthly(ctx: SyncContext, start_date: date | None = None, end_date: date | None = None) -> None:
    sync_daily_partitioned(
        ctx, "fut_monthly", ctx.fetcher.fetch_fut_monthly, start_date, end_date,
        data_dir=ctx.cfg.data_dir / "futures",
    )


def sync_fut_index_daily(ctx: SyncContext, start_date: date | None = None, end_date: date | None = None) -> None:
    if skip_if_not_trading(ctx, "SSE"):
        return
    today = date.today()
    last = ctx.meta.get_last_date("fut_index_daily")

    if start_date is None:
        start = (last + timedelta(days=1)) if last else FIRST_DATE
        end = today
    else:
        start = start_date
        end = end_date or today

    if start_date is None and start > end:
        logger.info("fut_index_daily 已是最新，无需同步")
        return
    if start > end:
        raise ValueError("start_date must be on or before end_date")

    logger.info(f"fut_index_daily 同步开始: {start} ~ {end}")
    futures_dir = ctx.cfg.data_dir / "futures"
    all_frames = []
    current = start
    while current <= end:
        try:
            df = ctx.fetcher.fetch_fut_index_daily(current)
            time.sleep(0.2)
            if not df.empty:
                all_frames.append(df)
        except Exception as e:
            logger.error(f"fut_index_daily {current} 拉取失败: {e}")
            ctx.notifier.send(f"fut_index_daily {current} 拉取失败: {e}")
        current += timedelta(days=1)

    if not all_frames:
        msg = "fut_index_daily 无数据，跳过"
        logger.info(msg)
        ctx.notifier.send(msg)
        return

    combined = pd.concat(all_frames, ignore_index=True)
    success = 0
    skipped_existing = 0
    frontier = last

    for trade_date_value, part in combined.groupby("trade_date"):
        trade_date = parse_tushare_date(trade_date_value)
        if daily_partition_exists(futures_dir, "fut_index_daily", trade_date):
            skipped_existing += 1
            continue
        write_daily_partition(futures_dir, "fut_index_daily", trade_date, part.reset_index(drop=True))
        if frontier is None or trade_date > frontier:
            ctx.meta.update_last_date("fut_index_daily", trade_date)
            frontier = trade_date
        success += 1

    msg = (
        f"fut_index_daily 同步完成: 成功 {success} 天, "
        f"跳过已存在 {skipped_existing} 天"
    )
    logger.info(msg)
    ctx.notifier.send(msg)


def sync_fut_weekly_detail(ctx: SyncContext, start_date: date | None = None, end_date: date | None = None) -> None:
    today = date.today()
    last = ctx.meta.get_last_date("fut_weekly_detail")

    if start_date is None:
        start = (last + timedelta(days=1)) if last else FIRST_DATE
        end = today
    else:
        start = start_date
        end = end_date or today

    if start > end:
        raise ValueError("start_date must be on or before end_date")

    futures_dir = ctx.cfg.data_dir / "futures"
    success = 0
    skipped_existing = 0
    frontier = last
    weeks = week_ranges(start, end)
    logger.info(f"fut_weekly_detail 同步开始: {start} ~ {end}, 共 {len(weeks)} 个周")

    for week_num, week_start in weeks:
        try:
            df = ctx.fetcher.fetch_fut_weekly_detail(week_num)
            time.sleep(0.2)
            if df.empty:
                continue
            if daily_partition_exists(futures_dir, "fut_weekly_detail", week_start):
                skipped_existing += 1
                continue
            write_daily_partition(futures_dir, "fut_weekly_detail", week_start, df)
            if frontier is None or week_start > frontier:
                ctx.meta.update_last_date("fut_weekly_detail", week_start)
                frontier = week_start
            success += 1
        except Exception as e:
            logger.error(f"fut_weekly_detail {week_num} 同步失败: {e}")
            ctx.notifier.send(f"fut_weekly_detail {week_num} 同步失败: {e}")
            raise

    msg = (
        f"fut_weekly_detail 同步完成: 成功 {success} 周, "
        f"跳过已存在 {skipped_existing} 周, 共 {len(weeks)} 周"
    )
    logger.info(msg)
    ctx.notifier.send(msg)
```

- [ ] **Step 5: Create sync/options.py**

```python
import time
from datetime import date
from loguru import logger

import pandas as pd

from zer0share.storage import write_opt_basic, write_daily_partition
from zer0share.sync import SyncContext
from zer0share.sync._helpers import skip_if_not_trading, sync_daily_partitioned
from zer0share.fetcher import OPTIONS_EXCHANGES


def sync_opt_basic(ctx: SyncContext) -> None:
    if skip_if_not_trading(ctx, "SSE"):
        return
    today = date.today()
    options_dir = ctx.cfg.data_dir / "options"
    all_frames = []
    try:
        for exchange in OPTIONS_EXCHANGES:
            df = ctx.fetcher.fetch_opt_basic(exchange)
            time.sleep(0.2)
            if not df.empty:
                all_frames.append(df)
        combined = pd.concat(all_frames, ignore_index=True) if all_frames else pd.DataFrame()
        write_opt_basic(options_dir, combined)
        ctx.meta.update_last_date("opt_basic", today)
        logger.info(f"opt_basic 同步完成: {len(combined)} 条")
    except Exception as e:
        logger.error(f"opt_basic 同步失败: {e}")
        ctx.notifier.send(f"opt_basic 同步失败: {e}")
        raise


def sync_opt_daily(ctx: SyncContext, start_date: date | None = None, end_date: date | None = None) -> None:
    sync_daily_partitioned(
        ctx, "opt_daily", ctx.fetcher.fetch_opt_daily, start_date, end_date,
        data_dir=ctx.cfg.data_dir / "options",
    )
```

- [ ] **Step 6: Run tests** (domain modules created but not yet used by anything)

```bash
uv run pytest --tb=short -q
```

Expected: `258 passed`

- [ ] **Step 7: Commit**

```bash
git add zer0share/sync/
git commit -m "refactor: add sync/ domain modules (calendar, equities, industry, futures, options)"
```

---

## Task 4: Replace pipeline.py with thin facade + update test_pipeline.py

**Files:**
- Modify: `zer0share/pipeline.py` (complete rewrite)
- Modify: `tests/test_pipeline.py` (update imports and patch paths)

- [ ] **Step 1: Rewrite pipeline.py**

Replace the entire contents of `zer0share/pipeline.py` with the following thin facade:

```python
from datetime import date

from zer0share.config import Config
from zer0share.fetcher import TushareFetcher
from zer0share.notifier import Notifier
from zer0share.storage import MetaStore
from zer0share.sync import SyncContext
from zer0share.sync import calendar, equities, industry, futures, options
from zer0share.sync._helpers import EXCHANGES, ALL_EXCHANGES


class Pipeline:
    def __init__(self, cfg: Config, fetcher: TushareFetcher, notifier: Notifier):
        self._ctx = SyncContext(cfg, fetcher, notifier, MetaStore(cfg.db_path))

    # For tests that access pipeline._meta / pipeline._fetcher / pipeline._notifier directly:
    @property
    def _meta(self):
        return self._ctx.meta

    @property
    def _fetcher(self):
        return self._ctx.fetcher

    @property
    def _notifier(self):
        return self._ctx.notifier

    # Calendar
    def sync_trade_cal(self):
        calendar.sync_trade_cal(self._ctx)

    # Equities
    def sync_basic(self):
        equities.sync_basic(self._ctx)

    def sync_daily_kline(self, start_date: date | None = None, end_date: date | None = None):
        equities.sync_daily_kline(self._ctx, start_date, end_date)

    def sync_adj_factor(self, start_date: date | None = None, end_date: date | None = None):
        equities.sync_adj_factor(self._ctx, start_date, end_date)

    def sync_daily_basic(self, start_date: date | None = None, end_date: date | None = None):
        equities.sync_daily_basic(self._ctx, start_date, end_date)

    def sync_stock_st(self, start_date: date | None = None, end_date: date | None = None):
        equities.sync_stock_st(self._ctx, start_date, end_date)

    def sync_suspend_d(self, start_date: date | None = None, end_date: date | None = None):
        equities.sync_suspend_d(self._ctx, start_date, end_date)

    def sync_stk_limit(self, start_date: date | None = None, end_date: date | None = None):
        equities.sync_stk_limit(self._ctx, start_date, end_date)

    def sync_index_weight(self, start_date: date | None = None, end_date: date | None = None):
        equities.sync_index_weight(self._ctx, start_date, end_date)

    def sync_index_daily(self, start_date: date | None = None, end_date: date | None = None):
        equities.sync_index_daily(self._ctx, start_date, end_date)

    # Industry
    def sync_industry(self):
        industry.sync_industry(self._ctx)

    def sync_ci_member(self):
        industry.sync_ci_member(self._ctx)

    # Futures
    def sync_fut_basic(self):
        futures.sync_fut_basic(self._ctx)

    def sync_fut_daily(self, start_date: date | None = None, end_date: date | None = None):
        futures.sync_fut_daily(self._ctx, start_date, end_date)

    def sync_fut_holding(self, start_date: date | None = None, end_date: date | None = None):
        futures.sync_fut_holding(self._ctx, start_date, end_date)

    def sync_fut_wsr(self, start_date: date | None = None, end_date: date | None = None):
        futures.sync_fut_wsr(self._ctx, start_date, end_date)

    def sync_fut_settle(self, start_date: date | None = None, end_date: date | None = None):
        futures.sync_fut_settle(self._ctx, start_date, end_date)

    def sync_fut_mapping(self, start_date: date | None = None, end_date: date | None = None):
        futures.sync_fut_mapping(self._ctx, start_date, end_date)

    def sync_ft_limit(self, start_date: date | None = None, end_date: date | None = None):
        futures.sync_ft_limit(self._ctx, start_date, end_date)

    def sync_fut_weekly(self, start_date: date | None = None, end_date: date | None = None):
        futures.sync_fut_weekly(self._ctx, start_date, end_date)

    def sync_fut_monthly(self, start_date: date | None = None, end_date: date | None = None):
        futures.sync_fut_monthly(self._ctx, start_date, end_date)

    def sync_fut_index_daily(self, start_date: date | None = None, end_date: date | None = None):
        futures.sync_fut_index_daily(self._ctx, start_date, end_date)

    def sync_fut_weekly_detail(self, start_date: date | None = None, end_date: date | None = None):
        futures.sync_fut_weekly_detail(self._ctx, start_date, end_date)

    # Options
    def sync_opt_basic(self):
        options.sync_opt_basic(self._ctx)

    def sync_opt_daily(self, start_date: date | None = None, end_date: date | None = None):
        options.sync_opt_daily(self._ctx, start_date, end_date)

    def close(self):
        self._ctx.meta.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
        return False
```

- [ ] **Step 2: Update test_pipeline.py imports and patch paths**

Apply these changes to `tests/test_pipeline.py`:

**Imports block (lines 7-11) — replace with:**
```python
from zer0share.pipeline import Pipeline
from zer0share.sync._helpers import EXCHANGES, ALL_EXCHANGES as NEW_ALL_EXCHANGES
from zer0share.storage import read_sw_classify, read_sw_member, read_ci_member, write_basic, write_trade_cal
from zer0share.fetcher import INDEX_DAILY_CODES, FUTURES_EXCHANGES, OPTIONS_EXCHANGES
from zer0share.storage import daily_partition_exists
```

**Patch path mapping** — apply the following sed-style replacements throughout the file:

| Old patch target | New patch target |
|---|---|
| `zer0share.pipeline.Pipeline._skip_if_not_trading` (in tests for `sync_basic` / failure) | `zer0share.sync.equities.skip_if_not_trading` |
| `zer0share.pipeline.Pipeline._skip_if_not_trading` (in tests for `sync_industry` / `sync_ci_member`) | `zer0share.sync.industry.skip_if_not_trading` |
| `zer0share.pipeline.Pipeline._skip_if_not_trading` (in tests for `sync_fut_basic`) | `zer0share.sync.futures.skip_if_not_trading` |
| `zer0share.pipeline.Pipeline._skip_if_not_trading` (in tests for `sync_opt_basic`) | `zer0share.sync.options.skip_if_not_trading` |
| `zer0share.pipeline.date` (in tests for `sync_daily_kline`, `sync_adj_factor`, `sync_daily_basic`, `sync_stock_st`, `sync_suspend_d`, `sync_stk_limit`, `sync_fut_daily`, `sync_fut_weekly_detail`, and other `_sync_daily_partitioned`-based methods) | `zer0share.sync._helpers.date` |
| `zer0share.pipeline.date` (in tests for `sync_basic`, `sync_industry`, `sync_ci_member`) | `zer0share.sync.industry.date` or `zer0share.sync.equities.date` respectively (note: `sync_basic` tests don't patch date; only `sync_industry`/`sync_ci_member` do → use `zer0share.sync.industry.date`) |
| `zer0share.pipeline.date` (in tests for `sync_index_weight`, `sync_index_daily`) | `zer0share.sync.equities.date` |
| `zer0share.pipeline.date` (in tests for `sync_fut_basic`, `sync_fut_index_daily`) | `zer0share.sync.futures.date` |
| `zer0share.pipeline.date` (in tests for `sync_opt_basic`) | `zer0share.sync.options.date` |
| `zer0share.pipeline.time.sleep` (in tests for `_sync_daily_partitioned`-based methods: `sync_daily_kline`, `sync_fut_daily`, etc.) | `zer0share.sync._helpers.time.sleep` |
| `zer0share.pipeline.time.sleep` (in tests for `sync_index_daily`, `sync_index_weight`) | `zer0share.sync.equities.time.sleep` |
| `zer0share.pipeline.time.sleep` (in tests for `sync_fut_basic`, `sync_fut_index_daily`, `sync_fut_weekly_detail`) | `zer0share.sync.futures.time.sleep` |
| `zer0share.pipeline.time.sleep` (in tests for `sync_opt_basic`) | `zer0share.sync.options.time.sleep` |
| `zer0share.pipeline.INDEX_CODES` | `zer0share.sync.equities.INDEX_CODES` |
| `zer0share.pipeline.FIRST_DATE` | `zer0share.sync._helpers.FIRST_DATE` |
| `zer0share.pipeline.logger.info` (in `sync_index_daily` tests) | `zer0share.sync.equities.logger.info` |

To identify which tests belong to which domain, use the test function name. Run `grep -n "patch" tests/test_pipeline.py` and read the surrounding test function name to determine which domain module's patch path to use.

- [ ] **Step 3: Run tests**

```bash
uv run pytest tests/test_pipeline.py --tb=short -q
```

Expected: all pipeline tests pass. If a patch target is wrong the test will pass unexpectedly or fail with `AssertionError` on a mock assertion — look at the test function name, identify which domain module it tests, and apply the correct patch path from the table above.

- [ ] **Step 4: Run full suite**

```bash
uv run pytest --tb=short -q
```

Expected: `258 passed`

- [ ] **Step 5: Commit**

```bash
git add zer0share/pipeline.py tests/test_pipeline.py
git commit -m "refactor: replace pipeline.py with thin facade, update test patch paths"
```

---

## Task 5: Create query/ skeleton

**Files:**
- Create: `zer0share/query/__init__.py`
- Create: `zer0share/query/_helpers.py`

- [ ] **Step 1: Create query/__init__.py**

```python
from dataclasses import dataclass
from pathlib import Path


@dataclass
class QueryContext:
    data_dir: Path
```

- [ ] **Step 2: Create query/_helpers.py**

```python
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd

from zer0share.query import QueryContext


def parse_fields(fields, default_columns: list[str]) -> list[str]:
    if fields is None:
        return list(default_columns)
    if isinstance(fields, str):
        parsed = [f.strip() for f in fields.split(",") if f.strip()]
    else:
        parsed = list(fields)
    unknown = [f for f in parsed if f not in default_columns]
    if unknown:
        raise ValueError(f"unknown fields: {', '.join(unknown)}")
    return parsed


def parse_date(value: str):
    try:
        return datetime.strptime(value, "%Y%m%d").date()
    except ValueError as e:
        raise ValueError(f"invalid date format: {value}; expected YYYYMMDD") from e


def parse_is_open(value) -> bool:
    if isinstance(value, bool):
        return value
    if value in (1, "1"):
        return True
    if value in (0, "0"):
        return False
    raise ValueError("is_open must be one of True, False, 1, 0, '1', or '0'")


def format_date_columns(df: pd.DataFrame, date_columns: list[str]) -> pd.DataFrame:
    for column in date_columns:
        if column not in df.columns:
            continue
        formatted = pd.to_datetime(df[column], errors="coerce").dt.strftime("%Y%m%d")
        df[column] = formatted.astype(object)
        df.loc[formatted.isna(), column] = None
    return df


def query_daily_partitioned(
    ctx: QueryContext,
    table_name: str,
    sync_table: str,
    columns: list[str],
    ts_code,
    trade_date,
    start_date,
    end_date,
    fields,
    extra_filters: dict | None = None,
    data_dir_override: Path | None = None,
    order_by: str = "ts_code, trade_date",
    limit: int | None = None,
    offset: int | None = None,
) -> pd.DataFrame:
    if trade_date is not None and (start_date is not None or end_date is not None):
        raise ValueError("trade_date cannot be combined with start_date or end_date")
    parsed_start = parse_date(start_date) if start_date is not None else None
    parsed_end = parse_date(end_date) if end_date is not None else None
    if parsed_start is not None and parsed_end is not None and parsed_end < parsed_start:
        raise ValueError("end_date must be on or after start_date")

    base_dir = data_dir_override or ctx.data_dir
    table_dir = base_dir / table_name
    if not table_dir.exists():
        raise FileNotFoundError(
            f"{sync_table} data not found; run `python main.py sync --table {sync_table}` first"
        )

    selected = parse_fields(fields, columns)
    where = []
    params = []
    if ts_code is not None:
        codes = [c.strip() for c in ts_code.split(",") if c.strip()]
        placeholders = ", ".join("?" for _ in codes)
        where.append(f"ts_code IN ({placeholders})")
        params.extend(codes)
    if trade_date is not None:
        where.append("trade_date = ?")
        params.append(parse_date(trade_date).strftime("%Y%m%d"))
    if parsed_start is not None:
        where.append("trade_date >= ?")
        params.append(parsed_start.strftime("%Y%m%d"))
    if parsed_end is not None:
        where.append("trade_date <= ?")
        params.append(parsed_end.strftime("%Y%m%d"))
    if extra_filters is not None:
        for col, val in extra_filters.items():
            where.append(f"{col} = ?")
            params.append(val)

    pattern = table_dir / "date=*" / "data.parquet"
    sql = (
        f"SELECT {', '.join(selected)} "
        "FROM read_parquet(?, hive_partitioning=true, union_by_name=true)"
    )
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += f" ORDER BY {order_by}"
    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)
    if offset is not None:
        sql += " OFFSET ?"
        params.append(offset)

    return duckdb.connect().execute(sql, [str(pattern), *params]).fetchdf()
```

- [ ] **Step 3: Run tests**

```bash
uv run pytest --tb=short -q
```

Expected: `258 passed`

- [ ] **Step 4: Commit**

```bash
git add zer0share/query/
git commit -m "refactor: add query/ package skeleton with QueryContext and helpers"
```

---

## Task 6: Create query/ domain modules

**Files:**
- Create: `zer0share/query/calendar.py`
- Create: `zer0share/query/equities.py`
- Create: `zer0share/query/industry.py`
- Create: `zer0share/query/futures.py`
- Create: `zer0share/query/options.py`

- [ ] **Step 1: Create query/calendar.py**

```python
import duckdb
import pandas as pd

from zer0share.query import QueryContext
from zer0share.query._helpers import parse_date, parse_fields, parse_is_open
from zer0share.schema import TRADE_CAL_COLS


def trade_cal(
    ctx: QueryContext,
    exchange: str = "SSE",
    start_date=None,
    end_date=None,
    is_open=None,
    fields=None,
    limit: int | None = None,
    offset: int | None = None,
) -> pd.DataFrame:
    """Query trading calendar. Returns cal_date, is_open, pretrade_date per exchange."""
    trade_cal_dir = ctx.data_dir / "trade_cal"
    if not trade_cal_dir.exists():
        raise FileNotFoundError(
            "trade_cal data not found; run `python main.py sync --table trade_cal` first"
        )

    columns = parse_fields(fields, TRADE_CAL_COLS)
    where = ["exchange = ?"]
    params = [exchange]
    parsed_start = parse_date(start_date) if start_date is not None else None
    parsed_end = parse_date(end_date) if end_date is not None else None
    if parsed_start is not None and parsed_end is not None and parsed_end < parsed_start:
        raise ValueError("end_date must be on or after start_date")
    if parsed_start is not None:
        where.append("cal_date >= ?")
        params.append(parsed_start.strftime("%Y%m%d"))
    if parsed_end is not None:
        where.append("cal_date <= ?")
        params.append(parsed_end.strftime("%Y%m%d"))
    if is_open is not None:
        where.append("is_open = ?")
        params.append(parse_is_open(is_open))

    pattern = trade_cal_dir / "exchange=*" / "data.parquet"
    sql = (
        f"SELECT {', '.join(columns)} FROM read_parquet(?, hive_partitioning=true) "
        f"WHERE {' AND '.join(where)} ORDER BY exchange, cal_date"
    )
    if limit is not None:
        sql += " LIMIT ?"; params.append(limit)
    if offset is not None:
        sql += " OFFSET ?"; params.append(offset)
    return duckdb.connect().execute(sql, [str(pattern), *params]).fetchdf()
```

- [ ] **Step 2: Create query/equities.py**

```python
import duckdb
import pandas as pd
from pathlib import Path
from datetime import timedelta

from zer0share.query import QueryContext
from zer0share.query._helpers import (
    format_date_columns, parse_date, parse_fields, query_daily_partitioned,
)
from zer0share.schema import (
    BASIC_COLS, DAILY_COLS, ADJ_FACTOR_COLS, DAILY_BASIC_COLS,
    STOCK_ST_COLS, SUSPEND_D_COLS, STK_LIMIT_COLS,
    INDEX_WEIGHT_COLS, INDEX_DAILY_COLS,
)

UNIVERSE_COLS = ["trade_date", "universe", "ts_code"]


def stock_basic(ctx: QueryContext, ts_code=None, name=None, market=None,
                list_status="L", exchange=None, is_hs=None, fields=None,
                limit: int | None = None, offset: int | None = None) -> pd.DataFrame:
    """Query stock basic info (name, market, list_status, etc.)."""
    path = ctx.data_dir / "basic" / "data.parquet"
    if not path.exists():
        raise FileNotFoundError("basic data not found; run `python main.py sync --table basic` first")

    columns = parse_fields(fields, BASIC_COLS)
    where = []
    params = []
    if ts_code is not None:
        where.append("ts_code = ?"); params.append(ts_code)
    if name is not None:
        where.append("name = ?"); params.append(name)
    if market is not None:
        where.append("market = ?"); params.append(market)
    if list_status is not None:
        where.append("list_status = ?"); params.append(list_status)
    if exchange is not None:
        where.append("exchange = ?"); params.append(exchange)
    if is_hs is not None:
        where.append("is_hs = ?"); params.append(is_hs)

    sql = f"SELECT {', '.join(columns)} FROM read_parquet(?)"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY ts_code"
    if limit is not None:
        sql += " LIMIT ?"; params.append(limit)
    if offset is not None:
        sql += " OFFSET ?"; params.append(offset)
    return duckdb.connect().execute(sql, [str(path), *params]).fetchdf()


def daily(ctx: QueryContext, ts_code=None, trade_date=None,
          start_date=None, end_date=None, fields=None,
          limit: int | None = None, offset: int | None = None) -> pd.DataFrame:
    """Query daily OHLCV bar data (open, high, low, close, vol, amount)."""
    return query_daily_partitioned(
        ctx, "daily_kline", "daily_kline", DAILY_COLS,
        ts_code, trade_date, start_date, end_date, fields,
        limit=limit, offset=offset,
    )


def adj_factor(ctx: QueryContext, ts_code=None, trade_date=None,
               start_date=None, end_date=None, fields=None,
               limit: int | None = None, offset: int | None = None) -> pd.DataFrame:
    """Query back-adjustment factors for split/dividend-adjusted price calculation."""
    return query_daily_partitioned(
        ctx, "adj_factor", "adj_factor", ADJ_FACTOR_COLS,
        ts_code, trade_date, start_date, end_date, fields,
        limit=limit, offset=offset,
    )


def daily_basic(ctx: QueryContext, ts_code=None, trade_date=None,
                start_date=None, end_date=None, fields=None,
                limit: int | None = None, offset: int | None = None) -> pd.DataFrame:
    """Query daily fundamental indicators (PE, PB, market cap, turnover rate, etc.)."""
    return query_daily_partitioned(
        ctx, "daily_basic", "daily_basic", DAILY_BASIC_COLS,
        ts_code, trade_date, start_date, end_date, fields,
        limit=limit, offset=offset,
    )


def stock_st(ctx: QueryContext, ts_code=None, trade_date=None,
             start_date=None, end_date=None, fields=None,
             limit: int | None = None, offset: int | None = None) -> pd.DataFrame:
    """Query ST/ST*/delisting-risk flag history per stock per trading day."""
    return query_daily_partitioned(
        ctx, "stock_st", "stock_st", STOCK_ST_COLS,
        ts_code, trade_date, start_date, end_date, fields,
        limit=limit, offset=offset,
    )


def suspend_d(ctx: QueryContext, ts_code=None, trade_date=None,
              start_date=None, end_date=None, fields=None,
              limit: int | None = None, offset: int | None = None) -> pd.DataFrame:
    """Query daily suspension records (stock halted from trading)."""
    return query_daily_partitioned(
        ctx, "suspend_d", "suspend_d", SUSPEND_D_COLS,
        ts_code, trade_date, start_date, end_date, fields,
        limit=limit, offset=offset,
    )


def stk_limit(ctx: QueryContext, ts_code=None, trade_date=None,
              start_date=None, end_date=None, fields=None,
              limit: int | None = None, offset: int | None = None) -> pd.DataFrame:
    """Query daily price limits (up_limit, down_limit, pre_close) per stock."""
    return query_daily_partitioned(
        ctx, "stk_limit", "stk_limit", STK_LIMIT_COLS,
        ts_code, trade_date, start_date, end_date, fields,
        limit=limit, offset=offset,
    )


def index_daily(ctx: QueryContext, ts_code=None, trade_date=None,
                start_date=None, end_date=None, fields=None,
                limit: int | None = None, offset: int | None = None) -> pd.DataFrame:
    """Query daily OHLCV bar data for broad market indices (SSE/SZSE/CSI)."""
    return query_daily_partitioned(
        ctx, "index_daily", "index_daily", INDEX_DAILY_COLS,
        ts_code, trade_date, start_date, end_date, fields,
        limit=limit, offset=offset,
    )


def index_weight(ctx: QueryContext, index_code=None, trade_date=None,
                 start_date=None, end_date=None, fields=None,
                 limit: int | None = None, offset: int | None = None) -> pd.DataFrame:
    """Query constituent weights for CSI 300/500/1000 index rebalancing dates."""
    if trade_date is not None and (start_date is not None or end_date is not None):
        raise ValueError("trade_date cannot be combined with start_date or end_date")
    parsed_start = parse_date(start_date) if start_date is not None else None
    parsed_end = parse_date(end_date) if end_date is not None else None
    if parsed_start is not None and parsed_end is not None and parsed_end < parsed_start:
        raise ValueError("end_date must be on or after start_date")

    table_dir = ctx.data_dir / "index_weight"
    if not table_dir.exists():
        raise FileNotFoundError(
            "index_weight data not found; run `python main.py sync --table index_weight` first"
        )

    selected = parse_fields(fields, INDEX_WEIGHT_COLS)
    where = []
    params = []
    if index_code is not None:
        where.append("index_code = ?"); params.append(index_code)
    if trade_date is not None:
        where.append("trade_date = ?"); params.append(parse_date(trade_date).strftime("%Y%m%d"))
    if parsed_start is not None:
        where.append("trade_date >= ?"); params.append(parsed_start.strftime("%Y%m%d"))
    if parsed_end is not None:
        where.append("trade_date <= ?"); params.append(parsed_end.strftime("%Y%m%d"))

    pattern = table_dir / "index_code=*" / "date=*" / "data.parquet"
    sql = (
        f"SELECT {', '.join(selected)} "
        "FROM read_parquet(?, hive_partitioning=true, union_by_name=true)"
    )
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY index_code, con_code, trade_date"
    if limit is not None:
        sql += " LIMIT ?"; params.append(limit)
    if offset is not None:
        sql += " OFFSET ?"; params.append(offset)
    return duckdb.connect().execute(sql, [str(pattern), *params]).fetchdf()


def universe(ctx: QueryContext, universe=None, ts_code=None, trade_date=None,
             start_date=None, end_date=None, fields=None,
             limit: int | None = None, offset: int | None = None) -> pd.DataFrame:
    """Query universe membership snapshots (which stocks belong to which universe on each date)."""
    if trade_date is not None and (start_date is not None or end_date is not None):
        raise ValueError("trade_date cannot be combined with start_date or end_date")
    parsed_start = parse_date(start_date) if start_date is not None else None
    parsed_end = parse_date(end_date) if end_date is not None else None
    if parsed_start is not None and parsed_end is not None and parsed_end < parsed_start:
        raise ValueError("end_date must be on or after start_date")

    table_dir = ctx.data_dir / "universe"
    if not table_dir.exists():
        raise FileNotFoundError("universe data not found; run `python main.py build-universe` first")

    selected = parse_fields(fields, UNIVERSE_COLS)
    where = []
    params = []
    if universe is not None:
        where.append("universe = ?"); params.append(universe)
    if ts_code is not None:
        codes = [c.strip() for c in ts_code.split(",") if c.strip()]
        placeholders = ", ".join("?" for _ in codes)
        where.append(f"ts_code IN ({placeholders})"); params.extend(codes)
    if trade_date is not None:
        where.append("trade_date = ?"); params.append(parse_date(trade_date))
    if parsed_start is not None:
        where.append("trade_date >= ?"); params.append(parsed_start)
    if parsed_end is not None:
        where.append("trade_date <= ?"); params.append(parsed_end)

    pattern = table_dir / "name=*" / "date=*" / "data.parquet"
    sql = (
        f"SELECT {', '.join(selected)} "
        "FROM read_parquet(?, hive_partitioning=true, union_by_name=true)"
    )
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY universe, ts_code, trade_date"
    if limit is not None:
        sql += " LIMIT ?"; params.append(limit)
    if offset is not None:
        sql += " OFFSET ?"; params.append(offset)
    df = duckdb.connect().execute(sql, [str(pattern), *params]).fetchdf()
    return format_date_columns(df, ["trade_date"])


def pro_bar(ctx: QueryContext, ts_code: str, start_date=None, end_date=None,
            asset: str = "E", adj=None, freq: str = "D", trade_date=None,
            ma=None, limit: int | None = None, offset: int | None = None) -> pd.DataFrame:
    """Query adjusted OHLCV bars; adj='qfq' for forward-adjusted, 'hfq' for back-adjusted."""
    if asset != "E":
        raise NotImplementedError("local pro_bar currently only supports asset='E'")
    if freq != "D":
        raise NotImplementedError("local pro_bar currently only supports freq='D'")
    if ma:
        raise NotImplementedError("local pro_bar does not support ma yet")
    if adj not in (None, "qfq", "hfq"):
        raise ValueError("adj must be one of None, 'qfq', or 'hfq'")

    import pandas as pd
    daily_df = daily(ctx, ts_code=ts_code, trade_date=trade_date,
                     start_date=start_date, end_date=end_date)
    if adj is None or daily_df.empty:
        result = daily_df
    else:
        factors = adj_factor(ctx, ts_code=ts_code, trade_date=trade_date,
                             start_date=start_date, end_date=end_date)
        if factors.empty:
            result = daily_df.iloc[0:0].copy()
        else:
            result = daily_df.merge(
                factors[["ts_code", "trade_date", "adj_factor"]],
                on=["ts_code", "trade_date"], how="left",
            ).sort_values(["ts_code", "trade_date"])
            result["adj_factor"] = result.groupby("ts_code")["adj_factor"].bfill()
            result = result.dropna(subset=["adj_factor"])
            if result.empty:
                result = daily_df.iloc[0:0].copy()
            else:
                price_columns = ["open", "high", "low", "close", "pre_close"]
                if adj == "qfq":
                    base_factor = result.groupby("ts_code")["adj_factor"].transform("last")
                    multiplier = result["adj_factor"] / base_factor
                else:
                    multiplier = result["adj_factor"]
                for col in price_columns:
                    result[col] = (result[col] * multiplier).round(2)
                result["change"] = (result["close"] - result["pre_close"]).round(2)
                result["pct_chg"] = (result["change"] / result["pre_close"] * 100).round(2)
                result = result.drop(columns=["adj_factor"])

    if offset is not None:
        result = result.iloc[offset:]
    if limit is not None:
        result = result.iloc[:limit]
    return result
```

- [ ] **Step 3: Create query/industry.py**

```python
import duckdb
import pandas as pd

from zer0share.query import QueryContext
from zer0share.query._helpers import parse_fields
from zer0share.schema import SW_CLASSIFY_COLS, SW_MEMBER_COLS, CI_MEMBER_COLS


def index_classify(ctx: QueryContext, level=None, src=None, fields=None,
                   limit: int | None = None, offset: int | None = None) -> pd.DataFrame:
    """Query Shenwan (SW) industry classification hierarchy (L1/L2/L3 levels)."""
    path = ctx.data_dir / "industry" / "sw_classify" / "data.parquet"
    if not path.exists():
        raise FileNotFoundError(
            "sw_classify data not found; run `python main.py sync --table industry` first"
        )
    selected = parse_fields(fields, SW_CLASSIFY_COLS)
    where = []
    params = []
    if level is not None:
        where.append("level = ?"); params.append(level)
    if src is not None:
        where.append("src = ?"); params.append(src)
    sql = f"SELECT {', '.join(selected)} FROM read_parquet(?)"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY industry_code"
    if limit is not None:
        sql += " LIMIT ?"; params.append(limit)
    if offset is not None:
        sql += " OFFSET ?"; params.append(offset)
    return duckdb.connect().execute(sql, [str(path), *params]).fetchdf()


def index_member_all(ctx: QueryContext, l1_code=None, ts_code=None, is_new=None, fields=None,
                     limit: int | None = None, offset: int | None = None) -> pd.DataFrame:
    """Query Shenwan industry membership: which stocks belong to which SW industry."""
    path = ctx.data_dir / "industry" / "sw_member" / "data.parquet"
    if not path.exists():
        raise FileNotFoundError(
            "sw_member data not found; run `python main.py sync --table industry` first"
        )
    selected = parse_fields(fields, SW_MEMBER_COLS)
    where = []
    params = []
    if l1_code is not None:
        where.append("l1_code = ?"); params.append(l1_code)
    if ts_code is not None:
        codes = [c.strip() for c in ts_code.split(",") if c.strip()]
        placeholders = ", ".join("?" for _ in codes)
        where.append(f"ts_code IN ({placeholders})"); params.extend(codes)
    if is_new is not None:
        where.append("is_new = ?"); params.append(is_new)
    sql = f"SELECT {', '.join(selected)} FROM read_parquet(?)"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY ts_code, l1_code"
    if limit is not None:
        sql += " LIMIT ?"; params.append(limit)
    if offset is not None:
        sql += " OFFSET ?"; params.append(offset)
    return duckdb.connect().execute(sql, [str(path), *params]).fetchdf()


def ci_index_member(ctx: QueryContext, l1_code=None, ts_code=None, is_new=None, fields=None,
                    limit: int | None = None, offset: int | None = None) -> pd.DataFrame:
    """Query China Securities Index (CI) industry membership."""
    path = ctx.data_dir / "industry" / "ci_member" / "data.parquet"
    if not path.exists():
        raise FileNotFoundError(
            "ci_member data not found; run `python main.py sync --table ci_member` first"
        )
    selected = parse_fields(fields, CI_MEMBER_COLS)
    where = []
    params = []
    if l1_code is not None:
        where.append("l1_code = ?"); params.append(l1_code)
    if ts_code is not None:
        codes = [c.strip() for c in ts_code.split(",") if c.strip()]
        placeholders = ", ".join("?" for _ in codes)
        where.append(f"ts_code IN ({placeholders})"); params.extend(codes)
    if is_new is not None:
        where.append("is_new = ?"); params.append(is_new)
    sql = f"SELECT {', '.join(selected)} FROM read_parquet(?)"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY ts_code, l1_code"
    if limit is not None:
        sql += " LIMIT ?"; params.append(limit)
    if offset is not None:
        sql += " OFFSET ?"; params.append(offset)
    return duckdb.connect().execute(sql, [str(path), *params]).fetchdf()
```

- [ ] **Step 4: Create query/futures.py**

```python
import duckdb
import pandas as pd

from zer0share.query import QueryContext
from zer0share.query._helpers import parse_fields, query_daily_partitioned
from zer0share.schema import (
    FUT_BASIC_COLS, FUT_DAILY_COLS, FUT_HOLDING_COLS, FUT_WSR_COLS,
    FUT_SETTLE_COLS, FUT_MAPPING_COLS, FT_LIMIT_COLS, FUT_WEEKLY_COLS,
    FUT_MONTHLY_COLS, FUT_INDEX_DAILY_COLS, FUT_WEEKLY_DETAIL_COLS,
)


def fut_basic(ctx: QueryContext, ts_code=None, exchange=None,
              fut_type=None, fut_code=None, fields=None,
              limit: int | None = None, offset: int | None = None) -> pd.DataFrame:
    """Query futures contract specifications (symbol, exchange, multiplier, list/delist dates)."""
    table_dir = ctx.data_dir / "futures" / "fut_basic"
    if not table_dir.exists():
        raise FileNotFoundError(
            "fut_basic data not found; run `python main.py sync --table fut_basic` first"
        )
    selected = parse_fields(fields, FUT_BASIC_COLS)
    where = []
    params = []
    if ts_code is not None:
        codes = [c.strip() for c in ts_code.split(",") if c.strip()]
        placeholders = ", ".join("?" for _ in codes)
        where.append(f"ts_code IN ({placeholders})"); params.extend(codes)
    if exchange is not None:
        where.append("exchange = ?"); params.append(exchange)
    if fut_code is not None:
        where.append("fut_code = ?"); params.append(fut_code)

    pattern = table_dir / "date=*" / "data.parquet"
    sql = (
        f"SELECT {', '.join(selected)} "
        "FROM read_parquet(?, hive_partitioning=true, union_by_name=true)"
    )
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY ts_code"
    if limit is not None:
        sql += " LIMIT ?"; params.append(limit)
    if offset is not None:
        sql += " OFFSET ?"; params.append(offset)
    return duckdb.connect().execute(sql, [str(pattern), *params]).fetchdf()


def fut_daily(ctx: QueryContext, ts_code=None, trade_date=None,
              start_date=None, end_date=None, fields=None,
              limit: int | None = None, offset: int | None = None) -> pd.DataFrame:
    """Query daily OHLCV and open interest data for individual futures contracts."""
    return query_daily_partitioned(
        ctx, "fut_daily", "fut_daily", FUT_DAILY_COLS,
        ts_code, trade_date, start_date, end_date, fields,
        data_dir_override=ctx.data_dir / "futures",
        limit=limit, offset=offset,
    )


def fut_holding(ctx: QueryContext, trade_date=None, symbol=None, start_date=None,
                end_date=None, exchange=None, fields=None,
                limit: int | None = None, offset: int | None = None) -> pd.DataFrame:
    """Query top 20 broker long/short positions per futures contract (席位持仓)."""
    extra = {}
    if symbol is not None:
        extra["symbol"] = symbol
    if exchange is not None:
        extra["exchange"] = exchange
    return query_daily_partitioned(
        ctx, "fut_holding", "fut_holding", FUT_HOLDING_COLS,
        None, trade_date, start_date, end_date, fields,
        extra_filters=extra or None,
        data_dir_override=ctx.data_dir / "futures",
        order_by="trade_date, symbol, broker",
        limit=limit, offset=offset,
    )


def fut_wsr(ctx: QueryContext, trade_date=None, symbol=None, start_date=None,
            end_date=None, exchange=None, fields=None,
            limit: int | None = None, offset: int | None = None) -> pd.DataFrame:
    """Query futures warehouse stock receipt data (仓单数据)."""
    extra = {}
    if symbol is not None:
        extra["symbol"] = symbol
    if exchange is not None:
        extra["exchange"] = exchange
    return query_daily_partitioned(
        ctx, "fut_wsr", "fut_wsr", FUT_WSR_COLS,
        None, trade_date, start_date, end_date, fields,
        extra_filters=extra or None,
        data_dir_override=ctx.data_dir / "futures",
        order_by="trade_date, symbol, warehouse",
        limit=limit, offset=offset,
    )


def fut_settle(ctx: QueryContext, ts_code=None, trade_date=None, start_date=None,
               end_date=None, exchange=None, fields=None,
               limit: int | None = None, offset: int | None = None) -> pd.DataFrame:
    """Query daily settlement prices and open interest for futures contracts."""
    extra = {"exchange": exchange} if exchange is not None else None
    return query_daily_partitioned(
        ctx, "fut_settle", "fut_settle", FUT_SETTLE_COLS,
        ts_code, trade_date, start_date, end_date, fields,
        extra_filters=extra,
        data_dir_override=ctx.data_dir / "futures",
        limit=limit, offset=offset,
    )


def fut_mapping(ctx: QueryContext, ts_code=None, trade_date=None,
                start_date=None, end_date=None, fields=None,
                limit: int | None = None, offset: int | None = None) -> pd.DataFrame:
    """Query main contract mapping: which specific contract was the main contract on each date."""
    return query_daily_partitioned(
        ctx, "fut_mapping", "fut_mapping", FUT_MAPPING_COLS,
        ts_code, trade_date, start_date, end_date, fields,
        data_dir_override=ctx.data_dir / "futures",
        order_by="ts_code, trade_date",
        limit=limit, offset=offset,
    )


def ft_limit(ctx: QueryContext, ts_code=None, trade_date=None, start_date=None,
             end_date=None, exchange=None, fields=None,
             limit: int | None = None, offset: int | None = None) -> pd.DataFrame:
    """Query daily price limits (up_limit, down_limit, pre_close) for futures contracts."""
    extra = {"exchange": exchange} if exchange is not None else None
    return query_daily_partitioned(
        ctx, "ft_limit", "ft_limit", FT_LIMIT_COLS,
        ts_code, trade_date, start_date, end_date, fields,
        extra_filters=extra,
        data_dir_override=ctx.data_dir / "futures",
        limit=limit, offset=offset,
    )


def fut_weekly(ctx: QueryContext, ts_code=None, trade_date=None, start_date=None,
               end_date=None, exchange=None, fields=None,
               limit: int | None = None, offset: int | None = None) -> pd.DataFrame:
    """Query weekly OHLCV and open interest bars for futures contracts."""
    extra = {"exchange": exchange} if exchange is not None else None
    return query_daily_partitioned(
        ctx, "fut_weekly", "fut_weekly", FUT_WEEKLY_COLS,
        ts_code, trade_date, start_date, end_date, fields,
        extra_filters=extra,
        data_dir_override=ctx.data_dir / "futures",
        limit=limit, offset=offset,
    )


def fut_monthly(ctx: QueryContext, ts_code=None, trade_date=None, start_date=None,
                end_date=None, exchange=None, fields=None,
                limit: int | None = None, offset: int | None = None) -> pd.DataFrame:
    """Query monthly OHLCV and open interest bars for futures contracts."""
    extra = {"exchange": exchange} if exchange is not None else None
    return query_daily_partitioned(
        ctx, "fut_monthly", "fut_monthly", FUT_MONTHLY_COLS,
        ts_code, trade_date, start_date, end_date, fields,
        extra_filters=extra,
        data_dir_override=ctx.data_dir / "futures",
        limit=limit, offset=offset,
    )


def fut_index_daily(ctx: QueryContext, ts_code=None, trade_date=None,
                    start_date=None, end_date=None, fields=None,
                    limit: int | None = None, offset: int | None = None) -> pd.DataFrame:
    """Query daily prices for continuous main contract index series."""
    return query_daily_partitioned(
        ctx, "fut_index_daily", "fut_index_daily", FUT_INDEX_DAILY_COLS,
        ts_code, trade_date, start_date, end_date, fields,
        data_dir_override=ctx.data_dir / "futures",
        limit=limit, offset=offset,
    )


def fut_weekly_detail(ctx: QueryContext, exchange=None, prd=None,
                      start_date=None, end_date=None, fields=None,
                      limit: int | None = None, offset: int | None = None) -> pd.DataFrame:
    """Query weekly COT-style long/short position breakdown by participant (龙虎榜周数据)."""
    extra = {}
    if exchange is not None:
        extra["exchange"] = exchange
    if prd is not None:
        extra["prd"] = prd
    return query_daily_partitioned(
        ctx, "fut_weekly_detail", "fut_weekly_detail", FUT_WEEKLY_DETAIL_COLS,
        None, None, start_date, end_date, fields,
        extra_filters=extra or None,
        data_dir_override=ctx.data_dir / "futures",
        order_by="date, exchange, prd",
        limit=limit, offset=offset,
    )
```

- [ ] **Step 5: Create query/options.py**

```python
import duckdb
import pandas as pd

from zer0share.query import QueryContext
from zer0share.query._helpers import parse_fields, query_daily_partitioned
from zer0share.schema import OPT_BASIC_COLS, OPT_DAILY_COLS


def opt_basic(ctx: QueryContext, ts_code=None, exchange=None, opt_code=None,
              call_put=None, name=None, list_date=None,
              limit: int | None = None, offset: int | None = None,
              fields=None) -> pd.DataFrame:
    """Query options contract specifications (strike, expiry, call/put, exercise type)."""
    path = ctx.data_dir / "options" / "opt_basic" / "data.parquet"
    if not path.exists():
        raise FileNotFoundError(
            "opt_basic data not found; run `python main.py sync --table opt_basic` first"
        )
    selected = parse_fields(fields, OPT_BASIC_COLS)
    where = []
    params = []
    if ts_code is not None:
        codes = [c.strip() for c in ts_code.split(",") if c.strip()]
        placeholders = ", ".join("?" for _ in codes)
        where.append(f"ts_code IN ({placeholders})"); params.extend(codes)
    if exchange is not None:
        where.append("exchange = ?"); params.append(exchange)
    if opt_code is not None:
        where.append("opt_code = ?"); params.append(opt_code)
    if call_put is not None:
        where.append("call_put = ?"); params.append(call_put)
    if name is not None:
        where.append("name = ?"); params.append(name)
    if list_date is not None:
        where.append("list_date = ?"); params.append(list_date)

    sql = f"SELECT {', '.join(selected)} FROM read_parquet(?)"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY ts_code"
    if limit is not None:
        sql += " LIMIT ?"; params.append(limit)
    if offset is not None:
        sql += " OFFSET ?"; params.append(offset)
    return duckdb.connect().execute(sql, [str(path), *params]).fetchdf()


def opt_daily(ctx: QueryContext, ts_code=None, trade_date=None, start_date=None,
              end_date=None, exchange=None,
              limit: int | None = None, offset: int | None = None,
              fields=None) -> pd.DataFrame:
    """Query daily OHLCV, settlement price, and open interest for options contracts."""
    extra = {"exchange": exchange} if exchange is not None else None
    return query_daily_partitioned(
        ctx, "opt_daily", "opt_daily", OPT_DAILY_COLS,
        ts_code, trade_date, start_date, end_date, fields,
        extra_filters=extra,
        data_dir_override=ctx.data_dir / "options",
        limit=limit,
        offset=offset,
    )
```

- [ ] **Step 6: Run tests**

```bash
uv run pytest --tb=short -q
```

Expected: `258 passed`

- [ ] **Step 7: Commit**

```bash
git add zer0share/query/
git commit -m "refactor: add query/ domain modules (calendar, equities, industry, futures, options)"
```

---

## Task 7: Replace api.py with thin facade

**Files:**
- Modify: `zer0share/api.py` (complete rewrite)

- [ ] **Step 1: Rewrite api.py**

Replace the entire contents of `zer0share/api.py` with:

```python
from pathlib import Path

from zer0share.config import load_config
from zer0share.query import QueryContext
from zer0share.query import calendar, equities, industry, futures, options


class LocalPro:
    def __init__(self, data_dir):
        self._ctx = QueryContext(Path(data_dir))

    # Calendar
    def trade_cal(self, **kwargs):
        return calendar.trade_cal(self._ctx, **kwargs)

    # Equities
    def stock_basic(self, **kwargs):
        return equities.stock_basic(self._ctx, **kwargs)

    def daily(self, **kwargs):
        return equities.daily(self._ctx, **kwargs)

    def adj_factor(self, **kwargs):
        return equities.adj_factor(self._ctx, **kwargs)

    def daily_basic(self, **kwargs):
        return equities.daily_basic(self._ctx, **kwargs)

    def stock_st(self, **kwargs):
        return equities.stock_st(self._ctx, **kwargs)

    def suspend_d(self, **kwargs):
        return equities.suspend_d(self._ctx, **kwargs)

    def stk_limit(self, **kwargs):
        return equities.stk_limit(self._ctx, **kwargs)

    def index_daily(self, **kwargs):
        return equities.index_daily(self._ctx, **kwargs)

    def index_weight(self, **kwargs):
        return equities.index_weight(self._ctx, **kwargs)

    def universe(self, **kwargs):
        return equities.universe(self._ctx, **kwargs)

    def pro_bar(self, **kwargs):
        return equities.pro_bar(self._ctx, **kwargs)

    # Industry
    def index_classify(self, **kwargs):
        return industry.index_classify(self._ctx, **kwargs)

    def index_member_all(self, **kwargs):
        return industry.index_member_all(self._ctx, **kwargs)

    def ci_index_member(self, **kwargs):
        return industry.ci_index_member(self._ctx, **kwargs)

    # Futures
    def fut_basic(self, **kwargs):
        return futures.fut_basic(self._ctx, **kwargs)

    def fut_daily(self, **kwargs):
        return futures.fut_daily(self._ctx, **kwargs)

    def fut_holding(self, **kwargs):
        return futures.fut_holding(self._ctx, **kwargs)

    def fut_wsr(self, **kwargs):
        return futures.fut_wsr(self._ctx, **kwargs)

    def fut_settle(self, **kwargs):
        return futures.fut_settle(self._ctx, **kwargs)

    def fut_mapping(self, **kwargs):
        return futures.fut_mapping(self._ctx, **kwargs)

    def ft_limit(self, **kwargs):
        return futures.ft_limit(self._ctx, **kwargs)

    def fut_weekly(self, **kwargs):
        return futures.fut_weekly(self._ctx, **kwargs)

    def fut_monthly(self, **kwargs):
        return futures.fut_monthly(self._ctx, **kwargs)

    def fut_index_daily(self, **kwargs):
        return futures.fut_index_daily(self._ctx, **kwargs)

    def fut_weekly_detail(self, **kwargs):
        return futures.fut_weekly_detail(self._ctx, **kwargs)

    # Options
    def opt_basic(self, **kwargs):
        return options.opt_basic(self._ctx, **kwargs)

    def opt_daily(self, **kwargs):
        return options.opt_daily(self._ctx, **kwargs)

    def query(self, api_name: str, **kwargs):
        dispatch = {
            "stock_basic": self.stock_basic,
            "trade_cal": self.trade_cal,
            "daily": self.daily,
            "adj_factor": self.adj_factor,
            "daily_basic": self.daily_basic,
            "stock_st": self.stock_st,
            "suspend_d": self.suspend_d,
            "stk_limit": self.stk_limit,
            "index_daily": self.index_daily,
            "index_weight": self.index_weight,
            "universe": self.universe,
            "pro_bar": self.pro_bar,
            "index_classify": self.index_classify,
            "index_member_all": self.index_member_all,
            "ci_index_member": self.ci_index_member,
            "fut_basic": self.fut_basic,
            "fut_daily": self.fut_daily,
            "fut_holding": self.fut_holding,
            "fut_wsr": self.fut_wsr,
            "fut_settle": self.fut_settle,
            "fut_mapping": self.fut_mapping,
            "ft_limit": self.ft_limit,
            "fut_weekly": self.fut_weekly,
            "fut_monthly": self.fut_monthly,
            "fut_index_daily": self.fut_index_daily,
            "fut_weekly_detail": self.fut_weekly_detail,
            "opt_basic": self.opt_basic,
            "opt_daily": self.opt_daily,
        }
        try:
            method = dispatch[api_name]
        except KeyError as e:
            raise ValueError(f"unknown api: {api_name}") from e
        return method(**kwargs)


def pro_api(config_path="config/settings.toml") -> LocalPro:
    cfg = load_config(Path(config_path))
    return LocalPro(cfg.data_dir)
```

- [ ] **Step 2: Run full test suite**

```bash
uv run pytest --tb=short -q
```

Expected: `258 passed`

If `test_api.py` fails because it imports `*_COLS` from `zer0share.fetcher` directly, update those imports to `from zer0share.schema import ...`.

- [ ] **Step 3: Commit**

```bash
git add zer0share/api.py
git commit -m "refactor: replace api.py with thin facade delegating to query/*"
```

---

## Self-Review

**Spec coverage:**
- ✅ `schema.py` — Task 1
- ✅ `sync/` skeleton (SyncContext, helpers) — Task 2
- ✅ All 5 sync domain modules — Task 3
- ✅ `pipeline.py` thin facade — Task 4
- ✅ `query/` skeleton (QueryContext, helpers) — Task 5
- ✅ All 5 query domain modules — Task 6
- ✅ `api.py` thin facade — Task 7
- ✅ test_pipeline.py patch paths updated — Task 4
- ✅ `sync_daily_kline` / `sync_adj_factor` deduplication — Task 3 (both delegate to `sync_daily_partitioned`)
- ✅ `cli.py` / `scheduler.py` — unchanged (both only import `Pipeline` which stays at `zer0share.pipeline`)

**Placeholder scan:** None found.

**Type consistency:**
- `SyncContext` defined in `sync/__init__.py`, used consistently across all sync domain modules and `pipeline.py`
- `QueryContext` defined in `query/__init__.py`, used consistently across all query domain modules and `api.py`
- `sync_daily_partitioned` signature in `_helpers.py` matches all call sites in domain modules
- `query_daily_partitioned` signature in `query/_helpers.py` matches all call sites in domain modules
