# Date String Full Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace all `date` objects with `YYYYMMDD` strings across every module boundary — fetcher, storage, MetaStore, sync loops, and CLI — leaving `date` objects only inside `dateutil.py` and `MetaStore` SQL internals.

**Architecture:** A new `zer0share/dateutil.py` module owns all date arithmetic and exposes a pure-string interface. `storage.py` converts strings to `date` objects only inside MetaStore SQL calls. Everything else — sync loop variables, function signatures, CLI args — uses `str`. The invariant is machine-verifiable: `grep "from datetime import date" zer0share/` must only match `dateutil.py` and `storage.py`.

**Tech Stack:** Python 3.11+, pytest, DuckDB, PyArrow/Parquet, Click 8

---

### Task 1: Create `zer0share/dateutil.py`

**Files:**
- Create: `zer0share/dateutil.py`
- Create: `tests/test_dateutil.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_dateutil.py
from zer0share.dateutil import add_days, month_ranges, today, week_ranges


def test_today_returns_yyyymmdd_string():
    result = today()
    assert len(result) == 8
    assert result.isdigit()


def test_add_days_simple():
    assert add_days("20240101", 1) == "20240102"


def test_add_days_year_boundary():
    assert add_days("20161231", 1) == "20170101"


def test_add_days_leap_year():
    assert add_days("20240229", 1) == "20240301"


def test_add_days_negative():
    assert add_days("20240103", -1) == "20240102"


def test_month_ranges_single_month():
    assert month_ranges("20240115", "20240125") == [("20240115", "20240125")]


def test_month_ranges_two_months():
    assert month_ranges("20240115", "20240210") == [
        ("20240115", "20240131"),
        ("20240201", "20240210"),
    ]


def test_month_ranges_three_months():
    # 2024 is a leap year, so Feb has 29 days
    assert month_ranges("20240115", "20240301") == [
        ("20240115", "20240131"),
        ("20240201", "20240229"),
        ("20240301", "20240301"),
    ]


def test_month_ranges_year_boundary():
    assert month_ranges("20231201", "20240131") == [
        ("20231201", "20231231"),
        ("20240101", "20240131"),
    ]


def test_week_ranges_single_week():
    # 2024-01-04 is Thursday of ISO week 202401; Monday is 2024-01-01
    result = week_ranges("20240104", "20240105")
    assert len(result) == 1
    week_num, monday = result[0]
    assert week_num == "202401"
    assert monday == "20240101"


def test_week_ranges_two_weeks():
    # 20240104 (Thu week 1) to 20240112 (Fri week 2)
    result = week_ranges("20240104", "20240112")
    assert len(result) == 2
    assert result[0][0] == "202401"
    assert result[1][0] == "202402"


def test_week_ranges_advances_by_7():
    # start and end in same week → 1 result
    result = week_ranges("20240101", "20240107")
    assert len(result) == 1
```

- [ ] **Step 2: Run tests to see them fail**

```bash
python -m pytest tests/test_dateutil.py -v
```
Expected: `ModuleNotFoundError: No module named 'zer0share.dateutil'`

- [ ] **Step 3: Implement `zer0share/dateutil.py`**

```python
from datetime import date, timedelta

_FMT = "%Y%m%d"


def _parse(s: str) -> date:
    return date(int(s[:4]), int(s[4:6]), int(s[6:]))


def today() -> str:
    return date.today().strftime(_FMT)


def add_days(s: str, n: int) -> str:
    return (_parse(s) + timedelta(days=n)).strftime(_FMT)


def month_ranges(start: str, end: str) -> list[tuple[str, str]]:
    s = _parse(start)
    e = _parse(end)
    ranges = []
    current = date(s.year, s.month, 1)
    while current <= e:
        if current.month == 12:
            next_month = date(current.year + 1, 1, 1)
        else:
            next_month = date(current.year, current.month + 1, 1)
        month_start = max(s, current)
        month_end = min(e, next_month - timedelta(days=1))
        ranges.append((month_start.strftime(_FMT), month_end.strftime(_FMT)))
        current = next_month
    return ranges


def week_ranges(start: str, end: str) -> list[tuple[str, str]]:
    s = _parse(start)
    e = _parse(end)
    weeks: list[tuple[str, str]] = []
    seen: set[tuple[int, int]] = set()
    current = s
    while current <= e:
        iso_year, iso_week, _ = current.isocalendar()
        week_key = (iso_year, iso_week)
        if week_key not in seen:
            seen.add(week_key)
            week_num = f"{iso_year}{iso_week:02d}"
            monday = current - timedelta(days=current.weekday())
            weeks.append((week_num, monday.strftime(_FMT)))
        current += timedelta(days=7)
    return weeks
```

- [ ] **Step 4: Run tests to see them pass**

```bash
python -m pytest tests/test_dateutil.py -v
```
Expected: all 11 tests PASS

- [ ] **Step 5: Commit**

```bash
git add zer0share/dateutil.py tests/test_dateutil.py
git commit -m "feat: add dateutil module with string-based date arithmetic"
```

---

### Task 2: Update `MetaStore` public API and all storage functions

**Files:**
- Modify: `zer0share/storage.py`
- Modify: `tests/test_storage.py`
- Modify: `tests/test_api.py`

- [ ] **Step 1: Update `tests/test_storage.py` — MetaStore assertions**

Replace all `date(...)` objects used with MetaStore API with `YYYYMMDD` strings.

```python
# tests/test_storage.py — change these 5 tests:

def test_update_and_get_last_date(store):
    store.update_last_date("daily_kline", "20240115")
    assert store.get_last_date("daily_kline") == "20240115"


def test_update_overwrites_previous(store):
    store.update_last_date("daily_kline", "20240101")
    store.update_last_date("daily_kline", "20240131")
    assert store.get_last_date("daily_kline") == "20240131"


def test_different_table_names_are_independent(store):
    store.update_last_date("daily_kline", "20240110")
    store.update_last_date("basic", "20240220")
    assert store.get_last_date("daily_kline") == "20240110"
    assert store.get_last_date("basic") == "20240220"


def test_context_manager(tmp_path):
    with MetaStore(tmp_path / "meta.duckdb") as store:
        store.update_last_date("daily_kline", "20240101")
        assert store.get_last_date("daily_kline") == "20240101"
```

Also update `test_get_trading_days`, `test_get_trading_days_returns_empty_when_no_cal`, `test_get_trading_days_exchange_isolation`, `test_is_trading_day_*`:

```python
def test_get_trading_days(tmp_path):
    db_path = tmp_path / "meta.duckdb"
    df = pd.DataFrame({
        "exchange": ["SSE"] * 5,
        "cal_date": ["20240102", "20240103", "20240104", "20240105", "20240106"],
        "is_open": [True, False, True, False, True],
        "pretrade_date": ["20231229", "20240102", "20240102", "20240104", "20240104"],
    })
    write_trade_cal(tmp_path, "SSE", df)
    with MetaStore(db_path) as store:
        store.load_trade_cal_from_parquet(tmp_path)
        days = store.get_trading_days("SSE", "20240101", "20240106")
    assert days == ["20240102", "20240104", "20240106"]


def test_get_trading_days_returns_empty_when_no_cal(tmp_path):
    db_path = tmp_path / "meta.duckdb"
    with MetaStore(db_path) as store:
        days = store.get_trading_days("SSE", "20240101", "20240106")
    assert days == []


def test_get_trading_days_exchange_isolation(tmp_path):
    db_path = tmp_path / "meta.duckdb"
    sse_df = pd.DataFrame({
        "exchange": ["SSE"], "cal_date": ["20240102"],
        "is_open": [True], "pretrade_date": ["20231229"],
    })
    szse_df = pd.DataFrame({
        "exchange": ["SZSE"], "cal_date": ["20240103"],
        "is_open": [True], "pretrade_date": ["20240102"],
    })
    write_trade_cal(tmp_path, "SSE", sse_df)
    write_trade_cal(tmp_path, "SZSE", szse_df)
    with MetaStore(db_path) as store:
        store.load_trade_cal_from_parquet(tmp_path)
        sse_days = store.get_trading_days("SSE", "20240101", "20240106")
        szse_days = store.get_trading_days("SZSE", "20240101", "20240106")
    assert sse_days == ["20240102"]
    assert szse_days == ["20240103"]


def test_is_trading_day_returns_true_for_open_day(tmp_path):
    db_path = tmp_path / "meta.duckdb"
    df = pd.DataFrame({
        "exchange": ["SSE"], "cal_date": ["20240102"],
        "is_open": [True], "pretrade_date": ["20231229"],
    })
    write_trade_cal(tmp_path, "SSE", df)
    with MetaStore(db_path) as store:
        store.load_trade_cal_from_parquet(tmp_path)
        assert store.is_trading_day("SSE", "20240102") is True


def test_is_trading_day_returns_false_for_closed_day(tmp_path):
    db_path = tmp_path / "meta.duckdb"
    df = pd.DataFrame({
        "exchange": ["SSE"], "cal_date": ["20240103"],
        "is_open": [False], "pretrade_date": ["20240102"],
    })
    write_trade_cal(tmp_path, "SSE", df)
    with MetaStore(db_path) as store:
        store.load_trade_cal_from_parquet(tmp_path)
        assert store.is_trading_day("SSE", "20240103") is False


def test_is_trading_day_returns_true_when_date_not_in_calendar(tmp_path):
    db_path = tmp_path / "meta.duckdb"
    df = pd.DataFrame({
        "exchange": ["SSE"], "cal_date": ["20240102"],
        "is_open": [True], "pretrade_date": ["20231229"],
    })
    write_trade_cal(tmp_path, "SSE", df)
    with MetaStore(db_path) as store:
        store.load_trade_cal_from_parquet(tmp_path)
        assert store.is_trading_day("SSE", "20240110") is True


def test_is_trading_day_returns_true_when_no_calendar_loaded(tmp_path):
    db_path = tmp_path / "meta.duckdb"
    with MetaStore(db_path) as store:
        assert store.is_trading_day("SSE", "20240102") is True
```

Also update storage function tests — change `date(...)` to `"YYYYMMDD"` strings for `write_daily_kline`, `read_daily_kline`, `daily_kline_partition_exists`:

```python
def test_write_and_read_daily_kline(tmp_path):
    df = pd.DataFrame({
        "ts_code": ["000001.SZ", "000002.SZ"],
        "trade_date": ["20240102", "20240102"],
        "open": [10.0, 20.0], "high": [11.0, 21.0], "low": [9.5, 19.5],
        "close": [10.5, 20.5], "pre_close": [10.0, 20.0],
        "change": [0.5, 0.5], "pct_chg": [5.0, 2.5],
        "vol": [100000.0, 200000.0], "amount": [1050000.0, 4100000.0],
    })
    write_daily_kline(tmp_path, "20240102", df)
    result = read_daily_kline(tmp_path, "20240102")
    assert len(result) == 2
    assert set(result["ts_code"]) == {"000001.SZ", "000002.SZ"}


def test_daily_kline_partition_path(tmp_path):
    df = pd.DataFrame({
        "ts_code": ["000001.SZ"], "trade_date": ["20240102"],
        "open": [10.0], "high": [11.0], "low": [9.5], "close": [10.5],
        "pre_close": [10.0], "change": [0.5], "pct_chg": [5.0],
        "vol": [100000.0], "amount": [1050000.0],
    })
    write_daily_kline(tmp_path, "20240102", df)
    assert (tmp_path / "daily_kline" / "date=20240102" / "data.parquet").exists()


def test_daily_kline_partition_exists(tmp_path):
    assert daily_kline_partition_exists(tmp_path, "20240102") is False
    df = pd.DataFrame({
        "ts_code": ["000001.SZ"], "trade_date": ["20240102"],
        "open": [10.0], "high": [11.0], "low": [9.5], "close": [10.5],
        "pre_close": [10.0], "change": [0.5], "pct_chg": [5.0],
        "vol": [100000.0], "amount": [1050000.0],
    })
    write_daily_kline(tmp_path, "20240102", df)
    assert daily_kline_partition_exists(tmp_path, "20240102") is True


def test_read_daily_kline_returns_empty_if_not_exists(tmp_path):
    result = read_daily_kline(tmp_path, "20240102")
    assert result.empty
```

Remove `from datetime import date` from `test_storage.py` (no longer needed).

- [ ] **Step 2: Update `tests/test_api.py` — storage function calls**

Change all `date(...)` arguments to `"YYYYMMDD"` strings in storage function calls, and change `date(...)` values inside DataFrames to strings:

```python
# test_daily_filters_multiple_codes_by_date_range_and_formats_dates
write_daily_kline(
    tmp_path,
    "20240102",                         # was date(2024, 1, 2)
    pd.DataFrame({
        "ts_code": ["000001.SZ", "600000.SH"],
        "trade_date": ["20240102", "20240102"],   # was date(2024, 1, 2)
        ...
    }),
)
write_daily_kline(
    tmp_path,
    "20240103",                         # was date(2024, 1, 3)
    pd.DataFrame({...}),
)

# test_daily_partitioned_query_handles_empty_partitions
write_daily_partition(
    tmp_path, "stock_st",
    "20240102",                         # was date(2024, 1, 2)
    pd.DataFrame(columns=["ts_code", "name", "trade_date", "type", "type_name"]),
)
write_daily_partition(
    tmp_path, "stock_st",
    "20240103",                         # was date(2024, 1, 3)
    pd.DataFrame({...}),
)

# test_universe_filters_by_name_date_and_code
write_universe(
    tmp_path, "univ_trade_base",
    "20240102",                         # was date(2024, 1, 2)
    pd.DataFrame({
        "ts_code": [...],
        "trade_date": ["20240102", "20240102"],   # was date(2024, 1, 2)
        ...
    }),
)
write_universe(
    tmp_path, "univ_trade_base",
    "20240102",                         # was date(2024, 1, 2)
    pd.DataFrame({
        "ts_code": [...],
        "trade_date": ["20240102"],              # was date(2024, 1, 2)
        ...
    }),
)

# test_adj_factor_filters_trade_date_and_formats_dates
write_adj_factor(
    tmp_path,
    "20240102",                         # was date(2024, 1, 2)
    pd.DataFrame({...}),
)
```

Remove `from datetime import date` from `test_api.py` (no longer needed).

- [ ] **Step 3: Run tests to see them fail**

```bash
python -m pytest tests/test_storage.py tests/test_api.py -v 2>&1 | tail -20
```
Expected: multiple failures — `AssertionError: date(2024, 1, 15) != '20240115'` and `TypeError: strftime: can't convert 'str' to date`

- [ ] **Step 4: Update `MetaStore` in `storage.py`**

Add `_parse` helper and update the four public methods. Keep `from datetime import date` (needed for DuckDB SQL boundary):

```python
def _parse(s: str) -> date:
    return date(int(s[:4]), int(s[4:6]), int(s[6:]))
```

Update `MetaStore` methods:

```python
def get_last_date(self, table_name: str) -> str | None:
    row = self._conn.execute(
        "SELECT last_date FROM sync_meta WHERE table_name = ?",
        [table_name]
    ).fetchone()
    return row[0].strftime("%Y%m%d") if row else None

def update_last_date(self, table_name: str, last_date: str):
    self._conn.execute("""
        INSERT INTO sync_meta (table_name, last_date, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT (table_name) DO UPDATE SET
            last_date = excluded.last_date,
            updated_at = excluded.updated_at
    """, [table_name, _parse(last_date), datetime.now(timezone.utc)])

def get_trading_days(
    self, exchange: str, start: str, end: str
) -> list[str]:
    rows = self._conn.execute(
        """
        SELECT cal_date FROM trade_cal
        WHERE exchange = ?
          AND cal_date >= ?
          AND cal_date <= ?
          AND is_open = TRUE
        ORDER BY cal_date
        """,
        [exchange, _parse(start), _parse(end)]
    ).fetchall()
    return [row[0].strftime("%Y%m%d") for row in rows]

def is_trading_day(self, exchange: str, cal_date: str) -> bool:
    row = self._conn.execute(
        "SELECT is_open FROM trade_cal WHERE exchange = ? AND cal_date = ?",
        [exchange, _parse(cal_date)]
    ).fetchone()
    if row is None:
        return True
    return bool(row[0])
```

- [ ] **Step 5: Update all storage functions in `storage.py`**

Change every `trade_date: date` parameter to `trade_date: str` and simplify path building from `f"date={trade_date.strftime('%Y%m%d')}"` to `f"date={trade_date}"`. Apply to all 11 functions:

- `write_daily_kline(data_dir, trade_date: str, df)`
- `daily_kline_partition_exists(data_dir, trade_date: str)`
- `read_daily_kline(data_dir, trade_date: str)`
- `write_adj_factor(data_dir, trade_date: str, df)`
- `adj_factor_partition_exists(data_dir, trade_date: str)`
- `write_daily_partition(data_dir, table_name, trade_date: str, df)`
- `daily_partition_exists(data_dir, table_name, trade_date: str)`
- `read_daily_partition(data_dir, table_name, trade_date: str)`
- `write_index_weight(data_dir, index_code, trade_date: str, df)`
- `index_weight_partition_exists(data_dir, index_code, trade_date: str)`
- `write_universe(data_dir, universe_name, trade_date: str, df)`

Example for `write_daily_partition`:

```python
def write_daily_partition(
    data_dir: Path, table_name: str, trade_date: str, df: pd.DataFrame
) -> None:
    partition_dir = data_dir / table_name / f"date={trade_date}"
    partition_dir.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, partition_dir / "data.parquet")


def daily_partition_exists(data_dir: Path, table_name: str, trade_date: str) -> bool:
    path = data_dir / table_name / f"date={trade_date}" / "data.parquet"
    return path.exists()
```

Apply the same pattern to the remaining 9 functions.

- [ ] **Step 6: Run tests to see them pass**

```bash
python -m pytest tests/test_storage.py tests/test_api.py -v 2>&1 | tail -20
```
Expected: all tests PASS

- [ ] **Step 7: Commit**

```bash
git add zer0share/storage.py tests/test_storage.py tests/test_api.py
git commit -m "refactor: MetaStore and storage functions accept/return str dates"
```

---

### Task 3: Update `sync/_helpers.py`

**Files:**
- Modify: `zer0share/sync/_helpers.py`

- [ ] **Step 1: Rewrite `_helpers.py`**

Full replacement — note `month_ranges` and `week_ranges` now live in `dateutil.py`; `_helpers.py` no longer defines them:

```python
import time
from pathlib import Path
from typing import Callable

from loguru import logger

import zer0share.dateutil as dateutil
from zer0share.storage import daily_partition_exists, write_daily_partition
from zer0share.sync import SyncContext


FIRST_DATE = "20160101"
TRADE_CAL_FIRST_DATE = "19900101"
PROGRESS_INTERVAL = 50
EXCHANGES = ["SSE", "SZSE"]
INDEX_CODES = ["399300.SZ", "000905.SH", "000852.SH"]
ALL_EXCHANGES = ["SSE", "SZSE", "CFFEX", "DCE", "SHFE", "CZCE", "INE", "GFEX"]


def should_log_progress(processed: int, total: int) -> bool:
    return processed == total or processed % PROGRESS_INTERVAL == 0


def log_daily_progress(
    table_name: str,
    processed: int,
    total: int,
    trade_date: str,
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


def index_weight_meta_key(index_code: str) -> str:
    return f"index_weight:{index_code}"


def ensure_trade_cal_loaded(ctx: SyncContext) -> None:
    from zer0share.sync import calendar as cal_module
    if ctx.meta.get_last_date("trade_cal") is None:
        cal_module.sync_trade_cal(ctx)


def skip_if_not_trading(ctx: SyncContext, exchange: str) -> bool:
    ensure_trade_cal_loaded(ctx)
    today = dateutil.today()
    if not ctx.meta.is_trading_day(exchange, today):
        logger.info(f"今日 {today} 非交易日，跳过同步")
        return True
    return False


def sync_daily_partitioned(
    ctx: SyncContext,
    table_name: str,
    fetch: Callable,
    start_date: str | None,
    end_date: str | None,
    write_empty: bool = False,
    data_dir: Path | None = None,
    exchange: str = "SSE",
) -> None:
    base_dir = data_dir or ctx.cfg.data_dir
    today = dateutil.today()
    last = ctx.meta.get_last_date(table_name)
    if start_date is None:
        start = dateutil.add_days(last, 1) if last else FIRST_DATE
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

- [ ] **Step 2: Run full test suite**

```bash
python -m pytest -v 2>&1 | tail -20
```
Expected: all tests PASS (no tests import `parse_tushare_date` or `month_ranges`/`week_ranges` from `_helpers` directly)

- [ ] **Step 3: Commit**

```bash
git add zer0share/sync/_helpers.py
git commit -m "refactor: sync/_helpers.py — str dates, remove parse_tushare_date, use dateutil"
```

---

### Task 4: Update sync domain modules

**Files:**
- Modify: `zer0share/sync/calendar.py`
- Modify: `zer0share/sync/equities.py`
- Modify: `zer0share/sync/futures.py`
- Modify: `zer0share/sync/options.py`
- Modify: `zer0share/sync/industry.py`

- [ ] **Step 1: Rewrite `sync/calendar.py`**

```python
import pandas as pd
from loguru import logger

import zer0share.dateutil as dateutil
from zer0share.storage import read_trade_cal, write_trade_cal
from zer0share.sync import SyncContext
from zer0share.sync._helpers import ALL_EXCHANGES, TRADE_CAL_FIRST_DATE


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
        today = dateutil.today()
        end = today[:4] + "1231"   # last day of current year: YYYY1231
        max_dates: list[str] = []
        for exchange in ALL_EXCHANGES:
            existing = read_trade_cal(ctx.cfg.data_dir, exchange)
            last = str(existing["cal_date"].max()) if not existing.empty else None
            start = dateutil.add_days(last, 1) if last else TRADE_CAL_FIRST_DATE

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
                max_dates.append(str(df["cal_date"].max()))

        ctx.meta.load_trade_cal_from_parquet(ctx.cfg.data_dir, ALL_EXCHANGES)
        if max_dates:
            ctx.meta.update_last_date("trade_cal", min(max_dates))
        logger.info("trade_cal 全部同步完成")
    except Exception as e:
        logger.error(f"trade_cal 同步失败: {e}")
        ctx.notifier.send(f"trade_cal 同步失败: {e}")
        raise
```

- [ ] **Step 2: Rewrite `sync/equities.py`**

```python
import time
from loguru import logger

import pandas as pd

import zer0share.dateutil as dateutil
from zer0share.storage import (
    daily_partition_exists, write_basic,
    write_daily_partition, write_index_weight, index_weight_partition_exists,
)
from zer0share.sync import SyncContext
from zer0share.sync._helpers import (
    FIRST_DATE, INDEX_CODES, index_weight_meta_key,
    should_log_progress, skip_if_not_trading, sync_daily_partitioned,
)
from zer0share.fetcher import INDEX_DAILY_CODES


def sync_basic(ctx: SyncContext) -> None:
    if skip_if_not_trading(ctx, "SSE"):
        return
    today = dateutil.today()
    try:
        df = ctx.fetcher.fetch_basic()
        write_basic(ctx.cfg.data_dir, df)
        ctx.meta.update_last_date("basic", today)
        logger.info(f"basic 同步完成: {len(df)} 条")
    except Exception as e:
        logger.error(f"basic 同步失败: {e}")
        ctx.notifier.send(f"basic 同步失败: {e}")
        raise


def sync_daily_kline(ctx: SyncContext, start_date: str | None = None, end_date: str | None = None) -> None:
    sync_daily_partitioned(ctx, "daily_kline", ctx.fetcher.fetch_daily_kline, start_date, end_date)


def sync_adj_factor(ctx: SyncContext, start_date: str | None = None, end_date: str | None = None) -> None:
    sync_daily_partitioned(ctx, "adj_factor", ctx.fetcher.fetch_adj_factor, start_date, end_date)


def sync_daily_basic(ctx: SyncContext, start_date: str | None = None, end_date: str | None = None) -> None:
    sync_daily_partitioned(ctx, "daily_basic", ctx.fetcher.fetch_daily_basic, start_date, end_date)


def sync_stock_st(ctx: SyncContext, start_date: str | None = None, end_date: str | None = None) -> None:
    sync_daily_partitioned(ctx, "stock_st", ctx.fetcher.fetch_stock_st, start_date, end_date, write_empty=True)


def sync_suspend_d(ctx: SyncContext, start_date: str | None = None, end_date: str | None = None) -> None:
    sync_daily_partitioned(ctx, "suspend_d", ctx.fetcher.fetch_suspend_d, start_date, end_date, write_empty=True)


def sync_stk_limit(ctx: SyncContext, start_date: str | None = None, end_date: str | None = None) -> None:
    sync_daily_partitioned(ctx, "stk_limit", ctx.fetcher.fetch_stk_limit, start_date, end_date)


def sync_index_weight(ctx: SyncContext, start_date: str | None = None, end_date: str | None = None) -> None:
    today = dateutil.today()
    end = end_date or today
    if start_date is not None and start_date > end:
        raise ValueError("start_date must be on or before end_date")

    success = 0
    skipped_existing = 0
    empty_months = 0
    requests = 0
    coverage_dates: list[str] = []
    for index_code in INDEX_CODES:
        meta_key = index_weight_meta_key(index_code)
        last = ctx.meta.get_last_date(meta_key)
        start = start_date or (dateutil.add_days(last, 1) if last else FIRST_DATE)
        if start > end:
            logger.info(f"index_weight {index_code} 已覆盖到 {last}，无需同步")
            if last is not None:
                coverage_dates.append(last)
            continue

        ranges = dateutil.month_ranges(start, end)
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
                        trade_date = str(trade_date_value)
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


def sync_index_daily(ctx: SyncContext, start_date: str | None = None, end_date: str | None = None) -> None:
    today = dateutil.today()
    last = ctx.meta.get_last_date("index_daily")

    if start_date is None:
        start = dateutil.add_days(last, 1) if last else FIRST_DATE
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
        trade_date = str(trade_date_value)
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

- [ ] **Step 3: Rewrite `sync/futures.py`**

```python
import time
from loguru import logger

import pandas as pd

import zer0share.dateutil as dateutil
from zer0share.storage import daily_partition_exists, write_daily_partition
from zer0share.sync import SyncContext
from zer0share.sync._helpers import (
    FIRST_DATE, skip_if_not_trading, sync_daily_partitioned,
)
from zer0share.fetcher import FUTURES_EXCHANGES


def sync_fut_basic(ctx: SyncContext) -> None:
    if skip_if_not_trading(ctx, "SSE"):
        return
    today = dateutil.today()
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


def sync_fut_daily(ctx: SyncContext, start_date: str | None = None, end_date: str | None = None) -> None:
    sync_daily_partitioned(
        ctx, "fut_daily", ctx.fetcher.fetch_fut_daily, start_date, end_date,
        data_dir=ctx.cfg.data_dir / "futures",
    )


def sync_fut_holding(ctx: SyncContext, start_date: str | None = None, end_date: str | None = None) -> None:
    sync_daily_partitioned(
        ctx, "fut_holding", ctx.fetcher.fetch_fut_holding, start_date, end_date,
        data_dir=ctx.cfg.data_dir / "futures",
    )


def sync_fut_wsr(ctx: SyncContext, start_date: str | None = None, end_date: str | None = None) -> None:
    sync_daily_partitioned(
        ctx, "fut_wsr", ctx.fetcher.fetch_fut_wsr, start_date, end_date,
        data_dir=ctx.cfg.data_dir / "futures",
    )


def sync_fut_settle(ctx: SyncContext, start_date: str | None = None, end_date: str | None = None) -> None:
    sync_daily_partitioned(
        ctx, "fut_settle", ctx.fetcher.fetch_fut_settle, start_date, end_date,
        data_dir=ctx.cfg.data_dir / "futures",
    )


def sync_fut_mapping(ctx: SyncContext, start_date: str | None = None, end_date: str | None = None) -> None:
    sync_daily_partitioned(
        ctx, "fut_mapping", ctx.fetcher.fetch_fut_mapping, start_date, end_date,
        data_dir=ctx.cfg.data_dir / "futures",
    )


def sync_ft_limit(ctx: SyncContext, start_date: str | None = None, end_date: str | None = None) -> None:
    sync_daily_partitioned(
        ctx, "ft_limit", ctx.fetcher.fetch_ft_limit, start_date, end_date,
        data_dir=ctx.cfg.data_dir / "futures",
    )


def sync_fut_weekly(ctx: SyncContext, start_date: str | None = None, end_date: str | None = None) -> None:
    sync_daily_partitioned(
        ctx, "fut_weekly", ctx.fetcher.fetch_fut_weekly, start_date, end_date,
        data_dir=ctx.cfg.data_dir / "futures",
    )


def sync_fut_monthly(ctx: SyncContext, start_date: str | None = None, end_date: str | None = None) -> None:
    sync_daily_partitioned(
        ctx, "fut_monthly", ctx.fetcher.fetch_fut_monthly, start_date, end_date,
        data_dir=ctx.cfg.data_dir / "futures",
    )


def sync_fut_index_daily(ctx: SyncContext, start_date: str | None = None, end_date: str | None = None) -> None:
    if skip_if_not_trading(ctx, "SSE"):
        return
    today = dateutil.today()
    last = ctx.meta.get_last_date("fut_index_daily")

    if start_date is None:
        start = dateutil.add_days(last, 1) if last else FIRST_DATE
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
        current = dateutil.add_days(current, 1)

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
        trade_date = str(trade_date_value)
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


def sync_fut_weekly_detail(ctx: SyncContext, start_date: str | None = None, end_date: str | None = None) -> None:
    today = dateutil.today()
    last = ctx.meta.get_last_date("fut_weekly_detail")

    if start_date is None:
        start = dateutil.add_days(last, 1) if last else FIRST_DATE
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
    weeks = dateutil.week_ranges(start, end)
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

- [ ] **Step 4: Update `sync/options.py`**

```python
import time
from loguru import logger

import pandas as pd

import zer0share.dateutil as dateutil
from zer0share.storage import write_opt_basic
from zer0share.sync import SyncContext
from zer0share.sync._helpers import skip_if_not_trading, sync_daily_partitioned
from zer0share.fetcher import OPTIONS_EXCHANGES


def sync_opt_basic(ctx: SyncContext) -> None:
    if skip_if_not_trading(ctx, "SSE"):
        return
    today = dateutil.today()
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


def sync_opt_daily(ctx: SyncContext, start_date: str | None = None, end_date: str | None = None) -> None:
    sync_daily_partitioned(
        ctx, "opt_daily", ctx.fetcher.fetch_opt_daily, start_date, end_date,
        data_dir=ctx.cfg.data_dir / "options",
    )
```

- [ ] **Step 5: Update `sync/industry.py`**

```python
from loguru import logger

import zer0share.dateutil as dateutil
from zer0share.storage import write_sw_classify, write_sw_member, write_ci_member
from zer0share.sync import SyncContext
from zer0share.sync._helpers import skip_if_not_trading


def sync_industry(ctx: SyncContext) -> None:
    if skip_if_not_trading(ctx, "SSE"):
        return
    today = dateutil.today()
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
    today = dateutil.today()
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

- [ ] **Step 6: Run full test suite**

```bash
python -m pytest -v 2>&1 | tail -20
```
Expected: all tests PASS

- [ ] **Step 7: Commit**

```bash
git add zer0share/sync/calendar.py zer0share/sync/equities.py zer0share/sync/futures.py zer0share/sync/options.py zer0share/sync/industry.py
git commit -m "refactor: sync domain modules — str dates, remove date imports"
```

---

### Task 5: Update CLI and its tests

**Files:**
- Modify: `zer0share/cli.py`
- Modify: `tests/test_cli.py`

- [ ] **Step 1: Update `tests/test_cli.py` — new assertions**

Change all `"YYYY-MM-DD"` date strings to `"YYYYMMDD"` format and all `date(...)` assertions in `assert_called_once_with` to string literals. Also update `test_build_universe_accepts_date_range`.

The affected tests are:
- `test_sync_daily_kline_accepts_date_range` — change `"2016-01-01"` → `"20160101"`, `"2016-01-31"` → `"20160131"`, `date(2016, 1, 1)` → `"20160101"`, `date(2016, 1, 31)` → `"20160131"`
- `test_sync_index_daily_accepts_date_range` — `"2024-01-01"` → `"20240101"`, `"2024-01-31"` → `"20240131"`, `date(2024, 1, 1)` → `"20240101"`, `date(2024, 1, 31)` → `"20240131"`
- `test_sync_fut_daily_accepts_date_range` — same pattern
- `test_sync_ft_limit_accepts_date_range` — same pattern
- `test_sync_fut_weekly_detail_accepts_date_range` — same pattern
- `test_sync_opt_daily_accepts_date_range` — same pattern
- `test_sync_industry_rejects_date_range` — `"2024-01-01"` → `"20240101"`
- `test_sync_fut_basic_rejects_date_range` — `"2024-01-01"` → `"20240101"`
- `test_sync_opt_basic_rejects_date_range` — `"2024-01-01"` → `"20240101"`
- `test_build_universe_accepts_date_range` — `"2024-01-01"` → `"20240101"`, `"2024-01-31"` → `"20240131"` (but `build_universes_range` still receives `date(...)` objects — CLI converts internally)
- `test_build_universe_rejects_date_with_range` — `"2024-01-31"` → `"20240131"`, `"2024-01-01"` → `"20240101"`

Remove `from datetime import date` from `test_cli.py`.

Full replacement for the first test as a template:
```python
def test_sync_daily_kline_accepts_date_range():
    runner = CliRunner()
    pipeline = MagicMock()
    pipeline.__enter__.return_value = pipeline
    pipeline.__exit__.return_value = False

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(
            cli,
            [
                "sync",
                "--table",
                "daily_kline",
                "--start-date",
                "20160101",
                "--end-date",
                "20160131",
            ],
        )

    assert result.exit_code == 0
    pipeline.sync_daily_kline.assert_called_once_with(
        start_date="20160101",
        end_date="20160131",
    )
```

For `test_build_universe_accepts_date_range`, the CLI converts strings to `date` before calling `build_universes_range`, so the mock assertion stays with `date`:
```python
def test_build_universe_accepts_date_range(tmp_path):
    runner = CliRunner()
    cfg = MagicMock()
    cfg.data_dir = "data"
    cfg.log_path = tmp_path / "pipeline.log"

    with (
        patch("zer0share.cli.load_config", return_value=cfg),
        patch("zer0share.cli.build_universes_range") as mock_build_range,
    ):
        mock_build_range.return_value = {
            "start_date": "20240101",
            "end_date": "20240131",
            "trading_days": 22,
            "built_days": 20,
            "skipped_days": 2,
            "counts": {"univ_trade_base": 100},
        }
        result = runner.invoke(
            cli,
            [
                "build-universe",
                "--start-date",
                "20240101",
                "--end-date",
                "20240131",
            ],
        )

    assert result.exit_code == 0
    from datetime import date
    mock_build_range.assert_called_once_with(
        "data",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 31),
    )
    assert "built: 20, skipped: 2" in result.output
```

- [ ] **Step 2: Run tests to see them fail**

```bash
python -m pytest tests/test_cli.py -v 2>&1 | tail -20
```
Expected: failures like `AssertionError: call(start_date=datetime.date(2016, 1, 1), ...) != call(start_date='20160101', ...)`

- [ ] **Step 3: Update `cli.py`**

Replace `click.DateTime` on the `sync` command with a `_validate_date` callback. For `build-universe`, keep the same callback but parse to `date` internally before calling `build_universes`/`build_universes_range` (since `universe.py` is out of scope):

```python
from datetime import date, datetime
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


def _validate_date(ctx, param, value):
    if value is None:
        return None
    try:
        datetime.strptime(value, "%Y%m%d")
        return value
    except ValueError:
        raise click.BadParameter("格式应为 YYYYMMDD，例如 20240102")


def _parse_date(s: str) -> date:
    return date(int(s[:4]), int(s[4:6]), int(s[6:]))


@click.group()
def cli():
    pass


# ... SYNC_TABLES unchanged ...


@cli.command()
@click.option("--table", type=click.Choice(SYNC_TABLES), default=None)
@click.option("--all", "sync_all", is_flag=True, default=False)
@click.option("--start-date", default=None, callback=_validate_date)
@click.option("--end-date", default=None, callback=_validate_date)
def sync(
    table: str | None,
    sync_all: bool,
    start_date: str | None,
    end_date: str | None,
) -> None:
    """同步数据。"""
    if end_date is not None and start_date is None:
        raise click.UsageError("--end-date requires --start-date")
    range_tables = {
        "daily_kline", "adj_factor", "daily_basic", "stock_st", "suspend_d",
        "stk_limit", "index_weight", "index_daily", "fut_daily", "fut_holding",
        "fut_wsr", "fut_settle", "fut_mapping", "ft_limit", "fut_weekly",
        "fut_monthly", "fut_index_daily", "fut_weekly_detail", "opt_daily",
    }
    if (start_date is not None or end_date is not None) and table not in range_tables:
        raise click.UsageError("date range options are only supported for daily partitioned tables")
    if start_date is not None and end_date is not None and end_date < start_date:
        raise click.UsageError("--end-date must be on or after --start-date")

    with _make_pipeline() as pipeline:
        if sync_all or table == "trade_cal":
            pipeline.sync_trade_cal()
        if sync_all or table == "basic":
            pipeline.sync_basic()
        if sync_all or table == "daily_kline":
            pipeline.sync_daily_kline(start_date=start_date, end_date=end_date)
        if sync_all or table == "adj_factor":
            pipeline.sync_adj_factor(start_date=start_date, end_date=end_date)
        if sync_all or table == "daily_basic":
            pipeline.sync_daily_basic(start_date=start_date, end_date=end_date)
        if sync_all or table == "stock_st":
            pipeline.sync_stock_st(start_date=start_date, end_date=end_date)
        if sync_all or table == "suspend_d":
            pipeline.sync_suspend_d(start_date=start_date, end_date=end_date)
        if sync_all or table == "stk_limit":
            pipeline.sync_stk_limit(start_date=start_date, end_date=end_date)
        if sync_all or table == "index_weight":
            pipeline.sync_index_weight(start_date=start_date, end_date=end_date)
        if sync_all or table == "index_daily":
            pipeline.sync_index_daily(start_date=start_date, end_date=end_date)
        if sync_all or table == "industry":
            pipeline.sync_industry()
        if sync_all or table == "ci_member":
            pipeline.sync_ci_member()
        if sync_all or table == "fut_basic":
            pipeline.sync_fut_basic()
        if sync_all or table == "fut_daily":
            pipeline.sync_fut_daily(start_date=start_date, end_date=end_date)
        if sync_all or table == "fut_holding":
            pipeline.sync_fut_holding(start_date=start_date, end_date=end_date)
        if sync_all or table == "fut_wsr":
            pipeline.sync_fut_wsr(start_date=start_date, end_date=end_date)
        if sync_all or table == "fut_settle":
            pipeline.sync_fut_settle(start_date=start_date, end_date=end_date)
        if sync_all or table == "fut_mapping":
            pipeline.sync_fut_mapping(start_date=start_date, end_date=end_date)
        if sync_all or table == "ft_limit":
            pipeline.sync_ft_limit(start_date=start_date, end_date=end_date)
        if sync_all or table == "fut_weekly":
            pipeline.sync_fut_weekly(start_date=start_date, end_date=end_date)
        if sync_all or table == "fut_monthly":
            pipeline.sync_fut_monthly(start_date=start_date, end_date=end_date)
        if sync_all or table == "fut_index_daily":
            pipeline.sync_fut_index_daily(start_date=start_date, end_date=end_date)
        if sync_all or table == "fut_weekly_detail":
            pipeline.sync_fut_weekly_detail(start_date=start_date, end_date=end_date)
        if sync_all or table == "opt_basic":
            pipeline.sync_opt_basic()
        if sync_all or table == "opt_daily":
            pipeline.sync_opt_daily(start_date=start_date, end_date=end_date)
```

For `build-universe`, accept `YYYYMMDD`, then parse to `date` before calling the function (since `universe.py` is out of scope):

```python
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
```

- [ ] **Step 4: Run tests to see them pass**

```bash
python -m pytest tests/test_cli.py -v 2>&1 | tail -20
```
Expected: all tests PASS

- [ ] **Step 5: Run full test suite**

```bash
python -m pytest -v 2>&1 | tail -30
```
Expected: all tests PASS

- [ ] **Step 6: Static invariant check**

```bash
grep -rn "from datetime import date" zer0share/
```
Expected output — exactly two lines:
```
zer0share/dateutil.py:1:from datetime import date, timedelta
zer0share/storage.py:5:from datetime import date, datetime, timezone
```

```bash
grep -rn "\.strftime\|timedelta\|date\.today\|parse_tushare_date" zer0share/sync/
```
Expected: no output (empty)

- [ ] **Step 7: Commit**

```bash
git add zer0share/cli.py tests/test_cli.py
git commit -m "refactor: CLI accepts YYYYMMDD strings, remove click.DateTime from sync command"
```
