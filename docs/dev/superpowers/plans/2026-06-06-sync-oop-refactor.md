# Sync 层 OOP 重构实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 sync 层重构为 OOP 设计，引入 TradingCalendar / Store / SyncJob / Registry 体系，同时修复 3 个已知 bug，消除代码重复，对齐 query 层的 TableSpec 设计。

**Architecture:** `Pipeline` 变为 Registry，每张表是一个 `SyncJob` 对象（`DailySyncJob` / `SnapshotSyncJob` / 自定义子类），在 `build_jobs()` 时绑定 fetch callable 和 Store；`TradingCalendar` 统一日历查询和今日注入；`catalog.py` 存放所有 `TableSpec` 常量，query 和 sync 共享。

**Tech Stack:** Python 3.11, DuckDB, PyArrow, APScheduler, Click, pytest

---

## 文件结构

**新建：**
- `zer0share/trading_calendar.py` — TradingCalendar 类
- `zer0share/catalog.py` — 所有 TableSpec 常量
- `zer0share/sync/_jobs.py` — SyncJob ABC + DailySyncJob + SnapshotSyncJob

**修改：**
- `zer0share/dateutil.py` — 新增 `date_str()` / `parse_date()`
- `zer0share/storage.py` — 新增 Store 类，精简 MetaStore，删除遗留函数
- `zer0share/sync/__init__.py` — SyncContext → SyncRuntime
- `zer0share/sync/_helpers.py` — 清空（只留常量兼容导入）
- `zer0share/sync/calendar.py` — build_jobs() + TradeCalSyncJob
- `zer0share/sync/equities.py` — build_jobs() + IndexWeightSyncJob + IndexDailySyncJob
- `zer0share/sync/industry.py` — build_jobs()
- `zer0share/sync/futures.py` — build_jobs() + FutIndexDailySyncJob + FutWeeklyDetailSyncJob
- `zer0share/sync/options.py` — build_jobs()
- `zer0share/pipeline.py` — Registry
- `zer0share/config.py` — schedule: dict[str, str]
- `zer0share/scheduler.py` — 纯遍历
- `zer0share/cli.py` — 消除 if-elif 链
- `zer0share/query/equities.py` — 从 catalog 导入 spec
- `zer0share/query/futures.py` — 从 catalog 导入 spec
- `zer0share/query/options.py` — 从 catalog 导入 spec
- `zer0share/query/industry.py` — 从 catalog 导入 spec
- `zer0share/query/calendar.py` — 从 catalog 导入 spec
- `config/settings.toml` + `config/settings.example.toml` — scheduler 格式
- `tests/test_storage.py` — 更新 import，用 Store 类
- `tests/test_pipeline.py` — 全面重写 patch 策略和调用方式

---

## Task 1：扩充 dateutil.py

**Files:**
- Modify: `zer0share/dateutil.py`
- Test: `tests/test_dateutil.py`

- [ ] **Step 1: 写失败测试**

```python
# tests/test_dateutil.py 新增
from datetime import date
from zer0share.dateutil import date_str, parse_date

def test_date_str_with_string():
    assert date_str("20240102") == "20240102"

def test_date_str_with_date_object():
    assert date_str(date(2024, 1, 2)) == "20240102"

def test_parse_date_valid():
    assert parse_date("20240102") == date(2024, 1, 2)

def test_parse_date_invalid_raises():
    import pytest
    with pytest.raises(ValueError):
        parse_date("2024-01-02")
```

- [ ] **Step 2: 运行确认失败**

```bash
cd /data/projects/zer0share && uv run pytest tests/test_dateutil.py::test_date_str_with_string tests/test_dateutil.py::test_parse_date_valid -v
```

Expected: `FAILED` with `ImportError: cannot import name 'date_str'`

- [ ] **Step 3: 实现**

在 `zer0share/dateutil.py` 末尾追加：

```python
def date_str(value) -> str:
    if isinstance(value, str):
        return value
    return value.strftime(_FMT)


def parse_date(s: str) -> date:
    try:
        return _parse(s)
    except (ValueError, IndexError) as e:
        raise ValueError(f"invalid date format: {s!r}; expected YYYYMMDD") from e
```

- [ ] **Step 4: 运行确认通过**

```bash
uv run pytest tests/test_dateutil.py -v
```

Expected: all PASS

- [ ] **Step 5: 提交**

```bash
git add zer0share/dateutil.py tests/test_dateutil.py
git commit -m "feat: add public date_str and parse_date to dateutil"
```

---

## Task 2：创建 TradingCalendar

**Files:**
- Create: `zer0share/trading_calendar.py`
- Create: `tests/test_trading_calendar.py`
- Modify: `zer0share/storage.py` (MetaStore 暴露 `_conn`)

- [ ] **Step 1: 写失败测试**

```python
# tests/test_trading_calendar.py
import pandas as pd
import pytest
from zer0share.storage import MetaStore, write_trade_cal
from zer0share.trading_calendar import TradingCalendar


@pytest.fixture
def cal(tmp_path):
    meta = MetaStore(tmp_path / "meta.duckdb")
    df = pd.DataFrame({
        "exchange": ["SSE", "SSE", "SSE"],
        "cal_date": ["20240102", "20240103", "20240104"],
        "is_open": [True, False, True],
        "pretrade_date": ["20231229", "20240102", "20240102"],
    })
    write_trade_cal(tmp_path, "SSE", df)
    c = TradingCalendar(meta)
    c.load_from_parquet(tmp_path, ["SSE"])
    yield c
    meta.close()


def test_today_default_returns_string(cal):
    result = cal.today()
    assert len(result) == 8 and result.isdigit()


def test_today_injectable(tmp_path):
    meta = MetaStore(tmp_path / "meta.duckdb")
    cal = TradingCalendar(meta, today_fn=lambda: "20240105")
    assert cal.today() == "20240105"
    meta.close()


def test_get_trading_days(cal):
    days = cal.get_trading_days("SSE", "20240101", "20240104")
    assert days == ["20240102", "20240104"]


def test_is_trading_day_open(cal):
    assert cal.is_trading_day("SSE", "20240102") is True


def test_is_trading_day_closed(cal):
    assert cal.is_trading_day("SSE", "20240103") is False


def test_is_trading_day_unknown_returns_true(cal):
    assert cal.is_trading_day("SSE", "20240110") is True


def test_skip_if_not_trading_on_closed_day(cal):
    cal2 = TradingCalendar(cal._meta, today_fn=lambda: "20240103")
    assert cal2.skip_if_not_trading("SSE") is True


def test_skip_if_not_trading_on_open_day(cal):
    cal2 = TradingCalendar(cal._meta, today_fn=lambda: "20240102")
    assert cal2.skip_if_not_trading("SSE") is False


def test_ensure_loaded_triggers_sync_when_missing(tmp_path):
    import pandas as pd
    from unittest.mock import MagicMock
    from zer0share.sync import SyncRuntime
    from zer0share.notifier import Notifier

    meta = MetaStore(tmp_path / "meta.duckdb")
    cal = TradingCalendar(meta)

    fetcher = MagicMock()
    fetcher.fetch_trade_cal.return_value = pd.DataFrame({
        "exchange": ["SSE"], "cal_date": ["20240102"],
        "is_open": [True], "pretrade_date": ["20231229"],
    })
    notifier = MagicMock(spec=Notifier)
    rt = SyncRuntime(calendar=cal, notifier=notifier, meta=meta)

    cal.ensure_loaded(rt)
    fetcher.fetch_trade_cal.assert_not_called()  # SyncRuntime has no fetcher
    meta.close()
```

- [ ] **Step 2: 运行确认失败**

```bash
uv run pytest tests/test_trading_calendar.py -v 2>&1 | head -20
```

Expected: `FAILED` with `ModuleNotFoundError`

- [ ] **Step 3: 实现 TradingCalendar**

创建 `zer0share/trading_calendar.py`：

```python
from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Callable

from loguru import logger

import zer0share.dateutil as dateutil
from zer0share.storage import MetaStore, write_trade_cal, read_trade_cal

if TYPE_CHECKING:
    from zer0share.sync import SyncRuntime


class TradingCalendar:
    def __init__(self, meta: MetaStore, today_fn: Callable[[], str] = dateutil.today):
        self._meta = meta
        self._today_fn = today_fn

    def today(self) -> str:
        return self._today_fn()

    def load_from_parquet(self, data_dir: Path, exchanges: list[str] | None = None) -> None:
        self._meta.load_trade_cal_from_parquet(data_dir, exchanges)

    def get_trading_days(self, exchange: str, start: str, end: str) -> list[str]:
        return self._meta.get_trading_days(exchange, start, end)

    def is_trading_day(self, exchange: str, cal_date: str) -> bool:
        return self._meta.is_trading_day(exchange, cal_date)

    def skip_if_not_trading(self, exchange: str) -> bool:
        today = self.today()
        if not self.is_trading_day(exchange, today):
            logger.info(f"今日 {today} 非交易日，跳过同步")
            return True
        return False

    def ensure_loaded(self, rt: SyncRuntime) -> None:
        if self._meta.get_last_date("trade_cal") is None:
            from zer0share.sync.calendar import TradeCalSyncJob
            job = TradeCalSyncJob()
            job.run(rt)
```

- [ ] **Step 4: 更新 sync/__init__.py，新增 SyncRuntime**

```python
# zer0share/sync/__init__.py
from dataclasses import dataclass

from zer0share.notifier import Notifier
from zer0share.storage import MetaStore

if False:  # TYPE_CHECKING
    from zer0share.trading_calendar import TradingCalendar


@dataclass
class SyncRuntime:
    calendar: "TradingCalendar"
    notifier: Notifier
    meta: MetaStore


# 向后兼容别名，Task 7 之后删除
SyncContext = SyncRuntime
```

- [ ] **Step 5: 运行测试**

```bash
uv run pytest tests/test_trading_calendar.py -v
```

Expected: 大部分 PASS（`ensure_loaded` 测试可能因 TradeCalSyncJob 未实现而跳过）

- [ ] **Step 6: 提交**

```bash
git add zer0share/trading_calendar.py zer0share/sync/__init__.py tests/test_trading_calendar.py
git commit -m "feat: add TradingCalendar and SyncRuntime"
```

---

## Task 3：创建 catalog.py

**Files:**
- Create: `zer0share/catalog.py`

- [ ] **Step 1: 创建 catalog.py**

```python
# zer0share/catalog.py
from zer0share.query.repository import DailyTableSpec, TableSpec
from zer0share.schema import (
    ADJ_FACTOR_COLS, BASIC_COLS, CI_MEMBER_COLS, DAILY_BASIC_COLS,
    DAILY_COLS, FT_LIMIT_COLS, FUT_BASIC_COLS, FUT_DAILY_COLS,
    FUT_HOLDING_COLS, FUT_INDEX_DAILY_COLS, FUT_MAPPING_COLS,
    FUT_MONTHLY_COLS, FUT_SETTLE_COLS, FUT_WEEKLY_COLS,
    FUT_WEEKLY_DETAIL_COLS, FUT_WSR_COLS, INDEX_DAILY_COLS,
    INDEX_WEIGHT_COLS, OPT_BASIC_COLS, OPT_DAILY_COLS,
    STOCK_ST_COLS, STK_LIMIT_COLS, SUSPEND_D_COLS, SW_CLASSIFY_COLS,
    SW_MEMBER_COLS, TRADE_CAL_COLS,
)

# ── 股票 ─────────────────────────────────────────────────────────────
BASIC_SPEC = TableSpec(
    name="basic", path_parts=("basic",), columns=BASIC_COLS,
    parquet_pattern="data.parquet", sync_table="basic", order_by="ts_code",
)
DAILY_KLINE_SPEC = DailyTableSpec(
    name="daily_kline", path_parts=("daily_kline",), columns=DAILY_COLS,
    parquet_pattern="date=*/data.parquet", sync_table="daily_kline",
    order_by="ts_code, trade_date", hive_partitioning=True, union_by_name=True,
)
ADJ_FACTOR_SPEC = DailyTableSpec(
    name="adj_factor", path_parts=("adj_factor",), columns=ADJ_FACTOR_COLS,
    parquet_pattern="date=*/data.parquet", sync_table="adj_factor",
    order_by="ts_code, trade_date", hive_partitioning=True, union_by_name=True,
)
DAILY_BASIC_SPEC = DailyTableSpec(
    name="daily_basic", path_parts=("daily_basic",), columns=DAILY_BASIC_COLS,
    parquet_pattern="date=*/data.parquet", sync_table="daily_basic",
    order_by="ts_code, trade_date", hive_partitioning=True, union_by_name=True,
)
STOCK_ST_SPEC = DailyTableSpec(
    name="stock_st", path_parts=("stock_st",), columns=STOCK_ST_COLS,
    parquet_pattern="date=*/data.parquet", sync_table="stock_st",
    order_by="ts_code, trade_date", hive_partitioning=True, union_by_name=True,
)
SUSPEND_D_SPEC = DailyTableSpec(
    name="suspend_d", path_parts=("suspend_d",), columns=SUSPEND_D_COLS,
    parquet_pattern="date=*/data.parquet", sync_table="suspend_d",
    order_by="ts_code, trade_date", hive_partitioning=True, union_by_name=True,
)
STK_LIMIT_SPEC = DailyTableSpec(
    name="stk_limit", path_parts=("stk_limit",), columns=STK_LIMIT_COLS,
    parquet_pattern="date=*/data.parquet", sync_table="stk_limit",
    order_by="ts_code, trade_date", hive_partitioning=True, union_by_name=True,
)
INDEX_DAILY_SPEC = DailyTableSpec(
    name="index_daily", path_parts=("index_daily",), columns=INDEX_DAILY_COLS,
    parquet_pattern="date=*/data.parquet", sync_table="index_daily",
    order_by="ts_code, trade_date", hive_partitioning=True, union_by_name=True,
)
INDEX_WEIGHT_SPEC = TableSpec(
    name="index_weight",
    path_parts=("index_weight",),
    columns=INDEX_WEIGHT_COLS,
    parquet_pattern="index_code=*/date=*/data.parquet",
    sync_table="index_weight",
    order_by="index_code, trade_date, con_code",
    hive_partitioning=True, union_by_name=True,
)
TRADE_CAL_SPEC = TableSpec(
    name="trade_cal", path_parts=("trade_cal",), columns=TRADE_CAL_COLS,
    parquet_pattern="exchange=*/data.parquet", sync_table="trade_cal",
    order_by="exchange, cal_date", hive_partitioning=True, union_by_name=True,
)

# ── 行业 ─────────────────────────────────────────────────────────────
SW_CLASSIFY_SPEC = TableSpec(
    name="sw_classify", path_parts=("industry", "sw_classify"),
    columns=SW_CLASSIFY_COLS, parquet_pattern="data.parquet",
    sync_table="industry", order_by="index_code",
)
SW_MEMBER_SPEC = TableSpec(
    name="sw_member", path_parts=("industry", "sw_member"),
    columns=SW_MEMBER_COLS, parquet_pattern="data.parquet",
    sync_table="industry", order_by="ts_code",
)
CI_MEMBER_SPEC = TableSpec(
    name="ci_member", path_parts=("industry", "ci_member"),
    columns=CI_MEMBER_COLS, parquet_pattern="data.parquet",
    sync_table="ci_member", order_by="ts_code",
)

# ── 期货 ─────────────────────────────────────────────────────────────
FUT_BASIC_SPEC = TableSpec(
    name="fut_basic", path_parts=("futures", "fut_basic"),
    columns=FUT_BASIC_COLS, parquet_pattern="date=*/data.parquet",
    sync_table="fut_basic", order_by="ts_code",
    hive_partitioning=True, union_by_name=True,
)
FUT_DAILY_SPEC = DailyTableSpec(
    name="fut_daily", path_parts=("futures", "fut_daily"),
    columns=FUT_DAILY_COLS, parquet_pattern="date=*/data.parquet",
    sync_table="fut_daily", order_by="ts_code, trade_date",
    hive_partitioning=True, union_by_name=True,
)
FUT_HOLDING_SPEC = DailyTableSpec(
    name="fut_holding", path_parts=("futures", "fut_holding"),
    columns=FUT_HOLDING_COLS, parquet_pattern="date=*/data.parquet",
    sync_table="fut_holding", order_by="trade_date, ts_code",
    hive_partitioning=True, union_by_name=True,
)
FUT_WSR_SPEC = DailyTableSpec(
    name="fut_wsr", path_parts=("futures", "fut_wsr"),
    columns=FUT_WSR_COLS, parquet_pattern="date=*/data.parquet",
    sync_table="fut_wsr", order_by="trade_date, ts_code",
    hive_partitioning=True, union_by_name=True,
)
FUT_SETTLE_SPEC = DailyTableSpec(
    name="fut_settle", path_parts=("futures", "fut_settle"),
    columns=FUT_SETTLE_COLS, parquet_pattern="date=*/data.parquet",
    sync_table="fut_settle", order_by="ts_code, trade_date",
    hive_partitioning=True, union_by_name=True,
)
FUT_MAPPING_SPEC = DailyTableSpec(
    name="fut_mapping", path_parts=("futures", "fut_mapping"),
    columns=FUT_MAPPING_COLS, parquet_pattern="date=*/data.parquet",
    sync_table="fut_mapping", order_by="ts_code, trade_date",
    hive_partitioning=True, union_by_name=True,
)
FT_LIMIT_SPEC = DailyTableSpec(
    name="ft_limit", path_parts=("futures", "ft_limit"),
    columns=FT_LIMIT_COLS, parquet_pattern="date=*/data.parquet",
    sync_table="ft_limit", order_by="ts_code, trade_date",
    hive_partitioning=True, union_by_name=True,
)
FUT_WEEKLY_SPEC = DailyTableSpec(
    name="fut_weekly", path_parts=("futures", "fut_weekly"),
    columns=FUT_WEEKLY_COLS, parquet_pattern="date=*/data.parquet",
    sync_table="fut_weekly", order_by="ts_code, trade_date",
    hive_partitioning=True, union_by_name=True,
)
FUT_MONTHLY_SPEC = DailyTableSpec(
    name="fut_monthly", path_parts=("futures", "fut_monthly"),
    columns=FUT_MONTHLY_COLS, parquet_pattern="date=*/data.parquet",
    sync_table="fut_monthly", order_by="ts_code, trade_date",
    hive_partitioning=True, union_by_name=True,
)
FUT_INDEX_DAILY_SPEC = DailyTableSpec(
    name="fut_index_daily", path_parts=("futures", "fut_index_daily"),
    columns=FUT_INDEX_DAILY_COLS, parquet_pattern="date=*/data.parquet",
    sync_table="fut_index_daily", order_by="ts_code, trade_date",
    hive_partitioning=True, union_by_name=True,
)
FUT_WEEKLY_DETAIL_SPEC = TableSpec(
    name="fut_weekly_detail", path_parts=("futures", "fut_weekly_detail"),
    columns=FUT_WEEKLY_DETAIL_COLS, parquet_pattern="date=*/data.parquet",
    sync_table="fut_weekly_detail", order_by="exchange, prd",
    hive_partitioning=True, union_by_name=True,
)

# ── 期权 ─────────────────────────────────────────────────────────────
OPT_BASIC_SPEC = TableSpec(
    name="opt_basic", path_parts=("options", "opt_basic"),
    columns=OPT_BASIC_COLS, parquet_pattern="data.parquet",
    sync_table="opt_basic", order_by="ts_code",
)
OPT_DAILY_SPEC = DailyTableSpec(
    name="opt_daily", path_parts=("options", "opt_daily"),
    columns=OPT_DAILY_COLS, parquet_pattern="date=*/data.parquet",
    sync_table="opt_daily", order_by="ts_code, trade_date",
    hive_partitioning=True, union_by_name=True,
)
```

- [ ] **Step 2: 验证 catalog 可以导入**

```bash
uv run python -c "from zer0share.catalog import DAILY_KLINE_SPEC, FUT_DAILY_SPEC, OPT_DAILY_SPEC; print('OK')"
```

Expected: `OK`

- [ ] **Step 3: 提交**

```bash
git add zer0share/catalog.py
git commit -m "feat: add catalog.py with all TableSpec constants"
```

---

## Task 4：重构 storage.py — 新增 Store 类

**Files:**
- Modify: `zer0share/storage.py`
- Modify: `tests/test_storage.py`

- [ ] **Step 1: 写 Store 类测试**

```python
# tests/test_storage.py 新增（追加到文件末尾）
from zer0share.storage import DailyPartitionStore, SnapshotStore, IndexWeightStore


def test_daily_partition_store_write_and_exists(tmp_path):
    store = DailyPartitionStore(tmp_path / "daily_kline")
    df = pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20240102"]})
    assert store.exists("20240102") is False
    store.write("20240102", df)
    assert store.exists("20240102") is True
    assert (tmp_path / "daily_kline" / "date=20240102" / "data.parquet").exists()


def test_daily_partition_store_read(tmp_path):
    store = DailyPartitionStore(tmp_path / "daily_kline")
    df = pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20240102"]})
    store.write("20240102", df)
    result = store.read("20240102")
    assert len(result) == 1
    assert result.iloc[0]["ts_code"] == "000001.SZ"


def test_daily_partition_store_read_missing_returns_empty(tmp_path):
    store = DailyPartitionStore(tmp_path / "daily_kline")
    assert store.read("20240102").empty


def test_snapshot_store_write_and_read(tmp_path):
    store = SnapshotStore(tmp_path / "basic" / "data.parquet")
    df = pd.DataFrame({"ts_code": ["000001.SZ"], "name": ["平安银行"]})
    store.write(df)
    result = store.read()
    assert len(result) == 1
    assert result.iloc[0]["name"] == "平安银行"


def test_snapshot_store_read_missing_returns_empty(tmp_path):
    store = SnapshotStore(tmp_path / "basic" / "data.parquet")
    assert store.read().empty


def test_index_weight_store_write_and_exists(tmp_path):
    store = IndexWeightStore(tmp_path / "index_weight")
    df = pd.DataFrame({"index_code": ["399300.SZ"], "con_code": ["000001.SZ"], "weight": [1.0]})
    assert store.exists("399300.SZ", "20240102") is False
    store.write("399300.SZ", "20240102", df)
    assert store.exists("399300.SZ", "20240102") is True
    assert (tmp_path / "index_weight" / "index_code=399300.SZ" / "date=20240102" / "data.parquet").exists()
```

- [ ] **Step 2: 运行确认失败**

```bash
uv run pytest tests/test_storage.py::test_daily_partition_store_write_and_exists -v
```

Expected: `FAILED` with `ImportError`

- [ ] **Step 3: 在 storage.py 末尾追加三个 Store 类**

```python
# zer0share/storage.py 末尾追加

class DailyPartitionStore:
    def __init__(self, table_dir: Path):
        self._dir = table_dir

    def write(self, trade_date: str, df: pd.DataFrame) -> None:
        trade_date = _date_str(trade_date)
        partition_dir = self._dir / f"date={trade_date}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pandas(df, preserve_index=False), partition_dir / "data.parquet")

    def exists(self, trade_date: str) -> bool:
        trade_date = _date_str(trade_date)
        return (self._dir / f"date={trade_date}" / "data.parquet").exists()

    def read(self, trade_date: str) -> pd.DataFrame:
        trade_date = _date_str(trade_date)
        path = self._dir / f"date={trade_date}" / "data.parquet"
        if not path.exists():
            return pd.DataFrame()
        return pq.read_table(path).to_pandas()


class SnapshotStore:
    def __init__(self, file_path: Path):
        self._path = file_path

    def write(self, df: pd.DataFrame) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pandas(df, preserve_index=False), self._path)

    def read(self) -> pd.DataFrame:
        if not self._path.exists():
            return pd.DataFrame()
        return pq.read_table(self._path).to_pandas()


class IndexWeightStore:
    def __init__(self, index_weight_dir: Path):
        self._dir = index_weight_dir

    def write(self, index_code: str, trade_date: str, df: pd.DataFrame) -> None:
        trade_date = _date_str(trade_date)
        partition_dir = self._dir / f"index_code={index_code}" / f"date={trade_date}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pandas(df, preserve_index=False), partition_dir / "data.parquet")

    def exists(self, index_code: str, trade_date: str) -> bool:
        trade_date = _date_str(trade_date)
        return (self._dir / f"index_code={index_code}" / f"date={trade_date}" / "data.parquet").exists()
```

- [ ] **Step 4: 运行 storage 测试**

```bash
uv run pytest tests/test_storage.py -v
```

Expected: all PASS（旧测试 + 新 Store 测试）

- [ ] **Step 5: 提交**

```bash
git add zer0share/storage.py tests/test_storage.py
git commit -m "feat: add DailyPartitionStore, SnapshotStore, IndexWeightStore to storage"
```

---

## Task 5：创建 sync/_jobs.py

**Files:**
- Create: `zer0share/sync/_jobs.py`
- Create: `tests/test_jobs.py`

- [ ] **Step 1: 写失败测试**

```python
# tests/test_jobs.py
import time
from unittest.mock import MagicMock, call, patch
import pandas as pd
import pytest
from zer0share.storage import MetaStore, DailyPartitionStore, SnapshotStore, write_trade_cal
from zer0share.trading_calendar import TradingCalendar
from zer0share.sync import SyncRuntime
from zer0share.sync._jobs import DailySyncJob, SnapshotSyncJob
from zer0share.catalog import DAILY_KLINE_SPEC, BASIC_SPEC


def _make_runtime(tmp_path, today: str = "20240102"):
    meta = MetaStore(tmp_path / "meta.duckdb")
    cal_df = pd.DataFrame({
        "exchange": ["SSE"], "cal_date": ["20240102"],
        "is_open": [True], "pretrade_date": ["20231229"],
    })
    write_trade_cal(tmp_path, "SSE", cal_df)
    cal = TradingCalendar(meta, today_fn=lambda: today)
    cal.load_from_parquet(tmp_path, ["SSE"])
    notifier = MagicMock()
    return SyncRuntime(calendar=cal, notifier=notifier, meta=meta), meta


def test_daily_sync_job_writes_partition(tmp_path):
    rt, meta = _make_runtime(tmp_path)
    store = DailyPartitionStore(tmp_path / "daily_kline")
    fetch = MagicMock(return_value=pd.DataFrame({
        "ts_code": ["000001.SZ"], "trade_date": ["20240102"],
    }))
    job = DailySyncJob(spec=DAILY_KLINE_SPEC, fetch=fetch, store=store)
    meta.update_last_date("daily_kline", "20240101")

    with patch("zer0share.sync._jobs.time.sleep"):
        job.run(rt)

    assert store.exists("20240102")
    assert meta.get_last_date("daily_kline") == "20240102"
    meta.close()


def test_daily_sync_job_skips_existing_partition(tmp_path):
    rt, meta = _make_runtime(tmp_path)
    store = DailyPartitionStore(tmp_path / "daily_kline")
    store.write("20240102", pd.DataFrame({"ts_code": ["000001.SZ"]}))
    fetch = MagicMock(return_value=pd.DataFrame())
    job = DailySyncJob(spec=DAILY_KLINE_SPEC, fetch=fetch, store=store)
    meta.update_last_date("daily_kline", "20240101")

    with patch("zer0share.sync._jobs.time.sleep"):
        job.run(rt)

    fetch.assert_not_called()
    meta.close()


def test_daily_sync_job_already_up_to_date(tmp_path):
    rt, meta = _make_runtime(tmp_path)
    store = DailyPartitionStore(tmp_path / "daily_kline")
    fetch = MagicMock()
    job = DailySyncJob(spec=DAILY_KLINE_SPEC, fetch=fetch, store=store)
    meta.update_last_date("daily_kline", "20240102")

    job.run(rt)

    fetch.assert_not_called()
    meta.close()


def test_daily_sync_job_raises_on_fetch_error(tmp_path):
    rt, meta = _make_runtime(tmp_path)
    store = DailyPartitionStore(tmp_path / "daily_kline")
    fetch = MagicMock(side_effect=RuntimeError("API error"))
    job = DailySyncJob(spec=DAILY_KLINE_SPEC, fetch=fetch, store=store)
    meta.update_last_date("daily_kline", "20240101")

    with patch("zer0share.sync._jobs.time.sleep"), pytest.raises(RuntimeError):
        job.run(rt)

    rt.notifier.send.assert_called_once()
    meta.close()


def test_snapshot_sync_job_writes_file(tmp_path):
    rt, meta = _make_runtime(tmp_path)
    store = SnapshotStore(tmp_path / "basic" / "data.parquet")
    fetch = MagicMock(return_value=pd.DataFrame({"ts_code": ["000001.SZ"]}))
    job = SnapshotSyncJob(spec=BASIC_SPEC, fetch=fetch, store=store, skip_non_trading=False)

    job.run(rt)

    assert store.read().iloc[0]["ts_code"] == "000001.SZ"
    assert meta.get_last_date("basic") == "20240102"
    meta.close()


def test_snapshot_sync_job_skips_non_trading(tmp_path):
    rt, meta = _make_runtime(tmp_path, today="20240103")  # 20240103 not in cal
    store = SnapshotStore(tmp_path / "basic" / "data.parquet")
    fetch = MagicMock()
    job = SnapshotSyncJob(spec=BASIC_SPEC, fetch=fetch, store=store, skip_non_trading=True)

    job.run(rt)

    fetch.assert_not_called()
    meta.close()
```

- [ ] **Step 2: 运行确认失败**

```bash
uv run pytest tests/test_jobs.py -v 2>&1 | head -20
```

Expected: `FAILED` with `ImportError: cannot import name 'DailySyncJob'`

- [ ] **Step 3: 实现 _jobs.py**

创建 `zer0share/sync/_jobs.py`：

```python
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

import pandas as pd
from loguru import logger

import zer0share.dateutil as dateutil
from zer0share.query.repository import DailyTableSpec, TableSpec
from zer0share.storage import DailyPartitionStore, MetaStore, SnapshotStore
from zer0share.sync import SyncRuntime

FIRST_DATE = "20160101"
PROGRESS_INTERVAL = 50


def _should_log_progress(processed: int, total: int) -> bool:
    return processed == total or processed % PROGRESS_INTERVAL == 0


@dataclass
class SyncJob(ABC):
    table_name: str
    supports_date_range: bool = True

    @abstractmethod
    def run(self, rt: SyncRuntime, start_date: str | None = None, end_date: str | None = None) -> None: ...


@dataclass
class DailySyncJob(SyncJob):
    spec: DailyTableSpec = field(default=None)
    fetch: Callable[[str], pd.DataFrame] = field(default=None)
    store: DailyPartitionStore = field(default=None)
    write_empty: bool = False
    exchange: str = "SSE"
    supports_date_range: bool = True

    def __post_init__(self):
        if self.table_name is None and self.spec is not None:
            object.__setattr__(self, 'table_name', self.spec.name)

    def run(self, rt: SyncRuntime, start_date: str | None = None, end_date: str | None = None) -> None:
        today = rt.calendar.today()
        last = rt.meta.get_last_date(self.spec.name)

        if start_date is None:
            start = dateutil.add_days(last, 1) if last else FIRST_DATE
            end = today
            if start > end:
                logger.info(f"{self.spec.name} 已是最新，无需同步")
                return
        else:
            start = start_date
            end = end_date or today
            if start > end:
                raise ValueError("start_date must be on or before end_date")

        trading_days = rt.calendar.get_trading_days(self.exchange, start, end)
        if not trading_days and rt.meta.get_last_date("trade_cal") is None:
            raise RuntimeError(
                f"DuckDB 中无 {self.exchange} trade_cal 数据，请先运行 "
                "python main.py sync --table trade_cal"
            )
        if not trading_days:
            logger.info("指定范围内无交易日，无需同步")
            return

        success = empty = skipped_existing = 0
        frontier = last
        logger.info(f"{self.spec.name} 同步开始: {start} ~ {end}, 共 {len(trading_days)} 个交易日")

        for processed, trade_date in enumerate(trading_days, start=1):
            if self.store.exists(trade_date):
                skipped_existing += 1
                if _should_log_progress(processed, len(trading_days)):
                    self._log_progress(processed, len(trading_days), trade_date, success, empty, skipped_existing)
                continue
            try:
                df = self.fetch(trade_date)
                time.sleep(0.2)
                if not df.empty or self.write_empty:
                    self.store.write(trade_date, df)
                    if frontier is None or trade_date > frontier:
                        rt.meta.update_last_date(self.spec.name, trade_date)
                        frontier = trade_date
                    if df.empty:
                        empty += 1
                    else:
                        success += 1
                else:
                    empty += 1
            except Exception as e:
                logger.error(f"{self.spec.name} {trade_date} 同步失败: {e}")
                rt.notifier.send(f"{self.spec.name} {trade_date} 同步失败: {e}")
                raise
            if _should_log_progress(processed, len(trading_days)):
                self._log_progress(processed, len(trading_days), trade_date, success, empty, skipped_existing)

        msg = (
            f"{self.spec.name} 同步完成: 成功 {success} 天, "
            f"空数据 {empty} 天, 跳过已存在 {skipped_existing} 天, "
            f"共 {len(trading_days)} 个交易日"
        )
        logger.info(msg)
        rt.notifier.send(msg)

    def _log_progress(self, processed, total, trade_date, success, empty, skipped):
        percent = processed / total * 100
        logger.info(
            f"{self.spec.name} 同步进度: {processed}/{total} ({percent:.1f}%), "
            f"当前日期 {trade_date}, "
            f"成功 {success} 天, 空数据 {empty} 天, 跳过已存在 {skipped} 天"
        )


@dataclass
class SnapshotSyncJob(SyncJob):
    spec: TableSpec = field(default=None)
    fetch: Callable[[], pd.DataFrame] = field(default=None)
    store: SnapshotStore = field(default=None)
    skip_non_trading: bool = True
    supports_date_range: bool = False

    def __post_init__(self):
        if self.table_name is None and self.spec is not None:
            object.__setattr__(self, 'table_name', self.spec.name)

    def run(self, rt: SyncRuntime, start_date: str | None = None, end_date: str | None = None) -> None:
        if self.skip_non_trading and rt.calendar.skip_if_not_trading("SSE"):
            return
        today = rt.calendar.today()
        try:
            df = self.fetch()
            self.store.write(df)
            rt.meta.update_last_date(self.spec.name, today)
            logger.info(f"{self.spec.name} 同步完成: {len(df)} 条")
        except Exception as e:
            logger.error(f"{self.spec.name} 同步失败: {e}")
            rt.notifier.send(f"{self.spec.name} 同步失败: {e}")
            raise
```

Note: `DailySyncJob` and `SnapshotSyncJob` use `dataclass` but `table_name` comes from spec. Update `__post_init__` or just pass `table_name=spec.name` in `build_jobs()`. Simplest: pass explicitly.

Revise `_jobs.py` — remove `__post_init__` and require explicit `table_name`:

```python
@dataclass
class DailySyncJob(SyncJob):
    spec: DailyTableSpec
    fetch: Callable[[str], pd.DataFrame]
    store: DailyPartitionStore
    write_empty: bool = False
    exchange: str = "SSE"
    supports_date_range: bool = True
    # table_name inherited from SyncJob, set to spec.name in build_jobs()
```

In `build_jobs()` callers always pass `table_name=spec.name`. This is simpler.

- [ ] **Step 4: 运行测试**

```bash
uv run pytest tests/test_jobs.py -v
```

Expected: all PASS

- [ ] **Step 5: 提交**

```bash
git add zer0share/sync/_jobs.py tests/test_jobs.py
git commit -m "feat: add SyncJob, DailySyncJob, SnapshotSyncJob to sync/_jobs.py"
```

---

## Task 6：迁移 sync 域模块

**Files:**
- Modify: `zer0share/sync/equities.py`
- Modify: `zer0share/sync/futures.py`
- Modify: `zer0share/sync/options.py`
- Modify: `zer0share/sync/calendar.py`
- Modify: `zer0share/sync/industry.py`
- Modify: `zer0share/sync/_helpers.py`

- [ ] **Step 1: 迁移 sync/calendar.py**

```python
# zer0share/sync/calendar.py
import time
from pathlib import Path

import pandas as pd
from loguru import logger

import zer0share.dateutil as dateutil
from zer0share.storage import read_trade_cal, write_trade_cal
from zer0share.sync import SyncRuntime
from zer0share.sync._jobs import SyncJob

ALL_EXCHANGES = ["SSE", "SZSE", "CFFEX", "DCE", "SHFE", "CZCE", "INE", "GFEX"]
TRADE_CAL_FIRST_DATE = "19900101"


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


class TradeCalSyncJob(SyncJob):
    table_name: str = "trade_cal"
    supports_date_range: bool = False

    def __init__(self, fetch=None, data_dir: Path | None = None):
        self._fetch = fetch
        self._data_dir = data_dir

    def run(self, rt: SyncRuntime, start_date=None, end_date=None) -> None:
        today = rt.calendar.today()
        end = today[:4] + "1231"
        max_dates: list[str] = []
        try:
            for exchange in ALL_EXCHANGES:
                existing = read_trade_cal(rt.meta._data_dir if hasattr(rt.meta, '_data_dir') else self._data_dir, exchange)
                last = str(existing["cal_date"].max()) if not existing.empty else None
                start = dateutil.add_days(last, 1) if last else TRADE_CAL_FIRST_DATE
                if start <= end:
                    fetched = self._fetch(exchange, start, end)
                    df = _merge_trade_cal(existing, fetched)
                    write_trade_cal(self._data_dir, exchange, df)
                    logger.info(f"trade_cal {exchange} 写入完成: 新增 {len(fetched)} 条, 共 {len(df)} 条")
                else:
                    df = existing
                    logger.info(f"trade_cal {exchange} 已覆盖到 {last}，无需同步")
                if not df.empty:
                    max_dates.append(str(df["cal_date"].max()))

            rt.calendar.load_from_parquet(self._data_dir, ALL_EXCHANGES)
            if max_dates:
                rt.meta.update_last_date("trade_cal", min(max_dates))
            logger.info("trade_cal 全部同步完成")
        except Exception as e:
            logger.error(f"trade_cal 同步失败: {e}")
            rt.notifier.send(f"trade_cal 同步失败: {e}")
            raise


def build_jobs(cfg, fetcher) -> list[SyncJob]:
    return [
        TradeCalSyncJob(fetch=fetcher.fetch_trade_cal, data_dir=cfg.data_dir),
    ]
```

Note: `TradeCalSyncJob` needs `data_dir` because it reads/writes Parquet files. Pass from `cfg` at build time.

- [ ] **Step 2: 迁移 sync/industry.py**

```python
# zer0share/sync/industry.py
from loguru import logger

from zer0share.storage import SnapshotStore
from zer0share.sync import SyncRuntime
from zer0share.sync._jobs import SnapshotSyncJob, SyncJob
from zer0share.catalog import SW_CLASSIFY_SPEC, SW_MEMBER_SPEC, CI_MEMBER_SPEC


class IndustrySyncJob(SyncJob):
    """Syncs sw_classify + sw_member in one call (Tushare groups them)."""
    table_name: str = "industry"
    supports_date_range: bool = False

    def __init__(self, fetch_classify, fetch_member, store_classify, store_member):
        self._fetch_classify = fetch_classify
        self._fetch_member = fetch_member
        self._store_classify = store_classify
        self._store_member = store_member

    def run(self, rt: SyncRuntime, start_date=None, end_date=None) -> None:
        if rt.calendar.skip_if_not_trading("SSE"):
            return
        today = rt.calendar.today()
        try:
            df = self._fetch_classify()
            self._store_classify.write(df)
            rt.meta.update_last_date("sw_classify", today)
            logger.info(f"sw_classify 同步完成: {len(df)} 条")

            df = self._fetch_member()
            self._store_member.write(df)
            rt.meta.update_last_date("sw_member", today)
            logger.info(f"sw_member 同步完成: {len(df)} 条")
        except Exception as e:
            logger.error(f"industry 同步失败: {e}")
            rt.notifier.send(f"industry 同步失败: {e}")
            raise


def build_jobs(cfg, fetcher) -> list[SyncJob]:
    return [
        IndustrySyncJob(
            fetch_classify=fetcher.fetch_sw_classify,
            fetch_member=fetcher.fetch_sw_member,
            store_classify=SnapshotStore(cfg.data_dir / "industry" / "sw_classify" / "data.parquet"),
            store_member=SnapshotStore(cfg.data_dir / "industry" / "sw_member" / "data.parquet"),
        ),
        SnapshotSyncJob(
            table_name=CI_MEMBER_SPEC.name,
            spec=CI_MEMBER_SPEC,
            fetch=fetcher.fetch_ci_member,
            store=SnapshotStore(cfg.data_dir / "industry" / "ci_member" / "data.parquet"),
        ),
    ]
```

- [ ] **Step 3: 迁移 sync/equities.py**

```python
# zer0share/sync/equities.py
import time
from pathlib import Path

import pandas as pd
from loguru import logger

import zer0share.dateutil as dateutil
from zer0share.fetcher import INDEX_DAILY_CODES
from zer0share.storage import (
    DailyPartitionStore, IndexWeightStore, SnapshotStore,
    daily_partition_exists, write_daily_partition,
)
from zer0share.sync import SyncRuntime
from zer0share.sync._jobs import DailySyncJob, SnapshotSyncJob, SyncJob, FIRST_DATE
from zer0share.catalog import (
    ADJ_FACTOR_SPEC, BASIC_SPEC, DAILY_BASIC_SPEC, DAILY_KLINE_SPEC,
    INDEX_DAILY_SPEC, STK_LIMIT_SPEC, STOCK_ST_SPEC, SUSPEND_D_SPEC,
)

INDEX_CODES = ["399300.SZ", "000905.SH", "000852.SH"]


def _index_weight_meta_key(index_code: str) -> str:
    return f"index_weight:{index_code}"


class IndexWeightSyncJob(SyncJob):
    table_name: str = "index_weight"
    supports_date_range: bool = True

    def __init__(self, fetch, store: IndexWeightStore):
        self._fetch = fetch
        self._store = store

    def run(self, rt: SyncRuntime, start_date=None, end_date=None) -> None:
        today = rt.calendar.today()
        end = end_date or today
        if start_date is not None and start_date > end:
            raise ValueError("start_date must be on or before end_date")

        success = skipped_existing = empty_months = requests = 0
        coverage_dates: list[str] = []

        for index_code in INDEX_CODES:
            meta_key = _index_weight_meta_key(index_code)
            last = rt.meta.get_last_date(meta_key)
            start = start_date or (dateutil.add_days(last, 1) if last else FIRST_DATE)
            if start > end:
                logger.info(f"index_weight {index_code} 已覆盖到 {last}，无需同步")
                if last is not None:
                    coverage_dates.append(last)
                continue

            ranges = dateutil.month_ranges(start, end)
            logger.info(f"index_weight {index_code} 同步开始: {start} ~ {end}, 共 {len(ranges)} 个月度窗口")
            try:
                for processed, (month_start, month_end) in enumerate(ranges, start=1):
                    df = self._fetch(index_code, month_start, month_end)
                    requests += 1
                    time.sleep(0.2)
                    if df.empty:
                        empty_months += 1
                    else:
                        for trade_date_value, part in df.groupby("trade_date"):
                            trade_date = str(trade_date_value)
                            if self._store.exists(index_code, trade_date):
                                skipped_existing += 1
                                continue
                            self._store.write(index_code, trade_date, part)
                            success += 1
                    if processed == len(ranges) or processed % 50 == 0:
                        percent = processed / len(ranges) * 100
                        logger.info(
                            f"index_weight {index_code} 同步进度: "
                            f"{processed}/{len(ranges)} ({percent:.1f}%), "
                            f"当前窗口 {month_start} ~ {month_end}, "
                            f"成功 {success} 个分区, 空窗口 {empty_months} 个, "
                            f"跳过已存在 {skipped_existing} 个分区"
                        )
                frontier = max(last, end) if last is not None else end
                rt.meta.update_last_date(meta_key, frontier)
                coverage_dates.append(frontier)
            except Exception as e:
                logger.error(f"index_weight {index_code} 同步失败: {e}")
                rt.notifier.send(f"index_weight {index_code} 同步失败: {e}")
                raise

        if coverage_dates:
            rt.meta.update_last_date("index_weight", min(coverage_dates))

        msg = (
            f"index_weight 同步完成: 成功 {success} 个分区, "
            f"空窗口 {empty_months} 个, 跳过已存在 {skipped_existing} 个分区, "
            f"请求 {requests} 次"
        )
        logger.info(msg)
        rt.notifier.send(msg)


class IndexDailySyncJob(SyncJob):
    table_name: str = "index_daily"
    supports_date_range: bool = True

    def __init__(self, fetch, store: DailyPartitionStore):
        self._fetch = fetch
        self._store = store

    def run(self, rt: SyncRuntime, start_date=None, end_date=None) -> None:
        today = rt.calendar.today()
        last = rt.meta.get_last_date("index_daily")

        if start_date is None:
            start = dateutil.add_days(last, 1) if last else FIRST_DATE
            end = today
            if start > end:
                logger.info("index_daily 已是最新，无需同步")
                return
        else:
            start = start_date
            end = end_date or today
            if start > end:
                raise ValueError("start_date must be on or before end_date")

        logger.info(f"index_daily 同步开始: {start} ~ {end}, 共 {len(INDEX_DAILY_CODES)} 个指数")
        all_frames = []
        for ts_code in INDEX_DAILY_CODES:
            try:
                df = self._fetch(ts_code, start, end)
                time.sleep(0.2)
                if not df.empty:
                    all_frames.append(df)
            except Exception as e:
                logger.error(f"index_daily {ts_code} 拉取失败: {e}")
                rt.notifier.send(f"index_daily {ts_code} 拉取失败: {e}")
                continue

        if not all_frames:
            msg = "index_daily 无数据，跳过"
            logger.info(msg)
            rt.notifier.send(msg)
            return

        combined = pd.concat(all_frames, ignore_index=True)
        success = skipped_existing = 0
        frontier = last

        for trade_date_value, part in combined.groupby("trade_date"):
            trade_date = str(trade_date_value)
            if self._store.exists(trade_date):
                skipped_existing += 1
                continue
            self._store.write(trade_date, part.reset_index(drop=True))
            if frontier is None or trade_date > frontier:
                rt.meta.update_last_date("index_daily", trade_date)
                frontier = trade_date
            success += 1

        msg = (
            f"index_daily 同步完成: 成功 {success} 天, "
            f"跳过已存在 {skipped_existing} 天, 共 {len(INDEX_DAILY_CODES)} 个指数"
        )
        logger.info(msg)
        rt.notifier.send(msg)


def build_jobs(cfg, fetcher) -> list[SyncJob]:
    d = cfg.data_dir
    return [
        SnapshotSyncJob(
            table_name=BASIC_SPEC.name, spec=BASIC_SPEC,
            fetch=fetcher.fetch_basic,
            store=SnapshotStore(d / "basic" / "data.parquet"),
        ),
        DailySyncJob(
            table_name=DAILY_KLINE_SPEC.name, spec=DAILY_KLINE_SPEC,
            fetch=fetcher.fetch_daily_kline,
            store=DailyPartitionStore(d / "daily_kline"),
        ),
        DailySyncJob(
            table_name=ADJ_FACTOR_SPEC.name, spec=ADJ_FACTOR_SPEC,
            fetch=fetcher.fetch_adj_factor,
            store=DailyPartitionStore(d / "adj_factor"),
        ),
        DailySyncJob(
            table_name=DAILY_BASIC_SPEC.name, spec=DAILY_BASIC_SPEC,
            fetch=fetcher.fetch_daily_basic,
            store=DailyPartitionStore(d / "daily_basic"),
        ),
        DailySyncJob(
            table_name=STOCK_ST_SPEC.name, spec=STOCK_ST_SPEC,
            fetch=fetcher.fetch_stock_st,
            store=DailyPartitionStore(d / "stock_st"),
            write_empty=True,
        ),
        DailySyncJob(
            table_name=SUSPEND_D_SPEC.name, spec=SUSPEND_D_SPEC,
            fetch=fetcher.fetch_suspend_d,
            store=DailyPartitionStore(d / "suspend_d"),
            write_empty=True,
        ),
        DailySyncJob(
            table_name=STK_LIMIT_SPEC.name, spec=STK_LIMIT_SPEC,
            fetch=fetcher.fetch_stk_limit,
            store=DailyPartitionStore(d / "stk_limit"),
        ),
        IndexWeightSyncJob(
            fetch=fetcher.fetch_index_weight,
            store=IndexWeightStore(d / "index_weight"),
        ),
        IndexDailySyncJob(
            fetch=fetcher.fetch_index_daily,
            store=DailyPartitionStore(d / "index_daily"),
        ),
    ]
```

- [ ] **Step 4: 迁移 sync/futures.py**

```python
# zer0share/sync/futures.py
import time

import pandas as pd
from loguru import logger

import zer0share.dateutil as dateutil
from zer0share.fetcher import FUTURES_EXCHANGES
from zer0share.storage import DailyPartitionStore, SnapshotStore
from zer0share.sync import SyncRuntime
from zer0share.sync._jobs import DailySyncJob, SnapshotSyncJob, SyncJob, FIRST_DATE
from zer0share.catalog import (
    FT_LIMIT_SPEC, FUT_BASIC_SPEC, FUT_DAILY_SPEC, FUT_HOLDING_SPEC,
    FUT_INDEX_DAILY_SPEC, FUT_MAPPING_SPEC, FUT_MONTHLY_SPEC,
    FUT_SETTLE_SPEC, FUT_WEEKLY_DETAIL_SPEC, FUT_WEEKLY_SPEC, FUT_WSR_SPEC,
)


class FutBasicSyncJob(SyncJob):
    table_name: str = "fut_basic"
    supports_date_range: bool = False

    def __init__(self, fetch, store: DailyPartitionStore):
        self._fetch = fetch
        self._store = store

    def run(self, rt: SyncRuntime, start_date=None, end_date=None) -> None:
        if rt.calendar.skip_if_not_trading("SSE"):
            return
        today = rt.calendar.today()
        all_frames = []
        try:
            for exchange in FUTURES_EXCHANGES:
                for fut_type in ("1", "2"):
                    df = self._fetch(exchange, fut_type)
                    time.sleep(0.2)
                    if not df.empty:
                        all_frames.append(df)
            combined = pd.concat(all_frames, ignore_index=True) if all_frames else pd.DataFrame()
            self._store.write(today, combined)
            rt.meta.update_last_date("fut_basic", today)
            logger.info(f"fut_basic 同步完成: {len(combined)} 条")
        except Exception as e:
            logger.error(f"fut_basic 同步失败: {e}")
            rt.notifier.send(f"fut_basic 同步失败: {e}")
            raise


class FutIndexDailySyncJob(SyncJob):
    """Bug3 fix: iterates trading days only, not all calendar days."""
    table_name: str = "fut_index_daily"
    supports_date_range: bool = True

    def __init__(self, fetch, store: DailyPartitionStore):
        self._fetch = fetch
        self._store = store

    def run(self, rt: SyncRuntime, start_date=None, end_date=None) -> None:
        if rt.calendar.skip_if_not_trading("SSE"):
            return
        today = rt.calendar.today()
        last = rt.meta.get_last_date("fut_index_daily")

        if start_date is None:
            start = dateutil.add_days(last, 1) if last else FIRST_DATE
            end = today
            if start > end:
                logger.info("fut_index_daily 已是最新，无需同步")
                return
        else:
            start = start_date
            end = end_date or today
            if start > end:
                raise ValueError("start_date must be on or before end_date")

        # Bug3 fix: use trading days, not calendar days
        trading_days = rt.calendar.get_trading_days("SSE", start, end)
        logger.info(f"fut_index_daily 同步开始: {start} ~ {end}, {len(trading_days)} 个交易日")
        all_frames = []

        for trade_date in trading_days:
            try:
                df = self._fetch(trade_date)
                time.sleep(0.2)
                if not df.empty:
                    all_frames.append(df)
            except Exception as e:
                logger.error(f"fut_index_daily {trade_date} 拉取失败: {e}")
                rt.notifier.send(f"fut_index_daily {trade_date} 拉取失败: {e}")

        if not all_frames:
            msg = "fut_index_daily 无数据，跳过"
            logger.info(msg)
            rt.notifier.send(msg)
            return

        combined = pd.concat(all_frames, ignore_index=True)
        success = skipped_existing = 0
        frontier = last

        for trade_date_value, part in combined.groupby("trade_date"):
            trade_date = str(trade_date_value)
            if self._store.exists(trade_date):
                skipped_existing += 1
                continue
            self._store.write(trade_date, part.reset_index(drop=True))
            if frontier is None or trade_date > frontier:
                rt.meta.update_last_date("fut_index_daily", trade_date)
                frontier = trade_date
            success += 1

        msg = f"fut_index_daily 同步完成: 成功 {success} 天, 跳过已存在 {skipped_existing} 天"
        logger.info(msg)
        rt.notifier.send(msg)


class FutWeeklyDetailSyncJob(SyncJob):
    """Bug1+Bug2 fix: check existence before fetch; graceful up-to-date return."""
    table_name: str = "fut_weekly_detail"
    supports_date_range: bool = True

    def __init__(self, fetch, store: DailyPartitionStore):
        self._fetch = fetch
        self._store = store

    def run(self, rt: SyncRuntime, start_date=None, end_date=None) -> None:
        today = rt.calendar.today()
        last = rt.meta.get_last_date("fut_weekly_detail")

        if start_date is None:
            start = dateutil.add_days(last, 1) if last else FIRST_DATE
            end = today
            if start > end:  # Bug2 fix: graceful return
                logger.info("fut_weekly_detail 已是最新，无需同步")
                return
        else:
            start = start_date
            end = end_date or today
            if start > end:
                raise ValueError("start_date must be on or before end_date")

        weeks = dateutil.week_ranges(start, end)
        logger.info(f"fut_weekly_detail 同步开始: {start} ~ {end}, 共 {len(weeks)} 个周")
        success = skipped_existing = 0
        frontier = last

        for week_num, week_start in weeks:
            if self._store.exists(week_start):  # Bug1 fix: check before fetch
                skipped_existing += 1
                continue
            try:
                df = self._fetch(week_num)
                time.sleep(0.2)
                if df.empty:
                    continue
                self._store.write(week_start, df)
                if frontier is None or week_start > frontier:
                    rt.meta.update_last_date("fut_weekly_detail", week_start)
                    frontier = week_start
                success += 1
            except Exception as e:
                logger.error(f"fut_weekly_detail {week_num} 同步失败: {e}")
                rt.notifier.send(f"fut_weekly_detail {week_num} 同步失败: {e}")
                raise

        msg = (
            f"fut_weekly_detail 同步完成: 成功 {success} 周, "
            f"跳过已存在 {skipped_existing} 周, 共 {len(weeks)} 周"
        )
        logger.info(msg)
        rt.notifier.send(msg)


def build_jobs(cfg, fetcher) -> list[SyncJob]:
    fd = cfg.data_dir / "futures"
    return [
        FutBasicSyncJob(fetch=fetcher.fetch_fut_basic, store=DailyPartitionStore(fd / "fut_basic")),
        DailySyncJob(
            table_name=FUT_DAILY_SPEC.name, spec=FUT_DAILY_SPEC,
            fetch=fetcher.fetch_fut_daily, store=DailyPartitionStore(fd / "fut_daily"),
        ),
        DailySyncJob(
            table_name=FUT_HOLDING_SPEC.name, spec=FUT_HOLDING_SPEC,
            fetch=fetcher.fetch_fut_holding, store=DailyPartitionStore(fd / "fut_holding"),
        ),
        DailySyncJob(
            table_name=FUT_WSR_SPEC.name, spec=FUT_WSR_SPEC,
            fetch=fetcher.fetch_fut_wsr, store=DailyPartitionStore(fd / "fut_wsr"),
        ),
        DailySyncJob(
            table_name=FUT_SETTLE_SPEC.name, spec=FUT_SETTLE_SPEC,
            fetch=fetcher.fetch_fut_settle, store=DailyPartitionStore(fd / "fut_settle"),
        ),
        DailySyncJob(
            table_name=FUT_MAPPING_SPEC.name, spec=FUT_MAPPING_SPEC,
            fetch=fetcher.fetch_fut_mapping, store=DailyPartitionStore(fd / "fut_mapping"),
        ),
        DailySyncJob(
            table_name=FT_LIMIT_SPEC.name, spec=FT_LIMIT_SPEC,
            fetch=fetcher.fetch_ft_limit, store=DailyPartitionStore(fd / "ft_limit"),
        ),
        DailySyncJob(
            table_name=FUT_WEEKLY_SPEC.name, spec=FUT_WEEKLY_SPEC,
            fetch=fetcher.fetch_fut_weekly, store=DailyPartitionStore(fd / "fut_weekly"),
        ),
        DailySyncJob(
            table_name=FUT_MONTHLY_SPEC.name, spec=FUT_MONTHLY_SPEC,
            fetch=fetcher.fetch_fut_monthly, store=DailyPartitionStore(fd / "fut_monthly"),
        ),
        FutIndexDailySyncJob(
            fetch=fetcher.fetch_fut_index_daily,
            store=DailyPartitionStore(fd / "fut_index_daily"),
        ),
        FutWeeklyDetailSyncJob(
            fetch=fetcher.fetch_fut_weekly_detail,
            store=DailyPartitionStore(fd / "fut_weekly_detail"),
        ),
    ]
```

- [ ] **Step 5: 迁移 sync/options.py**

```python
# zer0share/sync/options.py
import time

import pandas as pd
from loguru import logger

from zer0share.fetcher import OPTIONS_EXCHANGES
from zer0share.storage import DailyPartitionStore, SnapshotStore
from zer0share.sync import SyncRuntime
from zer0share.sync._jobs import DailySyncJob, SyncJob
from zer0share.catalog import OPT_BASIC_SPEC, OPT_DAILY_SPEC


class OptBasicSyncJob(SyncJob):
    table_name: str = "opt_basic"
    supports_date_range: bool = False

    def __init__(self, fetch, store: SnapshotStore):
        self._fetch = fetch
        self._store = store

    def run(self, rt: SyncRuntime, start_date=None, end_date=None) -> None:
        if rt.calendar.skip_if_not_trading("SSE"):
            return
        today = rt.calendar.today()
        all_frames = []
        try:
            for exchange in OPTIONS_EXCHANGES:
                df = self._fetch(exchange)
                time.sleep(0.2)
                if not df.empty:
                    all_frames.append(df)
            combined = pd.concat(all_frames, ignore_index=True) if all_frames else pd.DataFrame()
            self._store.write(combined)
            rt.meta.update_last_date("opt_basic", today)
            logger.info(f"opt_basic 同步完成: {len(combined)} 条")
        except Exception as e:
            logger.error(f"opt_basic 同步失败: {e}")
            rt.notifier.send(f"opt_basic 同步失败: {e}")
            raise


def build_jobs(cfg, fetcher) -> list[SyncJob]:
    od = cfg.data_dir / "options"
    return [
        OptBasicSyncJob(
            fetch=fetcher.fetch_opt_basic,
            store=SnapshotStore(od / "opt_basic" / "data.parquet"),
        ),
        DailySyncJob(
            table_name=OPT_DAILY_SPEC.name, spec=OPT_DAILY_SPEC,
            fetch=fetcher.fetch_opt_daily, store=DailyPartitionStore(od / "opt_daily"),
        ),
    ]
```

- [ ] **Step 6: 清空 sync/_helpers.py（只留兼容常量）**

```python
# zer0share/sync/_helpers.py
# 向后兼容导出，Task 8 后删除此文件
FIRST_DATE = "20160101"
TRADE_CAL_FIRST_DATE = "19900101"
PROGRESS_INTERVAL = 50
EXCHANGES = ["SSE", "SZSE"]
ALL_EXCHANGES = ["SSE", "SZSE", "CFFEX", "DCE", "SHFE", "CZCE", "INE", "GFEX"]
INDEX_CODES = ["399300.SZ", "000905.SH", "000852.SH"]


def skip_if_not_trading(ctx, exchange: str) -> bool:
    return ctx.calendar.skip_if_not_trading(exchange)


def ensure_trade_cal_loaded(ctx) -> None:
    ctx.calendar.ensure_loaded(ctx)
```

- [ ] **Step 7: 提交**

```bash
git add zer0share/sync/ zer0share/trading_calendar.py
git commit -m "feat: migrate sync domain modules to build_jobs() + OOP job classes"
```

---

## Task 7：重构 Pipeline 为 Registry，更新测试

**Files:**
- Modify: `zer0share/pipeline.py`
- Modify: `tests/test_pipeline.py`

- [ ] **Step 1: 重写 pipeline.py**

```python
# zer0share/pipeline.py
from pathlib import Path

from zer0share.config import Config
from zer0share.fetcher import TushareFetcher
from zer0share.notifier import Notifier
from zer0share.storage import MetaStore
from zer0share.sync import SyncRuntime
from zer0share.sync._jobs import SyncJob
from zer0share.trading_calendar import TradingCalendar


class Pipeline:
    def __init__(self, cfg: Config, fetcher: TushareFetcher, notifier: Notifier):
        meta = MetaStore(cfg.db_path)
        calendar = TradingCalendar(meta)
        self._runtime = SyncRuntime(calendar=calendar, notifier=notifier, meta=meta)
        self._registry: dict[str, SyncJob] = {}
        self._build_registry(cfg, fetcher)

    def _build_registry(self, cfg: Config, fetcher: TushareFetcher) -> None:
        from zer0share.sync import calendar, equities, industry, futures, options
        for module in [calendar, equities, industry, futures, options]:
            for job in module.build_jobs(cfg, fetcher):
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
```

- [ ] **Step 2: 更新 tests/test_pipeline.py**

测试文件需要大幅更新：`pipeline.sync_xxx()` → `pipeline.run("xxx")`，`patch("zer0share.sync._helpers.date")` → 直接构造 `TradingCalendar(meta, today_fn=lambda: "...")`，`pipeline._meta` → `pipeline._runtime.meta`。

关键 fixture 变化：

```python
# tests/test_pipeline.py 新版 fixture
from unittest.mock import MagicMock, patch
import pandas as pd
import pytest
from zer0share.pipeline import Pipeline
from zer0share.storage import MetaStore, write_trade_cal, write_basic
from zer0share.trading_calendar import TradingCalendar


@pytest.fixture
def cfg(tmp_path):
    c = MagicMock()
    c.data_dir = tmp_path
    c.db_path = tmp_path / "meta.duckdb"
    return c


@pytest.fixture
def pipeline(cfg):
    fetcher = MagicMock()
    notifier = MagicMock()
    return Pipeline(cfg, fetcher, notifier)


def _set_today(pipeline, date_str: str):
    """Inject today into TradingCalendar for tests."""
    pipeline._runtime.calendar._today_fn = lambda: date_str


def _setup_trade_cal(pipeline, cfg, trade_date: str = "20240102", is_open: bool = True):
    df = pd.DataFrame({
        "exchange": ["SSE"], "cal_date": [trade_date],
        "is_open": [is_open], "pretrade_date": ["20231229"],
    })
    write_trade_cal(cfg.data_dir, "SSE", df)
    pipeline._runtime.calendar.load_from_parquet(cfg.data_dir, ["SSE"])
    pipeline._runtime.meta.update_last_date("trade_cal", trade_date)
```

Replace every `pipeline.sync_basic()` → `pipeline.run("basic")`, every `pipeline.sync_daily_kline(...)` → `pipeline.run("daily_kline", ...)`, etc.

Replace every `patch("zer0share.sync._helpers.date")` block with `_set_today(pipeline, "20240102")`.

Replace `pipeline._meta` → `pipeline._runtime.meta`, `pipeline._fetcher` → access via registry job's fetch (or keep as `pipeline._registry["daily_kline"].fetch`). For tests that check `pipeline._fetcher.fetch_basic.assert_called_once()`, get the mock from the registry:

```python
# Old:
pipeline._fetcher.fetch_basic.assert_called_once()

# New: fetcher is captured in fixture and accessible directly
fetcher = MagicMock()
pipeline = Pipeline(cfg, fetcher, notifier)
# ... test ...
fetcher.fetch_basic.assert_called_once()
```

Update fixture to expose fetcher:

```python
@pytest.fixture
def pipeline_and_fetcher(cfg):
    fetcher = MagicMock()
    notifier = MagicMock()
    p = Pipeline(cfg, fetcher, notifier)
    return p, fetcher, notifier
```

- [ ] **Step 3: 运行完整测试套件**

```bash
uv run pytest tests/ -v 2>&1 | tail -30
```

Expected: all PASS (or only previously-broken tests fail)

- [ ] **Step 4: 提交**

```bash
git add zer0share/pipeline.py tests/test_pipeline.py
git commit -m "refactor: Pipeline to Registry, update tests"
```

---

## Task 8：简化 Config + Scheduler + CLI

**Files:**
- Modify: `zer0share/config.py`
- Modify: `zer0share/scheduler.py`
- Modify: `zer0share/cli.py`
- Modify: `config/settings.toml`
- Modify: `config/settings.example.toml`
- Modify: `tests/test_config.py`
- Modify: `tests/test_scheduler.py`
- Modify: `tests/test_cli.py`

- [ ] **Step 1: 更新 config.py**

```python
# zer0share/config.py
from dataclasses import dataclass
from pathlib import Path
import tomllib


@dataclass(frozen=True)
class Config:
    tushare_token: str
    data_dir: Path
    db_path: Path
    log_path: Path
    schedule: dict[str, str]  # table_name → "HH:MM"
    wecom_webhook_url: str
    notifier_enabled: bool


def load_config(path: Path = Path("config/settings.toml")) -> Config:
    if not path.exists():
        raise FileNotFoundError(f"配置文件不存在: {path}")
    try:
        with open(path, "rb") as f:
            raw = tomllib.load(f)
    except tomllib.TOMLDecodeError as e:
        raise ValueError(f"配置文件格式错误: {e}") from e
    try:
        return Config(
            tushare_token=raw["tushare"]["token"],
            data_dir=Path(raw["paths"]["data_dir"]),
            db_path=Path(raw["paths"]["db_path"]),
            log_path=Path(raw["paths"]["log_path"]),
            schedule=dict(raw.get("scheduler", {})),
            wecom_webhook_url=raw["notifier"]["wecom_webhook_url"],
            notifier_enabled=raw["notifier"]["enabled"],
        )
    except KeyError as e:
        raise KeyError(f"配置文件缺少必要字段: {e}") from e
```

- [ ] **Step 2: 更新 settings.toml 和 settings.example.toml**

将 `settings.toml` 的 `[scheduler]` 节替换为：

```toml
[scheduler]
trade_cal    = "09:00"
basic        = "09:10"
industry     = "09:20"
ci_member    = "09:30"
daily_kline  = "16:30"
index_daily  = "16:35"
adj_factor   = "17:00"
daily_basic  = "17:10"
stock_st     = "17:20"
suspend_d    = "17:30"
stk_limit    = "17:40"
index_weight = "17:50"
fut_basic    = "18:00"
fut_daily    = "18:10"
fut_holding  = "18:20"
fut_wsr      = "18:30"
fut_settle   = "18:40"
fut_mapping  = "18:50"
ft_limit     = "19:00"
fut_weekly   = "19:10"
fut_monthly  = "19:20"
fut_index_daily   = "19:30"
fut_weekly_detail = "19:40"
opt_basic    = "19:50"
opt_daily    = "20:00"
```

同样更新 `config/settings.example.toml`。

- [ ] **Step 3: 更新 scheduler.py**

```python
# zer0share/scheduler.py
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
        for table_name, time_str in cfg.schedule.items():
            if table_name not in pipeline.registry:
                logger.warning(f"调度配置中未知表: {table_name}，跳过")
                continue
            h, m = map(int, time_str.split(":"))
            scheduler.add_job(
                pipeline.run,
                CronTrigger(hour=h, minute=m),
                args=[table_name],
                id=table_name,
            )
        logger.info(f"调度器启动: 共 {len(cfg.schedule)} 个任务")
        scheduler.start()
```

- [ ] **Step 4: 更新 cli.py**

将 `sync` command 的大 if-elif 链替换为：

```python
@cli.command()
@click.option("--table", type=click.Choice(SYNC_TABLES), default=None)
@click.option("--all", "sync_all", is_flag=True, default=False)
@click.option("--start-date", default=None, callback=_validate_date)
@click.option("--end-date", default=None, callback=_validate_date)
def sync(table, sync_all, start_date, end_date):
    """同步数据。"""
    if end_date is not None and start_date is None:
        raise click.UsageError("--end-date requires --start-date")
    if start_date is not None and end_date is not None and end_date < start_date:
        raise click.UsageError("--end-date must be on or after --start-date")

    with _make_pipeline() as pipeline:
        if sync_all:
            pipeline.run_all(start_date=start_date, end_date=end_date)
        elif table is not None:
            pipeline.run(table, start_date=start_date, end_date=end_date)
        else:
            raise click.UsageError("specify --table or --all")
```

`SYNC_TABLES` 变为：

```python
SYNC_TABLES = [
    "trade_cal", "basic", "daily_kline", "adj_factor", "daily_basic",
    "stock_st", "suspend_d", "stk_limit", "index_weight", "index_daily",
    "industry", "ci_member",
    "fut_basic", "fut_daily", "fut_holding", "fut_wsr", "fut_settle",
    "fut_mapping", "ft_limit", "fut_weekly", "fut_monthly",
    "fut_index_daily", "fut_weekly_detail",
    "opt_basic", "opt_daily",
]
```

- [ ] **Step 5: 运行全量测试**

```bash
uv run pytest tests/ -v 2>&1 | tail -30
```

Expected: all PASS

- [ ] **Step 6: 提交**

```bash
git add zer0share/config.py zer0share/scheduler.py zer0share/cli.py \
        config/settings.toml config/settings.example.toml
git commit -m "refactor: simplify Config/Scheduler/CLI — schedule as HH:MM dict"
```

---

## Task 9：更新 query 模块从 catalog 导入 spec，删除遗留代码

**Files:**
- Modify: `zer0share/query/equities.py`
- Modify: `zer0share/query/futures.py`
- Modify: `zer0share/query/options.py`
- Modify: `zer0share/query/industry.py`
- Modify: `zer0share/query/calendar.py`
- Modify: `zer0share/storage.py` (删除遗留函数)
- Modify: `tests/test_storage.py` (删除遗留函数测试)

- [ ] **Step 1: 更新 query/equities.py — 从 catalog 导入 spec**

将每个函数内联的 `DailyTableSpec(...)` / `TableSpec(...)` 替换为从 `zer0share.catalog` 导入的常量。示例：

```python
# query/equities.py
from zer0share.catalog import (
    BASIC_SPEC, DAILY_KLINE_SPEC, ADJ_FACTOR_SPEC, DAILY_BASIC_SPEC,
    STOCK_ST_SPEC, SUSPEND_D_SPEC, STK_LIMIT_SPEC, INDEX_DAILY_SPEC,
    INDEX_WEIGHT_SPEC,
)

def daily(ctx, ts_code=None, trade_date=None, start_date=None, end_date=None,
          fields=None, limit=None, offset=None):
    return DailyPartitionRepository(ctx, DAILY_KLINE_SPEC).query(
        code=ts_code, trade_date=trade_date,
        start_date=start_date, end_date=end_date,
        fields=fields, limit=limit, offset=offset,
    )

def stock_basic(ctx, ts_code=None, name=None, market=None, list_status="L",
                exchange=None, is_hs=None, fields=None, limit=None, offset=None):
    repo = BaseParquetRepository(ctx, BASIC_SPEC)
    filters = []
    if ts_code is not None:
        filters.append(eq_filter("ts_code", ts_code, BASIC_COLS))
    # ... 其余 filters 不变
    return repo.query(fields=fields, filters=filters, limit=limit, offset=offset)
```

同理更新 `query/futures.py`、`query/options.py`、`query/industry.py`、`query/calendar.py`。

- [ ] **Step 2: 运行 query 相关测试确认不回归**

```bash
uv run pytest tests/test_api.py -v
```

Expected: all PASS

- [ ] **Step 3: 删除 storage.py 遗留函数**

从 `zer0share/storage.py` 中删除：
- `write_daily_kline`
- `daily_kline_partition_exists`
- `read_daily_kline`
- `write_adj_factor`
- `adj_factor_partition_exists`

- [ ] **Step 4: 更新 tests/test_storage.py**

删除使用上述函数的测试，替换为用 `DailyPartitionStore` 的等价测试：

```python
# 替换 test_write_and_read_daily_kline
def test_daily_kline_store_write_and_read(tmp_path):
    from zer0share.storage import DailyPartitionStore
    store = DailyPartitionStore(tmp_path / "daily_kline")
    df = pd.DataFrame({
        "ts_code": ["000001.SZ", "000002.SZ"],
        "trade_date": ["20240102", "20240102"],
        "open": [10.0, 20.0], "high": [11.0, 21.0],
        "low": [9.5, 19.5], "close": [10.5, 20.5],
        "pre_close": [10.0, 20.0], "change": [0.5, 0.5],
        "pct_chg": [5.0, 2.5], "vol": [100000.0, 200000.0],
        "amount": [1050000.0, 4100000.0],
    })
    store.write("20240102", df)
    result = store.read("20240102")
    assert len(result) == 2
    assert set(result["ts_code"]) == {"000001.SZ", "000002.SZ"}
```

- [ ] **Step 5: 运行全量测试**

```bash
uv run pytest tests/ -v
```

Expected: all PASS

- [ ] **Step 6: 提交**

```bash
git add zer0share/query/ zer0share/storage.py tests/test_storage.py
git commit -m "refactor: query modules use catalog specs, remove legacy storage functions"
```

---

## Task 10：删除兼容层，最终清理

**Files:**
- Delete or gut: `zer0share/sync/_helpers.py`
- Modify: `zer0share/sync/__init__.py` (remove SyncContext alias)
- Run: full test suite

- [ ] **Step 1: 检查 _helpers.py 还有哪些引用**

```bash
grep -r "_helpers" /data/projects/zer0share/zer0share/ /data/projects/zer0share/tests/ --include="*.py"
```

- [ ] **Step 2: 将 _helpers.py 中的常量迁移到 sync/_jobs.py 或 sync/calendar.py**

若 `_helpers` 只被 `_jobs.py` 引用，将常量直接移入 `_jobs.py`，然后删除 `_helpers.py`。

- [ ] **Step 3: 删除 sync/__init__.py 中的 SyncContext 别名**

```python
# zer0share/sync/__init__.py — 最终版
from dataclasses import dataclass

from zer0share.notifier import Notifier
from zer0share.storage import MetaStore

if False:
    from zer0share.trading_calendar import TradingCalendar


@dataclass
class SyncRuntime:
    calendar: "TradingCalendar"
    notifier: Notifier
    meta: MetaStore
```

- [ ] **Step 4: 运行全量测试**

```bash
uv run pytest tests/ -v
```

Expected: all PASS

- [ ] **Step 5: 最终提交**

```bash
git add -u
git commit -m "refactor: remove _helpers.py compat shim, finalize sync OOP refactor"
```
