# Tushare 日期字符串全栈统一设计

## 背景

Tushare API 所有日期参数和返回字段均使用 `YYYYMMDD` 字符串。项目历史上在多个层级做了 `str ↔ date` 转换：fetcher 接收 `date` 对象并在内部 `.strftime()`，sync 循环变量为 `date`，MetaStore 公开 API 返回 `date`，storage 函数接收 `date` 并在路径拼接时 `.strftime()`。这导致类型不一致、`parse_tushare_date` 之类的补丁函数散落各处，且查询过滤时容易出现字符串与 `date` 类型不匹配的问题。

`opt_basic` 已先行验证：同步和查询全程保持 `YYYYMMDD` 字符串可以正常工作。

## 设计规则

> **`YYYYMMDD` 字符串是整个代码库唯一合法的日期表示。`date` 对象只允许存在于两个地方：`zer0share/dateutil.py` 内部函数，以及 `MetaStore` SQL 查询边界内部。**

验证命令：

```bash
grep -rn "from datetime import date" zer0share/
# 只应出现在 dateutil.py 和 storage.py
```

## 数据流

```
CLI
  --start-date "20240102"  (YYYYMMDD 字符串，validate_date 回调校验格式)
        ↓ str
sync/* 函数签名: start_date: str | None, end_date: str | None
loop 变量 trade_date: str  (来自 MetaStore.get_trading_days())
        ↓ str
TushareFetcher  参数: trade_date: str
        ↓
Tushare API → DataFrame  日期列: str (YYYYMMDD)
        ↓ str
storage/* 函数  参数: trade_date: str
分区路径: f"date={trade_date}"  (无需 .strftime())
        ↓
MetaStore 公开 API
  get_last_date()           → str | None
  update_last_date(str)
  get_trading_days()        → list[str]
  is_trading_day(str)       → bool
        ↓ 内部转换
DuckDB  DATE 列保持不变
```

## 各模块改动

### 新增：`zer0share/dateutil.py`

整个代码库唯一可以 `from datetime import date` 的工具模块（`storage.py` 除外）。

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
    # 内部使用 date 运算，返回 (str, str) 元组列表
    ...

def week_ranges(start: str, end: str) -> list[tuple[str, str]]:
    # 内部使用 date / isocalendar，返回 (week_num: str, monday: str) 列表
    ...
```

### `zer0share/cli.py`

- 移除 `click.DateTime`，改用字符串 option + `validate_date` 回调
- `validate_date` 用 `datetime.strptime(value, "%Y%m%d")` 校验格式，通过则直接返回字符串
- 移除所有 `.date()` 调用，直接把字符串传给 sync 函数

```python
def validate_date(ctx, param, value):
    if value is None:
        return None
    try:
        datetime.strptime(value, "%Y%m%d")
        return value
    except ValueError:
        raise click.BadParameter("格式应为 YYYYMMDD，例如 20240102")

@click.option("--start-date", callback=validate_date, default=None)
@click.option("--end-date",   callback=validate_date, default=None)
```

### `zer0share/storage.py`（MetaStore）

MetaStore 公开 API 全换 `str`，DuckDB `DATE` 列不变，在方法内部做转换。

```python
def _parse(s: str) -> date:   # 模块私有
    return date(int(s[:4]), int(s[4:6]), int(s[6:]))

def get_last_date(self, table_name: str) -> str | None:
    row = ...
    return row[0].strftime("%Y%m%d") if row else None

def update_last_date(self, table_name: str, last_date: str):
    self._conn.execute(..., [table_name, _parse(last_date), ...])

def get_trading_days(self, exchange: str, start: str, end: str) -> list[str]:
    rows = self._conn.execute(..., [exchange, _parse(start), _parse(end)]).fetchall()
    return [row[0].strftime("%Y%m%d") for row in rows]

def is_trading_day(self, exchange: str, cal_date: str) -> bool:
    self._conn.execute(..., [exchange, _parse(cal_date)])
    ...
```

### `zer0share/storage.py`（storage 函数）

- 所有 `trade_date: date` 参数改为 `trade_date: str`
- 路径拼接：`f"date={trade_date.strftime('%Y%m%d')}"` → `f"date={trade_date}"`

### `zer0share/sync/_helpers.py`

- `FIRST_DATE = "20160101"`，`TRADE_CAL_FIRST_DATE = "19900101"`
- `date.today()` → `dateutil.today()`
- `last + timedelta(days=1)` → `dateutil.add_days(last, 1)`
- `start > end` / `start <= end`：字符串比较，天然正确
- `month_ranges`、`week_ranges` 签名改为 `str`
- 删除 `parse_tushare_date`
- `log_daily_progress` 和 `sync_daily_partitioned` 中 `trade_date: date` → `str`
- `sync_daily_partitioned` 里已有的 `trade_date.strftime("%Y%m%d")` 调用删除

### `zer0share/sync/equities.py` / `futures.py` / `calendar.py` / `industry.py`

- 所有 `sync_*` 函数签名：`start_date: str | None`，`end_date: str | None`
- `date.today()` → `dateutil.today()`
- `last + timedelta(days=1)` → `dateutil.add_days(last, 1)`
- `parse_tushare_date(...)` 调用全部删除（DataFrame 分组键已经是字符串）
- 已有的 `.strftime("%Y%m%d")` 调用全部删除

## 不变的部分

- DuckDB `sync_meta.last_date DATE` 和 `trade_cal.cal_date DATE` 列类型不变
- 分区目录格式 `date=YYYYMMDD` 不变（路径字符串内容不变，只是构建方式简化）
- `query/` 模块（已经是字符串接口）
- Parquet 中 Tushare 原始日期字段（已在 fetcher 重构中保持字符串）

## 验证标准

**静态检查**

```bash
# 只应出现在 dateutil.py 和 storage.py
grep -rn "from datetime import date" zer0share/

# sync 层不应有任何日期转换
grep -rn "\.strftime\|timedelta\|date\.today\|parse_tushare_date" zer0share/sync/
```

**单元测试**

- `dateutil.add_days("20161231", 1)` → `"20170101"`（跨年边界）
- `dateutil.add_days("20240229", 1)` → `"20240301"`（闰年边界）
- `dateutil.month_ranges("20240115", "20240301")` 返回三个 `(str, str)` 元组
- `MetaStore.get_last_date()` 返回 `str | None`，不返回 `date`
- `MetaStore.get_trading_days()` 返回 `list[str]`
- `daily_partition_exists` / `write_daily_partition` 传字符串后路径正确

**集成验证**

- CLI `--start-date 20240102` 正常传入；`--start-date 2024-01-02` 报错并提示格式
- `sync_daily_partitioned` 日志打印的日期为 `YYYYMMDD` 字符串
- 现有测试套件全部通过

## 风险与注意事项

- `skip_if_not_trading` 中的 `date.today()` 用 `dateutil.today()` 替换，`is_trading_day` 也改为接受字符串，两处需同步改动。
- `sync_fut_basic` 等用 `today` 做快照日期写分区的地方，`date.today()` 改成 `dateutil.today()`，同时 `write_daily_partition` 接受字符串，不受影响。
- 旧 Parquet 分区路径格式 `date=YYYYMMDD` 不变，存量数据无需迁移。
- `universe` 是内部生成表，依赖日期比较，不在本次改动范围内，需单独评估。
