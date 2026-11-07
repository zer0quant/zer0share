# 期权数据设计文档

**日期**: 2026-06-02  
**状态**: 已批准

## 背景

zer0share 已完成期货数据批次 1 和批次 2 的接入。本文档描述在同等模式下接入 Tushare Pro 期权数据的设计，涵盖 `opt_basic`（期权合约基础信息）和 `opt_daily`（期权日线行情）两张表。

所需积分：`opt_basic` ≥ 5000，`opt_daily` ≥ 2000。

## 数据源

| 接口 | 说明 | 积分要求 | 每次上限 |
|------|------|---------|---------|
| `opt_basic` | 期权合约基础信息 | ≥ 5000 | 无限制 |
| `opt_daily` | 期权日线行情 | ≥ 2000 | 15,000 条 |

支持交易所：SSE（上交所）、SZSE（深交所）、CFFEX（中金所）、DCE（大商所）、SHFE（上期所）、CZCE（郑商所）。

## 设计方案

选择**方案 A：完全镜像期货模式**。

- `opt_basic` 对应 `fut_basic`：全量拉取，按交易所迭代，每次覆盖写入
- `opt_daily` 对应 `fut_daily`：增量按交易日分区，复用 `_sync_daily_partitioned`

## 字段与常量（fetcher.py）

```python
OPTIONS_EXCHANGES = ["SSE", "SZSE", "CFFEX", "DCE", "SHFE", "CZCE"]
# 注：与 FUTURES_EXCHANGES 不同——期权有 SSE/SZSE，无 INE/GFEX

OPT_BASIC_COLS = [
    "ts_code", "symbol", "exchange", "name", "per_unit",
    "opt_code", "opt_type", "call_put", "exercise_type",
    "exercise_price", "s_month", "maturity_date",
    "list_date", "delist_date",
]

OPT_DAILY_COLS = [
    "ts_code", "trade_date", "exchange",
    "pre_settle", "pre_close",
    "open", "high", "low", "close", "settle",
    "vol", "amount", "oi",
]
```

### fetch 方法

**`fetch_opt_basic(exchange: str) -> pd.DataFrame`**
- 调用 `pro.opt_basic(exchange=exchange, fields=...)`
- 转换 `list_date`、`delist_date`、`maturity_date` 为 `date`（`%Y%m%d`，`errors="coerce"`）

**`fetch_opt_daily(trade_date: date) -> pd.DataFrame`**
- 调用 `pro.opt_daily(trade_date=date_str, fields=...)`
- 复用 `_format_trade_date` helper

## 存储结构（pipeline.py）

```
data/
└── options/
    ├── opt_basic/
    │   └── date=20260602/data.parquet   # 全量，每次全量覆盖
    └── opt_daily/
        ├── date=20160104/data.parquet
        ├── date=20160105/data.parquet
        └── ...
```

### sync 方法

**`sync_opt_basic()`**
- 迭代 `OPTIONS_EXCHANGES`，每次 `sleep(0.2)`
- 合并所有 DataFrame → `write_daily_partition(options_dir, "opt_basic", today, combined)`
- 更新 meta `update_last_date("opt_basic", today)`
- 异常时 `notifier.send` + `raise`

**`sync_opt_daily(start_date, end_date)`**
- 调用 `_sync_daily_partitioned(table_name="opt_daily", fetch=fetch_opt_daily, data_dir=data_dir/"options")`

## 本地查询 API（api.py）

**`opt_basic(ts_code, exchange, opt_code, call_put, fields)`**
- 读取 `data/options/opt_basic/date=*/data.parquet`
- 支持过滤：`ts_code`（逗号分隔）、`exchange`、`opt_code`、`call_put`
- 返回前格式化 `list_date`、`delist_date`、`maturity_date` 为 `date`

**`opt_daily(ts_code, trade_date, start_date, end_date, exchange, fields)`**
- 调用 `_query_daily_partitioned`，`data_dir_override=data_dir/"options"`
- 通过 `extra={"exchange": exchange}` 支持交易所过滤

注册进 `query()` 分发表：
```python
"opt_basic": self.opt_basic,
"opt_daily": self.opt_daily,
```

## 调度（scheduler.py）

复用 `futures_hour` 配置，offset 从 110 分钟起（接在期货最后一个任务之后）：

```python
options_tables = [
    ("opt_basic", pipeline.sync_opt_basic, 110),
    ("opt_daily", pipeline.sync_opt_daily, 120),
]
```

## CLI（cli.py）

- `SYNC_TABLES` 新增 `"opt_basic"`、`"opt_daily"`
- `range_tables` 新增 `"opt_daily"`
- `sync` 命令新增分支：
  ```python
  if sync_all or table == "opt_basic":
      pipeline.sync_opt_basic()
  if sync_all or table == "opt_daily":
      pipeline.sync_opt_daily(start_date=..., end_date=...)
  ```

CLI 用法：
```bash
uv run python main.py sync --table opt_basic
uv run python main.py sync --table opt_daily
uv run python main.py sync --table opt_daily --start-date 2024-01-01 --end-date 2024-01-31
uv run python main.py sync --all
```

## 变更文件清单

| 文件 | 变更内容 |
|------|---------|
| `zer0share/fetcher.py` | 新增 `OPTIONS_EXCHANGES`、`OPT_BASIC_COLS`、`OPT_DAILY_COLS`、`fetch_opt_basic`、`fetch_opt_daily` |
| `zer0share/pipeline.py` | 新增 `sync_opt_basic`、`sync_opt_daily` |
| `zer0share/api.py` | 新增 `opt_basic`、`opt_daily`，注册进 `query()` |
| `zer0share/scheduler.py` | 新增 `options_tables` block |
| `zer0share/cli.py` | 更新 `SYNC_TABLES`、`range_tables`、`sync` 命令 |
| `tests/` | 新增对应单元测试 |
