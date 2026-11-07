# 交易日历扩展设计

## 背景

当前 `sync_trade_cal` 仅同步 SSE、SZSE 两个交易所的交易日历。期货和期权的同步任务直接硬编码使用 SSE 日历判断交易日，`fut_basic`、`opt_basic` 等基础信息表在非交易日也会执行同步，浪费 Tushare API 积分。

## 需求

- 为全部 8 个交易所（SSE、SZSE、CFFEX、DCE、SHFE、CZCE、INE、GFEX）分别拉取交易日历
- 所有衍生品和股票基础信息同步任务在非交易日静默跳过
- trade_cal 同步优先于其他任务执行
- 向后兼容，不破坏现有 CLI 和手动同步行为

## 设计

### 1. 交易日历扩展

**文件**：`pipeline.py`

新增常量：

```python
ALL_EXCHANGES = ["SSE", "SZSE", "CFFEX", "DCE", "SHFE", "CZCE", "INE", "GFEX"]
```

`sync_trade_cal` 改为遍历 `ALL_EXCHANGES`，为每个交易所拉取并存储交易日历。存储结构不变：`data/trade_cal/exchange=<exchange>/data.parquet`。`load_trade_cal_from_parquet` 也传入 `ALL_EXCHANGES`，确保 DuckDB 加载全部交易所日历。

保留现有 `EXCHANGES = ["SSE", "SZSE"]` 用于股票相关逻辑，避免影响现有行为。

### 2. MetaStore 新增方法

**文件**：`storage.py`

新增 `is_trading_day(exchange, cal_date) -> bool`：

```python
def is_trading_day(self, exchange: str, cal_date: date) -> bool:
    row = self._conn.execute(
        "SELECT is_open FROM trade_cal WHERE exchange = ? AND cal_date = ?",
        [exchange, cal_date]
    ).fetchone()
    if row is None:
        return True  # 日历未覆盖，保守返回 True
    return bool(row[0])
```

日历未覆盖时返回 True（宁可多跑一次，不漏数据）。

### 3. Pipeline 新增工具方法

**文件**：`pipeline.py`

```python
def _ensure_trade_cal_loaded(self) -> None:
    """确保交易日历已加载到 DuckDB。"""
    if self._meta.get_last_date("trade_cal") is None:
        self.sync_trade_cal()

def _skip_if_not_trading(self, exchange: str) -> bool:
    """检查今天是否为交易日，非交易日静默跳过。返回 True 表示已跳过。"""
    self._ensure_trade_cal_loaded()
    today = date.today()
    if not self._meta.is_trading_day(exchange, today):
        logger.info(f"今日 {today} 非交易日，跳过同步")
        return True
    return False
```

### 4. 各 sync 方法加非交易日跳过

在以下方法开头加 `if self._skip_if_not_trading("SSE"): return`：

| 方法 | 说明 |
|------|------|
| `sync_basic` | 股票基础信息 |
| `sync_industry` | 申万行业分类 |
| `sync_ci_member` | 中信行业成分 |
| `sync_fut_basic` | 期货合约信息 |
| `sync_opt_basic` | 期权合约信息 |
| `sync_fut_index_daily` | 期货指数日线 |

`sync_trade_cal` 本身不加——日历同步是前置依赖，必须每天跑以确保覆盖到年末。

所有方法统一用 SSE 日历判断。中国交易所节假日统一安排，SSE 日历对所有交易所有效。

### 5. `_sync_daily_partitioned` 加 exchange 参数

**文件**：`pipeline.py`

```python
def _sync_daily_partitioned(
    self,
    table_name: str,
    fetch,
    start_date: date | None,
    end_date: date | None,
    write_empty: bool = False,
    data_dir: Path | None = None,
    exchange: str = "SSE",       # 新增
) -> None:
```

内部 `get_trading_days("SSE", ...)` 改为 `get_trading_days(exchange, ...)`。默认值 `"SSE"` 确保向后兼容，现有调用方无需修改。错误提示中的 `"SSE"` 也改为动态。

### 6. 调度器调整

**文件**：`scheduler.py`、`config.py`、`settings.example.toml`

- 新增 trade_cal 调度任务，排在衍生品同步之前
- Config 新增 `scheduler_trade_cal_hour` 和 `scheduler_trade_cal_minute`
- `_ensure_trade_cal_loaded` 确保手动触发时也能自动加载日历

### 7. 测试

| 测试 | 覆盖内容 |
|------|---------|
| `test_is_trading_day` | MetaStore 新方法：交易日/非交易日/未覆盖日期 |
| `test_skip_if_not_trading` | Pipeline 新方法：非交易日跳过、交易日正常执行 |
| `test_sync_trade_cal_all_exchanges` | 确认 8 个交易所均被同步 |
| `test_sync_fut_basic_skips_non_trading_day` | 端到端：非交易日不调用 fetcher |
| `test_sync_daily_partitioned_with_exchange` | 验证 exchange 参数传递到 get_trading_days |

## 改动清单

| 文件 | 改动 |
|------|------|
| `pipeline.py` | 新增 `ALL_EXCHANGES`；`sync_trade_cal` 扩展到 8 交易所；新增 `_ensure_trade_cal_loaded`、`_skip_if_not_trading`；`_sync_daily_partitioned` 加 `exchange` 参数；6 个 sync 方法加跳过判断 |
| `storage.py` | MetaStore 新增 `is_trading_day` |
| `scheduler.py` | 新增 trade_cal 调度任务 |
| `config.py` | 新增 `scheduler_trade_cal_hour`、`scheduler_trade_cal_minute` |
| `settings.example.toml` | 新增 trade_cal 调度时间配置 |
| `tests/` | 5 组新测试 |
