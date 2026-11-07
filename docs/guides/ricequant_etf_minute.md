# 米筐 ETF 分钟数据使用指南

本文档介绍如何使用 zer0share 的米筐 ETF 分钟数据功能。

## 功能概述

米筐 ETF 分钟数据模块提供以下功能：

1. **ETF 基础信息同步**：同步所有 ETF 的基础信息（order_book_id, symbol, type, market, status 等）
2. **ETF 分钟行情同步**：同步 ETF 的分钟级 OHLCV 数据
3. **本地查询接口**：提供与米筐 API 兼容的本地查询接口

## 数据字段

### ETF 基础信息字段
- `order_book_id`: 合约代码
- `symbol`: 代码简称
- `type`: 合约类型
- `market`: 市场（cn/hk）
- `status`: 状态（Active/Delisted 等）

### ETF 分钟数据字段
- `order_book_id`: 合约代码
- `datetime`: 分钟时间戳
- `open`: 开盘价
- `close`: 收盘价
- `high`: 最高价
- `low`: 最低价
- `limit_up`: 涨停价
- `limit_down`: 跌停价
- `total_turnover`: 成交额
- `volume`: 成交量
- `num_trades`: 成交笔数
- `prev_close`: 前收盘价
- `trade_date`: 交易日期（YYYYMMDD）

## 配置

### 1. 在 `config/settings.toml` 中启用米筐 ETF 分钟数据

```toml
[ricequant]
enabled = true
# 使用 license_key 或 username/password 之一
license_key = "your_license_key_here"
# username = "your_username"
# password = "your_password"

[ricequant.etf_minute]
enabled = true
request_sleep_seconds = 0.2
batch_size = 500
adjust_type = "none"
skip_suspended = true
```

### 2. 配置调度时间（可选）

在 `config/settings.toml` 的 `[scheduler]` 部分添加：

```toml
[scheduler]
ricequant_etf_basic = "02:10"    # ETF 基础信息同步时间
ricequant_etf_minute = "16:35"   # ETF 分钟数据同步时间
```

## 数据同步

### 同步 ETF 基础信息

```bash
python main.py sync --table ricequant_etf_basic
```

### 同步 ETF 分钟数据

同步指定日期范围：
```bash
python main.py sync --table ricequant_etf_minute --start-date 20240101 --end-date 20240131
```

增量同步（从上次同步位置继续）：
```bash
python main.py sync --table ricequant_etf_minute
```

## 数据查询

### 使用 rq_api 本地接口

```python
from zer0share.rq_api import rq_api

# 初始化 API
api = rq_api("config/settings.toml")

# 1. 查询所有 ETF 基础信息
etf_list = api.all_etf_instruments(type="ETF")
print(etf_list.head())

# 2. 查询 ETF 分钟数据
df = api.get_etf_price(
    order_book_ids=["510050.XSHG", "510300.XSHG"],
    start_date="20240102",
    end_date="20240105",
    fields=None,  # None 表示查询所有字段
)
print(df.head())

# 3. 查询指定字段
df_selected = api.get_etf_price(
    order_book_ids="510050.XSHG",
    start_date="20240102",
    end_date="20240102",
    fields="order_book_id,datetime,open,close,high,low,volume",
)

# 4. 查询每日汇总数据（聚合分钟数据）
df_daily = api.get_etf_daily_sum(
    order_book_ids=["510050.XSHG", "510300.XSHG"],
    fields=["volume", "total_turnover"],
    start_date="20240102",
    end_date="20240105",
)
print(df_daily)
```

### 使用底层 query 接口

```python
from pathlib import Path
from zer0share.query import QueryContext
from zer0share.query import ricequant

# 初始化查询上下文
ctx = QueryContext(Path("data"))

# 查询 ETF 分钟数据
df = ricequant.get_etf_price(
    ctx,
    order_book_ids=["510050.XSHG"],
    start_date="20240102",
    end_date="20240102",
)

# 查询 ETF 基础信息
etf_list = ricequant.all_etf_instruments(ctx, type="ETF")
```

## 示例代码

完整示例代码请参考：
```
examples/ricequant/etf_minute_query_smoke.py
```

运行示例：
```bash
python examples/ricequant/etf_minute_query_smoke.py
```

## 数据存储位置

- **ETF 基础信息**：`data/ricequant/etf_basic/data.parquet`
- **ETF 分钟数据**：`data/ricequant/etf_minute/date=YYYYMMDD/data.parquet`

数据采用 Hive 分区格式按日期存储，便于高效查询。

## 注意事项

1. **复权类型**：当前仅支持 `adjust_type="none"`（不复权），后续可根据需要扩展
2. **批量大小**：建议 ETF 的 `batch_size` 设置为 500-1000，避免单次请求过大
3. **请求频率**：`request_sleep_seconds` 控制请求间隔，避免触发米筐 API 限流
4. **数据完整性**：首次同步建议从较近的日期开始，避免一次性同步过多历史数据

## 与股票分钟数据的对比

| 特性 | 股票分钟数据 | ETF 分钟数据 |
|------|-------------|-------------|
| 表名 | `ricequant_stock_minute` | `ricequant_etf_minute` |
| 基础信息表 | `ricequant_basic` | `ricequant_etf_basic` |
| 数据源 | `all_instruments(type='CS')` | `all_instruments(type='ETF')` |
| 默认批量大小 | 1000 | 500 |
| 复权支持 | 支持前复权/后复权 | 支持前复权/后复权 |
| 当前实现 | 仅不复权 | 仅不复权 |

## 常见问题

### Q: 为什么需要先同步 ricequant_etf_basic？
A: 分钟数据同步任务需要从 ETF 基础信息表中读取 ETF 列表，因此必须先同步基础信息。

### Q: 如何只同步特定的 ETF？
A: 当前版本同步所有 Active 状态的 ETF。如需自定义，可以修改 `zer0share/sync/ricequant.py` 中的 `_load_order_book_ids` 方法。

### Q: 数据查询很慢怎么办？
A: ETF 分钟数据采用 DuckDB + Parquet 存储，查询效率较高。如果查询慢，检查：
1. 是否查询了过大的日期范围
2. 是否使用了 `fields` 参数限制返回字段
3. 考虑使用 `get_etf_daily_sum` 进行聚合查询

### Q: 支持哪些 ETF？
A: 支持米筐 `all_instruments(type='ETF', market='cn')` 返回的所有中国内地 ETF。

## 更新日志

- **2026-07-06**: 初始版本发布
  - 支持 ETF 基础信息同步
  - 支持 ETF 分钟数据同步
  - 提供本地查询接口
  - 保留所有米筐返回字段
