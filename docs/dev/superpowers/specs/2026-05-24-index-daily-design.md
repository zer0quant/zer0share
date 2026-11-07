# 指数日线行情设计文档

## 背景

新增对 12 个主要 A 股宽基指数的日线行情（OHLCV）同步与本地查询支持。数据来源为 Tushare Pro `index_daily` 接口（积分要求 ≥ 2000，单次最大 8000 行）。

主要用途：量化回测中取对冲基准收益率（参见 `dict_index_map`）。

## 同步指数列表

硬编码为 `fetcher.py` 中的 `INDEX_DAILY_CODES` 常量：

| 代码 | 名称 |
|------|------|
| `000001.SH` | 上证指数 |
| `399001.SZ` | 深证成指 |
| `000016.SH` | 上证50 |
| `000300.SH` | 沪深300 |
| `000905.SH` | 中证500 |
| `000852.SH` | 中证1000 |
| `000985.SH` | 中证全指 |
| `399006.SZ` | 创业板指 |
| `000688.SH` | 科创50 |
| `399005.SZ` | 中小板指 |
| `000922.SH` | 中证红利 |
| `932000.CSI` | 中证2000（实现时需实测确认 Tushare 代码，`.CSI` 后缀待验证） |

## 存储结构

按日期分区，与 `daily_kline`、`adj_factor` 保持一致：

```
data/index_daily/
├── date=20160104/data.parquet   # 含当日全部12个指数
├── date=20160105/data.parquet
└── ...
```

每个 parquet 文件包含该日所有已同步指数的行，字段与股票日线完全一致：

`ts_code, trade_date, open, high, low, close, pre_close, change, pct_chg, vol, amount`

历史回填起始日：**2016-01-01**（与 `daily_kline` 对齐）。

## 实现方案：按 ts_code 批量拉取（方案一）

核心选择：API 强制要求 `ts_code` 参数，因此循环指数代码而非日期。

```
sync_index_daily(start, end):
  all_frames = []
  for ts_code in INDEX_DAILY_CODES:          # 12 次 API 调用
      df = fetch_index_daily(ts_code, start, end)
      sleep(0.2)
      all_frames.append(df)

  combined = concat(all_frames)

  for trade_date, part in combined.groupby("trade_date"):
      if daily_partition_exists("index_daily", trade_date):
          skipped += 1; continue
      write_daily_partition("index_daily", trade_date, part)
      success += 1

  meta.update_last_date("index_daily", max_trade_date)
```

全量回填只需 **12 次** API 调用（每指数一次），增量同步同理。

MetaStore 维护一条记录 `index_daily → last_sync_date`，所有指数共享（同一交易日历）。

## 改动文件

| 文件 | 改动 |
|------|------|
| `fetcher.py` | 新增 `INDEX_DAILY_CODES`、`INDEX_DAILY_COLS`、`fetch_index_daily(ts_code, start, end)` |
| `pipeline.py` | 新增 `sync_index_daily(start_date, end_date)` |
| `storage.py` | **无改动**，复用 `write_daily_partition` / `daily_partition_exists` |
| `api.py` | 新增 `index_daily()` 方法 + 加入 `query()` dispatch 表，复用 `_query_daily_partitioned` |
| `cli.py` | 加 `--table index_daily` 选项；`sync --all` 顺序在 `index_weight` 之后插入 |
| `scheduler.py` | 加 `index_daily` 定时任务，与 `daily_kline` 同时间触发 |

## 本地查询 API

```python
pro = pro_api()

# 查单指数历史
df = pro.index_daily(ts_code="000300.SH", start_date="20240101", end_date="20240131")

# 查某日所有指数
df = pro.index_daily(trade_date="20240131")

# 查多个指数
df = pro.index_daily(ts_code="000300.SH,000905.SH", start_date="20240101", end_date="20240131")

# fields 筛选
df = pro.index_daily(ts_code="000300.SH", fields="ts_code,trade_date,close")

# query 分发
df = pro.query("index_daily", ts_code="000300.SH", start_date="20240101")
```

## CLI

```bash
uv run python main.py sync --table index_daily
uv run python main.py sync --table index_daily --start-date 2024-01-01 --end-date 2024-01-31
uv run python main.py sync --all   # index_daily 在 index_weight 之后执行
```

## 定时调度

与 `daily_kline` 使用相同的触发时间（`daily_kline_hour` / `daily_kline_minute`，默认 18:00），收盘后一次性同步当日全部12个指数。
