# 期货数据支持（第二批）设计文档

> 日期：2026-06-01
> 范围：第二批 5 个扩展期货数据类型
> 前置：第一批 6 个核心数据类型已完成

---

## 1. 概述

在第一批期货数据的基础上，新增 5 个扩展数据类型。采用与第一批完全一致的架构（方案 A：直接扩展现有模块），存储在 `data/futures/` 子目录下，按日期分区。

由于 `fut_weekly_monthly` 拆分为 `fut_weekly` 和 `fut_monthly` 两张表，原始 4 个 Tushare API 实际产生 5 个数据表。

### 数据类型总览

| # | 数据表 | Tushare API | 最低积分 | 同步模式 | 数据起始 |
|---|--------|------------|---------|---------|---------|
| 1 | `ft_limit` | `ft_limit` | 5000 | 按日增量 | 2005 |
| 2 | `fut_weekly` | `fut_weekly_monthly(freq='week')` | 2000 | 按日增量 | — |
| 3 | `fut_monthly` | `fut_weekly_monthly(freq='month')` | 2000 | 按日增量 | — |
| 4 | `fut_index_daily` | `fut_index_daily` | 2000 | 按日增量（全市场） | — |
| 5 | `fut_weekly_detail` | `fut_weekly_detail` | 600 | 按周增量 | 2010-03 |

### 设计原则

沿用第一批所有决策：
- 接口名称与 Tushare 严格对齐
- 所有数据按日期分区，`data/futures/` 子目录
- CLI 加入现有 `sync` 命令
- 每日自动同步加入 scheduler
- 交易日历复用 SSE

---

## 2. 字段定义（fetcher.py）

### 常量

```python
FT_LIMIT_COLS = [
    "trade_date", "ts_code", "name", "up_limit", "down_limit",
    "m_ratio", "cont", "exchange",
]

FUT_WEEKLY_COLS = [
    "ts_code", "trade_date", "freq", "open", "high", "low",
    "close", "pre_close", "settle", "pre_settle", "vol",
    "amount", "oi", "oi_chg", "exchange", "change1", "change2",
]

FUT_MONTHLY_COLS = FUT_WEEKLY_COLS  # 字段完全相同

FUT_INDEX_DAILY_COLS = [
    "ts_code", "trade_date", "close", "open", "high", "low",
    "pre_close", "change", "pct_chg", "vol", "amount",
]

FUT_WEEKLY_DETAIL_COLS = [
    "exchange", "prd", "name", "vol", "vol_yoy", "amount",
    "amout_yoy", "cumvol", "cumvol_yoy", "cumamt", "cumamt_yoy",
    "open_interest", "interest_wow", "mc_close", "close_wow",
    "week", "week_date",
]
```

### Fetch 方法

| 方法 | Tushare API | 参数 | 说明 |
|------|------------|------|------|
| `fetch_ft_limit(trade_date)` | `ft_limit` | trade_date | 按日拉取全市场涨跌停 |
| `fetch_fut_weekly(trade_date)` | `fut_weekly_monthly` | trade_date, freq='week' | 固定 freq=week |
| `fetch_fut_monthly(trade_date)` | `fut_weekly_monthly` | trade_date, freq='month' | 固定 freq=month |
| `fetch_fut_index_daily(trade_date)` | `fut_index_daily` | trade_date | 按日拉全市场指数 |
| `fetch_fut_weekly_detail(week)` | `fut_weekly_detail` | week (如 '202001') | 按周拉取 |

---

## 3. 存储层

无新增存储函数。复用现有 `write_daily_partition`、`daily_partition_exists`、`read_daily_partition`，传入 `data_dir / "futures"` 作为基础路径。

### 目录结构

```
data/
└── futures/
    ├── ft_limit/
    │   └── date=YYYYMMDD/data.parquet
    ├── fut_weekly/
    │   └── date=YYYYMMDD/data.parquet
    ├── fut_monthly/
    │   └── date=YYYYMMDD/data.parquet
    ├── fut_index_daily/
    │   └── date=YYYYMMDD/data.parquet
    └── fut_weekly_detail/
        └── date=YYYYMMDD/data.parquet
```

---

## 4. 同步层（pipeline.py）

### 模式一：标准按日增量（ft_limit、fut_weekly、fut_monthly）

直接复用 `_sync_daily_partitioned`，与第一批完全一致：

```python
def sync_ft_limit(self, start_date=None, end_date=None):
    self._sync_daily_partitioned(
        table_name="ft_limit",
        fetch=self._fetcher.fetch_ft_limit,
        start_date=start_date, end_date=end_date,
        data_dir=self._cfg.data_dir / "futures",
    )

def sync_fut_weekly(self, start_date=None, end_date=None):
    self._sync_daily_partitioned(
        table_name="fut_weekly",
        fetch=self._fetcher.fetch_fut_weekly,
        start_date=start_date, end_date=end_date,
        data_dir=self._cfg.data_dir / "futures",
    )

def sync_fut_monthly(self, start_date=None, end_date=None):
    self._sync_daily_partitioned(
        table_name="fut_monthly",
        fetch=self._fetcher.fetch_fut_monthly,
        start_date=start_date, end_date=end_date,
        data_dir=self._cfg.data_dir / "futures",
    )
```

### 模式二：全市场按日拉取（fut_index_daily）

与现有 `sync_index_daily` 模式一致 — 用 `trade_date` 拉全市场数据，按 trade_date 分组写入日期分区：

```python
def sync_fut_index_daily(self, start_date=None, end_date=None):
    # 1. 计算日期范围（同 _sync_daily_partitioned 的增量逻辑）
    # 2. 逐交易日调用 fetch_fut_index_daily(trade_date)
    # 3. 写入 futures/fut_index_daily/date=YYYYMMDD/data.parquet
    # 4. 更新 meta
```

### 模式三：按周增量（fut_weekly_detail）

新增按周遍历的同步模式：

```python
def sync_fut_weekly_detail(self, start_date=None, end_date=None):
    # 1. 计算需要同步的周范围（基于 start_date/end_date）
    # 2. 生成周编号列表（如 ['202001', '202002', ...]）
    # 3. 逐周调用 fetch_fut_weekly_detail(week)
    # 4. 每周数据按 week_date（输出字段）写入对应日期分区
    # 5. 跳过已存在的分区
    # 6. 更新 meta
```

### Meta 追踪

| meta 表名 | 说明 |
|-----------|------|
| `ft_limit` | 最后同步日期 |
| `fut_weekly` | 最后同步日期 |
| `fut_monthly` | 最后同步日期 |
| `fut_index_daily` | 最后同步日期 |
| `fut_weekly_detail` | 最后同步日期 |

---

## 5. 查询层（api.py）

### 查询方法

```python
def ft_limit(self, ts_code=None, trade_date=None, start_date=None,
             end_date=None, exchange=None, fields=None) -> pd.DataFrame
    # _query_daily_partitioned + extra_filters for exchange/cont

def fut_weekly(self, ts_code=None, trade_date=None, start_date=None,
               end_date=None, exchange=None, fields=None) -> pd.DataFrame
    # _query_daily_partitioned + extra_filters for exchange

def fut_monthly(self, ts_code=None, trade_date=None, start_date=None,
                end_date=None, exchange=None, fields=None) -> pd.DataFrame
    # _query_daily_partitioned + extra_filters for exchange

def fut_index_daily(self, ts_code=None, trade_date=None, start_date=None,
                    end_date=None, fields=None) -> pd.DataFrame
    # _query_daily_partitioned，与 index_daily 模式一致

def fut_weekly_detail(self, exchange=None, prd=None, start_date=None,
                      end_date=None, fields=None) -> pd.DataFrame
    # _query_daily_partitioned + extra_filters for exchange/prd
```

全部注册到 `query()` dispatch dict。

---

## 6. 调度器（scheduler.py）

追加到 `futures_tables` 列表，在第一批（偏移 0-50）之后继续：

| Job ID | 函数 | 偏移(分钟) |
|--------|------|-----------|
| `ft_limit` | `pipeline.sync_ft_limit` | 60 |
| `fut_weekly` | `pipeline.sync_fut_weekly` | 70 |
| `fut_monthly` | `pipeline.sync_fut_monthly` | 80 |
| `fut_index_daily` | `pipeline.sync_fut_index_daily` | 90 |
| `fut_weekly_detail` | `pipeline.sync_fut_weekly_detail` | 100 |

共享 `futures_hour`（默认 17:00），17:00-18:40 完成 11 个期货任务。

---

## 7. CLI（cli.py）

`SYNC_TABLES` 追加 5 个：`ft_limit`、`fut_weekly`、`fut_monthly`、`fut_index_daily`、`fut_weekly_detail`

`range_tables` 追加 5 个（全部支持日期范围）。

Sync dispatch 追加 5 个 if 块。

---

## 8. 变更文件清单

| 文件 | 变更内容 |
|------|---------|
| `zer0share/fetcher.py` | 新增 5 组字段常量 + 5 个 fetch 方法 |
| `zer0share/pipeline.py` | 新增 5 个 sync 方法（3 标准 daily + 1 全市场 + 1 按周） |
| `zer0share/api.py` | 新增 5 个查询方法 + dispatch 更新 |
| `zer0share/scheduler.py` | 追加 5 个定时任务到 futures_tables |
| `zer0share/cli.py` | SYNC_TABLES + range_tables + sync dispatch |

无新增文件，无新增依赖，完全在现有模块内扩展。
