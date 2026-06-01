# 期货数据支持（第一批）设计文档

> 日期：2026-06-01
> 范围：第一批 6 个核心期货数据类型
> 排除：历史分钟行情（ft_mins）、实时分钟行情、历史Tick（非API，云盘交付）
> 第二批（4 个扩展类型）将在本批完成后设计

---

## 1. 概述

在 zer0share 项目中新增期货数据支持，采用方案 A（直接扩展现有模块），严格对齐 Tushare API 接口命名和参数。期货数据统一存放在 `data/futures/` 子目录下，按日期分区存储。

### 第一批数据类型

| # | 数据类型 | Tushare API | 最低积分 | 单次限量 | 数据起始 | 更新频率 |
|---|---------|-------------|---------|---------|---------|---------|
| 1 | 合约信息 | `fut_basic` | 2000 | 10000 | 全历史 | 静态 |
| 2 | 日线行情 | `fut_daily` | 2000 | 2000 | 1996-01 | 每日盘后 |
| 3 | 持仓排名 | `fut_holding` | 2000 | 2000 | 2002-01 | 每日盘后 |
| 4 | 仓单日报 | `fut_wsr` | 2000 | 1000 | 2006-01 | 每日盘后 |
| 5 | 结算参数 | `fut_settle` | 2000 | 1600 | 2012-01 | 每日盘后 |
| 6 | 主力与连续合约映射 | `fut_mapping` | 2000 | 2000 | - | 每日 |

### 设计原则

- 接口名称与 Tushare 严格对齐
- 所有数据按日期分区，一天一个 parquet 文件包含全市场数据
- 存储在 `data/futures/` 子目录下
- CLI 命令加入现有 `sync` 命令的 table 列表
- 每日自动同步加入 scheduler

---

## 2. 交易所代码

```python
FUTURES_EXCHANGES = ["CZCE", "SHFE", "DCE", "CFFEX", "INE", "GFEX"]
```

| 交易所名称 | 代码 | 合约后缀 |
|-----------|------|---------|
| 郑州商品交易所 | CZCE | .ZCE |
| 上海期货交易所 | SHFE | .SHF |
| 大连商品交易所 | DCE | .DCE |
| 中国金融期货交易所 | CFFEX | .CFX |
| 上海国际能源交易所 | INE | .INE |
| 广州期货交易所 | GFEX | .GFE |

### 数据规则

- 行情相关数据（日线、结算参数等）：使用带交易所后缀的完整合约代码，如 `CU1811.SHF`
- 品种相关数据（持仓排名、仓单等）：使用品种代码，如 `CU`

---

## 3. 数据类型定义（fetcher.py）

### 3.1 字段常量

```python
FUT_BASIC_COLS = [
    "ts_code", "symbol", "exchange", "name", "fut_code",
    "multiplier", "trade_unit", "per_unit", "quote_unit",
    "quote_unit_desc", "d_mode_desc", "list_date", "delist_date",
    "d_month", "last_ddate", "trade_time_desc",
]

FUT_DAILY_COLS = [
    "ts_code", "trade_date", "pre_close", "pre_settle",
    "open", "high", "low", "close", "settle",
    "change1", "change2", "vol", "amount", "oi", "oi_chg",
    "delv_settle",
]

FUT_HOLDING_COLS = [
    "trade_date", "symbol", "broker", "vol", "vol_chg",
    "long_hld", "long_chg", "short_hld", "short_chg", "exchange",
]

FUT_WSR_COLS = [
    "trade_date", "symbol", "fut_name", "warehouse", "wh_id",
    "pre_vol", "vol", "vol_chg", "area", "year", "grade",
    "brand", "place", "pd", "is_ct", "unit", "exchange",
]

FUT_SETTLE_COLS = [
    "ts_code", "trade_date", "settle", "trading_fee_rate",
    "trading_fee", "delivery_fee", "b_hedging_margin_rate",
    "s_hedging_margin_rate", "long_margin_rate", "short_margin_rate",
    "offset_today_fee", "exchange",
]

FUT_MAPPING_COLS = [
    "ts_code", "trade_date", "mapping_ts_code",
]
```

### 3.2 Fetch 方法

| 方法 | Tushare API | 参数 | 说明 |
|------|------------|------|------|
| `fetch_fut_basic(exchange, fut_type='1')` | `fut_basic` | exchange（必选）, fut_type | 按交易所分批拉取合约列表 |
| `fetch_fut_daily(trade_date)` | `fut_daily` | trade_date | 按日期拉取全市场合约日线 |
| `fetch_fut_holding(trade_date)` | `fut_holding` | trade_date | 按日期拉取全市场持仓排名 |
| `fetch_fut_wsr(trade_date)` | `fut_wsr` | trade_date | 按日期拉取全市场仓单 |
| `fetch_fut_settle(trade_date)` | `fut_settle` | trade_date | 按日期拉取全市场结算参数 |
| `fetch_fut_mapping(trade_date)` | `fut_mapping` | trade_date | 按日期拉取主力合约映射 |

---

## 4. 存储层（storage.py）

### 4.1 目录结构

```
data/
└── futures/
    ├── fut_basic/
    │   └── date=20240101/data.parquet
    ├── fut_daily/
    │   └── date=20240101/data.parquet
    ├── fut_holding/
    │   └── date=20240101/data.parquet
    ├── fut_wsr/
    │   └── date=20240101/data.parquet
    ├── fut_settle/
    │   └── date=20240101/data.parquet
    └── fut_mapping/
        └── date=20240101/data.parquet
```

### 4.2 存储函数

每个数据类型 3 个函数：

| 函数 | 用途 |
|------|------|
| `write_fut_xxx(data_dir, trade_date, df)` | 写入某日数据 |
| `fut_xxx_partition_exists(data_dir, trade_date)` | 检查某日分区是否已存在 |
| `read_fut_xxx(data_dir, trade_date)` | 读取某日数据 |

`data_dir` 传入 `cfg.data_dir / "futures"`，pipeline 层负责拼接。

`fut_basic` 按日期分区存储，记录该日同步时点的完整合约列表。

---

## 5. 同步层（pipeline.py）

### 5.1 全量同步：fut_basic

```python
def sync_fut_basic(self) -> None:
```

- 遍历 6 个交易所，每个交易所分别拉取 `fut_type='1'`（普通合约）和 `fut_type='2'`（主力与连续合约）
- 共 12 次 API 调用
- 所有结果拼接后写入 `futures/fut_basic/date=YYYYMMDD/`
- 更新 meta 记录

### 5.2 按日期增量同步

```python
def sync_fut_daily(self, start_date=None, end_date=None) -> None:
def sync_fut_holding(self, start_date=None, end_date=None) -> None:
def sync_fut_wsr(self, start_date=None, end_date=None) -> None:
def sync_fut_settle(self, start_date=None, end_date=None) -> None:
def sync_fut_mapping(self, start_date=None, end_date=None) -> None:
```

增量同步流程（与现有 `_sync_daily_partitioned` 一致）：

1. 查询 meta 获取该表最后同步日期
2. 计算日期范围：`last_date + 1` → `end_date`（默认今天）
3. 从 meta 交易日历获取范围内交易日列表
4. 逐日处理：
   - 检查分区是否已存在，存在则跳过
   - 调用 fetch 方法拉取当日数据
   - 写入 parquet 分区
   - 更新 meta 的 `last_date`
5. 每日同步完成后通知 notifier

### 5.3 Meta 追踪

| meta 表名 | 说明 |
|-----------|------|
| `fut_basic` | 最后全量同步日期 |
| `fut_daily` | 最后日线同步日期 |
| `fut_holding` | 最后持仓排名同步日期 |
| `fut_wsr` | 最后仓单同步日期 |
| `fut_settle` | 最后结算参数同步日期 |
| `fut_mapping` | 最后主力映射同步日期 |

交易日历复用现有 `trade_cal`（交易所=SSE），无需单独维护。

---

## 6. 查询层（api.py）

### 6.1 查询方法

```python
def fut_basic(self, ts_code=None, exchange=None, fut_type=None,
              fut_code=None, fields=None) -> pd.DataFrame

def fut_daily(self, ts_code=None, trade_date=None, start_date=None,
              end_date=None, fields=None) -> pd.DataFrame

def fut_holding(self, trade_date=None, symbol=None, start_date=None,
                end_date=None, exchange=None, fields=None) -> pd.DataFrame

def fut_wsr(self, trade_date=None, symbol=None, start_date=None,
            end_date=None, exchange=None, fields=None) -> pd.DataFrame

def fut_settle(self, ts_code=None, trade_date=None, start_date=None,
               end_date=None, exchange=None, fields=None) -> pd.DataFrame

def fut_mapping(self, ts_code=None, trade_date=None, start_date=None,
                end_date=None, fields=None) -> pd.DataFrame
```

### 6.2 查询逻辑

通过 DuckDB 扫描 `data/futures/` 下的分区 parquet：

- **单日查询**：`trade_date` 指定日期，读取单个分区
- **日期范围**：`start_date` + `end_date` 范围扫描
- **合约/品种过滤**：`ts_code`/`symbol`/`exchange` 在 SQL WHERE 中过滤
- **字段选择**：`fields` 参数控制返回列，默认返回全部字段
- **数据目录**：指向 `cfg.data_dir / "futures"`

实现复用现有 `_query_daily_partitioned` 模式，通过 DuckDB SQL 查询分区 parquet 文件。

---

## 7. 调度器（scheduler.py）

### 7.1 任务注册

| Job ID | 函数 | 调度时间 | 说明 |
|--------|------|---------|------|
| `fut_basic` | `pipeline.sync_fut_basic` | 每日 17:00 | 全量合约信息 |
| `fut_daily` | `pipeline.sync_fut_daily` | 每日 17:10 | 日线行情 |
| `fut_holding` | `pipeline.sync_fut_holding` | 每日 17:20 | 持仓排名 |
| `fut_wsr` | `pipeline.sync_fut_wsr` | 每日 17:30 | 仓单日报 |
| `fut_settle` | `pipeline.sync_fut_settle` | 每日 17:40 | 结算参数 |
| `fut_mapping` | `pipeline.sync_fut_mapping` | 每日 17:50 | 主力合约映射 |

时间安排在 17:00 后（期货 15:15 收盘，Tushare 通常 16:00-17:00 更新）。每个任务间隔 10 分钟，避免 API 频率限制。调度时间可通过配置文件调整。单个期货任务失败不影响其他任务。

---

## 8. CLI（cli.py）

### 8.1 SYNC_TABLES 扩展

```python
SYNC_TABLES = [
    # 现有股票
    "daily_kline", "basic", "trade_cal", "adj_factor",
    "daily_basic", "stock_st", "suspend_d", "stk_limit",
    "index_weight", "index_daily", "industry", "ci_member",
    # 新增期货
    "fut_basic", "fut_daily", "fut_holding",
    "fut_wsr", "fut_settle", "fut_mapping",
]
```

### 8.2 使用方式

```bash
# 同步单个期货数据类型
python main.py sync --table fut_daily

# 同步全部期货数据（第一批）
python main.py sync --table fut_basic --table fut_daily --table fut_holding \
    --table fut_wsr --table fut_settle --table fut_mapping

# 指定日期范围
python main.py sync --table fut_daily --start-date 20240101 --end-date 20240601

# 同步所有（含股票+期货）
python main.py sync --all
```

---

## 9. 变更文件清单

| 文件 | 变更内容 |
|------|---------|
| `zer0share/fetcher.py` | 新增 FUT_*_COLS 常量、FUTURES_EXCHANGES、6 个 fetch 方法 |
| `zer0share/storage.py` | 新增 6 组 write/exists/read 函数（共 18 个函数） |
| `zer0share/pipeline.py` | 新增 6 个 sync 方法 |
| `zer0share/api.py` | LocalPro 新增 6 个查询方法 |
| `zer0share/scheduler.py` | 注册 6 个期货定时任务 |
| `zer0share/cli.py` | SYNC_TABLES 扩展 6 个期货表名 |

无新增文件，无新增依赖，完全在现有模块内扩展。

---

## 10. 第二批预告

以下 4 个数据类型将在第一批完成后设计：

| # | 数据类型 | Tushare API | 最低积分 |
|---|---------|-------------|---------|
| 7 | 涨跌停价格 | `ft_limit` | 5000 |
| 8 | 周/月线行情 | `fut_weekly_monthly` | 2000 |
| 9 | 南华期货指数 | `fut_index_daily` | 2000 |
| 10 | 品种交易周报 | `fut_weekly_detail` | 600 |

设计模式将完全沿用第一批的模式，仅需补充字段定义、fetch 方法和同步逻辑。
