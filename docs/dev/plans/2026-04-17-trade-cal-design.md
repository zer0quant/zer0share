# 交易日历模块设计

**日期:** 2026-04-17  
**状态:** 已确认

## 概述

新增交易日历（`trade_cal`）数据表，通过 Tushare `trade_cal` 接口拉取全部 7 个交易所的历史日历数据，以 Parquet 按交易所分区存储，同时加载进 DuckDB 供快速查询。`sync_daily_kline` 改造为依据 SSE 交易日历过滤，只对真实交易日调用 API，跳过周末和节假日。

## 数据存储

### Parquet 分区结构

```
data/
└── trade_cal/
    ├── exchange=SSE/
    │   └── data.parquet
    ├── exchange=SZSE/
    │   └── data.parquet
    ├── exchange=CFFEX/
    │   └── data.parquet
    ├── exchange=SHFE/
    │   └── data.parquet
    ├── exchange=CZCE/
    │   └── data.parquet
    ├── exchange=DCE/
    │   └── data.parquet
    └── exchange=INE/
        └── data.parquet
```

### trade_cal 字段

| 字段 | 类型 | 说明 |
|------|------|------|
| exchange | VARCHAR | 交易所代码 |
| cal_date | DATE | 日历日期 |
| is_open | BOOLEAN | 是否交易日 |
| pretrade_date | DATE | 上一交易日 |

### DuckDB 表

```sql
CREATE TABLE IF NOT EXISTS trade_cal (
    exchange      VARCHAR,
    cal_date      DATE,
    is_open       BOOLEAN,
    pretrade_date DATE,
    PRIMARY KEY (exchange, cal_date)
);
```

## 新增模块

### TushareFetcher.fetch_trade_cal(exchange)

- 拉取指定交易所从 19900101 至今全量日历
- `cal_date` / `pretrade_date` 转 `date` 类型
- `is_open` 转 `bool` 类型
- 交易所列表：SSE, SZSE, CFFEX, SHFE, CZCE, DCE, INE

### storage.py 新增函数

```python
def write_trade_cal(data_dir: Path, exchange: str, df: pd.DataFrame) -> None:
    # 写入 data/trade_cal/exchange=XXX/data.parquet（全量覆盖）

def read_trade_cal(data_dir: Path, exchange: str) -> pd.DataFrame:
    # 读取指定交易所日历，文件不存在返回空 DataFrame
```

### MetaStore 新增方法

```python
def load_trade_cal_from_parquet(self, data_dir: Path) -> None:
    # 从所有 Parquet 分区加载到 DuckDB trade_cal 表（DROP + INSERT）

def get_trading_days(self, exchange: str, start: date, end: date) -> list[date]:
    # 查询 is_open=True 的日期列表，按日期升序
```

### Pipeline.sync_trade_cal()

```
1. 对 7 个交易所依次调用 fetch_trade_cal
2. 写入对应 Parquet 分区（全量覆盖）
3. 调用 load_trade_cal_from_parquet 刷新 DuckDB
4. 更新 sync_meta: table_name='trade_cal'
5. 失败时告警并 raise
```

### Pipeline.sync_daily_kline() 改造

```
旧：last_date+1 → today，每天调用 API（含周末/节假日）
新：从 DuckDB trade_cal 查出 SSE is_open=True 的日期列表
    → 只对交易日调用 API
    → 非交易日完全跳过（不计入统计）
```

前置条件检查：若 DuckDB 中无 SSE 交易日历，抛出明确错误提示用户先同步 `trade_cal`。

## CLI 命令

```bash
python main.py sync --table trade_cal      # 同步全部交易所日历
python main.py sync --all                  # 同步顺序：trade_cal → basic → daily_kline
```

## 同步顺序依赖

```
trade_cal（无依赖）
    ↓
basic（无依赖）
    ↓
daily_kline（依赖 trade_cal 已加载到 DuckDB）
```

`sync --all` 需保证顺序：先 `trade_cal`，再 `basic`，最后 `daily_kline`。
