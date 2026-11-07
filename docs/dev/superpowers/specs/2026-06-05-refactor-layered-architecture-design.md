# zer0share 重构设计：分层架构

## 背景

当前代码库的主要问题是 `pipeline.py`（~990 行）和 `api.py`（~1000 行）两个 God class，所有领域逻辑（股票、期货、期权、日历、行业）全部混在一起。新增一张表需要改 6 个地方（fetcher、storage、pipeline、api、cli、scheduler），且每次改动都在已经很长的文件里追加。

根因：没有按数据流组织代码，领域边界模糊，文件职责不清。

## 设计原则

**数据流就是架构。** 代码目录结构直接反映数据的流动方向：

```
Tushare ──→ fetcher ──→ storage ──→ Parquet 文件
                                         ↓
                              query ←── DuckDB 读取
```

每一层只依赖它下面的层，没有跨层调用。

## 目标结构

```
zer0share/
  schema.py      # 层 0：所有 *_COLS 列定义（从 fetcher.py 移出）
  fetcher.py     # 层 1：从 Tushare 取数据（只做 API 调用）
  storage.py     # 层 2：读写 Parquet + MetaStore（不动）

  sync/          # 层 3：同步逻辑（调用层 1 + 层 2）
    __init__.py  #   SyncContext dataclass
    _helpers.py  #   sync_daily_partitioned、公共工具函数、常量
    equities.py  #   sync_basic, sync_daily_kline, sync_adj_factor,
                 #   sync_daily_basic, sync_stock_st, sync_suspend_d,
                 #   sync_stk_limit, sync_index_weight, sync_index_daily
    calendar.py  #   sync_trade_cal
    industry.py  #   sync_industry, sync_ci_member
    futures.py   #   sync_fut_basic, sync_fut_daily, sync_fut_holding,
                 #   sync_fut_wsr, sync_fut_settle, sync_fut_mapping,
                 #   sync_ft_limit, sync_fut_weekly, sync_fut_monthly,
                 #   sync_fut_index_daily, sync_fut_weekly_detail
    options.py   #   sync_opt_basic, sync_opt_daily

  query/         # 层 4：查询逻辑（调用层 2）
    __init__.py  #   QueryContext dataclass
    _helpers.py  #   query_daily_partitioned、_parse_date、_parse_fields 等
    equities.py  #   stock_basic, daily, adj_factor, daily_basic,
                 #   stock_st, suspend_d, stk_limit, index_weight, index_daily
    calendar.py  #   trade_cal
    industry.py  #   index_classify, index_member_all, ci_index_member
    futures.py   #   fut_basic, fut_daily, fut_holding, fut_wsr,
                 #   fut_settle, fut_mapping, ft_limit,
                 #   fut_weekly, fut_monthly, fut_index_daily, fut_weekly_detail
    options.py   #   opt_basic, opt_daily

  pipeline.py    # 门面：把 sync/* 包成 Pipeline 类，供 cli / scheduler 调用
  api.py         # 门面：把 query/* 包成 LocalPro 类 + pro_api()

  cli.py         # 改 import，逻辑不动
  scheduler.py   # 改 import，逻辑不动
  universe.py    # 不动
  config.py      # 不动
  storage.py     # 不动
  notifier.py    # 不动
  logging.py     # 不动
```

## 核心接口设计

### SyncContext

`sync/` 层所有函数接收同一个 context 对象，避免重复传参：

```python
# sync/__init__.py
from dataclasses import dataclass
from zer0share.config import Config
from zer0share.fetcher import TushareFetcher
from zer0share.notifier import Notifier
from zer0share.storage import MetaStore

@dataclass
class SyncContext:
    cfg: Config
    fetcher: TushareFetcher
    notifier: Notifier
    meta: MetaStore
```

### sync/ 层函数风格

每个函数体就是数据流，直接可读：

```python
# sync/equities.py
from zer0share.sync import SyncContext
from zer0share.sync._helpers import sync_daily_partitioned, skip_if_not_trading
from zer0share import storage

def sync_basic(ctx: SyncContext) -> None:
    if skip_if_not_trading(ctx, "SSE"):
        return
    df = ctx.fetcher.fetch_basic()
    storage.write_basic(ctx.cfg.data_dir, df)
    ctx.meta.update_last_date("basic", date.today())
    logger.info(f"basic 同步完成: {len(df)} 条")

def sync_daily_kline(ctx: SyncContext, start_date=None, end_date=None) -> None:
    sync_daily_partitioned(
        ctx,
        table_name="daily_kline",
        fetch=ctx.fetcher.fetch_daily_kline,
        start_date=start_date,
        end_date=end_date,
    )
```

### sync/_helpers.py 公共工具

```python
# 常量
FIRST_DATE = date(2016, 1, 1)
TRADE_CAL_FIRST_DATE = date(1990, 1, 1)
PROGRESS_INTERVAL = 50
ALL_EXCHANGES = ["SSE", "SZSE", "CFFEX", "DCE", "SHFE", "CZCE", "INE", "GFEX"]

# 核心同步引擎（原 _sync_daily_partitioned）
def sync_daily_partitioned(
    ctx: SyncContext,
    table_name: str,
    fetch: Callable,
    start_date: date | None,
    end_date: date | None,
    write_empty: bool = False,
    data_dir: Path | None = None,
    exchange: str = "SSE",
) -> None: ...

# 其他工具函数
def skip_if_not_trading(ctx: SyncContext, exchange: str) -> bool: ...
def ensure_trade_cal_loaded(ctx: SyncContext) -> None: ...
def parse_tushare_date(value: str | date) -> date: ...
def month_ranges(start: date, end: date) -> list[tuple[date, date]]: ...
def week_ranges(start: date, end: date) -> list[tuple[str, date]]: ...
```

### Pipeline 门面

`pipeline.py` 只做转发，不含业务逻辑：

```python
# pipeline.py
from zer0share.sync import SyncContext
from zer0share.sync import equities, calendar, industry, futures, options
from zer0share.storage import MetaStore

class Pipeline:
    def __init__(self, cfg: Config, fetcher: TushareFetcher, notifier: Notifier):
        self._ctx = SyncContext(cfg, fetcher, notifier, MetaStore(cfg.db_path))

    # 股票
    def sync_basic(self):                               equities.sync_basic(self._ctx)
    def sync_daily_kline(self, start_date=None, end_date=None):   equities.sync_daily_kline(self._ctx, start_date, end_date)
    def sync_adj_factor(self, start_date=None, end_date=None):   equities.sync_adj_factor(self._ctx, start_date, end_date)
    def sync_daily_basic(self, start_date=None, end_date=None):  equities.sync_daily_basic(self._ctx, start_date, end_date)
    def sync_stock_st(self, start_date=None, end_date=None):     equities.sync_stock_st(self._ctx, start_date, end_date)
    def sync_suspend_d(self, start_date=None, end_date=None):    equities.sync_suspend_d(self._ctx, start_date, end_date)
    def sync_stk_limit(self, start_date=None, end_date=None):    equities.sync_stk_limit(self._ctx, start_date, end_date)
    def sync_index_weight(self, start_date=None, end_date=None): equities.sync_index_weight(self._ctx, start_date, end_date)
    def sync_index_daily(self, start_date=None, end_date=None):  equities.sync_index_daily(self._ctx, start_date, end_date)
    # 日历
    def sync_trade_cal(self):                           calendar.sync_trade_cal(self._ctx)
    # 行业
    def sync_industry(self):                            industry.sync_industry(self._ctx)
    def sync_ci_member(self):                           industry.sync_ci_member(self._ctx)
    # 期货
    def sync_fut_basic(self):                                       futures.sync_fut_basic(self._ctx)
    def sync_fut_daily(self, start_date=None, end_date=None):       futures.sync_fut_daily(self._ctx, start_date, end_date)
    def sync_fut_holding(self, start_date=None, end_date=None):     futures.sync_fut_holding(self._ctx, start_date, end_date)
    def sync_fut_wsr(self, start_date=None, end_date=None):         futures.sync_fut_wsr(self._ctx, start_date, end_date)
    def sync_fut_settle(self, start_date=None, end_date=None):      futures.sync_fut_settle(self._ctx, start_date, end_date)
    def sync_fut_mapping(self, start_date=None, end_date=None):     futures.sync_fut_mapping(self._ctx, start_date, end_date)
    def sync_ft_limit(self, start_date=None, end_date=None):        futures.sync_ft_limit(self._ctx, start_date, end_date)
    def sync_fut_weekly(self, start_date=None, end_date=None):      futures.sync_fut_weekly(self._ctx, start_date, end_date)
    def sync_fut_monthly(self, start_date=None, end_date=None):     futures.sync_fut_monthly(self._ctx, start_date, end_date)
    def sync_fut_index_daily(self, start_date=None, end_date=None): futures.sync_fut_index_daily(self._ctx, start_date, end_date)
    def sync_fut_weekly_detail(self, start_date=None, end_date=None): futures.sync_fut_weekly_detail(self._ctx, start_date, end_date)
    # 期权
    def sync_opt_basic(self):                           options.sync_opt_basic(self._ctx)
    def sync_opt_daily(self, start=None, end=None):     options.sync_opt_daily(self._ctx, start, end)

    def close(self):                                    self._ctx.meta.close()
    def __enter__(self):                                return self
    def __exit__(self, *_):                             self.close(); return False
```

### query/ 层

同样的模式。`QueryContext` 只需要 `data_dir`（查询不写数据，不需要 fetcher / notifier / meta）：

```python
# query/__init__.py
@dataclass
class QueryContext:
    data_dir: Path
```

```python
# query/equities.py
def daily(ctx: QueryContext, ts_code=None, trade_date=None,
          start_date=None, end_date=None, fields=None) -> pd.DataFrame:
    return query_daily_partitioned(
        ctx, table_name="daily_kline", columns=DAILY_COLS,
        ts_code=ts_code, trade_date=trade_date,
        start_date=start_date, end_date=end_date, fields=fields,
    )
```

### api.py 门面

```python
# api.py
from zer0share.query import QueryContext
from zer0share.query import equities, calendar, industry, futures, options

class LocalPro:
    def __init__(self, data_dir: str | Path):
        self._ctx = QueryContext(Path(data_dir))

    def stock_basic(self, **kwargs):    return equities.stock_basic(self._ctx, **kwargs)
    def daily(self, **kwargs):          return equities.daily(self._ctx, **kwargs)
    def trade_cal(self, **kwargs):      return calendar.trade_cal(self._ctx, **kwargs)
    # ... 其余方法

    def pro_bar(self, ts_code, ...):    # 留在这里，因为跨 daily + adj_factor
        ...

    def query(self, api_name, **kwargs):
        dispatch = { "daily": self.daily, "trade_cal": self.trade_cal, ... }
        return dispatch[api_name](**kwargs)

def pro_api(config_path="config/settings.toml") -> LocalPro:
    cfg = load_config(Path(config_path))
    return LocalPro(cfg.data_dir)
```

## schema.py

把所有 `*_COLS` 从 `fetcher.py` 移到 `schema.py`。`fetcher.py` 和 `query/` 都从 `schema` 导入，两者不再互相依赖：

```python
# schema.py
BASIC_COLS = ["ts_code", "symbol", "name", ...]
DAILY_COLS = ["ts_code", "trade_date", "open", ...]
TRADE_CAL_COLS = ["exchange", "cal_date", "is_open", "pretrade_date"]
# ... 所有列定义
```

## 顺便修复的问题

`sync_daily_kline` 和 `sync_adj_factor` 是在 `sync_daily_partitioned` 提取之前手写的近似重复版本（各约 80 行），重构后统一走 `sync_daily_partitioned`，消除约 160 行重复代码。

## 不变的东西

| 内容 | 状态 |
|---|---|
| Parquet 文件格式和目录结构 | 不变 |
| `storage.py` 所有函数 | 不变 |
| `MetaStore` 接口 | 不变 |
| `TushareFetcher` 方法签名 | 不变（只是 `*_COLS` 移走） |
| CLI 命令名和行为 | 不变（改 import 路径） |
| APScheduler 任务定义 | 不变（改 import 路径） |
| `LocalPro` 方法签名 | 不变 |
| `Pipeline` 方法签名 | 不变 |
| `universe.py` | 不变 |

## 预期效果

| 指标 | 重构前 | 重构后 |
|---|---|---|
| 最长单文件 | ~1000 行（pipeline.py / api.py） | ~200 行（各领域模块） |
| 新增一张表需改的文件 | 6 个 | 3 个（fetcher + sync/domain + query/domain） |
| 重复代码 | sync_daily_kline / sync_adj_factor ~160 行 | 消除 |
| 模块间耦合 | api.py 直接 import fetcher.py 的列定义 | 统一从 schema.py 导入 |
