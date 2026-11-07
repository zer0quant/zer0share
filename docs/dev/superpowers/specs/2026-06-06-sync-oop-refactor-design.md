# Sync 层 OOP 重构设计

## 目标

将 `zer0share/sync/` 从函数式风格重构为面向对象设计，同时：

1. 修复已知 3 个 bug（`FutWeeklyDetailSyncJob` 跳过检查顺序、已最新时抛 ValueError、`FutIndexDailySyncJob` 遍历日历日）
2. 消除 5 个模块中重复的 `_today()` / `_date_str()` / `date = None` 测试注入代码
3. 与 query 层的 `TableSpec` / Repository 体系对称，通过共享 `catalog.py` 消除两层之间的路径和列名重复
4. `Pipeline` 重构为 Registry，消除 CLI 的 if-elif 链和 Scheduler 的手动枚举
5. 修复 Scheduler 漏排 4 张权益日线表（`daily_basic`、`stock_st`、`suspend_d`、`stk_limit`）
6. 清理 `storage.py` 遗留冗余函数

公共命令行接口（`python main.py sync --table <name>`）保持不变。

## 当前问题

### Bug

- **Bug 1**：`sync_fut_weekly_detail` 先拉 API 再检查本地存在性，重跑时浪费积分
- **Bug 2**：`sync_fut_weekly_detail` 数据已是最新时抛 `ValueError` 而非优雅返回，导致定时调度每天报错
- **Bug 3**：`sync_fut_index_daily` 遍历所有日历日（含周末节假日），向 Tushare 发大量无效请求

### 设计问题

- `_today()` / `_date_str()` / `date = None` 在 5 个模块各自重复，测试 patch 要打 5 处
- `skip_if_not_trading` / `ensure_trade_cal_loaded` 散落在 `_helpers.py`，没有明确归属
- `Pipeline` 有 25+ 个显式方法，每新增一张表要改 Pipeline、CLI、Scheduler 三处
- `storage.py` 混合了 `MetaStore` 类和一堆接受 `data_dir` 参数的独立函数
- `MetaStore` 同时管理 sync 元数据和交易日历查询，职责混杂
- Scheduler 硬编码所有表名和时间偏移，新增表必须改代码
- `Config` 用 `scheduler_daily_kline_hour` / `scheduler_daily_kline_minute` 分字段存时间，冗余

## 设计原则

- 用对象表达稳定的职责，不为用对象而用对象
- 简单表（日分区、快照）用配置驱动；复杂表（月度分区、双维分区、合并策略）用策略子类
- `SyncJob` 只描述"怎么同步"，不持有"何时跑"——调度是运维关注点
- 与 query 层平行，不强行合并（sync 需要 fetcher，query 不需要）

## 模块结构

```
zer0share/
├── dateutil.py           — 纯日期工具（扩充 date_str / parse_date，零依赖）
├── trading_calendar.py   — TradingCalendar（新建）
├── catalog.py            — 所有表的 TableSpec 常量（新建）
├── schema.py             — 列名常量（不变）
├── storage.py            — MetaStore + DailyPartitionStore
│                           + SnapshotStore + IndexWeightStore
├── pipeline.py           — Pipeline（Registry）
├── scheduler.py          — 纯配置驱动，无硬编码表名
├── config.py             — Config（schedule 改为 dict[str, str]）
├── cli.py                — if-elif 链消失
├── query/
│   ├── repository.py     — TableSpec / Repository（不变）
│   └── equities / futures / options / ...  — 从 catalog 导入 spec
└── sync/
    ├── __init__.py       — SyncRuntime dataclass
    ├── _jobs.py          — SyncJob ABC + DailySyncJob + SnapshotSyncJob
    └── equities / futures / options / calendar / industry.py
                          — build_jobs() + 自定义 Job 子类
```

## 核心类型

### dateutil.py（扩充）

新增两个公开函数，统一各处重复的私有实现：

```python
def date_str(value: str | date) -> str: ...   # 原各模块的 _date_str
def parse_date(s: str) -> date: ...            # 原各模块的 _parse
```

### TradingCalendar

```python
# zer0share/trading_calendar.py
class TradingCalendar:
    def __init__(self, meta: MetaStore, today_fn: Callable[[], str] = dateutil.today):
        self._meta = meta
        self._today_fn = today_fn

    def today(self) -> str:
        return self._today_fn()

    def is_trading_day(self, exchange: str, date: str) -> bool: ...
    def get_trading_days(self, exchange: str, start: str, end: str) -> list[str]: ...
    def skip_if_not_trading(self, exchange: str) -> bool: ...
    def ensure_loaded(self) -> None: ...
    def load_from_parquet(self, data_dir: Path, exchanges: list[str]) -> None: ...
```

`MetaStore` 中的 `load_trade_cal_from_parquet`、`get_trading_days`、`is_trading_day` 全部迁移至此。

测试注入今日日期：`TradingCalendar(meta, today_fn=lambda: "20240105")`，不再需要 patch 模块级 `date` 变量。

### catalog.py

所有表的 `TableSpec` 常量集中存放，query 层和 sync 层共同导入，消除路径和列名重复：

```python
# zer0share/catalog.py
from zer0share.query.repository import DailyTableSpec, TableSpec
from zer0share.schema import DAILY_COLS, ADJ_FACTOR_COLS, ...

DAILY_KLINE_SPEC = DailyTableSpec(
    name="daily_kline",
    path_parts=("daily_kline",),
    columns=DAILY_COLS,
    parquet_pattern="date=*/data.parquet",
    sync_table="daily_kline",
    order_by="ts_code, trade_date",
    hive_partitioning=True,
    union_by_name=True,
)

BASIC_SPEC = TableSpec(
    name="basic",
    path_parts=("basic",),
    columns=BASIC_COLS,
    parquet_pattern="data.parquet",
    sync_table="basic",
    order_by="ts_code",
)

FUT_DAILY_SPEC = DailyTableSpec(
    name="fut_daily",
    path_parts=("futures", "fut_daily"),
    ...
)

# 所有表依此类推
```

依赖方向：

```
         catalog.py（中立层）
           ↑          ↑
      query 层      sync 层
           ↑          ↑
         schema.py（列名）
```

### Storage 类

`storage.py` 拆分为一个精简的 `MetaStore` 和三种 Store 类：

**MetaStore**（只管 sync_meta 表）：

```python
class MetaStore:
    def get_last_date(self, table_name: str) -> str | None: ...
    def update_last_date(self, table_name: str, last_date: str) -> None: ...
    def close(self) -> None: ...
```

**DailyPartitionStore**（`date=YYYYMMDD/data.parquet` 结构）：

```python
class DailyPartitionStore:
    def __init__(self, table_dir: Path): ...
    def write(self, trade_date: str, df: DataFrame) -> None: ...
    def exists(self, trade_date: str) -> bool: ...
    def read(self, trade_date: str) -> DataFrame: ...
```

**SnapshotStore**（单文件快照：basic / opt_basic / sw_classify 等）：

```python
class SnapshotStore:
    def __init__(self, file_path: Path): ...
    def write(self, df: DataFrame) -> None: ...
    def read(self) -> DataFrame: ...
```

**IndexWeightStore**（双维分区：`index_code=XXX/date=YYYYMMDD/data.parquet`）：

```python
class IndexWeightStore:
    def __init__(self, index_weight_dir: Path): ...
    def write(self, index_code: str, trade_date: str, df: DataFrame) -> None: ...
    def exists(self, index_code: str, trade_date: str) -> bool: ...
```

删除遗留冗余函数：`write_daily_kline`、`daily_kline_partition_exists`、`read_daily_kline`、`write_adj_factor`、`adj_factor_partition_exists`。

### SyncRuntime

```python
# zer0share/sync/__init__.py
@dataclass
class SyncRuntime:
    calendar: TradingCalendar
    notifier: Notifier
    meta: MetaStore
```

不含 `data_dir`、`fetcher`、`cfg`。每个 Job 在构造时已绑定 store 和 fetch callable，runtime 只传运行时真正共享的三个依赖。

### SyncJob 层次结构

```python
# zer0share/sync/_jobs.py
class SyncJob(ABC):
    table_name: str
    supports_date_range: bool

    @abstractmethod
    def run(self, rt: SyncRuntime, start_date: str | None, end_date: str | None) -> None: ...
```

**DailySyncJob**（约 15 张日分区表）：

```python
@dataclass
class DailySyncJob(SyncJob):
    spec: DailyTableSpec
    fetch: Callable[[str], DataFrame]
    store: DailyPartitionStore
    write_empty: bool = False
    exchange: str = "SSE"
    supports_date_range: bool = True
```

**SnapshotSyncJob**（约 5 张快照表）：

```python
@dataclass
class SnapshotSyncJob(SyncJob):
    spec: TableSpec
    fetch: Callable[[], DataFrame]
    store: SnapshotStore
    skip_non_trading: bool = True
    supports_date_range: bool = False
```

**自定义子类**（复杂分区逻辑）：

```
TradeCalSyncJob        — 按交易所写 SnapshotStore，run() 末尾调 rt.calendar.load_from_parquet()
IndexWeightSyncJob     — 按月拉取，IndexWeightStore 双维分区（index_code + date）
IndexDailySyncJob      — 多 code 批量拉取后合并，DailyPartitionStore 按日写入
FutIndexDailySyncJob   — DailyPartitionStore，修复 Bug3：改用 get_trading_days 替代日历日遍历
FutWeeklyDetailSyncJob — DailyPartitionStore（key 为周一日期），修复 Bug1 + Bug2
```

### Pipeline

```python
# zer0share/pipeline.py
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

    def run(self, table_name: str, start_date=None, end_date=None) -> None:
        if table_name not in self._registry:
            raise ValueError(f"未知表: {table_name}")
        self._registry[table_name].run(self._runtime, start_date, end_date)

    def run_all(self, start_date=None, end_date=None) -> None:
        for job in self._registry.values():
            job.run(self._runtime, start_date, end_date)

    @property
    def registry(self) -> dict[str, SyncJob]:
        return self._registry
```

各域模块暴露 `build_jobs(cfg, fetcher) -> list[SyncJob]`，在此处绑定 fetch callable 和 store，之后不再流传 fetcher：

```python
# sync/equities.py
def build_jobs(cfg: Config, fetcher: TushareFetcher) -> list[SyncJob]:
    return [
        DailySyncJob(
            spec=DAILY_KLINE_SPEC,
            fetch=fetcher.fetch_daily_kline,
            store=DailyPartitionStore(cfg.data_dir / "daily_kline"),
        ),
        SnapshotSyncJob(
            spec=BASIC_SPEC,
            fetch=fetcher.fetch_basic,
            store=SnapshotStore(cfg.data_dir / "basic" / "data.parquet"),
        ),
        ...
    ]
```

### Config 简化

```python
@dataclass(frozen=True)
class Config:
    tushare_token: str
    data_dir: Path
    db_path: Path
    log_path: Path
    schedule: dict[str, str]   # table_name → "HH:MM"
    wecom_webhook_url: str
    notifier_enabled: bool
```

```toml
[scheduler]
trade_cal    = "09:00"
basic        = "09:10"
industry     = "09:20"
ci_member    = "09:30"
daily_kline  = "16:30"
index_daily  = "16:30"
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
fut_index_daily    = "19:30"
fut_weekly_detail  = "19:40"
opt_basic    = "19:50"
opt_daily    = "20:00"
```

### Scheduler 纯配置驱动

```python
def start_scheduler(config_path: str = "config/settings.toml") -> None:
    cfg = load_config(Path(config_path))
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
        scheduler.start()
```

### CLI 简化

```python
@cli.command()
@click.option("--table", type=click.Choice(list(SYNC_TABLES)), default=None)
@click.option("--all", "sync_all", is_flag=True, default=False)
@click.option("--start-date", default=None, callback=_validate_date)
@click.option("--end-date", default=None, callback=_validate_date)
def sync(table, sync_all, start_date, end_date):
    with _make_pipeline() as pipeline:
        if sync_all:
            pipeline.run_all(start_date=start_date, end_date=end_date)
        else:
            pipeline.run(table, start_date=start_date, end_date=end_date)
```

`SYNC_TABLES` 可从 registry 动态取，不再手动维护列表。

## 数据流

```
Pipeline.__init__(cfg, fetcher, notifier)
  ├─ MetaStore(cfg.db_path)
  ├─ TradingCalendar(meta)
  ├─ SyncRuntime(calendar, notifier, meta)
  └─ build_jobs(cfg, fetcher) 各域模块
       ├─ fetch callable 绑定至 Job（fetcher 不再流传）
       └─ Store 对象绑定至 Job（data_dir 不再流传）

pipeline.run("daily_kline", start_date, end_date)
  └─ DailySyncJob.run(runtime, start_date, end_date)
       ├─ runtime.calendar.get_trading_days(...)
       ├─ self.store.exists(trade_date)
       ├─ self.fetch(trade_date)
       ├─ self.store.write(trade_date, df)
       └─ runtime.meta.update_last_date(...)
```

## Bug 修复细节

### FutWeeklyDetailSyncJob（Bug 1 + Bug 2）

```python
def run(self, rt: SyncRuntime, start_date=None, end_date=None):
    today = rt.calendar.today()
    last = rt.meta.get_last_date(self.table_name)

    if start_date is None:
        start = dateutil.add_days(last, 1) if last else FIRST_DATE
        end = today
        if start > end:                          # Bug2：优雅返回
            logger.info(f"{self.table_name} 已是最新，无需同步")
            return
    else:
        start, end = start_date, end_date or today
        if start > end:
            raise ValueError("start_date must be on or before end_date")

    for week_num, week_start in dateutil.week_ranges(start, end):
        if self.store.exists(week_start):        # Bug1：先查存在
            skipped_existing += 1
            continue
        df = self.fetch(week_num)                # 再拉 API
        ...
```

### FutIndexDailySyncJob（Bug 3）

```python
def run(self, rt: SyncRuntime, start_date=None, end_date=None):
    ...
    trading_days = rt.calendar.get_trading_days("SSE", start, end)  # 仅交易日
    for trade_date in trading_days:
        df = self.fetch(trade_date)
        ...
```

## 非目标

- 不改变公共 CLI 接口（命令名、参数名不变）
- 不新增数据表
- 不改变存储布局（Parquet 分区结构不变）
- 不修改 query 层的 `repository.py`
- 不合并 query 和 sync 的注册表（职责不同）

## 迁移策略

分步实施，每步后运行完整测试套件：

1. 扩充 `dateutil.py`（新增 `date_str` / `parse_date`）
2. 新建 `trading_calendar.py`，迁移 `MetaStore` 中的日历方法
3. 新建 `catalog.py`，提升各域模块的 `TableSpec` 为模块级常量
4. 重构 `storage.py`：拆分三种 Store 类，删除遗留函数
5. 新建 `sync/_jobs.py`：`SyncJob` ABC + `DailySyncJob` + `SnapshotSyncJob`
6. 逐域迁移：`sync/equities.py` → `build_jobs()` + Job 对象
7. 迁移复杂表自定义子类（含 Bug 修复）
8. 重构 `Pipeline` 为 Registry
9. 简化 CLI 和 Scheduler
10. 简化 `Config`（schedule 改为 dict）
