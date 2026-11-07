# RiceQuant 数据源接入设计

## 目标

新增 RiceQuant 作为独立数据源 adapter，并优先接入 A 股基础信息与 1 分钟行情数据。设计必须满足：

1. RiceQuant 与 Tushare 在拉取层解耦，认证、代码体系、字段语义不混在同一个 fetcher 中。
2. RiceQuant 本地查询层独立，不挂到现有 `pro_api()` 的 Tushare-like 接口上。
3. 分钟线保留 RiceQuant 返回的字段形态，不补充 Tushare 风格字段，不做跨数据源字段转换。
4. 继续复用现有 Parquet + DuckDB 本地存储模型和 sync meta 机制。
5. 分钟线同步使用 RiceQuant 自己的基础信息表作为股票列表来源，不依赖 Tushare `stock/basic`。
6. 为后续 RiceQuant tick、指数分钟线、ETF 分钟线、期货分钟线留出自然扩展路径。

## 背景

当前仓库的同步链路以 Tushare 为唯一上游：

- `zer0share/fetcher.py` 封装 Tushare Pro API。
- `zer0share/sync/*.py` 注册同步作业。
- `zer0share/storage.py` 负责 Parquet 分区写入和 DuckDB meta。
- `zer0share/api.py` 暴露 `pro_api()`，本地查询接口刻意模拟 Tushare Pro。

RiceQuant RQData 的分钟行情接口是 `rqdatac.get_price(..., frequency="1m")`。官方文档说明该接口支持周线、日线、分钟线和 tick 数据；大量分钟或 tick 数据建议按单只合约长区间拉取；返回的 bar 数据字段包含 `open`、`close`、`high`、`low`、`limit_up`、`limit_down`、`total_turnover`、`volume`、`num_trades`、`prev_close` 等。RQData 需要安装 `rqdatac` 并在首次调用 API 前执行 `rqdatac.init()`。

RiceQuant 基础信息接口使用 `all_instruments(type=None, date=None, market='cn')`。其中 `type='CS'` 代表股票，返回 pandas DataFrame，字段以 RiceQuant 实际返回为准。`instruments(order_book_ids, market='cn')` 可查询单个或多个合约的详细对象；第一期先用 `all_instruments(type='CS', market='cn')` 做股票基础表。

RiceQuant 初始化需要兼容两种认证形态：

- 用户名/密码：`rqdatac.init(username="<user>", password="<password>")`
- License key：`rqdatac.init(username="license", password="<license_key>")`

这些约束说明 RiceQuant 不应伪装成 Tushare 的表，也不应直接塞进 `LocalPro`。

## 设计原则

- **数据源是 adapter**：每个上游供应商有自己的 adapter，封装认证和调用方式；数据落盘保持供应商自己的字段语义。
- **查询语义跟随数据源**：Tushare-like 数据继续走 `pro_api()`；RiceQuant-like 数据走新增 `rq_api()`。
- **存储可以共享，接口不共享**：RiceQuant 可以复用 Parquet、DuckDB 和 meta，但查询入口与表命名独立。
- **保留原始字段优先**：RiceQuant 返回新增字段时，存储层不能因为本地 schema 未列出而丢弃，也不能额外写入 Tushare 风格派生字段。
- **先做窄而完整的垂直切片**：第一期只做 A 股 1 分钟行情，不同时设计 tick 和实时推送。

## 模块结构

新增和调整后的结构：

```text
zer0share/
├── sources/
│   ├── __init__.py
│   ├── tushare.py          # 从 fetcher.py 迁入 TushareFetcher
│   └── ricequant.py        # RiceQuantFetcher
├── sync/
│   ├── stock.py            # Tushare 股票日频等
│   ├── futures.py
│   ├── options.py
│   └── ricequant.py        # RiceQuant 基础信息和分钟线同步作业
├── query/
│   ├── stock.py            # 现有 Tushare-like 查询
│   ├── futures.py
│   ├── options.py
│   └── ricequant.py        # RiceQuant-like 本地查询
├── api.py                  # pro_api() 保持 Tushare-like
└── rq_api.py               # 新增 rq_api()
```

`zer0share/fetcher.py` 可作为兼容 shim 保留一段时间：

```python
from zer0share.sources.tushare import TushareFetcher
```

这样现有测试和外部导入不会在第一期被迫迁移。

## 配置

`config/settings.toml` 新增 RiceQuant 配置：

```toml
[ricequant]
enabled = false
username = ""
password = ""
license_key = ""

[ricequant.stock_minute]
request_sleep_seconds = 0.2
adjust_type = "none"
skip_suspended = true

[scheduler]
ricequant_stock_minute = "16:30"
```

依赖新增：

```toml
"rqdatac>=3.0"
```

版本下限在实现时以当前可安装版本和 RiceQuant 官方要求为准。若用户环境通过 RiceQuant SDK 自带 `rqdatac`，实现也应支持可选依赖导入失败时给出明确错误。

## 数据源 Adapter

### DataSources

Pipeline 不再只接收一个 `TushareFetcher`，而是接收数据源容器：

```python
@dataclass
class DataSources:
    tushare: TushareFetcher
    ricequant: RiceQuantFetcher | None
```

构造规则：

- Tushare 保持必需，兼容现有功能。
- RiceQuant 只有在 `[ricequant].enabled = true` 时初始化。
- RiceQuant 认证支持 `license_key` 或 `username/password` 二选一；两者同时配置时报错，启用但两者都缺失时报错。
- `[ricequant]` 只放数据源级配置；分钟线限速、复权和停牌补齐策略放在 `[ricequant.stock_minute]`。
- 如果用户同步 RiceQuant 表但 RiceQuant 未启用，报明确配置错误。

### RiceQuantFetcher

接口职责：

```python
class RiceQuantFetcher:
    def __init__(
        self,
        username: str = "",
        password: str = "",
        license_key: str = "",
    ):
        ...

    def fetch_stock_minute(
        self,
        order_book_id: str,
        start_date: str,
        end_date: str,
        adjust_type: str = "none",
        skip_suspended: bool = True,
    ) -> pd.DataFrame:
        ...
```

实现要求：

- 内部导入 `rqdatac`，避免没有安装时影响 Tushare 用户。
- 如果传入 `license_key`，初始化时调用 `rqdatac.init(username="license", password=license_key)`。
- 如果传入用户名/密码，初始化时调用 `rqdatac.init(username=username, password=password)`。
- 如果认证参数为空或同时提供 license key 与用户名/密码，抛 `ValueError`。
- 使用 `rqdatac.get_price(..., frequency="1m", fields=None, expect_df=True)`。
- 对返回的 MultiIndex 执行 `reset_index()`，保留 `order_book_id` 和 `datetime`。
- 补充 `trade_date`，格式为 `YYYYMMDD`。
- 不补充 `ts_code`，不把 `000001.XSHE` 转为 `000001.SZ`。RiceQuant 数据表只保存 RiceQuant 代码体系。
- 不使用 `_select_columns_or_empty()`，不截断 RiceQuant 返回字段。

## 同步设计

### 表名

同步表名使用数据源前缀：

```text
ricequant_basic
ricequant_stock_minute
```

这样 `sync_meta` 中不会和 Tushare 表混淆，也方便未来新增：

```text
ricequant_index_minute
ricequant_etf_minute
ricequant_stock_tick
```

CLI：

```bash
uv run python main.py sync --table ricequant_stock_minute --start-date 20240102 --end-date 20240131
```

### 存储路径

RiceQuant 新表进入独立目录：

```text
data/
  ricequant/
    basic/
      data.parquet
    stock_minute/
      date=YYYYMMDD/
        data.parquet
```

不迁移现有 Tushare 数据目录。已有 `data/stock`、`data/index`、`data/futures`、`data/options` 保持不变。

### 基础信息字段

`ricequant_basic` 通过 `rqdatac.all_instruments(type="CS", market="cn")` 拉取并写入单文件快照：

```text
data/ricequant/basic/data.parquet
```

存储层保留 RiceQuant 返回的所有列，不定义固定列集合，不把字段改名为 Tushare 风格。分钟线同步从该表读取 `order_book_id` 作为请求列表。

### 分钟线字段

存储层保留 RiceQuant 返回的所有字段。第一期已知核心字段：

```text
order_book_id
datetime
open
close
high
low
limit_up
limit_down
total_turnover
volume
num_trades
prev_close
```

本地派生字段只允许：

```text
trade_date   # 从 datetime 得出，仅用于按交易日分区和过滤
```

不写入 `ts_code` 等 Tushare 风格字段。若研究代码需要跨数据源映射，应在研究层显式 join 映射表，而不是污染 RiceQuant 原始行情表。

### 拉取策略

RiceQuant 文档建议大量分钟数据按单只合约拉取。同步作业采用：

1. 按交易日推进，复用现有 `TradingCalendar` 和 `DailyPartitionStore`。
2. 每个交易日从 `data/ricequant/basic/data.parquet` 读取 `order_book_id` 列作为股票池。
3. 对每只股票调用 `fetch_stock_minute(order_book_id, trade_date, trade_date)`。
4. 合并所有非空 DataFrame。
5. 写入 `data/ricequant/stock_minute/date=YYYYMMDD/data.parquet`。
6. 更新 `sync_meta.ricequant_stock_minute`。

股票池来源第一期使用本地 `ricequant/basic/data.parquet`：

- 默认读取 `all_instruments(type="CS", market="cn")` 返回的全部股票基础信息。
- 如果返回中存在 `status` 字段，可优先使用 `status == "Active"` 的合约作为分钟线请求列表；若没有该字段，则使用所有 `order_book_id`。
- 后续可扩展为按 `listed_date`、`de_listed_date` 或 `all_instruments(date=...)` 做历史日期可交易过滤。

错误策略：

- 单只股票请求失败时记录错误并继续下一只。
- 当日失败列表写入日志和通知。
- 如果当日所有股票都失败，作业失败，不写空分区，不推进 meta。
- 如果部分失败但有成功数据，写成功部分并通知失败列表。下一期可增加失败重试清单。

## 本地查询层

新增入口：

```python
from zer0share import rq_api

rq = rq_api()
basic = rq.all_instruments(type="CS", market="cn")
df = rq.get_price(
    order_book_ids="000001.XSHE",
    start_date="20240102",
    end_date="20240102",
    frequency="1m",
)
```

`pro_api()` 不新增 `stock_minute()`，也不分发 RiceQuant 表。

### RQLocal

新增 `zer0share/rq_api.py`：

```python
class RQLocal:
    def __init__(self, data_dir):
        self._ctx = QueryContext(Path(data_dir))

    def get_price(
        self,
        order_book_ids,
        start_date=None,
        end_date=None,
        frequency="1m",
        fields=None,
        adjust_type="none",
        skip_suspended=None,
        expect_df=True,
        time_slice=None,
        market="cn",
        limit=None,
        offset=None,
    ) -> pd.DataFrame:
        ...

    def all_instruments(
        self,
        type=None,
        date=None,
        market="cn",
        fields=None,
        limit=None,
        offset=None,
    ) -> pd.DataFrame:
        ...
```

第一期支持范围：

- `frequency="1m"`。
- `market="cn"`。
- `expect_df=True`。
- `order_book_ids` 支持单个字符串或字符串列表。
- `fields` 支持 RiceQuant 字段名及 `trade_date`。
- `start_date` / `end_date` 支持 `YYYYMMDD` 字符串。
- `all_instruments(type="CS", market="cn")` 读取本地 `ricequant_basic` 快照；第一期不支持 `date` 参数的历史可交易过滤。

第一期明确不支持：

- `frequency="tick"`、`"1d"`、`"5m"` 等其他频率。
- `adjust_type` 现场复权计算。参数只用于校验本地数据是否匹配，默认 `none`。
- `time_slice`。后续可基于 `datetime` 增加。
- `expect_df=False` 的 RiceQuant 原生返回结构。

不支持功能必须抛明确的 `NotImplementedError` 或 `ValueError`，不能静默忽略。

### Query Repository

新增 `zer0share/query/ricequant.py`，内部使用现有 `BaseParquetRepository` / `DailyPartitionRepository` 模式，但 spec 独立：

```python
RICEQUANT_STOCK_MINUTE_SPEC = DailyTableSpec(
    name="ricequant_stock_minute",
    path_parts=("ricequant", "stock_minute"),
    columns=RICEQUANT_STOCK_MINUTE_COLS,
    parquet_pattern="date=*/data.parquet",
    sync_table="ricequant_stock_minute",
    order_by="order_book_id, datetime",
    hive_partitioning=True,
    union_by_name=True,
    date_column="trade_date",
    code_column="order_book_id",
)
```

因为字段需要“保留所有”，query 层不能只依赖固定 `columns` 拒绝新字段。第一期有两个可选实现：

1. 查询前用 DuckDB/Arrow 读取 Parquet schema，动态确定可选字段。
2. `fields=None` 时使用 `SELECT *`，仅当用户传 `fields` 时校验字段是否存在。

推荐实现 2，改动更小，且符合 RiceQuant 原字段保留目标。

## API 边界

最终对外边界：

```python
from zer0share import pro_api, rq_api

pro = pro_api()
pro.daily(...)

rq = rq_api()
rq.get_price(..., frequency="1m")
```

`pro_api()`：

- Tushare-like。
- 只查询 Tushare 同步数据。
- 不知道 RiceQuant。

`rq_api()`：

- RiceQuant-like。
- 只查询 `data/ricequant/...`。
- 参数尽量贴近 `rqdatac.get_price`。

## 测试计划

### Unit Tests

- `Config` 能解析 `[ricequant]`，默认禁用。
- RiceQuant 未安装时，只有初始化 RiceQuant 数据源才报错。
- `RiceQuantFetcher` 能调用 `all_instruments(type="CS", market="cn")` 并保留所有返回列。
- `ricequant_basic` 同步作业写入 `data/ricequant/basic/data.parquet`。
- `RiceQuantFetcher` 能把 MultiIndex 返回值转为普通列，并补充 `trade_date`，但不补充 `ts_code`。
- `ricequant_stock_minute` 同步作业写入 `data/ricequant/stock_minute/date=YYYYMMDD/data.parquet`。
- 单只股票失败时继续同步并记录失败；全失败时不推进 meta。
- `rq_api().get_price()` 能按 `order_book_ids` 和日期范围过滤。
- `rq_api().all_instruments()` 能按 `type`、`market` 和 `fields` 查询本地 RiceQuant 基础信息。
- `rq_api().get_price(fields=...)` 能选择 RiceQuant 原字段和 `trade_date`。
- `pro_api().query("ricequant_stock_minute")` 报 unknown api。

### Smoke Test

需要真实 RiceQuant 账号时运行：

```bash
uv run python main.py sync --table ricequant_stock_minute --start-date 20240102 --end-date 20240102
uv run python -c "from zer0share import rq_api; print(rq_api().get_price('000001.XSHE', start_date='20240102', end_date='20240102', frequency='1m').head())"
```

## 文档更新

实现时需要同步更新：

- `README.md`：新增 RiceQuant 数据源配置、license key/用户名密码认证、同步命令、本地查询示例。
- `docs/SYNC_GUIDE.md`：新增 `ricequant_stock_minute` 调度说明和账号前置条件。
- `skills/zer0share-data/references/api.md`：补充 `rq_api()` 的本地查询入口说明。
- `config/settings.example.toml`：新增 `[ricequant]` 示例配置。

## 非目标

第一期不做：

- RiceQuant tick 数据。
- 实时 websocket 推送。
- 自动分钟复权重算。
- Tushare 现有目录迁移。
- 对 `pro_bar(freq="1min")` 的兼容。
- 任意频率分钟线合成，如 `5m`、`15m`。

这些功能应在 `rq_api()` 和 `sync/ricequant.py` 的边界内后续扩展。
