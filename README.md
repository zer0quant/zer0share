# zer0share

![Python](https://img.shields.io/badge/python-3.11+-blue)
![License](https://img.shields.io/badge/license-MIT-green)
![Stars](https://img.shields.io/github/stars/zer0coldai/zer0share)

> **zer0share** — A local data pipeline for Chinese A-share market.  
> Pulls data from Tushare Pro, stores as Parquet partitions,  
> queries via DuckDB, with incremental sync & APScheduler automation.

> 我在公众号「极客投研笔记」记录这个项目的设计过程、踩坑记录和后续扩展。  
> 如果你对 AI + 量化投研、本地股票数据系统、因子研究工作流感兴趣，欢迎关注。

A-股数据本地化管道，基于 [Tushare Pro](https://tushare.pro) 拉取股票数据，以 Parquet 分区存储，DuckDB 提供快速元数据查询，支持增量同步与定时调度。

## 特性

- **核心数据同步**：支持交易日历、股票基础信息、日线行情、复权因子、每日指标、ST、停复牌、涨跌停价格、指数成分、行业映射
- **本地优先存储**：Parquet 分区文件 + DuckDB 元数据，无需数据库服务
- **Tushare-like 查询**：本地 `pro_api()` 直接返回 DataFrame，不消耗 Tushare 积分
- **复权行情**：本地 `pro_bar()` 支持不复权、前复权（qfq）和后复权（hfq）
- **股票池构建**：支持研究基础池、交易基础池、沪深300/中证500/中证1000交易池
- **自动化运维**：APScheduler 定时同步，支持企业微信失败告警

## 环境要求

- Python 3.11+
- [uv](https://github.com/astral-sh/uv)
- Tushare Pro Token（基础行情需积分 ≥ 2000；`stock_st` 需积分 ≥ 3000；中信行业成分需积分 ≥ 5000）

## 快速开始

### 1. 克隆并安装依赖

```bash
git clone https://github.com/your-username/zer0share.git
cd zer0share
uv sync
```

### 2. 配置

```bash
cp config/settings.example.toml config/settings.toml
```

编辑 `config/settings.toml`，填入 Tushare Token：

```toml
[tushare]
token = "your_tushare_token_here"
```

### 3. 首次同步

```bash
# 一键同步全部
uv run python main.py sync --all

# 或逐步执行（顺序不可颠倒）
uv run python main.py sync --table trade_cal    # 交易日历（必须最先）
uv run python main.py sync --table basic        # 股票基础信息
uv run python main.py sync --table daily_kline  # 日线行情（依赖交易日历）
uv run python main.py sync --table adj_factor   # 复权因子（依赖交易日历）
uv run python main.py sync --table daily_basic  # 每日指标：总市值、流通市值等
uv run python main.py sync --table stock_st     # 每日 ST 股票列表
uv run python main.py sync --table suspend_d    # 每日停牌列表
uv run python main.py sync --table stk_limit    # 每日涨跌停价格
uv run python main.py sync --table index_weight # 沪深300/中证500/中证1000成分
uv run python main.py sync --table index_daily  # 宽基指数日线行情
uv run python main.py sync --table industry    # 申万行业分类 + 成分映射
uv run python main.py sync --table ci_member   # 中信行业成分映射
```

首次验证建议先同步一个小区间，确认 Tushare 权限和字段可用后再全量回填：

```bash
uv run python main.py sync --table daily_basic --start-date 2024-01-01 --end-date 2024-01-31
uv run python main.py sync --table stock_st --start-date 2024-01-01 --end-date 2024-01-31
uv run python main.py sync --table suspend_d --start-date 2024-01-01 --end-date 2024-01-31
uv run python main.py sync --table stk_limit --start-date 2024-01-01 --end-date 2024-01-31
uv run python main.py sync --table index_weight --start-date 2024-01-01 --end-date 2024-01-31
```

### 4. 构建股票池

同步完对应交易日的数据后，可以构建股票池。默认按交易日从 2016-01-01 构建到今天，并跳过已存在的完整分区：

```bash
uv run python main.py build-universe
```

也可以指定区间或构建单日：

```bash
uv run python main.py build-universe --start-date 2024-01-01 --end-date 2024-01-31
uv run python main.py build-universe --date 2024-01-31
```

生成的股票池包括：

| 股票池 | 说明 |
|------|------|
| `univ_research_base` | 基础研究池，用于中性化、标准化、因子横截面分析 |
| `univ_trade_base` | 基础交易池，用于全 A 候选选股 |
| `univ_trade_hs300` | 沪深300成分中满足交易过滤条件的股票池 |
| `univ_trade_zz500` | 中证500成分中满足交易过滤条件的股票池 |
| `univ_trade_zz1000` | 中证1000成分中满足交易过滤条件的股票池 |

`univ_research_base` 过滤规则：

- A 股普通股票
- 当前交易日已上市、未退市
- 非 ST / 非 *ST
- 上市满 6 个月
- 过去 20 个交易日日均成交额 >= 1000 万元
- 总市值排名不在全市场最后 2%

`univ_trade_base` 在 `univ_research_base` 基础上继续过滤：

- 当前交易日非停牌
- 当前交易日非一字涨停
- 当前交易日非一字跌停
- 总市值排名不在全市场最后 5%

### 5. 查看同步状态

```bash
uv run python main.py status
```

### 6. 启动定时调度

```bash
uv run python main.py scheduler start
```

## 本地查询 API

同步完成后，可以在研究代码中使用类似 Tushare Pro 的本地 Python API 查询数据。查询只读取本地 Parquet 文件，通过 DuckDB 执行，不会访问 Tushare，也不会消耗积分。

```python
from zer0share import pro_api

pro = pro_api()

basic = pro.stock_basic(list_status="L")
cal = pro.trade_cal(exchange="SSE", start_date="20240101", end_date="20240131")
daily = pro.daily(ts_code="000001.SZ", start_date="20240101", end_date="20240331")
adj = pro.adj_factor(ts_code="000001.SZ", start_date="20240101", end_date="20240331")
daily_basic = pro.daily_basic(trade_date="20240131", fields="ts_code,trade_date,total_mv")
st = pro.stock_st(trade_date="20240131")
suspend = pro.suspend_d(trade_date="20240131")
limit = pro.stk_limit(trade_date="20240131")
hs300 = pro.index_weight(index_code="399300.SZ", start_date="20240101", end_date="20240131")

# 指数日线行情（用于对冲基准收益率）
idx_daily = pro.index_daily(ts_code="000300.SH", start_date="20240101", end_date="20240131")

# 行业数据（用于行业中性化）
sw_industries = pro.index_classify(level="L1", src="SW2021")       # 申万一级行业列表
sw_member = pro.index_member_all(ts_code="000001.SZ", is_new="Y")  # 查股票所属申万行业
ci_member = pro.ci_index_member(ts_code="000001.SZ", is_new="Y")   # 查股票所属中信行业

qfq = pro.pro_bar(
    ts_code="000001.SZ",
    start_date="20240101",
    end_date="20240331",
    adj="qfq",
)
```

支持的本地查询方法：

| 方法 | 说明 |
|------|------|
| `stock_basic` | 查询已同步的股票基础信息 |
| `trade_cal` | 查询已同步的交易日历 |
| `daily` | 查询已同步的 A 股日线行情 |
| `adj_factor` | 查询已同步的复权因子 |
| `daily_basic` | 查询已同步的每日指标 |
| `stock_st` | 查询已同步的每日 ST 股票列表 |
| `suspend_d` | 查询已同步的每日停复牌信息 |
| `stk_limit` | 查询已同步的每日涨跌停价格 |
| `index_weight` | 查询已同步的指数成分和权重 |
| `index_daily` | 查询已同步的宽基指数日线行情（12个指数） |
| `index_classify` | 查询申万行业分类树（L1/L2/L3） |
| `index_member_all` | 查询申万股票-行业映射（支持历史变更） |
| `ci_index_member` | 查询中信股票-行业映射（支持历史变更） |
| `pro_bar` | 查询本地 A 股日线行情，支持不复权、前复权（qfq）和后复权（hfq） |
| `query` | 按接口名分发，例如 `pro.query("daily", ...)` |

运行示例：

```bash
uv run python examples/local_query_api_smoke.py
```

## 数据存储结构

```
data/
├── trade_cal/
│   ├── exchange=SSE/data.parquet
│   └── exchange=SZSE/data.parquet
├── basic/
│   └── data.parquet
├── daily_kline/
│   ├── date=20160104/data.parquet
│   ├── date=20160105/data.parquet
│   └── ...
├── adj_factor/
│   ├── date=20160104/data.parquet
│   ├── date=20160105/data.parquet
│   └── ...
├── daily_basic/
│   └── date=20160104/data.parquet
├── stock_st/
│   └── date=20160104/data.parquet
├── suspend_d/
│   └── date=20160104/data.parquet
├── stk_limit/
│   └── date=20160104/data.parquet
├── index_weight/
│   ├── index_code=399300.SZ/date=20160104/data.parquet
│   ├── index_code=000905.SH/date=20160104/data.parquet
│   └── index_code=000852.SH/date=20160104/data.parquet
├── index_daily/
│   ├── date=20160104/data.parquet   # 含当日全部12个宽基指数
│   ├── date=20160105/data.parquet
│   └── ...
├── industry/
│   ├── sw_classify/data.parquet       # 申万行业分类树
│   ├── sw_member/data.parquet         # 申万股票-行业映射（全量历史）
│   └── ci_member/data.parquet         # 中信股票-行业映射（全量历史）
└── universe/
    ├── name=univ_research_base/date=20240131/data.parquet
    ├── name=univ_trade_base/date=20240131/data.parquet
    ├── name=univ_trade_hs300/date=20240131/data.parquet
    ├── name=univ_trade_zz500/date=20240131/data.parquet
    └── name=univ_trade_zz1000/date=20240131/data.parquet
db/
└── meta.duckdb                      # 同步记录 + 交易日历索引
```

## CLI 命令

| 命令 | 说明 |
|------|------|
| `sync --table trade_cal` | 增量同步交易日历（SSE、SZSE） |
| `sync --table basic` | 同步股票基础信息 |
| `sync --table daily_kline` | 增量同步日线行情 |
| `sync --table adj_factor` | 增量同步复权因子 |
| `sync --table daily_basic` | 增量同步每日指标 |
| `sync --table stock_st` | 增量同步每日 ST 股票列表 |
| `sync --table suspend_d` | 增量同步每日停复牌信息 |
| `sync --table stk_limit` | 增量同步每日涨跌停价格 |
| `sync --table index_weight` | 增量同步指数成分和权重 |
| `sync --table index_daily` | 增量同步12个宽基指数日线行情 |
| `sync --table industry` | 同步申万行业分类 + 成分映射（全量覆盖） |
| `sync --table ci_member` | 同步中信行业成分映射（全量覆盖） |
| `sync --all` | 按顺序同步全部 |
| `build-universe` | 从 2016-01-01 到今天增量构建 5 个股票池 |
| `build-universe --start-date YYYY-MM-DD --end-date YYYY-MM-DD` | 构建指定区间的 5 个股票池 |
| `build-universe --date YYYY-MM-DD` | 构建指定交易日的 5 个股票池 |
| `status` | 查看各表最后同步时间 |
| `scheduler start` | 启动定时调度 |

## 配置说明

```toml
[tushare]
token = "your_tushare_token_here"

[paths]
data_dir = "data"          # Parquet 存储目录
db_path = "db/meta.duckdb" # DuckDB 文件路径
log_path = "logs/pipeline.log"

[scheduler]
daily_kline_hour = 18      # 日线同步触发时间（小时）
daily_kline_minute = 0
basic_hour = 8             # 基础信息同步触发时间（小时）
adj_factor_hour = 18       # 复权因子同步触发时间（小时）
adj_factor_minute = 5

[notifier]
wecom_webhook_url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=YOUR_KEY"
enabled = false            # 填写真实 webhook_url 后改为 true
```

## 开发

```bash
# 安装开发依赖
uv sync --dev

# 运行测试
uv run pytest

# 运行单个测试文件
uv run pytest tests/test_pipeline.py -v
```

## 项目结构

```
src/
├── config.py     # 配置加载
├── api.py        # 本地 Tushare-like 查询 API
├── fetcher.py    # Tushare API 封装
├── storage.py    # Parquet 读写 + DuckDB MetaStore
├── pipeline.py   # 同步业务逻辑
├── scheduler.py  # APScheduler 定时任务
├── notifier.py   # 企业微信 Webhook 通知
└── cli.py        # Click CLI 入口
tests/            # pytest 测试套件
examples/         # 本地查询 API 示例
config/
└── settings.example.toml
```

## 交流

公众号：极客投研笔记

如果你对本地股票数据系统、AI 量化投研或因子研究工作流感兴趣，可以关注公众号看后续更新。

## License

MIT
