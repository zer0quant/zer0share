# zer0share

![Python](https://img.shields.io/badge/python-3.11+-blue)
![License](https://img.shields.io/badge/license-MIT-green)
![Stars](https://img.shields.io/github/stars/zer0quant/zer0share)

A 股、ETF、期货、期权数据本地化管道：基于 [Tushare Pro](https://tushare.pro) 拉取数据，以 Parquet 分区存储，DuckDB 提供快速本地查询，支持增量同步与定时调度。

它和 [zer0factor](https://github.com/zer0quant/zer0factor) 是配套关系：

- `zer0share`：负责本地 A 股数据采集、同步和查询
- `zer0factor`：负责因子规范、因子生成、预处理、评估和存储

## 为什么用 zer0share？

直接调 Tushare Pro 做研究有两个痛点：每次查询消耗积分，批量回测时 API 限速拖慢迭代。zer0share 把数据落到本地，查询走 DuckDB，既省积分又快。

| | 直接调 Tushare Pro | zer0share 本地查询 |
|---|---|---|
| 积分消耗 | 每次请求消耗 | 零消耗 |
| 网络依赖 | 必须联网 | 离线可用 |
| 查询速度 | 受 API 限速 | DuckDB 本地毫秒级 |
| 数据一致性 | 取决于调用时机 | 快照固定，结果可复现 |

## 特性

- **A 股数据**：交易日历、基础信息、日线、复权因子、每日指标、ST、停复牌、涨跌停、股东增减持、指数成分、申万/中信行业、申万行业指数日线
- **ETF 数据**：ETF 基础信息、日线行情、复权因子、份额规模、持仓组合
- **期货 & 期权数据**：期货合约、日线、持仓排名、仓单、结算、主连映射；期权合约与日线行情
- **分钟线（可选）**：接入 RiceQuant 数据源同步股票 / ETF 分钟线
- **新闻（可选）**：`major_news` 长篇通讯、`news` 快讯，存入 SQLite（去重、断点续跑、标题中文全文检索），可用单独的 Tushare 账号
- **本地优先存储**：Parquet 分区文件 + DuckDB 元数据，无需数据库服务
- **Tushare-like 查询**：本地 `pro_api()` 直接返回 DataFrame，不消耗 Tushare 积分
- **复权行情**：本地 `pro_bar()` 支持不复权、前复权（qfq）和后复权（hfq）
- **股票池构建**：研究基础池、交易基础池、沪深300/中证500/中证1000等交易池
- **数据质检**：本地表级质量检查，生成详细报告
- **自动化运维**：APScheduler 定时同步，支持企业微信失败告警

## 安装

前置要求：

- Python 3.11+ 和 [uv](https://github.com/astral-sh/uv)
- Tushare Pro Token（基础行情需积分 ≥ 2000，部分专题表要求更高，详见[同步指南](docs/SYNC_GUIDE.md)）

```bash
git clone https://github.com/zer0quant/zer0share.git
cd zer0share
uv sync
```

## 配置

```bash
cp config/settings.example.toml config/settings.toml
```

```toml
[tushare]
token = "your_tushare_token_here"
# http_url = ""  # 可选，Tushare 代理接口地址（留空使用官方接口）

[paths]
data_dir = "data"          # Parquet 存储目录
db_path = "db/meta.duckdb" # DuckDB 文件路径
log_path = "logs/pipeline.log"

[scheduler]
# 每个表单独配置触发时间（HH:MM），完整示例见 config/settings.example.toml

[notifier]
wecom_webhook_url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=YOUR_KEY"
enabled = false            # 填写真实 webhook_url 后改为 true
```

## 快速开始

```bash
# 1. 首次同步（建议先用小区间验证 Tushare 权限，再全量回填，详见同步指南）
uv run python main.py sync --all

# 2. 查看各表同步状态
uv run python main.py status

# 3. 构建股票池（默认从 2016-01-01 增量构建到今天）
uv run python main.py build-universe

# 4. 本地查询，不消耗积分
uv run python -c "
from zer0share import pro_api
pro = pro_api()
print(pro.daily(ts_code='000001.SZ', start_date='20240101', end_date='20240131'))
"
```

同步顺序、逐表命令和定时调度部署见[同步指南](docs/SYNC_GUIDE.md)。

## 数据覆盖（63 张表）

`sync` 覆盖 Tushare 全部 63 张表，按业务域分 12 组。RiceQuant 分钟线 4 张为可选数据源，默认不启用。

| 分组 | 张数 | 表 |
|------|-----|-----|
| `quote` 行情 | 10 | `daily_kline` 日线、`weekly` 周线、`monthly` 月线、`adj_factor` 复权因子、`daily_basic` 每日指标、`stk_limit` 涨跌停价、`suspend_d` 停牌、`stock_st` ST 名单、`share_float` 限售解禁、`block_trade` 大宗交易 |
| `fundflow` 资金流 | 4 | `moneyflow` 个股资金流、`margin` 融资融券汇总、`margin_detail` 融资融券明细、`hk_hold` 沪深港通持股 |
| `toplist` 龙虎榜 | 2 | `top_list` 龙虎榜明细、`top_inst` 龙虎榜营业部 |
| `finance` 财务 | 12 | `income` 利润表、`balancesheet` 资产负债表、`cashflow` 现金流量表、`fina_indicator` 财务指标、`dividend` 分红送股、`forecast` 业绩预告、`stk_holdernumber` 股东人数、`top10_holders` 十大股东、`top10_floatholders` 十大流通股东、`stk_managers` 管理层、`stk_rewards` 管理层薪酬、`stk_holdertrade` 股东增减持 |
| `index` 指数与行业 | 7 | `index_daily` 指数行情、`index_weight` 指数成分权重、`index_dailybasic` 指数每日指标、`sw_daily` 申万行情、`idx_anns` 指数公告、`industry` 行业分类、`ci_member` 中信行业成分 |
| `bond` 可转债 | 2 | `cb_basic` 可转债基础信息、`cb_daily` 可转债行情 |
| `fund` 基金与 ETF | 8 | `fund_portfolio` 基金持仓、`fund_share` 基金份额、`fund_daily` 基金行情、`fund_adj` 基金复权因子、`etf_basic` ETF 基础信息、`etf_index` ETF 指数、`etf_share_size` ETF 份额规模、`etf_sh_cons` ETF 成分股 |
| `macro` 宏观 | 3 | `shibor` Shibor 利率、`repurchase` 逆回购利率、`repo_daily` 债券回购日行情 |
| `futures` 期货 | 11 | `fut_basic` 期货基础信息、`fut_daily` 期货日行情、`fut_holding` 期货持仓、`fut_wsr` 期货仓单、`fut_settle` 期货结算、`fut_mapping` 期货映射、`ft_limit` 期货涨跌停板、`fut_weekly` 期货周行情、`fut_monthly` 期货月行情、`fut_index_daily` 期货指数日行情、`fut_weekly_detail` 期货周度明细 |
| `options` 期权 | 2 | `opt_basic` 期权基础信息、`opt_daily` 期权日行情 |
| `basic` 基础 | 2 | `basic` 股票列表、`trade_cal` 交易日历 |

按分组同步：

```bash
python -m zer0share sync --topic finance      # 财务 12 张
python -m zer0share sync --topic futures      # 期货 11 张
python -m zer0share sync --tables income,daily_kline   # 指定多张
python -m zer0share sync --all               # 全部 63 张
```

`trade_cal`（交易日历）是全部逐日同步的基准，需先于其它表同步。

## 增量刷新（`zer0share sync`）

```bash
# 默认增量：只补今天之前的缺口
python -m zer0share sync --all
# 只刷两张表到指定日期
python -m zer0share sync --tables daily_kline,moneyflow --end-date 20261016
# 只看计划，不联网、不写盘
python -m zer0share sync --dry-run --tables daily_kline,moneyflow,index_weight
```

同步是幂等的：已存在的分区自动跳过，中断后重跑只补缺口。批量取数表
（财务、宏观等）按 50 天 / 50 万行一批流式落盘，长历史回溯中断最多损失一批。

## 本地查询 API

同步完成后，用类似 Tushare Pro 的接口查询本地数据：

```python
from zer0share import pro_api

pro = pro_api()

daily = pro.daily(ts_code="000001.SZ", start_date="20240101", end_date="20240331")
qfq = pro.pro_bar(ts_code="000001.SZ", start_date="20240101", end_date="20240331", adj="qfq")
hs300 = pro.index_weight(index_code="399300.SZ", start_date="20240101", end_date="20240131")
pool = pro.universe("univ_trade_hs300", trade_date="20240131")
fut_bar = pro.fut_daily(ts_code="RB2410.SHFE", start_date="20240101", end_date="20240331")
opt_bar = pro.opt_daily(ts_code="10004462.SH", start_date="20240101", end_date="20240131")
```

全部 39 个查询方法、完整示例和冒烟测试见[本地查询 API 文档](docs/query-api.md)。

仓库还内置 AI Skill（`skills/zer0share-data`），支持让 Codex、Claude Code 等智能体把中文自然语言数据请求转成本地查询流程。

## CLI 命令一览

| 命令 | 说明 |
|------|------|
| `sync --table <name>` | 增量同步单个表（63 张表清单见上方[数据覆盖](#数据覆盖63-张表)） |
| `sync --tables a,b,c` | 同步多张表 |
| `sync --topic <组名>` | 按业务域同步（`quote`/`finance`/`futures`/`fund`/`macro` 等 12 组） |
| `sync --all` | 同步全部 63 张表 |
| `sync --dry-run` | 只看计划，不联网、不写盘 |
| `sync --stock` / `--etf` / `--futures` / `--options` | 旧版专题开关，等价对应 `--topic` |
| `sync --ricequant` | 同步 RiceQuant 分钟线（可选，需 RiceQuant 账号） |
| `news sync --api major_news\|news --start-date <YYYYMMDD> [--src ...]` | 同步新闻到 `data/news/news.db`（原始返回归档在 `data/news/raw/`），已完成的日期自动跳过 |
| `build-universe [--date / --start-date / --end-date]` | 构建股票池（详见[股票池文档](docs/universe.md)） |
| `status` | 查看各表最后同步时间 |
| `quality check [--all / --market / --table]` | 本地数据质检，报告写入 `reports/quality/` |
| `scheduler start` | 启动定时调度 |

所有命令通过 `uv run python main.py <命令>` 执行，加 `--help` 查看完整参数。

## 项目结构

```
zer0share/
├── main.py           # CLI 入口
├── zer0share/
│   ├── api.py        # 本地 Tushare-like 查询 API
│   ├── catalog.py    # 本地表结构、路径和查询规格（63 张表的唯一权威）
│   ├── cli.py        # Click CLI 实现
│   ├── fetcher.py    # Tushare API 封装
│   ├── pipeline.py   # 同步任务编排（SyncJob 注册表）
│   ├── sync/         # 各专题同步任务
│   │   ├── _jobs.py      # SyncJob / DailySyncJob / SnapshotSyncJob 基类
│   │   ├── _batch.py     # BatchSyncJob（批量取数 + 流式分批落盘）
│   │   └── _batch_jobs.py# 26 张批量取数表的注册
│   ├── query/        # 本地查询实现
│   ├── quality/      # 数据质检
│   ├── universe.py   # 股票池构建
│   ├── scheduler.py  # APScheduler 定时任务
│   ├── notifier.py   # 企业微信告警
│   └── storage.py    # Parquet 读写 + DuckDB MetaStore + 安全合并原语
├── config/           # settings.example.toml
├── docs/             # 文档
├── examples/         # 按专题分类的本地查询冒烟测试
├── skills/           # zer0share-data AI Skill
└── tests/            # pytest 测试套件
```

## 文档

- [同步指南](docs/SYNC_GUIDE.md)：积分要求、全部同步表、首次同步顺序和定时调度部署
- [本地查询 API](docs/query-api.md)：全部查询方法、完整示例和冒烟测试
- [股票池构建](docs/universe.md)：6 个股票池的定义和过滤规则
- [数据存储结构](docs/storage.md)：Parquet 分区布局和 DuckDB 元数据
- [examples/README.md](examples/README.md)：示例脚本清单和默认参数
- [RiceQuant 分钟线](docs/ricequant.md)：可选分钟线数据源的配置、同步和查询
- [开发笔记](docs/devlog.md)：记录设计过程和踩坑的系列文章目录

## 开发

```bash
uv sync --dev
uv run pytest                          # 运行测试
uv run pytest tests/test_pipeline.py -v  # 运行单个测试文件
```

## 交流

我持续记录这个项目的设计过程、踩坑记录和后续扩展：

- [开发笔记目录](docs/devlog.md)：所有文章的索引，跟踪开发进度从这里开始
- 微信公众号「极客投研笔记」：同步更新，欢迎关注

如果你对 AI + 量化投研、本地量化数据系统、因子研究工作流感兴趣，欢迎交流。

## 免责声明

本项目仅用于研究和工程实验，不构成任何投资建议。数据来自 Tushare Pro，使用请遵守其服务条款。

## License

MIT。详见 [LICENSE](LICENSE)。
