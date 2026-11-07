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
- **自动化运维**：APScheduler 定时同步，Docker 一键部署与回滚，支持企业微信 / 飞书告警（失败立即推送，成功每日汇总一条）

## 安装

前置要求：

- Python 3.11+ 和 [uv](https://github.com/astral-sh/uv)
- Tushare Pro Token（基础行情需积分 ≥ 2000，部分专题表要求更高，详见[同步指南](docs/guides/SYNC_GUIDE.md)）

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

[paths]
data_dir = "data"          # Parquet 存储目录
db_path = "db/meta.duckdb" # DuckDB 文件路径
log_path = "logs/pipeline.log"

[scheduler]
# 每个表单独配置触发时间（HH:MM），完整示例见 config/settings.example.toml

[notifier]
enabled = false            # 至少启用一个通道后改为 true

[notifier.wecom]
enabled = false
webhook_url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=YOUR_KEY"

[notifier.feishu]
enabled = false
webhook_url = "https://open.feishu.cn/open-apis/bot/v2/hook/YOUR_KEY"
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

同步顺序、逐表命令和定时调度部署见[同步指南](docs/guides/SYNC_GUIDE.md)。

## 生产部署（Docker）

定时调度推荐用 Docker 常驻运行：镜像只包含代码和锁定的依赖，数据、配置、日志放在宿主机目录（默认 `/data/zer0share-data`，可用环境变量 `ZER0SHARE_DATA_ROOT` 覆盖）并挂载进容器。

```
/data/zer0share-data/
├── config/settings.toml   # 只读挂载；路径保持相对写法（data、db/meta.duckdb、logs/pipeline.log）
├── data/                  # Parquet 数据
├── db/                    # DuckDB 元数据
├── logs/
└── reports/               # 数据质检报告
```

```bash
deploy/deploy.sh                       # 用当前 git HEAD 构建镜像（tag = 日期-短 hash）并更新容器
deploy/deploy.sh 20261107-48e1312      # 回滚到已有镜像
docker logs -f zer0share-scheduler     # 查看调度日志
docker exec zer0share-scheduler python main.py status   # 在生产容器里执行 CLI
```

开发与生产分离：在仓库里改代码、跑测试不会影响正在运行的调度；提交后执行 `deploy/deploy.sh` 才会上线（有未提交的代码改动时脚本会拒绝部署）。开发时如需调试同步逻辑，请把 `settings.toml` 的 `data_dir` 指向单独的测试目录，避免写入生产数据。

完整说明（含旧的 systemd 方式）见[同步指南的部署章节](docs/guides/SYNC_GUIDE.md#docker-部署推荐服务器常驻)。

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

全部 30+ 个查询方法、完整示例和冒烟测试见[本地查询 API 文档](docs/guides/query-api.md)。

仓库还内置 AI Skill（`skills/zer0share-data`），支持让 Codex、Claude Code 等智能体把中文自然语言数据请求转成本地查询流程。

## CLI 命令一览

| 命令 | 说明 |
|------|------|
| `sync --table <name>` | 增量同步单个表（全部 30+ 个表见[同步指南](docs/guides/SYNC_GUIDE.md)） |
| `sync --all` | 按顺序同步全部 |
| `sync --stock` / `--etf` / `--futures` / `--options` | 按专题分组同步 |
| `sync --ricequant` | 同步 RiceQuant 分钟线（可选，需 RiceQuant 账号） |
| `news sync --api major_news\|news --start-date <YYYYMMDD> [--src ...]` | 同步新闻到 `data/news/news.db`（原始返回归档在 `data/news/raw/`），已完成的日期自动跳过 |
| `build-universe [--date / --start-date / --end-date]` | 构建股票池（详见[股票池文档](docs/guides/universe.md)） |
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
│   ├── catalog.py    # 本地表结构、路径和查询规格
│   ├── cli.py        # Click CLI 实现
│   ├── pipeline.py   # 同步任务编排
│   ├── sources/      # 数据源封装（Tushare、RiceQuant）
│   ├── sync/         # 各专题同步任务
│   ├── query/        # 本地查询实现
│   ├── news/         # 新闻同步（JSONL 归档 + SQLite）
│   ├── quality/      # 数据质检
│   ├── universe.py   # 股票池构建
│   ├── scheduler.py  # APScheduler 定时任务
│   ├── notifier.py   # 企业微信 / 飞书告警
│   └── storage.py    # Parquet 读写 + DuckDB MetaStore
├── config/           # settings.example.toml
├── deploy/           # Docker 镜像、compose、发布脚本（systemd/ 为旧方案）
├── docs/
│   ├── guides/       # 使用文档
│   ├── dev/          # 开发笔记、设计与实施计划
│   └── reports/      # 数据质检等报告
├── examples/         # 按专题分类的本地查询冒烟测试
├── scripts/          # 一次性运维与数据校验脚本
├── skills/           # zer0share-data AI Skill
└── tests/            # pytest 测试套件
```

## 文档

- [同步指南](docs/guides/SYNC_GUIDE.md)：积分要求、全部同步表、首次同步顺序和定时调度部署
- [本地查询 API](docs/guides/query-api.md)：全部查询方法、完整示例和冒烟测试
- [股票池构建](docs/guides/universe.md)：6 个股票池的定义和过滤规则
- [数据存储结构](docs/guides/storage.md)：Parquet 分区布局和 DuckDB 元数据
- [examples/README.md](examples/README.md)：示例脚本清单和默认参数
- [RiceQuant 分钟线](docs/guides/ricequant.md)：可选分钟线数据源的配置、同步和查询
- [开发笔记](docs/dev/devlog.md)：记录设计过程和踩坑的系列文章目录

## 开发

```bash
uv sync --dev
uv run pytest                          # 运行测试
uv run pytest tests/test_pipeline.py -v  # 运行单个测试文件
```

## 交流

我持续记录这个项目的设计过程、踩坑记录和后续扩展：

- [开发笔记目录](docs/dev/devlog.md)：所有文章的索引，跟踪开发进度从这里开始
- 微信公众号「极客投研笔记」：同步更新，欢迎关注

如果你对 AI + 量化投研、本地量化数据系统、因子研究工作流感兴趣，欢迎交流。

## 免责声明

本项目仅用于研究和工程实验，不构成任何投资建议。数据来自 Tushare Pro，使用请遵守其服务条款。

## License

MIT。详见 [LICENSE](LICENSE)。
