# 数据同步指南

本文档是同步相关的完整参考：Tushare 积分要求、全部同步表清单、首次同步顺序和定时调度配置。

## Tushare 积分要求

同步数据需要 [Tushare Pro](https://tushare.pro) Token。各类数据的积分门槛：

| 数据 | 积分要求 |
|---|---|
| 基础行情（日历、基础信息、日线、复权因子、每日指标） | ≥ 2000 |
| `stock_st`（每日 ST 列表） | ≥ 3000 |
| 中信行业成分、`opt_basic`、部分期货扩展数据 | ≥ 5000 |
| ETF 日线行情 `fund_daily` | ≥ 5000（8000 积分频次更高） |
| 基金复权因子 `fund_adj` | ≥ 600（5000 积分以上频次更高） |
| 指数公告 `idx_anns` | ≥ 6000 |
| ETF 基础信息、ETF 份额规模、上交所 ETF 持仓 | ≥ 8000 |

## 首次同步

一键同步全部，或按业务域分组同步：

```bash
uv run python main.py sync --all             # 全部 63 张
uv run python main.py sync --topic quote     # 行情 10 张
uv run python main.py sync --topic finance   # 财务 12 张
uv run python main.py sync --topic futures   # 期货 11 张
uv run python main.py sync --topic fund      # 基金与 ETF 8 张
```

`--topic` 接受的分组共 12 个：`basic`、`quote`、`fundflow`、`toplist`、
`finance`、`index`、`bond`、`fund`、`macro`、`futures`、`options`、`etf`
（`etf` 与 `fund` 表清单相同，是 `--etf` 旧开关的等价物）。
旧版专题开关 `--stock/--etf/--futures/--options` 保留原表清单语义。

也可以逐表执行（顺序不可颠倒）。首次同步日线等历史数据量较大，受 Tushare
每分钟调用频次限制影响，全量回填可能需要数小时。建议先用 `--dry-run`
确认路由正确再正式跑。

### 统一 `sync` 命令

`sync` 是唯一公开入口。**所有 63 张表都是同一种 `SyncJob`**，共用同一套
run / 游标 / 重试 / 通知 / 日期区间语义，写同一套 catalog 规范布局
（时序表 `date=YYYYMMDD/data.parquet`，静态表 `data.parquet`，
`index_weight` 两级分区 `index_code=*/date=*/`）。

表之间只在**取数方式**上分两类，由 job 自身决定，对使用者透明：

| 取数方式 | 负责 | 适用 |
|---|---|---|
| `DailySyncJob` | 交易日历驱动逐日增量 | 行情/指标类按日发布的表 |
| `BatchSyncJob` | 批量取数 + 流式分批落盘 | 财报/资金流/宏观等需大窗口回溯的表 |

表名统一用 catalog 规范名（旧名 `daily`/`stock_basic` 仍作别名接受）。

```bash
zer0share sync --all                              # 全部表
zer0share sync --dry-run                          # 只打印计划，不联网不写盘
zer0share sync --topic quote,index                # 按主题
zer0share sync --table daily_kline                # 单表（可重复）
zer0share sync --tables daily_kline,daily_basic   # 逗号分隔
zer0share sync --table daily_kline --start-date 20260101 --end-date 20261021
zer0share sync --budget-seconds 3600              # 调整批量取数时间预算
zer0share sync --include-missing                  # 也回补本地从未建成的表
```

`--table` 与 `--topic` 同时给出时取并集。旧的主题开关
`--stock/--futures/--options/--etf/--ricequant` 保留原表清单语义不变。
`--start-date`/`--end-date` 只对按日分区的表有效，快照表（basic/etf_basic 等）
给了会直接报错。

工程约束（踩坑换来的，勿简化）：

- 覆盖情况只读 parquet footer 统计 + 尾部 row group，绝不整表 pandas 读入
  （`daily_basic` 8.07M×18 整表读入实测 2.98GB RSS，顶爆 cron cgroup 预算）
- 时序表统一写 `date=*` 分区，走 pyarrow 按 row group 流式读写 + 原子替换，
  内存只跟单个 row group 成正比
- 沿用旧文件压缩算法（`daily_basic` 原是 polars zstd，改 snappy 文件涨 53%；zstd 显式 level 3）
- 批量取数表每 50 天 / 50 万行一批流式落盘，长历史回溯中断最多损失一批
- 从未建成的表默认不发请求（否则每晚从零回补 1645 个交易日 → 3600s 窗口必超时）；
  `--include-missing` 可显式开启
- 预算耗尽后剩余表标 `DEFERRED(budget)`（默认 780s），
  exit code 仍按核心表覆盖判定：齐全 0，有缺口 3
- 按日接口首屏恰好 5000 行时自动 offset 翻页（`adj_factor`/`basic` 实测单日
  超过 5000，不翻页会静默丢 10% 数据）

### 股票核心（必选）

```bash
uv run python main.py sync --table trade_cal    # 交易日历（必须最先，8 个交易所）
uv run python main.py sync --table basic        # 股票基础信息
uv run python main.py sync --table daily_kline  # 日线行情（依赖交易日历）
uv run python main.py sync --table adj_factor   # 复权因子（依赖交易日历）
uv run python main.py sync --table daily_basic  # 每日指标：总市值、流通市值等
uv run python main.py sync --table stock_st     # 每日 ST 股票列表
uv run python main.py sync --table suspend_d    # 每日停牌列表
uv run python main.py sync --table stk_limit    # 每日涨跌停价格
uv run python main.py sync --table stk_holdertrade  # 股东增减持（按公告日，自然日同步，2006 年起）
uv run python main.py sync --table index_weight # 沪深300/中证500/中证1000成分
uv run python main.py sync --table index_daily  # 宽基指数日线行情
uv run python main.py sync --table idx_anns     # 指数公司公告（自然日同步）
uv run python main.py sync --table industry     # 申万行业分类 + 成分映射
uv run python main.py sync --table ci_member    # 中信行业成分映射
uv run python main.py sync --table sw_daily     # 申万行业指数日线行情
```

### ETF 专题（可选）

```bash
uv run python main.py sync --table etf_basic       # ETF 基础信息
uv run python main.py sync --table etf_index       # ETF 基准指数列表
uv run python main.py sync --table fund_daily      # ETF 日线行情
uv run python main.py sync --table fund_adj        # 基金复权因子
uv run python main.py sync --table etf_share_size  # ETF 份额规模（通常次日 08:30 后更新）
uv run python main.py sync --table etf_sh_cons     # 上交所 ETF 每日持仓组合
```

### 期货扩展（可选，需积分 ≥ 5000）

```bash
uv run python main.py sync --table fut_basic          # 期货合约基础信息
uv run python main.py sync --table fut_daily          # 期货日线行情
uv run python main.py sync --table fut_holding        # 期货持仓排名
uv run python main.py sync --table fut_wsr            # 期货仓单日报
uv run python main.py sync --table fut_settle         # 期货结算参数
uv run python main.py sync --table fut_mapping        # 期货主力与连续合约映射
uv run python main.py sync --table ft_limit           # 期货涨跌停价格
uv run python main.py sync --table fut_weekly         # 期货周线行情
uv run python main.py sync --table fut_monthly        # 期货月线行情
uv run python main.py sync --table fut_index_daily    # 期货指数日线行情
uv run python main.py sync --table fut_weekly_detail  # 期货交易所周度明细
```

### 期权扩展（可选，需积分 ≥ 5000）

```bash
uv run python main.py sync --table opt_basic          # 期权合约基础信息
uv run python main.py sync --table opt_daily          # 期权日线行情
```

### RiceQuant 分钟线（可选，需 RiceQuant 账号）

```bash
uv run python main.py sync --ricequant                          # 按顺序同步全部 RiceQuant 表
uv run python main.py sync --table ricequant_basic              # 合约基础信息
uv run python main.py sync --table ricequant_stock_minute       # 股票分钟线
uv run python main.py sync --table ricequant_etf_basic          # ETF 基础信息
uv run python main.py sync --table ricequant_etf_minute         # ETF 分钟线
```

需要在 `config/settings.toml` 的 `[ricequant]` 里配置 license key 或用户名/密码，详见 [RiceQuant 分钟线文档](ricequant.md)。

### 新闻（可选，需单独开通新闻权限）

新闻不走 `sync --table`，有单独的命令，数据存入 `data/news/news.db`（SQLite），原始返回归档在 `data/news/raw/`：

```bash
uv run python main.py news sync --api major_news --start-date 20250324              # 长篇通讯，默认同步到今天
uv run python main.py news sync --api news --start-date 20250324 --src sina --src cls  # 快讯，只同步指定来源
uv run python main.py news sync --api major_news --start-date 20250324 --with-content  # 同时存正文（整页 HTML，体积大）
uv run python main.py news sync --api news --start-date 20250324 --refresh             # 忽略断点，重拉已完成的日期
```

已完成的（接口, 来源, 日期）会自动跳过。新闻权限通常要单独开通，可在 `config/settings.toml` 的 `[tushare_news]` 里配置另一个账号的 token（不写则沿用 `[tushare]`），详见 `config/settings.example.toml`。

## 首次验证建议

先同步一个小区间，确认 Tushare 权限和字段可用后再全量回填：

```bash
uv run python main.py sync --table daily_basic --start-date 20240101 --end-date 20240131
uv run python main.py sync --table stock_st --start-date 20240101 --end-date 20240131
uv run python main.py sync --table suspend_d --start-date 20240101 --end-date 20240131
uv run python main.py sync --table stk_limit --start-date 20240101 --end-date 20240131
uv run python main.py sync --table index_weight --start-date 20240101 --end-date 20240131
uv run python main.py sync --table fut_daily --start-date 20240101 --end-date 20240131
uv run python main.py sync --table ft_limit --start-date 20240101 --end-date 20240131
uv run python main.py sync --table opt_daily --start-date 20240101 --end-date 20240131
```

## 查看同步状态

```bash
uv run python main.py status
```

## 定时调度

每个表在 `config/settings.toml` 的 `[scheduler]` 里单独配置触发时间（`HH:MM`），默认值基于 Tushare 各接口实际入库时间设计：

```toml
[scheduler]
# 凌晨 — 静态参考数据（增量检查，数据已最新时零API消耗）
trade_cal         = "02:00"
basic             = "02:05"
# 盘前 — Tushare 盘前推送
stk_limit         = "09:15"   # 8:40 ready; delayed to avoid early empty responses
adj_factor        = "09:25"   # 9:15~9:20 ready
stock_st          = "09:28"   # 9:20 ready
# 收盘后第一波 — 日线行情（15:00~16:00 ready）
daily_kline       = "16:10"
# 收盘后第二波 — 每日指标及其余数据（3min 间隔，17:05~17:56）
daily_basic       = "17:05"
# ... 完整示例见 config/settings.example.toml
```

同步失败可通过企业微信告警，在 `[notifier]` 里配置 webhook。

### systemd 服务（推荐，服务器常驻）

```bash
# 安装并启用服务
sudo cp scripts/zer0share-scheduler.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable zer0share-scheduler
sudo systemctl start zer0share-scheduler

# 查看状态
sudo systemctl status zer0share-scheduler

# 查看实时日志
journalctl -u zer0share-scheduler -f

# 修改 settings.toml 后重启生效
sudo systemctl restart zer0share-scheduler
```

### 手动启动（调试用）

```bash
uv run python main.py scheduler start
```
