# 数据同步指南

## 前置条件

### 1. 安装依赖

```bash
uv sync --dev
```

### 2. 配置文件

复制示例配置并填写真实参数：

```bash
cp config/settings.example.toml config/settings.toml
```

编辑 `config/settings.toml`：

```toml
[tushare]
token = "你的 Tushare Pro Token"

[paths]
data_dir = "data"
db_path = "db/meta.duckdb"
log_path = "logs/pipeline.log"

[scheduler]
daily_kline_hour = 18
daily_kline_minute = 0
basic_hour = 8
adj_factor_hour = 18
adj_factor_minute = 5
futures_hour = 17
futures_start_minute = 0

[notifier]
wecom_webhook_url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=YOUR_KEY"
enabled = false
```

> Tushare Token 在 [tushare.pro](https://tushare.pro) 注册后获取，需要积分 >= 2000 才能调用基础行情接口；`stock_st` 需积分 >= 3000，中信行业和部分期货扩展数据需更高积分。

---

## 首次同步

### 步骤一：同步交易日历

交易日历是其他同步的前置依赖，**必须最先执行**。

```bash
uv run python main.py sync --table trade_cal
```

此命令会：
- 首次拉取 SSE、SZSE 共 2 个交易所从 1990-01-01 到当年年底的日历
- 后续从本地最大 `cal_date` 的下一天增量拉取到当年年底
- 合并写入 `data/trade_cal/exchange=XXX/data.parquet`
- 加载到 DuckDB 供后续查询

预计耗时：1～3 分钟（受网络和 Tushare 限速影响）。

### 步骤二：同步股票基础信息

```bash
uv run python main.py sync --table basic
```

此命令会：
- 拉取全市场所有状态（上市 L、退市 D、暂停 P、精选层 G）的股票基础信息
- 写入 `data/basic/data.parquet`

### 步骤三：同步日线行情

```bash
uv run python main.py sync --table daily_kline
```

此命令会：
- 以 SSE 交易日历为基准，只对真实交易日拉取数据（跳过周末和节假日）
- 从 2016-01-01 起增量同步到今天
- 每个交易日写入 `data/daily_kline/date=YYYYMMDD/data.parquet`

> **注意**：首次同步历史数据量较大（约 10 年 × 3800 只股票），耗时可能在 1～2 小时，受 Tushare 每分钟调用频次限制影响。

### 步骤四：同步复权因子

```bash
uv run python main.py sync --table adj_factor
```

此命令会：
- 以 SSE 交易日历为基准，拉取每个交易日全市场的前复权因子
- 从 2016-01-01 起增量同步到今天
- 每个交易日写入 `data/adj_factor/date=YYYYMMDD/data.parquet`

字段：`ts_code`（股票代码）、`trade_date`（交易日）、`adj_factor`（复权因子值）。

---

## 一键同步全部

常用表可合并为一条命令。当前 `--all` 会按 CLI 中固定顺序同步股票、指数、行业和期货数据：

```bash
uv run python main.py sync --all
```

也可以分组逐步同步：

```bash
# 股票和指数扩展数据
uv run python main.py sync --table daily_basic
uv run python main.py sync --table stock_st
uv run python main.py sync --table suspend_d
uv run python main.py sync --table stk_limit
uv run python main.py sync --table index_weight
uv run python main.py sync --table index_daily
uv run python main.py sync --table industry
uv run python main.py sync --table ci_member

# 期货数据
uv run python main.py sync --table fut_basic
uv run python main.py sync --table fut_daily
uv run python main.py sync --table fut_holding
uv run python main.py sync --table fut_wsr
uv run python main.py sync --table fut_settle
uv run python main.py sync --table fut_mapping
uv run python main.py sync --table ft_limit
uv run python main.py sync --table fut_weekly
uv run python main.py sync --table fut_monthly
uv run python main.py sync --table fut_index_daily
uv run python main.py sync --table fut_weekly_detail
```

---

## 查看同步状态

```bash
uv run python main.py status
```

输出示例：

```
trade_cal    last sync: 2026-04-17
basic        last sync: 2026-04-17
daily_kline  last sync: 2026-04-17
adj_factor   last sync: 2026-04-17
```

---

## 增量更新

再次运行任意 `sync` 命令时，pipeline 会自动从上次同步的日期之后继续拉取，无需重新全量同步。交易日历按每个交易所本地已有的最大 `cal_date` 增量补齐。

```bash
# 每个交易日收盘后更新日线行情
uv run python main.py sync --table daily_kline

# 指定日期范围更新日分区表
uv run python main.py sync --table daily_basic --start-date 2024-01-01 --end-date 2024-01-31
uv run python main.py sync --table fut_daily --start-date 2024-01-01 --end-date 2024-01-31
uv run python main.py sync --table ft_limit --start-date 2024-01-01 --end-date 2024-01-31
```

---

## 自动化调度

启动后台定时任务，按配置自动在收盘后同步：

```bash
uv run python main.py scheduler start
```

默认调度时间（可在 `settings.toml` 修改）：

| 任务 | 触发时间 | 说明 |
|------|----------|------|
| daily_kline | 每天 18:00 | 仅交易日写入数据，非交易日自动跳过 |
| index_daily | 每天 18:00 | 同步宽基指数日线行情 |
| adj_factor | 每天 18:05 | 仅交易日写入数据，非交易日自动跳过 |
| basic | 每天 08:00 | 每日全量刷新 |
| futures | 每天 17:00 起 | 依次同步 11 个期货任务，每个任务间隔 10 分钟 |

> 调度器需保持进程运行。生产环境建议配合 `systemd` 或 `supervisor` 管理进程。

---

## 数据目录结构

同步完成后，本地数据布局如下：

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
│   └── index_code=399300.SZ/date=20160104/data.parquet
├── index_daily/
│   └── date=20160104/data.parquet
├── industry/
│   ├── sw_classify/data.parquet
│   ├── sw_member/data.parquet
│   └── ci_member/data.parquet
├── futures/
│   ├── fut_basic/date=YYYYMMDD/data.parquet
│   ├── fut_daily/date=YYYYMMDD/data.parquet
│   ├── fut_holding/date=YYYYMMDD/data.parquet
│   ├── fut_wsr/date=YYYYMMDD/data.parquet
│   ├── fut_settle/date=YYYYMMDD/data.parquet
│   ├── fut_mapping/date=YYYYMMDD/data.parquet
│   ├── ft_limit/date=YYYYMMDD/data.parquet
│   ├── fut_weekly/date=YYYYMMDD/data.parquet
│   ├── fut_monthly/date=YYYYMMDD/data.parquet
│   ├── fut_index_daily/date=YYYYMMDD/data.parquet
│   └── fut_weekly_detail/date=YYYYMMDD/data.parquet
db/
└── meta.duckdb
logs/
└── pipeline.log
```
