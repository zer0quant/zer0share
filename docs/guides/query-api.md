# 本地查询 API

同步完成后，可以在研究代码中使用类似 Tushare Pro 的本地 Python API 查询数据。查询只读取本地 Parquet 文件，通过 DuckDB 执行，不会访问 Tushare，也不会消耗积分。

## 使用示例

```python
from zer0share import pro_api

pro = pro_api()

# 股票
basic = pro.stock_basic(list_status="L")
cal = pro.trade_cal(exchange="SSE", start_date="20240101", end_date="20240131")
daily = pro.daily(ts_code="000001.SZ", start_date="20240101", end_date="20240331")
adj = pro.adj_factor(ts_code="000001.SZ", start_date="20240101", end_date="20240331")
daily_basic = pro.daily_basic(trade_date="20240131", fields="ts_code,trade_date,total_mv")
st = pro.stock_st(trade_date="20240131")
suspend = pro.suspend_d(trade_date="20240131")
limit = pro.stk_limit(trade_date="20240131")

# 复权行情
qfq = pro.pro_bar(
    ts_code="000001.SZ",
    start_date="20240101",
    end_date="20240331",
    adj="qfq",
)

# 指数
hs300 = pro.index_weight(index_code="399300.SZ", start_date="20240101", end_date="20240131")
idx_daily = pro.index_daily(ts_code="000300.SH", start_date="20240101", end_date="20240131")
idx_anns = pro.idx_anns(
    src="中证指数",
    start_date="20260401",
    end_date="20260430",
    fields="ann_date,title,source,type",
)

# 行业（用于行业中性化）
sw_industries = pro.index_classify(level="L1", src="SW2021")       # 申万一级行业列表
sw_member = pro.index_member_all(ts_code="000001.SZ", is_new="Y")  # 查股票所属申万行业
ci_member = pro.ci_index_member(ts_code="000001.SZ", is_new="Y")   # 查股票所属中信行业
sw_daily = pro.sw_daily(trade_date="20240131")                     # 申万行业指数日线

# 股票池
pool = pro.universe("univ_trade_hs300", trade_date="20240131")     # 某日沪深300交易池

# ETF
etf_basic = pro.etf_basic(list_status="L", exchange="SH")
etf_index = pro.etf_index(ts_code="000300.SH")
fund_daily = pro.fund_daily(
    ts_code="510330.SH",
    start_date="20250101",
    end_date="20250618",
    fields="trade_date,open,high,low,close,vol,amount",
)
fund_adj = pro.fund_adj(
    ts_code="513100.SH",
    start_date="20190101",
    end_date="20190926",
    fields="ts_code,trade_date,adj_factor,discount_rate",
)
etf_share_size = pro.etf_share_size(
    ts_code="510330.SH",
    start_date="20250101",
    end_date="20251224",
    fields="trade_date,ts_code,etf_name,total_share,total_size,exchange",
)
etf_sh_cons = pro.etf_sh_cons(
    trade_date="20260615",
    ts_code="517030.SH",
    fields="trade_date,ts_code,con_code,con_name,qty,sub_flag,cpr,rdr,sca,exchange",
)

# 期货
fut_contracts = pro.fut_basic(exchange="SHFE")                                    # 上期所合约列表
fut_bar = pro.fut_daily(ts_code="RB2410.SHFE", start_date="20240101", end_date="20240331")
fut_holding = pro.fut_holding(trade_date="20240131", exchange="SHFE")             # 某日持仓排名
fut_mapping = pro.fut_mapping(ts_code="RB.SHFE", start_date="20240101", end_date="20240331")  # 主连映射
fut_weekly = pro.fut_weekly(ts_code="RB2501.SHFE", start_date="20240101", end_date="20240331")

# 期权
opt_contracts = pro.opt_basic(exchange="SSE", call_put="C")           # 上交所认购合约列表
opt_bar = pro.opt_daily(ts_code="10004462.SH", start_date="20240101", end_date="20240131")
opt_snapshot = pro.opt_daily(trade_date="20240102", exchange="SSE")   # 某日全部上交所期权行情

# 新闻（需先运行 news sync，存于 SQLite）
major = pro.major_news(start_date="20250324", end_date="20250324", src="新华网")
flash = pro.news(start_date="20250324", end_date="20250324", keyword="降准")   # 按标题全文检索，至少 3 个字
```

## 支持的本地查询方法

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
| `stk_holdertrade` | 查询已同步的股东增减持（按 `ann_date` 分区；`holder_type` C 公司 / P 个人 / G 高管，`trade_type` IN 增持 / DE 减持） |
| `index_weight` | 查询已同步的指数成分和权重 |
| `index_daily` | 查询已同步的宽基指数日线行情（12个指数） |
| `idx_anns` | 查询已同步的指数公司公告（按 `ann_date` 分区） |
| `index_classify` | 查询申万行业分类树（L1/L2/L3） |
| `index_member_all` | 查询申万股票-行业映射（支持历史变更） |
| `ci_index_member` | 查询中信股票-行业映射（支持历史变更） |
| `sw_daily` | 查询已同步的申万行业指数日线行情 |
| `etf_basic` | 查询已同步的 ETF 基础信息 |
| `etf_index` | 查询已同步的 ETF 基准指数列表 |
| `fund_daily` | 查询已同步的 ETF 日线行情 |
| `fund_adj` | 查询已同步的基金复权因子 |
| `etf_share_size` | 查询已同步的 ETF 份额规模 |
| `etf_sh_cons` | 查询已同步的上交所 ETF 每日持仓组合 |
| `pro_bar` | 查询本地 A 股日线行情，支持不复权、前复权（qfq）和后复权（hfq） |
| `universe` | 查询已构建的股票池（支持按池名称、ts_code、日期过滤） |
| `fut_basic` | 查询已同步的期货合约基础信息（支持按交易所、fut_code 过滤） |
| `fut_daily` | 查询已同步的期货日线行情 |
| `fut_holding` | 查询已同步的期货持仓排名 |
| `fut_wsr` | 查询已同步的期货仓单日报 |
| `fut_settle` | 查询已同步的期货结算参数 |
| `fut_mapping` | 查询已同步的期货主力与连续合约映射 |
| `ft_limit` | 查询已同步的期货涨跌停价格 |
| `fut_weekly` | 查询已同步的期货周线行情 |
| `fut_monthly` | 查询已同步的期货月线行情 |
| `fut_index_daily` | 查询已同步的期货指数日线行情 |
| `fut_weekly_detail` | 查询已同步的期货交易所周度明细 |
| `opt_basic` | 查询已同步的期权合约基础信息（支持按交易所、call_put、opt_code 过滤） |
| `opt_daily` | 查询已同步的期权日线行情（支持按交易所过滤） |
| `major_news` | 查询已同步的长篇通讯（SQLite；北京时间日期含两端；`src` 过滤来源，`keyword` 按标题全文检索，至少 3 个字；`content` 仅在同步时开启 `--with-content` 才有） |
| `news` | 查询已同步的新闻快讯（参数同 `major_news`；很多快讯标题为空，正文在 `content`） |
| `query` | 按接口名分发，例如 `pro.query("daily", ...)` |

## AI Skill

仓库内置 AI Skill：`skills/zer0share-data`。支持让 Codex、Claude Code、OpenClaw 等智能体把中文自然语言数据请求转成 `zer0share` 本地查询流程。

## 冒烟测试

完整示例清单和默认参数见 [examples/README.md](../../examples/README.md)。

```bash
# 股票示例
uv run python examples/stock/basic_query_smoke.py
uv run python examples/stock/daily_query_smoke.py
uv run python examples/stock/adj_factor_query_smoke.py

# ETF 示例
uv run python examples/etf/etf_basic_query_smoke.py
uv run python examples/etf/etf_index_query_smoke.py
uv run python examples/etf/fund_daily_query_smoke.py
uv run python examples/etf/etf_share_size_query_smoke.py
uv run python examples/etf/etf_sh_cons_query_smoke.py

# 指数示例
uv run python examples/index/index_daily_query_smoke.py
uv run python examples/index/index_weight_query_smoke.py
uv run python examples/index/idx_anns_query_smoke.py

# 期货示例
uv run python examples/futures/fut_basic_query_smoke.py
uv run python examples/futures/fut_daily_query_smoke.py
uv run python examples/futures/fut_holding_query_smoke.py
uv run python examples/futures/fut_settle_query_smoke.py
uv run python examples/futures/fut_weekly_monthly_query_smoke.py
uv run python examples/futures/ft_limit_query_smoke.py

# 期权示例
uv run python examples/options/opt_basic_query_smoke.py
uv run python examples/options/opt_daily_query_smoke.py
```
