# 数据存储结构

目录按 Tushare 数据分类命名：`stock/`（股票数据）、`etf/`（ETF专题）、`index/`（指数专题）、`futures/`（期货数据）、`options/`（期权数据）。数据以 Parquet 分区文件存储，元数据（同步记录、交易日历索引）在 DuckDB。

```
data/
├── stock/                              # 股票数据
│   ├── trade_cal/
│   │   ├── exchange=SSE/data.parquet
│   │   ├── exchange=SZSE/data.parquet
│   │   ├── exchange=CFFEX/data.parquet
│   │   ├── exchange=SHFE/data.parquet
│   │   ├── exchange=DCE/data.parquet
│   │   ├── exchange=CZCE/data.parquet
│   │   ├── exchange=INE/data.parquet
│   │   └── exchange=GFEX/data.parquet
│   ├── basic/
│   │   └── data.parquet
│   ├── daily_kline/
│   │   └── date=YYYYMMDD/data.parquet
│   ├── adj_factor/
│   │   └── date=YYYYMMDD/data.parquet
│   ├── daily_basic/
│   │   └── date=YYYYMMDD/data.parquet
│   ├── stock_st/
│   │   └── date=YYYYMMDD/data.parquet
│   ├── suspend_d/
│   │   └── date=YYYYMMDD/data.parquet
│   ├── stk_limit/
│   │   └── date=YYYYMMDD/data.parquet
│   ├── stk_holdertrade/               # 股东增减持，按公告日（含周末）分区
│   │   └── date=YYYYMMDD/data.parquet
│   ├── industry/
│   │   ├── sw_classify/data.parquet    # 申万行业分类树
│   │   ├── sw_member/data.parquet      # 申万股票-行业映射（全量历史）
│   │   ├── ci_member/data.parquet      # 中信股票-行业映射（全量历史）
│   │   └── sw_daily/
│   │       └── date=YYYYMMDD/data.parquet  # 申万行业指数日线
│   └── universe/
│       ├── name=univ_research_base/date=YYYYMMDD/data.parquet
│       ├── name=univ_trade_base/date=YYYYMMDD/data.parquet
│       ├── name=univ_trade_hs300/date=YYYYMMDD/data.parquet
│       ├── name=univ_trade_zz500/date=YYYYMMDD/data.parquet
│       ├── name=univ_trade_zz1000/date=YYYYMMDD/data.parquet
│       └── name=univ_trade_smallcap/date=YYYYMMDD/data.parquet
├── etf/                                # ETF专题
│   ├── etf_basic/
│   │   └── data.parquet
│   ├── etf_index/
│   │   └── data.parquet
│   ├── fund_daily/
│   │   └── date=YYYYMMDD/data.parquet
│   ├── fund_adj/
│   │   └── date=YYYYMMDD/data.parquet
│   ├── etf_share_size/
│   │   └── date=YYYYMMDD/data.parquet
│   └── etf_sh_cons/
│       └── date=YYYYMMDD/data.parquet
├── index/                              # 指数专题
│   ├── index_daily/
│   │   └── date=YYYYMMDD/data.parquet  # 含当日全部12个宽基指数
│   ├── index_weight/
│   │   └── index_code=*/date=YYYYMMDD/data.parquet
│   └── idx_anns/
│       └── date=YYYYMMDD/data.parquet  # 指数公告
├── futures/                            # 期货数据
│   ├── fut_basic/data.parquet          # 全量，每次覆盖
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
├── options/                            # 期权数据
│   ├── opt_basic/data.parquet          # 全量，每次覆盖
│   └── opt_daily/date=YYYYMMDD/data.parquet
├── ricequant/                          # RiceQuant 分钟线（可选数据源）
│   ├── basic/data.parquet
│   ├── stock_minute/date=YYYYMMDD/data.parquet
│   ├── etf_basic/data.parquet
│   └── etf_minute/date=YYYYMMDD/data.parquet
└── news/                               # 新闻（可选，不用 Parquet 分区）
    ├── news.db                         # SQLite 主库：去重、正文修订记录、抓取日志、标题全文索引
    └── raw/{api}/{src}/YYYYMMDD.jsonl.gz  # 接口原始返回，只追加，主库损坏时可重建
db/
└── meta.duckdb                         # 同步记录 + 交易日历索引
```
