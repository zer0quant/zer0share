"""sync/_batch_jobs.py — 把原 bulk 的 27 张表注册成 BatchSyncJob。

这里每张表的 kind / api / 参数 都来自 bulk.build_tables 的注册表, 但
**接口参数经过实测校正** (探测脚本 probe_columns*.py):

  weekly       实测 trade_date=20260101 返回空; ts_code+start/end 有数据 →
               归为 kind="stock" (逐只查), 不是按日批量
  forecast     实测 start_date/end_date/ann_date 全部返回空; 只有 period 有数据 →
               归为 kind="quarter" (按季末查)
  fund_portfolio 实测必须带 period → kind="quarter"
  us_daily / moneyflow_ind / moneyflow_hsgt
               接口未开通或超出积分范围 → 不注册 (声明只会让 sync 空跑)

分区路径一律由 catalog spec 派生 (_part_dir), 与 job 引擎其它表一致。
"""
from __future__ import annotations

import pyarrow.parquet as pq
from loguru import logger

from zer0share.catalog import (
    WEEKLY_SPEC, MONTHLY_SPEC, MONEYFLOW_SPEC, MARGIN_DETAIL_SPEC,
    HK_HOLD_SPEC, TOP_LIST_SPEC, TOP_INST_SPEC, CB_DAILY_SPEC,
    INDEX_DAILYBASIC_SPEC, INCOME_SPEC, BALANCESHEET_SPEC, CASHFLOW_SPEC,
    FINA_INDICATOR_SPEC, DIVIDEND_SPEC, FORECAST_SPEC, STK_HOLDERNUMBER_SPEC,
    TOP10_HOLDERS_SPEC, TOP10_FLOATHOLDERS_SPEC, STK_MANAGERS_SPEC,
    STK_REWARDS_SPEC, FUND_PORTFOLIO_SPEC, SHIBOR_SPEC, REPURCHASE_SPEC,
    CB_BASIC_SPEC, FUND_NAV_SPEC, FUND_SHARE_SPEC, REPO_DAILY_SPEC,
)
from zer0share.sync._batch import BatchSyncJob
from zer0share.storage import DailyPartitionStore, SnapshotStore

# 表名 -> (kind, 额外取数参数, date_col)
# kind: date=按交易日批量 / stock=逐只 / quarter=按季末 / snapshot=一次性快照
BATCH_TABLES: dict[str, tuple[str, dict, str]] = {
    # ---- 按日批量 (date=*) ----
    "moneyflow":        ("date", {}, "trade_date"),
    "margin_detail":    ("date", {}, "trade_date"),
    "hk_hold":          ("date", {}, "trade_date"),
    "top_list":         ("date", {}, "trade_date"),
    "top_inst":         ("date", {}, "trade_date"),
    "index_dailybasic": ("date", {}, "trade_date"),
    "cb_daily":         ("date", {}, "trade_date"),
    # shibor 的日期列叫 date (不是 trade_date) — 实测列名 ['date','on','1w',...]
    "shibor":           ("date", {}, "date"),
    # 逆回购公告: 无交易日维度, 接口只认 start_date/end_date 窗口, 返回
    # ann_date/end_date。分区键取 ann_date (见 REPURCHASE_SPEC)
    "repurchase":       ("window", {}, "ann_date"),
    # ---- 逐只查 (财报类 / weekly / monthly) ----
    # monthly 实测 trade_date 参数被拒 ('ts_code,trade_date 至少输入一个'),
    # 与 weekly 同为 ts_code+start_date/end_date 逐只查
    "weekly":            ("stock", {}, "trade_date"),
    "monthly":           ("stock", {}, "trade_date"),
    "income":            ("stock", {}, "end_date"),
    "balancesheet":      ("stock", {}, "end_date"),
    "cashflow":          ("stock", {}, "end_date"),
    "fina_indicator":    ("stock", {}, "end_date"),
    "dividend":          ("stock", {}, "end_date"),
    "stk_holdernumber":  ("stock", {}, "end_date"),
    "top10_holders":     ("stock", {}, "end_date"),
    "top10_floatholders": ("stock", {}, "end_date"),
    "stk_managers":      ("stock", {}, "ann_date"),
    "stk_rewards":       ("stock", {}, "end_date"),
    # ---- 按季末查 ----
    "forecast":          ("quarter", {}, "end_date"),
    "fund_portfolio":    ("quarter", {}, "end_date"),
    # ---- 一次性快照 ----
    "cb_basic":          ("snapshot", {}, "trade_date"),
    "fund_nav":          ("snapshot", {}, "nav_date"),
    "fund_share":        ("snapshot", {}, "trade_date"),
    "repo_daily":        ("snapshot", {}, "trade_date"),
}

_SPECS = {
    "weekly": WEEKLY_SPEC, "monthly": MONTHLY_SPEC, "moneyflow": MONEYFLOW_SPEC,
    "margin_detail": MARGIN_DETAIL_SPEC, "hk_hold": HK_HOLD_SPEC,
    "top_list": TOP_LIST_SPEC, "top_inst": TOP_INST_SPEC, "cb_daily": CB_DAILY_SPEC,
    "index_dailybasic": INDEX_DAILYBASIC_SPEC, "income": INCOME_SPEC,
    "balancesheet": BALANCESHEET_SPEC, "cashflow": CASHFLOW_SPEC,
    "fina_indicator": FINA_INDICATOR_SPEC, "dividend": DIVIDEND_SPEC,
    "forecast": FORECAST_SPEC, "stk_holdernumber": STK_HOLDERNUMBER_SPEC,
    "top10_holders": TOP10_HOLDERS_SPEC, "top10_floatholders": TOP10_FLOATHOLDERS_SPEC,
    "stk_managers": STK_MANAGERS_SPEC, "stk_rewards": STK_REWARDS_SPEC,
    "fund_portfolio": FUND_PORTFOLIO_SPEC, "shibor": SHIBOR_SPEC,
    "repurchase": REPURCHASE_SPEC, "cb_basic": CB_BASIC_SPEC, "fund_nav": FUND_NAV_SPEC,
    "fund_share": FUND_SHARE_SPEC, "repo_daily": REPO_DAILY_SPEC,
}

# snapshot 表 fund_nav 历史上只抓了单只 ETF (510300) 的历史, 全市场拉取
# 需要另一套 symbol 枚举, 本次不自动全量; 保留接口但默认跳过。
SNAPSHOT_SKIP = {"fund_nav"}


def _part_dir(d, spec) -> DailyPartitionStore:
    return DailyPartitionStore(d.joinpath(*spec.path_parts))


def build_batch_jobs(cfg, fetcher, codes: list[str] | None = None) -> list[BatchSyncJob]:
    """注册 27 张 BatchSyncJob。codes 为按股票拉取的 ts_code 清单
    (通常来自 stock_basic 快照), 空则按股票类表全部跳过。"""
    d = cfg.data_dir
    pro = getattr(fetcher, "_pro", None) or getattr(fetcher, "pro", None)
    if pro is None:
        raise RuntimeError("fetcher 未暴露 pro 句柄, 无法批量取数")

    jobs: list[BatchSyncJob] = []
    for name, (kind, extra, date_col) in BATCH_TABLES.items():
        spec = _SPECS[name]
        if kind == "snapshot":
            if name in SNAPSHOT_SKIP:
                logger.info(f"batch job {name}: 默认跳过 (需显式 symbol 列表)")
                continue
            store = _part_dir(d, spec)
            job = BatchSyncJob(
                table_name=name, spec=spec, pro=pro, store=store,
                kind="snapshot", api_name=name,
                extra_params=extra,
            )
        else:
            store = _part_dir(d, spec)
            job = BatchSyncJob(
                table_name=name, spec=spec, pro=pro, store=store,
                kind=kind, api_name=name, date_col=date_col,
                codes=codes or [], extra_params=extra,
            )
        jobs.append(job)
    logger.info(f"batch jobs 注册完成: {len(jobs)} 张表")
    return jobs


def load_codes(data_dir) -> list[str]:
    """从 stock/basic 快照读 ts_code 清单, 供逐只拉取的表使用。"""
    path = data_dir.joinpath("stock", "basic", "data.parquet")
    if not path.exists():
        logger.warning(f"stock/basic 不存在 ({path}), 逐只拉取的表将无代码可用")
        return []
    tbl = pq.read_table(path, columns=["ts_code"])
    return sorted({str(x) for x in tbl.column("ts_code").to_pylist() if x})
