"""
sync_plan.py — 统一 `sync` 命令的表清单与取数方式。

背景: 历史上 `sync` (zer0share.sync, 每日分区 + MetaStore 记账) 与
`sync-bulk` (zer0share.bulk, 原 download_all_v2 的增量语义) 是两套并行命令,
表名不同 (bulk `daily` ↔ catalog `daily_kline`)、落盘规范不同 (单文件 vs
date=* 分区), 造成同一份数据两个拷贝、query 读不到其中一份。

现在所有表都写 catalog 声明的规范布局 (catalog.parquet_pattern 是唯一权威),
原 bulk 的取数能力已吸收进 job 引擎的 BatchSyncJob, bulk.py 已删除,
只保留**一个** `sync` 命令:
  - 表名统一用 catalog 名字 (daily_kline / basic / …)
  - 每张表都是 SyncJob, 共用同一套 run/游标/重试/通知/日期区间语义;
    取数方式由 job 自身决定 (DailySyncJob 逐日, BatchSyncJob 批量)。
"""
from __future__ import annotations

from dataclasses import dataclass, field

# ------------------------------------------------------------------ 引擎标识
# bulk.py 已删除: 取数能力吸收进 BatchSyncJob, 所有表都是 SyncJob。
# 保留这两个常量仅为兼容旧 import (cli/sync_runner 会 import 它们)。
ENGINE_BULK = "job"   # 已合并: 原 bulk 表现由 BatchSyncJob 承担
ENGINE_JOB = "job"    # 原 job 引擎

# ------------------------------------------------------------------ 表别名
# 旧命令的表名 → catalog 规范名。保留别名是为了不破坏既有脚本/习惯用法。
TABLE_ALIASES: dict[str, str] = {
    # bulk 侧旧名 → catalog 名
    "daily": "daily_kline",
    "stock_basic": "basic",
    # sw_classify / sw_member 由 industry 这一个 job 负责 (同一接口, 同一落盘根),
    # 没有独立的 job 注册项, 因此指向它。
    "sw_classify": "industry",
    "sw_member": "industry",
}

# 原 bulk 引擎负责的表 (catalog 名), 现已由 BatchSyncJob 承担。
#
# 保留这份名单的用途: (1) `sync --topic` 的主题分组仍要覆盖这些表;
# (2) `sync_runner` 用它判断"哪些表是批量取数方式", 以便在 job 侧启用
# 磁盘缺口检测与批量写入路径。
#
# 明确排除的表 (不在任何主题/注册表里):
#   - us_daily / moneyflow_ind / moneyflow_hsgt: Tushare 接口未开通或超出
#     积分范围 (实测 api_scope_denied / api_not_enabled), 注册只会让 sync 空跑
#   - fund_nav: 历史只抓单只 ETF, 全市场拉取需显式 symbol 枚举, 默认跳过
BULK_TABLES: frozenset[str] = frozenset({
    "weekly", "monthly",
    "moneyflow",
    "margin_detail", "hk_hold", "top_list", "top_inst",
    "income", "balancesheet", "cashflow", "fina_indicator", "dividend",
    "forecast", "stk_holdernumber", "top10_holders", "top10_floatholders",
    "stk_managers", "stk_rewards",
    "cb_basic", "cb_daily",
    "fund_portfolio", "fund_share",
    "shibor", "repurchase", "repo_daily",
    "index_dailybasic",
})

# 全部表都归 job 引擎 (每张表都是 SyncJob): 快照表 / 期货 / 期权 / ETF /
# 行业分类 / 自然日表 / 多交易所表 / 吸收 bulk 后的批量取数表。
# 原 BULK_TABLES 中的表已在 pipeline registry 注册为 BatchSyncJob,
# 这里列出的是**逐日/快照型** job, 便于 `sync --topic` 反查与文档说明。
JOB_TABLES: frozenset[str] = frozenset({
    # 快照 / 日历 / 两级分区
    "basic", "trade_cal", "index_weight",
    # 股票维度小表 (job 侧有 retry + 通知, 且已按 catalog 路径落盘)
    "daily_kline", "adj_factor", "daily_basic",
    "stock_st", "share_float", "block_trade", "margin",
    "stk_limit", "suspend_d", "stk_holdertrade",
    # 指数 / 行业
    "index_daily", "idx_anns", "sw_daily", "ci_member", "industry",
    # ETF
    "etf_basic", "etf_index", "fund_daily", "fund_adj",
    "etf_share_size", "etf_sh_cons",
    # RiceQuant
    "ricequant_basic", "ricequant_stock_minute",
    "ricequant_etf_basic", "ricequant_etf_minute",
    # 期货
    "fut_basic", "fut_daily", "fut_holding", "fut_wsr", "fut_settle",
    "fut_mapping", "ft_limit", "fut_weekly", "fut_monthly",
    "fut_index_daily", "fut_weekly_detail",
    # 期权
    "opt_basic", "opt_daily",
})

# ------------------------------------------------------------------ 主题分组
# 统一后的主题。旧的 --stock/--futures/--options/--etf/--ricequant 保留为别名。
TOPICS: dict[str, tuple[str, ...]] = {
    "quote": (
        "daily_kline", "weekly", "monthly", "adj_factor", "daily_basic",
        "stk_limit", "suspend_d", "stock_st", "share_float", "block_trade",
        "margin", "stk_holdertrade",
    ),
    "fundflow": (
        "moneyflow",
        "margin", "margin_detail", "hk_hold",
    ),
    "toplist": ("top_list", "top_inst"),
    "finance": (
        "income", "balancesheet", "cashflow", "fina_indicator", "dividend",
        "forecast", "stk_holdernumber", "top10_holders", "top10_floatholders",
        "stk_managers", "stk_rewards",
    ),
    "index": (
        "index_daily", "index_weight", "index_dailybasic", "sw_daily",
        "idx_anns", "industry", "ci_member",
    ),
    "bond": ("cb_basic", "cb_daily"),
    "fund": (
        "fund_portfolio", "fund_share",
        "fund_daily", "fund_adj", "etf_basic", "etf_index",
        "etf_share_size", "etf_sh_cons",
    ),
    "macro": ("shibor", "repurchase", "repo_daily"),
    "futures": (
        "fut_basic", "fut_daily", "fut_holding", "fut_wsr", "fut_settle",
        "fut_mapping", "ft_limit", "fut_weekly", "fut_monthly",
        "fut_index_daily", "fut_weekly_detail",
    ),
    "options": ("opt_basic", "opt_daily"),
    "etf": (
        "etf_basic", "etf_index", "fund_daily", "fund_adj",
        "etf_share_size", "etf_sh_cons",
    ),
    "ricequant": (
        "ricequant_basic", "ricequant_stock_minute",
        "ricequant_etf_basic", "ricequant_etf_minute",
    ),
    "basic": ("basic", "trade_cal"),
}

# 旧主题开关 → 新主题 (兼容老用法)
LEGACY_TOPIC_ALIASES: dict[str, str] = {
    "stock": "quote",
    "futures": "futures",
    "options": "options",
    "etf": "etf",
    "ricequant": "ricequant",
}

# 旧主题开关 (--stock/--futures/...) 各自对应的表清单。
#
# 这些开关保持**原语义不变** (与合并前的 sync 命令一致): 它们是"按市场分组"
# 的历史视图, 而不是新的功能主题。因此这里的表清单照搬旧 CLI 的常量,
# 不随 TOPICS 重构而变化 —— 否则 `--etf` 之类的老脚本会突然多拉或少拉表。
LEGACY_FLAG_TABLES: dict[str, tuple[str, ...]] = {
    "stock": (
        "basic", "trade_cal", "daily_kline", "adj_factor", "daily_basic",
        "stock_st", "suspend_d", "stk_limit", "share_float", "block_trade",
        "margin", "stk_holdertrade", "index_daily", "index_weight", "idx_anns",
        "industry", "sw_daily", "ci_member",
    ),
    "futures": (
        "fut_basic", "fut_daily", "fut_holding", "fut_wsr", "fut_settle",
        "fut_mapping", "ft_limit", "fut_weekly", "fut_monthly",
        "fut_index_daily", "fut_weekly_detail",
    ),
    "options": ("opt_basic", "opt_daily"),
    "etf": (
        "fund_daily", "fund_adj", "etf_share_size", "etf_sh_cons",
        "etf_basic", "etf_index",
    ),
    "ricequant": (
        "ricequant_basic", "ricequant_stock_minute",
        "ricequant_etf_basic", "ricequant_etf_minute",
    ),
}

ALL_TOPICS: tuple[str, ...] = tuple(TOPICS)


@dataclass(frozen=True)
class TablePlan:
    """一张表在统一 sync 中的计划。"""
    name: str                       # catalog 规范名
    topics: tuple[str, ...] = field(default_factory=tuple)   # 所属主题

    @property
    def is_batch(self) -> bool:
        """批量取数表 (吸收原 bulk 引擎的 BatchSyncJob)。"""
        return self.name in BULK_TABLES

    @property
    def is_job(self) -> bool:
        """逐日 / 快照型 job。"""
        return not self.is_batch


def normalize_table(name: str) -> str:
    """把别名/大小写不敏感的表名归一到 catalog 规范名。"""
    n = name.strip()
    low = n.lower()
    if low in TABLE_ALIASES:
        return TABLE_ALIASES[low]
    return n


def resolve_topic(name: str) -> str | None:
    """主题名归一化 (含旧开关别名); 未知主题返回 None。"""
    t = name.strip().lower()
    if t in TOPICS:
        return t
    return LEGACY_TOPIC_ALIASES.get(t)


def _build_plans() -> dict[str, TablePlan]:
    by_topic: dict[str, list[str]] = {}
    for topic, tables in TOPICS.items():
        for t in tables:
            by_topic.setdefault(t, []).append(topic)

    plans: dict[str, TablePlan] = {}
    for name in sorted(set(by_topic) | BULK_TABLES | JOB_TABLES):
        plans[name] = TablePlan(
            name=name,
            topics=tuple(sorted(by_topic.get(name, ()))),
        )
    return plans


PLANS: dict[str, TablePlan] = _build_plans()


def table_names() -> list[str]:
    return sorted(PLANS)


def tables_for_topic(topic: str) -> list[str]:
    resolved = resolve_topic(topic)
    if resolved is None:
        return []
    return list(TOPICS[resolved])


def all_table_names() -> list[str]:
    """全部表名 (catalog 规范名), 供 --all 使用。"""
    return table_names()


def bulk_table_names() -> list[str]:
    """批量取数方式的表名 (已归一为 catalog 名)。

    原 bulk 引擎的表, 现由 BatchSyncJob 承担, 与 job 引擎共用 Pipeline。
    """
    return sorted(BULK_TABLES)
