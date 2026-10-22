"""
catalog.py — central registry of all TableSpec / DailyTableSpec constants.

Both the query layer (zer0share/query/*.py) and the sync layer
(zer0share/sync/*.py) import specs from here so the two layers always
stay in sync with each other.  This module contains no logic — only
named constants.
"""

from zer0share.query.repository import DailyTableSpec, TableSpec
from zer0share.schema import (
    ADJ_FACTOR_COLS,
    BASIC_COLS,
    CI_MEMBER_COLS,
    DAILY_BASIC_COLS,
    DAILY_COLS,
    ETF_BASIC_COLS,
    ETF_INDEX_COLS,
    ETF_SHARE_SIZE_COLS,
    ETF_SH_CONS_COLS,
    FUND_ADJ_COLS,
    FUND_DAILY_COLS,
    FT_LIMIT_COLS,
    FUT_BASIC_COLS,
    FUT_DAILY_COLS,
    FUT_HOLDING_COLS,
    FUT_INDEX_DAILY_COLS,
    FUT_MAPPING_COLS,
    FUT_MONTHLY_COLS,
    FUT_SETTLE_COLS,
    FUT_WEEKLY_COLS,
    FUT_WEEKLY_DETAIL_COLS,
    FUT_WSR_COLS,
    INDEX_DAILY_COLS,
    IDX_ANNS_COLS,
    INDEX_WEIGHT_COLS,
    OPT_BASIC_COLS,
    OPT_DAILY_COLS,
    RICEQUANT_ETF_BASIC_COLS,
    RICEQUANT_ETF_MINUTE_COLS,
    STOCK_ST_COLS,
    STK_HOLDERTRADE_COLS,
    STK_LIMIT_COLS,
    SUSPEND_D_COLS,
    SHARE_FLOAT_COLS,
    BLOCK_TRADE_COLS,
    MARGIN_COLS,
    SW_CLASSIFY_COLS,
    SW_DAILY_COLS,
    SW_MEMBER_COLS,
    TRADE_CAL_COLS,
)

# ---------------------------------------------------------------------------
# Equities + calendar
# ---------------------------------------------------------------------------

BASIC_SPEC = TableSpec(
    name="basic",
    path_parts=("stock", "basic"),
    columns=BASIC_COLS,
    parquet_pattern="data.parquet",
    sync_table="basic",
    order_by="ts_code",
)

DAILY_KLINE_SPEC = DailyTableSpec(
    name="daily_kline",
    path_parts=("stock", "daily_kline"),
    columns=DAILY_COLS,
    parquet_pattern="date=*/data.parquet",
    sync_table="daily_kline",
    order_by="ts_code, trade_date",
    hive_partitioning=True,
    union_by_name=True,
    first_date="19901219",
)

ADJ_FACTOR_SPEC = DailyTableSpec(
    name="adj_factor",
    path_parts=("stock", "adj_factor"),
    columns=ADJ_FACTOR_COLS,
    parquet_pattern="date=*/data.parquet",
    sync_table="adj_factor",
    order_by="ts_code, trade_date",
    hive_partitioning=True,
    union_by_name=True,
    first_date="19901219",
)

DAILY_BASIC_SPEC = DailyTableSpec(
    name="daily_basic",
    path_parts=("stock", "daily_basic"),
    columns=DAILY_BASIC_COLS,
    parquet_pattern="date=*/data.parquet",
    sync_table="daily_basic",
    order_by="ts_code, trade_date",
    hive_partitioning=True,
    union_by_name=True,
    first_date="19901219",
)

STOCK_ST_SPEC = DailyTableSpec(
    name="stock_st",
    path_parts=("stock", "stock_st"),
    columns=STOCK_ST_COLS,
    parquet_pattern="date=*/data.parquet",
    sync_table="stock_st",
    order_by="ts_code, trade_date",
    hive_partitioning=True,
    union_by_name=True,
)

SUSPEND_D_SPEC = DailyTableSpec(
    name="suspend_d",
    path_parts=("stock", "suspend_d"),
    columns=SUSPEND_D_COLS,
    parquet_pattern="date=*/data.parquet",
    sync_table="suspend_d",
    order_by="ts_code, trade_date",
    hive_partitioning=True,
    union_by_name=True,
    first_date="20000104",
)

STK_LIMIT_SPEC = DailyTableSpec(
    name="stk_limit",
    path_parts=("stock", "stk_limit"),
    columns=STK_LIMIT_COLS,
    parquet_pattern="date=*/data.parquet",
    sync_table="stk_limit",
    order_by="ts_code, trade_date",
    hive_partitioning=True,
    union_by_name=True,
    first_date="20070104",
)

SHARE_FLOAT_SPEC = DailyTableSpec(
    name="share_float",
    path_parts=("stock", "share_float"),
    columns=SHARE_FLOAT_COLS,
    parquet_pattern="date=*/data.parquet",
    sync_table="share_float",
    order_by="ts_code, float_date",
    hive_partitioning=True,
    union_by_name=True,
    first_date="20100101",
    date_column="float_date",
)

BLOCK_TRADE_SPEC = DailyTableSpec(
    name="block_trade",
    path_parts=("stock", "block_trade"),
    columns=BLOCK_TRADE_COLS,
    parquet_pattern="date=*/data.parquet",
    sync_table="block_trade",
    order_by="ts_code, trade_date",
    hive_partitioning=True,
    union_by_name=True,
    first_date="20100101",
)

MARGIN_SPEC = DailyTableSpec(
    name="margin",
    path_parts=("stock", "margin"),
    columns=MARGIN_COLS,
    parquet_pattern="date=*/data.parquet",
    sync_table="margin",
    order_by="trade_date, exchange_id",
    hive_partitioning=True,
    union_by_name=True,
    first_date="20100331",
    code_column=None,
)

INDEX_DAILY_SPEC = DailyTableSpec(
    name="index_daily",
    path_parts=("index", "index_daily"),
    columns=INDEX_DAILY_COLS,
    parquet_pattern="date=*/data.parquet",
    sync_table="index_daily",
    order_by="ts_code, trade_date",
    hive_partitioning=True,
    union_by_name=True,
    first_date="19901219",
)

INDEX_WEIGHT_SPEC = TableSpec(
    name="index_weight",
    path_parts=("index", "index_weight"),
    columns=INDEX_WEIGHT_COLS,
    parquet_pattern="index_code=*/date=*/data.parquet",
    sync_table="index_weight",
    order_by="index_code, con_code, trade_date",
    hive_partitioning=True,
    union_by_name=True,
)

STK_HOLDERTRADE_SPEC = DailyTableSpec(
    name="stk_holdertrade",
    path_parts=("stock", "stk_holdertrade"),
    columns=STK_HOLDERTRADE_COLS,
    parquet_pattern="date=*/data.parquet",
    sync_table="stk_holdertrade",
    order_by="ann_date DESC, ts_code, holder_name",
    hive_partitioning=True,
    union_by_name=True,
    first_date="20060101",
    date_column="ann_date",
    code_column="ts_code",
)

IDX_ANNS_SPEC = DailyTableSpec(
    name="idx_anns",
    path_parts=("index", "idx_anns"),
    columns=IDX_ANNS_COLS,
    parquet_pattern="date=*/data.parquet",
    sync_table="idx_anns",
    order_by="ann_date DESC, source, title",
    hive_partitioning=True,
    union_by_name=True,
    first_date="20040101",
    date_column="ann_date",
    code_column=None,
)

# ---------------------------------------------------------------------------
# ETF
# ---------------------------------------------------------------------------

ETF_BASIC_SPEC = TableSpec(
    name="etf_basic",
    path_parts=("etf", "etf_basic"),
    columns=ETF_BASIC_COLS,
    parquet_pattern="data.parquet",
    sync_table="etf_basic",
    order_by="ts_code",
)

ETF_INDEX_SPEC = TableSpec(
    name="etf_index",
    path_parts=("etf", "etf_index"),
    columns=ETF_INDEX_COLS,
    parquet_pattern="data.parquet",
    sync_table="etf_index",
    order_by="ts_code",
)

FUND_DAILY_SPEC = DailyTableSpec(
    name="fund_daily",
    path_parts=("etf", "fund_daily"),
    columns=FUND_DAILY_COLS,
    parquet_pattern="date=*/data.parquet",
    sync_table="fund_daily",
    order_by="ts_code, trade_date",
    hive_partitioning=True,
    union_by_name=True,
    first_date="20050223",
)

FUND_ADJ_SPEC = DailyTableSpec(
    name="fund_adj",
    path_parts=("etf", "fund_adj"),
    columns=FUND_ADJ_COLS,
    parquet_pattern="date=*/data.parquet",
    sync_table="fund_adj",
    order_by="ts_code, trade_date",
    hive_partitioning=True,
    union_by_name=True,
    first_date="20100101",
)

ETF_SHARE_SIZE_SPEC = DailyTableSpec(
    name="etf_share_size",
    path_parts=("etf", "etf_share_size"),
    columns=ETF_SHARE_SIZE_COLS,
    parquet_pattern="date=*/data.parquet",
    sync_table="etf_share_size",
    order_by="ts_code, trade_date",
    hive_partitioning=True,
    union_by_name=True,
    first_date="20100101",
)

ETF_SH_CONS_SPEC = DailyTableSpec(
    name="etf_sh_cons",
    path_parts=("etf", "etf_sh_cons"),
    columns=ETF_SH_CONS_COLS,
    parquet_pattern="date=*/data.parquet",
    sync_table="etf_sh_cons",
    order_by="ts_code, trade_date, con_code",
    hive_partitioning=True,
    union_by_name=True,
    first_date="20100101",
)

# ---------------------------------------------------------------------------
# RiceQuant
# ---------------------------------------------------------------------------

RICEQUANT_ETF_BASIC_SPEC = TableSpec(
    name="ricequant_etf_basic",
    path_parts=("ricequant", "etf_basic"),
    columns=RICEQUANT_ETF_BASIC_COLS,
    parquet_pattern="data.parquet",
    sync_table="ricequant_etf_basic",
    order_by="order_book_id",
)

RICEQUANT_ETF_MINUTE_SPEC = DailyTableSpec(
    name="ricequant_etf_minute",
    path_parts=("ricequant", "etf_minute"),
    columns=RICEQUANT_ETF_MINUTE_COLS,
    parquet_pattern="date=*/data.parquet",
    sync_table="ricequant_etf_minute",
    order_by="order_book_id, datetime",
    hive_partitioning=True,
    union_by_name=True,
)

# ---------------------------------------------------------------------------
# Calendar
# ---------------------------------------------------------------------------

TRADE_CAL_SPEC = TableSpec(
    name="trade_cal",
    path_parts=("stock", "trade_cal"),
    columns=TRADE_CAL_COLS,
    parquet_pattern="exchange=*/data.parquet",
    sync_table="trade_cal",
    order_by="exchange, cal_date",
    hive_partitioning=True,
)

# ---------------------------------------------------------------------------
# Industry
# ---------------------------------------------------------------------------

SW_CLASSIFY_SPEC = TableSpec(
    name="sw_classify",
    path_parts=("stock", "industry", "sw_classify"),
    columns=SW_CLASSIFY_COLS,
    parquet_pattern="data.parquet",
    sync_table="industry",
    order_by="industry_code",
)

SW_MEMBER_SPEC = TableSpec(
    name="sw_member",
    path_parts=("stock", "industry", "sw_member"),
    columns=SW_MEMBER_COLS,
    parquet_pattern="data.parquet",
    sync_table="industry",
    order_by="ts_code, l1_code",
)

SW_DAILY_SPEC = DailyTableSpec(
    name="sw_daily",
    path_parts=("stock", "industry", "sw_daily"),
    columns=SW_DAILY_COLS,
    parquet_pattern="date=*/data.parquet",
    sync_table="sw_daily",
    order_by="ts_code, trade_date",
    hive_partitioning=True,
    union_by_name=True,
    first_date="20000104",
)

CI_MEMBER_SPEC = TableSpec(
    name="ci_member",
    path_parts=("stock", "industry", "ci_member"),
    columns=CI_MEMBER_COLS,
    parquet_pattern="data.parquet",
    sync_table="ci_member",
    order_by="ts_code, l1_code",
)

# ---------------------------------------------------------------------------
# Futures
# ---------------------------------------------------------------------------

FUT_BASIC_SPEC = TableSpec(
    name="fut_basic",
    path_parts=("futures", "fut_basic"),
    columns=FUT_BASIC_COLS,
    parquet_pattern="data.parquet",
    sync_table="fut_basic",
    order_by="ts_code",
)

FUT_DAILY_SPEC = DailyTableSpec(
    name="fut_daily",
    path_parts=("futures", "fut_daily"),
    columns=FUT_DAILY_COLS,
    parquet_pattern="date=*/data.parquet",
    sync_table="fut_daily",
    order_by="ts_code, trade_date",
    hive_partitioning=True,
    union_by_name=True,
    first_date="19950417",
)

FUT_HOLDING_SPEC = DailyTableSpec(
    name="fut_holding",
    path_parts=("futures", "fut_holding"),
    columns=FUT_HOLDING_COLS,
    parquet_pattern="date=*/data.parquet",
    sync_table="fut_holding",
    order_by="trade_date, symbol, broker",
    hive_partitioning=True,
    union_by_name=True,
    code_column=None,
    first_date="20020101",
)

FUT_WSR_SPEC = DailyTableSpec(
    name="fut_wsr",
    path_parts=("futures", "fut_wsr"),
    columns=FUT_WSR_COLS,
    parquet_pattern="date=*/data.parquet",
    sync_table="fut_wsr",
    order_by="trade_date, symbol, warehouse",
    hive_partitioning=True,
    union_by_name=True,
    code_column=None,
    first_date="20070101",
)

FUT_SETTLE_SPEC = DailyTableSpec(
    name="fut_settle",
    path_parts=("futures", "fut_settle"),
    columns=FUT_SETTLE_COLS,
    parquet_pattern="date=*/data.parquet",
    sync_table="fut_settle",
    order_by="ts_code, trade_date",
    hive_partitioning=True,
    union_by_name=True,
    first_date="20120101",
)

FUT_MAPPING_SPEC = DailyTableSpec(
    name="fut_mapping",
    path_parts=("futures", "fut_mapping"),
    columns=FUT_MAPPING_COLS,
    parquet_pattern="date=*/data.parquet",
    sync_table="fut_mapping",
    order_by="ts_code, trade_date",
    hive_partitioning=True,
    union_by_name=True,
    first_date="19950417",
)

FT_LIMIT_SPEC = DailyTableSpec(
    name="ft_limit",
    path_parts=("futures", "ft_limit"),
    columns=FT_LIMIT_COLS,
    parquet_pattern="date=*/data.parquet",
    sync_table="ft_limit",
    order_by="ts_code, trade_date",
    hive_partitioning=True,
    union_by_name=True,
    first_date="20050101",
)

FUT_WEEKLY_SPEC = DailyTableSpec(
    name="fut_weekly",
    path_parts=("futures", "fut_weekly"),
    columns=FUT_WEEKLY_COLS,
    parquet_pattern="date=*/data.parquet",
    sync_table="fut_weekly",
    order_by="ts_code, trade_date",
    hive_partitioning=True,
    union_by_name=True,
    first_date="19950417",
)

FUT_MONTHLY_SPEC = DailyTableSpec(
    name="fut_monthly",
    path_parts=("futures", "fut_monthly"),
    columns=FUT_MONTHLY_COLS,
    parquet_pattern="date=*/data.parquet",
    sync_table="fut_monthly",
    order_by="ts_code, trade_date",
    hive_partitioning=True,
    union_by_name=True,
    first_date="19950417",
)

FUT_INDEX_DAILY_SPEC = DailyTableSpec(
    name="fut_index_daily",
    path_parts=("futures", "fut_index_daily"),
    columns=FUT_INDEX_DAILY_COLS,
    parquet_pattern="date=*/data.parquet",
    sync_table="fut_index_daily",
    order_by="ts_code, trade_date",
    hive_partitioning=True,
    union_by_name=True,
    first_date="20060101",
)

FUT_WEEKLY_DETAIL_SPEC = DailyTableSpec(
    name="fut_weekly_detail",
    path_parts=("futures", "fut_weekly_detail"),
    columns=FUT_WEEKLY_DETAIL_COLS,
    parquet_pattern="date=*/data.parquet",
    sync_table="fut_weekly_detail",
    order_by="week_date, exchange, prd",
    hive_partitioning=True,
    union_by_name=True,
    date_column="week_date",
    code_column=None,
    first_date="20151201",
)

# ---------------------------------------------------------------------------
# Options
# ---------------------------------------------------------------------------

OPT_BASIC_SPEC = TableSpec(
    name="opt_basic",
    path_parts=("options", "opt_basic"),
    columns=OPT_BASIC_COLS,
    parquet_pattern="data.parquet",
    sync_table="opt_basic",
    order_by="ts_code",
)

OPT_DAILY_SPEC = DailyTableSpec(
    name="opt_daily",
    path_parts=("options", "opt_daily"),
    columns=OPT_DAILY_COLS,
    parquet_pattern="date=*/data.parquet",
    sync_table="opt_daily",
    order_by="ts_code, trade_date",
    hive_partitioning=True,
    union_by_name=True,
    first_date="20150209",
)

# --------------------------------------------------------------------------
# 吸收 bulk 引擎后新增的表 (原 bulk.build_tables 注册项)
# 列名来自 Tushare 接口真实返回; 分区路径由 catalog 统一管理,
# 不再由 bulk 侧单文件路径决定。
# --------------------------------------------------------------------------

WEEKLY_COLS = [
    "ts_code", "trade_date", "close",
    "open", "high", "low",
    "pre_close", "change", "pct_chg",
    "vol", "amount",
]

WEEKLY_SPEC = DailyTableSpec(
    name="weekly",
    path_parts=("stock", "weekly"),
    columns=WEEKLY_COLS,
    parquet_pattern="date=*/data.parquet",
    hive_partitioning=True,
    union_by_name=True,
    sync_table="weekly",
    order_by="ts_code, trade_date",
    date_column="trade_date",
    first_date="19900101",
)

MONTHLY_COLS = [
    "ts_code", "trade_date", "close",
    "open", "high", "low",
    "pre_close", "change", "pct_chg",
    "vol", "amount",
]

MONTHLY_SPEC = DailyTableSpec(
    name="monthly",
    path_parts=("stock", "monthly"),
    columns=MONTHLY_COLS,
    parquet_pattern="date=*/data.parquet",
    hive_partitioning=True,
    union_by_name=True,
    sync_table="monthly",
    order_by="ts_code, trade_date",
    date_column="trade_date",
    first_date="19900101",
)

MONEYFLOW_COLS = [
    "ts_code", "trade_date", "buy_sm_vol",
    "buy_sm_amount", "sell_sm_vol", "sell_sm_amount",
    "buy_md_vol", "buy_md_amount", "sell_md_vol",
    "sell_md_amount", "buy_lg_vol", "buy_lg_amount",
    "sell_lg_vol", "sell_lg_amount", "buy_elg_vol",
    "buy_elg_amount", "sell_elg_vol", "sell_elg_amount",
    "net_mf_vol", "net_mf_amount",
]

MONEYFLOW_SPEC = DailyTableSpec(
    name="moneyflow",
    path_parts=("stock", "moneyflow"),
    columns=MONEYFLOW_COLS,
    parquet_pattern="date=*/data.parquet",
    hive_partitioning=True,
    union_by_name=True,
    sync_table="moneyflow",
    order_by="ts_code, trade_date",
    date_column="trade_date",
    first_date="20100101",
)

MARGIN_DETAIL_COLS = [
    "trade_date", "ts_code", "rzye",
    "rqye", "rzmre", "rqyl",
    "rzche", "rqchl", "rqmcl",
    "rzrqye",
]

MARGIN_DETAIL_SPEC = DailyTableSpec(
    name="margin_detail",
    path_parts=("stock", "margin_detail"),
    columns=MARGIN_DETAIL_COLS,
    parquet_pattern="date=*/data.parquet",
    hive_partitioning=True,
    union_by_name=True,
    sync_table="margin_detail",
    order_by="ts_code, trade_date",
    date_column="trade_date",
    first_date="20100331",
)

HK_HOLD_COLS = [
    "code", "trade_date", "ts_code",
    "name", "vol", "ratio",
    "exchange",
]

HK_HOLD_SPEC = DailyTableSpec(
    name="hk_hold",
    path_parts=("stock", "hk_hold"),
    columns=HK_HOLD_COLS,
    parquet_pattern="date=*/data.parquet",
    hive_partitioning=True,
    union_by_name=True,
    sync_table="hk_hold",
    order_by="ts_code, trade_date",
    date_column="trade_date",
    first_date="20141117",
)

TOP_LIST_COLS = [
    "trade_date", "ts_code", "name",
    "close", "pct_change", "turnover_rate",
    "amount", "l_sell", "l_buy",
    "l_amount", "net_amount", "net_rate",
    "amount_rate", "float_values", "reason",
]

TOP_LIST_SPEC = DailyTableSpec(
    name="top_list",
    path_parts=("stock", "top_list"),
    columns=TOP_LIST_COLS,
    parquet_pattern="date=*/data.parquet",
    hive_partitioning=True,
    union_by_name=True,
    sync_table="top_list",
    order_by="ts_code, trade_date",
    date_column="trade_date",
    first_date="20050101",
)

TOP_INST_COLS = [
    "trade_date", "ts_code", "exalter",
    "buy", "buy_rate", "sell",
    "sell_rate", "net_buy", "side",
    "reason",
]

TOP_INST_SPEC = DailyTableSpec(
    name="top_inst",
    path_parts=("stock", "top_inst"),
    columns=TOP_INST_COLS,
    parquet_pattern="date=*/data.parquet",
    hive_partitioning=True,
    union_by_name=True,
    sync_table="top_inst",
    order_by="ts_code, trade_date",
    date_column="trade_date",
    first_date="20050101",
)

CB_DAILY_COLS = [
    "ts_code", "trade_date", "pre_close",
    "open", "high", "low",
    "close", "change", "pct_chg",
    "vol", "amount",
]

CB_DAILY_SPEC = DailyTableSpec(
    name="cb_daily",
    path_parts=("stock", "cb_daily"),
    columns=CB_DAILY_COLS,
    parquet_pattern="date=*/data.parquet",
    hive_partitioning=True,
    union_by_name=True,
    sync_table="cb_daily",
    order_by="ts_code, trade_date",
    date_column="trade_date",
    first_date="20100101",
)

INDEX_DAILYBASIC_COLS = [
    "ts_code", "trade_date", "total_mv",
    "float_mv", "total_share", "float_share",
    "free_share", "turnover_rate", "turnover_rate_f",
    "pe", "pe_ttm", "pb",
]

INDEX_DAILYBASIC_SPEC = DailyTableSpec(
    name="index_dailybasic",
    path_parts=("index", "index_dailybasic"),
    columns=INDEX_DAILYBASIC_COLS,
    parquet_pattern="date=*/data.parquet",
    hive_partitioning=True,
    union_by_name=True,
    sync_table="index_dailybasic",
    order_by="ts_code, trade_date",
    date_column="trade_date",
    first_date="20050101",
)

INCOME_COLS = [
    "ts_code", "ann_date", "f_ann_date",
    "end_date", "report_type", "comp_type",
    "end_type", "basic_eps", "diluted_eps",
    "total_revenue", "revenue", "int_income",
    "prem_earned", "comm_income", "n_commis_income",
    "n_oth_income", "n_oth_b_income", "prem_income",
    "out_prem", "une_prem_reser", "reins_income",
    "n_sec_tb_income", "n_sec_uw_income", "n_asset_mg_income",
    "oth_b_income", "fv_value_chg_gain", "invest_income",
    "ass_invest_income", "forex_gain", "total_cogs",
    "oper_cost", "int_exp", "comm_exp",
    "biz_tax_surchg", "sell_exp", "admin_exp",
    "fin_exp", "assets_impair_loss", "prem_refund",
    "compens_payout", "reser_insur_liab", "div_payt",
    "reins_exp", "oper_exp", "compens_payout_refu",
    "insur_reser_refu", "reins_cost_refund", "other_bus_cost",
    "operate_profit", "non_oper_income", "non_oper_exp",
    "nca_disploss", "total_profit", "income_tax",
    "n_income", "n_income_attr_p", "minority_gain",
    "oth_compr_income", "t_compr_income", "compr_inc_attr_p",
    "compr_inc_attr_m_s", "ebit", "ebitda",
    "insurance_exp", "undist_profit", "distable_profit",
    "rd_exp", "fin_exp_int_exp", "fin_exp_int_inc",
    "transfer_surplus_rese", "transfer_housing_imprest", "transfer_oth",
    "adj_lossgain", "withdra_legal_surplus", "withdra_legal_pubfund",
    "withdra_biz_devfund", "withdra_rese_fund", "withdra_oth_ersu",
    "workers_welfare", "distr_profit_shrhder", "prfshare_payable_dvd",
    "comshare_payable_dvd", "capit_comstock_div", "continued_net_profit",
    "update_flag",
]

INCOME_SPEC = DailyTableSpec(
    name="income",
    path_parts=("stock", "income"),
    columns=INCOME_COLS,
    parquet_pattern="date=*/data.parquet",
    hive_partitioning=True,
    union_by_name=True,
    sync_table="income",
    order_by="ts_code, end_date, report_type",
    date_column="end_date",
    first_date="19890101",
)

BALANCESHEET_COLS = [
    "ts_code", "ann_date", "f_ann_date",
    "end_date", "report_type", "comp_type",
    "end_type", "total_share", "cap_rese",
    "undistr_porfit", "surplus_rese", "special_rese",
    "money_cap", "trad_asset", "notes_receiv",
    "accounts_receiv", "oth_receiv", "prepayment",
    "div_receiv", "int_receiv", "inventories",
    "amor_exp", "nca_within_1y", "sett_rsrv",
    "loanto_oth_bank_fi", "premium_receiv", "reinsur_receiv",
    "reinsur_res_receiv", "pur_resale_fa", "oth_cur_assets",
    "total_cur_assets", "fa_avail_for_sale", "htm_invest",
    "lt_eqt_invest", "invest_real_estate", "time_deposits",
    "oth_assets", "lt_rec", "fix_assets",
    "cip", "const_materials", "fixed_assets_disp",
    "produc_bio_assets", "oil_and_gas_assets", "intan_assets",
    "r_and_d", "goodwill", "lt_amor_exp",
    "defer_tax_assets", "decr_in_disbur", "oth_nca",
    "total_nca", "cash_reser_cb", "depos_in_oth_bfi",
    "prec_metals", "deriv_assets", "rr_reins_une_prem",
    "rr_reins_outstd_cla", "rr_reins_lins_liab", "rr_reins_lthins_liab",
    "refund_depos", "ph_pledge_loans", "refund_cap_depos",
    "indep_acct_assets", "client_depos", "client_prov",
    "transac_seat_fee", "invest_as_receiv", "total_assets",
    "lt_borr", "st_borr", "cb_borr",
    "depos_ib_deposits", "loan_oth_bank", "trading_fl",
    "notes_payable", "acct_payable", "adv_receipts",
    "sold_for_repur_fa", "comm_payable", "payroll_payable",
    "taxes_payable", "int_payable", "div_payable",
    "oth_payable", "acc_exp", "deferred_inc",
    "st_bonds_payable", "payable_to_reinsurer", "rsrv_insur_cont",
    "acting_trading_sec", "acting_uw_sec", "non_cur_liab_due_1y",
    "oth_cur_liab", "total_cur_liab", "bond_payable",
    "lt_payable", "specific_payables", "estimated_liab",
    "defer_tax_liab", "defer_inc_non_cur_liab", "oth_ncl",
    "total_ncl", "depos_oth_bfi", "deriv_liab",
    "depos", "agency_bus_liab", "oth_liab",
    "prem_receiv_adva", "depos_received", "ph_invest",
    "reser_une_prem", "reser_outstd_claims", "reser_lins_liab",
    "reser_lthins_liab", "indept_acc_liab", "pledge_borr",
    "indem_payable", "policy_div_payable", "total_liab",
    "treasury_share", "ordin_risk_reser", "forex_differ",
    "invest_loss_unconf", "minority_int", "total_hldr_eqy_exc_min_int",
    "total_hldr_eqy_inc_min_int", "total_liab_hldr_eqy", "lt_payroll_payable",
    "oth_comp_income", "oth_eqt_tools", "oth_eqt_tools_p_shr",
    "lending_funds", "acc_receivable", "st_fin_payable",
    "payables", "hfs_assets", "hfs_sales",
    "cost_fin_assets", "fair_value_fin_assets", "contract_assets",
    "contract_liab", "accounts_receiv_bill", "accounts_pay",
    "oth_rcv_total", "fix_assets_total", "cip_total",
    "oth_pay_total", "long_pay_total", "debt_invest",
    "oth_debt_invest", "update_flag",
]

BALANCESHEET_SPEC = DailyTableSpec(
    name="balancesheet",
    path_parts=("stock", "balancesheet"),
    columns=BALANCESHEET_COLS,
    parquet_pattern="date=*/data.parquet",
    hive_partitioning=True,
    union_by_name=True,
    sync_table="balancesheet",
    order_by="ts_code, end_date, report_type",
    date_column="end_date",
    first_date="19890101",
)

CASHFLOW_COLS = [
    "ts_code", "ann_date", "f_ann_date",
    "end_date", "comp_type", "report_type",
    "end_type", "net_profit", "finan_exp",
    "c_fr_sale_sg", "recp_tax_rends", "n_depos_incr_fi",
    "n_incr_loans_cb", "n_inc_borr_oth_fi", "prem_fr_orig_contr",
    "n_incr_insured_dep", "n_reinsur_prem", "n_incr_disp_tfa",
    "ifc_cash_incr", "n_incr_disp_faas", "n_incr_loans_oth_bank",
    "n_cap_incr_repur", "c_fr_oth_operate_a", "c_inf_fr_operate_a",
    "c_paid_goods_s", "c_paid_to_for_empl", "c_paid_for_taxes",
    "n_incr_clt_loan_adv", "n_incr_dep_cbob", "c_pay_claims_orig_inco",
    "pay_handling_chrg", "pay_comm_insur_plcy", "oth_cash_pay_oper_act",
    "st_cash_out_act", "n_cashflow_act", "oth_recp_ral_inv_act",
    "c_disp_withdrwl_invest", "c_recp_return_invest", "n_recp_disp_fiolta",
    "n_recp_disp_sobu", "stot_inflows_inv_act", "c_pay_acq_const_fiolta",
    "c_paid_invest", "n_disp_subs_oth_biz", "oth_pay_ral_inv_act",
    "n_incr_pledge_loan", "stot_out_inv_act", "n_cashflow_inv_act",
    "c_recp_borrow", "proc_issue_bonds", "oth_cash_recp_ral_fnc_act",
    "stot_cash_in_fnc_act", "free_cashflow", "c_prepay_amt_borr",
    "c_pay_dist_dpcp_int_exp", "incl_dvd_profit_paid_sc_ms", "oth_cashpay_ral_fnc_act",
    "stot_cashout_fnc_act", "n_cash_flows_fnc_act", "eff_fx_flu_cash",
    "n_incr_cash_cash_equ", "c_cash_equ_beg_period", "c_cash_equ_end_period",
    "c_recp_cap_contrib", "incl_cash_rec_saims", "uncon_invest_loss",
    "prov_depr_assets", "depr_fa_coga_dpba", "amort_intang_assets",
    "lt_amort_deferred_exp", "decr_deferred_exp", "incr_acc_exp",
    "loss_disp_fiolta", "loss_scr_fa", "loss_fv_chg",
    "invest_loss", "decr_def_inc_tax_assets", "incr_def_inc_tax_liab",
    "decr_inventories", "decr_oper_payable", "incr_oper_payable",
    "others", "im_net_cashflow_oper_act", "conv_debt_into_cap",
    "conv_copbonds_due_within_1y", "fa_fnc_leases", "im_n_incr_cash_equ",
    "net_dism_capital_add", "net_cash_rece_sec", "credit_impa_loss",
    "use_right_asset_dep", "oth_loss_asset", "end_bal_cash",
    "beg_bal_cash", "end_bal_cash_equ", "beg_bal_cash_equ",
    "update_flag",
]

CASHFLOW_SPEC = DailyTableSpec(
    name="cashflow",
    path_parts=("stock", "cashflow"),
    columns=CASHFLOW_COLS,
    parquet_pattern="date=*/data.parquet",
    hive_partitioning=True,
    union_by_name=True,
    sync_table="cashflow",
    order_by="ts_code, end_date, report_type",
    date_column="end_date",
    first_date="19890101",
)

FINA_INDICATOR_COLS = [
    "ts_code", "ann_date", "end_date",
    "eps", "dt_eps", "total_revenue_ps",
    "revenue_ps", "capital_rese_ps", "surplus_rese_ps",
    "undist_profit_ps", "extra_item", "profit_dedt",
    "gross_margin", "current_ratio", "quick_ratio",
    "cash_ratio", "ar_turn", "ca_turn",
    "fa_turn", "assets_turn", "op_income",
    "ebit", "ebitda", "fcff",
    "fcfe", "current_exint", "noncurrent_exint",
    "interestdebt", "netdebt", "tangible_asset",
    "working_capital", "networking_capital", "invest_capital",
    "retained_earnings", "diluted2_eps", "bps",
    "ocfps", "retainedps", "cfps",
    "ebit_ps", "fcff_ps", "fcfe_ps",
    "netprofit_margin", "grossprofit_margin", "cogs_of_sales",
    "expense_of_sales", "profit_to_gr", "saleexp_to_gr",
    "adminexp_of_gr", "finaexp_of_gr", "impai_ttm",
    "gc_of_gr", "op_of_gr", "ebit_of_gr",
    "roe", "roe_waa", "roe_dt",
    "roa", "npta", "roic",
    "roe_yearly", "roa2_yearly", "debt_to_assets",
    "assets_to_eqt", "dp_assets_to_eqt", "ca_to_assets",
    "nca_to_assets", "tbassets_to_totalassets", "int_to_talcap",
    "eqt_to_talcapital", "currentdebt_to_debt", "longdeb_to_debt",
    "ocf_to_shortdebt", "debt_to_eqt", "eqt_to_debt",
    "eqt_to_interestdebt", "tangibleasset_to_debt", "tangasset_to_intdebt",
    "tangibleasset_to_netdebt", "ocf_to_debt", "turn_days",
    "roa_yearly", "roa_dp", "fixed_assets",
    "profit_to_op", "q_saleexp_to_gr", "q_gc_to_gr",
    "q_roe", "q_dt_roe", "q_npta",
    "q_ocf_to_sales", "basic_eps_yoy", "dt_eps_yoy",
    "cfps_yoy", "op_yoy", "ebt_yoy",
    "netprofit_yoy", "dt_netprofit_yoy", "ocf_yoy",
    "roe_yoy", "bps_yoy", "assets_yoy",
    "eqt_yoy", "tr_yoy", "or_yoy",
    "q_sales_yoy", "q_op_qoq", "equity_yoy",
]

FINA_INDICATOR_SPEC = DailyTableSpec(
    name="fina_indicator",
    path_parts=("stock", "fina_indicator"),
    columns=FINA_INDICATOR_COLS,
    parquet_pattern="date=*/data.parquet",
    hive_partitioning=True,
    union_by_name=True,
    sync_table="fina_indicator",
    order_by="ts_code, end_date, report_type",
    date_column="end_date",
    first_date="19890101",
)

DIVIDEND_COLS = [
    "ts_code", "end_date", "ann_date",
    "div_proc", "stk_div", "stk_bo_rate",
    "stk_co_rate", "cash_div", "cash_div_tax",
    "record_date", "ex_date", "pay_date",
    "div_listdate", "imp_ann_date",
]

DIVIDEND_SPEC = DailyTableSpec(
    name="dividend",
    path_parts=("stock", "dividend"),
    columns=DIVIDEND_COLS,
    parquet_pattern="date=*/data.parquet",
    hive_partitioning=True,
    union_by_name=True,
    sync_table="dividend",
    order_by="ts_code, end_date, ann_date, div_proc",
    date_column="end_date",
    first_date="19900101",
)

FORECAST_COLS = [
    "ts_code", "ann_date", "end_date",
    "type", "p_change_min", "p_change_max",
    "net_profit_min", "net_profit_max", "last_parent_net",
    "first_ann_date", "summary", "change_reason",
    "update_flag",
]

FORECAST_SPEC = DailyTableSpec(
    name="forecast",
    path_parts=("stock", "forecast"),
    columns=FORECAST_COLS,
    parquet_pattern="date=*/data.parquet",
    hive_partitioning=True,
    union_by_name=True,
    sync_table="forecast",
    order_by="ts_code, ann_date",
    date_column="end_date",
    first_date="20000101",
)

STK_HOLDERNUMBER_COLS = [
    "ts_code", "ann_date", "end_date",
    "holder_num",
]

STK_HOLDERNUMBER_SPEC = DailyTableSpec(
    name="stk_holdernumber",
    path_parts=("stock", "stk_holdernumber"),
    columns=STK_HOLDERNUMBER_COLS,
    parquet_pattern="date=*/data.parquet",
    hive_partitioning=True,
    union_by_name=True,
    sync_table="stk_holdernumber",
    order_by="ts_code, end_date",
    date_column="end_date",
    first_date="20000101",
)

TOP10_HOLDERS_COLS = [
    "ts_code", "ann_date", "end_date",
    "holder_name", "hold_amount", "hold_ratio",
    "hold_float_ratio", "hold_change", "holder_type",
]

TOP10_HOLDERS_SPEC = DailyTableSpec(
    name="top10_holders",
    path_parts=("stock", "top10_holders"),
    columns=TOP10_HOLDERS_COLS,
    parquet_pattern="date=*/data.parquet",
    hive_partitioning=True,
    union_by_name=True,
    sync_table="top10_holders",
    order_by="ts_code, end_date, holder_name",
    date_column="end_date",
    first_date="20000101",
)

TOP10_FLOATHOLDERS_COLS = [
    "ts_code", "ann_date", "end_date",
    "holder_name", "hold_amount", "hold_ratio",
    "hold_float_ratio", "hold_change", "holder_type",
]

TOP10_FLOATHOLDERS_SPEC = DailyTableSpec(
    name="top10_floatholders",
    path_parts=("stock", "top10_floatholders"),
    columns=TOP10_FLOATHOLDERS_COLS,
    parquet_pattern="date=*/data.parquet",
    hive_partitioning=True,
    union_by_name=True,
    sync_table="top10_floatholders",
    order_by="ts_code, end_date, holder_name",
    date_column="end_date",
    first_date="20000101",
)

STK_MANAGERS_COLS = [
    "ts_code", "ann_date", "name",
    "gender", "lev", "title",
    "edu", "national", "birthday",
    "begin_date", "end_date",
]

STK_MANAGERS_SPEC = DailyTableSpec(
    name="stk_managers",
    path_parts=("stock", "stk_managers"),
    columns=STK_MANAGERS_COLS,
    parquet_pattern="date=*/data.parquet",
    hive_partitioning=True,
    union_by_name=True,
    sync_table="stk_managers",
    order_by="ts_code, ann_date, name",
    date_column="ann_date",
    first_date="20000101",
)

STK_REWARDS_COLS = [
    "ts_code", "ann_date", "end_date",
    "name", "title", "reward",
    "hold_vol",
]

STK_REWARDS_SPEC = DailyTableSpec(
    name="stk_rewards",
    path_parts=("stock", "stk_rewards"),
    columns=STK_REWARDS_COLS,
    parquet_pattern="date=*/data.parquet",
    hive_partitioning=True,
    union_by_name=True,
    sync_table="stk_rewards",
    order_by="ts_code, end_date, name",
    date_column="end_date",
    first_date="20000101",
)

FUND_PORTFOLIO_COLS = [
    "ts_code", "ann_date", "end_date",
    "symbol", "mkv", "amount",
    "stk_mkv_ratio", "stk_float_ratio",
]

FUND_PORTFOLIO_SPEC = DailyTableSpec(
    name="fund_portfolio",
    path_parts=("fund", "fund_portfolio"),
    columns=FUND_PORTFOLIO_COLS,
    parquet_pattern="date=*/data.parquet",
    hive_partitioning=True,
    union_by_name=True,
    sync_table="fund_portfolio",
    order_by="ts_code, end_date, symbol",
    date_column="end_date",
    first_date="20050101",
)

SHIBOR_COLS = [
    "date", "on", "1w",
    "2w", "1m", "3m",
    "6m", "9m", "1y",
]

SHIBOR_SPEC = DailyTableSpec(
    name="shibor",
    path_parts=("macro", "shibor"),
    columns=SHIBOR_COLS,
    parquet_pattern="date=*/data.parquet",
    hive_partitioning=True,
    union_by_name=True,
    sync_table="shibor",
    # shibor 接口的日期列叫 date, 不是 trade_date (实测列名 date/on/1w/2w/...)
    order_by="date",
    date_column="date",
    first_date="20061001",
)

REPURCHASE_COLS = [
    "ts_code", "ann_date", "end_date",
    "proc", "exp_date", "vol",
    "amount", "high_limit", "low_limit",
]

REPURCHASE_SPEC = DailyTableSpec(
    name="repurchase",
    path_parts=("macro", "repurchase"),
    columns=REPURCHASE_COLS,
    parquet_pattern="date=*/data.parquet",
    hive_partitioning=True,
    union_by_name=True,
    sync_table="repurchase",
    # 逆回购公告无交易日列: 接口只认 start_date/end_date 窗口, 返回
    # ann_date/end_date 公告与到期日。分区键取 ann_date。
    order_by="ann_date",
    date_column="ann_date",
    first_date="20070101",
)

CB_BASIC_COLS = [
    "ts_code", "bond_full_name", "bond_short_name",
    "cb_type", "cb_code", "stk_code",
    "stk_short_name", "maturity", "par",
    "issue_price", "issue_size", "remain_size",
    "value_date", "maturity_date", "rate_type",
    "coupon_rate", "add_rate", "pay_per_year",
    "list_date", "delist_date", "exchange",
    "conv_start_date", "conv_end_date", "conv_stop_date",
    "first_conv_price", "conv_price", "rate_clause",
]

CB_BASIC_SPEC = TableSpec(
    name="cb_basic",
    path_parts=("stock", "cb_basic"),
    columns=CB_BASIC_COLS,
    parquet_pattern="data.parquet",
    sync_table="cb_basic",
    order_by="ts_code",
)

FUND_NAV_COLS = [
    "ts_code", "ann_date", "nav_date",
    "unit_nav", "accum_nav", "accum_div",
    "net_asset", "adj_nav", "update_flag",
]

FUND_NAV_SPEC = TableSpec(
    name="fund_nav",
    path_parts=("fund", "fund_nav"),
    columns=FUND_NAV_COLS,
    parquet_pattern="data.parquet",
    sync_table="fund_nav",
    order_by="ts_code, nav_date",
)

FUND_SHARE_COLS = [
    "ts_code", "trade_date", "fd_share",
    "fund_type", "market",
]

FUND_SHARE_SPEC = TableSpec(
    name="fund_share",
    path_parts=("fund", "fund_share"),
    columns=FUND_SHARE_COLS,
    parquet_pattern="data.parquet",
    sync_table="fund_share",
    order_by="ts_code, trade_date",
)

REPO_DAILY_COLS = [
    "ts_code", "trade_date", "repo_maturity",
    "pre_close", "open", "high",
    "low", "close", "weight",
    "weight_r", "amount", "num",
]

REPO_DAILY_SPEC = TableSpec(
    name="repo_daily",
    path_parts=("macro", "repo_daily"),
    columns=REPO_DAILY_COLS,
    parquet_pattern="data.parquet",
    sync_table="repo_daily",
    order_by="trade_date",
)
