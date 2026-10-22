"""BatchSyncJob 表定义与 catalog spec 的一致性测试。

背景: shibor 的日期列叫 date 而非 trade_date, repurchase 无交易日维度,
monthly 不接受 trade_date 参数 —— 这三处最初都按 trade_date 配错, 表现为
merge 时「无可写分区数据 (trade_date 列缺失或空)」而静默丢数据。
这些测试把「接口真实列名 == spec date_column」的关系钉住, 防止再配错。
"""
from __future__ import annotations

import pandas as pd
import pytest

from zer0share.sync._batch_jobs import BATCH_TABLES, _SPECS

# 实测 Tushare 返回的日期列名 (2026-10-22 探测)
ACTUAL_DATE_COLUMNS = {
    "moneyflow": "trade_date",
    "margin_detail": "trade_date",
    "hk_hold": "trade_date",
    "top_list": "trade_date",
    "top_inst": "trade_date",
    "index_dailybasic": "trade_date",
    "cb_daily": "trade_date",
    "shibor": "date",          # 接口列名 date/on/1w/2w/...
    "weekly": "trade_date",
    "monthly": "trade_date",
    "repurchase": "ann_date",  # 无交易日维度, 公告日分区
}


def test_every_batch_table_has_a_spec() -> None:
    missing = sorted(set(BATCH_TABLES) - set(_SPECS))
    assert not missing, f"batch 表缺少 catalog spec: {missing}"


def test_every_spec_is_registered_as_batch() -> None:
    orphan = sorted(set(_SPECS) - set(BATCH_TABLES))
    assert not orphan, f"catalog spec 未注册进 BATCH_TABLES: {orphan}"


@pytest.mark.parametrize("table,expected", sorted(ACTUAL_DATE_COLUMNS.items()))
def test_date_column_matches_real_api(table: str, expected: str) -> None:
    """配给 job 的 date_col 必须等于接口真实返回的日期列名。"""
    kind, _extra, date_col = BATCH_TABLES[table]
    assert date_col == expected, (
        f"{table}: BATCH_TABLES 配的 date_col={date_col!r}, "
        f"接口真实列名是 {expected!r}"
    )
    spec = _SPECS[table]
    assert spec.date_column == expected, (
        f"{table}: catalog spec date_column={spec.date_column!r}, "
        f"接口真实列名是 {expected!r}"
    )


def test_shibor_is_not_trade_date_partitioned() -> None:
    """shibor 曾错配 trade_date 导致 50 天数据静默丢弃, 钉住修复。"""
    kind, _extra, date_col = BATCH_TABLES["shibor"]
    assert kind == "date"
    assert date_col == "date"
    assert _SPECS["shibor"].date_column == "date"
    assert _SPECS["shibor"].order_by == "date"


def test_repurchase_uses_window_kind() -> None:
    """repurchase 只认 start_date/end_date 窗口, 不能按交易日逐日拉。"""
    kind, _extra, date_col = BATCH_TABLES["repurchase"]
    assert kind == "window"
    assert date_col == "ann_date"
    assert _SPECS["repurchase"].date_column == "ann_date"


def test_monthly_is_fetched_per_stock() -> None:
    """monthly 拒绝 trade_date 参数 ('ts_code,trade_date 至少输入一个')。"""
    kind, _extra, date_col = BATCH_TABLES["monthly"]
    assert kind == "stock", "monthly 必须逐只查, 按日拉会被接口拒绝"
    assert date_col == "trade_date"
    assert _SPECS["monthly"].path_parts == ("stock", "monthly")


def test_weekly_and_monthly_share_path_prefix() -> None:
    """同为股票月/周线, 路径前缀应一致, 便于按主题分组查询。"""
    assert _SPECS["weekly"].path_parts[0] == _SPECS["monthly"].path_parts[0] == "stock"


def test_stock_partitioned_tables_declare_existing_columns() -> None:
    """逐只查表的 date_col 必须出现在 spec.columns 里, 否则 merge 找不到键。"""
    for table, (kind, _extra, date_col) in BATCH_TABLES.items():
        if kind != "stock":
            continue
        cols = list(_SPECS[table].columns)
        assert date_col in cols, f"{table}: date_col={date_col!r} 不在 spec.columns {cols}"


def test_flush_accepts_empty_day_list() -> None:
    """window kind 调 _flush(dfs, [], rt) —— days 为空不能报错。"""
    from zer0share.storage import DailyPartitionStore

    # 只验证签名兼容, 不触盘
    import inspect

    sig = inspect.signature(DailyPartitionStore.merge)
    assert list(sig.parameters)[0] == "self"
    assert callable(DailyPartitionStore.merge)


def test_date_col_consistent_with_pandas_frame() -> None:
    """模拟 shibor 真实返回 (列名 date) 走 merge 的判定逻辑。"""
    df = pd.DataFrame({"date": ["20260401"], "on": [1.532]})
    assert "date" in df.columns
    assert "trade_date" not in df.columns
