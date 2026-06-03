import pandas as pd
import pytest
from datetime import date
from unittest.mock import Mock

from zer0share.trade_calendar import (
    is_trade_date,
    last_trade_date_of_month,
    last_trade_date_of_prev_month,
    prev_trade_date,
)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _trade_cal_df(dates_and_open: list[tuple[str, bool]],
                  pretrade: dict[str, str] | None = None) -> pd.DataFrame:
    """用 (date_str, is_open) 列表构建交易日历 DataFrame，模拟 fetcher 返回值。

    Args:
        dates_and_open: (日期字符串, 是否交易日) 列表。
        pretrade: cal_date → pretrade_date 的映射，不指定则填 None。
    """
    if pretrade is None:
        pretrade = {}
    rows = []
    for d, is_open in dates_and_open:
        pt = date.fromisoformat(pretrade[d]) if d in pretrade else None
        rows.append({
            "exchange": "SSE",
            "cal_date": date.fromisoformat(d),
            "is_open": is_open,
            "pretrade_date": pt,
        })
    return pd.DataFrame(rows)


COLUMNS = ["exchange", "cal_date", "is_open", "pretrade_date"]

def _fetcher(df: pd.DataFrame) -> Mock:
    """创建一个 mock fetcher，其 fetch_trade_cal 按日期范围过滤返回数据。

    模拟真实 fetcher 的行为：只返回 start_date～end_date 范围内的数据。
    """
    # 确保空 DataFrame 也有正确的列名
    if df.empty and df.columns.empty:
        df = pd.DataFrame(columns=COLUMNS)

    def fetch_trade_cal(exchange, start_date, end_date):
        if df.empty:
            return pd.DataFrame(columns=COLUMNS)
        mask = (df["cal_date"] >= start_date) & (df["cal_date"] <= end_date)
        return df[mask].reset_index(drop=True)

    f = Mock()
    f.fetch_trade_cal = Mock(side_effect=fetch_trade_cal)
    return f


# 一段真实的交易日历片段（2026 年 2 月–3 月）
FEB_MAR_2026 = [
    ("2026-02-23", True),
    ("2026-02-24", True),
    ("2026-02-25", True),
    ("2026-02-26", True),
    ("2026-02-27", True),   # 2 月最后一个交易日
    ("2026-02-28", False),  # 周六
    ("2026-03-01", False),  # 周日
    ("2026-03-02", True),
    ("2026-03-03", True),
    ("2026-03-04", True),
    ("2026-03-05", True),
    ("2026-03-06", True),
    ("2026-03-07", False),  # 周六
    ("2026-03-08", False),  # 周日
    ("2026-03-09", True),
    ("2026-03-10", True),
]


# =============================================================================
# is_trade_date
# =============================================================================

def test_is_trade_date_true():
    f = _fetcher(_trade_cal_df([("2026-03-09", True)]))
    assert is_trade_date(f, date(2026, 3, 9)) is True


def test_is_trade_date_false_weekend():
    f = _fetcher(_trade_cal_df([("2026-03-08", False)]))
    assert is_trade_date(f, date(2026, 3, 8)) is False


def test_is_trade_date_empty_calendar():
    f = _fetcher(pd.DataFrame(columns=["exchange", "cal_date", "is_open", "pretrade_date"]))
    assert is_trade_date(f, date(2026, 3, 8)) is False


# =============================================================================
# last_trade_date_of_prev_month
# =============================================================================

def test_last_trade_date_of_prev_month_normal():
    """ref_date = 3 月 10 日 → 上月最后交易日 = 2026-02-27"""
    f = _fetcher(_trade_cal_df(FEB_MAR_2026))
    result = last_trade_date_of_prev_month(f, date(2026, 3, 10))
    assert result == date(2026, 2, 27)


def test_last_trade_date_of_prev_month_january():
    """1 月的上个月是去年 12 月"""
    dec_jan = [
        ("2025-12-29", True),
        ("2025-12-30", True),
        ("2025-12-31", True),   # 12 月最后交易日
        ("2025-12-27", False),  # 周六
        ("2026-01-01", False),
        ("2026-01-02", True),
    ]
    f = _fetcher(_trade_cal_df(dec_jan))
    result = last_trade_date_of_prev_month(f, date(2026, 1, 15))
    assert result == date(2025, 12, 31)


def test_last_trade_date_of_prev_month_ref_is_first_day():
    """ref_date 是当月 1 日 → 上月最后交易日"""
    f = _fetcher(_trade_cal_df(FEB_MAR_2026))
    result = last_trade_date_of_prev_month(f, date(2026, 3, 1))
    assert result == date(2026, 2, 27)


# =============================================================================
# last_trade_date_of_month
# =============================================================================

def test_last_trade_date_of_month_normal():
    f = _fetcher(_trade_cal_df(FEB_MAR_2026))
    result = last_trade_date_of_month(f, date(2026, 2, 15))
    assert result == date(2026, 2, 27)


def test_last_trade_date_of_month_december():
    """12 月的最后交易日"""
    dec_data = [
        ("2026-12-29", True),
        ("2026-12-30", True),
        ("2026-12-31", True),
    ]
    f = _fetcher(_trade_cal_df(dec_data))
    result = last_trade_date_of_month(f, date(2026, 12, 20))
    assert result == date(2026, 12, 31)


# =============================================================================
# prev_trade_date
# =============================================================================

def test_prev_trade_date_normal():
    """3 月 10 日（周二）的前一个交易日是 3 月 9 日（周一）"""
    f = _fetcher(_trade_cal_df(
        [("2026-03-10", True)],
        pretrade={"2026-03-10": "2026-03-09"},
    ))
    result = prev_trade_date(f, date(2026, 3, 10))
    assert result == date(2026, 3, 9)


def test_prev_trade_date_after_weekend():
    """3 月 2 日（周一）的前一个交易日是 2 月 27 日（周五）"""
    f = _fetcher(_trade_cal_df(
        [("2026-03-02", True)],
        pretrade={"2026-03-02": "2026-02-27"},
    ))
    result = prev_trade_date(f, date(2026, 3, 2))
    assert result == date(2026, 2, 27)


def test_prev_trade_date_no_trade_date_before():
    """ref_date 之前没有交易日（无 pretrade_date）时应抛 ValueError"""
    f = _fetcher(_trade_cal_df([("2026-03-02", True)]))
    with pytest.raises(ValueError, match="未找到交易日"):
        prev_trade_date(f, date(2026, 3, 2))


def test_prev_trade_date_raises_when_no_calendar():
    """ref_date 不在交易日历中时应抛 ValueError"""
    f = _fetcher(_trade_cal_df([]))
    with pytest.raises(ValueError, match="交易日历中未找到"):
        prev_trade_date(f, date(2026, 3, 10))
