"""
交易日历工具模块。

基于 Fetcher 的交易日历数据，提供日期推导、交易日判断等功能。
不负责数据获取（由 fetcher.py 负责），只做业务层面的日期计算。
"""

from datetime import date, timedelta

import pandas as pd


def last_trade_date_of_prev_month(
    fetcher, ref_date: date, exchange: str = "SSE"
) -> date:
    """给定任意日期，返回上个月最后一个交易日。

    典型用途：一些 Tushare 接口（如 index_weight）只支持月度数据，
    需要传入月末最后一个交易日作为查询参数。

    Args:
        fetcher: TushareFetcher 实例。
        ref_date: 参考日期。
        exchange: 交易所，默认 SSE。

    Returns:
        date: 上个月最后一个交易日。

    Example:
        >>> ref = date(2026, 3, 25)
        >>> last_trade_date_of_prev_month(fetcher, ref)
        date(2026, 2, 27)
    """
    first_of_month = date(ref_date.year, ref_date.month, 1)
    last_of_prev = first_of_month - timedelta(days=1)

    # 拉取上月末附近交易日历（多拉几天兜底）
    cal = fetcher.fetch_trade_cal(
        exchange,
        last_of_prev - timedelta(days=10),
        last_of_prev,
    )

    open_days = cal[cal["is_open"] == True]
    if not open_days.empty:
        return open_days["cal_date"].max()

    return last_of_prev


def last_trade_date_of_month(
    fetcher, ref_date: date, exchange: str = "SSE"
) -> date:
    """给定任意日期，返回当月最后一个交易日。

    Args:
        fetcher: TushareFetcher 实例。
        ref_date: 参考日期。
        exchange: 交易所，默认 SSE。

    Returns:
        date: 当月最后一个交易日。
    """
    # 下个月第一天
    if ref_date.month == 12:
        first_of_next = date(ref_date.year + 1, 1, 1)
    else:
        first_of_next = date(ref_date.year, ref_date.month + 1, 1)
    last_of_month = first_of_next - timedelta(days=1)

    cal = fetcher.fetch_trade_cal(
        exchange,
        ref_date,
        last_of_month,
    )

    open_days = cal[cal["is_open"] == True]
    if not open_days.empty:
        return open_days["cal_date"].max()

    return last_of_month


def is_trade_date(
    fetcher, d: date, exchange: str = "SSE"
) -> bool:
    """判断给定日期是否为交易日。

    Args:
        fetcher: TushareFetcher 实例。
        d: 待判断的日期。
        exchange: 交易所，默认 SSE。

    Returns:
        bool: 是否为交易日。
    """
    cal = fetcher.fetch_trade_cal(exchange, d, d)
    if cal.empty:
        return False
    return bool(cal.iloc[0]["is_open"])


def prev_trade_date(
    fetcher, ref_date: date, exchange: str = "SSE"
) -> date:
    """返回 ref_date 之前最近的交易日（不含 ref_date 本身）。

    直接使用交易日历自带的 pretrade_date 列，无需自行推导。

    Args:
        fetcher: TushareFetcher 实例。
        ref_date: 参考日期。
        exchange: 交易所，默认 SSE。

    Returns:
        date: 前一个交易日。

    Raises:
        ValueError: ref_date 不在交易日历中。
    """
    cal = fetcher.fetch_trade_cal(exchange, ref_date, ref_date)
    if cal.empty:
        raise ValueError(f"交易日历中未找到 {ref_date} ({exchange})")
    result = cal.iloc[0]["pretrade_date"]
    if result is None or pd.isna(result):
        raise ValueError(f"{ref_date} ({exchange}) 之前未找到交易日")
    return result
