"""
交易日历工具模块。

基于 Fetcher 的交易日历数据，提供日期推导、交易日判断等功能。
不负责数据获取（由 fetcher.py 负责），只做业务层面的日期计算。

Usage::

    from zer0share.trade_calendar import TradeCalendar

    cal = TradeCalendar(fetcher)                # 默认 SSE
    d = cal.last_trade_date_of_prev_month(date(2026, 3, 25))
    d = cal.is_trade_date(date(2026, 3, 8))
"""

from datetime import date, timedelta

import pandas as pd


class TradeCalendar:
    """交易日历工具。

    持有 TushareFetcher 实例，提供交易日推导方法。构造时指定默认交易所，
    单次调用可覆盖。

    Usage::

        cal = TradeCalendar(fetcher)
        d = cal.last_trade_date_of_prev_month(date(2026, 3, 25))
        d = cal.prev_trade_date(date(2026, 3, 10), exchange="SZSE")
    """

    def __init__(self, fetcher, exchange: str = "SSE"):
        """初始化。

        Args:
            fetcher: TushareFetcher 实例。
            exchange: 默认交易所，SSE（上交所）或 SZSE（深交所）。
        """
        self._fetcher = fetcher
        self._exchange = exchange

    # ------------------------------------------------------------------
    # 上月 / 当月最后交易日
    # ------------------------------------------------------------------

    def last_trade_date_of_prev_month(
        self, ref_date: date, exchange: str | None = None
    ) -> date:
        """给定任意日期，返回上个月最后一个交易日。

        典型用途：一些 Tushare 接口（如 index_weight）只支持月度数据，
        需要传入月末最后一个交易日作为查询参数。

        Args:
            ref_date: 参考日期。
            exchange: 交易所，不传则使用构造时的默认值。

        Returns:
            date: 上个月最后一个交易日。
        """
        ex = exchange or self._exchange
        first_of_month = date(ref_date.year, ref_date.month, 1)
        last_of_prev = first_of_month - timedelta(days=1)

        cal = self._fetcher.fetch_trade_cal(
            ex,
            last_of_prev - timedelta(days=10),
            last_of_prev,
        )

        open_days = cal[cal["is_open"] == True]
        if not open_days.empty:
            return open_days["cal_date"].max()

        return last_of_prev

    def last_trade_date_of_month(
        self, ref_date: date, exchange: str | None = None
    ) -> date:
        """给定任意日期，返回当月最后一个交易日。

        Args:
            ref_date: 参考日期。
            exchange: 交易所，不传则使用构造时的默认值。

        Returns:
            date: 当月最后一个交易日。
        """
        ex = exchange or self._exchange
        if ref_date.month == 12:
            first_of_next = date(ref_date.year + 1, 1, 1)
        else:
            first_of_next = date(ref_date.year, ref_date.month + 1, 1)
        last_of_month = first_of_next - timedelta(days=1)

        cal = self._fetcher.fetch_trade_cal(ex, ref_date, last_of_month)

        open_days = cal[cal["is_open"] == True]
        if not open_days.empty:
            return open_days["cal_date"].max()

        return last_of_month

    # ------------------------------------------------------------------
    # 交易日判断
    # ------------------------------------------------------------------

    def is_trade_date(
        self, d: date, exchange: str | None = None
    ) -> bool:
        """判断给定日期是否为交易日。

        Args:
            d: 待判断的日期。
            exchange: 交易所，不传则使用构造时的默认值。

        Returns:
            bool: 是否为交易日。
        """
        ex = exchange or self._exchange
        cal = self._fetcher.fetch_trade_cal(ex, d, d)
        if cal.empty:
            return False
        return bool(cal.iloc[0]["is_open"])

    # ------------------------------------------------------------------
    # 前后交易日
    # ------------------------------------------------------------------

    def prev_trade_date(
        self, ref_date: date, exchange: str | None = None
    ) -> date:
        """返回 ref_date 之前最近的交易日（不含 ref_date 本身）。

        直接使用交易日历自带的 pretrade_date 列，一次 API 调用。

        Args:
            ref_date: 参考日期。
            exchange: 交易所，不传则使用构造时的默认值。

        Returns:
            date: 前一个交易日。

        Raises:
            ValueError: ref_date 不在交易日历中或之前无交易日。
        """
        ex = exchange or self._exchange
        cal = self._fetcher.fetch_trade_cal(ex, ref_date, ref_date)
        if cal.empty:
            raise ValueError(f"交易日历中未找到 {ref_date} ({ex})")
        result = cal.iloc[0]["pretrade_date"]
        if result is None or pd.isna(result):
            raise ValueError(f"{ref_date} ({ex}) 之前未找到交易日")
        return result
