import pandas as pd

from zer0share.catalog import (
    ADJ_FACTOR_SPEC,
    BASIC_SPEC,
    DAILY_BASIC_SPEC,
    DAILY_KLINE_SPEC,
    STOCK_ST_SPEC,
    STK_HOLDERTRADE_SPEC,
    STK_LIMIT_SPEC,
    SUSPEND_D_SPEC,
    BLOCK_TRADE_SPEC,
    SHARE_FLOAT_SPEC,
    MARGIN_SPEC,
)
from zer0share.query import QueryContext
from zer0share.query._helpers import format_date_columns
from zer0share.query.repository import (
    BaseParquetRepository,
    DailyPartitionRepository,
    SqlFilter,
    TableSpec,
    date_range_filters,
    eq_filter,
    in_filter,
)

UNIVERSE_COLS = ["trade_date", "universe", "ts_code"]


def _base_repo(ctx: QueryContext, spec) -> BaseParquetRepository:
    return BaseParquetRepository(ctx, spec)


def _daily_repo(ctx: QueryContext, spec) -> DailyPartitionRepository:
    return DailyPartitionRepository(ctx, spec)


def stock_basic(ctx: QueryContext, ts_code=None, name=None, market=None,
                list_status="L", exchange=None, is_hs=None, fields=None,
                limit: int | None = None, offset: int | None = None) -> pd.DataFrame:
    """Query stock basic info (name, market, list_status, etc.)."""
    repo = _base_repo(ctx, BASIC_SPEC)
    filters = []
    if ts_code is not None:
        filters.append(eq_filter("ts_code", ts_code, BASIC_SPEC.columns))
    if name is not None:
        filters.append(eq_filter("name", name, BASIC_SPEC.columns))
    if market is not None:
        filters.append(eq_filter("market", market, BASIC_SPEC.columns))
    if list_status is not None:
        filters.append(eq_filter("list_status", list_status, BASIC_SPEC.columns))
    if exchange is not None:
        filters.append(eq_filter("exchange", exchange, BASIC_SPEC.columns))
    if is_hs is not None:
        filters.append(eq_filter("is_hs", is_hs, BASIC_SPEC.columns))
    return repo.query(fields=fields, filters=filters, limit=limit, offset=offset)


def daily(ctx: QueryContext, ts_code=None, trade_date=None,
          start_date=None, end_date=None, fields=None,
          limit: int | None = None, offset: int | None = None) -> pd.DataFrame:
    """Query daily OHLCV bar data (open, high, low, close, vol, amount)."""
    return _daily_repo(ctx, DAILY_KLINE_SPEC).query(
        ts_code, trade_date, start_date, end_date, fields, limit=limit, offset=offset
    )


def adj_factor(ctx: QueryContext, ts_code=None, trade_date=None,
               start_date=None, end_date=None, fields=None,
               limit: int | None = None, offset: int | None = None) -> pd.DataFrame:
    """Query back-adjustment factors for split/dividend-adjusted price calculation."""
    return _daily_repo(ctx, ADJ_FACTOR_SPEC).query(
        ts_code, trade_date, start_date, end_date, fields, limit=limit, offset=offset
    )


def daily_basic(ctx: QueryContext, ts_code=None, trade_date=None,
                start_date=None, end_date=None, fields=None,
                limit: int | None = None, offset: int | None = None) -> pd.DataFrame:
    """Query daily fundamental indicators (PE, PB, market cap, turnover rate, etc.)."""
    return _daily_repo(ctx, DAILY_BASIC_SPEC).query(
        ts_code, trade_date, start_date, end_date, fields, limit=limit, offset=offset
    )


def stock_st(ctx: QueryContext, ts_code=None, trade_date=None,
             start_date=None, end_date=None, fields=None,
             limit: int | None = None, offset: int | None = None) -> pd.DataFrame:
    """Query ST/ST*/delisting-risk flag history per stock per trading day."""
    return _daily_repo(ctx, STOCK_ST_SPEC).query(
        ts_code, trade_date, start_date, end_date, fields, limit=limit, offset=offset
    )


def suspend_d(ctx: QueryContext, ts_code=None, trade_date=None,
              start_date=None, end_date=None, fields=None,
              limit: int | None = None, offset: int | None = None) -> pd.DataFrame:
    """Query daily suspension records (stock halted from trading)."""
    return _daily_repo(ctx, SUSPEND_D_SPEC).query(
        ts_code, trade_date, start_date, end_date, fields, limit=limit, offset=offset
    )


def stk_holdertrade(ctx: QueryContext, ts_code=None, ann_date=None,
                    start_date=None, end_date=None, holder_type=None, trade_type=None,
                    fields=None, limit: int | None = None, offset: int | None = None) -> pd.DataFrame:
    """Query shareholder increase/decrease announcements (holder_type C/P/G, trade_type IN/DE)."""
    filters: list[SqlFilter] = []
    if holder_type is not None:
        filters.append(eq_filter("holder_type", holder_type, STK_HOLDERTRADE_SPEC.columns))
    if trade_type is not None:
        filters.append(eq_filter("in_de", trade_type, STK_HOLDERTRADE_SPEC.columns))
    return _daily_repo(ctx, STK_HOLDERTRADE_SPEC).query(
        ts_code, ann_date, start_date, end_date, fields,
        filters=filters, limit=limit, offset=offset,
    )


def stk_limit(ctx: QueryContext, ts_code=None, trade_date=None,
              start_date=None, end_date=None, fields=None,
              limit: int | None = None, offset: int | None = None) -> pd.DataFrame:
    """Query daily price limits (up_limit, down_limit, pre_close) per stock."""
    return _daily_repo(ctx, STK_LIMIT_SPEC).query(
        ts_code, trade_date, start_date, end_date, fields, limit=limit, offset=offset
    )


def block_trade(ctx: QueryContext, ts_code=None, trade_date=None,
                start_date=None, end_date=None, fields=None,
                limit: int | None = None, offset: int | None = None) -> pd.DataFrame:
    """Query block trades (大宗交易) — price/vol/amount off-exchange per stock per day."""
    return _daily_repo(ctx, BLOCK_TRADE_SPEC).query(
        ts_code, trade_date, start_date, end_date, fields, limit=limit, offset=offset
    )


def share_float(ctx: QueryContext, ts_code=None, float_date=None,
                start_date=None, end_date=None, fields=None,
                limit: int | None = None, offset: int | None = None) -> pd.DataFrame:
    """Query restricted-share unlocks (限售股解禁).

    分区键是解禁日 float_date 而非交易日, 因此 trade_date 参数在此不可用;
    DailyPartitionRepository 按 spec.date_column 定位分区。
    """
    return _daily_repo(ctx, SHARE_FLOAT_SPEC).query(
        ts_code, float_date, start_date, end_date, fields, limit=limit, offset=offset
    )


def margin(ctx: QueryContext, exchange_id=None, trade_date=None,
           start_date=None, end_date=None, fields=None,
           limit: int | None = None, offset: int | None = None) -> pd.DataFrame:
    """Query margin trading (融资融券) aggregate balances per exchange per day."""
    repo = _daily_repo(ctx, MARGIN_SPEC)
    filters: list[SqlFilter] = []
    if exchange_id is not None:
        filters.append(eq_filter("exchange_id", exchange_id, MARGIN_SPEC.columns))
    filters.extend(
        date_range_filters("trade_date", trade_date, start_date, end_date, MARGIN_SPEC.columns)
    )
    return repo.query(fields=fields, filters=filters, limit=limit, offset=offset)


def universe(ctx: QueryContext, universe=None, ts_code=None, trade_date=None,
             start_date=None, end_date=None, fields=None,
             limit: int | None = None, offset: int | None = None) -> pd.DataFrame:
    """Query universe membership snapshots (which stocks belong to which universe on each date)."""
    repo = _base_repo(
        ctx,
        TableSpec(
            name="universe",
            path_parts=("stock", "universe"),
            columns=UNIVERSE_COLS,
            parquet_pattern="name=*/date=*/data.parquet",
            sync_table="build-universe",
            order_by="universe, ts_code, trade_date",
            hive_partitioning=True,
            union_by_name=True,
        ),
    )
    filters: list[SqlFilter] = []
    if universe is not None:
        filters.append(eq_filter("universe", universe, UNIVERSE_COLS))
    if ts_code is not None:
        filters.append(in_filter("ts_code", ts_code, UNIVERSE_COLS))
    filters.extend(date_range_filters("trade_date", trade_date, start_date, end_date, UNIVERSE_COLS))
    df = repo.query(fields=fields, filters=filters, limit=limit, offset=offset)
    return format_date_columns(df, ["trade_date"])


def pro_bar(ctx: QueryContext, ts_code: str, start_date=None, end_date=None,
            asset: str = "E", adj=None, freq: str = "D", trade_date=None,
            ma=None, limit: int | None = None, offset: int | None = None) -> pd.DataFrame:
    """Query adjusted OHLCV bars; adj='qfq' for forward-adjusted, 'hfq' for back-adjusted."""
    if asset != "E":
        raise NotImplementedError("local pro_bar currently only supports asset='E'")
    if freq != "D":
        raise NotImplementedError("local pro_bar currently only supports freq='D'")
    if ma:
        raise NotImplementedError("local pro_bar does not support ma yet")
    if adj not in (None, "qfq", "hfq"):
        raise ValueError("adj must be one of None, 'qfq', or 'hfq'")

    daily_df = daily(ctx, ts_code=ts_code, trade_date=trade_date,
                     start_date=start_date, end_date=end_date)
    if adj is None or daily_df.empty:
        result = daily_df
    else:
        factors = adj_factor(ctx, ts_code=ts_code, trade_date=trade_date,
                             start_date=start_date, end_date=end_date)
        if factors.empty:
            result = daily_df.iloc[0:0].copy()
        else:
            result = daily_df.merge(
                factors[["ts_code", "trade_date", "adj_factor"]],
                on=["ts_code", "trade_date"], how="left",
            ).sort_values(["ts_code", "trade_date"])
            result["adj_factor"] = result.groupby("ts_code")["adj_factor"].bfill()
            result = result.dropna(subset=["adj_factor"])
            if result.empty:
                result = daily_df.iloc[0:0].copy()
            else:
                price_columns = ["open", "high", "low", "close", "pre_close"]
                if adj == "qfq":
                    base_factor = result.groupby("ts_code")["adj_factor"].transform("last")
                    multiplier = result["adj_factor"] / base_factor
                else:
                    multiplier = result["adj_factor"]
                for col in price_columns:
                    result[col] = (result[col] * multiplier).round(2)
                result["change"] = (result["close"] - result["pre_close"]).round(2)
                result["pct_chg"] = (result["change"] / result["pre_close"] * 100).round(2)
                result = result.drop(columns=["adj_factor"])

    if offset is not None:
        result = result.iloc[offset:]
    if limit is not None:
        result = result.iloc[:limit]
    return result
