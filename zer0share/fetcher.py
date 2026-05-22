import tushare as ts
import pandas as pd
import time
from datetime import date
from loguru import logger


BASIC_COLS = [
    "ts_code",
    "symbol",
    "name",
    "area",
    "industry",
    "fullname",
    "enname",
    "cnspell",
    "market",
    "exchange",
    "curr_type",
    "list_status",
    "list_date",
    "delist_date",
    "is_hs",
    "act_name",
    "act_ent_type",
]
DAILY_COLS = [
    "ts_code", "trade_date", "open", "high", "low",
    "close", "pre_close", "change", "pct_chg", "vol", "amount"
]
TRADE_CAL_COLS = ["exchange", "cal_date", "is_open", "pretrade_date"]
ADJ_FACTOR_COLS = ["ts_code", "trade_date", "adj_factor"]
DAILY_BASIC_COLS = [
    "ts_code",
    "trade_date",
    "close",
    "turnover_rate",
    "turnover_rate_f",
    "volume_ratio",
    "pe",
    "pe_ttm",
    "pb",
    "ps",
    "ps_ttm",
    "dv_ratio",
    "dv_ttm",
    "total_share",
    "float_share",
    "free_share",
    "total_mv",
    "circ_mv",
]
STOCK_ST_COLS = ["ts_code", "name", "trade_date", "type", "type_name"]
SUSPEND_D_COLS = ["ts_code", "trade_date", "suspend_timing", "suspend_type"]
STK_LIMIT_COLS = ["trade_date", "ts_code", "pre_close", "up_limit", "down_limit"]
INDEX_WEIGHT_COLS = ["index_code", "con_code", "trade_date", "weight"]
SW_CLASSIFY_COLS = [
    "index_code", "industry_name", "level", "parent_code",
    "industry_code", "is_pub", "src",
]
SW_MEMBER_COLS = [
    "l1_code", "l1_name", "l2_code", "l2_name",
    "l3_code", "l3_name", "ts_code", "name",
    "in_date", "out_date", "is_new",
]
CI_MEMBER_COLS = [
    "l1_code", "l1_name", "l2_code", "l2_name",
    "l3_code", "l3_name", "ts_code", "name",
    "in_date", "out_date", "is_new",
]


class TushareFetcher:
    def __init__(self, token: str):
        self._pro = ts.pro_api(token)

    def fetch_basic(self) -> pd.DataFrame:
        logger.info("拉取 stock_basic")
        df = self._pro.stock_basic(
            exchange="",
            list_status="L,D,P,G",
            fields=",".join(BASIC_COLS)
        )
        df["list_date"] = pd.to_datetime(
            df["list_date"], format="%Y%m%d", errors="coerce"
        ).dt.date
        df["delist_date"] = pd.to_datetime(
            df["delist_date"], format="%Y%m%d", errors="coerce"
        ).apply(lambda x: x.date() if not pd.isnull(x) else None)
        return df[BASIC_COLS]

    def fetch_daily_kline(self, trade_date: date) -> pd.DataFrame:
        date_str = trade_date.strftime("%Y%m%d")
        logger.debug(f"拉取日线行情: {date_str}")
        df = self._pro.daily(trade_date=date_str, fields=",".join(DAILY_COLS))
        if df is None or df.empty:
            return pd.DataFrame(columns=DAILY_COLS)
        df["trade_date"] = pd.to_datetime(
            df["trade_date"], format="%Y%m%d"
        ).dt.date
        return df[DAILY_COLS]

    def fetch_adj_factor(self, trade_date: date) -> pd.DataFrame:
        date_str = trade_date.strftime("%Y%m%d")
        logger.debug(f"拉取复权因子: {date_str}")
        df = self._pro.adj_factor(trade_date=date_str, fields=",".join(ADJ_FACTOR_COLS))
        if df is None or df.empty:
            return pd.DataFrame(columns=ADJ_FACTOR_COLS)
        df["trade_date"] = pd.to_datetime(
            df["trade_date"], format="%Y%m%d"
        ).dt.date
        return df[ADJ_FACTOR_COLS]

    def fetch_daily_basic(self, trade_date: date) -> pd.DataFrame:
        date_str = trade_date.strftime("%Y%m%d")
        logger.debug(f"拉取每日指标: {date_str}")
        df = self._pro.daily_basic(
            ts_code="",
            trade_date=date_str,
            fields=",".join(DAILY_BASIC_COLS),
        )
        return _format_trade_date(df, DAILY_BASIC_COLS)

    def fetch_stock_st(self, trade_date: date) -> pd.DataFrame:
        date_str = trade_date.strftime("%Y%m%d")
        logger.debug(f"拉取ST股票列表: {date_str}")
        df = self._pro.stock_st(trade_date=date_str, fields=",".join(STOCK_ST_COLS))
        return _format_trade_date(df, STOCK_ST_COLS)

    def fetch_suspend_d(self, trade_date: date) -> pd.DataFrame:
        date_str = trade_date.strftime("%Y%m%d")
        logger.debug(f"拉取每日停复牌: {date_str}")
        df = self._pro.suspend_d(
            trade_date=date_str,
            suspend_type="S",
            fields=",".join(SUSPEND_D_COLS),
        )
        return _format_trade_date(df, SUSPEND_D_COLS)

    def fetch_stk_limit(self, trade_date: date) -> pd.DataFrame:
        date_str = trade_date.strftime("%Y%m%d")
        logger.debug(f"拉取每日涨跌停价格: {date_str}")
        df = self._pro.stk_limit(trade_date=date_str, fields=",".join(STK_LIMIT_COLS))
        return _format_trade_date(df, STK_LIMIT_COLS)

    def fetch_index_weight(
        self, index_code: str, start_date: date, end_date: date
    ) -> pd.DataFrame:
        logger.debug(
            f"拉取指数成分: {index_code} "
            f"{start_date.strftime('%Y%m%d')}~{end_date.strftime('%Y%m%d')}"
        )
        df = self._pro.index_weight(
            index_code=index_code,
            start_date=start_date.strftime("%Y%m%d"),
            end_date=end_date.strftime("%Y%m%d"),
            fields=",".join(INDEX_WEIGHT_COLS),
        )
        return _format_trade_date(df, INDEX_WEIGHT_COLS)

    def fetch_trade_cal(
        self,
        exchange: str,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> pd.DataFrame:
        start = start_date or date(1990, 1, 1)
        end = end_date or date.today()
        logger.info(
            f"拉取交易日历: {exchange} "
            f"{start.strftime('%Y%m%d')}~{end.strftime('%Y%m%d')}"
        )
        df = self._pro.trade_cal(
            exchange=exchange,
            start_date=start.strftime("%Y%m%d"),
            end_date=end.strftime("%Y%m%d"),
            fields=",".join(TRADE_CAL_COLS),
        )
        if df is None or df.empty:
            return pd.DataFrame(columns=TRADE_CAL_COLS)
        df["cal_date"] = pd.to_datetime(
            df["cal_date"], format="%Y%m%d", errors="coerce"
        ).dt.date
        df["pretrade_date"] = pd.to_datetime(
            df["pretrade_date"], format="%Y%m%d", errors="coerce"
        ).apply(lambda x: x.date() if not pd.isnull(x) else None)
        df["is_open"] = df["is_open"].astype(str).map({"1": True, "0": False}).astype(object)
        return df[TRADE_CAL_COLS]

    SW_VERSIONS = ("SW2014", "SW2021")

    def fetch_sw_classify(self) -> pd.DataFrame:
        frames = []
        for src in self.SW_VERSIONS:
            logger.info(f"拉取申万行业分类: {src}")
            for level in ("L1", "L2", "L3"):
                df = self._pro.index_classify(level=level, src=src)
                if df is not None and not df.empty:
                    df["src"] = src
                    frames.append(df)
        if not frames:
            return pd.DataFrame(columns=SW_CLASSIFY_COLS)
        result = pd.concat(frames, ignore_index=True)
        return result[SW_CLASSIFY_COLS]

    def fetch_sw_member(self) -> pd.DataFrame:
        frames = []
        for src in self.SW_VERSIONS:
            l1_df = self._pro.index_classify(level="L1", src=src)
            if l1_df is None or l1_df.empty:
                continue
            l1_codes = l1_df["index_code"].tolist()
            logger.info(f"拉取申万行业成分({src}): {len(l1_codes)} 个一级行业")
            for l1_code in l1_codes:
                for is_new in ("Y", "N"):
                    df = self._pro.index_member_all(l1_code=l1_code, is_new=is_new)
                    time.sleep(0.2)
                    if df is not None and not df.empty:
                        frames.append(df)
        if not frames:
            return pd.DataFrame(columns=SW_MEMBER_COLS)
        result = (
            pd.concat(frames, ignore_index=True)
            .drop_duplicates(subset=["ts_code", "l3_code", "in_date"], keep="last")
            .reset_index(drop=True)
        )
        return _format_industry_dates(result, SW_MEMBER_COLS)

    def fetch_ci_member(self) -> pd.DataFrame:
        initial_df = self._pro.ci_index_member()
        if initial_df is None or initial_df.empty:
            return pd.DataFrame(columns=CI_MEMBER_COLS)
        l1_codes = initial_df["l1_code"].unique().tolist()
        logger.info(f"拉取中信行业成分: {len(l1_codes)} 个一级行业")
        frames = []
        for l1_code in l1_codes:
            for is_new in ("Y", "N"):
                df = self._pro.ci_index_member(l1_code=l1_code, is_new=is_new)
                time.sleep(0.2)
                if df is not None and not df.empty:
                    frames.append(df)
        if not frames:
            return pd.DataFrame(columns=CI_MEMBER_COLS)
        result = (
            pd.concat(frames, ignore_index=True)
            .drop_duplicates(subset=["ts_code", "l3_code", "in_date"], keep="last")
            .reset_index(drop=True)
        )
        return _format_industry_dates(result, CI_MEMBER_COLS)


def _format_trade_date(df: pd.DataFrame | None, columns: list[str]) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=columns)
    df["trade_date"] = pd.to_datetime(
        df["trade_date"], format="%Y%m%d", errors="coerce"
    ).dt.date
    return df[columns]


def _format_industry_dates(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=columns)
    for col in ("in_date", "out_date"):
        df[col] = pd.to_datetime(df[col], format="%Y%m%d", errors="coerce").apply(
            lambda x: x.date() if not pd.isna(x) and not pd.isnull(x) else None
        )
    return df[columns]
