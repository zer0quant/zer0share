import tushare as ts
import pandas as pd
import requests
import time
from loguru import logger

import zer0share.dateutil as dateutil

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
    STOCK_ST_COLS,
    STK_LIMIT_COLS,
    SUSPEND_D_COLS,
    SW_CLASSIFY_COLS,
    SW_DAILY_COLS,
    SW_MEMBER_COLS,
    TRADE_CAL_COLS,
)

__all__ = [
    "CI_MEMBER_COLS",
    "FUTURES_EXCHANGES",
    "FUT_INDEX_CODES",
    "FUT_BASIC_COLS",
    "FUT_DAILY_COLS",
    "FUT_HOLDING_COLS",
    "FUT_WSR_COLS",
    "FUT_SETTLE_COLS",
    "FUT_MAPPING_COLS",
    "FT_LIMIT_COLS",
    "FUT_WEEKLY_COLS",
    "FUT_MONTHLY_COLS",
    "FUT_INDEX_DAILY_COLS",
    "FUT_WEEKLY_DETAIL_COLS",
    "OPTIONS_EXCHANGES",
    "OPT_BASIC_COLS",
    "OPT_DAILY_COLS",
    "INDEX_DAILY_CODES",
    "TushareFetcher",
    "_select_columns_or_empty",
]

INDEX_DAILY_CODES = [
    "000001.SH",  # 上证指数
    "399001.SZ",  # 深证成指
    "000016.SH",  # 上证50
    "000300.SH",  # 沪深300
    "000905.SH",  # 中证500
    "000852.SH",  # 中证1000
    "000985.SH",  # 中证全指
    "399006.SZ",  # 创业板指
    "000688.SH",  # 科创50
    "399005.SZ",  # 中小板指
    "000922.SH",  # 中证红利
]

FUTURES_EXCHANGES = ["CZCE", "SHFE", "DCE", "CFFEX", "INE", "GFEX"]

FUT_INDEX_CODES = ["NHCI.NH", "NHAI.NH", "NHMI.NH"]

OPTIONS_EXCHANGES = ["SSE", "SZSE", "CFFEX", "DCE", "SHFE", "CZCE"]


class TushareFetcher:
    SW_VERSIONS = ("SW2014", "SW2021")

    def __init__(self, token: str, http_url: str = ""):
        self._pro = ts.pro_api(token)
        if http_url:
            self._pro._DataApi__http_url = http_url

    def fetch_basic(self) -> pd.DataFrame:
        logger.info("拉取 stock_basic")
        df = self._pro.stock_basic(
            exchange="",
            list_status="L,D,P,G",
            fields=",".join(BASIC_COLS)
        )
        return _select_columns_or_empty(df, BASIC_COLS)

    def fetch_daily_kline(self, trade_date: str) -> pd.DataFrame:
        logger.debug(f"拉取日线行情: {trade_date}")
        df = self._pro.daily(trade_date=trade_date, fields=",".join(DAILY_COLS))
        return _select_columns_or_empty(df, DAILY_COLS)

    def fetch_adj_factor(self, trade_date: str) -> pd.DataFrame:
        logger.debug(f"拉取复权因子: {trade_date}")
        df = self._pro.adj_factor(trade_date=trade_date, fields=",".join(ADJ_FACTOR_COLS))
        return _select_columns_or_empty(df, ADJ_FACTOR_COLS)

    def fetch_daily_basic(self, trade_date: str) -> pd.DataFrame:
        logger.debug(f"拉取每日指标: {trade_date}")
        df = self._pro.daily_basic(
            ts_code="",
            trade_date=trade_date,
            fields=",".join(DAILY_BASIC_COLS),
        )
        return _select_columns_or_empty(df, DAILY_BASIC_COLS)

    def fetch_stock_st(self, trade_date: str) -> pd.DataFrame:
        logger.debug(f"拉取ST股票列表: {trade_date}")
        df = self._pro.stock_st(trade_date=trade_date, fields=",".join(STOCK_ST_COLS))
        return _select_columns_or_empty(df, STOCK_ST_COLS)

    def fetch_suspend_d(self, trade_date: str) -> pd.DataFrame:
        logger.debug(f"拉取每日停复牌: {trade_date}")
        df = self._pro.suspend_d(
            trade_date=trade_date,
            suspend_type="S",
            fields=",".join(SUSPEND_D_COLS),
        )
        return _select_columns_or_empty(df, SUSPEND_D_COLS)

    def fetch_stk_limit(self, trade_date: str) -> pd.DataFrame:
        logger.debug(f"拉取每日涨跌停价格: {trade_date}")
        df = self._pro.stk_limit(trade_date=trade_date, fields=",".join(STK_LIMIT_COLS))
        return _select_columns_or_empty(df, STK_LIMIT_COLS)

    def fetch_index_weight(
        self, index_code: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        logger.debug(f"拉取指数成分: {index_code} {start_date}~{end_date}")
        df = self._pro.index_weight(
            index_code=index_code,
            start_date=start_date,
            end_date=end_date,
            fields=",".join(INDEX_WEIGHT_COLS),
        )
        return _select_columns_or_empty(df, INDEX_WEIGHT_COLS)

    def fetch_index_daily(
        self, ts_code: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        logger.debug(f"拉取指数日线: {ts_code} {start_date}~{end_date}")
        df = self._pro.index_daily(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            fields=",".join(INDEX_DAILY_COLS),
        )
        return _select_columns_or_empty(df, INDEX_DAILY_COLS)

    def fetch_idx_anns(self, ann_date: str) -> pd.DataFrame:
        logger.debug(f"拉取指数公告: {ann_date}")
        frames = []
        limit = 1000
        offset = 0
        while True:
            df = self._pro.idx_anns(
                ann_date=ann_date,
                fields=",".join(IDX_ANNS_COLS),
                limit=limit,
                offset=offset,
            )
            if df is None or df.empty:
                break
            frames.append(df)
            if len(df) < limit:
                break
            offset += limit
        combined = (
            pd.concat(frames, ignore_index=True)
            if frames
            else pd.DataFrame(columns=IDX_ANNS_COLS)
        )
        return _select_columns_or_empty(combined, IDX_ANNS_COLS)

    def fetch_etf_basic(
        self,
        ts_code: str | None = None,
        index_code: str | None = None,
        list_date: str | None = None,
        list_status: str | None = None,
        exchange: str | None = None,
        mgr: str | None = None,
    ) -> pd.DataFrame:
        logger.debug("拉取ETF基础信息")
        df = self._pro.etf_basic(
            ts_code=ts_code,
            index_code=index_code,
            list_date=list_date,
            list_status=list_status,
            exchange=exchange,
            mgr=mgr,
            fields=",".join(ETF_BASIC_COLS),
        )
        return _select_columns_or_empty(df, ETF_BASIC_COLS)

    def fetch_etf_index(
        self,
        ts_code: str | None = None,
        pub_date: str | None = None,
        base_date: str | None = None,
    ) -> pd.DataFrame:
        logger.debug("拉取ETF基准指数列表")
        df = self._pro.etf_index(
            ts_code=ts_code,
            pub_date=pub_date,
            base_date=base_date,
            fields=",".join(ETF_INDEX_COLS),
        )
        return _select_columns_or_empty(df, ETF_INDEX_COLS)

    def fetch_fund_daily(self, trade_date: str) -> pd.DataFrame:
        logger.debug(f"拉取ETF基金日线: {trade_date}")
        df = self._pro.fund_daily(trade_date=trade_date, fields=",".join(FUND_DAILY_COLS))
        return _select_columns_or_empty(df, FUND_DAILY_COLS)

    def fetch_fund_adj(self, trade_date: str) -> pd.DataFrame:
        logger.debug(f"拉取ETF基金复权因子: {trade_date}")
        df = self._pro.fund_adj(trade_date=trade_date, fields=",".join(FUND_ADJ_COLS))
        return _select_columns_or_empty(df, FUND_ADJ_COLS)

    def fetch_etf_share_size(self, trade_date: str) -> pd.DataFrame:
        logger.debug(f"拉取ETF份额规模: {trade_date}")
        frames = []
        for exchange in ("SSE", "SZSE"):
            df = self._pro.etf_share_size(
                trade_date=trade_date,
                exchange=exchange,
                fields=",".join(ETF_SHARE_SIZE_COLS),
            )
            if df is not None and not df.empty:
                frames.append(df)
        combined = (
            pd.concat(frames, ignore_index=True)
            if frames
            else pd.DataFrame(columns=ETF_SHARE_SIZE_COLS)
        )
        return _select_columns_or_empty(combined, ETF_SHARE_SIZE_COLS)

    def fetch_etf_sh_cons(self, trade_date: str) -> pd.DataFrame:
        logger.debug(f"拉取上交所ETF持仓组合: {trade_date}")
        frames = []
        limit = 3000
        offset = 0
        retry_delays = (0.2, 1.0)
        while True:
            stop_pagination = False
            for attempt, delay in enumerate((*retry_delays, None), start=1):
                try:
                    df = self._pro.etf_sh_cons(
                        trade_date=trade_date,
                        fields=",".join(ETF_SH_CONS_COLS),
                        limit=limit,
                        offset=offset,
                    )
                    break
                except Exception as exc:
                    if frames and "查询数据失败，请确认参数" in str(exc):
                        logger.warning(
                            f"etf_sh_cons {trade_date}: pagination stopped at offset={offset}: {exc}"
                        )
                        stop_pagination = True
                        break
                    if (
                        delay is not None
                        and isinstance(
                            exc,
                            (
                                requests.exceptions.ConnectionError,
                                requests.exceptions.Timeout,
                            ),
                        )
                    ):
                        logger.warning(
                            f"etf_sh_cons {trade_date}: page offset={offset} failed "
                            f"(attempt {attempt}), retry in {delay}s: {exc}"
                        )
                        time.sleep(delay)
                        continue
                    raise
            if stop_pagination:
                break
            if df is None or df.empty:
                break
            frames.append(df)
            if len(df) < limit:
                break
            offset += limit
        combined = (
            pd.concat(frames, ignore_index=True)
            if frames
            else pd.DataFrame(columns=ETF_SH_CONS_COLS)
        )
        return _select_columns_or_empty(combined, ETF_SH_CONS_COLS)

    def fetch_trade_cal(
        self,
        exchange: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        start_date = start_date if start_date is not None else "19900101"
        end_date = end_date if end_date is not None else dateutil.today()
        logger.info(f"拉取交易日历: {exchange} {start_date}~{end_date}")
        df = self._pro.trade_cal(
            exchange=exchange,
            start_date=start_date,
            end_date=end_date,
            fields=",".join(TRADE_CAL_COLS),
        )
        if df is None or df.empty:
            return pd.DataFrame(columns=TRADE_CAL_COLS)
        df["is_open"] = df["is_open"].astype(str).map({"1": True, "0": False}).astype(object)
        return df[TRADE_CAL_COLS]

    def fetch_fut_basic(self, exchange: str, fut_type: str = "1") -> pd.DataFrame:
        logger.debug(f"拉取期货合约: exchange={exchange}, fut_type={fut_type}")
        df = self._pro.fut_basic(
            exchange=exchange,
            fut_type=fut_type,
            fields=",".join(FUT_BASIC_COLS),
        )
        return _select_columns_or_empty(df, FUT_BASIC_COLS)

    def fetch_fut_daily(self, trade_date: str) -> pd.DataFrame:
        logger.debug(f"拉取期货日线: {trade_date}")
        df = self._pro.fut_daily(trade_date=trade_date, fields=",".join(FUT_DAILY_COLS))
        return _select_columns_or_empty(df, FUT_DAILY_COLS)

    def fetch_fut_holding(self, trade_date: str) -> pd.DataFrame:
        logger.debug(f"拉取期货持仓排名: {trade_date}")
        df = self._pro.fut_holding(trade_date=trade_date, fields=",".join(FUT_HOLDING_COLS))
        return _select_columns_or_empty(df, FUT_HOLDING_COLS)

    def fetch_fut_wsr(self, trade_date: str) -> pd.DataFrame:
        logger.debug(f"拉取期货仓单: {trade_date}")
        df = self._pro.fut_wsr(trade_date=trade_date, fields=",".join(FUT_WSR_COLS))
        return _select_columns_or_empty(df, FUT_WSR_COLS)

    def fetch_fut_settle(self, trade_date: str) -> pd.DataFrame:
        logger.debug(f"拉取期货结算参数: {trade_date}")
        df = self._pro.fut_settle(trade_date=trade_date, fields=",".join(FUT_SETTLE_COLS))
        return _select_columns_or_empty(df, FUT_SETTLE_COLS)

    def fetch_fut_mapping(self, trade_date: str) -> pd.DataFrame:
        logger.debug(f"拉取期货主力映射: {trade_date}")
        df = self._pro.fut_mapping(trade_date=trade_date, fields=",".join(FUT_MAPPING_COLS))
        return _select_columns_or_empty(df, FUT_MAPPING_COLS)

    def fetch_ft_limit(self, trade_date: str) -> pd.DataFrame:
        logger.debug(f"拉取期货涨跌停: {trade_date}")
        df = self._pro.ft_limit(trade_date=trade_date, fields=",".join(FT_LIMIT_COLS))
        return _select_columns_or_empty(df, FT_LIMIT_COLS)

    def fetch_fut_weekly(self, trade_date: str) -> pd.DataFrame:
        logger.debug(f"拉取期货周线: {trade_date}")
        df = self._pro.fut_weekly_monthly(
            trade_date=trade_date, freq="week", fields=",".join(FUT_WEEKLY_COLS),
        )
        return _select_columns_or_empty(df, FUT_WEEKLY_COLS)

    def fetch_fut_monthly(self, trade_date: str) -> pd.DataFrame:
        logger.debug(f"拉取期货月线: {trade_date}")
        df = self._pro.fut_weekly_monthly(
            trade_date=trade_date, freq="month", fields=",".join(FUT_MONTHLY_COLS),
        )
        return _select_columns_or_empty(df, FUT_MONTHLY_COLS)

    def fetch_fut_index_daily(self, trade_date: str) -> pd.DataFrame:
        logger.debug(f"拉取南华期货指数: {trade_date}")
        parts = []
        for code in FUT_INDEX_CODES:
            df = self._pro.fut_index_daily(
                ts_code=code, trade_date=trade_date, fields=",".join(FUT_INDEX_DAILY_COLS),
            )
            if df is not None and not df.empty:
                parts.append(df)
        combined = pd.concat(parts) if parts else pd.DataFrame(columns=FUT_INDEX_DAILY_COLS)
        return _select_columns_or_empty(combined, FUT_INDEX_DAILY_COLS)

    def fetch_fut_weekly_detail(self, week: str) -> pd.DataFrame:
        logger.debug(f"拉取期货品种周报: {week}")
        df = self._pro.fut_weekly_detail(
            week=week, fields=",".join(FUT_WEEKLY_DETAIL_COLS),
        )
        return _select_columns_or_empty(df, FUT_WEEKLY_DETAIL_COLS)

    def fetch_opt_basic(self, exchange: str) -> pd.DataFrame:
        logger.debug(f"拉取期权合约: exchange={exchange}")
        df = self._pro.opt_basic(exchange=exchange, fields=",".join(OPT_BASIC_COLS))
        return _select_columns_or_empty(df, OPT_BASIC_COLS)

    def fetch_opt_daily(self, trade_date: str) -> pd.DataFrame:
        logger.debug(f"拉取期权日线: {trade_date}")
        frames = []
        for exchange in OPTIONS_EXCHANGES:
            logger.debug(f"拉取期权日线: {trade_date} exchange={exchange}")
            df = self._pro.opt_daily(trade_date=trade_date, exchange=exchange, fields=",".join(OPT_DAILY_COLS))
            if df is not None and not df.empty:
                frames.append(df)
            time.sleep(0.2)
        if not frames:
            return pd.DataFrame(columns=OPT_DAILY_COLS)
        combined = pd.concat(frames, ignore_index=True)
        return _select_columns_or_empty(combined, OPT_DAILY_COLS)

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
        return _select_columns_or_empty(result, SW_MEMBER_COLS)

    def fetch_sw_daily(
        self, ts_code: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        logger.debug(f"拉取申万行业日线: {ts_code} {start_date}~{end_date}")
        df = self._pro.sw_daily(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            fields=",".join(SW_DAILY_COLS),
        )
        return _select_columns_or_empty(df, SW_DAILY_COLS)

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
        return _select_columns_or_empty(result, CI_MEMBER_COLS)


def _select_columns_or_empty(df: pd.DataFrame | None, columns: list[str]) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=columns)
    missing = [c for c in columns if c not in df.columns]
    if missing:
        df[missing] = pd.NA  # ponytail: Tushare may omit columns added after early dates (e.g. limit_status)
    return df[columns]
