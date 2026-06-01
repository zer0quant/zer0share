from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd

from zer0share.config import load_config
from zer0share.fetcher import (
    ADJ_FACTOR_COLS,
    BASIC_COLS,
    CI_MEMBER_COLS,
    DAILY_BASIC_COLS,
    DAILY_COLS,
    INDEX_DAILY_COLS,
    INDEX_WEIGHT_COLS,
    STOCK_ST_COLS,
    STK_LIMIT_COLS,
    SUSPEND_D_COLS,
    SW_CLASSIFY_COLS,
    SW_MEMBER_COLS,
    TRADE_CAL_COLS,
    FUT_BASIC_COLS,
    FUT_DAILY_COLS,
    FUT_HOLDING_COLS,
    FUT_WSR_COLS,
    FUT_SETTLE_COLS,
    FUT_MAPPING_COLS,
)


UNIVERSE_COLS = ["trade_date", "universe", "ts_code"]


class LocalPro:
    def __init__(self, data_dir: str | Path):
        self._data_dir = Path(data_dir)

    def stock_basic(
        self,
        ts_code: str | None = None,
        name: str | None = None,
        market: str | None = None,
        list_status: str | None = "L",
        exchange: str | None = None,
        is_hs: str | None = None,
        fields: str | list[str] | None = None,
    ) -> pd.DataFrame:
        path = self._data_dir / "basic" / "data.parquet"
        if not path.exists():
            raise FileNotFoundError("basic data not found; run `python main.py sync --table basic` first")

        columns = _parse_fields(fields, BASIC_COLS)
        where = []
        params = []
        if ts_code is not None:
            where.append("ts_code = ?")
            params.append(ts_code)
        if name is not None:
            where.append("name = ?")
            params.append(name)
        if market is not None:
            where.append("market = ?")
            params.append(market)
        if list_status is not None:
            where.append("list_status = ?")
            params.append(list_status)
        if exchange is not None:
            where.append("exchange = ?")
            params.append(exchange)
        if is_hs is not None:
            where.append("is_hs = ?")
            params.append(is_hs)

        sql = f"SELECT {', '.join(columns)} FROM read_parquet(?)"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY ts_code"

        df = duckdb.connect().execute(sql, [str(path), *params]).fetchdf()
        return _format_date_columns(df, ["list_date", "delist_date"])

    def trade_cal(
        self,
        exchange: str = "SSE",
        start_date: str | None = None,
        end_date: str | None = None,
        is_open: str | int | bool | None = None,
        fields: str | list[str] | None = None,
    ) -> pd.DataFrame:
        trade_cal_dir = self._data_dir / "trade_cal"
        if not trade_cal_dir.exists():
            raise FileNotFoundError(
                "trade_cal data not found; run `python main.py sync --table trade_cal` first"
            )

        columns = _parse_fields(fields, TRADE_CAL_COLS)
        where = ["exchange = ?"]
        params = [exchange]
        parsed_start = _parse_date(start_date) if start_date is not None else None
        parsed_end = _parse_date(end_date) if end_date is not None else None
        if parsed_start is not None and parsed_end is not None and parsed_end < parsed_start:
            raise ValueError("end_date must be on or after start_date")
        if parsed_start is not None:
            where.append("cal_date >= ?")
            params.append(parsed_start)
        if parsed_end is not None:
            where.append("cal_date <= ?")
            params.append(parsed_end)
        if is_open is not None:
            where.append("is_open = ?")
            params.append(_parse_is_open(is_open))

        pattern = trade_cal_dir / "exchange=*" / "data.parquet"
        sql = (
            f"SELECT {', '.join(columns)} FROM read_parquet(?, hive_partitioning=true) "
            f"WHERE {' AND '.join(where)} ORDER BY exchange, cal_date"
        )
        df = duckdb.connect().execute(sql, [str(pattern), *params]).fetchdf()
        return _format_date_columns(df, ["cal_date", "pretrade_date"])

    def daily(
        self,
        ts_code: str | None = None,
        trade_date: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        fields: str | list[str] | None = None,
    ) -> pd.DataFrame:
        return self._query_daily_partitioned(
            table_name="daily_kline",
            sync_table="daily_kline",
            columns=DAILY_COLS,
            ts_code=ts_code,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
            fields=fields,
        )

    def adj_factor(
        self,
        ts_code: str | None = None,
        trade_date: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        fields: str | list[str] | None = None,
    ) -> pd.DataFrame:
        return self._query_daily_partitioned(
            table_name="adj_factor",
            sync_table="adj_factor",
            columns=ADJ_FACTOR_COLS,
            ts_code=ts_code,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
            fields=fields,
        )

    def daily_basic(
        self,
        ts_code: str | None = None,
        trade_date: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        fields: str | list[str] | None = None,
    ) -> pd.DataFrame:
        return self._query_daily_partitioned(
            table_name="daily_basic",
            sync_table="daily_basic",
            columns=DAILY_BASIC_COLS,
            ts_code=ts_code,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
            fields=fields,
        )

    def stock_st(
        self,
        ts_code: str | None = None,
        trade_date: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        fields: str | list[str] | None = None,
    ) -> pd.DataFrame:
        return self._query_daily_partitioned(
            table_name="stock_st",
            sync_table="stock_st",
            columns=STOCK_ST_COLS,
            ts_code=ts_code,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
            fields=fields,
        )

    def suspend_d(
        self,
        ts_code: str | None = None,
        trade_date: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        fields: str | list[str] | None = None,
    ) -> pd.DataFrame:
        return self._query_daily_partitioned(
            table_name="suspend_d",
            sync_table="suspend_d",
            columns=SUSPEND_D_COLS,
            ts_code=ts_code,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
            fields=fields,
        )

    def stk_limit(
        self,
        ts_code: str | None = None,
        trade_date: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        fields: str | list[str] | None = None,
    ) -> pd.DataFrame:
        return self._query_daily_partitioned(
            table_name="stk_limit",
            sync_table="stk_limit",
            columns=STK_LIMIT_COLS,
            ts_code=ts_code,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
            fields=fields,
        )

    def index_daily(
        self,
        ts_code: str | None = None,
        trade_date: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        fields: str | list[str] | None = None,
    ) -> pd.DataFrame:
        return self._query_daily_partitioned(
            table_name="index_daily",
            sync_table="index_daily",
            columns=INDEX_DAILY_COLS,
            ts_code=ts_code,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
            fields=fields,
        )

    def index_weight(
        self,
        index_code: str | None = None,
        trade_date: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        fields: str | list[str] | None = None,
    ) -> pd.DataFrame:
        if trade_date is not None and (start_date is not None or end_date is not None):
            raise ValueError("trade_date cannot be combined with start_date or end_date")
        parsed_start = _parse_date(start_date) if start_date is not None else None
        parsed_end = _parse_date(end_date) if end_date is not None else None
        if parsed_start is not None and parsed_end is not None and parsed_end < parsed_start:
            raise ValueError("end_date must be on or after start_date")

        table_dir = self._data_dir / "index_weight"
        if not table_dir.exists():
            raise FileNotFoundError(
                "index_weight data not found; run `python main.py sync --table index_weight` first"
            )

        selected = _parse_fields(fields, INDEX_WEIGHT_COLS)
        where = []
        params = []
        if index_code is not None:
            where.append("index_code = ?")
            params.append(index_code)
        if trade_date is not None:
            where.append("trade_date = ?")
            params.append(_parse_date(trade_date))
        if parsed_start is not None:
            where.append("trade_date >= ?")
            params.append(parsed_start)
        if parsed_end is not None:
            where.append("trade_date <= ?")
            params.append(parsed_end)

        pattern = table_dir / "index_code=*" / "date=*" / "data.parquet"
        sql = (
            f"SELECT {', '.join(selected)} "
            "FROM read_parquet(?, hive_partitioning=true, union_by_name=true)"
        )
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY index_code, con_code, trade_date"

        df = duckdb.connect().execute(sql, [str(pattern), *params]).fetchdf()
        return _format_date_columns(df, ["trade_date"])

    def universe(
        self,
        universe: str | None = None,
        ts_code: str | None = None,
        trade_date: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        fields: str | list[str] | None = None,
    ) -> pd.DataFrame:
        if trade_date is not None and (start_date is not None or end_date is not None):
            raise ValueError("trade_date cannot be combined with start_date or end_date")
        parsed_start = _parse_date(start_date) if start_date is not None else None
        parsed_end = _parse_date(end_date) if end_date is not None else None
        if parsed_start is not None and parsed_end is not None and parsed_end < parsed_start:
            raise ValueError("end_date must be on or after start_date")

        table_dir = self._data_dir / "universe"
        if not table_dir.exists():
            raise FileNotFoundError(
                "universe data not found; run `python main.py build-universe` first"
            )

        selected = _parse_fields(fields, UNIVERSE_COLS)
        where = []
        params = []
        if universe is not None:
            where.append("universe = ?")
            params.append(universe)
        if ts_code is not None:
            codes = [code.strip() for code in ts_code.split(",") if code.strip()]
            placeholders = ", ".join("?" for _ in codes)
            where.append(f"ts_code IN ({placeholders})")
            params.extend(codes)
        if trade_date is not None:
            where.append("trade_date = ?")
            params.append(_parse_date(trade_date))
        if parsed_start is not None:
            where.append("trade_date >= ?")
            params.append(parsed_start)
        if parsed_end is not None:
            where.append("trade_date <= ?")
            params.append(parsed_end)

        pattern = table_dir / "name=*" / "date=*" / "data.parquet"
        sql = (
            f"SELECT {', '.join(selected)} "
            "FROM read_parquet(?, hive_partitioning=true, union_by_name=true)"
        )
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY universe, ts_code, trade_date"

        df = duckdb.connect().execute(sql, [str(pattern), *params]).fetchdf()
        return _format_date_columns(df, ["trade_date"])

    def index_classify(
        self,
        level: str | None = None,
        src: str | None = None,
        fields: str | list[str] | None = None,
    ) -> pd.DataFrame:
        path = self._data_dir / "industry" / "sw_classify" / "data.parquet"
        if not path.exists():
            raise FileNotFoundError(
                "sw_classify data not found; run `python main.py sync --table industry` first"
            )
        selected = _parse_fields(fields, SW_CLASSIFY_COLS)
        where = []
        params = []
        if level is not None:
            where.append("level = ?")
            params.append(level)
        if src is not None:
            where.append("src = ?")
            params.append(src)
        sql = f"SELECT {', '.join(selected)} FROM read_parquet(?)"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY industry_code"
        df = duckdb.connect().execute(sql, [str(path), *params]).fetchdf()
        return df

    def index_member_all(
        self,
        l1_code: str | None = None,
        ts_code: str | None = None,
        is_new: str | None = None,
        fields: str | list[str] | None = None,
    ) -> pd.DataFrame:
        path = self._data_dir / "industry" / "sw_member" / "data.parquet"
        if not path.exists():
            raise FileNotFoundError(
                "sw_member data not found; run `python main.py sync --table industry` first"
            )
        selected = _parse_fields(fields, SW_MEMBER_COLS)
        where = []
        params = []
        if l1_code is not None:
            where.append("l1_code = ?")
            params.append(l1_code)
        if ts_code is not None:
            codes = [code.strip() for code in ts_code.split(",") if code.strip()]
            placeholders = ", ".join("?" for _ in codes)
            where.append(f"ts_code IN ({placeholders})")
            params.extend(codes)
        if is_new is not None:
            where.append("is_new = ?")
            params.append(is_new)
        sql = f"SELECT {', '.join(selected)} FROM read_parquet(?)"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY ts_code, l1_code"
        df = duckdb.connect().execute(sql, [str(path), *params]).fetchdf()
        return _format_date_columns(df, ["in_date", "out_date"])

    def ci_index_member(
        self,
        l1_code: str | None = None,
        ts_code: str | None = None,
        is_new: str | None = None,
        fields: str | list[str] | None = None,
    ) -> pd.DataFrame:
        path = self._data_dir / "industry" / "ci_member" / "data.parquet"
        if not path.exists():
            raise FileNotFoundError(
                "ci_member data not found; run `python main.py sync --table ci_member` first"
            )
        selected = _parse_fields(fields, CI_MEMBER_COLS)
        where = []
        params = []
        if l1_code is not None:
            where.append("l1_code = ?")
            params.append(l1_code)
        if ts_code is not None:
            codes = [code.strip() for code in ts_code.split(",") if code.strip()]
            placeholders = ", ".join("?" for _ in codes)
            where.append(f"ts_code IN ({placeholders})")
            params.extend(codes)
        if is_new is not None:
            where.append("is_new = ?")
            params.append(is_new)
        sql = f"SELECT {', '.join(selected)} FROM read_parquet(?)"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY ts_code, l1_code"
        df = duckdb.connect().execute(sql, [str(path), *params]).fetchdf()
        return _format_date_columns(df, ["in_date", "out_date"])

    def fut_basic(
        self,
        ts_code: str | None = None,
        exchange: str | None = None,
        fut_type: str | None = None,
        fut_code: str | None = None,
        fields: str | list[str] | None = None,
    ) -> pd.DataFrame:
        table_dir = self._data_dir / "futures" / "fut_basic"
        if not table_dir.exists():
            raise FileNotFoundError(
                "fut_basic data not found; run `python main.py sync --table fut_basic` first"
            )

        selected = _parse_fields(fields, FUT_BASIC_COLS)
        where = []
        params = []
        if ts_code is not None:
            codes = [code.strip() for code in ts_code.split(",") if code.strip()]
            placeholders = ", ".join("?" for _ in codes)
            where.append(f"ts_code IN ({placeholders})")
            params.extend(codes)
        if exchange is not None:
            where.append("exchange = ?")
            params.append(exchange)
        if fut_code is not None:
            where.append("fut_code = ?")
            params.append(fut_code)

        pattern = table_dir / "date=*" / "data.parquet"
        sql = (
            f"SELECT {', '.join(selected)} "
            "FROM read_parquet(?, hive_partitioning=true, union_by_name=true)"
        )
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY ts_code"

        df = duckdb.connect().execute(sql, [str(pattern), *params]).fetchdf()
        return _format_date_columns(df, ["list_date", "delist_date", "last_ddate"])

    def fut_daily(
        self,
        ts_code: str | None = None,
        trade_date: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        fields: str | list[str] | None = None,
    ) -> pd.DataFrame:
        return self._query_daily_partitioned(
            table_name="fut_daily",
            sync_table="fut_daily",
            columns=FUT_DAILY_COLS,
            ts_code=ts_code,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
            fields=fields,
            data_dir_override=self._data_dir / "futures",
        )

    def fut_holding(
        self,
        trade_date: str | None = None,
        symbol: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        exchange: str | None = None,
        fields: str | list[str] | None = None,
    ) -> pd.DataFrame:
        extra = {}
        if symbol is not None:
            extra["symbol"] = symbol
        if exchange is not None:
            extra["exchange"] = exchange
        return self._query_daily_partitioned(
            table_name="fut_holding",
            sync_table="fut_holding",
            columns=FUT_HOLDING_COLS,
            ts_code=None,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
            fields=fields,
            extra_filters=extra or None,
            data_dir_override=self._data_dir / "futures",
            order_by="trade_date, symbol, broker",
        )

    def fut_wsr(
        self,
        trade_date: str | None = None,
        symbol: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        exchange: str | None = None,
        fields: str | list[str] | None = None,
    ) -> pd.DataFrame:
        extra = {}
        if symbol is not None:
            extra["symbol"] = symbol
        if exchange is not None:
            extra["exchange"] = exchange
        return self._query_daily_partitioned(
            table_name="fut_wsr",
            sync_table="fut_wsr",
            columns=FUT_WSR_COLS,
            ts_code=None,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
            fields=fields,
            extra_filters=extra or None,
            data_dir_override=self._data_dir / "futures",
            order_by="trade_date, symbol, warehouse",
        )

    def fut_settle(
        self,
        ts_code: str | None = None,
        trade_date: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        exchange: str | None = None,
        fields: str | list[str] | None = None,
    ) -> pd.DataFrame:
        extra = {}
        if exchange is not None:
            extra["exchange"] = exchange
        return self._query_daily_partitioned(
            table_name="fut_settle",
            sync_table="fut_settle",
            columns=FUT_SETTLE_COLS,
            ts_code=ts_code,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
            fields=fields,
            extra_filters=extra or None,
            data_dir_override=self._data_dir / "futures",
        )

    def fut_mapping(
        self,
        ts_code: str | None = None,
        trade_date: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        fields: str | list[str] | None = None,
    ) -> pd.DataFrame:
        return self._query_daily_partitioned(
            table_name="fut_mapping",
            sync_table="fut_mapping",
            columns=FUT_MAPPING_COLS,
            ts_code=ts_code,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
            fields=fields,
            data_dir_override=self._data_dir / "futures",
            order_by="ts_code, trade_date",
        )

    def pro_bar(
        self,
        ts_code: str,
        start_date: str | None = None,
        end_date: str | None = None,
        asset: str = "E",
        adj: str | None = None,
        freq: str = "D",
        trade_date: str | None = None,
        ma: list[int] | None = None,
    ) -> pd.DataFrame:
        if asset != "E":
            raise NotImplementedError("local pro_bar currently only supports asset='E'")
        if freq != "D":
            raise NotImplementedError("local pro_bar currently only supports freq='D'")
        if ma:
            raise NotImplementedError("local pro_bar does not support ma yet")
        if adj not in (None, "qfq", "hfq"):
            raise ValueError("adj must be one of None, 'qfq', or 'hfq'")

        daily = self.daily(
            ts_code=ts_code,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
        )
        if adj is None or daily.empty:
            return daily

        factors = self.adj_factor(
            ts_code=ts_code,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
        )
        if factors.empty:
            return daily.iloc[0:0].copy()

        result = daily.merge(
            factors[["ts_code", "trade_date", "adj_factor"]],
            on=["ts_code", "trade_date"],
            how="left",
        ).sort_values(["ts_code", "trade_date"])
        result["adj_factor"] = result.groupby("ts_code")["adj_factor"].bfill()
        result = result.dropna(subset=["adj_factor"])
        if result.empty:
            return daily.iloc[0:0].copy()

        price_columns = ["open", "high", "low", "close", "pre_close"]
        if adj == "qfq":
            base_factor = result.groupby("ts_code")["adj_factor"].transform("last")
            multiplier = result["adj_factor"] / base_factor
        else:
            multiplier = result["adj_factor"]

        for column in price_columns:
            result[column] = (result[column] * multiplier).round(2)

        result["change"] = (result["close"] - result["pre_close"]).round(2)
        result["pct_chg"] = (result["change"] / result["pre_close"] * 100).round(2)

        return result.drop(columns=["adj_factor"])

    def query(self, api_name: str, **kwargs) -> pd.DataFrame:
        dispatch = {
            "stock_basic": self.stock_basic,
            "trade_cal": self.trade_cal,
            "daily": self.daily,
            "adj_factor": self.adj_factor,
            "daily_basic": self.daily_basic,
            "stock_st": self.stock_st,
            "suspend_d": self.suspend_d,
            "stk_limit": self.stk_limit,
            "index_daily": self.index_daily,
            "index_weight": self.index_weight,
            "universe": self.universe,
            "pro_bar": self.pro_bar,
            "index_classify": self.index_classify,
            "index_member_all": self.index_member_all,
            "ci_index_member": self.ci_index_member,
            "fut_basic": self.fut_basic,
            "fut_daily": self.fut_daily,
            "fut_holding": self.fut_holding,
            "fut_wsr": self.fut_wsr,
            "fut_settle": self.fut_settle,
            "fut_mapping": self.fut_mapping,
        }
        try:
            method = dispatch[api_name]
        except KeyError as e:
            raise ValueError(f"unknown api: {api_name}") from e
        return method(**kwargs)

    def _query_daily_partitioned(
        self,
        table_name: str,
        sync_table: str,
        columns: list[str],
        ts_code: str | None,
        trade_date: str | None,
        start_date: str | None,
        end_date: str | None,
        fields: str | list[str] | None,
        extra_filters: dict[str, str] | None = None,
        data_dir_override: Path | None = None,
        order_by: str = "ts_code, trade_date",
    ) -> pd.DataFrame:
        if trade_date is not None and (start_date is not None or end_date is not None):
            raise ValueError("trade_date cannot be combined with start_date or end_date")
        parsed_start = _parse_date(start_date) if start_date is not None else None
        parsed_end = _parse_date(end_date) if end_date is not None else None
        if parsed_start is not None and parsed_end is not None and parsed_end < parsed_start:
            raise ValueError("end_date must be on or after start_date")

        base_dir = data_dir_override or self._data_dir
        table_dir = base_dir / table_name
        if not table_dir.exists():
            raise FileNotFoundError(
                f"{sync_table} data not found; run `python main.py sync --table {sync_table}` first"
            )

        selected = _parse_fields(fields, columns)
        where = []
        params = []
        if ts_code is not None:
            codes = [code.strip() for code in ts_code.split(",") if code.strip()]
            placeholders = ", ".join("?" for _ in codes)
            where.append(f"ts_code IN ({placeholders})")
            params.extend(codes)
        if trade_date is not None:
            where.append("trade_date = ?")
            params.append(_parse_date(trade_date))
        if parsed_start is not None:
            where.append("trade_date >= ?")
            params.append(parsed_start)
        if parsed_end is not None:
            where.append("trade_date <= ?")
            params.append(parsed_end)
        if extra_filters is not None:
            for column, value in extra_filters.items():
                where.append(f"{column} = ?")
                params.append(value)

        pattern = table_dir / "date=*" / "data.parquet"
        sql = (
            f"SELECT {', '.join(selected)} "
            "FROM read_parquet(?, hive_partitioning=true, union_by_name=true)"
        )
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += f" ORDER BY {order_by}"

        df = duckdb.connect().execute(sql, [str(pattern), *params]).fetchdf()
        return _format_date_columns(df, ["trade_date"])


def pro_api(config_path: str | Path = "config/settings.toml") -> LocalPro:
    cfg = load_config(Path(config_path))
    return LocalPro(cfg.data_dir)


def _parse_fields(fields: str | list[str] | None, default_columns: list[str]) -> list[str]:
    if fields is None:
        return list(default_columns)
    if isinstance(fields, str):
        parsed = [field.strip() for field in fields.split(",") if field.strip()]
    else:
        parsed = list(fields)
    unknown = [field for field in parsed if field not in default_columns]
    if unknown:
        raise ValueError(f"unknown fields: {', '.join(unknown)}")
    return parsed


def _parse_date(value: str):
    for fmt in ("%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"invalid date format: {value}")


def _parse_is_open(value: str | int | bool) -> bool:
    if isinstance(value, bool):
        return value
    if value in (1, "1"):
        return True
    if value in (0, "0"):
        return False
    raise ValueError("is_open must be one of True, False, 1, 0, '1', or '0'")


def _format_date_columns(df: pd.DataFrame, date_columns: list[str]) -> pd.DataFrame:
    for column in date_columns:
        if column not in df.columns:
            continue
        formatted = pd.to_datetime(df[column], errors="coerce").dt.strftime("%Y%m%d")
        df[column] = formatted.astype(object)
        df.loc[formatted.isna(), column] = None
    return df
