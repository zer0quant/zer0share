from datetime import date

import pandas as pd
import pytest

from zer0share.storage import (
    MetaStore,
    daily_kline_partition_exists,
    read_basic,
    read_ci_member,
    read_daily_kline,
    read_sw_classify,
    read_sw_member,
    read_trade_cal,
    write_basic,
    write_ci_member,
    write_daily_kline,
    write_sw_classify,
    write_sw_member,
    write_trade_cal,
)


FULL_BASIC_COLUMNS = [
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


def _basic_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ts_code": ["000001.SZ"],
            "symbol": ["000001"],
            "name": ["平安银行"],
            "area": ["深圳"],
            "industry": ["银行"],
            "fullname": ["平安银行股份有限公司"],
            "enname": ["Ping An Bank"],
            "cnspell": ["payh"],
            "market": ["主板"],
            "exchange": ["SZSE"],
            "curr_type": ["CNY"],
            "list_status": ["L"],
            "list_date": [date(1991, 4, 3)],
            "delist_date": [None],
            "is_hs": ["S"],
            "act_name": ["深圳市投资控股有限公司"],
            "act_ent_type": ["地方国企"],
        }
    )


def _basic_df_two_rows() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "000002.SZ"],
            "symbol": ["000001", "000002"],
            "name": ["平安银行", "万科A"],
            "area": ["深圳", "深圳"],
            "industry": ["银行", "房地产"],
            "fullname": ["平安银行股份有限公司", "万科企业股份有限公司"],
            "enname": ["Ping An Bank", "China Vanke Co., Ltd."],
            "cnspell": ["payh", "wka"],
            "market": ["主板", "主板"],
            "exchange": ["SZSE", "SZSE"],
            "curr_type": ["CNY", "CNY"],
            "list_status": ["L", "L"],
            "list_date": [date(1991, 4, 3), date(1991, 1, 29)],
            "delist_date": [None, None],
            "is_hs": ["S", "S"],
            "act_name": ["深圳市投资控股有限公司", "深圳地铁集团有限公司"],
            "act_ent_type": ["地方国企", "地方国企"],
        }
    )


@pytest.fixture
def store(tmp_path):
    s = MetaStore(tmp_path / "meta.duckdb")
    yield s
    s.close()


def test_init_creates_table(store):
    assert store.get_last_date("daily_kline") is None


def test_update_and_get_last_date(store):
    store.update_last_date("daily_kline", date(2024, 1, 15))
    assert store.get_last_date("daily_kline") == date(2024, 1, 15)


def test_update_overwrites_previous(store):
    store.update_last_date("daily_kline", date(2024, 1, 1))
    store.update_last_date("daily_kline", date(2024, 1, 31))
    assert store.get_last_date("daily_kline") == date(2024, 1, 31)


def test_different_table_names_are_independent(store):
    store.update_last_date("daily_kline", date(2024, 1, 10))
    store.update_last_date("basic", date(2024, 2, 20))
    assert store.get_last_date("daily_kline") == date(2024, 1, 10)
    assert store.get_last_date("basic") == date(2024, 2, 20)


def test_context_manager(tmp_path):
    with MetaStore(tmp_path / "meta.duckdb") as store:
        store.update_last_date("daily_kline", date(2024, 1, 1))
        assert store.get_last_date("daily_kline") == date(2024, 1, 1)


def test_write_and_read_daily_kline(tmp_path):
    df = pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "000002.SZ"],
            "trade_date": [date(2024, 1, 2), date(2024, 1, 2)],
            "open": [10.0, 20.0],
            "high": [11.0, 21.0],
            "low": [9.5, 19.5],
            "close": [10.5, 20.5],
            "pre_close": [10.0, 20.0],
            "change": [0.5, 0.5],
            "pct_chg": [5.0, 2.5],
            "vol": [100000.0, 200000.0],
            "amount": [1050000.0, 4100000.0],
        }
    )
    write_daily_kline(tmp_path, date(2024, 1, 2), df)
    result = read_daily_kline(tmp_path, date(2024, 1, 2))
    assert len(result) == 2
    assert set(result["ts_code"]) == {"000001.SZ", "000002.SZ"}


def test_daily_kline_partition_path(tmp_path):
    df = pd.DataFrame(
        {
            "ts_code": ["000001.SZ"],
            "trade_date": [date(2024, 1, 2)],
            "open": [10.0],
            "high": [11.0],
            "low": [9.5],
            "close": [10.5],
            "pre_close": [10.0],
            "change": [0.5],
            "pct_chg": [5.0],
            "vol": [100000.0],
            "amount": [1050000.0],
        }
    )
    write_daily_kline(tmp_path, date(2024, 1, 2), df)
    assert (tmp_path / "daily_kline" / "date=20240102" / "data.parquet").exists()


def test_daily_kline_partition_exists(tmp_path):
    assert daily_kline_partition_exists(tmp_path, date(2024, 1, 2)) is False

    df = pd.DataFrame(
        {
            "ts_code": ["000001.SZ"],
            "trade_date": [date(2024, 1, 2)],
            "open": [10.0],
            "high": [11.0],
            "low": [9.5],
            "close": [10.5],
            "pre_close": [10.0],
            "change": [0.5],
            "pct_chg": [5.0],
            "vol": [100000.0],
            "amount": [1050000.0],
        }
    )
    write_daily_kline(tmp_path, date(2024, 1, 2), df)

    assert daily_kline_partition_exists(tmp_path, date(2024, 1, 2)) is True


def test_write_and_read_basic(tmp_path):
    df = _basic_df()
    write_basic(tmp_path, df)
    result = read_basic(tmp_path)
    assert len(result) == 1
    assert list(result.columns) == FULL_BASIC_COLUMNS
    assert result.iloc[0]["name"] == "平安银行"
    assert result.iloc[0]["fullname"] == "平安银行股份有限公司"


def test_basic_overwrites_on_second_write(tmp_path):
    write_basic(tmp_path, _basic_df())
    write_basic(tmp_path, _basic_df_two_rows())
    result = read_basic(tmp_path)
    assert len(result) == 2
    assert list(result.columns) == FULL_BASIC_COLUMNS
    assert set(result["ts_code"]) == {"000001.SZ", "000002.SZ"}


def test_read_daily_kline_returns_empty_if_not_exists(tmp_path):
    result = read_daily_kline(tmp_path, date(2024, 1, 2))
    assert result.empty


def test_read_basic_returns_empty_if_not_exists(tmp_path):
    result = read_basic(tmp_path)
    assert result.empty


def test_write_and_read_trade_cal(tmp_path):
    df = pd.DataFrame({
        "exchange": ["SSE", "SSE"],
        "cal_date": [date(2024, 1, 2), date(2024, 1, 3)],
        "is_open": [True, False],
        "pretrade_date": [date(2023, 12, 29), date(2024, 1, 2)],
    })
    write_trade_cal(tmp_path, "SSE", df)
    result = read_trade_cal(tmp_path, "SSE")
    assert len(result) == 2
    assert (tmp_path / "trade_cal" / "exchange=SSE" / "data.parquet").exists()


def test_read_trade_cal_returns_empty_if_not_exists(tmp_path):
    result = read_trade_cal(tmp_path, "SSE")
    assert result.empty


def test_load_trade_cal_from_parquet(tmp_path):
    db_path = tmp_path / "meta.duckdb"
    df = pd.DataFrame({
        "exchange": ["SSE", "SSE"],
        "cal_date": [date(2024, 1, 2), date(2024, 1, 3)],
        "is_open": [True, False],
        "pretrade_date": [date(2023, 12, 29), date(2024, 1, 2)],
    })
    write_trade_cal(tmp_path, "SSE", df)
    with MetaStore(db_path) as store:
        store.load_trade_cal_from_parquet(tmp_path)
        row = store._conn.execute(
            "SELECT COUNT(*) FROM trade_cal WHERE exchange='SSE'"
        ).fetchone()
        assert row[0] == 2


def test_load_trade_cal_from_parquet_filters_exchanges(tmp_path):
    db_path = tmp_path / "meta.duckdb"
    sse_df = pd.DataFrame({
        "exchange": ["SSE"],
        "cal_date": [date(2024, 1, 2)],
        "is_open": [True],
        "pretrade_date": [date(2023, 12, 29)],
    })
    cffex_df = pd.DataFrame({
        "exchange": ["CFFEX"],
        "cal_date": [date(2024, 1, 2)],
        "is_open": [True],
        "pretrade_date": [date(2023, 12, 29)],
    })
    write_trade_cal(tmp_path, "SSE", sse_df)
    write_trade_cal(tmp_path, "CFFEX", cffex_df)
    with MetaStore(db_path) as store:
        store.load_trade_cal_from_parquet(tmp_path, ["SSE"])
        exchanges = store._conn.execute(
            "SELECT DISTINCT exchange FROM trade_cal"
        ).fetchall()
    assert exchanges == [("SSE",)]


def test_get_trading_days(tmp_path):
    db_path = tmp_path / "meta.duckdb"
    df = pd.DataFrame({
        "exchange": ["SSE"] * 5,
        "cal_date": [
            date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4),
            date(2024, 1, 5), date(2024, 1, 6),
        ],
        "is_open": [True, False, True, False, True],
        "pretrade_date": [
            date(2023, 12, 29), date(2024, 1, 2), date(2024, 1, 2),
            date(2024, 1, 4), date(2024, 1, 4),
        ],
    })
    write_trade_cal(tmp_path, "SSE", df)
    with MetaStore(db_path) as store:
        store.load_trade_cal_from_parquet(tmp_path)
        days = store.get_trading_days("SSE", date(2024, 1, 1), date(2024, 1, 6))
    assert days == [date(2024, 1, 2), date(2024, 1, 4), date(2024, 1, 6)]


def test_get_trading_days_returns_empty_when_no_cal(tmp_path):
    db_path = tmp_path / "meta.duckdb"
    with MetaStore(db_path) as store:
        days = store.get_trading_days("SSE", date(2024, 1, 1), date(2024, 1, 6))
    assert days == []


def test_get_trading_days_exchange_isolation(tmp_path):
    db_path = tmp_path / "meta.duckdb"
    sse_df = pd.DataFrame({
        "exchange": ["SSE"],
        "cal_date": [date(2024, 1, 2)],
        "is_open": [True],
        "pretrade_date": [date(2023, 12, 29)],
    })
    szse_df = pd.DataFrame({
        "exchange": ["SZSE"],
        "cal_date": [date(2024, 1, 3)],
        "is_open": [True],
        "pretrade_date": [date(2024, 1, 2)],
    })
    write_trade_cal(tmp_path, "SSE", sse_df)
    write_trade_cal(tmp_path, "SZSE", szse_df)
    with MetaStore(db_path) as store:
        store.load_trade_cal_from_parquet(tmp_path)
        sse_days = store.get_trading_days("SSE", date(2024, 1, 1), date(2024, 1, 6))
        szse_days = store.get_trading_days("SZSE", date(2024, 1, 1), date(2024, 1, 6))
    assert sse_days == [date(2024, 1, 2)]
    assert szse_days == [date(2024, 1, 3)]


def test_write_and_read_sw_classify(tmp_path):
    df = pd.DataFrame(
        {
            "index_code": ["801010.SI", "801030.SI"],
            "industry_name": ["农林牧渔", "化工"],
            "level": ["L1", "L1"],
            "parent_code": ["0", "0"],
            "industry_code": ["110000", "220000"],
            "is_pub": ["1", "1"],
            "src": ["SW2021", "SW2021"],
        }
    )
    write_sw_classify(tmp_path, df)
    result = read_sw_classify(tmp_path)
    assert len(result) == 2
    assert result.iloc[0]["industry_name"] == "农林牧渔"
    assert (tmp_path / "industry" / "sw_classify" / "data.parquet").exists()


def test_sw_classify_overwrites_on_second_write(tmp_path):
    df1 = pd.DataFrame(
        {
            "index_code": ["801010.SI"],
            "industry_name": ["农林牧渔"],
            "level": ["L1"],
            "parent_code": ["0"],
            "industry_code": ["110000"],
            "is_pub": ["1"],
            "src": ["SW2021"],
        }
    )
    df2 = pd.DataFrame(
        {
            "index_code": ["801010.SI", "801030.SI"],
            "industry_name": ["农林牧渔", "化工"],
            "level": ["L1", "L1"],
            "parent_code": ["0", "0"],
            "industry_code": ["110000", "220000"],
            "is_pub": ["1", "1"],
            "src": ["SW2021", "SW2021"],
        }
    )
    write_sw_classify(tmp_path, df1)
    write_sw_classify(tmp_path, df2)
    result = read_sw_classify(tmp_path)
    assert len(result) == 2


def test_read_sw_classify_returns_empty_if_not_exists(tmp_path):
    result = read_sw_classify(tmp_path)
    assert result.empty


def test_write_and_read_sw_member(tmp_path):
    df = pd.DataFrame(
        {
            "l1_code": ["801010.SI", "801010.SI"],
            "l1_name": ["农林牧渔", "农林牧渔"],
            "l2_code": ["801016.SI", "801016.SI"],
            "l2_name": ["种植业", "种植业"],
            "l3_code": ["850111.SI", "850112.SI"],
            "l3_name": ["种子", "粮食种植"],
            "ts_code": ["002041.SZ", "600313.SH"],
            "name": ["登海种业", "农发种业"],
            "in_date": [date(2021, 12, 13), date(2021, 12, 13)],
            "out_date": [None, None],
            "is_new": ["Y", "Y"],
        }
    )
    write_sw_member(tmp_path, df)
    result = read_sw_member(tmp_path)
    assert len(result) == 2
    assert result.iloc[0]["ts_code"] == "002041.SZ"
    assert (tmp_path / "industry" / "sw_member" / "data.parquet").exists()


def test_sw_member_overwrites_on_second_write(tmp_path):
    df1 = pd.DataFrame(
        {
            "l1_code": ["801010.SI"],
            "l1_name": ["农林牧渔"],
            "l2_code": ["801016.SI"],
            "l2_name": ["种植业"],
            "l3_code": ["850111.SI"],
            "l3_name": ["种子"],
            "ts_code": ["002041.SZ"],
            "name": ["登海种业"],
            "in_date": [date(2021, 12, 13)],
            "out_date": [None],
            "is_new": ["Y"],
        }
    )
    df2 = pd.DataFrame(
        {
            "l1_code": ["801010.SI", "801010.SI"],
            "l1_name": ["农林牧渔", "农林牧渔"],
            "l2_code": ["801016.SI", "801016.SI"],
            "l2_name": ["种植业", "种植业"],
            "l3_code": ["850111.SI", "850112.SI"],
            "l3_name": ["种子", "粮食种植"],
            "ts_code": ["002041.SZ", "600313.SH"],
            "name": ["登海种业", "农发种业"],
            "in_date": [date(2021, 12, 13), date(2021, 12, 13)],
            "out_date": [None, None],
            "is_new": ["Y", "Y"],
        }
    )
    write_sw_member(tmp_path, df1)
    write_sw_member(tmp_path, df2)
    result = read_sw_member(tmp_path)
    assert len(result) == 2


def test_read_sw_member_returns_empty_if_not_exists(tmp_path):
    result = read_sw_member(tmp_path)
    assert result.empty


def test_write_and_read_ci_member(tmp_path):
    df = pd.DataFrame(
        {
            "l1_code": ["CI005001.CI", "CI005001.CI"],
            "l1_name": ["农林牧渔", "农林牧渔"],
            "l2_code": ["CI005005.CI", "CI005005.CI"],
            "l2_name": ["农产品加工", "农产品加工"],
            "l3_code": ["CI005006.CI", "CI005007.CI"],
            "l3_name": ["粮油加工", "果蔬加工"],
            "ts_code": ["000876.SZ", "600737.SH"],
            "name": ["新 希 望", "中粮糖业"],
            "in_date": [date(2020, 1, 1), date(2020, 1, 1)],
            "out_date": [None, None],
            "is_new": ["Y", "Y"],
        }
    )
    write_ci_member(tmp_path, df)
    result = read_ci_member(tmp_path)
    assert len(result) == 2
    assert result.iloc[0]["ts_code"] == "000876.SZ"
    assert (tmp_path / "industry" / "ci_member" / "data.parquet").exists()


def test_ci_member_overwrites_on_second_write(tmp_path):
    df1 = pd.DataFrame(
        {
            "l1_code": ["CI005001.CI"],
            "l1_name": ["农林牧渔"],
            "l2_code": ["CI005005.CI"],
            "l2_name": ["农产品加工"],
            "l3_code": ["CI005006.CI"],
            "l3_name": ["粮油加工"],
            "ts_code": ["000876.SZ"],
            "name": ["新 希 望"],
            "in_date": [date(2020, 1, 1)],
            "out_date": [None],
            "is_new": ["Y"],
        }
    )
    df2 = pd.DataFrame(
        {
            "l1_code": ["CI005001.CI", "CI005002.CI"],
            "l1_name": ["农林牧渔", "采掘"],
            "l2_code": ["CI005005.CI", "CI005010.CI"],
            "l2_name": ["农产品加工", "煤炭开采"],
            "l3_code": ["CI005006.CI", "CI005011.CI"],
            "l3_name": ["粮油加工", "动力煤"],
            "ts_code": ["000876.SZ", "601088.SH"],
            "name": ["新 希 望", "中国神华"],
            "in_date": [date(2020, 1, 1), date(2020, 1, 1)],
            "out_date": [None, None],
            "is_new": ["Y", "Y"],
        }
    )
    write_ci_member(tmp_path, df1)
    write_ci_member(tmp_path, df2)
    result = read_ci_member(tmp_path)
    assert len(result) == 2


def test_read_ci_member_returns_empty_if_not_exists(tmp_path):
    result = read_ci_member(tmp_path)
    assert result.empty
