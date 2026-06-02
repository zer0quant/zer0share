import pandas as pd
import pytest
import time
from datetime import date
from unittest.mock import patch

from zer0share.fetcher import TushareFetcher


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

INDEX_DAILY_COLS = [
    "ts_code", "trade_date", "open", "high", "low",
    "close", "pre_close", "change", "pct_chg", "vol", "amount",
]


def _basic_row(
    *,
    list_status: str = "L",
    list_date: str = "19910403",
    delist_date: str | None = None,
) -> dict[str, object]:
    return {
        "ts_code": "000001.SZ",
        "symbol": "000001",
        "name": "平安银行",
        "area": "深圳",
        "industry": "银行",
        "fullname": "平安银行股份有限公司",
        "enname": "Ping An Bank",
        "cnspell": "payh",
        "market": "主板",
        "exchange": "SZSE",
        "curr_type": "CNY",
        "list_status": list_status,
        "list_date": list_date,
        "delist_date": delist_date,
        "is_hs": "S",
        "act_name": "深圳市投资控股有限公司",
        "act_ent_type": "地方国企",
    }


def _index_daily_row(ts_code: str = "000300.SH", trade_date: str = "20240102") -> dict:
    return {
        "ts_code": ts_code,
        "trade_date": trade_date,
        "open": 3500.0,
        "high": 3550.0,
        "low": 3480.0,
        "close": 3520.0,
        "pre_close": 3490.0,
        "change": 30.0,
        "pct_chg": 0.86,
        "vol": 50000000.0,
        "amount": 1750000000.0,
    }


@pytest.fixture
def mock_pro():
    with patch("tushare.pro_api") as mock:
        yield mock.return_value


def test_fetch_basic_returns_all_documented_columns(mock_pro):
    mock_pro.stock_basic.return_value = pd.DataFrame([_basic_row()])
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_basic()

    assert list(df.columns) == BASIC_COLS
    assert len(df) == 1


def test_fetch_basic_requests_all_statuses_and_fields(mock_pro):
    mock_pro.stock_basic.return_value = pd.DataFrame([_basic_row()])
    fetcher = TushareFetcher("fake_token")

    fetcher.fetch_basic()

    mock_pro.stock_basic.assert_called_once_with(
        exchange="",
        list_status="L,D,P,G",
        fields=",".join(BASIC_COLS),
    )


def test_fetch_basic_converts_only_date_fields(mock_pro):
    mock_pro.stock_basic.return_value = pd.DataFrame(
        [_basic_row(list_status="D", delist_date="20240131")]
    )
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_basic()

    assert df.iloc[0]["list_date"] == date(1991, 4, 3)
    assert df.iloc[0]["delist_date"] == date(2024, 1, 31)
    assert df.iloc[0]["fullname"] == "平安银行股份有限公司"
    assert df.iloc[0]["act_ent_type"] == "地方国企"


def test_fetch_daily_kline_returns_correct_data(mock_pro):
    mock_pro.daily.return_value = pd.DataFrame(
        {
            "ts_code": ["000001.SZ"],
            "trade_date": ["20240102"],
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
    fetcher = TushareFetcher("fake_token")
    df = fetcher.fetch_daily_kline(date(2024, 1, 2))
    assert len(df) == 1
    assert df.iloc[0]["ts_code"] == "000001.SZ"
    assert df.iloc[0]["trade_date"] == date(2024, 1, 2)


def test_fetch_daily_kline_returns_empty_on_no_data(mock_pro):
    mock_pro.daily.return_value = pd.DataFrame()
    fetcher = TushareFetcher("fake_token")
    df = fetcher.fetch_daily_kline(date(2024, 1, 1))
    assert df.empty


def test_fetch_daily_kline_returns_empty_when_none(mock_pro):
    mock_pro.daily.return_value = None
    fetcher = TushareFetcher("fake_token")
    df = fetcher.fetch_daily_kline(date(2024, 1, 1))
    assert df.empty


def test_fetch_trade_cal_returns_correct_columns(mock_pro):
    mock_pro.trade_cal.return_value = pd.DataFrame({
        "exchange": ["SSE", "SSE"],
        "cal_date": ["20240102", "20240103"],
        "is_open": ["1", "0"],
        "pretrade_date": ["20231229", "20240102"],
    })
    fetcher = TushareFetcher("fake_token")
    df = fetcher.fetch_trade_cal("SSE")
    assert list(df.columns) == ["exchange", "cal_date", "is_open", "pretrade_date"]
    assert len(df) == 2


def test_fetch_trade_cal_uses_date_range(mock_pro):
    mock_pro.trade_cal.return_value = pd.DataFrame({
        "exchange": ["SSE"],
        "cal_date": ["20240102"],
        "is_open": ["1"],
        "pretrade_date": ["20231229"],
    })
    fetcher = TushareFetcher("fake_token")

    fetcher.fetch_trade_cal("SSE", date(2024, 1, 1), date(2024, 12, 31))

    mock_pro.trade_cal.assert_called_once_with(
        exchange="SSE",
        start_date="20240101",
        end_date="20241231",
        fields="exchange,cal_date,is_open,pretrade_date",
    )


def test_fetch_trade_cal_converts_types(mock_pro):
    mock_pro.trade_cal.return_value = pd.DataFrame({
        "exchange": ["SSE", "SSE"],
        "cal_date": ["20240102", "20240103"],
        "is_open": ["1", "0"],
        "pretrade_date": ["20231229", "20240102"],
    })
    fetcher = TushareFetcher("fake_token")
    df = fetcher.fetch_trade_cal("SSE")
    assert df.iloc[0]["cal_date"] == date(2024, 1, 2)
    assert df.iloc[0]["is_open"] is True
    assert df.iloc[1]["is_open"] is False
    assert df.iloc[0]["pretrade_date"] == date(2023, 12, 29)


def test_fetch_trade_cal_returns_empty_when_none(mock_pro):
    mock_pro.trade_cal.return_value = None
    fetcher = TushareFetcher("fake_token")
    df = fetcher.fetch_trade_cal("SSE")
    assert df.empty


def test_fetch_sw_classify_calls_all_levels(mock_pro):
    sw2014_l1 = pd.DataFrame({
        "index_code": ["801010.SI"],
        "industry_name": ["农林牧渔"],
        "level": ["L1"],
        "parent_code": ["0"],
        "industry_code": ["110000"],
        "is_pub": ["1"],
    })
    sw2014_l2 = pd.DataFrame({
        "index_code": ["801016.SI"],
        "industry_name": ["种植业"],
        "level": ["L2"],
        "parent_code": ["110000"],
        "industry_code": ["110100"],
        "is_pub": ["1"],
    })
    sw2014_l3 = pd.DataFrame({
        "index_code": ["850111.SI"],
        "industry_name": ["种子"],
        "level": ["L3"],
        "parent_code": ["110100"],
        "industry_code": ["110101"],
        "is_pub": ["1"],
    })
    sw2021_l1 = pd.DataFrame({
        "index_code": ["801011.SI"],
        "industry_name": ["农林牧渔"],
        "level": ["L1"],
        "parent_code": ["0"],
        "industry_code": ["210000"],
        "is_pub": ["1"],
    })
    sw2021_l2 = pd.DataFrame({
        "index_code": ["801017.SI"],
        "industry_name": ["种植业"],
        "level": ["L2"],
        "parent_code": ["210000"],
        "industry_code": ["210100"],
        "is_pub": ["1"],
    })
    sw2021_l3 = pd.DataFrame({
        "index_code": ["850112.SI"],
        "industry_name": ["种子"],
        "level": ["L3"],
        "parent_code": ["210100"],
        "industry_code": ["210101"],
        "is_pub": ["1"],
    })
    mock_pro.index_classify.side_effect = [
        sw2014_l1, sw2014_l2, sw2014_l3,
        sw2021_l1, sw2021_l2, sw2021_l3,
    ]
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_sw_classify()

    assert list(df.columns) == SW_CLASSIFY_COLS
    assert len(df) == 6
    mock_pro.index_classify.assert_any_call(level="L1", src="SW2014")
    mock_pro.index_classify.assert_any_call(level="L2", src="SW2014")
    mock_pro.index_classify.assert_any_call(level="L3", src="SW2014")
    mock_pro.index_classify.assert_any_call(level="L1", src="SW2021")
    mock_pro.index_classify.assert_any_call(level="L2", src="SW2021")
    mock_pro.index_classify.assert_any_call(level="L3", src="SW2021")


def test_fetch_sw_member_iterates_l1_codes(mock_pro):
    # SW2014 L1 codes
    sw2014_l1 = pd.DataFrame({
        "index_code": ["801010.SI", "801030.SI"],
        "industry_name": ["农林牧渔", "化工"],
        "level": ["L1", "L1"],
    })
    # SW2021 L1 codes
    sw2021_l1 = pd.DataFrame({
        "index_code": ["801011.SI"],
        "industry_name": ["农林牧渔"],
        "level": ["L1"],
    })
    mock_pro.index_classify.side_effect = [sw2014_l1, sw2021_l1]
    member_dfs = [
        # SW2014 801010 is_new="Y"
        pd.DataFrame({
            "l1_code": ["801010.SI"], "l1_name": ["农林牧渔"],
            "l2_code": ["801016.SI"], "l2_name": ["种植业"],
            "l3_code": ["850111.SI"], "l3_name": ["种子"],
            "ts_code": ["002041.SZ"], "name": ["登海种业"],
            "in_date": ["20211213"], "out_date": [None], "is_new": ["Y"],
        }),
        # SW2014 801010 is_new="N"
        pd.DataFrame({
            "l1_code": ["801010.SI"], "l1_name": ["农林牧渔"],
            "l2_code": ["801016.SI"], "l2_name": ["种植业"],
            "l3_code": ["850111.SI"], "l3_name": ["种子"],
            "ts_code": ["600313.SH"], "name": ["农发种业"],
            "in_date": ["20180101"], "out_date": ["20211213"], "is_new": ["N"],
        }),
        # SW2014 801030 is_new="Y"
        pd.DataFrame({
            "l1_code": ["801030.SI"], "l1_name": ["化工"],
            "l2_code": ["801033.SI"], "l2_name": ["化学原料"],
            "l3_code": ["850321.SI"], "l3_name": ["纯碱"],
            "ts_code": ["600291.SH"], "name": ["西水股份"],
            "in_date": ["20211213"], "out_date": [None], "is_new": ["Y"],
        }),
        # SW2014 801030 is_new="N"
        pd.DataFrame(columns=["l1_code", "l1_name", "l2_code", "l2_name",
                               "l3_code", "l3_name", "ts_code", "name",
                               "in_date", "out_date", "is_new"]),
        # SW2021 801011 is_new="Y"
        pd.DataFrame({
            "l1_code": ["801011.SI"], "l1_name": ["农林牧渔"],
            "l2_code": ["801017.SI"], "l2_name": ["种植业"],
            "l3_code": ["850112.SI"], "l3_name": ["种子"],
            "ts_code": ["002041.SZ"], "name": ["登海种业"],
            "in_date": ["20211213"], "out_date": [None], "is_new": ["Y"],
        }),
        # SW2021 801011 is_new="N"
        pd.DataFrame(columns=["l1_code", "l1_name", "l2_code", "l2_name",
                               "l3_code", "l3_name", "ts_code", "name",
                               "in_date", "out_date", "is_new"]),
    ]
    mock_pro.index_member_all.side_effect = member_dfs
    fetcher = TushareFetcher("fake_token")

    with patch("zer0share.fetcher.time.sleep"):
        df = fetcher.fetch_sw_member()

    assert list(df.columns) == SW_MEMBER_COLS
    assert len(df) == 4
    # SW2014
    mock_pro.index_member_all.assert_any_call(l1_code="801010.SI", is_new="Y")
    mock_pro.index_member_all.assert_any_call(l1_code="801010.SI", is_new="N")
    mock_pro.index_member_all.assert_any_call(l1_code="801030.SI", is_new="Y")
    mock_pro.index_member_all.assert_any_call(l1_code="801030.SI", is_new="N")
    # SW2021
    mock_pro.index_member_all.assert_any_call(l1_code="801011.SI", is_new="Y")
    mock_pro.index_member_all.assert_any_call(l1_code="801011.SI", is_new="N")


def test_fetch_sw_member_converts_dates(mock_pro):
    sw2014_l1 = pd.DataFrame({"index_code": ["801010.SI"], "industry_name": ["农林牧渔"], "level": ["L1"]})
    sw2021_l1 = pd.DataFrame({"index_code": ["801011.SI"], "industry_name": ["农林牧渔"], "level": ["L1"]})
    mock_pro.index_classify.side_effect = [sw2014_l1, sw2021_l1]
    mock_pro.index_member_all.side_effect = [
        # SW2014 801010 is_new="Y"
        pd.DataFrame({
            "l1_code": ["801010.SI"], "l1_name": ["农林牧渔"],
            "l2_code": ["801016.SI"], "l2_name": ["种植业"],
            "l3_code": ["850111.SI"], "l3_name": ["种子"],
            "ts_code": ["002041.SZ"], "name": ["登海种业"],
            "in_date": ["20211213"], "out_date": [None], "is_new": ["Y"],
        }),
        # SW2014 801010 is_new="N"
        pd.DataFrame({
            "l1_code": ["801010.SI"], "l1_name": ["农林牧渔"],
            "l2_code": ["801016.SI"], "l2_name": ["种植业"],
            "l3_code": ["850111.SI"], "l3_name": ["种子"],
            "ts_code": ["002041.SZ"], "name": ["登海种业"],
            "in_date": ["20211213"], "out_date": ["20220630"], "is_new": ["N"],
        }),
        # SW2021 801011 is_new="Y"
        pd.DataFrame({
            "l1_code": ["801011.SI"], "l1_name": ["农林牧渔"],
            "l2_code": ["801017.SI"], "l2_name": ["种植业"],
            "l3_code": ["850112.SI"], "l3_name": ["种子"],
            "ts_code": ["002041.SZ"], "name": ["登海种业"],
            "in_date": ["20211213"], "out_date": [None], "is_new": ["Y"],
        }),
        # SW2021 801011 is_new="N"
        pd.DataFrame(columns=["l1_code", "l1_name", "l2_code", "l2_name",
                               "l3_code", "l3_name", "ts_code", "name",
                               "in_date", "out_date", "is_new"]),
    ]
    fetcher = TushareFetcher("fake_token")

    with patch("zer0share.fetcher.time.sleep"):
        df = fetcher.fetch_sw_member()

    # Two versions with different l3_codes don't deduplicate
    assert len(df) == 2
    for row in df.itertuples():
        assert row.in_date == date(2021, 12, 13)
    # SW2014 row kept last (has out_date from is_new="N")
    sw2014_row = df[df["l3_code"] == "850111.SI"].iloc[0]
    assert sw2014_row["out_date"] == date(2022, 6, 30)
    # SW2021 row (is_new="Y", no out_date)
    sw2021_row = df[df["l3_code"] == "850112.SI"].iloc[0]
    assert sw2021_row["out_date"] is None


def test_fetch_ci_member_iterates_l1_codes(mock_pro):
    initial_df = pd.DataFrame({
        "l1_code": ["CI005001.CI", "CI005002.CI"],
        "l1_name": ["农林牧渔", "采掘"],
    })
    mock_pro.ci_index_member.return_value = initial_df
    member_dfs = [
        # CI005001 is_new="Y"
        pd.DataFrame({
            "l1_code": ["CI005001.CI"], "l1_name": ["农林牧渔"],
            "l2_code": ["CI005005.CI"], "l2_name": ["农产品加工"],
            "l3_code": ["CI005006.CI"], "l3_name": ["粮油加工"],
            "ts_code": ["000876.SZ"], "name": ["新 希 望"],
            "in_date": ["20200101"], "out_date": [None], "is_new": ["Y"],
        }),
        # CI005001 is_new="N"
        pd.DataFrame(columns=["l1_code", "l1_name", "l2_code", "l2_name",
                               "l3_code", "l3_name", "ts_code", "name",
                               "in_date", "out_date", "is_new"]),
        # CI005002 is_new="Y"
        pd.DataFrame({
            "l1_code": ["CI005002.CI"], "l1_name": ["采掘"],
            "l2_code": ["CI005010.CI"], "l2_name": ["煤炭开采"],
            "l3_code": ["CI005011.CI"], "l3_name": ["动力煤"],
            "ts_code": ["601088.SH"], "name": ["中国神华"],
            "in_date": ["20200101"], "out_date": [None], "is_new": ["Y"],
        }),
        # CI005002 is_new="N"
        pd.DataFrame(columns=["l1_code", "l1_name", "l2_code", "l2_name",
                               "l3_code", "l3_name", "ts_code", "name",
                               "in_date", "out_date", "is_new"]),
    ]
    mock_pro.ci_index_member.side_effect = [initial_df] + member_dfs
    fetcher = TushareFetcher("fake_token")

    with patch("zer0share.fetcher.time.sleep"):
        df = fetcher.fetch_ci_member()

    assert list(df.columns) == CI_MEMBER_COLS
    assert len(df) == 2
    mock_pro.ci_index_member.assert_any_call(l1_code="CI005001.CI", is_new="Y")
    mock_pro.ci_index_member.assert_any_call(l1_code="CI005001.CI", is_new="N")
    mock_pro.ci_index_member.assert_any_call(l1_code="CI005002.CI", is_new="Y")
    mock_pro.ci_index_member.assert_any_call(l1_code="CI005002.CI", is_new="N")


def test_fetch_ci_member_converts_dates(mock_pro):
    initial_df = pd.DataFrame({
        "l1_code": ["CI005001.CI"], "l1_name": ["农林牧渔"],
    })
    mock_pro.ci_index_member.side_effect = [
        initial_df,
        # is_new="Y"
        pd.DataFrame({
            "l1_code": ["CI005001.CI"], "l1_name": ["农林牧渔"],
            "l2_code": ["CI005005.CI"], "l2_name": ["农产品加工"],
            "l3_code": ["CI005006.CI"], "l3_name": ["粮油加工"],
            "ts_code": ["000876.SZ"], "name": ["新 希 望"],
            "in_date": ["20200101"], "out_date": [None], "is_new": ["Y"],
        }),
        # is_new="N"
        pd.DataFrame({
            "l1_code": ["CI005001.CI"], "l1_name": ["农林牧渔"],
            "l2_code": ["CI005005.CI"], "l2_name": ["农产品加工"],
            "l3_code": ["CI005006.CI"], "l3_name": ["粮油加工"],
            "ts_code": ["000876.SZ"], "name": ["新 希 望"],
            "in_date": ["20200101"], "out_date": ["20231231"], "is_new": ["N"],
        }),
    ]
    fetcher = TushareFetcher("fake_token")

    with patch("zer0share.fetcher.time.sleep"):
        df = fetcher.fetch_ci_member()

    assert len(df) == 1
    assert df.iloc[0]["in_date"] == date(2020, 1, 1)
    assert df.iloc[0]["out_date"] == date(2023, 12, 31)


def test_fetch_index_daily_returns_correct_columns(mock_pro):
    mock_pro.index_daily.return_value = pd.DataFrame([_index_daily_row()])
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_index_daily("000300.SH", date(2024, 1, 1), date(2024, 1, 31))

    assert list(df.columns) == INDEX_DAILY_COLS
    assert len(df) == 1


def test_fetch_index_daily_calls_api_with_correct_params(mock_pro):
    mock_pro.index_daily.return_value = pd.DataFrame([_index_daily_row()])
    fetcher = TushareFetcher("fake_token")

    fetcher.fetch_index_daily("000300.SH", date(2024, 1, 1), date(2024, 1, 31))

    mock_pro.index_daily.assert_called_once_with(
        ts_code="000300.SH",
        start_date="20240101",
        end_date="20240131",
        fields=",".join(INDEX_DAILY_COLS),
    )


def test_fetch_index_daily_converts_trade_date(mock_pro):
    mock_pro.index_daily.return_value = pd.DataFrame([_index_daily_row()])
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_index_daily("000300.SH", date(2024, 1, 1), date(2024, 1, 31))

    assert df.iloc[0]["trade_date"] == date(2024, 1, 2)


def test_fetch_index_daily_returns_empty_when_none(mock_pro):
    mock_pro.index_daily.return_value = None
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_index_daily("000300.SH", date(2024, 1, 1), date(2024, 1, 31))

    assert df.empty
    assert list(df.columns) == INDEX_DAILY_COLS


def test_fetch_index_daily_returns_empty_when_empty_df(mock_pro):
    mock_pro.index_daily.return_value = pd.DataFrame()
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_index_daily("000300.SH", date(2024, 1, 1), date(2024, 1, 31))

    assert df.empty


# --- Futures tests ---

from zer0share.fetcher import (
    FUT_BASIC_COLS, FUT_DAILY_COLS, FUT_HOLDING_COLS,
    FUT_WSR_COLS, FUT_SETTLE_COLS, FUT_MAPPING_COLS,
    FUTURES_EXCHANGES,
)


def _fut_basic_row(exchange: str = "SHFE", list_date: str = "20240101") -> dict:
    return {
        "ts_code": "CU2401.SHF",
        "symbol": "CU2401",
        "exchange": exchange,
        "name": "沪铜2401",
        "fut_code": "CU",
        "multiplier": None,
        "trade_unit": "5吨/手",
        "per_unit": 5.0,
        "quote_unit": "元(人民币)/吨",
        "quote_unit_desc": "10元/吨",
        "d_mode_desc": "实物交割",
        "list_date": list_date,
        "delist_date": "20240115",
        "d_month": "202401",
        "last_ddate": "20240115",
        "trade_time_desc": None,
    }


def _fut_daily_row(ts_code: str = "CU2401.SHF", trade_date: str = "20240102") -> dict:
    return {
        "ts_code": ts_code,
        "trade_date": trade_date,
        "pre_close": 50000.0,
        "pre_settle": 50100.0,
        "open": 50200.0,
        "high": 50500.0,
        "low": 49900.0,
        "close": 50300.0,
        "settle": 50250.0,
        "change1": 200.0,
        "change2": 150.0,
        "vol": 10000.0,
        "amount": 251250.0,
        "oi": 50000.0,
        "oi_chg": 500.0,
        "delv_settle": None,
    }


def test_futures_exchanges_has_six_entries():
    assert len(FUTURES_EXCHANGES) == 6
    assert "CZCE" in FUTURES_EXCHANGES
    assert "SHFE" in FUTURES_EXCHANGES
    assert "DCE" in FUTURES_EXCHANGES
    assert "CFFEX" in FUTURES_EXCHANGES
    assert "INE" in FUTURES_EXCHANGES
    assert "GFEX" in FUTURES_EXCHANGES


def test_fetch_fut_basic_returns_correct_columns(mock_pro):
    mock_pro.fut_basic.return_value = pd.DataFrame([_fut_basic_row()])
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_fut_basic("SHFE", "1")

    assert list(df.columns) == FUT_BASIC_COLS
    assert len(df) == 1


def test_fetch_fut_basic_converts_dates(mock_pro):
    mock_pro.fut_basic.return_value = pd.DataFrame([_fut_basic_row()])
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_fut_basic("SHFE", "1")

    assert df.iloc[0]["list_date"] == date(2024, 1, 1)
    assert df.iloc[0]["delist_date"] == date(2024, 1, 15)
    assert df.iloc[0]["d_month"] == "202401"  # kept as string, not converted to date
    assert df.iloc[0]["last_ddate"] == date(2024, 1, 15)


def test_fetch_fut_basic_returns_empty_when_none(mock_pro):
    mock_pro.fut_basic.return_value = None
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_fut_basic("SHFE", "1")

    assert df.empty
    assert list(df.columns) == FUT_BASIC_COLS


def test_fetch_fut_basic_calls_api_correctly(mock_pro):
    mock_pro.fut_basic.return_value = pd.DataFrame([_fut_basic_row()])
    fetcher = TushareFetcher("fake_token")

    fetcher.fetch_fut_basic("DCE", "2")

    mock_pro.fut_basic.assert_called_once_with(
        exchange="DCE",
        fut_type="2",
        fields=",".join(FUT_BASIC_COLS),
    )


def test_fetch_fut_daily_returns_correct_columns(mock_pro):
    mock_pro.fut_daily.return_value = pd.DataFrame([_fut_daily_row()])
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_fut_daily(date(2024, 1, 2))

    assert list(df.columns) == FUT_DAILY_COLS
    assert len(df) == 1
    assert df.iloc[0]["trade_date"] == date(2024, 1, 2)


def test_fetch_fut_daily_returns_empty_when_none(mock_pro):
    mock_pro.fut_daily.return_value = None
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_fut_daily(date(2024, 1, 1))

    assert df.empty
    assert list(df.columns) == FUT_DAILY_COLS


def test_fetch_fut_daily_calls_api_with_date(mock_pro):
    mock_pro.fut_daily.return_value = pd.DataFrame([_fut_daily_row()])
    fetcher = TushareFetcher("fake_token")

    fetcher.fetch_fut_daily(date(2024, 1, 2))

    mock_pro.fut_daily.assert_called_once_with(
        trade_date="20240102",
        fields=",".join(FUT_DAILY_COLS),
    )


def test_fetch_fut_holding_returns_correct_columns(mock_pro):
    mock_pro.fut_holding.return_value = pd.DataFrame({
        "trade_date": ["20240102"],
        "symbol": ["CU"],
        "broker": ["中信期货"],
        "vol": [10000],
        "vol_chg": [500],
        "long_hld": [15000],
        "long_chg": [200],
        "short_hld": [12000],
        "short_chg": [-100],
        "exchange": ["SHFE"],
    })
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_fut_holding(date(2024, 1, 2))

    assert list(df.columns) == FUT_HOLDING_COLS
    assert len(df) == 1


def test_fetch_fut_wsr_returns_correct_columns(mock_pro):
    mock_pro.fut_wsr.return_value = pd.DataFrame({
        "trade_date": ["20240102"],
        "symbol": ["CU"],
        "fut_name": ["沪铜"],
        "warehouse": ["仓库A"],
        "wh_id": ["WH001"],
        "pre_vol": [100],
        "vol": [120],
        "vol_chg": [20],
        "area": ["上海"],
        "year": ["2024"],
        "grade": [None],
        "brand": [None],
        "place": [None],
        "pd": [0],
        "is_ct": ["N"],
        "unit": ["吨"],
        "exchange": ["SHFE"],
    })
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_fut_wsr(date(2024, 1, 2))

    assert list(df.columns) == FUT_WSR_COLS
    assert len(df) == 1


def test_fetch_fut_settle_returns_correct_columns(mock_pro):
    mock_pro.fut_settle.return_value = pd.DataFrame({
        "ts_code": ["CU2401.SHF"],
        "trade_date": ["20240102"],
        "settle": [50250.0],
        "trading_fee_rate": [None],
        "trading_fee": [None],
        "delivery_fee": [None],
        "b_hedging_margin_rate": [0.08],
        "s_hedging_margin_rate": [0.08],
        "long_margin_rate": [0.10],
        "short_margin_rate": [0.10],
        "offset_today_fee": [None],
        "exchange": ["SHFE"],
    })
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_fut_settle(date(2024, 1, 2))

    assert list(df.columns) == FUT_SETTLE_COLS
    assert len(df) == 1


def test_fetch_fut_mapping_returns_correct_columns(mock_pro):
    mock_pro.fut_mapping.return_value = pd.DataFrame({
        "ts_code": ["CU.SHF"],
        "trade_date": ["20240102"],
        "mapping_ts_code": ["CU2401.SHF"],
    })
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_fut_mapping(date(2024, 1, 2))

    assert list(df.columns) == FUT_MAPPING_COLS
    assert len(df) == 1
    assert df.iloc[0]["trade_date"] == date(2024, 1, 2)


# --- Futures batch 2 tests ---

from zer0share.fetcher import (
    FT_LIMIT_COLS, FUT_WEEKLY_COLS, FUT_MONTHLY_COLS,
    FUT_INDEX_DAILY_COLS, FUT_WEEKLY_DETAIL_COLS,
)


def test_fetch_ft_limit_returns_correct_columns(mock_pro):
    mock_pro.ft_limit.return_value = pd.DataFrame({
        "trade_date": ["20240102"],
        "ts_code": ["CU2401.SHF"],
        "name": ["沪铜2401"],
        "up_limit": [51000.0],
        "down_limit": [49000.0],
        "m_ratio": [0.10],
        "cont": ["CU"],
        "exchange": ["SHFE"],
    })
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_ft_limit(date(2024, 1, 2))

    assert list(df.columns) == FT_LIMIT_COLS
    assert len(df) == 1
    assert df.iloc[0]["trade_date"] == date(2024, 1, 2)


def test_fetch_ft_limit_calls_api_correctly(mock_pro):
    mock_pro.ft_limit.return_value = pd.DataFrame({
        "trade_date": ["20240102"],
        "ts_code": ["CU2401.SHF"], "name": ["沪铜2401"],
        "up_limit": [51000.0], "down_limit": [49000.0],
        "m_ratio": [0.10], "cont": ["CU"], "exchange": ["SHFE"],
    })
    fetcher = TushareFetcher("fake_token")

    fetcher.fetch_ft_limit(date(2024, 1, 2))

    mock_pro.ft_limit.assert_called_once_with(
        trade_date="20240102",
        fields=",".join(FT_LIMIT_COLS),
    )


def test_fetch_ft_limit_returns_empty_when_none(mock_pro):
    mock_pro.ft_limit.return_value = None
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_ft_limit(date(2024, 1, 1))

    assert df.empty
    assert list(df.columns) == FT_LIMIT_COLS


def test_fetch_fut_weekly_returns_correct_columns(mock_pro):
    mock_pro.fut_weekly_monthly.return_value = pd.DataFrame({
        "ts_code": ["CU2401.SHF"],
        "trade_date": ["20240102"],
        "freq": ["week"],
        "open": [50000.0], "high": [50500.0], "low": [49900.0],
        "close": [50300.0], "pre_close": [50000.0],
        "settle": [50250.0], "pre_settle": [50100.0],
        "vol": [10000.0], "amount": [251250.0],
        "oi": [50000.0], "oi_chg": [500.0],
        "exchange": ["SHFE"],
        "change1": [200.0], "change2": [150.0],
    })
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_fut_weekly(date(2024, 1, 2))

    assert list(df.columns) == FUT_WEEKLY_COLS
    assert df.iloc[0]["trade_date"] == date(2024, 1, 2)


def test_fetch_fut_weekly_calls_api_with_freq_week(mock_pro):
    mock_pro.fut_weekly_monthly.return_value = pd.DataFrame({
        "ts_code": ["CU2401.SHF"], "trade_date": ["20240102"],
        "freq": ["week"], "open": [50000.0], "high": [50500.0],
        "low": [49900.0], "close": [50300.0], "pre_close": [50000.0],
        "settle": [50250.0], "pre_settle": [50100.0], "vol": [10000.0],
        "amount": [251250.0], "oi": [50000.0], "oi_chg": [500.0],
        "exchange": ["SHFE"], "change1": [200.0], "change2": [150.0],
    })
    fetcher = TushareFetcher("fake_token")

    fetcher.fetch_fut_weekly(date(2024, 1, 2))

    mock_pro.fut_weekly_monthly.assert_called_once_with(
        trade_date="20240102", freq="week", fields=",".join(FUT_WEEKLY_COLS),
    )


def test_fetch_fut_monthly_calls_api_with_freq_month(mock_pro):
    mock_pro.fut_weekly_monthly.return_value = pd.DataFrame({
        "ts_code": ["CU2401.SHF"], "trade_date": ["20240102"],
        "freq": ["month"], "open": [50000.0], "high": [50500.0],
        "low": [49900.0], "close": [50300.0], "pre_close": [50000.0],
        "settle": [50250.0], "pre_settle": [50100.0], "vol": [10000.0],
        "amount": [251250.0], "oi": [50000.0], "oi_chg": [500.0],
        "exchange": ["SHFE"], "change1": [200.0], "change2": [150.0],
    })
    fetcher = TushareFetcher("fake_token")

    fetcher.fetch_fut_monthly(date(2024, 1, 2))

    mock_pro.fut_weekly_monthly.assert_called_once_with(
        trade_date="20240102", freq="month", fields=",".join(FUT_MONTHLY_COLS),
    )


def test_fetch_fut_index_daily_returns_correct_columns(mock_pro):
    mock_pro.fut_index_daily.return_value = pd.DataFrame({
        "ts_code": ["NHAI.NH"],
        "trade_date": ["20240102"],
        "close": [1000.0], "open": [998.0], "high": [1005.0], "low": [995.0],
        "pre_close": [998.0], "change": [2.0], "pct_chg": [0.2],
        "vol": [50000.0], "amount": [50000000.0],
    })
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_fut_index_daily(date(2024, 1, 2))

    assert list(df.columns) == FUT_INDEX_DAILY_COLS
    assert df.iloc[0]["trade_date"] == date(2024, 1, 2)


def test_fetch_fut_index_daily_calls_api_with_trade_date(mock_pro):
    mock_pro.fut_index_daily.return_value = pd.DataFrame({
        "ts_code": ["NHAI.NH"], "trade_date": ["20240102"],
        "close": [1000.0], "open": [998.0], "high": [1005.0], "low": [995.0],
        "pre_close": [998.0], "change": [2.0], "pct_chg": [0.2],
        "vol": [50000.0], "amount": [50000000.0],
    })
    fetcher = TushareFetcher("fake_token")

    fetcher.fetch_fut_index_daily(date(2024, 1, 2))

    mock_pro.fut_index_daily.assert_called_once_with(
        trade_date="20240102", fields=",".join(FUT_INDEX_DAILY_COLS),
    )


def test_fetch_fut_weekly_detail_returns_correct_columns(mock_pro):
    mock_pro.fut_weekly_detail.return_value = pd.DataFrame({
        "exchange": ["SHFE"], "prd": ["CU"], "name": ["沪铜"],
        "vol": [100000], "vol_yoy": [5.0], "amount": [250.0],
        "amout_yoy": [3.0], "cumvol": [5000000], "cumvol_yoy": [4.0],
        "cumamt": [12500.0], "cumamt_yoy": [2.0],
        "open_interest": [200000], "interest_wow": [1.0],
        "mc_close": [50300.0], "close_wow": [0.5],
        "week": ["202401"], "week_date": ["20240105"],
    })
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_fut_weekly_detail("202401")

    assert list(df.columns) == FUT_WEEKLY_DETAIL_COLS
    assert len(df) == 1
    assert df.iloc[0]["week_date"] == date(2024, 1, 5)


def test_fetch_fut_weekly_detail_calls_api_correctly(mock_pro):
    mock_pro.fut_weekly_detail.return_value = pd.DataFrame({
        "exchange": ["SHFE"], "prd": ["CU"], "name": ["沪铜"],
        "vol": [100000], "vol_yoy": [5.0], "amount": [250.0],
        "amout_yoy": [3.0], "cumvol": [5000000], "cumvol_yoy": [4.0],
        "cumamt": [12500.0], "cumamt_yoy": [2.0],
        "open_interest": [200000], "interest_wow": [1.0],
        "mc_close": [50300.0], "close_wow": [0.5],
        "week": ["202401"], "week_date": ["20240105"],
    })
    fetcher = TushareFetcher("fake_token")

    fetcher.fetch_fut_weekly_detail("202401")

    mock_pro.fut_weekly_detail.assert_called_once_with(
        week="202401", fields=",".join(FUT_WEEKLY_DETAIL_COLS),
    )


def test_fetch_fut_weekly_detail_returns_empty_when_none(mock_pro):
    mock_pro.fut_weekly_detail.return_value = None
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_fut_weekly_detail("202401")

    assert df.empty
    assert list(df.columns) == FUT_WEEKLY_DETAIL_COLS


# --- Options fetcher tests ---

from zer0share.fetcher import (
    OPTIONS_EXCHANGES, OPT_BASIC_COLS, OPT_DAILY_COLS,
)


def _opt_basic_row(exchange: str = "SSE", list_date: str = "20240101") -> dict:
    return {
        "ts_code": "10004462.SH",
        "symbol": "10004462",
        "exchange": exchange,
        "name": "50ETF购4月2700",
        "per_unit": 10000.0,
        "opt_code": "OP510050",
        "opt_type": "E",
        "call_put": "C",
        "exercise_type": "E",
        "exercise_price": 2.7,
        "s_month": "202404",
        "maturity_date": "20240424",
        "list_date": list_date,
        "delist_date": "20240424",
    }


def _opt_daily_row(ts_code: str = "10004462.SH", trade_date: str = "20240102") -> dict:
    return {
        "ts_code": ts_code,
        "trade_date": trade_date,
        "exchange": "SSE",
        "pre_settle": 0.15,
        "pre_close": 0.148,
        "open": 0.152,
        "high": 0.16,
        "low": 0.148,
        "close": 0.155,
        "settle": 0.154,
        "vol": 5000.0,
        "amount": 7700000.0,
        "oi": 20000.0,
    }


def test_options_exchanges_has_six_entries():
    assert len(OPTIONS_EXCHANGES) == 6
    assert "SSE" in OPTIONS_EXCHANGES
    assert "SZSE" in OPTIONS_EXCHANGES
    assert "CFFEX" in OPTIONS_EXCHANGES
    assert "DCE" in OPTIONS_EXCHANGES
    assert "SHFE" in OPTIONS_EXCHANGES
    assert "CZCE" in OPTIONS_EXCHANGES


def test_fetch_opt_basic_returns_correct_columns(mock_pro):
    mock_pro.opt_basic.return_value = pd.DataFrame([_opt_basic_row()])
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_opt_basic("SSE")

    assert list(df.columns) == OPT_BASIC_COLS
    assert len(df) == 1


def test_fetch_opt_basic_converts_dates(mock_pro):
    mock_pro.opt_basic.return_value = pd.DataFrame([_opt_basic_row()])
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_opt_basic("SSE")

    assert df.iloc[0]["list_date"] == date(2024, 1, 1)
    assert df.iloc[0]["delist_date"] == date(2024, 4, 24)
    assert df.iloc[0]["maturity_date"] == date(2024, 4, 24)


def test_fetch_opt_basic_returns_empty_when_none(mock_pro):
    mock_pro.opt_basic.return_value = None
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_opt_basic("SSE")

    assert df.empty
    assert list(df.columns) == OPT_BASIC_COLS


def test_fetch_opt_basic_calls_api_correctly(mock_pro):
    mock_pro.opt_basic.return_value = pd.DataFrame([_opt_basic_row()])
    fetcher = TushareFetcher("fake_token")

    fetcher.fetch_opt_basic("CFFEX")

    mock_pro.opt_basic.assert_called_once_with(
        exchange="CFFEX",
        fields=",".join(OPT_BASIC_COLS),
    )


def test_fetch_opt_daily_returns_correct_columns(mock_pro):
    mock_pro.opt_daily.return_value = pd.DataFrame([_opt_daily_row()])
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_opt_daily(date(2024, 1, 2))

    assert list(df.columns) == OPT_DAILY_COLS
    assert len(df) == 1
    assert df.iloc[0]["trade_date"] == date(2024, 1, 2)


def test_fetch_opt_daily_returns_empty_when_none(mock_pro):
    mock_pro.opt_daily.return_value = None
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_opt_daily(date(2024, 1, 1))

    assert df.empty
    assert list(df.columns) == OPT_DAILY_COLS


def test_fetch_opt_daily_calls_api_with_date(mock_pro):
    mock_pro.opt_daily.return_value = pd.DataFrame([_opt_daily_row()])
    fetcher = TushareFetcher("fake_token")

    fetcher.fetch_opt_daily(date(2024, 1, 2))

    mock_pro.opt_daily.assert_called_once_with(
        trade_date="20240102",
        fields=",".join(OPT_DAILY_COLS),
    )
