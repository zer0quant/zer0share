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
