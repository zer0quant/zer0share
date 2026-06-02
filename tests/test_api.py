from datetime import date

import pandas as pd
import pytest

from zer0share.api import LocalPro
from zer0share.storage import (
    write_adj_factor,
    write_basic,
    write_ci_member,
    write_daily_kline,
    write_daily_partition,
    write_sw_classify,
    write_sw_member,
    write_trade_cal,
    write_universe,
)


def test_stock_basic_filters_and_formats_dates(tmp_path):
    df = pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "600000.SH"],
            "symbol": ["000001", "600000"],
            "name": ["Ping An Bank", "SPDB"],
            "area": ["Shenzhen", "Shanghai"],
            "industry": ["Bank", "Bank"],
            "fullname": ["Ping An Bank Co., Ltd.", "Shanghai Pudong Development Bank"],
            "enname": ["Ping An Bank", "SPDB"],
            "cnspell": ["payh", "pfyh"],
            "market": ["Main Board", "Main Board"],
            "exchange": ["SZSE", "SSE"],
            "curr_type": ["CNY", "CNY"],
            "list_status": ["L", "L"],
            "list_date": [date(1991, 4, 3), date(1999, 11, 10)],
            "delist_date": [None, None],
            "is_hs": ["S", "H"],
            "act_name": ["Shenzhen Investment Holdings", "Shanghai SASAC"],
            "act_ent_type": ["Local SOE", "Local SOE"],
        }
    )
    write_basic(tmp_path, df)

    pro = LocalPro(tmp_path)
    result = pro.stock_basic(
        ts_code="000001.SZ",
        fields="ts_code,name,list_date,delist_date",
    )

    assert result.to_dict("records") == [
        {
            "ts_code": "000001.SZ",
            "name": "Ping An Bank",
            "list_date": "19910403",
            "delist_date": None,
        }
    ]


def test_trade_cal_filters_open_days_and_formats_dates(tmp_path):
    df = pd.DataFrame(
        {
            "exchange": ["SSE", "SSE", "SSE"],
            "cal_date": [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)],
            "is_open": [False, True, True],
            "pretrade_date": [date(2023, 12, 29), date(2023, 12, 29), date(2024, 1, 2)],
        }
    )
    write_trade_cal(tmp_path, "SSE", df)

    pro = LocalPro(tmp_path)
    result = pro.trade_cal(
        exchange="SSE",
        start_date="2024-01-02",
        end_date="20240103",
        is_open="1",
        fields=["exchange", "cal_date", "is_open", "pretrade_date"],
    )

    assert result.to_dict("records") == [
        {
            "exchange": "SSE",
            "cal_date": "20240102",
            "is_open": True,
            "pretrade_date": "20231229",
        },
        {
            "exchange": "SSE",
            "cal_date": "20240103",
            "is_open": True,
            "pretrade_date": "20240102",
        },
    ]


def test_daily_filters_multiple_codes_by_date_range_and_formats_dates(tmp_path):
    write_daily_kline(
        tmp_path,
        date(2024, 1, 2),
        pd.DataFrame(
            {
                "ts_code": ["000001.SZ", "600000.SH"],
                "trade_date": [date(2024, 1, 2), date(2024, 1, 2)],
                "open": [10.0, 20.0],
                "high": [11.0, 21.0],
                "low": [9.0, 19.0],
                "close": [10.5, 20.5],
                "pre_close": [10.0, 20.0],
                "change": [0.5, 0.5],
                "pct_chg": [5.0, 2.5],
                "vol": [1000.0, 2000.0],
                "amount": [10000.0, 20000.0],
            }
        ),
    )
    write_daily_kline(
        tmp_path,
        date(2024, 1, 3),
        pd.DataFrame(
            {
                "ts_code": ["000001.SZ", "600000.SH"],
                "trade_date": [date(2024, 1, 3), date(2024, 1, 3)],
                "open": [11.0, 21.0],
                "high": [12.0, 22.0],
                "low": [10.0, 20.0],
                "close": [11.5, 21.5],
                "pre_close": [10.5, 20.5],
                "change": [1.0, 1.0],
                "pct_chg": [9.5, 4.9],
                "vol": [1100.0, 2100.0],
                "amount": [11000.0, 21000.0],
            }
        ),
    )

    pro = LocalPro(tmp_path)
    result = pro.daily(
        ts_code="600000.SH,000001.SZ",
        start_date="20240103",
        end_date="20240103",
        fields=["ts_code", "trade_date", "close"],
    )

    assert result.to_dict("records") == [
        {"ts_code": "000001.SZ", "trade_date": "20240103", "close": 11.5},
        {"ts_code": "600000.SH", "trade_date": "20240103", "close": 21.5},
    ]


def test_daily_partitioned_query_handles_empty_partitions(tmp_path):
    write_daily_partition(
        tmp_path,
        "stock_st",
        date(2024, 1, 2),
        pd.DataFrame(columns=["ts_code", "name", "trade_date", "type", "type_name"]),
    )
    write_daily_partition(
        tmp_path,
        "stock_st",
        date(2024, 1, 3),
        pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "name": ["ST Test"],
                "trade_date": [date(2024, 1, 3)],
                "type": ["ST"],
                "type_name": ["Risk"],
            }
        ),
    )

    pro = LocalPro(tmp_path)
    result = pro.stock_st(
        start_date="20240102",
        end_date="20240103",
        fields="ts_code,name,trade_date,type,type_name",
    )

    assert result.to_dict("records") == [
        {
            "ts_code": "000001.SZ",
            "name": "ST Test",
            "trade_date": "20240103",
            "type": "ST",
            "type_name": "Risk",
        }
    ]


def test_universe_filters_by_name_date_and_code(tmp_path):
    write_universe(
        tmp_path,
        "univ_trade_base",
        date(2024, 1, 2),
        pd.DataFrame(
            {
                "trade_date": [date(2024, 1, 2), date(2024, 1, 2)],
                "universe": ["univ_trade_base", "univ_trade_base"],
                "ts_code": ["000001.SZ", "600000.SH"],
            }
        ),
    )
    write_universe(
        tmp_path,
        "univ_research_base",
        date(2024, 1, 2),
        pd.DataFrame(
            {
                "trade_date": [date(2024, 1, 2)],
                "universe": ["univ_research_base"],
                "ts_code": ["000001.SZ"],
            }
        ),
    )

    pro = LocalPro(tmp_path)
    result = pro.universe(
        universe="univ_trade_base",
        ts_code="000001.SZ",
        trade_date="20240102",
    )

    assert result.to_dict("records") == [
        {
            "trade_date": "20240102",
            "universe": "univ_trade_base",
            "ts_code": "000001.SZ",
        }
    ]


def test_adj_factor_filters_trade_date_and_formats_dates(tmp_path):
    write_adj_factor(
        tmp_path,
        date(2024, 1, 2),
        pd.DataFrame(
            {
                "ts_code": ["000001.SZ", "600000.SH"],
                "trade_date": [date(2024, 1, 2), date(2024, 1, 2)],
                "adj_factor": [100.1, 200.2],
            }
        ),
    )

    pro = LocalPro(tmp_path)
    result = pro.adj_factor(trade_date="20240102", fields="ts_code,trade_date,adj_factor")

    assert result.to_dict("records") == [
        {"ts_code": "000001.SZ", "trade_date": "20240102", "adj_factor": 100.1},
        {"ts_code": "600000.SH", "trade_date": "20240102", "adj_factor": 200.2},
    ]


def test_daily_rejects_ambiguous_trade_date_and_range(tmp_path):
    pro = LocalPro(tmp_path)

    with pytest.raises(ValueError, match="trade_date"):
        pro.daily(trade_date="20240102", start_date="20240101")


def test_query_dispatches_to_named_api(tmp_path):
    write_adj_factor(
        tmp_path,
        date(2024, 1, 2),
        pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "trade_date": [date(2024, 1, 2)],
                "adj_factor": [100.1],
            }
        ),
    )

    pro = LocalPro(tmp_path)
    result = pro.query("adj_factor", ts_code="000001.SZ")

    assert result.to_dict("records") == [
        {"ts_code": "000001.SZ", "trade_date": "20240102", "adj_factor": 100.1}
    ]


def test_unknown_query_api_raises_value_error(tmp_path):
    pro = LocalPro(tmp_path)

    with pytest.raises(ValueError, match="unknown api"):
        pro.query("moneyflow")


def test_unknown_field_raises_value_error(tmp_path):
    write_basic(
        tmp_path,
        pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "symbol": ["000001"],
                "name": ["Ping An Bank"],
                "area": ["Shenzhen"],
                "industry": ["Bank"],
                "fullname": ["Ping An Bank Co., Ltd."],
                "enname": ["Ping An Bank"],
                "cnspell": ["payh"],
                "market": ["Main Board"],
                "exchange": ["SZSE"],
                "curr_type": ["CNY"],
                "list_status": ["L"],
                "list_date": [date(1991, 4, 3)],
                "delist_date": [None],
                "is_hs": ["S"],
                "act_name": ["Shenzhen Investment Holdings"],
                "act_ent_type": ["Local SOE"],
            }
        ),
    )
    pro = LocalPro(tmp_path)

    with pytest.raises(ValueError, match="unknown fields"):
        pro.stock_basic(fields="ts_code,not_a_field")


def test_missing_data_raises_file_not_found_with_sync_hint(tmp_path):
    pro = LocalPro(tmp_path)

    with pytest.raises(FileNotFoundError, match="sync --table basic"):
        pro.stock_basic()


def test_invalid_date_format_raises_value_error(tmp_path):
    write_trade_cal(
        tmp_path,
        "SSE",
        pd.DataFrame(
            {
                "exchange": ["SSE"],
                "cal_date": [date(2024, 1, 2)],
                "is_open": [True],
                "pretrade_date": [date(2023, 12, 29)],
            }
        ),
    )
    pro = LocalPro(tmp_path)

    with pytest.raises(ValueError, match="invalid date"):
        pro.trade_cal(start_date="2024/01/02")


def test_invalid_date_range_raises_value_error(tmp_path):
    write_daily_kline(
        tmp_path,
        date(2024, 1, 2),
        pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "trade_date": [date(2024, 1, 2)],
                "open": [10.0],
                "high": [11.0],
                "low": [9.0],
                "close": [10.5],
                "pre_close": [10.0],
                "change": [0.5],
                "pct_chg": [5.0],
                "vol": [1000.0],
                "amount": [10000.0],
            }
        ),
    )
    pro = LocalPro(tmp_path)

    with pytest.raises(ValueError, match="start_date"):
        pro.daily(start_date="20240103", end_date="20240102")


def test_trade_cal_invalid_date_range_raises_value_error(tmp_path):
    write_trade_cal(
        tmp_path,
        "SSE",
        pd.DataFrame(
            {
                "exchange": ["SSE"],
                "cal_date": [date(2024, 1, 2)],
                "is_open": [True],
                "pretrade_date": [date(2023, 12, 29)],
            }
        ),
    )
    pro = LocalPro(tmp_path)

    with pytest.raises(ValueError, match="start_date"):
        pro.trade_cal(start_date="20240103", end_date="20240102")


def test_pro_bar_returns_qfq_prices_using_end_date_factor(tmp_path):
    write_daily_kline(
        tmp_path,
        date(2024, 1, 2),
        pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "trade_date": [date(2024, 1, 2)],
                "open": [10.0],
                "high": [12.0],
                "low": [9.0],
                "close": [11.0],
                "pre_close": [10.0],
                "change": [1.0],
                "pct_chg": [10.0],
                "vol": [1000.0],
                "amount": [11000.0],
            }
        ),
    )
    write_daily_kline(
        tmp_path,
        date(2024, 1, 3),
        pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "trade_date": [date(2024, 1, 3)],
                "open": [20.0],
                "high": [22.0],
                "low": [19.0],
                "close": [21.0],
                "pre_close": [11.0],
                "change": [10.0],
                "pct_chg": [90.91],
                "vol": [2000.0],
                "amount": [42000.0],
            }
        ),
    )
    write_adj_factor(
        tmp_path,
        date(2024, 1, 2),
        pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "trade_date": [date(2024, 1, 2)],
                "adj_factor": [2.0],
            }
        ),
    )
    write_adj_factor(
        tmp_path,
        date(2024, 1, 3),
        pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "trade_date": [date(2024, 1, 3)],
                "adj_factor": [4.0],
            }
        ),
    )

    pro = LocalPro(tmp_path)
    result = pro.pro_bar(
        ts_code="000001.SZ",
        start_date="20240102",
        end_date="20240103",
        adj="qfq",
    )

    assert result[
        ["ts_code", "trade_date", "open", "high", "low", "close", "pre_close", "change", "pct_chg", "vol"]
    ].to_dict(
        "records"
    ) == [
        {
            "ts_code": "000001.SZ",
            "trade_date": "20240102",
            "open": 5.0,
            "high": 6.0,
            "low": 4.5,
            "close": 5.5,
            "pre_close": 5.0,
            "change": 0.5,
            "pct_chg": 10.0,
            "vol": 1000.0,
        },
        {
            "ts_code": "000001.SZ",
            "trade_date": "20240103",
            "open": 20.0,
            "high": 22.0,
            "low": 19.0,
            "close": 21.0,
            "pre_close": 11.0,
            "change": 10.0,
            "pct_chg": 90.91,
            "vol": 2000.0,
        },
    ]


def test_pro_bar_returns_hfq_prices(tmp_path):
    write_daily_kline(
        tmp_path,
        date(2024, 1, 2),
        pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "trade_date": [date(2024, 1, 2)],
                "open": [10.0],
                "high": [12.0],
                "low": [9.0],
                "close": [11.0],
                "pre_close": [10.0],
                "change": [1.0],
                "pct_chg": [10.0],
                "vol": [1000.0],
                "amount": [11000.0],
            }
        ),
    )
    write_adj_factor(
        tmp_path,
        date(2024, 1, 2),
        pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "trade_date": [date(2024, 1, 2)],
                "adj_factor": [2.0],
            }
        ),
    )

    pro = LocalPro(tmp_path)
    result = pro.pro_bar(ts_code="000001.SZ", trade_date="20240102", adj="hfq")

    assert result[["open", "high", "low", "close", "pre_close"]].to_dict("records") == [
        {"open": 20.0, "high": 24.0, "low": 18.0, "close": 22.0, "pre_close": 20.0}
    ]


def test_pro_bar_rounds_adjusted_prices_to_two_decimals(tmp_path):
    write_daily_kline(
        tmp_path,
        date(2024, 1, 2),
        pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "trade_date": [date(2024, 1, 2)],
                "open": [10.0],
                "high": [10.0],
                "low": [10.0],
                "close": [10.0],
                "pre_close": [10.0],
                "change": [0.0],
                "pct_chg": [0.0],
                "vol": [1000.0],
                "amount": [10000.0],
            }
        ),
    )
    write_adj_factor(
        tmp_path,
        date(2024, 1, 2),
        pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "trade_date": [date(2024, 1, 2)],
                "adj_factor": [1.234],
            }
        ),
    )

    pro = LocalPro(tmp_path)
    result = pro.pro_bar(ts_code="000001.SZ", trade_date="20240102", adj="hfq")

    assert result.iloc[0]["close"] == 12.34


def test_pro_bar_supports_multiple_codes_with_qfq_base_per_stock(tmp_path):
    for day, rows in [
        (
            date(2024, 1, 1),
            [
                ["000001.SZ", 10.0, 10.0],
                ["000002.SZ", 20.0, 20.0],
            ],
        ),
        (
            date(2024, 1, 2),
            [
                ["000001.SZ", 12.0, 12.0],
                ["000002.SZ", 22.0, 22.0],
            ],
        ),
    ]:
        write_daily_kline(
            tmp_path,
            day,
            pd.DataFrame(
                {
                    "ts_code": [row[0] for row in rows],
                    "trade_date": [day, day],
                    "open": [row[1] for row in rows],
                    "high": [row[1] for row in rows],
                    "low": [row[1] for row in rows],
                    "close": [row[2] for row in rows],
                    "pre_close": [row[2] for row in rows],
                    "change": [0.0, 0.0],
                    "pct_chg": [0.0, 0.0],
                    "vol": [1000.0, 2000.0],
                    "amount": [10000.0, 20000.0],
                }
            ),
        )
    for day, factors in [
        (date(2024, 1, 1), [1.0, 10.0]),
        (date(2024, 1, 2), [2.0, 20.0]),
    ]:
        write_adj_factor(
            tmp_path,
            day,
            pd.DataFrame(
                {
                    "ts_code": ["000001.SZ", "000002.SZ"],
                    "trade_date": [day, day],
                    "adj_factor": factors,
                }
            ),
        )

    pro = LocalPro(tmp_path)
    result = pro.pro_bar(
        ts_code="000001.SZ,000002.SZ",
        start_date="20240101",
        end_date="20240102",
        adj="qfq",
    )

    assert result[["ts_code", "trade_date", "close"]].to_dict("records") == [
        {"ts_code": "000001.SZ", "trade_date": "20240101", "close": 5.0},
        {"ts_code": "000001.SZ", "trade_date": "20240102", "close": 12.0},
        {"ts_code": "000002.SZ", "trade_date": "20240101", "close": 10.0},
        {"ts_code": "000002.SZ", "trade_date": "20240102", "close": 22.0},
    ]


def test_pro_bar_rejects_unsupported_asset_and_freq(tmp_path):
    pro = LocalPro(tmp_path)

    with pytest.raises(NotImplementedError, match="asset='E'"):
        pro.pro_bar(ts_code="000001.SZ", asset="I")

    with pytest.raises(NotImplementedError, match="freq='D'"):
        pro.pro_bar(ts_code="000001.SZ", freq="W")


def test_index_classify_filters_by_level(tmp_path):
    write_sw_classify(tmp_path, pd.DataFrame({
        "index_code": ["801010.SI", "801016.SI"],
        "industry_name": ["农林牧渔", "种植业"],
        "level": ["L1", "L2"],
        "parent_code": ["0", "110000"],
        "industry_code": ["110000", "110100"],
        "is_pub": ["1", "1"],
        "src": ["SW2021", "SW2021"],
    }))
    pro = LocalPro(tmp_path)
    result = pro.index_classify(level="L1")
    assert len(result) == 1
    assert result.iloc[0]["industry_name"] == "农林牧渔"


def test_index_classify_filters_by_src(tmp_path):
    write_sw_classify(tmp_path, pd.DataFrame({
        "index_code": ["801010.SI", "801010.SI"],
        "industry_name": ["农林牧渔", "农林牧渔"],
        "level": ["L1", "L1"],
        "parent_code": ["0", "0"],
        "industry_code": ["110000", "110000"],
        "is_pub": ["1", "1"],
        "src": ["SW2021", "SW2014"],
    }))
    pro = LocalPro(tmp_path)
    result = pro.index_classify(src="SW2014")
    assert len(result) == 1


def test_index_classify_no_filter_returns_all(tmp_path):
    write_sw_classify(tmp_path, pd.DataFrame({
        "index_code": ["801010.SI", "801016.SI"],
        "industry_name": ["农林牧渔", "种植业"],
        "level": ["L1", "L2"],
        "parent_code": ["0", "110000"],
        "industry_code": ["110000", "110100"],
        "is_pub": ["1", "1"],
        "src": ["SW2021", "SW2021"],
    }))
    pro = LocalPro(tmp_path)
    result = pro.index_classify()
    assert len(result) == 2


def test_index_member_all_filters_by_ts_code(tmp_path):
    write_sw_member(tmp_path, pd.DataFrame({
        "l1_code": ["801010.SI", "801030.SI"],
        "l1_name": ["农林牧渔", "化工"],
        "l2_code": ["801016.SI", "801033.SI"],
        "l2_name": ["种植业", "化学原料"],
        "l3_code": ["850111.SI", "850321.SI"],
        "l3_name": ["种子", "纯碱"],
        "ts_code": ["002041.SZ", "600291.SH"],
        "name": ["登海种业", "西水股份"],
        "in_date": [date(2021, 12, 13), date(2021, 12, 13)],
        "out_date": [None, None],
        "is_new": ["Y", "Y"],
    }))
    pro = LocalPro(tmp_path)
    result = pro.index_member_all(ts_code="002041.SZ")
    assert len(result) == 1
    assert result.iloc[0]["ts_code"] == "002041.SZ"


def test_index_member_all_filters_by_is_new(tmp_path):
    write_sw_member(tmp_path, pd.DataFrame({
        "l1_code": ["801010.SI", "801010.SI"],
        "l1_name": ["农林牧渔", "农林牧渔"],
        "l2_code": ["801016.SI", "801016.SI"],
        "l2_name": ["种植业", "种植业"],
        "l3_code": ["850111.SI", "850111.SI"],
        "l3_name": ["种子", "种子"],
        "ts_code": ["002041.SZ", "600313.SH"],
        "name": ["登海种业", "农发种业"],
        "in_date": [date(2021, 12, 13), date(2021, 12, 13)],
        "out_date": [date(2022, 6, 30), None],
        "is_new": ["N", "Y"],
    }))
    pro = LocalPro(tmp_path)
    result = pro.index_member_all(is_new="Y")
    assert len(result) == 1
    assert result.iloc[0]["ts_code"] == "600313.SH"


def test_index_member_all_filters_by_l1_code(tmp_path):
    write_sw_member(tmp_path, pd.DataFrame({
        "l1_code": ["801010.SI", "801030.SI"],
        "l1_name": ["农林牧渔", "化工"],
        "l2_code": ["801016.SI", "801033.SI"],
        "l2_name": ["种植业", "化学原料"],
        "l3_code": ["850111.SI", "850321.SI"],
        "l3_name": ["种子", "纯碱"],
        "ts_code": ["002041.SZ", "600291.SH"],
        "name": ["登海种业", "西水股份"],
        "in_date": [date(2021, 12, 13), date(2021, 12, 13)],
        "out_date": [None, None],
        "is_new": ["Y", "Y"],
    }))
    pro = LocalPro(tmp_path)
    result = pro.index_member_all(l1_code="801010.SI")
    assert len(result) == 1


def test_index_member_all_supports_multi_ts_code(tmp_path):
    write_sw_member(tmp_path, pd.DataFrame({
        "l1_code": ["801010.SI", "801030.SI", "801040.SI"],
        "l1_name": ["农林牧渔", "化工", "钢铁"],
        "l2_code": ["801016.SI", "801033.SI", "801043.SI"],
        "l2_name": ["种植业", "化学原料", "冶钢原料"],
        "l3_code": ["850111.SI", "850321.SI", "850431.SI"],
        "l3_name": ["种子", "纯碱", "铁矿石"],
        "ts_code": ["002041.SZ", "600291.SH", "000002.SZ"],
        "name": ["登海种业", "西水股份", "万科A"],
        "in_date": [date(2021, 12, 13), date(2021, 12, 13), date(2021, 12, 13)],
        "out_date": [None, None, None],
        "is_new": ["Y", "Y", "Y"],
    }))
    pro = LocalPro(tmp_path)
    result = pro.index_member_all(ts_code="002041.SZ,000002.SZ")
    assert len(result) == 2


def test_index_member_all_formats_dates(tmp_path):
    write_sw_member(tmp_path, pd.DataFrame({
        "l1_code": ["801010.SI"],
        "l1_name": ["农林牧渔"],
        "l2_code": ["801016.SI"],
        "l2_name": ["种植业"],
        "l3_code": ["850111.SI"],
        "l3_name": ["种子"],
        "ts_code": ["002041.SZ"],
        "name": ["登海种业"],
        "in_date": [date(2021, 12, 13)],
        "out_date": [date(2022, 6, 30)],
        "is_new": ["N"],
    }))
    pro = LocalPro(tmp_path)
    result = pro.index_member_all(ts_code="002041.SZ")
    assert result.iloc[0]["in_date"] == "20211213"
    assert result.iloc[0]["out_date"] == "20220630"


def test_ci_index_member_filters_by_ts_code(tmp_path):
    write_ci_member(tmp_path, pd.DataFrame({
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
    }))
    pro = LocalPro(tmp_path)
    result = pro.ci_index_member(ts_code="000876.SZ")
    assert len(result) == 1
    assert result.iloc[0]["ts_code"] == "000876.SZ"


def test_ci_index_member_filters_by_is_new(tmp_path):
    write_ci_member(tmp_path, pd.DataFrame({
        "l1_code": ["CI005001.CI", "CI005001.CI"],
        "l1_name": ["农林牧渔", "农林牧渔"],
        "l2_code": ["CI005005.CI", "CI005005.CI"],
        "l2_name": ["农产品加工", "农产品加工"],
        "l3_code": ["CI005006.CI", "CI005006.CI"],
        "l3_name": ["粮油加工", "粮油加工"],
        "ts_code": ["000876.SZ", "000877.SZ"],
        "name": ["新 希 望", "天山股份"],
        "in_date": [date(2020, 1, 1), date(2020, 1, 1)],
        "out_date": [date(2023, 12, 31), None],
        "is_new": ["N", "Y"],
    }))
    pro = LocalPro(tmp_path)
    result = pro.ci_index_member(is_new="Y")
    assert len(result) == 1


def test_ci_index_member_formats_dates(tmp_path):
    write_ci_member(tmp_path, pd.DataFrame({
        "l1_code": ["CI005001.CI"],
        "l1_name": ["农林牧渔"],
        "l2_code": ["CI005005.CI"],
        "l2_name": ["农产品加工"],
        "l3_code": ["CI005006.CI"],
        "l3_name": ["粮油加工"],
        "ts_code": ["000876.SZ"],
        "name": ["新 希 望"],
        "in_date": [date(2020, 1, 1)],
        "out_date": [date(2023, 12, 31)],
        "is_new": ["N"],
    }))
    pro = LocalPro(tmp_path)
    result = pro.ci_index_member(ts_code="000876.SZ")
    assert result.iloc[0]["in_date"] == "20200101"
    assert result.iloc[0]["out_date"] == "20231231"


def test_query_dispatches_index_classify(tmp_path):
    write_sw_classify(tmp_path, pd.DataFrame({
        "index_code": ["801010.SI"],
        "industry_name": ["农林牧渔"],
        "level": ["L1"],
        "parent_code": ["0"],
        "industry_code": ["110000"],
        "is_pub": ["1"],
        "src": ["SW2021"],
    }))
    pro = LocalPro(tmp_path)
    result = pro.query("index_classify")
    assert len(result) == 1


def test_query_dispatches_index_member_all(tmp_path):
    write_sw_member(tmp_path, pd.DataFrame({
        "l1_code": ["801010.SI"], "l1_name": ["农林牧渔"],
        "l2_code": ["801016.SI"], "l2_name": ["种植业"],
        "l3_code": ["850111.SI"], "l3_name": ["种子"],
        "ts_code": ["002041.SZ"], "name": ["登海种业"],
        "in_date": [date(2021, 12, 13)], "out_date": [None], "is_new": ["Y"],
    }))
    pro = LocalPro(tmp_path)
    result = pro.query("index_member_all", ts_code="002041.SZ")
    assert len(result) == 1


def test_query_dispatches_ci_index_member(tmp_path):
    write_ci_member(tmp_path, pd.DataFrame({
        "l1_code": ["CI005001.CI"], "l1_name": ["农林牧渔"],
        "l2_code": ["CI005005.CI"], "l2_name": ["农产品加工"],
        "l3_code": ["CI005006.CI"], "l3_name": ["粮油加工"],
        "ts_code": ["000876.SZ"], "name": ["新 希 望"],
        "in_date": [date(2020, 1, 1)], "out_date": [None], "is_new": ["Y"],
    }))
    pro = LocalPro(tmp_path)
    result = pro.query("ci_index_member", ts_code="000876.SZ")
    assert len(result) == 1


def test_index_classify_raises_file_not_found_with_sync_hint(tmp_path):
    pro = LocalPro(tmp_path)
    with pytest.raises(FileNotFoundError, match="sync --table industry"):
        pro.index_classify()


def test_index_member_all_raises_file_not_found_with_sync_hint(tmp_path):
    pro = LocalPro(tmp_path)
    with pytest.raises(FileNotFoundError, match="sync --table industry"):
        pro.index_member_all()


def test_ci_index_member_raises_file_not_found_with_sync_hint(tmp_path):
    pro = LocalPro(tmp_path)
    with pytest.raises(FileNotFoundError, match="sync --table ci_member"):
        pro.ci_index_member()


def _index_daily_partition(trade_date: date, ts_codes: list[str] | None = None) -> pd.DataFrame:
    codes = ts_codes or ["000300.SH", "000905.SH"]
    return pd.DataFrame({
        "ts_code": codes,
        "trade_date": [trade_date] * len(codes),
        "open": [3500.0] * len(codes),
        "high": [3550.0] * len(codes),
        "low": [3480.0] * len(codes),
        "close": [3520.0] * len(codes),
        "pre_close": [3490.0] * len(codes),
        "change": [30.0] * len(codes),
        "pct_chg": [0.86] * len(codes),
        "vol": [50000000.0] * len(codes),
        "amount": [1750000000.0] * len(codes),
    })


def test_index_daily_returns_all_on_no_filter(tmp_path):
    write_daily_partition(tmp_path, "index_daily", date(2024, 1, 2), _index_daily_partition(date(2024, 1, 2)))
    write_daily_partition(tmp_path, "index_daily", date(2024, 1, 3), _index_daily_partition(date(2024, 1, 3)))

    pro = LocalPro(tmp_path)
    result = pro.index_daily()

    assert len(result) == 4  # 2 dates × 2 codes


def test_index_daily_filters_by_ts_code(tmp_path):
    write_daily_partition(tmp_path, "index_daily", date(2024, 1, 2), _index_daily_partition(date(2024, 1, 2)))

    pro = LocalPro(tmp_path)
    result = pro.index_daily(ts_code="000300.SH")

    assert len(result) == 1
    assert result.iloc[0]["ts_code"] == "000300.SH"


def test_index_daily_filters_by_trade_date(tmp_path):
    write_daily_partition(tmp_path, "index_daily", date(2024, 1, 2), _index_daily_partition(date(2024, 1, 2)))
    write_daily_partition(tmp_path, "index_daily", date(2024, 1, 3), _index_daily_partition(date(2024, 1, 3)))

    pro = LocalPro(tmp_path)
    result = pro.index_daily(trade_date="20240102")

    assert len(result) == 2
    assert all(result["trade_date"] == "20240102")


def test_index_daily_filters_by_date_range(tmp_path):
    write_daily_partition(tmp_path, "index_daily", date(2024, 1, 2), _index_daily_partition(date(2024, 1, 2)))
    write_daily_partition(tmp_path, "index_daily", date(2024, 1, 3), _index_daily_partition(date(2024, 1, 3)))
    write_daily_partition(tmp_path, "index_daily", date(2024, 1, 4), _index_daily_partition(date(2024, 1, 4)))

    pro = LocalPro(tmp_path)
    result = pro.index_daily(start_date="20240102", end_date="20240103")

    assert len(result) == 4  # 2 dates × 2 codes
    dates = sorted(result["trade_date"].unique())
    assert dates == ["20240102", "20240103"]


def test_index_daily_raises_when_data_missing(tmp_path):
    pro = LocalPro(tmp_path)

    with pytest.raises(FileNotFoundError, match="index_daily"):
        pro.index_daily(ts_code="000300.SH")


def test_index_daily_fields_filter(tmp_path):
    write_daily_partition(tmp_path, "index_daily", date(2024, 1, 2), _index_daily_partition(date(2024, 1, 2)))

    pro = LocalPro(tmp_path)
    result = pro.index_daily(ts_code="000300.SH", fields="ts_code,trade_date,close")

    assert list(result.columns) == ["ts_code", "trade_date", "close"]


def test_index_daily_in_query_dispatch(tmp_path):
    write_daily_partition(tmp_path, "index_daily", date(2024, 1, 2), _index_daily_partition(date(2024, 1, 2)))

    pro = LocalPro(tmp_path)
    result = pro.query("index_daily", ts_code="000300.SH")

    assert len(result) == 1


# --- Futures API tests ---

from zer0share.storage import write_daily_partition
from zer0share.fetcher import (
    FUT_BASIC_COLS, FUT_DAILY_COLS, FUT_HOLDING_COLS,
    FUT_WSR_COLS, FUT_SETTLE_COLS, FUT_MAPPING_COLS,
)


def _write_fut_daily_data(data_dir, trade_date, ts_codes=None):
    ts_codes = ts_codes or ["CU2401.SHF", "AG2401.SHF"]
    rows = []
    for ts_code in ts_codes:
        rows.append({
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
        })
    df = pd.DataFrame(rows)
    futures_dir = data_dir / "futures"
    write_daily_partition(futures_dir, "fut_daily", trade_date, df)
    return df


@pytest.fixture
def futures_api(tmp_path):
    return LocalPro(tmp_path)


def test_fut_basic_query_returns_data(tmp_path, futures_api):
    from datetime import date as dt
    df = pd.DataFrame({
        "ts_code": ["CU2401.SHF", "AG2401.SHF"],
        "symbol": ["CU2401", "AG2401"],
        "exchange": ["SHFE", "SHFE"],
        "name": ["沪铜2401", "沪银2401"],
        "fut_code": ["CU", "AG"],
        "multiplier": [None, None],
        "trade_unit": ["5吨/手", "15千克/手"],
        "per_unit": [5.0, 15.0],
        "quote_unit": ["元/吨", "元/千克"],
        "quote_unit_desc": ["10元/吨", "1元/千克"],
        "d_mode_desc": ["实物交割", "实物交割"],
        "list_date": ["20240101", "20240101"],
        "delist_date": ["20240115", "20240115"],
        "d_month": ["202401", "202401"],
        "last_ddate": ["20240115", "20240115"],
        "trade_time_desc": [None, None],
    })
    write_daily_partition(tmp_path / "futures", "fut_basic", dt(2024, 1, 2), df)

    result = futures_api.fut_basic()
    assert len(result) == 2
    assert set(result["ts_code"]) == {"CU2401.SHF", "AG2401.SHF"}


def test_fut_basic_query_filters_by_exchange(tmp_path, futures_api):
    from datetime import date as dt
    df = pd.DataFrame({
        "ts_code": ["CU2401.SHF", "A2505.DCE"],
        "symbol": ["CU2401", "A2505"],
        "exchange": ["SHFE", "DCE"],
        "name": ["沪铜2401", "豆一2505"],
        "fut_code": ["CU", "A"],
        "multiplier": [None, None],
        "trade_unit": ["5吨/手", "10吨/手"],
        "per_unit": [5.0, 10.0],
        "quote_unit": ["元/吨", "元/吨"],
        "quote_unit_desc": ["10元/吨", "1元/吨"],
        "d_mode_desc": ["实物交割", "实物交割"],
        "list_date": ["20240101", "20240101"],
        "delist_date": ["20240115", "20240515"],
        "d_month": ["202401", "202405"],
        "last_ddate": ["20240115", "20240515"],
        "trade_time_desc": [None, None],
    })
    write_daily_partition(tmp_path / "futures", "fut_basic", dt(2024, 1, 2), df)

    result = futures_api.fut_basic(exchange="DCE")
    assert len(result) == 1
    assert result.iloc[0]["ts_code"] == "A2505.DCE"


def test_fut_daily_query_returns_data(tmp_path):
    from datetime import date as dt
    api = LocalPro(tmp_path)
    _write_fut_daily_data(tmp_path, dt(2024, 1, 2))

    result = api.fut_daily(trade_date="20240102")
    assert len(result) == 2


def test_fut_daily_query_filters_by_ts_code(tmp_path):
    from datetime import date as dt
    api = LocalPro(tmp_path)
    _write_fut_daily_data(tmp_path, dt(2024, 1, 2))

    result = api.fut_daily(ts_code="CU2401.SHF", trade_date="20240102")
    assert len(result) == 1
    assert result.iloc[0]["ts_code"] == "CU2401.SHF"


def test_fut_daily_query_raises_when_no_data(tmp_path):
    api = LocalPro(tmp_path)
    with pytest.raises(FileNotFoundError):
        api.fut_daily(trade_date="20240102")


def test_fut_holding_query_returns_data(tmp_path):
    from datetime import date as dt
    api = LocalPro(tmp_path)
    df = pd.DataFrame({
        "trade_date": [dt(2024, 1, 2), dt(2024, 1, 2)],
        "symbol": ["CU", "CU"],
        "broker": ["中信期货", "国泰君安"],
        "vol": [10000, 8000],
        "vol_chg": [500, -200],
        "long_hld": [15000, 12000],
        "long_chg": [200, -100],
        "short_hld": [12000, 10000],
        "short_chg": [-100, 300],
        "exchange": ["SHFE", "SHFE"],
    })
    write_daily_partition(tmp_path / "futures", "fut_holding", dt(2024, 1, 2), df)

    result = api.fut_holding(trade_date="20240102")
    assert len(result) == 2


def test_fut_mapping_query_returns_data(tmp_path):
    from datetime import date as dt
    api = LocalPro(tmp_path)
    df = pd.DataFrame({
        "ts_code": ["CU.SHF", "AG.SHF"],
        "trade_date": [dt(2024, 1, 2), dt(2024, 1, 2)],
        "mapping_ts_code": ["CU2401.SHF", "AG2401.SHF"],
    })
    write_daily_partition(tmp_path / "futures", "fut_mapping", dt(2024, 1, 2), df)

    result = api.fut_mapping(trade_date="20240102")
    assert len(result) == 2


def test_query_dispatch_supports_futures(tmp_path):
    from datetime import date as dt
    api = LocalPro(tmp_path)
    _write_fut_daily_data(tmp_path, dt(2024, 1, 2))

    result = api.query("fut_daily", trade_date="20240102")
    assert len(result) == 2


# --- Futures batch 2 API tests ---


def test_ft_limit_query_returns_data(tmp_path):
    from datetime import date as dt
    api = LocalPro(tmp_path)
    df = pd.DataFrame({
        "trade_date": [dt(2024, 1, 2)],
        "ts_code": ["CU2401.SHF"], "name": ["沪铜2401"],
        "up_limit": [51000.0], "down_limit": [49000.0],
        "m_ratio": [0.10], "cont": ["CU"], "exchange": ["SHFE"],
    })
    write_daily_partition(tmp_path / "futures", "ft_limit", dt(2024, 1, 2), df)

    result = api.ft_limit(trade_date="20240102")
    assert len(result) == 1


def test_fut_weekly_query_returns_data(tmp_path):
    from datetime import date as dt
    api = LocalPro(tmp_path)
    df = pd.DataFrame({
        "ts_code": ["CU2401.SHF"], "trade_date": [dt(2024, 1, 2)],
        "freq": ["week"], "open": [50000.0], "high": [50500.0],
        "low": [49900.0], "close": [50300.0], "pre_close": [50000.0],
        "settle": [50250.0], "pre_settle": [50100.0], "vol": [10000.0],
        "amount": [251250.0], "oi": [50000.0], "oi_chg": [500.0],
        "exchange": ["SHFE"], "change1": [200.0], "change2": [150.0],
    })
    write_daily_partition(tmp_path / "futures", "fut_weekly", dt(2024, 1, 2), df)

    result = api.fut_weekly(trade_date="20240102")
    assert len(result) == 1


def test_fut_monthly_query_returns_data(tmp_path):
    from datetime import date as dt
    api = LocalPro(tmp_path)
    df = pd.DataFrame({
        "ts_code": ["CU2401.SHF"], "trade_date": [dt(2024, 1, 2)],
        "freq": ["month"], "open": [50000.0], "high": [50500.0],
        "low": [49900.0], "close": [50300.0], "pre_close": [50000.0],
        "settle": [50250.0], "pre_settle": [50100.0], "vol": [10000.0],
        "amount": [251250.0], "oi": [50000.0], "oi_chg": [500.0],
        "exchange": ["SHFE"], "change1": [200.0], "change2": [150.0],
    })
    write_daily_partition(tmp_path / "futures", "fut_monthly", dt(2024, 1, 2), df)

    result = api.fut_monthly(trade_date="20240102")
    assert len(result) == 1


def test_fut_index_daily_query_returns_data(tmp_path):
    from datetime import date as dt
    api = LocalPro(tmp_path)
    df = pd.DataFrame({
        "ts_code": ["NHAI.NH", "NHCI.NH"],
        "trade_date": [dt(2024, 1, 2), dt(2024, 1, 2)],
        "close": [1000.0, 800.0], "open": [998.0, 798.0],
        "high": [1005.0, 805.0], "low": [995.0, 795.0],
        "pre_close": [998.0, 798.0], "change": [2.0, 2.0],
        "pct_chg": [0.2, 0.25], "vol": [50000.0, 30000.0],
        "amount": [50000000.0, 24000000.0],
    })
    write_daily_partition(tmp_path / "futures", "fut_index_daily", dt(2024, 1, 2), df)

    result = api.fut_index_daily(trade_date="20240102")
    assert len(result) == 2


def test_fut_weekly_detail_query_returns_data(tmp_path):
    from datetime import date as dt
    api = LocalPro(tmp_path)
    df = pd.DataFrame({
        "exchange": ["SHFE", "DCE"],
        "prd": ["CU", "A"],
        "name": ["沪铜", "豆一"],
        "vol": [100000, 80000], "vol_yoy": [5.0, 3.0],
        "amount": [250.0, 400.0], "amout_yoy": [3.0, 2.0],
        "cumvol": [5000000, 4000000], "cumvol_yoy": [4.0, 3.0],
        "cumamt": [12500.0, 20000.0], "cumamt_yoy": [2.0, 1.0],
        "open_interest": [200000, 150000], "interest_wow": [1.0, -0.5],
        "mc_close": [50300.0, 5000.0], "close_wow": [0.5, -0.3],
        "week": ["202401", "202401"], "week_date": [dt(2024, 1, 1), dt(2024, 1, 1)],
    })
    write_daily_partition(tmp_path / "futures", "fut_weekly_detail", dt(2024, 1, 1), df)

    result = api.fut_weekly_detail()
    assert len(result) == 2


def test_batch2_query_dispatch(tmp_path):
    from datetime import date as dt
    api = LocalPro(tmp_path)
    df = pd.DataFrame({
        "trade_date": [dt(2024, 1, 2)], "ts_code": ["CU2401.SHF"],
        "name": ["沪铜2401"], "up_limit": [51000.0], "down_limit": [49000.0],
        "m_ratio": [0.10], "cont": ["CU"], "exchange": ["SHFE"],
    })
    write_daily_partition(tmp_path / "futures", "ft_limit", dt(2024, 1, 2), df)

    result = api.query("ft_limit", trade_date="20240102")
    assert len(result) == 1
