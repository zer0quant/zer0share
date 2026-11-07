import pandas as pd
import pytest

from zer0share.api import LocalPro
from zer0share.storage import (
    DailyPartitionStore,
    SnapshotStore,
    write_trade_cal,
    write_universe,
)


def _make_etf_basic_df():
    return pd.DataFrame({
        "ts_code": ["510300.SH", "159919.SZ", "513100.SH"],
        "csname": ["沪深300ETF", "沪深300ETF", "纳指ETF"],
        "extname": ["沪深300ETF", "沪深300ETF", "纳指ETF"],
        "cname": [
            "华泰柏瑞沪深300交易型开放式指数证券投资基金",
            "嘉实沪深300交易型开放式指数证券投资基金",
            "国泰纳斯达克100交易型开放式指数证券投资基金",
        ],
        "index_code": ["000300.SH", "000300.SH", "NDX.GI"],
        "index_name": ["沪深300指数", "沪深300指数", "纳斯达克100指数"],
        "setup_date": ["20120504", "20120507", "20130425"],
        "list_date": ["20120528", "20120528", "20130515"],
        "list_status": ["L", "L", "L"],
        "exchange": ["SH", "SZ", "SH"],
        "mgr_name": ["华泰柏瑞基金", "嘉实基金", "国泰基金"],
        "custod_name": ["中国工商银行", "中国银行", "中国建设银行"],
        "mgt_fee": [0.5, 0.5, 0.8],
        "etf_type": ["境内", "境内", "QDII"],
    })


def _make_etf_index_df():
    return pd.DataFrame({
        "ts_code": ["000300.SH", "000905.SH", "000001.SH"],
        "indx_name": ["沪深300指数", "中证500指数", "上证综合指数"],
        "indx_csname": ["沪深300", "中证500", "上证指数"],
        "pub_party_name": ["中证指数有限公司", "中证指数有限公司", "上海证券交易所"],
        "pub_date": ["20050408", "20070115", "19910715"],
        "base_date": ["20041231", "20041231", "19901219"],
        "bp": [1000.0, 1000.0, 100.0],
        "adj_circle": ["半年", "半年", "不定期"],
    })


def _make_fund_daily_df():
    return pd.DataFrame({
        "ts_code": ["510300.SH", "510300.SH", "159919.SZ", "159919.SZ"],
        "trade_date": ["20240102", "20240103", "20240102", "20240103"],
        "open": [3.1, 3.2, 2.1, 2.2],
        "high": [3.2, 3.3, 2.2, 2.3],
        "low": [3.0, 3.1, 2.0, 2.1],
        "close": [3.15, 3.25, 2.15, 2.25],
        "pre_close": [3.05, 3.15, 2.05, 2.15],
        "change": [0.1, 0.1, 0.1, 0.1],
        "pct_chg": [3.28, 3.17, 4.88, 4.65],
        "vol": [1000000.0, 1100000.0, 2000000.0, 2100000.0],
        "amount": [3150000.0, 3575000.0, 4300000.0, 4725000.0],
    })


def _make_fund_adj_df():
    return pd.DataFrame(
        {
            "ts_code": ["159919.SZ", "159919.SZ", "513100.SH", "513100.SH"],
            "trade_date": ["20240102", "20240103", "20240102", "20240103"],
            "adj_factor": [1.0, 1.01, 1.2345, 1.2456],
            "discount_rate": [0.01, 0.02, 0.12, 0.15],
        }
    )


def _make_etf_share_size_df():
    return pd.DataFrame(
        {
            "trade_date": ["20240102", "20240103", "20240102", "20240103"],
            "ts_code": ["510330.SH", "510330.SH", "159919.SZ", "159919.SZ"],
            "etf_name": ["沪深300ETF华夏", "沪深300ETF华夏", "沪深300ETF嘉实", "沪深300ETF嘉实"],
            "total_share": [3986754.98, 3994674.98, 1200000.0, 1210000.0],
            "total_size": [15939050.0, 15781760.0, 2500000.0, 2510000.0],
            "nav": [4.0, 3.95, 2.1, 2.11],
            "close": [4.01, 3.96, 2.12, 2.13],
            "exchange": ["SSE", "SSE", "SZSE", "SZSE"],
        }
    )


def _make_etf_sh_cons_df():
    return pd.DataFrame(
        {
            "trade_date": ["20260615", "20260615", "20260616", "20260616"],
            "ts_code": ["517030.SH", "517030.SH", "517030.SH", "510300.SH"],
            "con_code": ["000001.SZ", "00003.HK", "000001.SZ", "600519.SH"],
            "con_name": ["平安银行", "香港中华煤气", "平安银行", "贵州茅台"],
            "qty": [1100, 1000, 1200, 200],
            "sub_flag": ["允许", "允许", "允许", "必须"],
            "cpr": ["15", "30", "15", "-"],
            "rdr": ["60", "0", "60", "-"],
            "sca": ["12364.000", "5928.350", "12500.000", "0.000"],
            "exchange": ["SZ", "HK", "SZ", "SH"],
        }
    )


def _make_idx_anns_df():
    return pd.DataFrame(
        {
            "ann_date": ["20260416", "20260416", "20260417", "20260420"],
            "title": [
                "关于发布中证交易所AAA综合债指数系列的公告",
                "关于调整三板指数样本股的公告",
                "恒生中国高股息率指数年度指数检讨结果",
                "关于发布华证HALO指数的公告",
            ],
            "url": [
                "https://www.csindex.com.cn/#/about/newsDetail?id=2",
                "https://www.csindex.com.cn/#/about/newsDetail?id=1",
                "https://www.hsi.com.hk/static/uploads/contents/zh_cn/news/1.pdf",
                "https://www.chindices.com/news_detail.html?id=777",
            ],
            "source": ["中证指数", "中证指数", "恒生指数", "华证指数"],
            "type": ["新指数发布", "指数调样", "其他", ""],
        }
    )


def test_etf_basic_query_returns_data(tmp_path):
    SnapshotStore(tmp_path / "etf" / "etf_basic" / "data.parquet").write(_make_etf_basic_df())

    api = LocalPro(tmp_path)
    result = api.etf_basic(fields="ts_code,extname,index_code,exchange")

    assert result.to_dict("records") == [
        {"ts_code": "159919.SZ", "extname": "沪深300ETF", "index_code": "000300.SH", "exchange": "SZ"},
        {"ts_code": "510300.SH", "extname": "沪深300ETF", "index_code": "000300.SH", "exchange": "SH"},
        {"ts_code": "513100.SH", "extname": "纳指ETF", "index_code": "NDX.GI", "exchange": "SH"},
    ]


def test_etf_basic_filters_by_codes_status_exchange_and_mgr(tmp_path):
    SnapshotStore(tmp_path / "etf" / "etf_basic" / "data.parquet").write(_make_etf_basic_df())

    api = LocalPro(tmp_path)
    result = api.etf_basic(
        ts_code="510300.SH,159919.SZ",
        index_code="000300.SH",
        list_status="L",
        exchange="SH",
        mgr="华泰柏瑞基金",
        fields="ts_code,mgr_name,exchange",
    )

    assert result.to_dict("records") == [
        {"ts_code": "510300.SH", "mgr_name": "华泰柏瑞基金", "exchange": "SH"}
    ]


def test_etf_basic_filters_by_mgr_name_list_date_limit_offset(tmp_path):
    SnapshotStore(tmp_path / "etf" / "etf_basic" / "data.parquet").write(_make_etf_basic_df())

    api = LocalPro(tmp_path)
    result = api.etf_basic(
        list_date="20120528",
        mgr_name="嘉实基金",
        limit=1,
        offset=0,
        fields="ts_code,list_date,mgr_name",
    )

    assert result.to_dict("records") == [
        {"ts_code": "159919.SZ", "list_date": "20120528", "mgr_name": "嘉实基金"}
    ]


def test_etf_basic_rejects_conflicting_mgr_aliases(tmp_path):
    SnapshotStore(tmp_path / "etf" / "etf_basic" / "data.parquet").write(_make_etf_basic_df())

    api = LocalPro(tmp_path)

    with pytest.raises(ValueError, match="mgr and mgr_name must match"):
        api.etf_basic(mgr="华泰柏瑞基金", mgr_name="嘉实基金")


def test_query_dispatch_supports_etf_basic(tmp_path):
    SnapshotStore(tmp_path / "etf" / "etf_basic" / "data.parquet").write(_make_etf_basic_df())

    api = LocalPro(tmp_path)
    result = api.query("etf_basic", exchange="SZ", fields="ts_code,exchange")

    assert result.to_dict("records") == [
        {"ts_code": "159919.SZ", "exchange": "SZ"}
    ]


def test_etf_basic_query_raises_when_no_data(tmp_path):
    api = LocalPro(tmp_path)

    with pytest.raises(FileNotFoundError, match="etf_basic"):
        api.etf_basic()


def test_etf_index_query_returns_data(tmp_path):
    SnapshotStore(tmp_path / "etf" / "etf_index" / "data.parquet").write(_make_etf_index_df())

    api = LocalPro(tmp_path)
    result = api.etf_index(fields="ts_code,indx_name,pub_date,bp")

    assert result.to_dict("records") == [
        {"ts_code": "000001.SH", "indx_name": "上证综合指数", "pub_date": "19910715", "bp": 100.0},
        {"ts_code": "000300.SH", "indx_name": "沪深300指数", "pub_date": "20050408", "bp": 1000.0},
        {"ts_code": "000905.SH", "indx_name": "中证500指数", "pub_date": "20070115", "bp": 1000.0},
    ]


def test_etf_index_filters_by_code_pub_date_and_base_date(tmp_path):
    SnapshotStore(tmp_path / "etf" / "etf_index" / "data.parquet").write(_make_etf_index_df())

    api = LocalPro(tmp_path)
    result = api.etf_index(
        ts_code="000300.SH,000905.SH",
        pub_date="20050408",
        base_date="20041231",
        fields="ts_code,pub_date,base_date",
    )

    assert result.to_dict("records") == [
        {"ts_code": "000300.SH", "pub_date": "20050408", "base_date": "20041231"}
    ]


def test_etf_index_supports_limit_offset_and_query_dispatch(tmp_path):
    SnapshotStore(tmp_path / "etf" / "etf_index" / "data.parquet").write(_make_etf_index_df())

    api = LocalPro(tmp_path)
    result = api.query(
        "etf_index",
        base_date="20041231",
        offset=1,
        limit=1,
        fields="ts_code,base_date",
    )

    assert result.to_dict("records") == [
        {"ts_code": "000905.SH", "base_date": "20041231"}
    ]


def test_etf_index_validates_date_filters(tmp_path):
    SnapshotStore(tmp_path / "etf" / "etf_index" / "data.parquet").write(_make_etf_index_df())

    api = LocalPro(tmp_path)
    with pytest.raises(ValueError, match="YYYYMMDD"):
        api.etf_index(pub_date="2005-04-08")


def test_etf_index_query_raises_when_no_data(tmp_path):
    api = LocalPro(tmp_path)

    with pytest.raises(FileNotFoundError, match="etf_index"):
        api.etf_index()


def test_fund_daily_query_returns_local_data_and_selected_fields(tmp_path):
    data = _make_fund_daily_df()
    DailyPartitionStore(tmp_path / "etf" / "fund_daily").write("20240102", data[data["trade_date"] == "20240102"])
    DailyPartitionStore(tmp_path / "etf" / "fund_daily").write("20240103", data[data["trade_date"] == "20240103"])

    api = LocalPro(tmp_path)
    result = api.fund_daily(
        start_date="20240102",
        end_date="20240103",
        fields="ts_code,trade_date,close,amount",
    )

    assert result.to_dict("records") == [
        {"ts_code": "159919.SZ", "trade_date": "20240102", "close": 2.15, "amount": 4300000.0},
        {"ts_code": "159919.SZ", "trade_date": "20240103", "close": 2.25, "amount": 4725000.0},
        {"ts_code": "510300.SH", "trade_date": "20240102", "close": 3.15, "amount": 3150000.0},
        {"ts_code": "510300.SH", "trade_date": "20240103", "close": 3.25, "amount": 3575000.0},
    ]


def test_fund_daily_filters_by_ts_code_and_date_range(tmp_path):
    data = _make_fund_daily_df()
    DailyPartitionStore(tmp_path / "etf" / "fund_daily").write("20240102", data[data["trade_date"] == "20240102"])
    DailyPartitionStore(tmp_path / "etf" / "fund_daily").write("20240103", data[data["trade_date"] == "20240103"])

    api = LocalPro(tmp_path)
    result = api.fund_daily(
        ts_code="510300.SH",
        start_date="20240103",
        end_date="20240103",
        fields="ts_code,trade_date,close",
    )

    assert result.to_dict("records") == [
        {"ts_code": "510300.SH", "trade_date": "20240103", "close": 3.25}
    ]


def test_fund_daily_rejects_unbounded_query_by_default(tmp_path):
    data = _make_fund_daily_df()
    DailyPartitionStore(tmp_path / "etf" / "fund_daily").write("20240102", data[data["trade_date"] == "20240102"])

    api = LocalPro(tmp_path)

    with pytest.raises(ValueError, match="daily-partitioned table"):
        api.fund_daily(ts_code="510300.SH", limit=1)


def test_fund_daily_supports_trade_date_limit_offset_and_query_dispatch(tmp_path):
    data = _make_fund_daily_df()
    DailyPartitionStore(tmp_path / "etf" / "fund_daily").write("20240102", data[data["trade_date"] == "20240102"])
    DailyPartitionStore(tmp_path / "etf" / "fund_daily").write("20240103", data[data["trade_date"] == "20240103"])

    api = LocalPro(tmp_path)
    result = api.query(
        "fund_daily",
        trade_date="20240102",
        offset=1,
        limit=1,
        fields="ts_code,trade_date,close",
    )

    assert result.to_dict("records") == [
        {"ts_code": "510300.SH", "trade_date": "20240102", "close": 3.15}
    ]


def test_fund_daily_validates_date_filters(tmp_path):
    data = _make_fund_daily_df()
    DailyPartitionStore(tmp_path / "etf" / "fund_daily").write("20240102", data[data["trade_date"] == "20240102"])

    api = LocalPro(tmp_path)
    with pytest.raises(ValueError, match="YYYYMMDD"):
        api.fund_daily(trade_date="2024-01-02")


def test_fund_daily_query_raises_when_no_data(tmp_path):
    api = LocalPro(tmp_path)

    with pytest.raises(FileNotFoundError, match="fund_daily"):
        api.fund_daily()


def test_fund_adj_query_returns_local_data_and_selected_fields(tmp_path):
    data = _make_fund_adj_df()
    DailyPartitionStore(tmp_path / "etf" / "fund_adj").write("20240102", data[data["trade_date"] == "20240102"])
    DailyPartitionStore(tmp_path / "etf" / "fund_adj").write("20240103", data[data["trade_date"] == "20240103"])

    api = LocalPro(tmp_path)
    result = api.fund_adj(
        start_date="20240102",
        end_date="20240103",
        fields="ts_code,trade_date,adj_factor",
    )

    assert result.to_dict("records") == [
        {"ts_code": "159919.SZ", "trade_date": "20240102", "adj_factor": 1.0},
        {"ts_code": "159919.SZ", "trade_date": "20240103", "adj_factor": 1.01},
        {"ts_code": "513100.SH", "trade_date": "20240102", "adj_factor": 1.2345},
        {"ts_code": "513100.SH", "trade_date": "20240103", "adj_factor": 1.2456},
    ]


def test_fund_adj_filters_by_ts_code_and_date_range(tmp_path):
    data = _make_fund_adj_df()
    DailyPartitionStore(tmp_path / "etf" / "fund_adj").write("20240102", data[data["trade_date"] == "20240102"])
    DailyPartitionStore(tmp_path / "etf" / "fund_adj").write("20240103", data[data["trade_date"] == "20240103"])

    api = LocalPro(tmp_path)
    result = api.fund_adj(
        ts_code="513100.SH",
        start_date="20240103",
        end_date="20240103",
        fields="ts_code,trade_date,adj_factor,discount_rate",
    )

    assert result.to_dict("records") == [
        {
            "ts_code": "513100.SH",
            "trade_date": "20240103",
            "adj_factor": 1.2456,
            "discount_rate": 0.15,
        }
    ]


def test_fund_adj_supports_trade_date_limit_offset_and_query_dispatch(tmp_path):
    data = _make_fund_adj_df()
    DailyPartitionStore(tmp_path / "etf" / "fund_adj").write("20240102", data[data["trade_date"] == "20240102"])
    DailyPartitionStore(tmp_path / "etf" / "fund_adj").write("20240103", data[data["trade_date"] == "20240103"])

    api = LocalPro(tmp_path)
    result = api.query(
        "fund_adj",
        trade_date="20240102",
        offset=1,
        limit=1,
        fields="ts_code,trade_date,adj_factor",
    )

    assert result.to_dict("records") == [
        {"ts_code": "513100.SH", "trade_date": "20240102", "adj_factor": 1.2345}
    ]


def test_fund_adj_validates_date_filters(tmp_path):
    data = _make_fund_adj_df()
    DailyPartitionStore(tmp_path / "etf" / "fund_adj").write("20240102", data[data["trade_date"] == "20240102"])

    api = LocalPro(tmp_path)
    with pytest.raises(ValueError, match="YYYYMMDD"):
        api.fund_adj(trade_date="2024-01-02")


def test_fund_adj_query_raises_when_no_data(tmp_path):
    api = LocalPro(tmp_path)

    with pytest.raises(FileNotFoundError, match="fund_adj"):
        api.fund_adj()


def test_etf_share_size_query_returns_local_data_and_selected_fields(tmp_path):
    data = _make_etf_share_size_df()
    DailyPartitionStore(tmp_path / "etf" / "etf_share_size").write("20240102", data[data["trade_date"] == "20240102"])
    DailyPartitionStore(tmp_path / "etf" / "etf_share_size").write("20240103", data[data["trade_date"] == "20240103"])

    api = LocalPro(tmp_path)
    result = api.etf_share_size(
        start_date="20240102",
        end_date="20240103",
        fields="trade_date,ts_code,etf_name,total_share,total_size,exchange",
    )

    assert result.to_dict("records") == [
        {"trade_date": "20240102", "ts_code": "159919.SZ", "etf_name": "沪深300ETF嘉实", "total_share": 1200000.0, "total_size": 2500000.0, "exchange": "SZSE"},
        {"trade_date": "20240103", "ts_code": "159919.SZ", "etf_name": "沪深300ETF嘉实", "total_share": 1210000.0, "total_size": 2510000.0, "exchange": "SZSE"},
        {"trade_date": "20240102", "ts_code": "510330.SH", "etf_name": "沪深300ETF华夏", "total_share": 3986754.98, "total_size": 15939050.0, "exchange": "SSE"},
        {"trade_date": "20240103", "ts_code": "510330.SH", "etf_name": "沪深300ETF华夏", "total_share": 3994674.98, "total_size": 15781760.0, "exchange": "SSE"},
    ]


def test_etf_share_size_filters_by_ts_code_date_range_and_exchange(tmp_path):
    data = _make_etf_share_size_df()
    DailyPartitionStore(tmp_path / "etf" / "etf_share_size").write("20240102", data[data["trade_date"] == "20240102"])
    DailyPartitionStore(tmp_path / "etf" / "etf_share_size").write("20240103", data[data["trade_date"] == "20240103"])

    api = LocalPro(tmp_path)
    result = api.etf_share_size(
        ts_code="510330.SH,159919.SZ",
        start_date="20240103",
        end_date="20240103",
        exchange="SSE",
        fields="trade_date,ts_code,total_share,exchange",
    )

    assert result.to_dict("records") == [
        {"trade_date": "20240103", "ts_code": "510330.SH", "total_share": 3994674.98, "exchange": "SSE"}
    ]


def test_etf_share_size_supports_trade_date_limit_offset_and_query_dispatch(tmp_path):
    data = _make_etf_share_size_df()
    DailyPartitionStore(tmp_path / "etf" / "etf_share_size").write("20240102", data[data["trade_date"] == "20240102"])
    DailyPartitionStore(tmp_path / "etf" / "etf_share_size").write("20240103", data[data["trade_date"] == "20240103"])

    api = LocalPro(tmp_path)
    result = api.query(
        "etf_share_size",
        trade_date="20240102",
        offset=1,
        limit=1,
        fields="trade_date,ts_code,total_size,exchange",
    )

    assert result.to_dict("records") == [
        {"trade_date": "20240102", "ts_code": "510330.SH", "total_size": 15939050.0, "exchange": "SSE"}
    ]


def test_etf_share_size_validates_date_filters(tmp_path):
    data = _make_etf_share_size_df()
    DailyPartitionStore(tmp_path / "etf" / "etf_share_size").write("20240102", data[data["trade_date"] == "20240102"])

    api = LocalPro(tmp_path)
    with pytest.raises(ValueError, match="YYYYMMDD"):
        api.etf_share_size(trade_date="2024-01-02")


def test_etf_share_size_query_raises_when_no_data(tmp_path):
    api = LocalPro(tmp_path)

    with pytest.raises(FileNotFoundError, match="etf_share_size"):
        api.etf_share_size()


def test_etf_sh_cons_query_returns_local_data_and_selected_fields(tmp_path):
    data = _make_etf_sh_cons_df()
    DailyPartitionStore(tmp_path / "etf" / "etf_sh_cons").write("20260615", data[data["trade_date"] == "20260615"])
    DailyPartitionStore(tmp_path / "etf" / "etf_sh_cons").write("20260616", data[data["trade_date"] == "20260616"])

    api = LocalPro(tmp_path)
    result = api.etf_sh_cons(
        start_date="20260615",
        end_date="20260616",
        fields="trade_date,ts_code,con_code,con_name,qty,exchange",
    )

    assert result.to_dict("records") == [
        {"trade_date": "20260616", "ts_code": "510300.SH", "con_code": "600519.SH", "con_name": "贵州茅台", "qty": 200, "exchange": "SH"},
        {"trade_date": "20260615", "ts_code": "517030.SH", "con_code": "000001.SZ", "con_name": "平安银行", "qty": 1100, "exchange": "SZ"},
        {"trade_date": "20260615", "ts_code": "517030.SH", "con_code": "00003.HK", "con_name": "香港中华煤气", "qty": 1000, "exchange": "HK"},
        {"trade_date": "20260616", "ts_code": "517030.SH", "con_code": "000001.SZ", "con_name": "平安银行", "qty": 1200, "exchange": "SZ"},
    ]


def test_etf_sh_cons_filters_by_ts_code_date_range_and_con_code(tmp_path):
    data = _make_etf_sh_cons_df()
    DailyPartitionStore(tmp_path / "etf" / "etf_sh_cons").write("20260615", data[data["trade_date"] == "20260615"])
    DailyPartitionStore(tmp_path / "etf" / "etf_sh_cons").write("20260616", data[data["trade_date"] == "20260616"])

    api = LocalPro(tmp_path)
    result = api.etf_sh_cons(
        ts_code="517030.SH",
        start_date="20260615",
        end_date="20260616",
        con_code="000001.SZ",
        fields="trade_date,ts_code,con_code,qty",
    )

    assert result.to_dict("records") == [
        {"trade_date": "20260615", "ts_code": "517030.SH", "con_code": "000001.SZ", "qty": 1100},
        {"trade_date": "20260616", "ts_code": "517030.SH", "con_code": "000001.SZ", "qty": 1200},
    ]


def test_etf_sh_cons_supports_trade_date_limit_offset_and_query_dispatch(tmp_path):
    data = _make_etf_sh_cons_df()
    DailyPartitionStore(tmp_path / "etf" / "etf_sh_cons").write("20260615", data[data["trade_date"] == "20260615"])

    api = LocalPro(tmp_path)
    result = api.query(
        "etf_sh_cons",
        trade_date="20260615",
        offset=1,
        limit=1,
        fields="trade_date,ts_code,con_code,exchange",
    )

    assert result.to_dict("records") == [
        {"trade_date": "20260615", "ts_code": "517030.SH", "con_code": "00003.HK", "exchange": "HK"}
    ]


def test_etf_sh_cons_validates_date_filters(tmp_path):
    data = _make_etf_sh_cons_df()
    DailyPartitionStore(tmp_path / "etf" / "etf_sh_cons").write("20260615", data[data["trade_date"] == "20260615"])

    api = LocalPro(tmp_path)
    with pytest.raises(ValueError, match="YYYYMMDD"):
        api.etf_sh_cons(trade_date="2026-06-15")


def test_etf_sh_cons_query_raises_when_no_data(tmp_path):
    api = LocalPro(tmp_path)

    with pytest.raises(FileNotFoundError, match="etf_sh_cons"):
        api.etf_sh_cons()


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
            "list_date": ["19910403", "19991110"],
            "delist_date": [None, None],
            "is_hs": ["S", "H"],
            "act_name": ["Shenzhen Investment Holdings", "Shanghai SASAC"],
            "act_ent_type": ["Local SOE", "Local SOE"],
        }
    )
    SnapshotStore(tmp_path / "stock" / "basic" / "data.parquet").write(df)

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
            "cal_date": ["20240101", "20240102", "20240103"],
            "is_open": [False, True, True],
            "pretrade_date": ["20231229", "20231229", "20240102"],
        }
    )
    write_trade_cal(tmp_path, "SSE", df)

    pro = LocalPro(tmp_path)
    result = pro.trade_cal(
        exchange="SSE",
        start_date="20240102",
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


def test_api_date_filters_reject_dash_separated_dates(tmp_path):
    df = pd.DataFrame(
        {
            "exchange": ["SSE"],
            "cal_date": ["20240102"],
            "is_open": [True],
            "pretrade_date": ["20240101"],
        }
    )
    write_trade_cal(tmp_path, "SSE", df)

    pro = LocalPro(tmp_path)
    with pytest.raises(ValueError, match="YYYYMMDD"):
        pro.trade_cal(exchange="SSE", start_date="2024-01-02")


def test_daily_filters_multiple_codes_by_date_range_and_formats_dates(tmp_path):
    DailyPartitionStore(tmp_path / "stock" / "daily_kline").write("20240102", pd.DataFrame(
            {
                "ts_code": ["000001.SZ", "600000.SH"],
                "trade_date": ["20240102", "20240102"],
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
        ))
    DailyPartitionStore(tmp_path / "stock" / "daily_kline").write("20240103", pd.DataFrame(
            {
                "ts_code": ["000001.SZ", "600000.SH"],
                "trade_date": ["20240103", "20240103"],
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
        ))

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
    DailyPartitionStore(tmp_path / "stock" / "stock_st").write("20240102", pd.DataFrame(columns=["ts_code", "name", "trade_date", "type", "type_name"]))
    DailyPartitionStore(tmp_path / "stock" / "stock_st").write("20240103", pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "name": ["ST Test"],
                "trade_date": ["20240103"],
                "type": ["ST"],
                "type_name": ["Risk"],
            }
        ))

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
        "20240102",
        pd.DataFrame(
            {
                "trade_date": ["20240102", "20240102"],
                "universe": ["univ_trade_base", "univ_trade_base"],
                "ts_code": ["000001.SZ", "600000.SH"],
            }
        ),
    )
    write_universe(
        tmp_path,
        "univ_research_base",
        "20240102",
        pd.DataFrame(
            {
                "trade_date": ["20240102"],
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
    DailyPartitionStore(tmp_path / "stock" / "adj_factor").write("20240102", pd.DataFrame(
            {
                "ts_code": ["000001.SZ", "600000.SH"],
                "trade_date": ["20240102", "20240102"],
                "adj_factor": [100.1, 200.2],
            }
        ))

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
    DailyPartitionStore(tmp_path / "stock" / "adj_factor").write("20240102", pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "trade_date": ["20240102"],
                "adj_factor": [100.1],
            }
        ))

    pro = LocalPro(tmp_path)
    result = pro.query("adj_factor", ts_code="000001.SZ", trade_date="20240102")

    assert result.to_dict("records") == [
        {"ts_code": "000001.SZ", "trade_date": "20240102", "adj_factor": 100.1}
    ]


def test_unknown_query_api_raises_value_error(tmp_path):
    pro = LocalPro(tmp_path)

    with pytest.raises(ValueError, match="unknown api"):
        pro.query("moneyflow")


def test_unknown_field_raises_value_error(tmp_path):
    SnapshotStore(tmp_path / "stock" / "basic" / "data.parquet").write(pd.DataFrame(
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
                "list_date": ["19910403"],
                "delist_date": [None],
                "is_hs": ["S"],
                "act_name": ["Shenzhen Investment Holdings"],
                "act_ent_type": ["Local SOE"],
            }
        ))
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
                "cal_date": ["20240102"],
                "is_open": [True],
                "pretrade_date": ["20231229"],
            }
        ),
    )
    pro = LocalPro(tmp_path)

    with pytest.raises(ValueError, match="invalid date"):
        pro.trade_cal(start_date="2024/01/02")


def test_invalid_date_range_raises_value_error(tmp_path):
    DailyPartitionStore(tmp_path / "stock" / "daily_kline").write("20240102", pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "trade_date": ["20240102"],
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
        ))
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
                "cal_date": ["20240102"],
                "is_open": [True],
                "pretrade_date": ["20231229"],
            }
        ),
    )
    pro = LocalPro(tmp_path)

    with pytest.raises(ValueError, match="start_date"):
        pro.trade_cal(start_date="20240103", end_date="20240102")


def test_pro_bar_returns_qfq_prices_using_end_date_factor(tmp_path):
    DailyPartitionStore(tmp_path / "stock" / "daily_kline").write("20240102", pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "trade_date": ["20240102"],
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
        ))
    DailyPartitionStore(tmp_path / "stock" / "daily_kline").write("20240103", pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "trade_date": ["20240103"],
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
        ))
    DailyPartitionStore(tmp_path / "stock" / "adj_factor").write("20240102", pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "trade_date": ["20240102"],
                "adj_factor": [2.0],
            }
        ))
    DailyPartitionStore(tmp_path / "stock" / "adj_factor").write("20240103", pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "trade_date": ["20240103"],
                "adj_factor": [4.0],
            }
        ))

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
    DailyPartitionStore(tmp_path / "stock" / "daily_kline").write("20240102", pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "trade_date": ["20240102"],
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
        ))
    DailyPartitionStore(tmp_path / "stock" / "adj_factor").write("20240102", pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "trade_date": ["20240102"],
                "adj_factor": [2.0],
            }
        ))

    pro = LocalPro(tmp_path)
    result = pro.pro_bar(ts_code="000001.SZ", trade_date="20240102", adj="hfq")

    assert result[["open", "high", "low", "close", "pre_close"]].to_dict("records") == [
        {"open": 20.0, "high": 24.0, "low": 18.0, "close": 22.0, "pre_close": 20.0}
    ]


def test_pro_bar_rounds_adjusted_prices_to_two_decimals(tmp_path):
    DailyPartitionStore(tmp_path / "stock" / "daily_kline").write("20240102", pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "trade_date": ["20240102"],
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
        ))
    DailyPartitionStore(tmp_path / "stock" / "adj_factor").write("20240102", pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "trade_date": ["20240102"],
                "adj_factor": [1.234],
            }
        ))

    pro = LocalPro(tmp_path)
    result = pro.pro_bar(ts_code="000001.SZ", trade_date="20240102", adj="hfq")

    assert result.iloc[0]["close"] == 12.34


def test_pro_bar_supports_multiple_codes_with_qfq_base_per_stock(tmp_path):
    for day, rows in [
        (
            "20240101",
            [
                ["000001.SZ", 10.0, 10.0],
                ["000002.SZ", 20.0, 20.0],
            ],
        ),
        (
            "20240102",
            [
                ["000001.SZ", 12.0, 12.0],
                ["000002.SZ", 22.0, 22.0],
            ],
        ),
    ]:
        DailyPartitionStore(tmp_path / "stock" / "daily_kline").write(day, pd.DataFrame(
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
            ))
    for day, factors in [
        ("20240101", [1.0, 10.0]),
        ("20240102", [2.0, 20.0]),
    ]:
        DailyPartitionStore(tmp_path / "stock" / "adj_factor").write(day, pd.DataFrame(
                {
                    "ts_code": ["000001.SZ", "000002.SZ"],
                    "trade_date": [day, day],
                    "adj_factor": factors,
                }
            ))

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
    SnapshotStore(tmp_path / "stock" / "industry" / "sw_classify" / "data.parquet").write(pd.DataFrame({
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
    SnapshotStore(tmp_path / "stock" / "industry" / "sw_classify" / "data.parquet").write(pd.DataFrame({
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
    SnapshotStore(tmp_path / "stock" / "industry" / "sw_classify" / "data.parquet").write(pd.DataFrame({
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
    SnapshotStore(tmp_path / "stock" / "industry" / "sw_member" / "data.parquet").write(pd.DataFrame({
        "l1_code": ["801010.SI", "801030.SI"],
        "l1_name": ["农林牧渔", "化工"],
        "l2_code": ["801016.SI", "801033.SI"],
        "l2_name": ["种植业", "化学原料"],
        "l3_code": ["850111.SI", "850321.SI"],
        "l3_name": ["种子", "纯碱"],
        "ts_code": ["002041.SZ", "600291.SH"],
        "name": ["登海种业", "西水股份"],
        "in_date": ["20211213", "20211213"],
        "out_date": [None, None],
        "is_new": ["Y", "Y"],
    }))
    pro = LocalPro(tmp_path)
    result = pro.index_member_all(ts_code="002041.SZ")
    assert len(result) == 1
    assert result.iloc[0]["ts_code"] == "002041.SZ"


def test_index_member_all_filters_by_is_new(tmp_path):
    SnapshotStore(tmp_path / "stock" / "industry" / "sw_member" / "data.parquet").write(pd.DataFrame({
        "l1_code": ["801010.SI", "801010.SI"],
        "l1_name": ["农林牧渔", "农林牧渔"],
        "l2_code": ["801016.SI", "801016.SI"],
        "l2_name": ["种植业", "种植业"],
        "l3_code": ["850111.SI", "850111.SI"],
        "l3_name": ["种子", "种子"],
        "ts_code": ["002041.SZ", "600313.SH"],
        "name": ["登海种业", "农发种业"],
        "in_date": ["20211213", "20211213"],
        "out_date": ["20220630", None],
        "is_new": ["N", "Y"],
    }))
    pro = LocalPro(tmp_path)
    result = pro.index_member_all(is_new="Y")
    assert len(result) == 1
    assert result.iloc[0]["ts_code"] == "600313.SH"


def test_index_member_all_filters_by_l1_code(tmp_path):
    SnapshotStore(tmp_path / "stock" / "industry" / "sw_member" / "data.parquet").write(pd.DataFrame({
        "l1_code": ["801010.SI", "801030.SI"],
        "l1_name": ["农林牧渔", "化工"],
        "l2_code": ["801016.SI", "801033.SI"],
        "l2_name": ["种植业", "化学原料"],
        "l3_code": ["850111.SI", "850321.SI"],
        "l3_name": ["种子", "纯碱"],
        "ts_code": ["002041.SZ", "600291.SH"],
        "name": ["登海种业", "西水股份"],
        "in_date": ["20211213", "20211213"],
        "out_date": [None, None],
        "is_new": ["Y", "Y"],
    }))
    pro = LocalPro(tmp_path)
    result = pro.index_member_all(l1_code="801010.SI")
    assert len(result) == 1


def test_index_member_all_supports_multi_ts_code(tmp_path):
    SnapshotStore(tmp_path / "stock" / "industry" / "sw_member" / "data.parquet").write(pd.DataFrame({
        "l1_code": ["801010.SI", "801030.SI", "801040.SI"],
        "l1_name": ["农林牧渔", "化工", "钢铁"],
        "l2_code": ["801016.SI", "801033.SI", "801043.SI"],
        "l2_name": ["种植业", "化学原料", "冶钢原料"],
        "l3_code": ["850111.SI", "850321.SI", "850431.SI"],
        "l3_name": ["种子", "纯碱", "铁矿石"],
        "ts_code": ["002041.SZ", "600291.SH", "000002.SZ"],
        "name": ["登海种业", "西水股份", "万科A"],
        "in_date": ["20211213", "20211213", "20211213"],
        "out_date": [None, None, None],
        "is_new": ["Y", "Y", "Y"],
    }))
    pro = LocalPro(tmp_path)
    result = pro.index_member_all(ts_code="002041.SZ,000002.SZ")
    assert len(result) == 2


def test_index_member_all_formats_dates(tmp_path):
    SnapshotStore(tmp_path / "stock" / "industry" / "sw_member" / "data.parquet").write(pd.DataFrame({
        "l1_code": ["801010.SI"],
        "l1_name": ["农林牧渔"],
        "l2_code": ["801016.SI"],
        "l2_name": ["种植业"],
        "l3_code": ["850111.SI"],
        "l3_name": ["种子"],
        "ts_code": ["002041.SZ"],
        "name": ["登海种业"],
        "in_date": ["20211213"],
        "out_date": ["20220630"],
        "is_new": ["N"],
    }))
    pro = LocalPro(tmp_path)
    result = pro.index_member_all(ts_code="002041.SZ")
    assert result.iloc[0]["in_date"] == "20211213"
    assert result.iloc[0]["out_date"] == "20220630"


def test_ci_index_member_filters_by_ts_code(tmp_path):
    SnapshotStore(tmp_path / "stock" / "industry" / "ci_member" / "data.parquet").write(pd.DataFrame({
        "l1_code": ["CI005001.CI", "CI005002.CI"],
        "l1_name": ["农林牧渔", "采掘"],
        "l2_code": ["CI005005.CI", "CI005010.CI"],
        "l2_name": ["农产品加工", "煤炭开采"],
        "l3_code": ["CI005006.CI", "CI005011.CI"],
        "l3_name": ["粮油加工", "动力煤"],
        "ts_code": ["000876.SZ", "601088.SH"],
        "name": ["新 希 望", "中国神华"],
        "in_date": ["20200101", "20200101"],
        "out_date": [None, None],
        "is_new": ["Y", "Y"],
    }))
    pro = LocalPro(tmp_path)
    result = pro.ci_index_member(ts_code="000876.SZ")
    assert len(result) == 1
    assert result.iloc[0]["ts_code"] == "000876.SZ"


def test_ci_index_member_filters_by_is_new(tmp_path):
    SnapshotStore(tmp_path / "stock" / "industry" / "ci_member" / "data.parquet").write(pd.DataFrame({
        "l1_code": ["CI005001.CI", "CI005001.CI"],
        "l1_name": ["农林牧渔", "农林牧渔"],
        "l2_code": ["CI005005.CI", "CI005005.CI"],
        "l2_name": ["农产品加工", "农产品加工"],
        "l3_code": ["CI005006.CI", "CI005006.CI"],
        "l3_name": ["粮油加工", "粮油加工"],
        "ts_code": ["000876.SZ", "000877.SZ"],
        "name": ["新 希 望", "天山股份"],
        "in_date": ["20200101", "20200101"],
        "out_date": ["20231231", None],
        "is_new": ["N", "Y"],
    }))
    pro = LocalPro(tmp_path)
    result = pro.ci_index_member(is_new="Y")
    assert len(result) == 1


def test_ci_index_member_formats_dates(tmp_path):
    SnapshotStore(tmp_path / "stock" / "industry" / "ci_member" / "data.parquet").write(pd.DataFrame({
        "l1_code": ["CI005001.CI"],
        "l1_name": ["农林牧渔"],
        "l2_code": ["CI005005.CI"],
        "l2_name": ["农产品加工"],
        "l3_code": ["CI005006.CI"],
        "l3_name": ["粮油加工"],
        "ts_code": ["000876.SZ"],
        "name": ["新 希 望"],
        "in_date": ["20200101"],
        "out_date": ["20231231"],
        "is_new": ["N"],
    }))
    pro = LocalPro(tmp_path)
    result = pro.ci_index_member(ts_code="000876.SZ")
    assert result.iloc[0]["in_date"] == "20200101"
    assert result.iloc[0]["out_date"] == "20231231"


def test_query_dispatches_index_classify(tmp_path):
    SnapshotStore(tmp_path / "stock" / "industry" / "sw_classify" / "data.parquet").write(pd.DataFrame({
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
    SnapshotStore(tmp_path / "stock" / "industry" / "sw_member" / "data.parquet").write(pd.DataFrame({
        "l1_code": ["801010.SI"], "l1_name": ["农林牧渔"],
        "l2_code": ["801016.SI"], "l2_name": ["种植业"],
        "l3_code": ["850111.SI"], "l3_name": ["种子"],
        "ts_code": ["002041.SZ"], "name": ["登海种业"],
        "in_date": ["20211213"], "out_date": [None], "is_new": ["Y"],
    }))
    pro = LocalPro(tmp_path)
    result = pro.query("index_member_all", ts_code="002041.SZ")
    assert len(result) == 1


def test_query_dispatches_ci_index_member(tmp_path):
    SnapshotStore(tmp_path / "stock" / "industry" / "ci_member" / "data.parquet").write(pd.DataFrame({
        "l1_code": ["CI005001.CI"], "l1_name": ["农林牧渔"],
        "l2_code": ["CI005005.CI"], "l2_name": ["农产品加工"],
        "l3_code": ["CI005006.CI"], "l3_name": ["粮油加工"],
        "ts_code": ["000876.SZ"], "name": ["新 希 望"],
        "in_date": ["20200101"], "out_date": [None], "is_new": ["Y"],
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


def _index_daily_partition(trade_date: str, ts_codes: list[str] | None = None) -> pd.DataFrame:
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
    DailyPartitionStore(tmp_path / "index" / "index_daily").write("20240102", _index_daily_partition("20240102"))
    DailyPartitionStore(tmp_path / "index" / "index_daily").write("20240103", _index_daily_partition("20240103"))

    pro = LocalPro(tmp_path)
    result = pro.index_daily(start_date="20240102", end_date="20240103")

    assert len(result) == 4  # 2 dates × 2 codes


def test_index_daily_filters_by_ts_code(tmp_path):
    DailyPartitionStore(tmp_path / "index" / "index_daily").write("20240102", _index_daily_partition("20240102"))

    pro = LocalPro(tmp_path)
    result = pro.index_daily(ts_code="000300.SH", trade_date="20240102")

    assert len(result) == 1
    assert result.iloc[0]["ts_code"] == "000300.SH"


def test_index_daily_filters_by_trade_date(tmp_path):
    DailyPartitionStore(tmp_path / "index" / "index_daily").write("20240102", _index_daily_partition("20240102"))
    DailyPartitionStore(tmp_path / "index" / "index_daily").write("20240103", _index_daily_partition("20240103"))

    pro = LocalPro(tmp_path)
    result = pro.index_daily(trade_date="20240102")

    assert len(result) == 2
    assert all(result["trade_date"] == "20240102")


def test_index_daily_filters_by_date_range(tmp_path):
    DailyPartitionStore(tmp_path / "index" / "index_daily").write("20240102", _index_daily_partition("20240102"))
    DailyPartitionStore(tmp_path / "index" / "index_daily").write("20240103", _index_daily_partition("20240103"))
    DailyPartitionStore(tmp_path / "index" / "index_daily").write("20240104", _index_daily_partition("20240104"))

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
    DailyPartitionStore(tmp_path / "index" / "index_daily").write("20240102", _index_daily_partition("20240102"))

    pro = LocalPro(tmp_path)
    result = pro.index_daily(
        ts_code="000300.SH",
        trade_date="20240102",
        fields="ts_code,trade_date,close",
    )

    assert list(result.columns) == ["ts_code", "trade_date", "close"]


def test_index_daily_in_query_dispatch(tmp_path):
    DailyPartitionStore(tmp_path / "index" / "index_daily").write("20240102", _index_daily_partition("20240102"))

    pro = LocalPro(tmp_path)
    result = pro.query("index_daily", ts_code="000300.SH", trade_date="20240102")

    assert len(result) == 1


# --- Futures API tests ---

from zer0share.storage import DailyPartitionStore
from zer0share.sources.tushare import (
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
    DailyPartitionStore(futures_dir / "fut_daily").write(trade_date, df)
    return df


@pytest.fixture
def futures_api(tmp_path):
    return LocalPro(tmp_path)


def test_fut_basic_query_returns_data(tmp_path, futures_api):
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
    fut_basic_dir = tmp_path / "futures" / "fut_basic"
    fut_basic_dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(fut_basic_dir / "data.parquet", index=False)

    result = futures_api.fut_basic()
    assert len(result) == 2
    assert set(result["ts_code"]) == {"CU2401.SHF", "AG2401.SHF"}


def test_fut_basic_query_filters_by_exchange(tmp_path, futures_api):
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
    fut_basic_dir = tmp_path / "futures" / "fut_basic"
    fut_basic_dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(fut_basic_dir / "data.parquet", index=False)

    result = futures_api.fut_basic(exchange="DCE")
    assert len(result) == 1
    assert result.iloc[0]["ts_code"] == "A2505.DCE"


def test_fut_daily_query_returns_data(tmp_path):
    api = LocalPro(tmp_path)
    _write_fut_daily_data(tmp_path, "20240102")

    result = api.fut_daily(trade_date="20240102")
    assert len(result) == 2


def test_fut_daily_query_filters_by_ts_code(tmp_path):
    api = LocalPro(tmp_path)
    _write_fut_daily_data(tmp_path, "20240102")

    result = api.fut_daily(ts_code="CU2401.SHF", trade_date="20240102")
    assert len(result) == 1
    assert result.iloc[0]["ts_code"] == "CU2401.SHF"


def test_fut_daily_query_raises_when_no_data(tmp_path):
    api = LocalPro(tmp_path)
    with pytest.raises(FileNotFoundError):
        api.fut_daily(trade_date="20240102")


def test_fut_holding_query_returns_data(tmp_path):
    api = LocalPro(tmp_path)
    df = pd.DataFrame({
        "trade_date": ["20240102", "20240102"],
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
    DailyPartitionStore(tmp_path / "futures" / "fut_holding").write("20240102", df)

    result = api.fut_holding(trade_date="20240102")
    assert len(result) == 2


def test_fut_mapping_query_returns_data(tmp_path):
    api = LocalPro(tmp_path)
    df = pd.DataFrame({
        "ts_code": ["CU.SHF", "AG.SHF"],
        "trade_date": ["20240102", "20240102"],
        "mapping_ts_code": ["CU2401.SHF", "AG2401.SHF"],
    })
    DailyPartitionStore(tmp_path / "futures" / "fut_mapping").write("20240102", df)

    result = api.fut_mapping(trade_date="20240102")
    assert len(result) == 2


def test_query_dispatch_supports_futures(tmp_path):
    api = LocalPro(tmp_path)
    _write_fut_daily_data(tmp_path, "20240102")

    result = api.query("fut_daily", trade_date="20240102")
    assert len(result) == 2


# --- Futures batch 2 API tests ---


def test_ft_limit_query_returns_data(tmp_path):
    api = LocalPro(tmp_path)
    df = pd.DataFrame({
        "trade_date": ["20240102"],
        "ts_code": ["CU2401.SHF"], "name": ["沪铜2401"],
        "up_limit": [51000.0], "down_limit": [49000.0],
        "m_ratio": [0.10], "cont": ["CU"], "exchange": ["SHFE"],
    })
    DailyPartitionStore(tmp_path / "futures" / "ft_limit").write("20240102", df)

    result = api.ft_limit(trade_date="20240102")
    assert len(result) == 1


def test_fut_weekly_query_returns_data(tmp_path):
    api = LocalPro(tmp_path)
    df = pd.DataFrame({
        "ts_code": ["CU2401.SHF"], "trade_date": ["20240102"],
        "freq": ["week"], "open": [50000.0], "high": [50500.0],
        "low": [49900.0], "close": [50300.0], "pre_close": [50000.0],
        "settle": [50250.0], "pre_settle": [50100.0], "vol": [10000.0],
        "amount": [251250.0], "oi": [50000.0], "oi_chg": [500.0],
        "exchange": ["SHFE"], "change1": [200.0], "change2": [150.0],
    })
    DailyPartitionStore(tmp_path / "futures" / "fut_weekly").write("20240102", df)

    result = api.fut_weekly(trade_date="20240102")
    assert len(result) == 1


def test_fut_monthly_query_returns_data(tmp_path):
    api = LocalPro(tmp_path)
    df = pd.DataFrame({
        "ts_code": ["CU2401.SHF"], "trade_date": ["20240102"],
        "freq": ["month"], "open": [50000.0], "high": [50500.0],
        "low": [49900.0], "close": [50300.0], "pre_close": [50000.0],
        "settle": [50250.0], "pre_settle": [50100.0], "vol": [10000.0],
        "amount": [251250.0], "oi": [50000.0], "oi_chg": [500.0],
        "exchange": ["SHFE"], "change1": [200.0], "change2": [150.0],
    })
    DailyPartitionStore(tmp_path / "futures" / "fut_monthly").write("20240102", df)

    result = api.fut_monthly(trade_date="20240102")
    assert len(result) == 1


def test_fut_index_daily_query_returns_data(tmp_path):
    api = LocalPro(tmp_path)
    df = pd.DataFrame({
        "ts_code": ["NHAI.NH", "NHCI.NH"],
        "trade_date": ["20240102", "20240102"],
        "close": [1000.0, 800.0], "open": [998.0, 798.0],
        "high": [1005.0, 805.0], "low": [995.0, 795.0],
        "pre_close": [998.0, 798.0], "change": [2.0, 2.0],
        "pct_chg": [0.2, 0.25], "vol": [50000.0, 30000.0],
        "amount": [50000000.0, 24000000.0],
    })
    DailyPartitionStore(tmp_path / "futures" / "fut_index_daily").write("20240102", df)

    result = api.fut_index_daily(trade_date="20240102")
    assert len(result) == 2


def test_fut_weekly_detail_query_returns_data(tmp_path):
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
        "week": ["202401", "202401"], "week_date": ["20240101", "20240101"],
    })
    DailyPartitionStore(tmp_path / "futures" / "fut_weekly_detail").write("20240101", df)

    result = api.fut_weekly_detail(start_date="20240101", end_date="20240101")
    assert len(result) == 2


def test_batch2_query_dispatch(tmp_path):
    api = LocalPro(tmp_path)
    df = pd.DataFrame({
        "trade_date": ["20240102"], "ts_code": ["CU2401.SHF"],
        "name": ["沪铜2401"], "up_limit": [51000.0], "down_limit": [49000.0],
        "m_ratio": [0.10], "cont": ["CU"], "exchange": ["SHFE"],
    })
    DailyPartitionStore(tmp_path / "futures" / "ft_limit").write("20240102", df)

    result = api.query("ft_limit", trade_date="20240102")
    assert len(result) == 1


# --- Options API tests ---

from zer0share.sources.tushare import OPT_BASIC_COLS, OPT_DAILY_COLS


def _write_opt_daily_data(data_dir, trade_date, ts_codes=None):
    ts_codes = ts_codes or ["10004462.SH", "10004463.SH"]
    rows = []
    for ts_code in ts_codes:
        rows.append({
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
        })
    df = pd.DataFrame(rows)
    DailyPartitionStore(data_dir / "options" / "opt_daily").write(trade_date, df)
    return df


def _make_opt_basic_df():
    return pd.DataFrame({
        "ts_code": ["10004462.SH", "10004463.SH"],
        "symbol": ["10004462", "10004463"],
        "exchange": ["SSE", "SSE"],
        "name": ["50ETF购4月2700", "50ETF沽4月2700"],
        "per_unit": [10000.0, 10000.0],
        "opt_code": ["OP510050", "OP510050"],
        "opt_type": ["E", "E"],
        "call_put": ["C", "P"],
        "exercise_type": ["E", "E"],
        "exercise_price": [2.7, 2.7],
        "s_month": ["202404", "202404"],
        "maturity_date": ["20240424", "20240424"],
        "list_price": [2.5, 2.5],
        "list_date": ["20240101", "20240101"],
        "delist_date": ["20240424", "20240424"],
        "last_edate": ["20240424", "20240424"],
        "last_ddate": ["20240426", "20240426"],
        "quote_unit": [None, None],
        "min_price_chg": [None, None],
    })


def test_opt_basic_query_returns_data(tmp_path):
    SnapshotStore(tmp_path / "options" / "opt_basic" / "data.parquet").write(_make_opt_basic_df())

    api = LocalPro(tmp_path)
    result = api.opt_basic()
    assert len(result) == 2
    assert set(result["ts_code"]) == {"10004462.SH", "10004463.SH"}
    row = result[result["ts_code"] == "10004462.SH"].iloc[0]
    assert row["list_date"] == "20240101"
    assert row["maturity_date"] == "20240424"
    assert row["delist_date"] == "20240424"


def test_opt_basic_query_filters_by_call_put(tmp_path):
    SnapshotStore(tmp_path / "options" / "opt_basic" / "data.parquet").write(_make_opt_basic_df())

    api = LocalPro(tmp_path)
    result = api.opt_basic(call_put="C")
    assert len(result) == 1
    assert result.iloc[0]["ts_code"] == "10004462.SH"


def test_opt_basic_filters_by_name_and_list_date(tmp_path):
    SnapshotStore(tmp_path / "options" / "opt_basic" / "data.parquet").write(_make_opt_basic_df())

    api = LocalPro(tmp_path)
    result = api.opt_basic(
        name="50ETF购4月2700",
        list_date="20240101",
        fields="ts_code,name,list_date",
    )

    assert result.to_dict("records") == [
        {
            "ts_code": "10004462.SH",
            "name": "50ETF购4月2700",
            "list_date": "20240101",
        }
    ]

    empty_result = api.opt_basic(
        name="50ETF购4月2700",
        list_date="20240102",
        fields="ts_code,name,list_date",
    )

    assert empty_result.to_dict("records") == []


def test_opt_basic_supports_limit_and_offset(tmp_path):
    SnapshotStore(tmp_path / "options" / "opt_basic" / "data.parquet").write(_make_opt_basic_df())

    api = LocalPro(tmp_path)
    result = api.opt_basic(limit=1, offset=1, fields="ts_code")

    assert result.to_dict("records") == [
        {"ts_code": "10004463.SH"}
    ]


def test_opt_basic_combines_new_filters_with_fields(tmp_path):
    SnapshotStore(tmp_path / "options" / "opt_basic" / "data.parquet").write(_make_opt_basic_df())

    api = LocalPro(tmp_path)
    result = api.opt_basic(
        exchange="SSE",
        call_put="P",
        list_date="20240101",
        limit=1,
        fields=["ts_code", "call_put", "list_date"],
    )

    assert result.to_dict("records") == [
        {
            "ts_code": "10004463.SH",
            "call_put": "P",
            "list_date": "20240101",
        }
    ]


def test_opt_basic_query_raises_when_no_data(tmp_path):
    api = LocalPro(tmp_path)
    with pytest.raises(FileNotFoundError):
        api.opt_basic()


def test_opt_daily_query_returns_data(tmp_path):
    _write_opt_daily_data(tmp_path, "20240102")

    api = LocalPro(tmp_path)
    result = api.opt_daily(trade_date="20240102")
    assert len(result) == 2
    assert result.iloc[0]["trade_date"] == "20240102"


def test_opt_daily_query_filters_by_ts_code(tmp_path):
    _write_opt_daily_data(tmp_path, "20240102")

    api = LocalPro(tmp_path)
    result = api.opt_daily(ts_code="10004462.SH", trade_date="20240102")
    assert len(result) == 1
    assert result.iloc[0]["ts_code"] == "10004462.SH"


def test_opt_daily_query_raises_when_no_data(tmp_path):
    api = LocalPro(tmp_path)
    with pytest.raises(FileNotFoundError):
        api.opt_daily(trade_date="20240102")


def test_opt_daily_query_filters_by_exchange(tmp_path):
    rows = []
    for ts_code in ["10004462.SH", "10004463.SH"]:
        rows.append({
            "ts_code": ts_code,
            "trade_date": "20240102",
            "exchange": "SSE",
            "pre_settle": 0.15, "pre_close": 0.148,
            "open": 0.152, "high": 0.16, "low": 0.148,
            "close": 0.155, "settle": 0.154,
            "vol": 5000.0, "amount": 7700000.0, "oi": 20000.0,
        })
    rows.append({
        "ts_code": "90000001.SZ",
        "trade_date": "20240102",
        "exchange": "SZSE",
        "pre_settle": 0.2, "pre_close": 0.19,
        "open": 0.21, "high": 0.22, "low": 0.19,
        "close": 0.205, "settle": 0.204,
        "vol": 3000.0, "amount": 6120000.0, "oi": 10000.0,
    })
    DailyPartitionStore(tmp_path / "options" / "opt_daily").write("20240102", pd.DataFrame(rows))

    api = LocalPro(tmp_path)
    result = api.opt_daily(trade_date="20240102", exchange="SSE")
    assert len(result) == 2
    assert all(result["exchange"] == "SSE")


def test_opt_daily_supports_limit_and_offset(tmp_path):
    _write_opt_daily_data(
        tmp_path,
        "20240102",
        ts_codes=["10004462.SH", "10004463.SH", "10004464.SH"],
    )

    api = LocalPro(tmp_path)
    result = api.opt_daily(trade_date="20240102", limit=1, offset=1, fields="ts_code,trade_date")

    assert result.to_dict("records") == [
        {"ts_code": "10004463.SH", "trade_date": "20240102"}
    ]


def test_query_dispatch_supports_options(tmp_path):
    _write_opt_daily_data(tmp_path, "20240102")

    api = LocalPro(tmp_path)
    result = api.query("opt_daily", trade_date="20240102")
    assert len(result) == 2


def test_query_dispatch_supports_opt_basic(tmp_path):
    SnapshotStore(tmp_path / "options" / "opt_basic" / "data.parquet").write(_make_opt_basic_df().head(1))

    api = LocalPro(tmp_path)
    result = api.query("opt_basic")
    assert len(result) == 1
    assert result.iloc[0]["ts_code"] == "10004462.SH"


def test_pro_api_query_does_not_dispatch_ricequant_table(tmp_path):
    pro = LocalPro(tmp_path)
    with pytest.raises(ValueError, match="unknown api"):
        pro.query("ricequant_stock_minute")


def test_idx_anns_query_returns_data(tmp_path):
    data = _make_idx_anns_df()
    DailyPartitionStore(tmp_path / "index" / "idx_anns").write("20260416", data[data["ann_date"] == "20260416"])
    DailyPartitionStore(tmp_path / "index" / "idx_anns").write("20260417", data[data["ann_date"] == "20260417"])
    DailyPartitionStore(tmp_path / "index" / "idx_anns").write("20260420", data[data["ann_date"] == "20260420"])

    api = LocalPro(tmp_path)
    result = api.idx_anns(ann_date="20260416", fields="ann_date,title,type")

    assert result.to_dict("records") == [
        {"ann_date": "20260416", "title": "关于发布中证交易所AAA综合债指数系列的公告", "type": "新指数发布"},
        {"ann_date": "20260416", "title": "关于调整三板指数样本股的公告", "type": "指数调样"},
    ]


def test_idx_anns_filters_by_range_src_limit_offset_and_query_dispatch(tmp_path):
    data = _make_idx_anns_df()
    DailyPartitionStore(tmp_path / "index" / "idx_anns").write("20260416", data[data["ann_date"] == "20260416"])
    DailyPartitionStore(tmp_path / "index" / "idx_anns").write("20260417", data[data["ann_date"] == "20260417"])
    DailyPartitionStore(tmp_path / "index" / "idx_anns").write("20260420", data[data["ann_date"] == "20260420"])

    api = LocalPro(tmp_path)
    result = api.query(
        "idx_anns",
        src="中证指数",
        start_date="20260401",
        end_date="20260430",
        fields="ann_date,title,source",
        limit=1,
        offset=1,
    )

    assert result.to_dict("records") == [
        {"ann_date": "20260416", "title": "关于调整三板指数样本股的公告", "source": "中证指数"}
    ]


def test_idx_anns_rejects_ann_date_with_range(tmp_path):
    data = _make_idx_anns_df()
    DailyPartitionStore(tmp_path / "index" / "idx_anns").write("20260416", data[data["ann_date"] == "20260416"])

    api = LocalPro(tmp_path)
    with pytest.raises(ValueError, match="trade_date cannot be combined"):
        api.idx_anns(ann_date="20260416", start_date="20260401")


def test_idx_anns_validates_date_filters(tmp_path):
    api = LocalPro(tmp_path)

    with pytest.raises(ValueError, match="YYYYMMDD"):
        api.idx_anns(ann_date="2026-04-16")


def test_idx_anns_query_raises_when_no_data(tmp_path):
    api = LocalPro(tmp_path)

    with pytest.raises(FileNotFoundError, match="idx_anns"):
        api.idx_anns(ann_date="20260416")


def _make_holdertrade_df():
    return pd.DataFrame(
        {
            "ts_code": ["600519.SH", "300170.SZ", "300170.SZ", "000407.SZ"],
            "ann_date": ["20250902", "20250915", "20250915", "20250915"],
            "holder_name": ["中国贵州茅台酒厂(集团)有限责任公司", "陈迪清", "范建震", "阳光人寿"],
            "holder_type": ["C", "G", "P", "C"],
            "in_de": ["IN", "IN", "DE", "DE"],
            "change_vol": [67821.0, 2766512.0, 2766512.0, 2464500.0],
            "change_ratio": [0.0054, 0.284, 0.284, 0.2814],
            "after_share": [679279397.0, 14787149.0, None, 41539641.0],
            "after_ratio": [54.2438, 1.518, None, 4.7431],
            "avg_price": [None, 17.67, 17.67, 3.6555],
            "total_share": [679279397.0, 50849061.0, None, 41539641.0],
        }
    )


def test_stk_holdertrade_filters_by_code_type_direction_and_dispatch(tmp_path):
    data = _make_holdertrade_df()
    store = DailyPartitionStore(tmp_path / "stock" / "stk_holdertrade")
    for ann_date, part in data.groupby("ann_date"):
        store.write(ann_date, part)

    api = LocalPro(tmp_path)
    increases = api.stk_holdertrade(
        start_date="20250901", end_date="20250930", trade_type="IN", fields="ts_code,ann_date,holder_name"
    )
    assert increases.to_dict("records") == [
        {"ts_code": "300170.SZ", "ann_date": "20250915", "holder_name": "陈迪清"},
        {"ts_code": "600519.SH", "ann_date": "20250902", "holder_name": "中国贵州茅台酒厂(集团)有限责任公司"},
    ]

    companies = api.query(
        "stk_holdertrade", ts_code="000407.SZ,600519.SH", start_date="20250901", holder_type="C",
        fields="ts_code,in_de",
    )
    assert sorted(companies["ts_code"]) == ["000407.SZ", "600519.SH"]

    one_day = api.stk_holdertrade(ann_date="20250915", fields="holder_name")
    assert len(one_day) == 3
