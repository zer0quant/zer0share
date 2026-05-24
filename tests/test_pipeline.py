from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from zer0share.pipeline import Pipeline, EXCHANGES
from zer0share.storage import read_sw_classify, read_sw_member, read_ci_member, write_basic, write_trade_cal
from zer0share.fetcher import INDEX_DAILY_CODES
from zer0share.storage import daily_partition_exists


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


@pytest.fixture
def cfg(tmp_path):
    c = MagicMock()
    c.data_dir = tmp_path
    c.db_path = tmp_path / "meta.duckdb"
    return c


@pytest.fixture
def pipeline(cfg):
    fetcher = MagicMock()
    notifier = MagicMock()
    return Pipeline(cfg, fetcher, notifier)


def test_sync_basic_first_run_writes_parquet(pipeline, cfg):
    pipeline._fetcher.fetch_basic.return_value = _basic_df()
    pipeline.sync_basic()
    assert (cfg.data_dir / "basic" / "data.parquet").exists()


def test_sync_basic_refreshes_even_if_recently_updated(pipeline):
    pipeline._fetcher.fetch_basic.return_value = _basic_df()
    pipeline.sync_basic()
    pipeline._fetcher.fetch_basic.reset_mock()
    pipeline.sync_basic()
    pipeline._fetcher.fetch_basic.assert_called_once()


def _setup_trade_cal_sse(pipeline, cfg) -> None:
    """Load SSE trade_cal with 2024-01-02 as open into DuckDB."""
    trade_cal = pd.DataFrame({
        "exchange": ["SSE"],
        "cal_date": [date(2024, 1, 2)],
        "is_open": [True],
        "pretrade_date": [date(2023, 12, 29)],
    })
    write_trade_cal(cfg.data_dir, "SSE", trade_cal)
    pipeline._meta.load_trade_cal_from_parquet(cfg.data_dir)
    pipeline._meta.update_last_date("trade_cal", date(2024, 1, 2))


def test_sync_daily_kline_writes_parquet(pipeline, cfg):
    write_basic(cfg.data_dir, _basic_df())
    _setup_trade_cal_sse(pipeline, cfg)
    kline_df = pd.DataFrame(
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
    pipeline._fetcher.fetch_daily_kline.return_value = kline_df
    pipeline._meta.update_last_date("daily_kline", date(2024, 1, 1))

    with patch("zer0share.pipeline.date") as mock_date:
        mock_date.today.return_value = date(2024, 1, 2)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        pipeline.sync_daily_kline()

    assert (cfg.data_dir / "daily_kline" / "date=20240102" / "data.parquet").exists()


def test_sync_daily_kline_skips_empty_dates(pipeline, cfg):
    write_basic(cfg.data_dir, _basic_df())
    _setup_trade_cal_sse(pipeline, cfg)
    pipeline._fetcher.fetch_daily_kline.return_value = pd.DataFrame()
    pipeline._meta.update_last_date("daily_kline", date(2024, 1, 1))

    with patch("zer0share.pipeline.date") as mock_date:
        mock_date.today.return_value = date(2024, 1, 2)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        pipeline.sync_daily_kline()

    assert not (cfg.data_dir / "daily_kline" / "date=20240102" / "data.parquet").exists()


def test_sync_daily_kline_sends_completion_notification(pipeline, cfg):
    write_basic(cfg.data_dir, _basic_df())
    _setup_trade_cal_sse(pipeline, cfg)
    kline_df = pd.DataFrame(
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
    pipeline._fetcher.fetch_daily_kline.return_value = kline_df
    pipeline._meta.update_last_date("daily_kline", date(2024, 1, 1))

    with patch("zer0share.pipeline.date") as mock_date:
        mock_date.today.return_value = date(2024, 1, 2)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        pipeline.sync_daily_kline()

    pipeline._notifier.send.assert_called_once()
    msg = pipeline._notifier.send.call_args[0][0]
    assert "成功" in msg


def test_sync_daily_kline_already_up_to_date(pipeline, cfg):
    write_basic(cfg.data_dir, _basic_df())
    today = date.today()
    pipeline._meta.update_last_date("daily_kline", today)
    pipeline.sync_daily_kline()
    pipeline._fetcher.fetch_daily_kline.assert_not_called()


def test_sync_basic_failure_sends_alert_and_raises(pipeline):
    pipeline._fetcher.fetch_basic.side_effect = RuntimeError("API error")
    with pytest.raises(RuntimeError):
        pipeline.sync_basic()
    pipeline._notifier.send.assert_called_once()
    msg = pipeline._notifier.send.call_args[0][0]
    assert "basic 同步失败" in msg


def test_sync_daily_kline_failure_sends_alert_and_raises(pipeline, cfg):
    write_basic(cfg.data_dir, _basic_df())
    _setup_trade_cal_sse(pipeline, cfg)
    pipeline._fetcher.fetch_daily_kline.side_effect = RuntimeError("API error")
    pipeline._meta.update_last_date("daily_kline", date(2024, 1, 1))

    with patch("zer0share.pipeline.date") as mock_date:
        mock_date.today.return_value = date(2024, 1, 2)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        with pytest.raises(RuntimeError):
            pipeline.sync_daily_kline()

    pipeline._notifier.send.assert_called_once()
    msg = pipeline._notifier.send.call_args[0][0]
    assert "同步失败" in msg


def test_pipeline_context_manager(cfg):
    fetcher = MagicMock()
    notifier = MagicMock()
    with Pipeline(cfg, fetcher, notifier) as p:
        assert p is not None





def _trade_cal_df(exchange: str) -> pd.DataFrame:
    return pd.DataFrame({
        "exchange": [exchange] * 3,
        "cal_date": [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)],
        "is_open": [True, False, True],
        "pretrade_date": [date(2023, 12, 29), date(2024, 1, 2), date(2024, 1, 2)],
    })


def test_sync_trade_cal_writes_all_exchanges(pipeline, cfg):
    pipeline._fetcher.fetch_trade_cal.return_value = _trade_cal_df("SSE")
    pipeline.sync_trade_cal()
    for ex in EXCHANGES:
        assert (cfg.data_dir / "trade_cal" / f"exchange={ex}" / "data.parquet").exists()


def test_sync_trade_cal_loads_to_duckdb(pipeline, cfg):
    pipeline._fetcher.fetch_trade_cal.return_value = _trade_cal_df("SSE")
    pipeline.sync_trade_cal()
    days = pipeline._meta.get_trading_days("SSE", date(2024, 1, 1), date(2024, 1, 5))
    assert date(2024, 1, 2) in days
    assert date(2024, 1, 3) not in days


def test_sync_trade_cal_updates_meta(pipeline, cfg):
    pipeline._fetcher.fetch_trade_cal.return_value = _trade_cal_df("SSE")
    pipeline.sync_trade_cal()
    assert pipeline._meta.get_last_date("trade_cal") is not None


def test_sync_trade_cal_uses_incremental_range(pipeline, cfg):
    write_trade_cal(
        cfg.data_dir,
        "SSE",
        pd.DataFrame({
            "exchange": ["SSE", "SSE"],
            "cal_date": [date(2024, 1, 1), date(2024, 1, 2)],
            "is_open": [True, True],
            "pretrade_date": [date(2023, 12, 29), date(2024, 1, 1)],
        }),
    )
    write_trade_cal(
        cfg.data_dir,
        "SZSE",
        pd.DataFrame({
            "exchange": ["SZSE"],
            "cal_date": [date(2024, 1, 3)],
            "is_open": [True],
            "pretrade_date": [date(2024, 1, 2)],
        }),
    )

    def fetch_trade_cal(exchange, start, end):
        return pd.DataFrame({
            "exchange": [exchange],
            "cal_date": [start],
            "is_open": [True],
            "pretrade_date": [date(2024, 1, 2)],
        })

    pipeline._fetcher.fetch_trade_cal.side_effect = fetch_trade_cal
    with patch("zer0share.pipeline.date") as mock_date:
        mock_date.today.return_value = date(2024, 5, 18)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        pipeline.sync_trade_cal()

    pipeline._fetcher.fetch_trade_cal.assert_any_call(
        "SSE", date(2024, 1, 3), date(2024, 12, 31)
    )
    pipeline._fetcher.fetch_trade_cal.assert_any_call(
        "SZSE", date(2024, 1, 4), date(2024, 12, 31)
    )
    assert pipeline._meta.get_last_date("trade_cal") == date(2024, 1, 3)


def test_sync_trade_cal_skips_when_already_covers_year_end(pipeline, cfg):
    for exchange in EXCHANGES:
        write_trade_cal(
            cfg.data_dir,
            exchange,
            pd.DataFrame({
                "exchange": [exchange],
                "cal_date": [date(2024, 12, 31)],
                "is_open": [False],
                "pretrade_date": [date(2024, 12, 30)],
            }),
        )

    with patch("zer0share.pipeline.date") as mock_date:
        mock_date.today.return_value = date(2024, 5, 18)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        pipeline.sync_trade_cal()

    pipeline._fetcher.fetch_trade_cal.assert_not_called()
    assert pipeline._meta.get_last_date("trade_cal") == date(2024, 12, 31)


def test_sync_trade_cal_failure_sends_alert(pipeline, cfg):
    pipeline._fetcher.fetch_trade_cal.side_effect = RuntimeError("API error")
    with pytest.raises(RuntimeError):
        pipeline.sync_trade_cal()
    pipeline._notifier.send.assert_called_once()


def test_sync_daily_kline_uses_trading_calendar(pipeline, cfg):
    write_trade_cal(cfg.data_dir, "SSE", _trade_cal_df("SSE"))
    pipeline._meta.load_trade_cal_from_parquet(cfg.data_dir)

    kline_df = pd.DataFrame({
        "ts_code": ["000001.SZ"], "trade_date": [date(2024, 1, 2)],
        "open": [10.0], "high": [11.0], "low": [9.5], "close": [10.5],
        "pre_close": [10.0], "change": [0.5], "pct_chg": [5.0],
        "vol": [100000.0], "amount": [1050000.0],
    })
    pipeline._fetcher.fetch_daily_kline.return_value = kline_df
    pipeline._meta.update_last_date("daily_kline", date(2024, 1, 1))

    with patch("zer0share.pipeline.date") as mock_date:
        mock_date.today.return_value = date(2024, 1, 4)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        pipeline.sync_daily_kline()

    # 只调用 2 次（1/2 和 1/4），不调用 1/3（休市）
    assert pipeline._fetcher.fetch_daily_kline.call_count == 2


def test_sync_daily_kline_raises_if_no_trade_cal(pipeline, cfg):
    pipeline._meta.update_last_date("daily_kline", date(2024, 1, 1))
    with patch("zer0share.pipeline.date") as mock_date:
        mock_date.today.return_value = date(2024, 1, 4)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        with pytest.raises(RuntimeError, match="trade_cal"):
            pipeline.sync_daily_kline()


def test_sync_daily_kline_range_skips_existing_partitions(pipeline, cfg):
    write_basic(cfg.data_dir, _basic_df())
    trade_cal = pd.DataFrame(
        {
            "exchange": ["SSE", "SSE", "SSE"],
            "cal_date": [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)],
            "is_open": [True, True, True],
            "pretrade_date": [date(2023, 12, 29), date(2024, 1, 2), date(2024, 1, 3)],
        }
    )
    write_trade_cal(cfg.data_dir, "SSE", trade_cal)
    pipeline._meta.load_trade_cal_from_parquet(cfg.data_dir)

    existing_df = pd.DataFrame(
        {
            "ts_code": ["000001.SZ"],
            "trade_date": [date(2024, 1, 3)],
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
    from zer0share.storage import write_daily_kline

    write_daily_kline(cfg.data_dir, date(2024, 1, 3), existing_df)

    pipeline._fetcher.fetch_daily_kline.side_effect = [
        existing_df.assign(trade_date=[date(2024, 1, 2)]),
        existing_df.assign(trade_date=[date(2024, 1, 4)]),
    ]

    pipeline.sync_daily_kline(start_date=date(2024, 1, 2), end_date=date(2024, 1, 4))

    called_dates = [call.args[0] for call in pipeline._fetcher.fetch_daily_kline.call_args_list]
    assert called_dates == [date(2024, 1, 2), date(2024, 1, 4)]


def test_sync_daily_kline_old_range_does_not_rewind_meta(pipeline, cfg):
    write_basic(cfg.data_dir, _basic_df())
    trade_cal = pd.DataFrame(
        {
            "exchange": ["SSE", "SSE"],
            "cal_date": [date(2024, 1, 2), date(2024, 1, 3)],
            "is_open": [True, True],
            "pretrade_date": [date(2023, 12, 29), date(2024, 1, 2)],
        }
    )
    write_trade_cal(cfg.data_dir, "SSE", trade_cal)
    pipeline._meta.load_trade_cal_from_parquet(cfg.data_dir)
    pipeline._meta.update_last_date("daily_kline", date(2024, 2, 1))

    kline_df = pd.DataFrame(
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
    pipeline._fetcher.fetch_daily_kline.return_value = kline_df

    pipeline.sync_daily_kline(start_date=date(2024, 1, 2), end_date=date(2024, 1, 3))

    assert pipeline._meta.get_last_date("daily_kline") == date(2024, 2, 1)


def test_sync_daily_kline_range_defaults_end_date_to_today(pipeline, cfg):
    write_basic(cfg.data_dir, _basic_df())
    trade_cal = pd.DataFrame(
        {
            "exchange": ["SSE", "SSE"],
            "cal_date": [date(2024, 1, 2), date(2024, 1, 3)],
            "is_open": [True, True],
            "pretrade_date": [date(2023, 12, 29), date(2024, 1, 2)],
        }
    )
    write_trade_cal(cfg.data_dir, "SSE", trade_cal)
    pipeline._meta.load_trade_cal_from_parquet(cfg.data_dir)
    pipeline._fetcher.fetch_daily_kline.return_value = pd.DataFrame(
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

    with patch("zer0share.pipeline.date") as mock_date:
        mock_date.today.return_value = date(2024, 1, 3)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        pipeline.sync_daily_kline(start_date=date(2024, 1, 2))

    called_dates = [call.args[0] for call in pipeline._fetcher.fetch_daily_kline.call_args_list]
    assert called_dates == [date(2024, 1, 2), date(2024, 1, 3)]


def test_sync_daily_kline_sleeps_between_requests(pipeline, cfg):
    write_basic(cfg.data_dir, _basic_df())
    trade_cal = pd.DataFrame(
        {
            "exchange": ["SSE", "SSE"],
            "cal_date": [date(2024, 1, 2), date(2024, 1, 3)],
            "is_open": [True, True],
            "pretrade_date": [date(2023, 12, 29), date(2024, 1, 2)],
        }
    )
    write_trade_cal(cfg.data_dir, "SSE", trade_cal)
    pipeline._meta.load_trade_cal_from_parquet(cfg.data_dir)
    pipeline._fetcher.fetch_daily_kline.return_value = pd.DataFrame(
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

    with patch("zer0share.pipeline.time.sleep") as mock_sleep:
        pipeline.sync_daily_kline(start_date=date(2024, 1, 2), end_date=date(2024, 1, 3))

    assert mock_sleep.call_count == 2
    mock_sleep.assert_any_call(0.2)


def test_sync_daily_partitioned_logs_progress(pipeline, cfg):
    days = [date(2024, 1, 1) + timedelta(days=i) for i in range(51)]
    trade_cal = pd.DataFrame(
        {
            "exchange": ["SSE"] * len(days),
            "cal_date": days,
            "is_open": [True] * len(days),
            "pretrade_date": [None] * len(days),
        }
    )
    write_trade_cal(cfg.data_dir, "SSE", trade_cal)
    pipeline._meta.load_trade_cal_from_parquet(cfg.data_dir)
    pipeline._fetcher.fetch_stock_st.return_value = pd.DataFrame(
        {
            "ts_code": ["000001.SZ"],
            "name": ["ST测试"],
            "trade_date": [days[0]],
            "type": ["S"],
            "type_name": ["ST"],
        }
    )

    with (
        patch("zer0share.pipeline.time.sleep"),
        patch("zer0share.pipeline.logger.info") as mock_info,
    ):
        pipeline.sync_stock_st(start_date=days[0], end_date=days[-1])

    messages = [call.args[0] for call in mock_info.call_args_list]
    assert any("stock_st 同步开始" in message for message in messages)
    assert any("stock_st 同步进度: 50/51 (98.0%)" in message for message in messages)
    assert any("stock_st 同步进度: 51/51 (100.0%)" in message for message in messages)


def test_sync_index_weight_fetches_monthly_ranges(pipeline, cfg):
    def fetch_index_weight(index_code, start, end):
        return pd.DataFrame(
            {
                "index_code": [index_code],
                "con_code": ["000001.SZ"],
                "trade_date": [end],
                "weight": [1.0],
            }
        )

    pipeline._fetcher.fetch_index_weight.side_effect = fetch_index_weight

    with (
        patch("zer0share.pipeline.INDEX_CODES", ["399300.SZ"]),
        patch("zer0share.pipeline.time.sleep"),
    ):
        pipeline.sync_index_weight(
            start_date=date(2024, 1, 15),
            end_date=date(2024, 2, 10),
        )

    called_ranges = [
        (call.args[1], call.args[2])
        for call in pipeline._fetcher.fetch_index_weight.call_args_list
    ]
    assert called_ranges == [
        (date(2024, 1, 15), date(2024, 1, 31)),
        (date(2024, 2, 1), date(2024, 2, 10)),
    ]
    assert pipeline._meta.get_last_date("index_weight:399300.SZ") == date(2024, 2, 10)
    assert pipeline._meta.get_last_date("index_weight") == date(2024, 2, 10)


def test_sync_index_weight_does_not_use_global_meta_for_new_index_meta(pipeline):
    pipeline._meta.update_last_date("index_weight", date(2024, 1, 31))
    pipeline._fetcher.fetch_index_weight.return_value = pd.DataFrame()

    with (
        patch("zer0share.pipeline.INDEX_CODES", ["399300.SZ"]),
        patch("zer0share.pipeline.FIRST_DATE", date(2024, 1, 1)),
        patch("zer0share.pipeline.date") as mock_date,
        patch("zer0share.pipeline.time.sleep"),
    ):
        mock_date.today.return_value = date(2024, 1, 31)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        pipeline.sync_index_weight()

    pipeline._fetcher.fetch_index_weight.assert_called_once_with(
        "399300.SZ", date(2024, 1, 1), date(2024, 1, 31)
    )
    assert pipeline._meta.get_last_date("index_weight:399300.SZ") == date(2024, 1, 31)


def test_sync_industry_writes_sw_classify_and_member(pipeline, cfg):
    classify_df = pd.DataFrame({
        "index_code": ["801010.SI"],
        "industry_name": ["农林牧渔"],
        "level": ["L1"],
        "parent_code": ["0"],
        "industry_code": ["110000"],
        "is_pub": ["1"],
        "src": ["SW2021"],
    })
    member_df = pd.DataFrame({
        "l1_code": ["801010.SI"], "l1_name": ["农林牧渔"],
        "l2_code": ["801016.SI"], "l2_name": ["种植业"],
        "l3_code": ["850111.SI"], "l3_name": ["种子"],
        "ts_code": ["002041.SZ"], "name": ["登海种业"],
        "in_date": [date(2021, 12, 13)], "out_date": [None], "is_new": ["Y"],
    })
    pipeline._fetcher.fetch_sw_classify.return_value = classify_df
    pipeline._fetcher.fetch_sw_member.return_value = member_df

    with patch("zer0share.pipeline.date") as mock_date:
        mock_date.today.return_value = date(2024, 5, 18)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        pipeline.sync_industry()

    assert read_sw_classify(cfg.data_dir).equals(classify_df)
    assert read_sw_member(cfg.data_dir).equals(member_df)
    assert pipeline._meta.get_last_date("sw_classify") == date(2024, 5, 18)
    assert pipeline._meta.get_last_date("sw_member") == date(2024, 5, 18)


def test_sync_industry_failure_sends_alert_and_raises(pipeline):
    pipeline._fetcher.fetch_sw_classify.side_effect = RuntimeError("API error")
    with pytest.raises(RuntimeError):
        pipeline.sync_industry()
    pipeline._notifier.send.assert_called_once()
    msg = pipeline._notifier.send.call_args[0][0]
    assert "industry 同步失败" in msg


def test_sync_ci_member_writes_parquet(pipeline, cfg):
    member_df = pd.DataFrame({
        "l1_code": ["CI005001.CI"], "l1_name": ["农林牧渔"],
        "l2_code": ["CI005005.CI"], "l2_name": ["农产品加工"],
        "l3_code": ["CI005006.CI"], "l3_name": ["粮油加工"],
        "ts_code": ["000876.SZ"], "name": ["新 希 望"],
        "in_date": [date(2020, 1, 1)], "out_date": [None], "is_new": ["Y"],
    })
    pipeline._fetcher.fetch_ci_member.return_value = member_df

    with patch("zer0share.pipeline.date") as mock_date:
        mock_date.today.return_value = date(2024, 5, 18)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        pipeline.sync_ci_member()

    assert read_ci_member(cfg.data_dir).equals(member_df)
    assert pipeline._meta.get_last_date("ci_member") == date(2024, 5, 18)


def test_sync_ci_member_failure_sends_alert_and_raises(pipeline):
    pipeline._fetcher.fetch_ci_member.side_effect = RuntimeError("API error")
    with pytest.raises(RuntimeError):
        pipeline.sync_ci_member()
    pipeline._notifier.send.assert_called_once()
    msg = pipeline._notifier.send.call_args[0][0]
    assert "ci_member 同步失败" in msg


def _index_daily_df(ts_code: str = "000300.SH", trade_date: date = date(2024, 1, 2)) -> pd.DataFrame:
    return pd.DataFrame({
        "ts_code": [ts_code],
        "trade_date": [trade_date],
        "open": [3500.0],
        "high": [3550.0],
        "low": [3480.0],
        "close": [3520.0],
        "pre_close": [3490.0],
        "change": [30.0],
        "pct_chg": [0.86],
        "vol": [50000000.0],
        "amount": [1750000000.0],
    })


def test_sync_index_daily_fetches_all_codes(pipeline, cfg):
    pipeline._fetcher.fetch_index_daily.return_value = pd.DataFrame()

    with patch("zer0share.pipeline.date") as mock_date:
        mock_date.today.return_value = date(2024, 1, 2)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        pipeline.sync_index_daily()

    assert pipeline._fetcher.fetch_index_daily.call_count == len(INDEX_DAILY_CODES)
    for ts_code in INDEX_DAILY_CODES:
        pipeline._fetcher.fetch_index_daily.assert_any_call(
            ts_code,
            date(2016, 1, 1),
            date(2024, 1, 2),
        )


def test_sync_index_daily_writes_date_partitions(pipeline, cfg):
    pipeline._fetcher.fetch_index_daily.side_effect = [
        _index_daily_df(ts_code=ts_code, trade_date=date(2024, 1, 2))
        for ts_code in INDEX_DAILY_CODES
    ]

    with patch("zer0share.pipeline.date") as mock_date, \
         patch("zer0share.pipeline.time.sleep"):
        mock_date.today.return_value = date(2024, 1, 2)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        pipeline.sync_index_daily()

    assert daily_partition_exists(cfg.data_dir, "index_daily", date(2024, 1, 2))


def test_sync_index_daily_skips_existing_partitions(pipeline, cfg):
    from zer0share.storage import write_daily_partition
    existing = _index_daily_df(ts_code="000300.SH", trade_date=date(2024, 1, 2))
    write_daily_partition(cfg.data_dir, "index_daily", date(2024, 1, 2), existing)

    pipeline._fetcher.fetch_index_daily.side_effect = [
        _index_daily_df(ts_code=ts_code, trade_date=date(2024, 1, 2))
        for ts_code in INDEX_DAILY_CODES
    ]

    with patch("zer0share.pipeline.date") as mock_date, \
         patch("zer0share.pipeline.time.sleep"):
        mock_date.today.return_value = date(2024, 1, 2)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        pipeline.sync_index_daily()

    # Partition already existed — same file should still be there (written once, not twice)
    assert daily_partition_exists(cfg.data_dir, "index_daily", date(2024, 1, 2))


def test_sync_index_daily_up_to_date_skips_fetch(pipeline, cfg):
    pipeline._meta.update_last_date("index_daily", date(2024, 1, 2))

    with patch("zer0share.pipeline.date") as mock_date:
        mock_date.today.return_value = date(2024, 1, 2)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        pipeline.sync_index_daily()

    pipeline._fetcher.fetch_index_daily.assert_not_called()


def test_sync_index_daily_no_data_sends_notification(pipeline, cfg):
    pipeline._fetcher.fetch_index_daily.return_value = pd.DataFrame()

    with patch("zer0share.pipeline.date") as mock_date, \
         patch("zer0share.pipeline.time.sleep"):
        mock_date.today.return_value = date(2024, 1, 2)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        pipeline.sync_index_daily()

    pipeline._notifier.send.assert_called_once_with("index_daily 无数据，跳过")


def test_sync_index_daily_updates_metastore(pipeline, cfg):
    pipeline._fetcher.fetch_index_daily.side_effect = [
        _index_daily_df(ts_code=ts_code, trade_date=date(2024, 1, 2))
        for ts_code in INDEX_DAILY_CODES
    ]

    with patch("zer0share.pipeline.date") as mock_date, \
         patch("zer0share.pipeline.time.sleep"):
        mock_date.today.return_value = date(2024, 1, 2)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        pipeline.sync_index_daily()

    assert pipeline._meta.get_last_date("index_daily") == date(2024, 1, 2)
