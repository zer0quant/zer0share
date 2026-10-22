# -*- coding: utf-8 -*-
"""build_sector_fundflow 最小单测: 3 只股票 × 2 天合成数据验证聚合算术。

直接 `python test_sector_fundflow.py` 或 `pytest test_sector_fundflow.py` 均可。
"""
import os
import sys
import tempfile

import polars as pl

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'scripts'))
from build_sector_fundflow import build_sector_fundflow, atomic_write  # noqa: E402


def _make_fixtures(tmpdir: str) -> tuple[str, str, pl.DataFrame]:
    # 3 股票: 000001.SZ/000002.SZ 属行业A, 600000.SH 属行业B; 2 个交易日
    rows = [
        # ts_code, trade_date, buy_lg, sell_lg, buy_elg, sell_elg (万元)
        ('000001.SZ', '20240102', 100.0, 40.0, 50.0, 10.0),   # net=100
        ('000002.SZ', '20240102', 30.0, 20.0, 0.0, 10.0),     # net=0
        ('600000.SH', '20240102', 10.0, 100.0, 5.0, 0.0),     # net=-85
        ('000001.SZ', '20240103', 0.0, 0.0, 0.0, 0.0),        # net=0
        ('000002.SZ', '20240103', 60.0, 10.0, 20.0, 50.0),    # net=20
        ('600000.SH', '20240103', 7.0, 3.0, 0.0, 0.0),        # net=4
    ]
    mf = pl.DataFrame(
        rows, schema=['ts_code', 'trade_date', 'buy_lg_amount', 'sell_lg_amount',
                      'buy_elg_amount', 'sell_elg_amount'], orient='row')
    mf_path = os.path.join(tmpdir, 'moneyflow.parquet')
    mf.write_parquet(mf_path)

    # daily.amount 为千元口径: 行业A 两股 20240102 合计 10000千元 -> 1000万元
    daily = pl.DataFrame(
        [('000001.SZ', '20240102', 6000.0),
         ('000002.SZ', '20240102', 4000.0),
         ('600000.SH', '20240102', 20000.0),
         ('000001.SZ', '20240103', 5000.0),
         ('000002.SZ', '20240103', 5000.0),
         ('600000.SH', '20240103', 8000.0)],
        schema=['ts_code', 'trade_date', 'amount'], orient='row')
    daily_path = os.path.join(tmpdir, 'daily.parquet')
    daily.write_parquet(daily_path)

    ind_map = pl.DataFrame({'symbol': ['000001.SZ', '000002.SZ', '600000.SH'],
                            'industry_name': ['行业A', '行业A', '行业B']})
    return mf_path, daily_path, ind_map


def test_aggregation():
    with tempfile.TemporaryDirectory() as tmpdir:
        mf_path, daily_path, ind_map = _make_fixtures(tmpdir)
        out = build_sector_fundflow(mf_path, daily_path, ind_map).collect()

        assert out.columns == ['trade_date', 'industry_name', 'net_main',
                               'net_main_ratio', 'amt_sum', 'stock_n']
        assert out.height == 4  # 2 行业 × 2 天
        assert out['trade_date'].to_list() == ['20240102', '20240102',
                                               '20240103', '20240103']

        got = {(r['trade_date'], r['industry_name']): r
               for r in out.to_dicts()}
        # 20240102 行业A: net=100+0=100, amt=(6000+4000)/10=1000万, ratio=0.1, n=2
        r = got[('20240102', '行业A')]
        assert r['net_main'] == 100.0, r
        assert r['amt_sum'] == 1000.0, r
        assert abs(r['net_main_ratio'] - 0.1) < 1e-12, r
        assert r['stock_n'] == 2, r
        # 20240102 行业B: net=-85, amt=20000/10=2000万
        r = got[('20240102', '行业B')]
        assert r['net_main'] == -85.0, r
        assert r['amt_sum'] == 2000.0, r
        assert abs(r['net_main_ratio'] - (-85.0 / 2000.0)) < 1e-12, r
        assert r['stock_n'] == 1, r
        # 20240103 行业A: net=0+20=20, amt=(5000+5000)/10=1000万
        r = got[('20240103', '行业A')]
        assert r['net_main'] == 20.0, r
        assert r['amt_sum'] == 1000.0, r
        assert abs(r['net_main_ratio'] - 0.02) < 1e-12, r
        assert r['stock_n'] == 2, r
        # 20240103 行业B: net=4, amt=800万
        r = got[('20240103', '行业B')]
        assert r['net_main'] == 4.0, r
        assert r['amt_sum'] == 800.0, r
        assert r['stock_n'] == 1, r


def test_atomic_write_concurrency_guard():
    with tempfile.TemporaryDirectory() as tmpdir:
        target = os.path.join(tmpdir, 'sector_fundflow.parquet')
        df = pl.DataFrame({'trade_date': ['20240102'], 'industry_name': ['行业A'],
                           'net_main': [1.0], 'net_main_ratio': [0.1],
                           'amt_sum': [10.0], 'stock_n': [1]})
        assert atomic_write(df, target) is True
        assert os.path.exists(target) and not os.path.exists(target + '.tmp')
        # 模拟并发: 在 atomic_write 写 tmp 期间另一个进程修改了目标文件
        import unittest.mock as mock
        real_write = pl.DataFrame.write_parquet
        df2 = df.with_columns(pl.col('net_main') * 2)

        def racing_write(self, path, *a, **kw):
            real_write(self, path, *a, **kw)
            os.utime(target, (1, 1))   # 写 tmp 后、replace 前并发进程改了目标 mtime

        with mock.patch.object(pl.DataFrame, 'write_parquet', racing_write):
            assert atomic_write(df2, target) is False
        assert not os.path.exists(target + '.tmp')
        assert pl.read_parquet(target)['net_main'][0] == 1.0  # 原文件未被覆盖


if __name__ == '__main__':
    test_aggregation()
    test_atomic_write_concurrency_guard()
    print('OK: test_aggregation + test_atomic_write_concurrency_guard 通过')
