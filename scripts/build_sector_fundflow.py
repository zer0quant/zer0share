#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""把 zer0share 个股资金流 moneyflow.parquet 按行业成分聚合成「板块×日期」缓存表。

输入:
  - /quantdata/zer0share/data/stock/moneyflow.parquet
    列含 ts_code/trade_date/buy_lg_amount,sell_lg_amount,buy_elg_amount,
    sell_elg_amount,buy_md_...,net_mf_amount 等，单位万元。
    文件可达 1.5GB+ (1643 交易日 × ~5500 股)，全程 polars scan_parquet 惰性扫描，
    只 select 所需列，绝不一次性 to_pandas。
  - /quantdata/zer0share/data/stock/daily.parquet 的 amount 列
    (tushare 原生口径: 千元；本脚本 /10 转为万元后参与 net_main_ratio 计算)
  - /quantdata/quantdb/6_ml_datasets/features_daily/dt=YYYYMMDD/*.parquet
    用最新一个分区的 symbol→industry_name (东财 128 行业) 构建映射。
    局限: 行业成分用最新时点快照回溯套用全部历史日期，点时成分不完美
    (新股/改行业/退市股的历史归属会有偏差)，作为缓存表可接受。

输出:
  - /quantdata/zer0share/data/stock/sector_fundflow.parquet
    列: trade_date(YYYYMMDD 字符串), industry_name, net_main(主力净流入万元),
    net_main_ratio(净流入/成交额), amt_sum(行业总成交额万元), stock_n(成分股数)
    原子写入: 先写 .tmp 再 os.replace；写前写后各 stat 一次目标文件 (expect_stat
    并发保护)，若期间被其它进程修改则放弃写入并打印警告。

用法:
  python build_sector_fundflow.py                # 全量重建
  python build_sector_fundflow.py --incremental  # 只重算已有输出最大 trade_date 之后的日期
  python build_sector_fundflow.py --dry-run      # 只打印输入覆盖范围与将计算的日期数
  可用 --moneyflow/--daily/--features-dir/--output 覆盖默认路径 (冒烟测试用)。
"""
from __future__ import annotations

import argparse
import glob
import os
import sys

import polars as pl

STOCK_DIR = '/quantdata/zer0share/data/stock'
DEFAULT_MONEYFLOW = os.path.join(STOCK_DIR, 'moneyflow.parquet')
DEFAULT_DAILY = os.path.join(STOCK_DIR, 'daily.parquet')
DEFAULT_FEATURES_DIR = '/quantdata/quantdb/6_ml_datasets/features_daily'
DEFAULT_OUTPUT = os.path.join(STOCK_DIR, 'sector_fundflow.parquet')

OUT_COLS = ['trade_date', 'industry_name', 'net_main', 'net_main_ratio', 'amt_sum', 'stock_n']


def load_industry_map(features_dir: str) -> pl.DataFrame:
    """取 features_daily 最新 dt 分区，构建 symbol -> industry_name 映射 (约 5~6k 行)。

    局限 (见模块注释): 用最新时点快照回溯全部历史日期，点时成分不完美。
    """
    parts = sorted(glob.glob(os.path.join(features_dir, 'dt=*')))
    if not parts:
        raise FileNotFoundError(f'features_daily 下无 dt=* 分区: {features_dir}')
    latest = parts[-1]
    mp = (
        pl.scan_parquet(os.path.join(latest, '*.parquet'))
        .select(['symbol', 'industry_name'])
        .drop_nulls()
        .unique()
        .sort('symbol')
        .collect()
    )
    # 个别 symbol 可能对应多个 industry_name，保留最后一个保证确定性
    mp = mp.unique(subset='symbol', keep='last')
    print(f'行业映射: {latest} -> {mp.height} 只股票, '
          f'{mp["industry_name"].n_unique()} 个行业')
    return mp


def build_sector_fundflow(moneyflow_path: str, daily_path: str,
                          industry_map: pl.DataFrame,
                          dates: list[str] | None = None) -> pl.LazyFrame:
    """惰性构建「行业×日期」资金流聚合。dates 非空则只算这些 trade_date。"""
    mf_cols = ['ts_code', 'trade_date', 'buy_lg_amount', 'sell_lg_amount',
               'buy_elg_amount', 'sell_elg_amount']
    mf = pl.scan_parquet(moneyflow_path).select(mf_cols)
    if dates:
        mf = mf.filter(pl.col('trade_date').is_in(dates))
    mf = mf.join(
        industry_map.rename({'symbol': 'ts_code'}).lazy(),
        on='ts_code', how='inner',          # 无行业归属的股票丢弃
    )
    net = (
        mf.with_columns(
            (pl.col('buy_lg_amount') + pl.col('buy_elg_amount')
             - pl.col('sell_lg_amount') - pl.col('sell_elg_amount')).alias('net')
        )
        .group_by(['trade_date', 'industry_name'])
        .agg(
            pl.col('net').sum().alias('net_main'),          # 万元
            pl.len().alias('stock_n'),
        )
    )
    amt = (
        pl.scan_parquet(daily_path)
        .select(['ts_code', 'trade_date', 'amount'])         # 只取所需列
        .join(industry_map.rename({'symbol': 'ts_code'}).lazy(),
              on='ts_code', how='inner')
        .group_by(['trade_date', 'industry_name'])
        .agg((pl.col('amount').sum() / 10.0).alias('amt_sum'))  # 千元->万元
    )
    out = (
        net.join(amt, on=['trade_date', 'industry_name'], how='left')
        .with_columns(
            pl.when(pl.col('amt_sum') > 0)
            .then(pl.col('net_main') / pl.col('amt_sum'))
            .otherwise(None)
            .alias('net_main_ratio')
        )
        .select(OUT_COLS)
        .sort(['trade_date', 'industry_name'])
    )
    return out


def _stat_sig(path: str) -> tuple[int, int] | None:
    try:
        st = os.stat(path)
        return (st.st_mtime_ns, st.st_size)
    except FileNotFoundError:
        return None


def atomic_write(df: pl.DataFrame, target: str) -> bool:
    """原子写入 + expect_stat 并发保护。返回是否写入成功。

    写前 stat 目标文件；写 tmp 期间若目标被其它进程改过 (mtime/size 变化) 则
    放弃 os.replace，删除 tmp 并打印警告，避免覆盖并发写入方的结果。
    """
    pre = _stat_sig(target)
    tmp = target + '.tmp'
    df.write_parquet(tmp)
    post = _stat_sig(target)
    if pre != post:
        os.remove(tmp)
        print(f'⚠️ 并发保护: {target} 在写入期间被其它进程修改 '
              f'({pre} -> {post})，放弃写入，已删除 {tmp}')
        return False
    os.replace(tmp, target)
    return True


def date_coverage(path: str, col: str = 'trade_date') -> tuple:
    """惰性扫描取某 parquet 单列的 min/max/去重天数。"""
    d = pl.scan_parquet(path).select(col).collect()
    return d[col].min(), d[col].max(), d[col].n_unique()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description='聚合个股资金流为板块×日期缓存表')
    ap.add_argument('--moneyflow', default=DEFAULT_MONEYFLOW)
    ap.add_argument('--daily', default=DEFAULT_DAILY)
    ap.add_argument('--features-dir', default=DEFAULT_FEATURES_DIR)
    ap.add_argument('--output', default=DEFAULT_OUTPUT)
    ap.add_argument('--incremental', action='store_true',
                    help='若输出已存在，只重算其最大 trade_date 之后的日期并合并(keep=last)')
    ap.add_argument('--dry-run', action='store_true',
                    help='只打印输入覆盖范围和将计算的日期数，不写文件')
    args = ap.parse_args(argv)

    if not os.path.exists(args.moneyflow):
        print(f'❌ moneyflow 不存在: {args.moneyflow}')
        return 1

    mf_min, mf_max, mf_days = date_coverage(args.moneyflow)
    print(f'moneyflow: {args.moneyflow}')
    print(f'  交易日覆盖: {mf_min} ~ {mf_max} ({mf_days} 天)')

    dates: list[str] | None = None
    if args.incremental and os.path.exists(args.output):
        prev = pl.scan_parquet(args.output).select('trade_date').collect()
        last = prev['trade_date'].max()
        print(f'增量模式: 已有输出最大 trade_date={last}，只重算其后的日期')
        # 直接在 moneyflow 惰性扫描上过滤，避免物化全量日期列表
        dates = [d for d in pl.scan_parquet(args.moneyflow)
                 .select('trade_date').unique()
                 .collect()['trade_date'].sort().to_list() if d > last]
        if not dates:
            print('无新日期可算，退出')
            return 0

    n = len(dates) if dates is not None else mf_days
    print(f'将计算的日期数: {n}')
    if args.dry_run:
        print('Dry-run 结束: 未计算、未写入')
        return 0

    ind_map = load_industry_map(args.features_dir)
    out_lf = build_sector_fundflow(args.moneyflow, args.daily, ind_map, dates=dates)
    new_df = out_lf.collect()
    print(f'本次聚合: {new_df.height} 行 (行业×日期)')

    if args.incremental and os.path.exists(args.output):
        prev_df = pl.read_parquet(args.output)
        merged = pl.concat([prev_df, new_df], how='diagonal')
        merged = (merged.sort('trade_date')
                  .unique(subset=['trade_date', 'industry_name'], keep='last')
                  .sort(['trade_date', 'industry_name']))
        print(f'合并后: {merged.height} 行 (原 {prev_df.height} + 新 {new_df.height})')
        to_write = merged
    else:
        to_write = new_df

    ok = atomic_write(to_write, args.output)
    if not ok:
        return 2
    print(f'✅ 已写入 {args.output} ({os.path.getsize(args.output) / 1e6:.1f} MB)')
    return 0


if __name__ == '__main__':
    sys.exit(main())
