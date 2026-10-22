#!/usr/bin/env python3
"""
下载融资融券、沪深港通持股、龙虎榜数据
数据源: akshare (免费，无需token)
"""
import akshare as ak
import pandas as pd
import os, time, traceback
from datetime import datetime

OUTPUT_DIR = '/quantdata/zer0share/data/stock'
os.makedirs(OUTPUT_DIR, exist_ok=True)

START_DATE = '2020-01-01'
END_DATE = '2026-09-30'

def save_parquet(df, name):
    path = os.path.join(OUTPUT_DIR, f'{name}.parquet')
    df.to_parquet(path, index=False)
    print(f"  Saved: {path} ({len(df):,} rows, {os.path.getsize(path)/1e6:.1f} MB)")

def get_trade_dates(start, end):
    dates = pd.bdate_range(start, end)
    return [d.strftime('%Y%m%d') for d in dates]

def download_margin():
    """融资融券交易明细 — akshare stock_margin_detail_sse + stock_margin_detail_szse"""
    print("\n[1/3] margin_detail (融资融券明细)...")
    all_dfs = []
    trade_dates = get_trade_dates(START_DATE, END_DATE)
    
    for i, d in enumerate(trade_dates):
        # 上交所
        try:
            df = ak.stock_margin_detail_sse(date=d)
            if df is not None and len(df) > 0:
                df['exchange'] = 'SSE'
                df['trade_date'] = d
                all_dfs.append(df)
        except Exception:
            pass
        time.sleep(0.1)
        # 深交所
        try:
            df = ak.stock_margin_detail_szse(date=d)
            if df is not None and len(df) > 0:
                df['exchange'] = 'SZSE'
                df['trade_date'] = d
                all_dfs.append(df)
        except Exception:
            pass
        time.sleep(0.1)
        
        if (i+1) % 50 == 0:
            total = sum(len(x) for x in all_dfs)
            print(f"  ...{d} ({total:,} rows, {i+1}/{len(trade_dates)})")
    
    if all_dfs:
        df = pd.concat(all_dfs, ignore_index=True)
        save_parquet(df, 'margin_detail')
        return df
    print("  No margin data")
    return None

def download_hk_hold():
    """沪深港通持股明细 — akshare stock_hsgt_individual_em (北向持股明细)"""
    print("\n[2/3] hk_hold (沪深港通持股)...")
    # stock_hsgt_hist_em 返回北向资金历史数据
    # stock_hsgt_hold_stock_em 按市场返回持股
    all_dfs = []
    
    # 用 stock_hsgt_stock_statistics_em 获取沪深港通持股统计
    for market in ['北向', '沪股通', '深股通']:
        try:
            df = ak.stock_hsgt_stock_statistics_em(symbol=market, indicator='月涨跌幅')
            if df is not None and len(df) > 0:
                df['market'] = market
                all_dfs.append(df)
                print(f"  {market}: {len(df)} rows")
        except Exception as e:
            print(f"  {market} error: {str(e)[:80]}")
    
    # 用 stock_hsgt_hist_em 获取北向资金历史
    try:
        df = ak.stock_hsgt_hist_em(symbol='北向资金')
        if df is not None and len(df) > 0:
            save_parquet(df, 'hsgt_hist')
            print(f"  hsgt_hist: {len(df)} rows")
    except Exception as e:
        print(f"  hsgt_hist error: {str(e)[:80]}")
    
    # 用 stock_hsgt_hold_stock_em 获取持股明细
    for market in ['北向', '沪股通', '深股通']:
        for indicator in ['今日排行', '5日排行', '10日排行']:
            try:
                df = ak.stock_hsgt_hold_stock_em(symbol=market, indicator=indicator)
                if df is not None and len(df) > 0:
                    df['market'] = market
                    df['indicator'] = indicator
                    all_dfs.append(df)
            except Exception:
                pass
            time.sleep(0.2)
    
    if all_dfs:
        df = pd.concat(all_dfs, ignore_index=True)
        save_parquet(df, 'hk_hold')
        return df
    print("  No hk_hold data")
    return None

def download_top_list():
    """龙虎榜每日明细 — akshare stock_lhb_detail_em"""
    print("\n[3/3] top_list (龙虎榜)...")
    all_dfs = []
    trade_dates = get_trade_dates(START_DATE, END_DATE)
    
    for i, d in enumerate(trade_dates):
        try:
            df = ak.stock_lhb_detail_em(start_date=d, end_date=d)
            if df is not None and len(df) > 0:
                df['trade_date'] = d
                all_dfs.append(df)
        except Exception:
            pass
        time.sleep(0.2)
        
        if (i+1) % 50 == 0:
            total = sum(len(x) for x in all_dfs)
            print(f"  ...{d} ({total:,} rows, {i+1}/{len(trade_dates)})")
    
    if all_dfs:
        df = pd.concat(all_dfs, ignore_index=True)
        save_parquet(df, 'top_list')
        return df
    print("  No top_list data")
    return None

def main():
    print("="*60)
    print("融资融券 + 沪深港通 + 龙虎榜 下载 (akshare)")
    print(f"  区间: {START_DATE} ~ {END_DATE}")
    print(f"  输出: {OUTPUT_DIR}")
    print("="*60)

    t0 = time.time()

    download_margin()
    download_hk_hold()
    download_top_list()

    print(f"\n{'='*60}")
    print(f"All done. Total time: {time.time()-t0:.0f}s")
    print(f"Files in {OUTPUT_DIR}:")
    for f in sorted(os.listdir(OUTPUT_DIR)):
        path = os.path.join(OUTPUT_DIR, f)
        if os.path.isfile(path) and f.endswith('.parquet'):
            print(f"  {f}: {os.path.getsize(path)/1e6:.1f} MB")

if __name__ == '__main__':
    main()
