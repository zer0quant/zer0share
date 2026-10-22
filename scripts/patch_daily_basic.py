#!/usr/bin/env python3
"""Patch missing dates in daily_basic.parquet by re-downloading from Tushare."""
import pandas as pd
import numpy as np
import os, sys, time
from pathlib import Path

# Tushare config
TUSHARE_TOKEN = os.environ.get("TUSHARE_TOKEN", "")
TUSHARE_URL = "https://jiaoch.top/"

DATA_PATH = "/quantdata/zer0share/data/stock/daily_basic.parquet"
CAL_PATH = "/quantdata/zer0share/data/stock/trade_cal.parquet"

def get_missing_dates():
    df = pd.read_parquet(DATA_PATH, columns=["trade_date"])
    tc = pd.read_parquet(CAL_PATH)
    tc_trade = set(tc[tc['is_open']==1]['cal_date'].tolist())
    db_dates = set(df['trade_date'].unique())
    missing = sorted(tc_trade - db_dates)
    return missing

def download_date(date_str, retries=3):
    """Download daily_basic for one date via Tushare HTTP API."""
    import requests
    for attempt in range(retries):
        try:
            r = requests.post(
                TUSHARE_URL + "api/v1/pro",
                json={
                    "api_name": "daily_basic",
                    "token": TUSHARE_TOKEN,
                    "params": {"trade_date": date_str},
                    "fields": "",
                },
                timeout=30,
            )
            data = r.json()
            if data.get("code") == 0 and data.get("data"):
                df = pd.DataFrame(data["data"]["items"], columns=data["data"]["fields"])
                return df
            else:
                print(f"  {date_str}: API error {data.get('code')} {data.get('msg','')}")
        except Exception as e:
            print(f"  {date_str}: attempt {attempt+1} failed: {e}")
            time.sleep(3)
    return None

def main():
    import tushare as ts
    # Method that actually works (与原 download_all.py 的取数方式一致)
    ts.set_token(TUSHARE_TOKEN)
    api = ts.pro_api(TUSHARE_TOKEN)
    api._DataApi__http_url = TUSHARE_URL

    missing = get_missing_dates()
    print(f"Missing dates: {len(missing)}")
    print(f"Range: {missing[0]} ~ {missing[-1]}")

    if not missing:
        print("Nothing to patch.")
        return

    # Load existing data
    existing = pd.read_parquet(DATA_PATH)
    print(f"Existing: {len(existing)} rows")

    # Download missing dates
    new_rows = []
    failed = []
    for i, d in enumerate(missing):
        print(f"[{i+1}/{len(missing)}] Downloading {d}...", end=" ")
        sys.stdout.flush()
        try:
            df = api.daily_basic(trade_date=d)
            if df is not None and len(df) > 0:
                print(f"{len(df)} rows")
                new_rows.append(df)
            else:
                print("empty")
                failed.append(d)
        except Exception as e:
            print(f"error: {e}")
            failed.append(d)
        time.sleep(0.3)  # rate limit

    if new_rows:
        new_df = pd.concat(new_rows, ignore_index=True)
        print(f"\nDownloaded: {len(new_df)} rows, {new_df['trade_date'].nunique()} dates")

        # Merge with existing
        combined = pd.concat([existing, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["ts_code", "trade_date"])
        combined = combined.sort_values(["trade_date", "ts_code"]).reset_index(drop=True)

        print(f"Combined: {len(combined)} rows (was {len(existing)})")

        # Backup original
        import shutil
        shutil.copy(DATA_PATH, DATA_PATH + ".bak")
        combined.to_parquet(DATA_PATH, index=False)
        print(f"Saved to {DATA_PATH}")

        # Verify
        still_missing = get_missing_dates()
        print(f"\nStill missing: {len(still_missing)}")
        if still_missing:
            print(f"  {still_missing[:20]}")
    else:
        print("No new data downloaded.")

    if failed:
        print(f"\nFailed dates ({len(failed)}): {failed}")

if __name__ == "__main__":
    # Load token from zer0share config
    try:
        import tomllib
        with open("/git/zer0share/config/settings.toml", "rb") as f:
            cfg = tomllib.load(f)
    except ModuleNotFoundError:
        import tomli
        with open("/git/zer0share/config/settings.toml", "rb") as f:
            cfg = tomli.load(f)
    os.environ["TUSHARE_TOKEN"] = cfg["tushare"]["token"]
    TUSHARE_URL = cfg["tushare"]["http_url"]
    main()
