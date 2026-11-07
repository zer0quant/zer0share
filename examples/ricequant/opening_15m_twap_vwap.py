"""A 股/ETF 开盘后 15 分钟均价，支持本地分钟数据和 RiceQuant 在线取数。

TWAP 口径为 09:31—09:45 共 15 个分钟收盘价的等权平均。
使用方法见 opening_15m_twap_vwap.md。
"""

from __future__ import annotations

import argparse
from datetime import datetime, time
from pathlib import Path

import pandas as pd


RESULT_COLUMNS = [
    "order_book_id", "trade_date", "minute_count", "complete",
    "twap_15m", "vwap_15m", "volume_15m", "total_turnover_15m", "status",
]


def calculate_opening_15m(bars: pd.DataFrame) -> pd.DataFrame:
    """按证券、自然日计算；缺失或重复分钟不输出均价，不自动填充数据。"""
    if bars is None or bars.empty:
        return pd.DataFrame(columns=RESULT_COLUMNS)
    if "datetime" not in bars.columns or "order_book_id" not in bars.columns:
        bars = bars.reset_index()
    bars = bars.copy()
    bars["datetime"] = pd.to_datetime(bars["datetime"])
    bars["trade_date"] = bars["datetime"].dt.strftime("%Y%m%d")
    rows = []
    # 先按日分组：当天有行情但整个窗口缺失时也能输出 missing_minutes。
    for (code, date), day in bars.groupby(["order_book_id", "trade_date"], sort=True):
        window = day.loc[
            day["datetime"].dt.time.between(time(9, 31), time(9, 45))
        ].copy()
        expected = pd.date_range(f"{date} 09:31:00", periods=15, freq="min")
        complete = len(window) == 15 and set(window["datetime"]) == set(expected)
        values = window[["close", "volume", "total_turnover"]].apply(
            pd.to_numeric, errors="coerce"
        )
        valid = (
            values.notna().all().all()
            and values.abs().lt(float("inf")).all().all()
            and values["close"].gt(0).all()
            and values[["volume", "total_turnover"]].ge(0).all().all()
            and not ((values["volume"] == 0) & (values["total_turnover"] != 0)).any()
        )
        volume = values["volume"].sum() if valid else float("nan")
        turnover = values["total_turnover"].sum() if valid else float("nan")
        status = (
            "missing_or_duplicate_minutes" if not complete else
            "invalid_values" if not valid else
            "zero_volume" if volume == 0 else "ok"
        )
        rows.append({
            "order_book_id": code,
            "trade_date": date,
            "minute_count": window["datetime"].nunique(),
            "complete": complete,
            "twap_15m": values["close"].mean() if complete and valid else float("nan"),
            "vwap_15m": turnover / volume if status == "ok" else float("nan"),
            "volume_15m": volume,
            "total_turnover_15m": turnover,
            "status": status,
        })
    return pd.DataFrame(rows, columns=RESULT_COLUMNS)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", choices=["local", "online"], default="local")
    parser.add_argument("--asset", choices=["stock", "etf"], default="stock")
    parser.add_argument("--order-book-ids", nargs="+", default=["000001.XSHE"])
    parser.add_argument("--start-date", required=True, help="YYYYMMDD")
    parser.add_argument("--end-date", help="YYYYMMDD，默认等于开始日期")
    parser.add_argument("--config", default="config/settings.toml")
    parser.add_argument("--output", default="data/analysis/opening_15m_twap_vwap.csv")
    args = parser.parse_args()
    args.end_date = args.end_date or args.start_date
    try:
        for date in (args.start_date, args.end_date):
            datetime.strptime(date, "%Y%m%d")
            if len(date) != 8:
                raise ValueError("expected YYYYMMDD")
        if args.end_date < args.start_date:
            raise ValueError("end_date must be on or after start_date")
    except ValueError as exc:
        parser.error(str(exc))

    fields = ["close", "volume", "total_turnover"]
    if args.source == "local":
        from zer0share.rq_api import rq_api

        api = rq_api(args.config)
        get_price = api.get_etf_price if args.asset == "etf" else api.get_price
        bars = get_price(
            args.order_book_ids, start_date=args.start_date, end_date=args.end_date,
            fields=["order_book_id", "datetime", *fields], adjust_type="none",
        )
    else:
        import rqdatac
        from zer0share.config import load_config

        credentials = load_config(Path(args.config)).ricequant
        if credentials.license_key:
            rqdatac.init(username="license", password=credentials.license_key)
        elif credentials.username and credentials.password:
            rqdatac.init(username=credentials.username, password=credentials.password)
        else:
            parser.error("请在配置文件 [ricequant] 中填写 license_key 或 username/password")
        bars = rqdatac.get_price(
            args.order_book_ids, start_date=args.start_date, end_date=args.end_date,
            frequency="1m", fields=fields, adjust_type="none",
            skip_suspended=True, expect_df=True,
        )

    result = calculate_opening_15m(bars)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(output, index=False)
    print(result.to_string(index=False))
    print(f"已保存：{output}")
    if result.empty:
        print("未查询到数据：请检查日期、证券代码，以及本地数据是否已同步。")


if __name__ == "__main__":
    main()
