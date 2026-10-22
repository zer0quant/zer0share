#!/usr/bin/env python3
"""
验证脚本：基于 daily_kline 的 pre_close 计算后复权因子，
并与 Tushare adj_factor 接口做交叉验证。

用法：
    uv run python scripts/validate_adj_factor.py --ts_code 000001.SZ --start 20230101 --end 20231231
"""

import argparse
from pathlib import Path

import pandas as pd
import tushare as ts

from zer0share.config import load_config


def load_kline(data_dir: Path, ts_code: str, start: str, end: str) -> pd.DataFrame:
    """从分区 Parquet 读取指定标的在日期区间内的日线数据。"""
    kline_dir = data_dir / "daily_kline"
    frames = []
    for partition in sorted(kline_dir.iterdir()):
        if not partition.is_dir():
            continue
        date_str = partition.name.split("=")[-1]
        if date_str < start or date_str > end:
            continue
        parquet = partition / "data.parquet"
        if not parquet.exists():
            continue
        df = pd.read_parquet(parquet, columns=["ts_code", "trade_date", "close", "pre_close"])
        filtered = df[df["ts_code"] == ts_code]
        if not filtered.empty:
            frames.append(filtered)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True).sort_values("trade_date").reset_index(drop=True)


def compute_adj_factor(df: pd.DataFrame) -> pd.DataFrame:
    """
    后复权因子计算逻辑：
      - 当 pre_close[t] != close[t-1] 时发生除权除息事件
      - 单日调整比率 = close[t-1] / pre_close[t]（无事件时 = 1）
      - 后复权因子 = 从首日开始累乘各单日调整比率，首日因子 = 1
    """
    result = df.copy()
    prev_close = result["close"].shift(1)

    # 首日无前收盘，ratio = 1；其余日：close[t-1] / pre_close[t]
    ratio = (prev_close / result["pre_close"]).fillna(1.0)
    ratio.iloc[0] = 1.0

    result["adj_factor_calc"] = ratio.cumprod()
    return result[["ts_code", "trade_date", "close", "pre_close", "adj_factor_calc"]]


def fetch_tushare_adj(token: str, ts_code: str, start: str, end: str, http_url: str = "") -> pd.DataFrame:
    pro = ts.pro_api(token)
    if http_url:
        pro._DataApi__http_url = http_url
    df = pro.adj_factor(ts_code=ts_code, start_date=start, end_date=end)
    if df is None or df.empty:
        raise RuntimeError("Tushare adj_factor 未返回数据，请检查 token 积分或日期范围")
    return (
        df[["trade_date", "adj_factor"]]
        .rename(columns={"adj_factor": "adj_factor_ts"})
        .sort_values("trade_date")
        .reset_index(drop=True)
    )


def compare(calc_df: pd.DataFrame, tushare_df: pd.DataFrame) -> pd.DataFrame:
    calc_df = calc_df.copy()
    tushare_df = tushare_df.copy()
    # 统一为 YYYYMMDD 字符串，避免 date/str/Timestamp 类型不一致导致 merge 失败
    calc_df["trade_date"] = calc_df["trade_date"].astype(str).str.replace("-", "")
    tushare_df["trade_date"] = tushare_df["trade_date"].astype(str).str.replace("-", "")
    merged = calc_df.merge(tushare_df, on="trade_date", how="inner")
    if merged.empty:
        raise RuntimeError("无重叠日期，无法比较")

    # 两者绝对基准不同，统一归一化到首个重叠日 = 1.0
    calc_base = merged["adj_factor_calc"].iloc[0]
    ts_base = merged["adj_factor_ts"].iloc[0]
    merged["calc_norm"] = merged["adj_factor_calc"] / calc_base
    merged["ts_norm"] = merged["adj_factor_ts"] / ts_base

    merged["diff_pct"] = (
        (merged["calc_norm"] - merged["ts_norm"]).abs() / merged["ts_norm"] * 100
    )
    return merged


def print_report(result: pd.DataFrame, threshold: float = 0.01) -> None:
    print(f"\n{'='*60}")
    print(f"总交易日数：{len(result)}")
    print(f"最大差异：  {result['diff_pct'].max():.6f}%")
    print(f"平均差异：  {result['diff_pct'].mean():.6f}%")

    outliers = result[result["diff_pct"] > threshold]
    print(f"\n差异 > {threshold}% 的日期：{len(outliers)} 个")
    if not outliers.empty:
        print(
            outliers[["trade_date", "close", "pre_close", "calc_norm", "ts_norm", "diff_pct"]]
            .to_string(index=False)
        )

    print(f"\n{'='*60}")
    print("样本（前15行）：")
    print(
        result[["trade_date", "close", "pre_close", "calc_norm", "ts_norm", "diff_pct"]]
        .head(15)
        .to_string(index=False)
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="后复权因子交叉验证")
    parser.add_argument("--ts_code", required=True, help="股票代码，如 000001.SZ")
    parser.add_argument("--start", required=True, help="开始日期 YYYYMMDD")
    parser.add_argument("--end", required=True, help="结束日期 YYYYMMDD")
    parser.add_argument("--threshold", type=float, default=0.01, help="告警差异阈值（%%），默认 0.01")
    args = parser.parse_args()

    cfg = load_config()

    print(f"\n>>> 加载本地日线：{args.ts_code}  {args.start} ~ {args.end}")
    kline = load_kline(cfg.data_dir, args.ts_code, args.start, args.end)
    if kline.empty:
        print("未找到数据，请先执行：uv run python main.py sync --table daily_kline")
        return
    print(f"    共 {len(kline)} 个交易日")

    calc_df = compute_adj_factor(kline)

    print(f"\n>>> 拉取 Tushare adj_factor ...")
    tushare_df = fetch_tushare_adj(cfg.tushare_token, args.ts_code, args.start, args.end, cfg.tushare_http_url)
    print(f"    Tushare 返回 {len(tushare_df)} 条")

    result = compare(calc_df, tushare_df)
    print_report(result, threshold=args.threshold)


if __name__ == "__main__":
    main()
