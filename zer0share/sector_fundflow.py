"""zer0share/sector_fundflow.py — 个股资金流聚合成「板块×日期」缓存表。

融合自 scripts/build_sector_fundflow.py (2026-10-21)。行为不变，变化只有两点：
路径改为由 config.settings.toml 派生（不再硬编码 /quantdata/...），
日志改用 loguru 并走 zer0share.logging 初始化。

原版保留在 scripts/build_sector_fundflow.py，两处可独立运行。
"""

from __future__ import annotations

import argparse
import glob
import os
import sys
from pathlib import Path

import polars as pl
from loguru import logger

from zer0share.config import Config, load_config
from zer0share.logging import init_logger

OUT_COLS = ["trade_date", "industry_name", "net_main", "net_main_ratio", "amt_sum", "stock_n"]

DEFAULT_FEATURES_DIR = "/quantdata/quantdb/6_ml_datasets/features_daily"


def stock_dir(cfg: Config) -> Path:
    return Path(cfg.data_dir) / "stock"


def load_industry_map(features_dir: str) -> pl.DataFrame:
    """取 features_daily 最新 dt 分区，构建 symbol -> industry_name 映射 (约 5~6k 行)。

    局限 (见模块注释): 用最新时点快照回溯全部历史日期，点时成分不完美。
    """
    parts = sorted(glob.glob(os.path.join(features_dir, "dt=*")))
    if not parts:
        raise FileNotFoundError(f"features_daily 下无 dt=* 分区: {features_dir}")
    latest = parts[-1]
    mp = (
        pl.scan_parquet(os.path.join(latest, "*.parquet"))
        .select(["symbol", "industry_name"])
        .drop_nulls()
        .unique()
        .sort("symbol")
        .collect()
    )
    # 个别 symbol 可能对应多个 industry_name，保留最后一个保证确定性
    mp = mp.unique(subset="symbol", keep="last")
    logger.info(f"行业映射: {latest} -> {mp.height} 只股票, {mp['industry_name'].n_unique()} 个行业")
    return mp


def build_sector_fundflow(
    moneyflow_path: str,
    daily_path: str,
    industry_map: pl.DataFrame,
    dates: list[str] | None = None,
) -> pl.LazyFrame:
    """惰性构建「行业×日期」资金流聚合。dates 非空则只算这些 trade_date。"""
    mf_cols = ["ts_code", "trade_date", "buy_lg_amount", "sell_lg_amount",
               "buy_elg_amount", "sell_elg_amount"]
    mf = pl.scan_parquet(moneyflow_path).select(mf_cols)
    if dates:
        mf = mf.filter(pl.col("trade_date").is_in(dates))
    mf = mf.join(
        industry_map.rename({"symbol": "ts_code"}).lazy(),
        on="ts_code", how="inner",  # 无行业归属的股票丢弃
    )
    net = (
        mf.with_columns(
            (pl.col("buy_lg_amount") + pl.col("buy_elg_amount")
             - pl.col("sell_lg_amount") - pl.col("sell_elg_amount")).alias("net")
        )
        .group_by(["trade_date", "industry_name"])
        .agg(
            pl.col("net").sum().alias("net_main"),  # 万元
            pl.len().alias("stock_n"),
        )
    )
    amt = (
        pl.scan_parquet(daily_path)
        .select(["ts_code", "trade_date", "amount"])  # 只取所需列
        .join(industry_map.rename({"symbol": "ts_code"}).lazy(),
              on="ts_code", how="inner")
        .group_by(["trade_date", "industry_name"])
        .agg((pl.col("amount").sum() / 10.0).alias("amt_sum"))  # 千元->万元
    )
    out = (
        net.join(amt, on=["trade_date", "industry_name"], how="left")
        .with_columns(
            pl.when(pl.col("amt_sum") > 0)
            .then(pl.col("net_main") / pl.col("amt_sum"))
            .otherwise(None)
            .alias("net_main_ratio")
        )
        .select(OUT_COLS)
        .sort(["trade_date", "industry_name"])
    )
    return out


def _stat_sig(path: str) -> tuple[int, int] | None:
    try:
        st = os.stat(path)
        return (st.st_mtime_ns, st.st_size)
    except OSError:
        return None


def atomic_write(df: pl.DataFrame, target: str) -> bool:
    """原子写入 + expect_stat 并发保护。返回是否写入成功。"""
    pre = _stat_sig(target)
    tmp = target + ".tmp"
    df.write_parquet(tmp)
    post = _stat_sig(target)
    if pre != post:
        if os.path.exists(tmp):
            os.remove(tmp)
        logger.warning(f"并发保护: {target} 在写入期间被其它进程修改 ({pre} -> {post})，放弃写入")
        return False
    os.replace(tmp, target)
    return True


def date_coverage(path: str, col: str = "trade_date") -> tuple:
    """惰性扫描取某 parquet 单列的 min/max/去重天数。"""
    d = pl.scan_parquet(path).select(col).collect()
    return d[col].min(), d[col].max(), d[col].n_unique()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="聚合个股资金流为板块×日期缓存表")
    ap.add_argument("--moneyflow", default=None)
    ap.add_argument("--daily", default=None)
    ap.add_argument("--features-dir", default=DEFAULT_FEATURES_DIR)
    ap.add_argument("--output", default=None)
    ap.add_argument("--incremental", action="store_true",
                    help="若输出已存在，只重算其最大 trade_date 之后的日期并合并(keep=last)")
    ap.add_argument("--dry-run", action="store_true",
                    help="只打印输入覆盖范围和将计算的日期数，不写文件")
    return ap.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    cfg = load_config()
    init_logger(cfg.log_path)
    sdir = stock_dir(cfg)
    moneyflow = args.moneyflow or str(sdir / "moneyflow.parquet")
    daily = args.daily or str(sdir / "daily.parquet")
    output = args.output or str(sdir / "sector_fundflow.parquet")

    if not os.path.exists(moneyflow):
        logger.error(f"moneyflow 不存在: {moneyflow}")
        return 1

    mf_min, mf_max, mf_days = date_coverage(moneyflow)
    logger.info(f"moneyflow: {moneyflow}")
    logger.info(f"  交易日覆盖: {mf_min} ~ {mf_max} ({mf_days} 天)")

    dates: list[str] | None = None
    if args.incremental and os.path.exists(output):
        prev = pl.scan_parquet(output).select("trade_date").collect()
        last = prev["trade_date"].max()
        logger.info(f"增量模式: 已有输出最大 trade_date={last}，只重算其后的日期")
        # 直接在 moneyflow 惰性扫描上过滤，避免物化全量日期列表
        dates = [d for d in pl.scan_parquet(moneyflow)
                 .select("trade_date").unique()
                 .collect()["trade_date"].sort().to_list() if d > last]
        if not dates:
            logger.info("无新日期可算，退出")
            return 0

    n = len(dates) if dates is not None else mf_days
    logger.info(f"将计算的日期数: {n}")
    if args.dry_run:
        logger.info("Dry-run 结束: 未计算、未写入")
        return 0

    ind_map = load_industry_map(args.features_dir)
    out_lf = build_sector_fundflow(moneyflow, daily, ind_map, dates=dates)
    new_df = out_lf.collect()
    logger.info(f"本次聚合: {new_df.height} 行 (行业×日期)")

    if args.incremental and os.path.exists(output):
        prev_df = pl.read_parquet(output)
        merged = pl.concat([prev_df, new_df], how="diagonal")
        merged = (merged.sort("trade_date")
                  .unique(subset=["trade_date", "industry_name"], keep="last")
                  .sort(["trade_date", "industry_name"]))
        logger.info(f"合并后: {merged.height} 行 (原 {prev_df.height} + 新 {new_df.height})")
        to_write = merged
    else:
        to_write = new_df

    ok = atomic_write(to_write, output)
    if not ok:
        return 2
    logger.info(f"✅ 已写入 {output} ({os.path.getsize(output) / 1e6:.1f} MB)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
