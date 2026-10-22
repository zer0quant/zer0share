#!/usr/bin/env python3
"""统一 /quantdata/zer0share/data 存储格式为 catalog 声明规范。

catalog 是唯一权威:
  - 时序表   date=YYYYMMDD/data.parquet
  - 静态表   data.parquet
  - index_weight  index_code=*/date=*/data.parquet
  - trade_cal     exchange=*/data.parquet

现状 (改前, 2026-10-21 迁移时的历史快照, 现已完成):
  ① 单文件  <name>.parquet          —— 原 bulk 引擎写, 最新 (到 20261020/21)
  ② date=*  分区 <name>/            —— sync 遗留, 停更 (到 20260930 或更早)
  ③ dt=*    分区                     —— block_trade/share_float (已 rename 成 date=*)
  ④ exchange=* / index_code=*        —— trade_cal / index_weight (本就是规范, 不动)

合并方向 = 分区保留历史 + 单文件补分区没有的日期 (分区历史更长, 单文件更新)。
重复拷贝移 <name>.parquet.trash-<ts>/ , 一周验证后由 --purge-trash 真正删除。

注: bulk 引擎已删除 (能力吸收进 BatchSyncJob), 本脚本仅用于历史数据迁移,
新数据不会再产生单文件路径。
"""
from __future__ import annotations

import argparse
import os
import shutil
import sys
import time
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

DATA = Path("/quantdata/zer0share/data")
TRASH = DATA / ".trash"

# 时序表: (分区目录, 同名单文件, 分区日期列)
# 注: index_daily 无同名单文件 (磁盘上只有 index/index_daily/ 分区), 故不列入。
TIME_SERIES = [
    ("daily_kline", "stock/daily_kline",       "stock/daily.parquet",     "trade_date"),
    ("daily_basic", "stock/daily_basic",       "stock/daily_basic.parquet", "trade_date"),
    ("stk_limit",   "stock/stk_limit_dir",     "stock/stk_limit.parquet", "trade_date"),
    ("stock_st",    "stock/stock_st",          "stock/stock_st.parquet",  "trade_date"),
    ("suspend_d",   "stock/suspend_d",         "stock/suspend_d.parquet", "trade_date"),
]
# 静态表 / 其它布局: (名, 分区目录, 同名单文件)
# index_weight 本就是 index_code=*/date=* 规范布局, 无需合并, 只在有同名单文件时才移 .trash。
STATIC = [
    ("index_weight", "index/index_weight", "index/index_weight.parquet"),
    ("stock_basic",  "stock/basic",        "stock/stock_basic.parquet"),
]


def log(msg: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


def partition_glob(rel: str) -> str:
    return str(DATA / rel / "date=*" / "data.parquet")


def partition_dates(rel: str, date_col: str) -> set[str]:
    sql = (f"SELECT DISTINCT {date_col} FROM "
           f"read_parquet('{partition_glob(rel)}', hive_partitioning=true)")
    return set(r[0] for r in duckdb.sql(sql).fetchall())


def merge_single_into_partitions(name, part_rel, single_rel, date_col, dry=False) -> dict:
    """把单文件中分区没有的日期, 按 date=YYYYMMDD/data.parquet 并入分区。"""
    single = DATA / single_rel
    out = {"name": name, "merged_days": 0, "merged_rows": 0, "missing_cols": []}
    if not single.exists():
        log(f"  {name}: 单文件不存在, 跳过")
        return out
    have = partition_dates(part_rel, date_col)
    df = duckdb.sql(f"SELECT * FROM read_parquet('{single}')").df()
    if date_col not in df.columns:
        log(f"  ❌ {name}: 单文件缺 {date_col} 列, 跳过")
        return out
    todo = sorted(set(df[date_col].astype(str)) - have)
    if not todo:
        log(f"  {name}: 单文件无独有日期 (分区已是最新), 跳过")
        return out
    # 分区 schema: 单文件多出的列补 null; 分区多出的列单文件补 null
    part_files = sorted((DATA / part_rel).glob("date=*/data.parquet"))
    part_cols = list(pq.ParquetFile(part_files[-1]).schema_arrow.names) if part_files else []
    single_cols = list(df.columns)
    if part_cols:
        out["missing_cols"] = [c for c in part_cols if c not in single_cols]
    log(f"  {name}: 待合并 {len(todo)} 天 ({todo[0]}~{todo[-1]}); "
        f"分区多出的列将填空: {out['missing_cols']}")
    for d in todo:
        sub = df[df[date_col].astype(str) == d]
        if part_cols:
            for c in part_cols:
                if c not in sub.columns:
                    sub = sub.copy()
                    sub[c] = None
            sub = sub[part_cols]
        dst = DATA / part_rel / f"date={d}"
        fp = dst / "data.parquet"
        if not dry:
            dst.mkdir(parents=True, exist_ok=True)
            sub.to_parquet(fp, index=False)
        out["merged_days"] += 1
        out["merged_rows"] += len(sub)
    log(f"  ✅ {name}: 并入 {out['merged_days']} 天 / {out['merged_rows']:,} 行"
        + (" (dry-run, 未写盘)" if dry else ""))
    return out


def trash_single(single_rel: str, dry=False) -> bool:
    """单文件移 .trash/<ts>/ 保留相对路径, 一周后 --purge-trash 真删。"""
    src = DATA / single_rel
    if not src.exists():
        return False
    if not dry:
        ts = time.strftime("%Y%m%d-%H%M%S")
        dst = TRASH / ts / single_rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(src), str(dst))
        log(f"  🗑️  {single_rel} → .trash/{ts}/{single_rel}")
    else:
        log(f"  🗑️  (dry-run) 将移动 {single_rel} → .trash/")
    return True


def merge_index_weight_extra(dry=False) -> dict:
    """把 index/index_weight_extra.parquet 并入规范布局 index_code=*/date=*/data.parquet。

    extra 含 3 个规范布局里没有的指数 (000688.SH 科创50 / 932000.CSI /
    399006.SZ 创业板指), 是真实数据, 不能丢。按 index_code + date 拆分写入。
    """
    extra = DATA / "index/index_weight_extra.parquet"
    root = DATA / "index/index_weight"
    out = {"name": "index_weight_extra", "files": 0, "rows": 0}
    if not extra.exists():
        log("  index_weight_extra: 不存在, 跳过")
        return out
    df = duckdb.sql(f"SELECT * FROM read_parquet('{extra}')").df()
    df["trade_date"] = df["trade_date"].astype(str)
    for (code, d), grp in df.groupby(["index_code", "trade_date"]):
        dst = root / f"index_code={code}" / f"date={d}"
        fp = dst / "data.parquet"
        if fp.exists():
            old = pq.read_table(fp).to_pandas()
            merged = pd.concat([old, grp], ignore_index=True) \
                .drop_duplicates(subset=["index_code", "con_code", "trade_date"], keep="last")
        else:
            merged = grp
        if not dry:
            dst.mkdir(parents=True, exist_ok=True)
            merged.to_parquet(fp, index=False)
        out["files"] += 1
        out["rows"] += len(merged)
    log(f"  ✅ index_weight_extra: {out['files']} 个 (index_code,date) 组合 / "
        f"{out['rows']:,} 行" + (" (dry-run)" if dry else ""))
    return out


def fix_stock_basic(dry=False) -> bool:
    """stock/basic/data.parquet 是 0 行 (sync 从未成功写入), 而同名单文件
    stock/stock_basic.parquet 有 5577 行真实数据。query 层认的是分区那份
    (catalog BASIC_SPEC.path_parts = stock/basic), 所以要把单文件内容搬到分区。"""
    part = DATA / "stock/basic/data.parquet"
    single = DATA / "stock/stock_basic.parquet"
    if not single.exists():
        log("  ❌ stock_basic: 单文件不存在, 跳过")
        return False
    n = pq.ParquetFile(single).metadata.num_rows
    if part.exists():
        pn = pq.ParquetFile(part).metadata.num_rows
        if pn > 0:
            log(f"  ℹ️ stock_basic: 分区已有 {pn:,} 行 (≥ 单文件 {n:,} 行), 保留分区")
            return False
        log(f"  stock_basic: 分区 0 行 → 用单文件 {n:,} 行填充")
    else:
        log(f"  stock_basic: 分区文件不存在 → 用单文件 {n:,} 行创建")
    if not dry:
        part.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(str(single), str(part))
    return True


def rename_dirs(dry=False) -> None:
    """把磁盘分区目录名改成 catalog 权威名 (path_parts 末段)。

    目前只有 stock/stk_limit_dir → stock/stk_limit: sync 遗留目录用了
    _dir 后缀, 而 catalog / query / bulk 都认 stk_limit, 导致 sync 写
    stk_limit/、既有数据在 stk_limit_dir/ — query 读不到。
    """
    renames = [("stock/stk_limit_dir", "stock/stk_limit")]
    for old_rel, new_rel in renames:
        old, new = DATA / old_rel, DATA / new_rel
        if not old.is_dir():
            continue
        if new.exists():
            log(f"  ⚠️ {old_rel}: 目标 {new_rel} 已存在, 跳过改名")
            continue
        log(f"  📂 {old_rel} → {new_rel}")
        if not dry:
            os.rename(old, new)


def resolve_part_rel(rel: str) -> str:
    """分区目录可能已被改名 (stk_limit_dir → stk_limit), 兼容两种名字。"""
    if (DATA / rel).is_dir():
        return rel
    alt = rel.replace("_dir", "") if rel.endswith("_dir") else f"{rel}_dir"
    if (DATA / alt).is_dir():
        return alt
    return rel


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--purge-trash", action="store_true", help="真正删除 .trash 内容")
    ap.add_argument("--trash-age-days", type=int, default=7)
    args = ap.parse_args()

    if args.purge_trash:
        if not TRASH.exists():
            log(".trash 不存在")
            return 0
        cutoff = time.time() - args.trash_age_days * 86400
        freed = 0
        for d in sorted(TRASH.iterdir()):
            if d.stat().st_mtime >= cutoff:
                log(f"  ⏳ {d.name} 未满 {args.trash_age_days} 天, 保留")
                continue
            sz = sum(f.stat().st_size for f in d.rglob("*") if f.is_file())
            if not args.dry_run:
                shutil.rmtree(d)
            freed += sz
            log(f"  🔥 删除 {d.name} ({sz/2**30:.2f} GiB)" + (" (dry-run)" if args.dry_run else ""))
        log(f"共释放 {freed/2**30:.2f} GiB")
        return 0

    log("=== 统一存储格式 (dry-run)" if args.dry_run else "=== 统一存储格式 (实跑)")
    report = []
    log("\n=== 时序表: 单文件补入分区 ===")
    for name, part_rel, single_rel, date_col in TIME_SERIES:
        part_rel = resolve_part_rel(part_rel)
        if not (DATA / part_rel).is_dir():
            log(f"  {name}: 分区目录 {part_rel} 不存在, 跳过")
            continue
        r = merge_single_into_partitions(name, part_rel, single_rel, date_col, args.dry_run)
        report.append(r)
    log("\n=== index_weight: extra 并入规范布局 ===")
    report.append(merge_index_weight_extra(args.dry_run))
    log("\n=== 静态表 ===")
    fix_stock_basic(args.dry_run)
    log("\n=== 目录改名: 让磁盘目录名 = catalog 权威名 ===")
    rename_dirs(args.dry_run)
    log("\n=== 移走同名单文件 (保留在 .trash, 一周后再删) ===")
    for name, part_rel, single_rel, _ in TIME_SERIES:
        if trash_single(single_rel, args.dry_run):
            log(f"     (分区数据保留在 {part_rel})")
    for name, part_rel, single_rel in STATIC:
        if name == "stock_basic":
            continue
        trash_single(single_rel, args.dry_run)
    trash_single("index/index_weight_extra.parquet", args.dry_run)
    log("\n=== 汇总 ===")
    for r in report:
        if "merged_days" in r:
            log(f"  {r['name']:<18s} +{r['merged_days']} 天 / {r['merged_rows']:,} 行")
        else:
            log(f"  {r['name']:<18s} +{r['files']} 文件 / {r['rows']:,} 行")
    return 0


if __name__ == "__main__":
    sys.exit(main())
