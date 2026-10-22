import duckdb
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import date, datetime, timezone
from loguru import logger
from pathlib import Path


def _parse(s: str) -> date:
    s = _date_str(s)
    return date(int(s[:4]), int(s[4:6]), int(s[6:]))


def _date_str(value) -> str:
    if isinstance(value, str):
        return value
    return value.strftime("%Y%m%d")


# ---------------------------------------------------------------- 分区写入原语
# 这三块逻辑原先在 bulk.py 里 (dedup_keys / match_dtypes / _extend_partition_schema),
# 吸收 bulk 后所有 writer 都要用, 故上移到 storage 层成为共享原语。

def _is_text(dt) -> bool:
    return dt.kind in ("O", "U", "S") or pd.api.types.is_string_dtype(dt)


def _is_num(dt) -> bool:
    return dt.kind in ("i", "u", "f")


def match_dtypes(new: pd.DataFrame, old: pd.DataFrame) -> pd.DataFrame:
    """对齐新旧同名列的 dtype。

    现有文件多把 trade_date/日期列存成字符串, 而新下载数据经 clean_df 后会变成
    int64 — 直接 concat 会让同一列混着 int 与 str, 写 parquet 时报 ArrowInvalid。
    """
    for col in new.columns:
        if col not in old.columns:
            continue
        ao, an = old[col].dtype, new[col].dtype
        if ao == an:
            continue
        ao_text, an_text = _is_text(ao), _is_text(an)
        ao_num, an_num = _is_num(ao), _is_num(an)
        if an_num and ao_num:
            # 新列浮点而旧列整型, 且新列确有小数 → 旧列升为浮点, 否则新列降为整型
            if an.kind == "f" and ao.kind == "i" and not new[col].dropna().mod(1).eq(0).all():
                old[col] = old[col].astype(float)
            else:
                new[col] = new[col].astype(ao)
        elif an_num and ao_text:
            new[col] = new[col].map(str)  # 旧行是字符串 → 新行对齐成字符串
        elif an_text and ao_num:
            conv = pd.to_numeric(new[col], errors="coerce")
            if conv.notna().all():
                new[col] = conv
            else:
                old[col] = old[col].map(str)
    return new


def partition_dedup_keys(df: pd.DataFrame, date_col: str) -> list[str]:
    """分区表业务主键: (ts_code, date_col) 或仅 date_col。

    与 bulk.dedup_keys(new, spec) 等价, 但不依赖 spec dict — storage 层只认识
    DataFrame 与日期列名。
    """
    keys = (["ts_code"] if "ts_code" in df.columns else []) + \
           ([date_col] if date_col in df.columns else [])
    return keys


def _extend_partition_schema(table_dir: Path, old_cols: list[str], new_cols: list[str]) -> None:
    """给 table_dir 下既有分区补列 (新列填 None), 保持全表 schema 唯一。

    只在调用方确认新数据列是既有分区的超集时调用; 逐个分区原子替换,
    单分区失败不影响其他分区 (下次同步仍会尝试)。
    """
    import glob as _glob

    add = [c for c in new_cols if c not in old_cols]
    for fp in sorted(_glob.glob(str(table_dir / "date=*" / "data.parquet"))):
        try:
            tbl = pq.read_table(fp)
            for c in add:
                if c not in tbl.column_names:
                    tbl = tbl.append_column(pa.field(c, pa.string()),
                                            pa.array([None] * tbl.num_rows))
            tmp = fp + ".tmp"
            pq.write_table(tbl, tmp)
            os.replace(tmp, fp)
        except Exception:
            continue


class MetaStore:
    def __init__(self, db_path: Path):
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = duckdb.connect(str(db_path))
        self._init_schema()

    def _init_schema(self):
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS sync_meta (
                table_name  VARCHAR PRIMARY KEY,
                last_date   DATE,
                updated_at  TIMESTAMP
            )
        """)
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS trade_cal (
                exchange      VARCHAR,
                cal_date      DATE,
                is_open       BOOLEAN,
                pretrade_date DATE,
                PRIMARY KEY (exchange, cal_date)
            )
        """)

    def get_last_date(self, table_name: str) -> str | None:
        row = self._conn.execute(
            "SELECT last_date FROM sync_meta WHERE table_name = ?",
            [table_name]
        ).fetchone()
        return row[0].strftime("%Y%m%d") if row else None

    def update_last_date(self, table_name: str, last_date: str):
        self._conn.execute("""
            INSERT INTO sync_meta (table_name, last_date, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT (table_name) DO UPDATE SET
                last_date = excluded.last_date,
                updated_at = excluded.updated_at
        """, [table_name, _parse(last_date), datetime.now(timezone.utc)])

    def __enter__(self) -> "MetaStore":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self.close()
        return False

    def load_trade_cal_from_parquet(
        self, data_dir: Path, exchanges: list[str] | None = None
    ) -> None:
        trade_cal_dir = data_dir / "stock" / "trade_cal"
        if not trade_cal_dir.exists():
            return
        allowed_exchanges = set(exchanges) if exchanges is not None else None
        self._conn.execute("BEGIN")
        try:
            self._conn.execute("DELETE FROM trade_cal")
            for exchange_dir in sorted(trade_cal_dir.iterdir()):
                if not exchange_dir.is_dir():
                    continue
                exchange = exchange_dir.name.removeprefix("exchange=")
                if allowed_exchanges is not None and exchange not in allowed_exchanges:
                    continue
                parquet_path = exchange_dir / "data.parquet"
                if not parquet_path.exists():
                    continue
                self._conn.execute(
                    """
                    INSERT INTO trade_cal
                    SELECT
                        exchange,
                        strptime(CAST(cal_date AS VARCHAR), '%Y%m%d')::DATE AS cal_date,
                        is_open,
                        CASE
                            WHEN pretrade_date IS NULL THEN NULL
                            ELSE strptime(CAST(pretrade_date AS VARCHAR), '%Y%m%d')::DATE
                        END AS pretrade_date
                    FROM read_parquet(?)
                    """,
                    [str(parquet_path)]
                )
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    def get_trading_days(
        self, exchange: str, start: str, end: str
    ) -> list[str]:
        rows = self._conn.execute(
            """
            SELECT cal_date FROM trade_cal
            WHERE exchange = ?
              AND cal_date >= ?
              AND cal_date <= ?
              AND is_open = TRUE
            ORDER BY cal_date
            """,
            [exchange, _parse(start), _parse(end)]
        ).fetchall()
        return [row[0].strftime("%Y%m%d") for row in rows]

    def is_trading_day(self, exchange: str, cal_date: str) -> bool:
        """Check whether a given date is a trading day for an exchange.

        Returns True when the date is not covered by the calendar
        (conservative: don't skip syncs for dates we don't know about).
        """
        row = self._conn.execute(
            "SELECT is_open FROM trade_cal WHERE exchange = ? AND cal_date = ?",
            [exchange, _parse(cal_date)]
        ).fetchone()
        if row is None:
            return True
        return bool(row[0])

    def close(self):
        self._conn.close()


def write_universe(data_dir: Path, universe_name: str, trade_date: str, df: pd.DataFrame) -> None:
    trade_date = _date_str(trade_date)
    partition_dir = (
        data_dir / "stock" / "universe" / f"name={universe_name}" / f"date={trade_date}"
    )
    partition_dir.mkdir(parents=True, exist_ok=True)
    if "trade_date" in df.columns:
        # 与其他表保持一致：trade_date 统一存为 YYYYMMDD 字符串
        df = df.assign(
            trade_date=pd.to_datetime(df["trade_date"]).dt.strftime("%Y%m%d")
        )
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, partition_dir / "data.parquet")


def write_trade_cal(data_dir: Path, exchange: str, df: pd.DataFrame) -> None:
    partition_dir = data_dir / "stock" / "trade_cal" / f"exchange={exchange}"
    partition_dir.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, partition_dir / "data.parquet")


def read_trade_cal(data_dir: Path, exchange: str) -> pd.DataFrame:
    path = data_dir / "stock" / "trade_cal" / f"exchange={exchange}" / "data.parquet"
    if not path.exists():
        return pd.DataFrame()
    return pq.read_table(path, schema=pq.read_schema(path)).to_pandas()


class DailyPartitionStore:
    """Reads and writes Parquet files partitioned by date=YYYYMMDD.

    write()  整段替换单个分区 (job 逐日写入用; 分区小, 内存无压力)
    merge()  按业务主键去重合并 + schema 对齐 + row-group 流式写 (bulk 吸收来的
             安全路径; 重跑可幂等, 不会因追加造成重复行)
    """

    def __init__(self, table_dir: Path):
        self._dir = table_dir

    def write(self, trade_date: str, df: pd.DataFrame) -> None:
        partition_dir = self._dir / f"date={trade_date}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pandas(df, preserve_index=False), partition_dir / "data.parquet")

    def merge(
        self,
        df: pd.DataFrame,
        *,
        date_col: str = "trade_date",
        keys: list[str] | None = None,
        name: str = "",
        row_group_size: int = 1_000_000,
    ) -> int:
        """把 df 按 date_col 合并进 date=* 分区, 返回写入的分区数。

        与 write() 的差别 (也是从 bulk 吸收来的必要机制):
          1. 已存在分区: 读旧分区 → 按 keys 去重 (keep=last, 新数据覆盖旧)
             → 原子替换。整表 load 单分区即可, 单分区量级在百万行内。
          2. schema 对齐: 新数据列集与既有分区不一致时, 以既有分区 schema 为
             权威 (同表 schema 唯一), 缺失列补 null、多出列扩展既有分区。
          3. 原子替换: 先写 .tmp 再 os.replace, 旧文件读不出时跳过该分区,
             绝不覆盖坏数据。
        """
        if df is None or len(df) == 0 or date_col not in df.columns:
            logger.info(f"  ⚠️ {name or self._dir.name}: 无可写分区数据 ({date_col} 列缺失或空)")
            return 0
        keys = keys or partition_dedup_keys(df, date_col)
        df = self._align_schema(df, name=name)
        dcol = df[date_col].astype(str).str.replace("-", "").str.slice(0, 8)
        wrote = rows = 0
        for d, grp in df.groupby(dcol, sort=True):
            fp = self._dir / f"date={d}" / "data.parquet"
            try:
                if fp.exists():
                    old = pq.read_table(fp).to_pandas()
                    grp = match_dtypes(grp, old)
                    merged = (
                        pd.concat([old, grp], ignore_index=True)
                        .drop_duplicates(subset=keys, keep="last")
                    )
                else:
                    merged = grp
                fp.parent.mkdir(parents=True, exist_ok=True)
                tmp = fp.with_suffix(".parquet.tmp")
                pq.write_table(
                    pa.Table.from_pandas(merged, preserve_index=False),
                    tmp,
                    row_group_size=row_group_size,
                )
                os.replace(tmp, fp)
                wrote += 1
                rows += len(merged)
            except Exception as e:
                logger.info(f"  ⚠️ {name or self._dir.name}: 分区 {d} 写入失败 {str(e)[:80]}")
        logger.info(f"  ✅ {name or self._dir.name}: {rows:,} 行 → {wrote} 个 date=* 分区 ({self._dir})")
        return wrote

    def _align_schema(self, df: pd.DataFrame, name: str = "") -> pd.DataFrame:
        """以既有分区 schema 为权威对齐新数据列集, 保证同表 schema 唯一。

        新数据是既有分区的超集 (接口新增列): 扩展既有分区并保留新列 — 否则
        新增列每批都被裁掉, 数据永久丢失。
        新数据列更少: 按既有分区补齐 (缺失列填 null), 不破坏既有分区。
        """
        import glob as _glob

        existing = sorted(_glob.glob(str(self._dir / "date=*" / "data.parquet")))
        want = None
        if existing:
            try:
                want = list(pq.ParquetFile(existing[-1]).schema_arrow.names)
            except Exception:
                want = None
        if want is None:
            return df
        new_cols = list(df.columns)
        if new_cols == want:
            return df
        extra_new = [c for c in new_cols if c not in want]
        missing_new = [c for c in want if c not in new_cols]
        if extra_new and not missing_new:
            target = want + extra_new
            logger.info(f"  🔧 {name}: 既有分区少列 {extra_new} → 扩展分区 schema (回填空值)")
            _extend_partition_schema(self._dir, want, target)
            return df[target]
        if not new_cols:
            return df
        keep = [c for c in want if c in new_cols]
        missing = [c for c in want if c not in new_cols]
        if not keep:
            return df
        out = df[keep].copy()
        for c in missing:
            out[c] = None
        logger.info(
            f"  🔧 {name}: 列集对齐分区 schema "
            f"(+{len(missing)} 缺失列填空 / -{len(new_cols) - len(keep)} 多出列裁掉)"
        )
        return out[want]

    def exists(self, trade_date: str) -> bool:
        return (self._dir / f"date={trade_date}" / "data.parquet").exists()

    def read(self, trade_date: str) -> pd.DataFrame:
        path = self._dir / f"date={trade_date}" / "data.parquet"
        if not path.exists():
            return pd.DataFrame()
        return pq.read_table(path).to_pandas()


class SnapshotStore:
    """Reads and writes a single Parquet snapshot file."""

    def __init__(self, file_path: Path):
        self._path = file_path

    def write(self, df: pd.DataFrame) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pandas(df, preserve_index=False), self._path)

    def read(self) -> pd.DataFrame:
        if not self._path.exists():
            return pd.DataFrame()
        return pq.read_table(self._path).to_pandas()


class IndexWeightStore:
    """Reads and writes Parquet files partitioned by index_code=X/date=YYYYMMDD."""

    def __init__(self, index_weight_dir: Path):
        self._dir = index_weight_dir

    def write(self, index_code: str, trade_date: str, df: pd.DataFrame) -> None:
        partition_dir = self._dir / f"index_code={index_code}" / f"date={trade_date}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pandas(df, preserve_index=False), partition_dir / "data.parquet")

    def exists(self, index_code: str, trade_date: str) -> bool:
        return (self._dir / f"index_code={index_code}" / f"date={trade_date}" / "data.parquet").exists()
