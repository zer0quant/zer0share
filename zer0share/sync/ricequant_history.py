"""RiceQuant history sync utilities and manifest tracking."""

from __future__ import annotations

import calendar
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb
from loguru import logger

from zer0share.notifier import NullNotifier
from zer0share.storage import DailyPartitionStore


def parse_bytes(value: str | None) -> int | None:
    """Parse strings like '50G', '512M', '1024K', '100B' (or bare int) to bytes.

    Returns None if value is None or empty string.
    Suffixes: G/GiB=1024^3, M/MiB=1024^2, K/KiB=1024, B=1
    """
    if value is None or value == "":
        return None
    value = value.strip()
    _suffixes = {
        "G": 1024**3,
        "GIB": 1024**3,
        "M": 1024**2,
        "MIB": 1024**2,
        "K": 1024,
        "KIB": 1024,
        "B": 1,
    }
    upper = value.upper()
    for suffix, multiplier in _suffixes.items():
        if upper.endswith(suffix):
            number_part = value[: -len(suffix)]
            return int(float(number_part) * multiplier)
    # bare integer
    return int(value)


def month_chunks(start_date: str, end_date: str) -> list[tuple[str, str]]:
    """Split [start_date, end_date] into month-aligned chunks.

    Each chunk is (chunk_start, chunk_end) in YYYYMMDD format.
    First chunk starts at start_date, last chunk ends at end_date.
    Month boundaries: chunk ends on last day of the month, next starts on
    first of next month.

    Example:
        month_chunks("20260506", "20260621") == [
            ("20260506", "20260531"),
            ("20260601", "20260621"),
        ]
    """
    start_y = int(start_date[:4])
    start_m = int(start_date[4:6])
    start_d = int(start_date[6:])
    end_y = int(end_date[:4])
    end_m = int(end_date[4:6])

    chunks: list[tuple[str, str]] = []
    cur_y, cur_m, cur_d = start_y, start_m, start_d

    while True:
        # last day of current month
        last_day = calendar.monthrange(cur_y, cur_m)[1]
        chunk_start = f"{cur_y:04d}{cur_m:02d}{cur_d:02d}"

        if (cur_y, cur_m) >= (end_y, end_m):
            # final chunk
            chunks.append((chunk_start, end_date))
            break
        else:
            chunk_end = f"{cur_y:04d}{cur_m:02d}{last_day:02d}"
            chunks.append((chunk_start, chunk_end))
            # advance to first day of next month
            if cur_m == 12:
                cur_y += 1
                cur_m = 1
            else:
                cur_m += 1
            cur_d = 1

    return chunks


_CREATE_DAYS_TABLE = """
CREATE TABLE IF NOT EXISTS ricequant_history_days (
    trade_date        VARCHAR PRIMARY KEY,
    status            VARCHAR,
    rows              INTEGER,
    symbols           INTEGER,
    parquet_size      BIGINT,
    bytes_used_before BIGINT,
    bytes_used_after  BIGINT,
    elapsed_seconds   DOUBLE,
    error             VARCHAR,
    reason            VARCHAR,
    recorded_at       TIMESTAMP
)
"""

_CREATE_CHUNKS_TABLE = """
CREATE TABLE IF NOT EXISTS ricequant_history_chunks (
    chunk_id    VARCHAR PRIMARY KEY,
    start_date  VARCHAR,
    end_date    VARCHAR,
    status      VARCHAR,
    recorded_at TIMESTAMP
)
"""


class RiceQuantHistoryManifest:
    """Tracks historical sync state in a DuckDB database."""

    def __init__(self, db_path: Path) -> None:
        """Open DuckDB at db_path, create tables if missing."""
        db_path = Path(db_path)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = duckdb.connect(str(db_path))
        self._conn.execute(_CREATE_DAYS_TABLE)
        self._conn.execute(_CREATE_CHUNKS_TABLE)

    def record_day_success(
        self,
        trade_date: str,
        rows: int,
        symbols: int,
        parquet_size: int,
        bytes_used_before: int,
        bytes_used_after: int,
        elapsed_seconds: float,
    ) -> None:
        """Upsert a successful day record."""
        now = datetime.now(timezone.utc)
        self._conn.execute(
            """
            INSERT INTO ricequant_history_days
                (trade_date, status, rows, symbols, parquet_size,
                 bytes_used_before, bytes_used_after, elapsed_seconds,
                 error, reason, recorded_at)
            VALUES (?, 'success', ?, ?, ?, ?, ?, ?, NULL, NULL, ?)
            ON CONFLICT (trade_date) DO UPDATE SET
                status            = excluded.status,
                rows              = excluded.rows,
                symbols           = excluded.symbols,
                parquet_size      = excluded.parquet_size,
                bytes_used_before = excluded.bytes_used_before,
                bytes_used_after  = excluded.bytes_used_after,
                elapsed_seconds   = excluded.elapsed_seconds,
                error             = excluded.error,
                reason            = excluded.reason,
                recorded_at       = excluded.recorded_at
            """,
            [trade_date, rows, symbols, parquet_size,
             bytes_used_before, bytes_used_after, elapsed_seconds, now],
        )

    def record_day_skipped(self, trade_date: str, reason: str) -> None:
        """Record a skipped day."""
        now = datetime.now(timezone.utc)
        self._conn.execute(
            """
            INSERT INTO ricequant_history_days
                (trade_date, status, rows, symbols, parquet_size,
                 bytes_used_before, bytes_used_after, elapsed_seconds,
                 error, reason, recorded_at)
            VALUES (?, 'skipped', NULL, NULL, NULL, NULL, NULL, NULL, NULL, ?, ?)
            ON CONFLICT (trade_date) DO UPDATE SET
                status      = excluded.status,
                reason      = excluded.reason,
                recorded_at = excluded.recorded_at
            """,
            [trade_date, reason, now],
        )

    def record_day_failure(self, trade_date: str, error: str) -> None:
        """Record a failed day."""
        now = datetime.now(timezone.utc)
        self._conn.execute(
            """
            INSERT INTO ricequant_history_days
                (trade_date, status, rows, symbols, parquet_size,
                 bytes_used_before, bytes_used_after, elapsed_seconds,
                 error, reason, recorded_at)
            VALUES (?, 'failed', NULL, NULL, NULL, NULL, NULL, NULL, ?, NULL, ?)
            ON CONFLICT (trade_date) DO UPDATE SET
                status      = excluded.status,
                error       = excluded.error,
                recorded_at = excluded.recorded_at
            """,
            [trade_date, error, now],
        )

    def get_day(self, trade_date: str) -> dict | None:
        """Return a dict with at minimum 'status', 'rows' keys. None if not found."""
        row = self._conn.execute(
            "SELECT * FROM ricequant_history_days WHERE trade_date = ?",
            [trade_date],
        ).fetchone()
        if row is None:
            return None
        cols = [desc[0] for desc in self._conn.description]
        return dict(zip(cols, row))

    def is_day_done(self, trade_date: str) -> bool:
        """Return True if trade_date has status='success' or status='skipped'."""
        row = self._conn.execute(
            "SELECT status FROM ricequant_history_days WHERE trade_date = ?",
            [trade_date],
        ).fetchone()
        if row is None:
            return False
        return row[0] in ("success", "skipped")

    def close(self) -> None:
        self._conn.close()

    def __del__(self) -> None:
        try:
            self._conn.close()
        except Exception:
            pass

    def __enter__(self) -> "RiceQuantHistoryManifest":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self.close()
        return False


class RiceQuantHistoryRunner:
    """Orchestrates day-by-day historical sync for RiceQuant stock minute data."""

    def __init__(
        self,
        pipeline,
        manifest: RiceQuantHistoryManifest,
        calendar,
        data_dir: Path,
        notifier,
        get_quota=None,
    ) -> None:
        self._pipeline = pipeline
        self._manifest = manifest
        self._calendar = calendar
        self._data_dir = Path(data_dir)
        self._notifier = notifier
        self._get_quota = get_quota
        self._store = DailyPartitionStore(
            self._data_dir / "ricequant" / "stock_minute"
        )

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def run(
        self,
        start_date: str,
        end_date: str,
        chunk: str = "month",
        max_bytes: int | None = None,
        stop_remaining_below: int | None = None,
        retries: int = 3,
    ) -> None:
        """Sync historical data from start_date to end_date, one day at a time."""
        logger.info(f"开始同步 {start_date}~{end_date}")
        self._notifier.notify_start("ricequant_history", {"range": f"{start_date}~{end_date}"})

        chunks = month_chunks(start_date, end_date)

        for chunk_start, chunk_end in chunks:
            logger.info(f"月份 {chunk_start}~{chunk_end} 开始")
            _chunk_t0 = time.monotonic()
            _synced = 0
            _skipped = 0

            trading_days = self._calendar.get_trading_days("SSE", chunk_start, chunk_end)

            for trade_date in trading_days:
                stop = self._sync_day(
                    trade_date,
                    max_bytes=max_bytes,
                    stop_remaining_below=stop_remaining_below,
                    retries=retries,
                )
                day_row = self._manifest.get_day(trade_date)
                if day_row and day_row.get("status") == "success":
                    _synced += 1
                elif day_row and day_row.get("status") == "skipped":
                    _skipped += 1
                if stop:
                    logger.info(f"完成 {start_date}~{end_date}")
                    return

            _elapsed = time.monotonic() - _chunk_t0
            logger.info(f"月份 {chunk_start}~{chunk_end} 完成")
            self._notifier.notify_stage_done(
                chunk_start,
                {"同步": str(_synced), "跳过": str(_skipped)},
                round(_elapsed, 1),
            )

        logger.info(f"完成 {start_date}~{end_date}")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _run_pipeline_silently(self, trade_date: str) -> None:
        runtime = getattr(self._pipeline, "_runtime", None)
        if runtime is None or not hasattr(runtime, "notifier"):
            self._pipeline.run("ricequant_stock_minute", trade_date, trade_date)
            return

        original_notifier = runtime.notifier
        runtime.notifier = NullNotifier()
        try:
            self._pipeline.run("ricequant_stock_minute", trade_date, trade_date)
        finally:
            runtime.notifier = original_notifier

    def _sync_day(
        self,
        trade_date: str,
        max_bytes: int | None,
        stop_remaining_below: int | None,
        retries: int,
    ) -> bool:
        """Sync a single trading day.  Returns True if the caller should stop."""
        # Skip if already recorded in manifest
        if self._manifest.is_day_done(trade_date):
            return False

        # Skip if parquet partition exists and has rows
        if self._store.exists(trade_date):
            df = self._store.read(trade_date)
            if len(df) > 0:
                logger.info(f"跳过 {trade_date}: existing partition")
                self._manifest.record_day_skipped(trade_date, "existing partition")
                return False

        logger.info(f"开始同步 {trade_date}")

        # Log quota before
        bytes_used_before = 0
        if self._get_quota is not None:
            used, remaining = self._get_quota()
            bytes_used_before = used
            logger.info(f"quota: used={used} remaining={remaining}")

        # Attempt sync with retries
        t_start = time.monotonic()
        last_error: Exception | None = None

        for attempt in range(retries):
            try:
                self._run_pipeline_silently(trade_date)
                last_error = None
                break
            except Exception as exc:
                last_error = exc
                logger.warning(f"失败 {trade_date}: {exc}")
                if attempt < retries - 1:
                    time.sleep(0.5)

        elapsed = time.monotonic() - t_start

        if last_error is not None:
            logger.error(f"放弃 {trade_date}: {last_error}")
            self._manifest.record_day_failure(trade_date, str(last_error))
            self._notifier.notify_error("ricequant_history", f"{trade_date}: {last_error}")
            return False

        # Collect parquet stats
        rows, symbols, parquet_size = 0, 0, 0
        if self._store.exists(trade_date):
            df = self._store.read(trade_date)
            rows = len(df)
            symbols = df["order_book_id"].nunique() if "order_book_id" in df.columns else 0
            parquet_path = (
                self._data_dir / "ricequant" / "stock_minute"
                / f"date={trade_date}" / "data.parquet"
            )
            parquet_size = parquet_path.stat().st_size if parquet_path.exists() else 0

        # Log quota after and check stop conditions
        bytes_used_after = bytes_used_before
        if self._get_quota is not None:
            used, remaining = self._get_quota()
            bytes_used_after = used
            logger.info(f"quota: used={used} remaining={remaining}")

            if max_bytes is not None and used >= max_bytes:
                logger.info(f"已达 max_bytes={max_bytes}，停止同步")
                self._manifest.record_day_success(
                    trade_date, rows, symbols, parquet_size,
                    bytes_used_before, bytes_used_after, elapsed,
                )
                logger.info(f"完成 {trade_date}")
                return True

            if stop_remaining_below is not None and remaining < stop_remaining_below:
                logger.info(f"剩余 {remaining} < stop_remaining_below={stop_remaining_below}，停止同步")
                self._manifest.record_day_success(
                    trade_date, rows, symbols, parquet_size,
                    bytes_used_before, bytes_used_after, elapsed,
                )
                logger.info(f"完成 {trade_date}")
                return True

        self._manifest.record_day_success(
            trade_date, rows, symbols, parquet_size,
            bytes_used_before, bytes_used_after, elapsed,
        )
        logger.info(f"完成 {trade_date}")
        return False
