# Futures Data Batch 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add 6 core futures data types (fut_basic, fut_daily, fut_holding, fut_wsr, fut_settle, fut_mapping) to zer0share, fully integrated across all layers.

**Architecture:** Direct extension of existing modules (fetcher, pipeline, api, scheduler, cli). Futures data stored in `data/futures/` subdirectory using the same date-partitioned parquet pattern. Reuse existing generic storage functions (`write_daily_partition`, `daily_partition_exists`, `read_daily_partition`) by passing the futures subdirectory as base path. Extend `_sync_daily_partitioned` and `_query_daily_partitioned` with `data_dir` override parameters.

**Tech Stack:** Python 3.11+, tushare, duckdb, pyarrow, apscheduler, click, pytest

---

### Task 1: Fetcher — Constants and Fetch Methods

**Files:**
- Modify: `zer0share/fetcher.py` (add after `INDEX_DAILY_COLS` block, around line 89)
- Modify: `tests/test_fetcher.py` (add at end of file)

- [ ] **Step 1: Add futures column constants and FUTURES_EXCHANGES to fetcher.py**

Add after `INDEX_DAILY_COLS` (line 89), before the `TushareFetcher` class:

```python
FUTURES_EXCHANGES = ["CZCE", "SHFE", "DCE", "CFFEX", "INE", "GFEX"]

FUT_BASIC_COLS = [
    "ts_code", "symbol", "exchange", "name", "fut_code",
    "multiplier", "trade_unit", "per_unit", "quote_unit",
    "quote_unit_desc", "d_mode_desc", "list_date", "delist_date",
    "d_month", "last_ddate", "trade_time_desc",
]

FUT_DAILY_COLS = [
    "ts_code", "trade_date", "pre_close", "pre_settle",
    "open", "high", "low", "close", "settle",
    "change1", "change2", "vol", "amount", "oi", "oi_chg",
    "delv_settle",
]

FUT_HOLDING_COLS = [
    "trade_date", "symbol", "broker", "vol", "vol_chg",
    "long_hld", "long_chg", "short_hld", "short_chg", "exchange",
]

FUT_WSR_COLS = [
    "trade_date", "symbol", "fut_name", "warehouse", "wh_id",
    "pre_vol", "vol", "vol_chg", "area", "year", "grade",
    "brand", "place", "pd", "is_ct", "unit", "exchange",
]

FUT_SETTLE_COLS = [
    "ts_code", "trade_date", "settle", "trading_fee_rate",
    "trading_fee", "delivery_fee", "b_hedging_margin_rate",
    "s_hedging_margin_rate", "long_margin_rate", "short_margin_rate",
    "offset_today_fee", "exchange",
]

FUT_MAPPING_COLS = [
    "ts_code", "trade_date", "mapping_ts_code",
]
```

- [ ] **Step 2: Add fetch_fut_basic method to TushareFetcher class**

Add after `fetch_trade_cal` method (around line 222):

```python
    def fetch_fut_basic(self, exchange: str, fut_type: str = "1") -> pd.DataFrame:
        logger.debug(f"拉取期货合约: exchange={exchange}, fut_type={fut_type}")
        df = self._pro.fut_basic(
            exchange=exchange,
            fut_type=fut_type,
            fields=",".join(FUT_BASIC_COLS),
        )
        if df is None or df.empty:
            return pd.DataFrame(columns=FUT_BASIC_COLS)
        for col in ("list_date", "delist_date", "d_month", "last_ddate"):
            if col in df.columns:
                df[col] = pd.to_datetime(
                    df[col], format="%Y%m%d", errors="coerce"
                ).apply(lambda x: x.date() if not pd.isna(x) and not pd.isnull(x) else None)
        return df[FUT_BASIC_COLS]
```

- [ ] **Step 3: Add fetch_fut_daily, fetch_fut_holding, fetch_fut_wsr, fetch_fut_settle, fetch_fut_mapping methods**

Add after `fetch_fut_basic`:

```python
    def fetch_fut_daily(self, trade_date: date) -> pd.DataFrame:
        date_str = trade_date.strftime("%Y%m%d")
        logger.debug(f"拉取期货日线: {date_str}")
        df = self._pro.fut_daily(trade_date=date_str, fields=",".join(FUT_DAILY_COLS))
        return _format_trade_date(df, FUT_DAILY_COLS)

    def fetch_fut_holding(self, trade_date: date) -> pd.DataFrame:
        date_str = trade_date.strftime("%Y%m%d")
        logger.debug(f"拉取期货持仓排名: {date_str}")
        df = self._pro.fut_holding(trade_date=date_str, fields=",".join(FUT_HOLDING_COLS))
        return _format_trade_date(df, FUT_HOLDING_COLS)

    def fetch_fut_wsr(self, trade_date: date) -> pd.DataFrame:
        date_str = trade_date.strftime("%Y%m%d")
        logger.debug(f"拉取期货仓单: {date_str}")
        df = self._pro.fut_wsr(trade_date=date_str, fields=",".join(FUT_WSR_COLS))
        return _format_trade_date(df, FUT_WSR_COLS)

    def fetch_fut_settle(self, trade_date: date) -> pd.DataFrame:
        date_str = trade_date.strftime("%Y%m%d")
        logger.debug(f"拉取期货结算参数: {date_str}")
        df = self._pro.fut_settle(trade_date=date_str, fields=",".join(FUT_SETTLE_COLS))
        return _format_trade_date(df, FUT_SETTLE_COLS)

    def fetch_fut_mapping(self, trade_date: date) -> pd.DataFrame:
        date_str = trade_date.strftime("%Y%m%d")
        logger.debug(f"拉取期货主力映射: {date_str}")
        df = self._pro.fut_mapping(trade_date=date_str, fields=",".join(FUT_MAPPING_COLS))
        return _format_trade_date(df, FUT_MAPPING_COLS)
```

- [ ] **Step 4: Write tests for futures fetcher methods**

Add at the end of `tests/test_fetcher.py`:

```python
# --- Futures tests ---

from zer0share.fetcher import (
    FUT_BASIC_COLS, FUT_DAILY_COLS, FUT_HOLDING_COLS,
    FUT_WSR_COLS, FUT_SETTLE_COLS, FUT_MAPPING_COLS,
    FUTURES_EXCHANGES,
)


def _fut_basic_row(exchange: str = "SHFE", list_date: str = "20240101") -> dict:
    return {
        "ts_code": "CU2401.SHF",
        "symbol": "CU2401",
        "exchange": exchange,
        "name": "沪铜2401",
        "fut_code": "CU",
        "multiplier": None,
        "trade_unit": "5吨/手",
        "per_unit": 5.0,
        "quote_unit": "元(人民币)/吨",
        "quote_unit_desc": "10元/吨",
        "d_mode_desc": "实物交割",
        "list_date": list_date,
        "delist_date": "20240115",
        "d_month": "202401",
        "last_ddate": "20240115",
        "trade_time_desc": None,
    }


def _fut_daily_row(ts_code: str = "CU2401.SHF", trade_date: str = "20240102") -> dict:
    return {
        "ts_code": ts_code,
        "trade_date": trade_date,
        "pre_close": 50000.0,
        "pre_settle": 50100.0,
        "open": 50200.0,
        "high": 50500.0,
        "low": 49900.0,
        "close": 50300.0,
        "settle": 50250.0,
        "change1": 200.0,
        "change2": 150.0,
        "vol": 10000.0,
        "amount": 251250.0,
        "oi": 50000.0,
        "oi_chg": 500.0,
        "delv_settle": None,
    }


def test_futures_exchanges_has_six_entries():
    assert len(FUTURES_EXCHANGES) == 6
    assert "CZCE" in FUTURES_EXCHANGES
    assert "SHFE" in FUTURES_EXCHANGES
    assert "DCE" in FUTURES_EXCHANGES
    assert "CFFEX" in FUTURES_EXCHANGES
    assert "INE" in FUTURES_EXCHANGES
    assert "GFEX" in FUTURES_EXCHANGES


def test_fetch_fut_basic_returns_correct_columns(mock_pro):
    mock_pro.fut_basic.return_value = pd.DataFrame([_fut_basic_row()])
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_fut_basic("SHFE", "1")

    assert list(df.columns) == FUT_BASIC_COLS
    assert len(df) == 1


def test_fetch_fut_basic_converts_dates(mock_pro):
    mock_pro.fut_basic.return_value = pd.DataFrame([_fut_basic_row()])
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_fut_basic("SHFE", "1")

    assert df.iloc[0]["list_date"] == date(2024, 1, 1)
    assert df.iloc[0]["delist_date"] == date(2024, 1, 15)


def test_fetch_fut_basic_returns_empty_when_none(mock_pro):
    mock_pro.fut_basic.return_value = None
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_fut_basic("SHFE", "1")

    assert df.empty
    assert list(df.columns) == FUT_BASIC_COLS


def test_fetch_fut_basic_calls_api_correctly(mock_pro):
    mock_pro.fut_basic.return_value = pd.DataFrame([_fut_basic_row()])
    fetcher = TushareFetcher("fake_token")

    fetcher.fetch_fut_basic("DCE", "2")

    mock_pro.fut_basic.assert_called_once_with(
        exchange="DCE",
        fut_type="2",
        fields=",".join(FUT_BASIC_COLS),
    )


def test_fetch_fut_daily_returns_correct_columns(mock_pro):
    mock_pro.fut_daily.return_value = pd.DataFrame([_fut_daily_row()])
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_fut_daily(date(2024, 1, 2))

    assert list(df.columns) == FUT_DAILY_COLS
    assert len(df) == 1
    assert df.iloc[0]["trade_date"] == date(2024, 1, 2)


def test_fetch_fut_daily_returns_empty_when_none(mock_pro):
    mock_pro.fut_daily.return_value = None
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_fut_daily(date(2024, 1, 1))

    assert df.empty
    assert list(df.columns) == FUT_DAILY_COLS


def test_fetch_fut_daily_calls_api_with_date(mock_pro):
    mock_pro.fut_daily.return_value = pd.DataFrame([_fut_daily_row()])
    fetcher = TushareFetcher("fake_token")

    fetcher.fetch_fut_daily(date(2024, 1, 2))

    mock_pro.fut_daily.assert_called_once_with(
        trade_date="20240102",
        fields=",".join(FUT_DAILY_COLS),
    )


def test_fetch_fut_holding_returns_correct_columns(mock_pro):
    mock_pro.fut_holding.return_value = pd.DataFrame({
        "trade_date": ["20240102"],
        "symbol": ["CU"],
        "broker": ["中信期货"],
        "vol": [10000],
        "vol_chg": [500],
        "long_hld": [15000],
        "long_chg": [200],
        "short_hld": [12000],
        "short_chg": [-100],
        "exchange": ["SHFE"],
    })
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_fut_holding(date(2024, 1, 2))

    assert list(df.columns) == FUT_HOLDING_COLS
    assert len(df) == 1


def test_fetch_fut_wsr_returns_correct_columns(mock_pro):
    mock_pro.fut_wsr.return_value = pd.DataFrame({
        "trade_date": ["20240102"],
        "symbol": ["CU"],
        "fut_name": ["沪铜"],
        "warehouse": ["仓库A"],
        "wh_id": ["WH001"],
        "pre_vol": [100],
        "vol": [120],
        "vol_chg": [20],
        "area": ["上海"],
        "year": ["2024"],
        "grade": [None],
        "brand": [None],
        "place": [None],
        "pd": [0],
        "is_ct": ["N"],
        "unit": ["吨"],
        "exchange": ["SHFE"],
    })
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_fut_wsr(date(2024, 1, 2))

    assert list(df.columns) == FUT_WSR_COLS
    assert len(df) == 1


def test_fetch_fut_settle_returns_correct_columns(mock_pro):
    mock_pro.fut_settle.return_value = pd.DataFrame({
        "ts_code": ["CU2401.SHF"],
        "trade_date": ["20240102"],
        "settle": [50250.0],
        "trading_fee_rate": [None],
        "trading_fee": [None],
        "delivery_fee": [None],
        "b_hedging_margin_rate": [0.08],
        "s_hedging_margin_rate": [0.08],
        "long_margin_rate": [0.10],
        "short_margin_rate": [0.10],
        "offset_today_fee": [None],
        "exchange": ["SHFE"],
    })
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_fut_settle(date(2024, 1, 2))

    assert list(df.columns) == FUT_SETTLE_COLS
    assert len(df) == 1


def test_fetch_fut_mapping_returns_correct_columns(mock_pro):
    mock_pro.fut_mapping.return_value = pd.DataFrame({
        "ts_code": ["CU.SHF"],
        "trade_date": ["20240102"],
        "mapping_ts_code": ["CU2401.SHF"],
    })
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_fut_mapping(date(2024, 1, 2))

    assert list(df.columns) == FUT_MAPPING_COLS
    assert len(df) == 1
    assert df.iloc[0]["trade_date"] == date(2024, 1, 2)
```

- [ ] **Step 5: Run tests to verify**

Run: `cd /data/projects/zer0share && python -m pytest tests/test_fetcher.py -v -k "futures or fut_" 2>&1 | tail -30`
Expected: All new tests PASS

- [ ] **Step 6: Run full test suite to ensure no regressions**

Run: `cd /data/projects/zer0share && python -m pytest tests/test_fetcher.py -v 2>&1 | tail -40`
Expected: All tests PASS (existing + new)

- [ ] **Step 7: Commit**

```bash
git add zer0share/fetcher.py tests/test_fetcher.py
git commit -m "feat: add futures fetcher constants and 6 fetch methods"
```

---

### Task 2: Pipeline — Sync Methods for Futures Data

**Files:**
- Modify: `zer0share/pipeline.py`
- Modify: `tests/test_pipeline.py`

**Key insight:** The existing `_sync_daily_partitioned` method uses `self._cfg.data_dir` directly. We add a `data_dir` override parameter so futures sync methods can pass `self._cfg.data_dir / "futures"`. The generic `write_daily_partition`, `daily_partition_exists`, `read_daily_partition` from `storage.py` work as-is with the futures subdirectory.

- [ ] **Step 1: Add data_dir parameter to `_sync_daily_partitioned`**

In `zer0share/pipeline.py`, change the `_sync_daily_partitioned` method signature (around line 551) to accept a `data_dir` override:

Replace:
```python
    def _sync_daily_partitioned(
        self,
        table_name: str,
        fetch,
        start_date: date | None,
        end_date: date | None,
        write_empty: bool = False,
    ) -> None:
        today = date.today()
        last = self._meta.get_last_date(table_name)
        if start_date is None:
            start = (last + timedelta(days=1)) if last else FIRST_DATE
            end = today
        else:
            start = start_date
            end = end_date or today

        if start_date is None and start > end:
            logger.info(f"{table_name} 已是最新，无需同步")
            return
        if start > end:
            raise ValueError("start_date must be on or before end_date")

        trading_days = self._meta.get_trading_days("SSE", start, end)
        if not trading_days and self._meta.get_last_date("trade_cal") is None:
            raise RuntimeError(
                "DuckDB 中无 SSE trade_cal 数据，请先运行 "
                "python main.py sync --table trade_cal"
            )
        if not trading_days:
            logger.info("指定范围内无交易日，无需同步")
            return

        success = 0
        empty = 0
        skipped_existing = 0
        frontier = last
        logger.info(
            f"{table_name} 同步开始: {start} ~ {end}, 共 {len(trading_days)} 个交易日"
        )
        for processed, trade_date in enumerate(trading_days, start=1):
            if daily_partition_exists(self._cfg.data_dir, table_name, trade_date):
                skipped_existing += 1
                if _should_log_progress(processed, len(trading_days)):
                    _log_daily_progress(
                        table_name,
                        processed,
                        len(trading_days),
                        trade_date,
                        success,
                        empty,
                        skipped_existing,
                    )
                continue
            try:
                df = fetch(trade_date)
                time.sleep(0.2)
                if not df.empty or write_empty:
                    write_daily_partition(self._cfg.data_dir, table_name, trade_date, df)
                    if frontier is None or trade_date > frontier:
                        self._meta.update_last_date(table_name, trade_date)
                        frontier = trade_date
                    if df.empty:
                        empty += 1
                    else:
                        success += 1
                else:
                    empty += 1
            except Exception as e:
                logger.error(f"{table_name} {trade_date} 同步失败: {e}")
                self._notifier.send(f"{table_name} {trade_date} 同步失败: {e}")
                raise
            if _should_log_progress(processed, len(trading_days)):
                _log_daily_progress(
                    table_name,
                    processed,
                    len(trading_days),
                    trade_date,
                    success,
                    empty,
                    skipped_existing,
                )

        msg = (
            f"{table_name} 同步完成: 成功 {success} 天, "
            f"空数据 {empty} 天, 跳过已存在 {skipped_existing} 天, "
            f"共 {len(trading_days)} 个交易日"
        )
        logger.info(msg)
        self._notifier.send(msg)
```

With:
```python
    def _sync_daily_partitioned(
        self,
        table_name: str,
        fetch,
        start_date: date | None,
        end_date: date | None,
        write_empty: bool = False,
        data_dir: Path | None = None,
    ) -> None:
        base_dir = data_dir or self._cfg.data_dir
        today = date.today()
        last = self._meta.get_last_date(table_name)
        if start_date is None:
            start = (last + timedelta(days=1)) if last else FIRST_DATE
            end = today
        else:
            start = start_date
            end = end_date or today

        if start_date is None and start > end:
            logger.info(f"{table_name} 已是最新，无需同步")
            return
        if start > end:
            raise ValueError("start_date must be on or before end_date")

        trading_days = self._meta.get_trading_days("SSE", start, end)
        if not trading_days and self._meta.get_last_date("trade_cal") is None:
            raise RuntimeError(
                "DuckDB 中无 SSE trade_cal 数据，请先运行 "
                "python main.py sync --table trade_cal"
            )
        if not trading_days:
            logger.info("指定范围内无交易日，无需同步")
            return

        success = 0
        empty = 0
        skipped_existing = 0
        frontier = last
        logger.info(
            f"{table_name} 同步开始: {start} ~ {end}, 共 {len(trading_days)} 个交易日"
        )
        for processed, trade_date in enumerate(trading_days, start=1):
            if daily_partition_exists(base_dir, table_name, trade_date):
                skipped_existing += 1
                if _should_log_progress(processed, len(trading_days)):
                    _log_daily_progress(
                        table_name,
                        processed,
                        len(trading_days),
                        trade_date,
                        success,
                        empty,
                        skipped_existing,
                    )
                continue
            try:
                df = fetch(trade_date)
                time.sleep(0.2)
                if not df.empty or write_empty:
                    write_daily_partition(base_dir, table_name, trade_date, df)
                    if frontier is None or trade_date > frontier:
                        self._meta.update_last_date(table_name, trade_date)
                        frontier = trade_date
                    if df.empty:
                        empty += 1
                    else:
                        success += 1
                else:
                    empty += 1
            except Exception as e:
                logger.error(f"{table_name} {trade_date} 同步失败: {e}")
                self._notifier.send(f"{table_name} {trade_date} 同步失败: {e}")
                raise
            if _should_log_progress(processed, len(trading_days)):
                _log_daily_progress(
                    table_name,
                    processed,
                    len(trading_days),
                    trade_date,
                    success,
                    empty,
                    skipped_existing,
                )

        msg = (
            f"{table_name} 同步完成: 成功 {success} 天, "
            f"空数据 {empty} 天, 跳过已存在 {skipped_existing} 天, "
            f"共 {len(trading_days)} 个交易日"
        )
        logger.info(msg)
        self._notifier.send(msg)
```

Also add `Path` to the import at the top of `pipeline.py` — add `from pathlib import Path` after the existing `from datetime import date, timedelta` line.

- [ ] **Step 2: Add import of futures constants**

Add to the imports in `pipeline.py` (line 8):

```python
from zer0share.fetcher import TushareFetcher, INDEX_DAILY_CODES, FUTURES_EXCHANGES
```

- [ ] **Step 3: Add `sync_fut_basic` method to Pipeline class**

Add after the `sync_ci_member` method (around line 549):

```python
    def sync_fut_basic(self) -> None:
        today = date.today()
        futures_dir = self._cfg.data_dir / "futures"
        all_frames = []
        try:
            for exchange in FUTURES_EXCHANGES:
                for fut_type in ("1", "2"):
                    df = self._fetcher.fetch_fut_basic(exchange, fut_type)
                    time.sleep(0.2)
                    if not df.empty:
                        all_frames.append(df)
            if all_frames:
                combined = pd.concat(all_frames, ignore_index=True)
            else:
                combined = pd.DataFrame()
            write_daily_partition(futures_dir, "fut_basic", today, combined)
            self._meta.update_last_date("fut_basic", today)
            logger.info(f"fut_basic 同步完成: {len(combined)} 条")
        except Exception as e:
            logger.error(f"fut_basic 同步失败: {e}")
            self._notifier.send(f"fut_basic 同步失败: {e}")
            raise
```

- [ ] **Step 4: Add 5 daily futures sync methods**

Add after `sync_fut_basic`:

```python
    def sync_fut_daily(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> None:
        self._sync_daily_partitioned(
            table_name="fut_daily",
            fetch=self._fetcher.fetch_fut_daily,
            start_date=start_date,
            end_date=end_date,
            data_dir=self._cfg.data_dir / "futures",
        )

    def sync_fut_holding(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> None:
        self._sync_daily_partitioned(
            table_name="fut_holding",
            fetch=self._fetcher.fetch_fut_holding,
            start_date=start_date,
            end_date=end_date,
            data_dir=self._cfg.data_dir / "futures",
        )

    def sync_fut_wsr(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> None:
        self._sync_daily_partitioned(
            table_name="fut_wsr",
            fetch=self._fetcher.fetch_fut_wsr,
            start_date=start_date,
            end_date=end_date,
            data_dir=self._cfg.data_dir / "futures",
        )

    def sync_fut_settle(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> None:
        self._sync_daily_partitioned(
            table_name="fut_settle",
            fetch=self._fetcher.fetch_fut_settle,
            start_date=start_date,
            end_date=end_date,
            data_dir=self._cfg.data_dir / "futures",
        )

    def sync_fut_mapping(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> None:
        self._sync_daily_partitioned(
            table_name="fut_mapping",
            fetch=self._fetcher.fetch_fut_mapping,
            start_date=start_date,
            end_date=end_date,
            data_dir=self._cfg.data_dir / "futures",
        )
```

- [ ] **Step 5: Write tests for futures pipeline methods**

Add at the end of `tests/test_pipeline.py`:

```python
# --- Futures pipeline tests ---

from zer0share.fetcher import FUTURES_EXCHANGES


def test_sync_fut_basic_writes_to_futures_subdir(pipeline, cfg):
    def fut_basic_side_effect(exchange, fut_type):
        return pd.DataFrame({
            "ts_code": [f"CU2401.{exchange[:2]}"],
            "symbol": ["CU2401"],
            "exchange": [exchange],
            "name": ["沪铜2401"],
            "fut_code": ["CU"],
            "multiplier": [None],
            "trade_unit": ["5吨/手"],
            "per_unit": [5.0],
            "quote_unit": ["元/吨"],
            "quote_unit_desc": ["10元/吨"],
            "d_mode_desc": ["实物交割"],
            "list_date": [date(2024, 1, 1)],
            "delist_date": [date(2024, 1, 15)],
            "d_month": [None],
            "last_ddate": [None],
            "trade_time_desc": [None],
        })

    pipeline._fetcher.fetch_fut_basic.side_effect = fut_basic_side_effect

    with patch("zer0share.pipeline.time.sleep"), \
         patch("zer0share.pipeline.date") as mock_date:
        mock_date.today.return_value = date(2024, 1, 2)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        pipeline.sync_fut_basic()

    assert (cfg.data_dir / "futures" / "fut_basic" / "date=20240102" / "data.parquet").exists()
    assert pipeline._meta.get_last_date("fut_basic") == date(2024, 1, 2)


def test_sync_fut_basic_calls_all_exchanges_and_types(pipeline, cfg):
    pipeline._fetcher.fetch_fut_basic.return_value = pd.DataFrame()

    with patch("zer0share.pipeline.time.sleep"), \
         patch("zer0share.pipeline.date") as mock_date:
        mock_date.today.return_value = date(2024, 1, 2)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        pipeline.sync_fut_basic()

    assert pipeline._fetcher.fetch_fut_basic.call_count == len(FUTURES_EXCHANGES) * 2
    for exchange in FUTURES_EXCHANGES:
        pipeline._fetcher.fetch_fut_basic.assert_any_call(exchange, "1")
        pipeline._fetcher.fetch_fut_basic.assert_any_call(exchange, "2")


def test_sync_fut_basic_failure_sends_alert_and_raises(pipeline, cfg):
    pipeline._fetcher.fetch_fut_basic.side_effect = RuntimeError("API error")
    with pytest.raises(RuntimeError):
        pipeline.sync_fut_basic()
    pipeline._notifier.send.assert_called_once()
    msg = pipeline._notifier.send.call_args[0][0]
    assert "fut_basic 同步失败" in msg


def _setup_futures_trade_cal(pipeline, cfg):
    """Load SSE trade_cal with 2024-01-02 as open into DuckDB."""
    trade_cal = pd.DataFrame({
        "exchange": ["SSE"],
        "cal_date": [date(2024, 1, 2)],
        "is_open": [True],
        "pretrade_date": [date(2023, 12, 29)],
    })
    write_trade_cal(cfg.data_dir, "SSE", trade_cal)
    pipeline._meta.load_trade_cal_from_parquet(cfg.data_dir)
    pipeline._meta.update_last_date("trade_cal", date(2024, 1, 2))


def test_sync_fut_daily_writes_to_futures_subdir(pipeline, cfg):
    _setup_futures_trade_cal(pipeline, cfg)
    fut_df = pd.DataFrame({
        "ts_code": ["CU2401.SHF"],
        "trade_date": [date(2024, 1, 2)],
        "pre_close": [50000.0],
        "pre_settle": [50100.0],
        "open": [50200.0],
        "high": [50500.0],
        "low": [49900.0],
        "close": [50300.0],
        "settle": [50250.0],
        "change1": [200.0],
        "change2": [150.0],
        "vol": [10000.0],
        "amount": [251250.0],
        "oi": [50000.0],
        "oi_chg": [500.0],
        "delv_settle": [None],
    })
    pipeline._fetcher.fetch_fut_daily.return_value = fut_df
    pipeline._meta.update_last_date("fut_daily", date(2024, 1, 1))

    with patch("zer0share.pipeline.date") as mock_date, \
         patch("zer0share.pipeline.time.sleep"):
        mock_date.today.return_value = date(2024, 1, 2)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        pipeline.sync_fut_daily()

    assert (cfg.data_dir / "futures" / "fut_daily" / "date=20240102" / "data.parquet").exists()


def test_sync_fut_daily_skips_existing_partitions(pipeline, cfg):
    _setup_futures_trade_cal(pipeline, cfg)
    pipeline._fetcher.fetch_fut_daily.return_value = pd.DataFrame()
    pipeline._meta.update_last_date("fut_daily", date(2024, 1, 1))

    # Pre-create the partition
    from zer0share.storage import write_daily_partition
    write_daily_partition(
        cfg.data_dir / "futures", "fut_daily", date(2024, 1, 2),
        pd.DataFrame({"ts_code": ["CU2401.SHF"], "trade_date": [date(2024, 1, 2)]}),
    )

    with patch("zer0share.pipeline.date") as mock_date, \
         patch("zer0share.pipeline.time.sleep"):
        mock_date.today.return_value = date(2024, 1, 2)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        pipeline.sync_fut_daily()

    pipeline._fetcher.fetch_fut_daily.assert_not_called()


def test_sync_fut_daily_up_to_date(pipeline, cfg):
    pipeline._meta.update_last_date("fut_daily", date.today())
    pipeline.sync_fut_daily()
    pipeline._fetcher.fetch_fut_daily.assert_not_called()
```

- [ ] **Step 6: Run tests to verify**

Run: `cd /data/projects/zer0share && python -m pytest tests/test_pipeline.py -v -k "fut_" 2>&1 | tail -30`
Expected: All new tests PASS

- [ ] **Step 7: Run full test suite to ensure no regressions**

Run: `cd /data/projects/zer0share && python -m pytest tests/test_pipeline.py -v 2>&1 | tail -40`
Expected: All tests PASS

- [ ] **Step 8: Commit**

```bash
git add zer0share/pipeline.py tests/test_pipeline.py
git commit -m "feat: add futures pipeline sync methods with data_dir override"
```

---

### Task 3: API — Futures Query Methods

**Files:**
- Modify: `zer0share/api.py`
- Modify: `tests/test_api.py`

**Key insight:** Extend `_query_daily_partitioned` with `data_dir_override` and `order_by` parameters. For `fut_basic`, write a custom query method since it has no `trade_date` column and needs to scan the latest partition. For other futures tables, reuse `_query_daily_partitioned` with the futures data_dir override and `extra_filters` for `symbol`/`exchange`.

- [ ] **Step 1: Add futures column imports to api.py**

Add to the imports in `zer0share/api.py` (after line 21):

```python
from zer0share.fetcher import (
    ADJ_FACTOR_COLS,
    BASIC_COLS,
    CI_MEMBER_COLS,
    DAILY_BASIC_COLS,
    DAILY_COLS,
    INDEX_DAILY_COLS,
    INDEX_WEIGHT_COLS,
    STOCK_ST_COLS,
    STK_LIMIT_COLS,
    SUSPEND_D_COLS,
    SW_CLASSIFY_COLS,
    SW_MEMBER_COLS,
    TRADE_CAL_COLS,
    FUT_BASIC_COLS,
    FUT_DAILY_COLS,
    FUT_HOLDING_COLS,
    FUT_WSR_COLS,
    FUT_SETTLE_COLS,
    FUT_MAPPING_COLS,
)
```

- [ ] **Step 2: Extend `_query_daily_partitioned` with `data_dir_override` and `order_by`**

Replace the `_query_daily_partitioned` method signature and body (starting around line 532). The changes are:
1. Add `data_dir_override=None` parameter
2. Add `order_by="ts_code, trade_date"` parameter
3. Use `base_dir = data_dir_override or self._data_dir` instead of `self._data_dir`
4. Use the `order_by` parameter in the SQL

```python
    def _query_daily_partitioned(
        self,
        table_name: str,
        sync_table: str,
        columns: list[str],
        ts_code: str | None,
        trade_date: str | None,
        start_date: str | None,
        end_date: str | None,
        fields: str | list[str] | None,
        extra_filters: dict[str, str] | None = None,
        data_dir_override: Path | None = None,
        order_by: str = "ts_code, trade_date",
    ) -> pd.DataFrame:
        if trade_date is not None and (start_date is not None or end_date is not None):
            raise ValueError("trade_date cannot be combined with start_date or end_date")
        parsed_start = _parse_date(start_date) if start_date is not None else None
        parsed_end = _parse_date(end_date) if end_date is not None else None
        if parsed_start is not None and parsed_end is not None and parsed_end < parsed_start:
            raise ValueError("end_date must be on or after start_date")

        base_dir = data_dir_override or self._data_dir
        table_dir = base_dir / table_name
        if not table_dir.exists():
            raise FileNotFoundError(
                f"{sync_table} data not found; run `python main.py sync --table {sync_table}` first"
            )

        selected = _parse_fields(fields, columns)
        where = []
        params = []
        if ts_code is not None:
            codes = [code.strip() for code in ts_code.split(",") if code.strip()]
            placeholders = ", ".join("?" for _ in codes)
            where.append(f"ts_code IN ({placeholders})")
            params.extend(codes)
        if trade_date is not None:
            where.append("trade_date = ?")
            params.append(_parse_date(trade_date))
        if parsed_start is not None:
            where.append("trade_date >= ?")
            params.append(parsed_start)
        if parsed_end is not None:
            where.append("trade_date <= ?")
            params.append(parsed_end)
        if extra_filters is not None:
            for column, value in extra_filters.items():
                where.append(f"{column} = ?")
                params.append(value)

        pattern = table_dir / "date=*" / "data.parquet"
        sql = (
            f"SELECT {', '.join(selected)} "
            "FROM read_parquet(?, hive_partitioning=true, union_by_name=true)"
        )
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += f" ORDER BY {order_by}"

        df = duckdb.connect().execute(sql, [str(pattern), *params]).fetchdf()
        date_cols = ["trade_date"]
        return _format_date_columns(df, date_cols)
```

Also add `Path` import at the top if not already present — add `from pathlib import Path` to the imports.

- [ ] **Step 3: Add 6 futures query methods to LocalPro**

Add after the `ci_index_member` method (around line 443), before `pro_bar`:

```python
    def fut_basic(
        self,
        ts_code: str | None = None,
        exchange: str | None = None,
        fut_type: str | None = None,
        fut_code: str | None = None,
        fields: str | list[str] | None = None,
    ) -> pd.DataFrame:
        table_dir = self._data_dir / "futures" / "fut_basic"
        if not table_dir.exists():
            raise FileNotFoundError(
                "fut_basic data not found; run `python main.py sync --table fut_basic` first"
            )

        selected = _parse_fields(fields, FUT_BASIC_COLS)
        where = []
        params = []
        if ts_code is not None:
            codes = [code.strip() for code in ts_code.split(",") if code.strip()]
            placeholders = ", ".join("?" for _ in codes)
            where.append(f"ts_code IN ({placeholders})")
            params.extend(codes)
        if exchange is not None:
            where.append("exchange = ?")
            params.append(exchange)
        if fut_code is not None:
            where.append("fut_code = ?")
            params.append(fut_code)

        pattern = table_dir / "date=*" / "data.parquet"
        sql = (
            f"SELECT {', '.join(selected)} "
            "FROM read_parquet(?, hive_partitioning=true, union_by_name=true)"
        )
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY ts_code"

        df = duckdb.connect().execute(sql, [str(pattern), *params]).fetchdf()
        return _format_date_columns(df, ["list_date", "delist_date", "d_month", "last_ddate"])

    def fut_daily(
        self,
        ts_code: str | None = None,
        trade_date: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        fields: str | list[str] | None = None,
    ) -> pd.DataFrame:
        return self._query_daily_partitioned(
            table_name="fut_daily",
            sync_table="fut_daily",
            columns=FUT_DAILY_COLS,
            ts_code=ts_code,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
            fields=fields,
            data_dir_override=self._data_dir / "futures",
        )

    def fut_holding(
        self,
        trade_date: str | None = None,
        symbol: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        exchange: str | None = None,
        fields: str | list[str] | None = None,
    ) -> pd.DataFrame:
        extra = {}
        if symbol is not None:
            extra["symbol"] = symbol
        if exchange is not None:
            extra["exchange"] = exchange
        return self._query_daily_partitioned(
            table_name="fut_holding",
            sync_table="fut_holding",
            columns=FUT_HOLDING_COLS,
            ts_code=None,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
            fields=fields,
            extra_filters=extra or None,
            data_dir_override=self._data_dir / "futures",
            order_by="trade_date, symbol, broker",
        )

    def fut_wsr(
        self,
        trade_date: str | None = None,
        symbol: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        exchange: str | None = None,
        fields: str | list[str] | None = None,
    ) -> pd.DataFrame:
        extra = {}
        if symbol is not None:
            extra["symbol"] = symbol
        if exchange is not None:
            extra["exchange"] = exchange
        return self._query_daily_partitioned(
            table_name="fut_wsr",
            sync_table="fut_wsr",
            columns=FUT_WSR_COLS,
            ts_code=None,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
            fields=fields,
            extra_filters=extra or None,
            data_dir_override=self._data_dir / "futures",
            order_by="trade_date, symbol, warehouse",
        )

    def fut_settle(
        self,
        ts_code: str | None = None,
        trade_date: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        exchange: str | None = None,
        fields: str | list[str] | None = None,
    ) -> pd.DataFrame:
        extra = {}
        if exchange is not None:
            extra["exchange"] = exchange
        return self._query_daily_partitioned(
            table_name="fut_settle",
            sync_table="fut_settle",
            columns=FUT_SETTLE_COLS,
            ts_code=ts_code,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
            fields=fields,
            extra_filters=extra or None,
            data_dir_override=self._data_dir / "futures",
        )

    def fut_mapping(
        self,
        ts_code: str | None = None,
        trade_date: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        fields: str | list[str] | None = None,
    ) -> pd.DataFrame:
        return self._query_daily_partitioned(
            table_name="fut_mapping",
            sync_table="fut_mapping",
            columns=FUT_MAPPING_COLS,
            ts_code=ts_code,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
            fields=fields,
            data_dir_override=self._data_dir / "futures",
            order_by="ts_code, trade_date",
        )
```

- [ ] **Step 4: Add futures methods to the query dispatch dict**

In the `query` method (around line 508), add entries to the `dispatch` dict:

```python
        dispatch = {
            "stock_basic": self.stock_basic,
            "trade_cal": self.trade_cal,
            "daily": self.daily,
            "adj_factor": self.adj_factor,
            "daily_basic": self.daily_basic,
            "stock_st": self.stock_st,
            "suspend_d": self.suspend_d,
            "stk_limit": self.stk_limit,
            "index_daily": self.index_daily,
            "index_weight": self.index_weight,
            "universe": self.universe,
            "pro_bar": self.pro_bar,
            "index_classify": self.index_classify,
            "index_member_all": self.index_member_all,
            "ci_index_member": self.ci_index_member,
            "fut_basic": self.fut_basic,
            "fut_daily": self.fut_daily,
            "fut_holding": self.fut_holding,
            "fut_wsr": self.fut_wsr,
            "fut_settle": self.fut_settle,
            "fut_mapping": self.fut_mapping,
        }
```

- [ ] **Step 5: Write tests for futures API query methods**

Read `tests/test_api.py` first to understand the test setup patterns, then add futures tests at the end. The tests need to create parquet data in `data/futures/` subdirectories first.

Add at the end of `tests/test_api.py`:

```python
# --- Futures API tests ---

from zer0share.storage import write_daily_partition
from zer0share.fetcher import (
    FUT_BASIC_COLS, FUT_DAILY_COLS, FUT_HOLDING_COLS,
    FUT_WSR_COLS, FUT_SETTLE_COLS, FUT_MAPPING_COLS,
)


def _write_fut_daily_data(data_dir, trade_date, ts_codes=None):
    ts_codes = ts_codes or ["CU2401.SHF", "AG2401.SHF"]
    rows = []
    for ts_code in ts_codes:
        rows.append({
            "ts_code": ts_code,
            "trade_date": trade_date,
            "pre_close": 50000.0,
            "pre_settle": 50100.0,
            "open": 50200.0,
            "high": 50500.0,
            "low": 49900.0,
            "close": 50300.0,
            "settle": 50250.0,
            "change1": 200.0,
            "change2": 150.0,
            "vol": 10000.0,
            "amount": 251250.0,
            "oi": 50000.0,
            "oi_chg": 500.0,
            "delv_settle": None,
        })
    df = pd.DataFrame(rows)
    futures_dir = data_dir / "futures"
    write_daily_partition(futures_dir, "fut_daily", trade_date, df)
    return df


@pytest.fixture
def futures_api(tmp_path):
    return LocalPro(tmp_path)


def test_fut_basic_query_returns_data(tmp_path, futures_api):
    from datetime import date as dt
    df = pd.DataFrame({
        "ts_code": ["CU2401.SHF", "AG2401.SHF"],
        "symbol": ["CU2401", "AG2401"],
        "exchange": ["SHFE", "SHFE"],
        "name": ["沪铜2401", "沪银2401"],
        "fut_code": ["CU", "AG"],
        "multiplier": [None, None],
        "trade_unit": ["5吨/手", "15千克/手"],
        "per_unit": [5.0, 15.0],
        "quote_unit": ["元/吨", "元/千克"],
        "quote_unit_desc": ["10元/吨", "1元/千克"],
        "d_mode_desc": ["实物交割", "实物交割"],
        "list_date": ["20240101", "20240101"],
        "delist_date": ["20240115", "20240115"],
        "d_month": ["202401", "202401"],
        "last_ddate": ["20240115", "20240115"],
        "trade_time_desc": [None, None],
    })
    write_daily_partition(tmp_path / "futures", "fut_basic", dt(2024, 1, 2), df)

    result = futures_api.fut_basic()
    assert len(result) == 2
    assert set(result["ts_code"]) == {"CU2401.SHF", "AG2401.SHF"}


def test_fut_basic_query_filters_by_exchange(tmp_path, futures_api):
    from datetime import date as dt
    df = pd.DataFrame({
        "ts_code": ["CU2401.SHF", "A2505.DCE"],
        "symbol": ["CU2401", "A2505"],
        "exchange": ["SHFE", "DCE"],
        "name": ["沪铜2401", "豆一2505"],
        "fut_code": ["CU", "A"],
        "multiplier": [None, None],
        "trade_unit": ["5吨/手", "10吨/手"],
        "per_unit": [5.0, 10.0],
        "quote_unit": ["元/吨", "元/吨"],
        "quote_unit_desc": ["10元/吨", "1元/吨"],
        "d_mode_desc": ["实物交割", "实物交割"],
        "list_date": ["20240101", "20240101"],
        "delist_date": ["20240115", "20240515"],
        "d_month": ["202401", "202405"],
        "last_ddate": ["20240115", "20240515"],
        "trade_time_desc": [None, None],
    })
    write_daily_partition(tmp_path / "futures", "fut_basic", dt(2024, 1, 2), df)

    result = futures_api.fut_basic(exchange="DCE")
    assert len(result) == 1
    assert result.iloc[0]["ts_code"] == "A2505.DCE"


def test_fut_daily_query_returns_data(tmp_path):
    from datetime import date as dt
    api = LocalPro(tmp_path)
    _write_fut_daily_data(tmp_path, dt(2024, 1, 2))

    result = api.fut_daily(trade_date="20240102")
    assert len(result) == 2


def test_fut_daily_query_filters_by_ts_code(tmp_path):
    from datetime import date as dt
    api = LocalPro(tmp_path)
    _write_fut_daily_data(tmp_path, dt(2024, 1, 2))

    result = api.fut_daily(ts_code="CU2401.SHF", trade_date="20240102")
    assert len(result) == 1
    assert result.iloc[0]["ts_code"] == "CU2401.SHF"


def test_fut_daily_query_raises_when_no_data(tmp_path):
    api = LocalPro(tmp_path)
    with pytest.raises(FileNotFoundError):
        api.fut_daily(trade_date="20240102")


def test_fut_holding_query_returns_data(tmp_path):
    from datetime import date as dt
    api = LocalPro(tmp_path)
    df = pd.DataFrame({
        "trade_date": [dt(2024, 1, 2), dt(2024, 1, 2)],
        "symbol": ["CU", "CU"],
        "broker": ["中信期货", "国泰君安"],
        "vol": [10000, 8000],
        "vol_chg": [500, -200],
        "long_hld": [15000, 12000],
        "long_chg": [200, -100],
        "short_hld": [12000, 10000],
        "short_chg": [-100, 300],
        "exchange": ["SHFE", "SHFE"],
    })
    write_daily_partition(tmp_path / "futures", "fut_holding", dt(2024, 1, 2), df)

    result = api.fut_holding(trade_date="20240102")
    assert len(result) == 2


def test_fut_mapping_query_returns_data(tmp_path):
    from datetime import date as dt
    api = LocalPro(tmp_path)
    df = pd.DataFrame({
        "ts_code": ["CU.SHF", "AG.SHF"],
        "trade_date": [dt(2024, 1, 2), dt(2024, 1, 2)],
        "mapping_ts_code": ["CU2401.SHF", "AG2401.SHF"],
    })
    write_daily_partition(tmp_path / "futures", "fut_mapping", dt(2024, 1, 2), df)

    result = api.fut_mapping(trade_date="20240102")
    assert len(result) == 2


def test_query_dispatch_supports_futures(tmp_path):
    from datetime import date as dt
    api = LocalPro(tmp_path)
    _write_fut_daily_data(tmp_path, dt(2024, 1, 2))

    result = api.query("fut_daily", trade_date="20240102")
    assert len(result) == 2
```

- [ ] **Step 6: Run tests to verify**

Run: `cd /data/projects/zer0share && python -m pytest tests/test_api.py -v -k "fut_" 2>&1 | tail -30`
Expected: All new tests PASS

- [ ] **Step 7: Run full test suite to ensure no regressions**

Run: `cd /data/projects/zer0share && python -m pytest tests/test_api.py -v 2>&1 | tail -40`
Expected: All tests PASS

- [ ] **Step 8: Commit**

```bash
git add zer0share/api.py tests/test_api.py
git commit -m "feat: add futures query methods to LocalPro API"
```

---

### Task 4: Config + Scheduler

**Files:**
- Modify: `zer0share/config.py`
- Modify: `zer0share/scheduler.py`
- Modify: `config/settings.toml`
- Modify: `tests/test_config.py`
- Modify: `tests/test_scheduler.py`

- [ ] **Step 1: Add scheduler config fields for futures**

In `zer0share/config.py`, add futures scheduler fields to the `Config` dataclass (after `scheduler_adj_factor_minute`):

```python
    scheduler_futures_hour: int
    scheduler_futures_start_minute: int
```

In the `load_config` function, add the corresponding field extractions (after the `scheduler_adj_factor_minute` line):

```python
            scheduler_futures_hour=raw["scheduler"]["futures_hour"],
            scheduler_futures_start_minute=raw["scheduler"]["futures_start_minute"],
```

- [ ] **Step 2: Add futures scheduler config to settings.toml**

Read `config/settings.toml` to see the existing `[scheduler]` section, then add:

```toml
futures_hour = 17
futures_start_minute = 0
```

- [ ] **Step 3: Register 6 futures jobs in scheduler.py**

In `zer0share/scheduler.py`, add after the existing job registrations (before the `logger.info` call). Add import for `FUTURES_EXCHANGES` is not needed here. Add 6 new `scheduler.add_job` calls:

```python
        futures_tables = [
            ("fut_basic", pipeline.sync_fut_basic, 0),
            ("fut_daily", pipeline.sync_fut_daily, 10),
            ("fut_holding", pipeline.sync_fut_holding, 20),
            ("fut_wsr", pipeline.sync_fut_wsr, 30),
            ("fut_settle", pipeline.sync_fut_settle, 40),
            ("fut_mapping", pipeline.sync_fut_mapping, 50),
        ]
        for job_id, func, offset in futures_tables:
            scheduler.add_job(
                func,
                CronTrigger(
                    hour=cfg.scheduler_futures_hour,
                    minute=cfg.scheduler_futures_start_minute + offset,
                ),
                id=job_id,
            )
```

Also update the `logger.info` message to include futures schedule info:

```python
        logger.info(
            f"调度器启动: daily_kline + index_daily 每天 "
            f"{cfg.scheduler_daily_kline_hour}:{cfg.scheduler_daily_kline_minute:02d}, "
            f"adj_factor 每天 "
            f"{cfg.scheduler_adj_factor_hour}:{cfg.scheduler_adj_factor_minute:02d}, "
            f"basic 每天 {cfg.scheduler_basic_hour}:00, "
            f"futures 每天 {cfg.scheduler_futures_hour}:{cfg.scheduler_futures_start_minute:02d}+"
        )
```

- [ ] **Step 4: Update config tests**

In `tests/test_config.py`, read the file to understand the test pattern. Add the new fields to the test fixture that creates test config. The test should verify the new config fields are loaded correctly.

- [ ] **Step 5: Update scheduler tests**

In `tests/test_scheduler.py`, read the file to understand the test pattern. Add a test that verifies 6 futures jobs are registered.

- [ ] **Step 6: Run tests**

Run: `cd /data/projects/zer0share && python -m pytest tests/test_config.py tests/test_scheduler.py -v 2>&1 | tail -30`
Expected: All tests PASS

- [ ] **Step 7: Commit**

```bash
git add zer0share/config.py zer0share/scheduler.py config/settings.toml tests/test_config.py tests/test_scheduler.py
git commit -m "feat: add futures scheduler config and register 6 daily jobs"
```

---

### Task 5: CLI — Add Futures to Sync Command

**Files:**
- Modify: `zer0share/cli.py`
- Modify: `tests/test_cli.py`

- [ ] **Step 1: Add futures tables to SYNC_TABLES and range_tables**

In `zer0share/cli.py`, extend `SYNC_TABLES` (line 29) to include:

```python
SYNC_TABLES = [
    "daily_kline",
    "basic",
    "trade_cal",
    "adj_factor",
    "daily_basic",
    "stock_st",
    "suspend_d",
    "stk_limit",
    "index_weight",
    "index_daily",
    "industry",
    "ci_member",
    "fut_basic",
    "fut_daily",
    "fut_holding",
    "fut_wsr",
    "fut_settle",
    "fut_mapping",
]
```

Add futures daily tables to `range_tables` (around line 63):

```python
    range_tables = {
        "daily_kline",
        "adj_factor",
        "daily_basic",
        "stock_st",
        "suspend_d",
        "stk_limit",
        "index_weight",
        "index_daily",
        "fut_daily",
        "fut_holding",
        "fut_wsr",
        "fut_settle",
        "fut_mapping",
    }
```

- [ ] **Step 2: Add futures dispatch in sync function**

Add after the existing `ci_member` dispatch block (around line 133), before the `build-universe` command:

```python
        if sync_all or table == "fut_basic":
            pipeline.sync_fut_basic()
        if sync_all or table == "fut_daily":
            pipeline.sync_fut_daily(
                start_date=parsed_start_date,
                end_date=parsed_end_date,
            )
        if sync_all or table == "fut_holding":
            pipeline.sync_fut_holding(
                start_date=parsed_start_date,
                end_date=parsed_end_date,
            )
        if sync_all or table == "fut_wsr":
            pipeline.sync_fut_wsr(
                start_date=parsed_start_date,
                end_date=parsed_end_date,
            )
        if sync_all or table == "fut_settle":
            pipeline.sync_fut_settle(
                start_date=parsed_start_date,
                end_date=parsed_end_date,
            )
        if sync_all or table == "fut_mapping":
            pipeline.sync_fut_mapping(
                start_date=parsed_start_date,
                end_date=parsed_end_date,
            )
```

- [ ] **Step 3: Run CLI tests**

Run: `cd /data/projects/zer0share && python -m pytest tests/test_cli.py -v 2>&1 | tail -30`
Expected: All tests PASS

- [ ] **Step 4: Run full test suite**

Run: `cd /data/projects/zer0share && python -m pytest -v 2>&1 | tail -50`
Expected: All tests PASS across all test files

- [ ] **Step 5: Manual smoke test**

Verify the CLI recognizes futures tables:

```bash
cd /data/projects/zer0share && python -m zer0share.cli sync --help
```

Expected: `--table` option should list `fut_basic`, `fut_daily`, `fut_holding`, `fut_wsr`, `fut_settle`, `fut_mapping` among the choices.

- [ ] **Step 6: Commit**

```bash
git add zer0share/cli.py tests/test_cli.py
git commit -m "feat: add futures tables to CLI sync command"
```
