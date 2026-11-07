# Trading Calendar Expansion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expand trading calendar to all 8 exchanges, skip non-trading-day syncs, and ensure trade_cal syncs before derivatives in the scheduler.

**Architecture:** Add `is_trading_day()` to MetaStore, `_skip_if_not_trading()` / `_ensure_trade_cal_loaded()` to Pipeline, extend `sync_trade_cal` to all 8 exchanges, add `exchange` param to `_sync_daily_partitioned`, and wire trade_cal into the scheduler as a first-class job.

**Tech Stack:** Python 3.11+, DuckDB, pandas, pytest, APScheduler, Click

---

### Task 1: Add `is_trading_day` to MetaStore

**Files:**
- Modify: `zer0share/storage.py:84-98` (after `get_trading_days`)
- Test: `tests/test_storage.py`

- [ ] **Step 1: Write failing tests for `is_trading_day`**

Append to `tests/test_storage.py`:

```python
def test_is_trading_day_returns_true_for_open_day(tmp_path):
    db_path = tmp_path / "meta.duckdb"
    df = pd.DataFrame({
        "exchange": ["SSE"],
        "cal_date": [date(2024, 1, 2)],
        "is_open": [True],
        "pretrade_date": [date(2023, 12, 29)],
    })
    write_trade_cal(tmp_path, "SSE", df)
    with MetaStore(db_path) as store:
        store.load_trade_cal_from_parquet(tmp_path)
        assert store.is_trading_day("SSE", date(2024, 1, 2)) is True


def test_is_trading_day_returns_false_for_closed_day(tmp_path):
    db_path = tmp_path / "meta.duckdb"
    df = pd.DataFrame({
        "exchange": ["SSE"],
        "cal_date": [date(2024, 1, 3)],
        "is_open": [False],
        "pretrade_date": [date(2024, 1, 2)],
    })
    write_trade_cal(tmp_path, "SSE", df)
    with MetaStore(db_path) as store:
        store.load_trade_cal_from_parquet(tmp_path)
        assert store.is_trading_day("SSE", date(2024, 1, 3)) is False


def test_is_trading_day_returns_true_when_date_not_in_calendar(tmp_path):
    db_path = tmp_path / "meta.duckdb"
    df = pd.DataFrame({
        "exchange": ["SSE"],
        "cal_date": [date(2024, 1, 2)],
        "is_open": [True],
        "pretrade_date": [date(2023, 12, 29)],
    })
    write_trade_cal(tmp_path, "SSE", df)
    with MetaStore(db_path) as store:
        store.load_trade_cal_from_parquet(tmp_path)
        # 2024-01-10 is not in the calendar — conservative default True
        assert store.is_trading_day("SSE", date(2024, 1, 10)) is True


def test_is_trading_day_returns_true_when_no_calendar_loaded(tmp_path):
    db_path = tmp_path / "meta.duckdb"
    with MetaStore(db_path) as store:
        assert store.is_trading_day("SSE", date(2024, 1, 2)) is True
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /data/projects/zer0share && uv run pytest tests/test_storage.py::test_is_trading_day_returns_true_for_open_day tests/test_storage.py::test_is_trading_day_returns_false_for_closed_day tests/test_storage.py::test_is_trading_day_returns_true_when_date_not_in_calendar tests/test_storage.py::test_is_trading_day_returns_true_when_no_calendar_loaded -v`
Expected: FAIL — `AttributeError: 'MetaStore' object has no attribute 'is_trading_day'`

- [ ] **Step 3: Implement `is_trading_day` in MetaStore**

Add after `get_trading_days` in `zer0share/storage.py` (after line 98):

```python
    def is_trading_day(self, exchange: str, cal_date: date) -> bool:
        """Check whether a given date is a trading day for an exchange.

        Returns True when the date is not covered by the calendar
        (conservative: don't skip syncs for dates we don't know about).
        """
        row = self._conn.execute(
            "SELECT is_open FROM trade_cal WHERE exchange = ? AND cal_date = ?",
            [exchange, cal_date]
        ).fetchone()
        if row is None:
            return True
        return bool(row[0])
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /data/projects/zer0share && uv run pytest tests/test_storage.py::test_is_trading_day_returns_true_for_open_day tests/test_storage.py::test_is_trading_day_returns_false_for_closed_day tests/test_storage.py::test_is_trading_day_returns_true_when_date_not_in_calendar tests/test_storage.py::test_is_trading_day_returns_true_when_no_calendar_loaded -v`
Expected: All 4 PASS

- [ ] **Step 5: Run full test suite to confirm no regressions**

Run: `cd /data/projects/zer0share && uv run pytest tests/ -v`
Expected: All tests PASS

- [ ] **Step 6: Commit**

```bash
cd /data/projects/zer0share && git add zer0share/storage.py tests/test_storage.py && git commit -m "feat: add is_trading_day method to MetaStore"
```

---

### Task 2: Add `ALL_EXCHANGES`, `_ensure_trade_cal_loaded`, and `_skip_if_not_trading` to Pipeline

**Files:**
- Modify: `zer0share/pipeline.py:33` (add constant) and `pipeline.py:110-115` (Pipeline class)
- Test: `tests/test_pipeline.py`

- [ ] **Step 1: Write failing tests for `_skip_if_not_trading` and `_ensure_trade_cal_loaded`**

Append to `tests/test_pipeline.py`:

```python
from zer0share.pipeline import ALL_EXCHANGES


def test_all_exchanges_contains_all_8():
    assert ALL_EXCHANGES == ["SSE", "SZSE", "CFFEX", "DCE", "SHFE", "CZCE", "INE", "GFEX"]


def test_skip_if_not_trading_returns_true_on_non_trading_day(pipeline, cfg):
    trade_cal = pd.DataFrame({
        "exchange": ["SSE"],
        "cal_date": [date(2024, 1, 3)],
        "is_open": [False],
        "pretrade_date": [date(2024, 1, 2)],
    })
    write_trade_cal(cfg.data_dir, "SSE", trade_cal)
    pipeline._meta.load_trade_cal_from_parquet(cfg.data_dir)
    pipeline._meta.update_last_date("trade_cal", date(2024, 1, 3))

    with patch("zer0share.pipeline.date") as mock_date:
        mock_date.today.return_value = date(2024, 1, 3)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        assert pipeline._skip_if_not_trading("SSE") is True


def test_skip_if_not_trading_returns_false_on_trading_day(pipeline, cfg):
    trade_cal = pd.DataFrame({
        "exchange": ["SSE"],
        "cal_date": [date(2024, 1, 2)],
        "is_open": [True],
        "pretrade_date": [date(2023, 12, 29)],
    })
    write_trade_cal(cfg.data_dir, "SSE", trade_cal)
    pipeline._meta.load_trade_cal_from_parquet(cfg.data_dir)
    pipeline._meta.update_last_date("trade_cal", date(2024, 1, 2))

    with patch("zer0share.pipeline.date") as mock_date:
        mock_date.today.return_value = date(2024, 1, 2)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        assert pipeline._skip_if_not_trading("SSE") is False


def test_ensure_trade_cal_loaded_triggers_sync_when_no_meta(pipeline, cfg):
    pipeline._fetcher.fetch_trade_cal.return_value = pd.DataFrame({
        "exchange": ["SSE"],
        "cal_date": [date(2024, 1, 2)],
        "is_open": [True],
        "pretrade_date": [date(2023, 12, 29)],
    })
    with patch("zer0share.pipeline.date") as mock_date:
        mock_date.today.return_value = date(2024, 6, 1)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        pipeline._ensure_trade_cal_loaded()
    pipeline._fetcher.fetch_trade_cal.assert_called()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /data/projects/zer0share && uv run pytest tests/test_pipeline.py::test_all_exchanges_contains_all_8 tests/test_pipeline.py::test_skip_if_not_trading_returns_true_on_non_trading_day tests/test_pipeline.py::test_skip_if_not_trading_returns_false_on_trading_day tests/test_pipeline.py::test_ensure_trade_cal_loaded_triggers_sync_when_no_meta -v`
Expected: FAIL — `ImportError: cannot import name 'ALL_EXCHANGES'`

- [ ] **Step 3: Implement changes in `pipeline.py`**

3a. Add the `ALL_EXCHANGES` constant after line 34 (`INDEX_CODES = ...`):

```python
ALL_EXCHANGES = ["SSE", "SZSE", "CFFEX", "DCE", "SHFE", "CZCE", "INE", "GFEX"]
```

3b. Add two methods to the `Pipeline` class, after `__init__` (after line 115):

```python
    def _ensure_trade_cal_loaded(self) -> None:
        """Ensure trade calendar is loaded into DuckDB. Syncs if needed."""
        if self._meta.get_last_date("trade_cal") is None:
            self.sync_trade_cal()

    def _skip_if_not_trading(self, exchange: str) -> bool:
        """Check if today is a trading day. Returns True if sync should be skipped."""
        self._ensure_trade_cal_loaded()
        today = date.today()
        if not self._meta.is_trading_day(exchange, today):
            logger.info(f"今日 {today} 非交易日，跳过同步")
            return True
        return False
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /data/projects/zer0share && uv run pytest tests/test_pipeline.py::test_all_exchanges_contains_all_8 tests/test_pipeline.py::test_skip_if_not_trading_returns_true_on_non_trading_day tests/test_pipeline.py::test_skip_if_not_trading_returns_false_on_trading_day tests/test_pipeline.py::test_ensure_trade_cal_loaded_triggers_sync_when_no_meta -v`
Expected: All 4 PASS

- [ ] **Step 5: Run full test suite**

Run: `cd /data/projects/zer0share && uv run pytest tests/ -v`
Expected: All tests PASS

- [ ] **Step 6: Commit**

```bash
cd /data/projects/zer0share && git add zer0share/pipeline.py tests/test_pipeline.py && git commit -m "feat: add ALL_EXCHANGES constant, _skip_if_not_trading and _ensure_trade_cal_loaded to Pipeline"
```

---

### Task 3: Extend `sync_trade_cal` to all 8 exchanges

**Files:**
- Modify: `zer0share/pipeline.py:129-164` (`sync_trade_cal` method)
- Test: `tests/test_pipeline.py`

- [ ] **Step 1: Write failing test**

Append to `tests/test_pipeline.py`:

```python
from zer0share.pipeline import ALL_EXCHANGES as NEW_ALL_EXCHANGES


def test_sync_trade_cal_writes_all_8_exchanges(pipeline, cfg):
    pipeline._fetcher.fetch_trade_cal.return_value = _trade_cal_df("SSE")
    with patch("zer0share.pipeline.date") as mock_date:
        mock_date.today.return_value = date(2024, 5, 18)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        pipeline.sync_trade_cal()
    for ex in NEW_ALL_EXCHANGES:
        assert (cfg.data_dir / "trade_cal" / f"exchange={ex}" / "data.parquet").exists(), f"Missing {ex}"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /data/projects/zer0share && uv run pytest tests/test_pipeline.py::test_sync_trade_cal_writes_all_8_exchanges -v`
Expected: FAIL — only SSE and SZSE parquet files exist

- [ ] **Step 3: Update `sync_trade_cal` to use `ALL_EXCHANGES`**

In `zer0share/pipeline.py`, change `sync_trade_cal` to use `ALL_EXCHANGES` instead of `EXCHANGES`:

Line 133: change `for exchange in EXCHANGES:` to `for exchange in ALL_EXCHANGES:`

Line 157: change `self._meta.load_trade_cal_from_parquet(self._cfg.data_dir, EXCHANGES)` to `self._meta.load_trade_cal_from_parquet(self._cfg.data_dir, ALL_EXCHANGES)`

- [ ] **Step 4: Run new test**

Run: `cd /data/projects/zer0share && uv run pytest tests/test_pipeline.py::test_sync_trade_cal_writes_all_8_exchanges -v`
Expected: PASS

- [ ] **Step 5: Run full test suite**

Run: `cd /data/projects/zer0share && uv run pytest tests/ -v`
Expected: All tests PASS. Note: existing `test_sync_trade_cal_writes_all_exchanges` uses `EXCHANGES` (2 exchanges) and will still pass because the test asserts files exist for those 2 — they still will.

- [ ] **Step 6: Update existing test to import from pipeline**

The existing `test_sync_trade_cal_writes_all_exchanges` imports `EXCHANGES` from pipeline. It should keep working because `EXCHANGES` still exists. Verify it passes in Step 5.

- [ ] **Step 7: Commit**

```bash
cd /data/projects/zer0share && git add zer0share/pipeline.py tests/test_pipeline.py && git commit -m "feat: extend sync_trade_cal to all 8 exchanges"
```

---

### Task 4: Add non-trading-day skip to 6 sync methods

**Files:**
- Modify: `zer0share/pipeline.py` — 6 methods: `sync_basic`, `sync_industry`, `sync_ci_member`, `sync_fut_basic`, `sync_opt_basic`, `sync_fut_index_daily`
- Test: `tests/test_pipeline.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/test_pipeline.py`:

```python
def _setup_non_trading_day(pipeline, cfg):
    """Set up a non-trading day (2024-01-03, SSE) in the calendar."""
    trade_cal = pd.DataFrame({
        "exchange": ["SSE"],
        "cal_date": [date(2024, 1, 3)],
        "is_open": [False],
        "pretrade_date": [date(2024, 1, 2)],
    })
    write_trade_cal(cfg.data_dir, "SSE", trade_cal)
    pipeline._meta.load_trade_cal_from_parquet(cfg.data_dir)
    pipeline._meta.update_last_date("trade_cal", date(2024, 1, 3))


def test_sync_basic_skips_non_trading_day(pipeline, cfg):
    _setup_non_trading_day(pipeline, cfg)
    with patch("zer0share.pipeline.date") as mock_date:
        mock_date.today.return_value = date(2024, 1, 3)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        pipeline.sync_basic()
    pipeline._fetcher.fetch_basic.assert_not_called()
    pipeline._notifier.send.assert_not_called()


def test_sync_industry_skips_non_trading_day(pipeline, cfg):
    _setup_non_trading_day(pipeline, cfg)
    with patch("zer0share.pipeline.date") as mock_date:
        mock_date.today.return_value = date(2024, 1, 3)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        pipeline.sync_industry()
    pipeline._fetcher.fetch_sw_classify.assert_not_called()
    pipeline._notifier.send.assert_not_called()


def test_sync_ci_member_skips_non_trading_day(pipeline, cfg):
    _setup_non_trading_day(pipeline, cfg)
    with patch("zer0share.pipeline.date") as mock_date:
        mock_date.today.return_value = date(2024, 1, 3)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        pipeline.sync_ci_member()
    pipeline._fetcher.fetch_ci_member.assert_not_called()
    pipeline._notifier.send.assert_not_called()


def test_sync_fut_basic_skips_non_trading_day(pipeline, cfg):
    _setup_non_trading_day(pipeline, cfg)
    with patch("zer0share.pipeline.date") as mock_date:
        mock_date.today.return_value = date(2024, 1, 3)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        pipeline.sync_fut_basic()
    pipeline._fetcher.fetch_fut_basic.assert_not_called()
    pipeline._notifier.send.assert_not_called()


def test_sync_opt_basic_skips_non_trading_day(pipeline, cfg):
    _setup_non_trading_day(pipeline, cfg)
    with patch("zer0share.pipeline.date") as mock_date:
        mock_date.today.return_value = date(2024, 1, 3)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        pipeline.sync_opt_basic()
    pipeline._fetcher.fetch_opt_basic.assert_not_called()
    pipeline._notifier.send.assert_not_called()


def test_sync_fut_index_daily_skips_non_trading_day(pipeline, cfg):
    _setup_non_trading_day(pipeline, cfg)
    pipeline._meta.update_last_date("fut_index_daily", date(2024, 1, 1))
    with patch("zer0share.pipeline.date") as mock_date:
        mock_date.today.return_value = date(2024, 1, 3)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        pipeline.sync_fut_index_daily()
    pipeline._fetcher.fetch_fut_index_daily.assert_not_called()
    pipeline._notifier.send.assert_not_called()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /data/projects/zer0share && uv run pytest tests/test_pipeline.py::test_sync_basic_skips_non_trading_day tests/test_pipeline.py::test_sync_industry_skips_non_trading_day tests/test_pipeline.py::test_sync_ci_member_skips_non_trading_day tests/test_pipeline.py::test_sync_fut_basic_skips_non_trading_day tests/test_pipeline.py::test_sync_opt_basic_skips_non_trading_day tests/test_pipeline.py::test_sync_fut_index_daily_skips_non_trading_day -v`
Expected: FAIL — fetcher is called (no skip guard in place)

- [ ] **Step 3: Add skip guard to each of the 6 methods in `pipeline.py`**

**`sync_basic`** (line 117) — add at the very start of the method body, before `today = date.today()`:

```python
        if self._skip_if_not_trading("SSE"):
            return
```

**`sync_industry`** (line 542) — add at the start:

```python
        if self._skip_if_not_trading("SSE"):
            return
```

**`sync_ci_member`** (line 559) — add at the start:

```python
        if self._skip_if_not_trading("SSE"):
            return
```

**`sync_fut_basic`** (line 571) — add at the start, before `today = date.today()`:

```python
        if self._skip_if_not_trading("SSE"):
            return
```

**`sync_opt_basic`** (line 607) — add at the start, before `today = date.today()`:

```python
        if self._skip_if_not_trading("SSE"):
            return
```

**`sync_fut_index_daily`** (line 733) — add at the start, before `today = date.today()`:

```python
        if self._skip_if_not_trading("SSE"):
            return
```

- [ ] **Step 4: Run the 6 new tests**

Run: `cd /data/projects/zer0share && uv run pytest tests/test_pipeline.py::test_sync_basic_skips_non_trading_day tests/test_pipeline.py::test_sync_industry_skips_non_trading_day tests/test_pipeline.py::test_sync_ci_member_skips_non_trading_day tests/test_pipeline.py::test_sync_fut_basic_skips_non_trading_day tests/test_pipeline.py::test_sync_opt_basic_skips_non_trading_day tests/test_pipeline.py::test_sync_fut_index_daily_skips_non_trading_day -v`
Expected: All 6 PASS

- [ ] **Step 5: Run full test suite**

Run: `cd /data/projects/zer0share && uv run pytest tests/ -v`
Expected: All tests PASS. Existing tests for these methods mock `date.today()` to a trading day or don't set up a calendar (so `_skip_if_not_trading` triggers `_ensure_trade_cal_loaded` which syncs a calendar). Review output carefully.

**Important:** Existing tests like `test_sync_basic_first_run_writes_parquet` do NOT set up a trade_cal. With the new `_skip_if_not_trading` guard, `_ensure_trade_cal_loaded` will fire and call `sync_trade_cal`, which calls `fetch_trade_cal` on the mock. The mock returns a DataFrame with is_open=True by default. Since `date.today()` isn't mocked in that test, it will check the real today against the calendar. We need to ensure existing tests still pass. If they fail, patch `date.today()` in the existing tests or make `_skip_if_not_trading` more resilient.

If existing tests break, the fix is to add `date` patching to existing tests that call these 6 methods. Check the test output and fix accordingly.

- [ ] **Step 6: Commit**

```bash
cd /data/projects/zer0share && git add zer0share/pipeline.py tests/test_pipeline.py && git commit -m "feat: add non-trading-day skip to sync_basic, sync_industry, sync_ci_member, sync_fut_basic, sync_opt_basic, sync_fut_index_daily"
```

---

### Task 5: Add `exchange` parameter to `_sync_daily_partitioned`

**Files:**
- Modify: `zer0share/pipeline.py:854-945` (`_sync_daily_partitioned` method)
- Test: `tests/test_pipeline.py`

- [ ] **Step 1: Write failing test**

Append to `tests/test_pipeline.py`:

```python
def test_sync_daily_partitioned_uses_exchange_param(pipeline, cfg):
    """Verify that _sync_daily_partitioned passes exchange to get_trading_days."""
    dce_cal = pd.DataFrame({
        "exchange": ["DCE"],
        "cal_date": [date(2024, 1, 2)],
        "is_open": [True],
        "pretrade_date": [date(2023, 12, 29)],
    })
    sse_cal = pd.DataFrame({
        "exchange": ["SSE"],
        "cal_date": [date(2024, 1, 2)],
        "is_open": [False],
        "pretrade_date": [date(2024, 1, 1)],
    })
    write_trade_cal(cfg.data_dir, "DCE", dce_cal)
    write_trade_cal(cfg.data_dir, "SSE", sse_cal)
    pipeline._meta.load_trade_cal_from_parquet(cfg.data_dir)
    pipeline._meta.update_last_date("trade_cal", date(2024, 1, 2))

    pipeline._fetcher.fetch_fut_daily.return_value = pd.DataFrame({
        "ts_code": ["CU2401.SHF"], "trade_date": [date(2024, 1, 2)],
        "pre_close": [50000.0], "pre_settle": [50100.0], "open": [50200.0],
        "high": [50500.0], "low": [49900.0], "close": [50300.0], "settle": [50250.0],
        "change1": [200.0], "change2": [150.0], "vol": [10000.0], "amount": [251250.0],
        "oi": [50000.0], "oi_chg": [500.0], "delv_settle": [None],
    })
    pipeline._meta.update_last_date("fut_daily", date(2024, 1, 1))

    with patch("zer0share.pipeline.date") as mock_date, \
         patch("zer0share.pipeline.time.sleep"):
        mock_date.today.return_value = date(2024, 1, 2)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        # DCE has 1/2 as trading day; SSE does not
        # Using exchange="DCE" should sync, "SSE" should skip
        pipeline.sync_fut_daily()

    # Default _sync_daily_partitioned uses "SSE" — SSE 1/2 is closed,
    # so fetcher should NOT be called
    pipeline._fetcher.fetch_fut_daily.assert_not_called()
```

- [ ] **Step 2: Run test to see current behavior**

Run: `cd /data/projects/zer0share && uv run pytest tests/test_pipeline.py::test_sync_daily_partitioned_uses_exchange_param -v`
Expected: FAIL — currently `_sync_daily_partitioned` hardcodes `"SSE"` and doesn't have the `exchange` parameter yet

- [ ] **Step 3: Add `exchange` parameter to `_sync_daily_partitioned`**

Change the method signature in `zer0share/pipeline.py` (line 854):

From:
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
```

To:
```python
    def _sync_daily_partitioned(
        self,
        table_name: str,
        fetch,
        start_date: date | None,
        end_date: date | None,
        write_empty: bool = False,
        data_dir: Path | None = None,
        exchange: str = "SSE",
    ) -> None:
```

Then inside the method body, change line 879:

From:
```python
        trading_days = self._meta.get_trading_days("SSE", start, end)
```

To:
```python
        trading_days = self._meta.get_trading_days(exchange, start, end)
```

And change the error message (lines 881-883):

From:
```python
                "DuckDB 中无 SSE trade_cal 数据，请先运行 "
                "python main.py sync --table trade_cal"
```

To:
```python
                f"DuckDB 中无 {exchange} trade_cal 数据，请先运行 "
                "python main.py sync --table trade_cal"
```

- [ ] **Step 4: Run the new test**

Run: `cd /data/projects/zer0share && uv run pytest tests/test_pipeline.py::test_sync_daily_partitioned_uses_exchange_param -v`
Expected: PASS (SSE says 1/2 is closed, fetcher not called)

- [ ] **Step 5: Run full test suite**

Run: `cd /data/projects/zer0share && uv run pytest tests/ -v`
Expected: All tests PASS — default `"SSE"` maintains backward compatibility

- [ ] **Step 6: Commit**

```bash
cd /data/projects/zer0share && git add zer0share/pipeline.py tests/test_pipeline.py && git commit -m "feat: add exchange parameter to _sync_daily_partitioned"
```

---

### Task 6: Add trade_cal scheduler job and config

**Files:**
- Modify: `zer0share/config.py` — add `scheduler_trade_cal_hour`, `scheduler_trade_cal_minute`
- Modify: `config/settings.example.toml` — add trade_cal schedule
- Modify: `zer0share/scheduler.py` — add trade_cal as first job
- Test: `tests/test_config.py`
- Test: `tests/test_scheduler.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/test_config.py`:

```python
CONFIG_WITH_TRADE_CAL = """
[tushare]
token = "test_token"

[paths]
data_dir = "data"
db_path = "db/meta.duckdb"
log_path = "logs/pipeline.log"

[scheduler]
daily_kline_hour = 18
daily_kline_minute = 0
basic_hour = 8
adj_factor_hour = 18
adj_factor_minute = 5
futures_hour = 17
futures_start_minute = 0
trade_cal_hour = 16
trade_cal_minute = 0

[notifier]
wecom_webhook_url = "https://example.com/webhook"
enabled = false
"""


def test_load_config_with_trade_cal_schedule(tmp_path):
    cfg_file = tmp_path / "settings.toml"
    cfg_file.write_text(CONFIG_WITH_TRADE_CAL, encoding="utf-8")
    cfg = load_config(cfg_file)
    assert cfg.scheduler_trade_cal_hour == 16
    assert cfg.scheduler_trade_cal_minute == 0
```

Update `VALID_TOML` and `VALID_CONFIG` in existing tests to include the new fields. In `tests/test_config.py`, change `VALID_TOML`:

From:
```python
VALID_TOML = """
[tushare]
token = "test_token"

[paths]
data_dir = "data"
db_path = "db/meta.duckdb"
log_path = "logs/pipeline.log"

[scheduler]
daily_kline_hour = 18
daily_kline_minute = 0
basic_hour = 8
adj_factor_hour = 18
adj_factor_minute = 5
futures_hour = 17
futures_start_minute = 0

[notifier]
wecom_webhook_url = "https://example.com/webhook"
enabled = false
"""
```

To:
```python
VALID_TOML = """
[tushare]
token = "test_token"

[paths]
data_dir = "data"
db_path = "db/meta.duckdb"
log_path = "logs/pipeline.log"

[scheduler]
daily_kline_hour = 18
daily_kline_minute = 0
basic_hour = 8
adj_factor_hour = 18
adj_factor_minute = 5
futures_hour = 17
futures_start_minute = 0
trade_cal_hour = 16
trade_cal_minute = 0

[notifier]
wecom_webhook_url = "https://example.com/webhook"
enabled = false
"""
```

Add assertions to `test_load_config_returns_all_fields`:

```python
    assert cfg.scheduler_trade_cal_hour == 16
    assert cfg.scheduler_trade_cal_minute == 0
```

Append to `tests/test_scheduler.py`:

```python
VALID_CONFIG_WITH_TRADE_CAL = """
[tushare]
token = "test"

[paths]
data_dir = "data"
db_path = "db/meta.duckdb"
log_path = "logs/pipeline.log"

[scheduler]
daily_kline_hour = 18
daily_kline_minute = 0
basic_hour = 8
adj_factor_hour = 18
adj_factor_minute = 5
futures_hour = 17
futures_start_minute = 0
trade_cal_hour = 16
trade_cal_minute = 0

[notifier]
wecom_webhook_url = "https://example.com"
enabled = false
"""


def test_start_scheduler_registers_trade_cal_job(tmp_path):
    cfg_file = tmp_path / "settings.toml"
    cfg_file.write_text(VALID_CONFIG_WITH_TRADE_CAL, encoding="utf-8")

    registered_jobs = []

    def fake_add_job(func, trigger, id=None, **kwargs):
        registered_jobs.append(id)

    with (
        patch("tushare.pro_api"),
        patch("apscheduler.schedulers.blocking.BlockingScheduler.start"),
        patch(
            "apscheduler.schedulers.blocking.BlockingScheduler.add_job",
            side_effect=fake_add_job,
        ),
        patch("zer0share.scheduler.Pipeline") as mock_pipeline_cls,
    ):
        mock_pipeline_cls.return_value.__enter__ = lambda s: s
        mock_pipeline_cls.return_value.__exit__ = MagicMock(return_value=False)
        from zer0share.scheduler import start_scheduler

        start_scheduler(str(cfg_file))

    assert "trade_cal" in registered_jobs
```

Also update `VALID_CONFIG` in `tests/test_scheduler.py` to include the new fields:

Change `VALID_CONFIG` to add after `futures_start_minute = 0`:
```
trade_cal_hour = 16
trade_cal_minute = 0
```

Update `test_start_scheduler_registers_two_jobs` to assert 18 jobs (was 17):

Change `assert len(registered_jobs) == 17` to `assert len(registered_jobs) == 18`

And add `"trade_cal"` to the expected set:

```python
    assert set(registered_jobs) == {
        "trade_cal",
        "daily_kline",
        ...
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /data/projects/zer0share && uv run pytest tests/test_config.py tests/test_scheduler.py -v`
Expected: FAIL — `Config` doesn't have `scheduler_trade_cal_hour` field

- [ ] **Step 3: Update `config.py`**

Add two fields to the `Config` dataclass in `zer0share/config.py`:

```python
    scheduler_trade_cal_hour: int
    scheduler_trade_cal_minute: int
```

Add to `load_config`:

```python
            scheduler_trade_cal_hour=raw["scheduler"]["trade_cal_hour"],
            scheduler_trade_cal_minute=raw["scheduler"]["trade_cal_minute"],
```

- [ ] **Step 4: Update `settings.example.toml`**

Add after `futures_start_minute = 0`:

```toml
trade_cal_hour = 16
trade_cal_minute = 0
```

- [ ] **Step 5: Update `scheduler.py`**

In `zer0share/scheduler.py`, add a trade_cal job before the existing daily_kline job (after line 22, before the first `scheduler.add_job`):

```python
        scheduler.add_job(
            pipeline.sync_trade_cal,
            CronTrigger(
                hour=cfg.scheduler_trade_cal_hour,
                minute=cfg.scheduler_trade_cal_minute,
            ),
            id="trade_cal",
        )
```

Update the log message to include trade_cal:

```python
        logger.info(
            f"调度器启动: trade_cal 每天 "
            f"{cfg.scheduler_trade_cal_hour}:{cfg.scheduler_trade_cal_minute:02d}, "
            f"daily_kline + index_daily 每天 "
            f"{cfg.scheduler_daily_kline_hour}:{cfg.scheduler_daily_kline_minute:02d}, "
            ...
```

- [ ] **Step 6: Run tests**

Run: `cd /data/projects/zer0share && uv run pytest tests/test_config.py tests/test_scheduler.py -v`
Expected: All PASS

- [ ] **Step 7: Run full test suite**

Run: `cd /data/projects/zer0share && uv run pytest tests/ -v`
Expected: All tests PASS

- [ ] **Step 8: Commit**

```bash
cd /data/projects/zer0share && git add zer0share/config.py zer0share/scheduler.py config/settings.example.toml tests/test_config.py tests/test_scheduler.py && git commit -m "feat: add trade_cal scheduler job with config support"
```

---

### Task 7: Fix any existing tests broken by the skip guard and final verification

**Files:**
- Possibly modify: `tests/test_pipeline.py` (fix any broken existing tests)
- Possibly modify: `zer0share/pipeline.py` (minor adjustments)

- [ ] **Step 1: Run full test suite**

Run: `cd /data/projects/zer0share && uv run pytest tests/ -v`
Expected: All tests PASS

If any tests fail due to the `_skip_if_not_trading` guard (e.g., `test_sync_basic_first_run_writes_parquet`), the fix pattern is to either:

a) Mock `date.today()` to a trading day and ensure a trade_cal is loaded, OR
b) Mock `_skip_if_not_trading` to return `False` for that specific test

Option (b) is cleaner — add `patch("zer0share.pipeline.Pipeline._skip_if_not_trading", return_value=False)` as a decorator or context manager to existing tests that don't set up a trade_cal.

- [ ] **Step 2: Run full test suite again**

Run: `cd /data/projects/zer0share && uv run pytest tests/ -v`
Expected: All tests PASS

- [ ] **Step 3: Commit any test fixes**

```bash
cd /data/projects/zer0share && git add tests/ zer0share/ && git commit -m "fix: update existing tests for non-trading-day skip guard"
```
