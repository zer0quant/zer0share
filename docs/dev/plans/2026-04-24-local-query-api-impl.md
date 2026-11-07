# Local Query API Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a local Tushare-like Python API for querying synced `stock_basic`, `trade_cal`, `daily`, and `adj_factor` Parquet data.

**Architecture:** Add a small public `LocalPro` API that reads local Parquet files through DuckDB and returns pandas DataFrames. Keep Tushare-style parameter names and convert output date columns to `YYYYMMDD` strings.

**Tech Stack:** Python 3.11+, pandas, DuckDB, pyarrow, pytest, uv.

---

### Task 0: Restore Green Baseline

**Files:**
- Modify: `tests/test_config.py`
- Modify: `tests/test_scheduler.py`

**Step 1:** Add `adj_factor_hour = 18` and `adj_factor_minute = 5` to valid scheduler TOML fixtures.

**Step 2:** Assert `scheduler_adj_factor_hour` and `scheduler_adj_factor_minute` in config tests.

**Step 3:** Update scheduler expectations to include `adj_factor`.

**Step 4:** Run `uv run pytest -q`; expect existing tests to pass before adding API tests.

### Task 1: Add API Test Fixtures

**Files:**
- Create: `tests/test_api.py`

**Step 1:** Create temporary Parquet fixtures for basic, trade calendar, daily kline, and adj factor data.

**Step 2:** Write `test_stock_basic_filters_and_formats_dates` against `from src.api import LocalPro`.

**Step 3:** Run `uv run pytest tests/test_api.py::test_stock_basic_filters_and_formats_dates -v`; expect failure because `src.api` does not exist.

### Task 2: Implement Minimal `stock_basic`

**Files:**
- Create: `src/api.py`
- Modify: `src/__init__.py`
- Test: `tests/test_api.py`

**Step 1:** Implement `LocalPro.__init__` and `stock_basic` with DuckDB Parquet reads, filters, fields, sorting, and `YYYYMMDD` date output.

**Step 2:** Expose `pro_api(config_path="config/settings.toml")` from `src/__init__.py`.

**Step 3:** Run the stock basic API test; expect pass.

### Task 3: Add `trade_cal`

**Files:**
- Modify: `tests/test_api.py`
- Modify: `src/api.py`

**Step 1:** Add failing tests for exchange, date range, `is_open`, fields, and date output.

**Step 2:** Implement `trade_cal` over `data/trade_cal/exchange=*/data.parquet`.

**Step 3:** Run `uv run pytest tests/test_api.py -v`; expect pass.

### Task 4: Add `daily` and `adj_factor`

**Files:**
- Modify: `tests/test_api.py`
- Modify: `src/api.py`

**Step 1:** Add failing tests for `trade_date`, date ranges, single and multiple `ts_code`, list fields, ambiguous dates, and date output.

**Step 2:** Implement shared daily-style query logic for `daily` and `adj_factor`.

**Step 3:** Run `uv run pytest tests/test_api.py -v`; expect pass.

### Task 5: Add Dispatch and Error Handling

**Files:**
- Modify: `tests/test_api.py`
- Modify: `src/api.py`

**Step 1:** Add failing tests for `query()`, unknown API names, unknown fields, missing data, invalid dates, and invalid date ranges.

**Step 2:** Implement field parsing, date parsing, missing data checks, and `query()`.

**Step 3:** Run `uv run pytest tests/test_api.py -v`; expect pass.

### Task 6: Final Verification

**Step 1:** Run `uv run pytest -q`.

**Step 2:** Run `git status --short` and `git diff --stat`.

**Step 3:** Commit the implementation with `git commit -m "feat: add local query api"`.
