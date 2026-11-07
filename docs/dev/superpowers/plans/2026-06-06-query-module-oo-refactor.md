# Query Module OO Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor `zer0share.query` to use object-oriented Parquet repositories and remove the old internal `query_daily_partitioned()` helper.

**Architecture:** Add `TableSpec`, `DailyTableSpec`, `SqlFilter`, `ParquetQueryEngine`, `BaseParquetRepository`, and `DailyPartitionRepository`. Query modules build explicit filters and call repositories directly; `LocalPro` keeps the public Tushare-like API stable.

**Tech Stack:** Python 3.11, DuckDB, Pandas, PyArrow, pytest, uv.

---

## File Structure

- Create `zer0share/query/repository.py`: table specs, SQL filters, DuckDB engine, base repository, daily repository.
- Modify `zer0share/query/_helpers.py`: keep only `parse_is_open()` and `format_date_columns()`; remove `query_daily_partitioned()`.
- Modify `zer0share/query/equities.py`: use repositories directly for stock, daily, index, universe, and `pro_bar` dependencies.
- Modify `zer0share/query/futures.py`: use repositories directly for futures static and daily partitioned tables.
- Modify `zer0share/query/options.py`: use repositories directly for options static and daily partitioned tables.
- Modify `zer0share/query/industry.py`: use repositories directly for industry static tables.
- Modify `zer0share/query/calendar.py`: use repositories directly for trade calendar.
- Add `tests/test_query_repository.py`: focused tests for repository primitives.

## Tasks

- [x] Add repository primitives and tests.
- [x] Remove `query_daily_partitioned()` from `_helpers.py`.
- [x] Migrate equities queries to repositories.
- [x] Migrate futures queries to repositories.
- [x] Migrate options queries to repositories.
- [x] Migrate industry and calendar queries to repositories.
- [x] Run repository tests, API tests, and full test suite.
- [x] Commit the scoped refactor.

## Verification Commands

Run after implementation:

```bash
PYTHONDONTWRITEBYTECODE=1 uv run pytest -p no:cacheprovider tests/test_query_repository.py -q
PYTHONDONTWRITEBYTECODE=1 uv run pytest -p no:cacheprovider tests/test_api.py -q
PYTHONDONTWRITEBYTECODE=1 uv run pytest -p no:cacheprovider tests -q
```

Expected: all tests pass.

## Compatibility Notes

Internal compatibility is intentionally not preserved:

- `query_daily_partitioned()` is removed.
- `data_dir_override` is removed.
- `extra_filters: dict` is replaced by explicit `SqlFilter` lists.

Public API compatibility remains required:

- `LocalPro.daily(...)`
- `LocalPro.opt_daily(...)`
- `LocalPro.fut_daily(...)`
- `LocalPro.query(api_name, **kwargs)`
