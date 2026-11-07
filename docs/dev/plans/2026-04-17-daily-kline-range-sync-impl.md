# Daily Kline Range Sync Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add date-bounded `daily_kline` sync so users can backfill historical ranges from a specified `start_date` and optional `end_date`, using the trade calendar and skipping already existing daily partitions by default.

**Architecture:** Keep one `sync_daily_kline` entry point in `Pipeline`, but let it accept optional range bounds. The CLI passes `start_date` and `end_date`, the pipeline resolves trading days from `trade_cal`, skips existing partitions, and only advances `sync_meta` when the processed range extends the latest known frontier.

**Tech Stack:** Python 3.11, Click, pytest, DuckDB, PyArrow Parquet

---

### Task 1: Lock in CLI Semantics With Tests

**Files:**
- Modify: `tests/test_cli.py`

**Step 1: Write a failing test for `start-date` / `end-date` passthrough**

Add a CLI test that invokes:

```python
runner.invoke(cli, [
    "sync",
    "--table", "daily_kline",
    "--start-date", "2016-01-01",
    "--end-date", "2016-01-31",
])
```

Assert `pipeline.sync_daily_kline` is called with parsed `date(2016, 1, 1)` and `date(2016, 1, 31)`.

**Step 2: Run the test to verify it fails**

Run: `python -m pytest tests/test_cli.py::test_sync_daily_kline_accepts_date_range -v`

Expected: FAIL because the CLI currently does not define these options.

**Step 3: Write a failing validation test for `end-date` without `start-date`**

Invoke:

```python
runner.invoke(cli, ["sync", "--table", "daily_kline", "--end-date", "2016-01-31"])
```

Assert non-zero exit code and an error message explaining that `--end-date` requires `--start-date`.

**Step 4: Run the validation test to verify it fails**

Run: `python -m pytest tests/test_cli.py::test_sync_end_date_requires_start_date -v`

Expected: FAIL because the CLI currently has no such validation.

### Task 2: Lock in Pipeline Range Behavior With Tests

**Files:**
- Modify: `tests/test_pipeline.py`

**Step 1: Write a failing test for range-mode skip-existing behavior**

Set up SSE trade calendar covering three open days. Pre-create the partition for the middle day, then call:

```python
pipeline.sync_daily_kline(start_date=date(2024, 1, 2), end_date=date(2024, 1, 4))
```

Assert the fetcher is called only for the missing days.

**Step 2: Run the test to verify it fails**

Run: `python -m pytest tests/test_pipeline.py::test_sync_daily_kline_range_skips_existing_partitions -v`

Expected: FAIL because the pipeline does not accept `start_date` / `end_date` yet.

**Step 3: Write a failing test for `last_date` safety during old-range backfill**

Pre-set:

```python
pipeline._meta.update_last_date("daily_kline", date(2024, 2, 1))
```

Backfill an older range in January and assert `last_date` remains `2024-02-01`.

**Step 4: Run the test to verify it fails**

Run: `python -m pytest tests/test_pipeline.py::test_sync_daily_kline_old_range_does_not_rewind_meta -v`

Expected: FAIL because range-mode meta rules do not exist yet.

**Step 5: Write a failing test for open-ended range**

Patch `date.today()` to a known value, call `sync_daily_kline(start_date=...)`, and assert the range runs through patched today.

**Step 6: Run the test to verify it fails**

Run: `python -m pytest tests/test_pipeline.py::test_sync_daily_kline_range_defaults_end_date_to_today -v`

Expected: FAIL because `sync_daily_kline` currently accepts no parameters.

### Task 3: Add Minimal Storage Helper

**Files:**
- Modify: `tests/test_storage.py`
- Modify: `src/storage.py`

**Step 1: Write a failing test for partition existence helper**

Add:

```python
assert daily_kline_partition_exists(tmp_path, date(2024, 1, 2)) is False
write_daily_kline(...)
assert daily_kline_partition_exists(tmp_path, date(2024, 1, 2)) is True
```

**Step 2: Run the test to verify it fails**

Run: `python -m pytest tests/test_storage.py::test_daily_kline_partition_exists -v`

Expected: FAIL because helper is missing.

**Step 3: Implement the minimal helper**

Expose:

```python
def daily_kline_partition_exists(data_dir: Path, trade_date: date) -> bool:
    ...
```

Use the same partition path convention as `write_daily_kline`.

**Step 4: Run the test to verify it passes**

Run: `python -m pytest tests/test_storage.py::test_daily_kline_partition_exists -v`

Expected: PASS.

### Task 4: Implement CLI and Pipeline Changes

**Files:**
- Modify: `src/cli.py`
- Modify: `src/pipeline.py`

**Step 1: Add CLI options**

Add:

```python
@click.option("--start-date", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option("--end-date", type=click.DateTime(formats=["%Y-%m-%d"]))
```

Convert values to `date`.

**Step 2: Add CLI validation**

Rules:
- `--end-date` requires `--start-date`
- `end_date` must not be earlier than `start_date`
- Range options are only valid when syncing `daily_kline`

**Step 3: Update pipeline signature**

Change:

```python
def sync_daily_kline(self, start_date: date | None = None, end_date: date | None = None) -> None:
```

**Step 4: Implement range resolution**

Behavior:
- If `start_date` is `None`, preserve incremental mode
- If `start_date` is set and `end_date` is `None`, default `end_date` to `today`
- Use `trade_cal` to get actual trading days in the chosen range

**Step 5: Implement skip-existing**

Before fetching, check the partition helper and skip existing dates.

**Step 6: Implement safe meta updates**

Only advance `daily_kline.last_date` when writing a date newer than the current stored frontier.

**Step 7: Run focused tests**

Run:

```bash
python -m pytest tests/test_cli.py tests/test_pipeline.py tests/test_storage.py -v
```

Expected: PASS.

### Task 5: Run Relevant Full Verification

**Files:**
- Check: `tests/test_fetcher.py`
- Check: `tests/test_storage.py`
- Check: `tests/test_config.py`
- Check: `tests/test_pipeline.py`
- Check: `tests/test_cli.py`
- Check: `tests/test_scheduler.py`

**Step 1: Run the relevant suite**

Run:

```bash
python -m pytest tests/test_fetcher.py tests/test_storage.py tests/test_config.py tests/test_pipeline.py tests/test_cli.py tests/test_scheduler.py -v
```

Expected: PASS.

