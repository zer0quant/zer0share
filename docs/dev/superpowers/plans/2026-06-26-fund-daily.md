# Fund Daily ETF Interface Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add local sync and query support for the Tushare `fund_daily` ETF daily OHLCV interface.

**Architecture:** Model `fund_daily` as an ETF-domain daily partitioned table using the existing `DailyTableSpec`, `DailyPartitionStore`, `DailySyncJob`, and `DailyPartitionRepository` patterns. Fetching pulls all ETF rows by `trade_date`; querying exposes Tushare-like filters through `pro.fund_daily(...)` and `pro.query("fund_daily", ...)`.

**Tech Stack:** Python 3.11, pandas, pyarrow Parquet, DuckDB, Click, pytest, Tushare Pro.

---

## File Structure

- Modify `zer0share/schema.py`: add `FUND_DAILY_COLS`.
- Modify `zer0share/catalog.py`: import `FUND_DAILY_COLS` and add `FUND_DAILY_SPEC`.
- Modify `zer0share/fetcher.py`: import `FUND_DAILY_COLS` and add `TushareFetcher.fetch_fund_daily`.
- Modify `zer0share/query/etf.py`: import `FUND_DAILY_SPEC`, `DailyPartitionRepository`, and add `fund_daily`.
- Modify `zer0share/api.py`: add `LocalPro.fund_daily` and dispatch support.
- Modify `zer0share/sync/etf.py`: import `FUND_DAILY_SPEC`, `DailyPartitionStore`, `DailySyncJob`, and register the daily job.
- Modify `zer0share/cli.py`: include `fund_daily` in `ETF_TABLES`.
- Modify `README.md`: document sync command, local API example, API table row, data tree, and CLI command summary.
- Create `examples/etf/fund_daily_query_smoke.py`: local query smoke script.
- Modify `examples/README.md`: list and show the ETF daily smoke example.
- Modify `tests/test_fetcher.py`: add fetcher tests.
- Modify `tests/test_api.py`: add local query/API tests.
- Modify `tests/test_pipeline.py`: add registry and sync pipeline coverage.
- Modify `tests/test_cli.py`: add CLI table and ETF batch coverage.

---

### Task 1: Schema, Catalog, And Fetcher

**Files:**
- Modify: `zer0share/schema.py`
- Modify: `zer0share/catalog.py`
- Modify: `zer0share/fetcher.py`
- Test: `tests/test_fetcher.py`

- [ ] **Step 1: Write failing fetcher tests**

Append this to the ETF section in `tests/test_fetcher.py`, after `test_fetch_etf_index_returns_empty_when_none`:

```python
FUND_DAILY_COLS = [
    "ts_code", "trade_date", "open", "high", "low",
    "close", "pre_close", "change", "pct_chg", "vol", "amount",
]


def _fund_daily_row() -> dict:
    return {
        "ts_code": "510330.SH",
        "trade_date": "20250618",
        "open": 4.008,
        "high": 4.024,
        "low": 3.996,
        "close": 4.017,
        "pre_close": 4.006,
        "change": 0.011,
        "pct_chg": 0.2746,
        "vol": 382896.00,
        "amount": 153574.446,
    }


def test_fetch_fund_daily_returns_correct_columns(mock_pro):
    mock_pro.fund_daily.return_value = pd.DataFrame([_fund_daily_row()])
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_fund_daily("20250618")

    assert list(df.columns) == FUND_DAILY_COLS
    assert df.to_dict("records") == [_fund_daily_row()]


def test_fetch_fund_daily_calls_api_with_trade_date_and_fields(mock_pro):
    mock_pro.fund_daily.return_value = pd.DataFrame([_fund_daily_row()])
    fetcher = TushareFetcher("fake_token")

    fetcher.fetch_fund_daily("20250618")

    mock_pro.fund_daily.assert_called_once_with(
        trade_date="20250618",
        fields=",".join(FUND_DAILY_COLS),
    )


def test_fetch_fund_daily_returns_empty_when_none(mock_pro):
    mock_pro.fund_daily.return_value = None
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_fund_daily("20250618")

    assert df.empty
    assert list(df.columns) == FUND_DAILY_COLS
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 uv run pytest -p no:cacheprovider tests/test_fetcher.py::test_fetch_fund_daily_returns_correct_columns tests/test_fetcher.py::test_fetch_fund_daily_calls_api_with_trade_date_and_fields tests/test_fetcher.py::test_fetch_fund_daily_returns_empty_when_none -q
```

Expected: FAIL with `AttributeError: 'TushareFetcher' object has no attribute 'fetch_fund_daily'`.

- [ ] **Step 3: Add schema columns**

In `zer0share/schema.py`, add this after `ETF_INDEX_COLS`:

```python
FUND_DAILY_COLS = [
    "ts_code", "trade_date", "open", "high", "low",
    "close", "pre_close", "change", "pct_chg", "vol", "amount",
]
```

- [ ] **Step 4: Add catalog spec**

In `zer0share/catalog.py`, add `FUND_DAILY_COLS` to the `from zer0share.schema import (...)` import list.

Add this after `ETF_INDEX_SPEC`:

```python
FUND_DAILY_SPEC = DailyTableSpec(
    name="fund_daily",
    path_parts=("etf", "fund_daily"),
    columns=FUND_DAILY_COLS,
    parquet_pattern="date=*/data.parquet",
    sync_table="fund_daily",
    order_by="ts_code, trade_date",
    hive_partitioning=True,
    union_by_name=True,
    first_date="20100101",
)
```

- [ ] **Step 5: Add fetcher method**

In `zer0share/fetcher.py`, add `FUND_DAILY_COLS` to the schema import list.

Add this method after `fetch_etf_index`:

```python
    def fetch_fund_daily(self, trade_date: str) -> pd.DataFrame:
        logger.debug(f"拉取ETF日线行情: {trade_date}")
        df = self._pro.fund_daily(
            trade_date=trade_date,
            fields=",".join(FUND_DAILY_COLS),
        )
        return _select_columns_or_empty(df, FUND_DAILY_COLS)
```

- [ ] **Step 6: Run tests to verify they pass**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 uv run pytest -p no:cacheprovider tests/test_fetcher.py::test_fetch_fund_daily_returns_correct_columns tests/test_fetcher.py::test_fetch_fund_daily_calls_api_with_trade_date_and_fields tests/test_fetcher.py::test_fetch_fund_daily_returns_empty_when_none -q
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add zer0share/schema.py zer0share/catalog.py zer0share/fetcher.py tests/test_fetcher.py
git commit -m "feat: add fund_daily fetcher metadata"
```

---

### Task 2: Local Query API

**Files:**
- Modify: `zer0share/query/etf.py`
- Modify: `zer0share/api.py`
- Test: `tests/test_api.py`

- [ ] **Step 1: Write failing API tests**

In `tests/test_api.py`, add this helper after `_make_etf_index_df`:

```python
def _make_fund_daily_df(trade_date: str):
    return pd.DataFrame({
        "ts_code": ["510330.SH", "159919.SZ"],
        "trade_date": [trade_date, trade_date],
        "open": [4.008, 3.980],
        "high": [4.024, 4.010],
        "low": [3.996, 3.970],
        "close": [4.017, 4.002],
        "pre_close": [4.006, 3.990],
        "change": [0.011, 0.012],
        "pct_chg": [0.2746, 0.3008],
        "vol": [382896.00, 250000.00],
        "amount": [153574.446, 100250.000],
    })
```

Add these tests after `test_etf_index_query_raises_when_no_data`:

```python
def test_fund_daily_query_filters_code_and_date_range(tmp_path):
    DailyPartitionStore(tmp_path / "etf" / "fund_daily").write(
        "20250617", _make_fund_daily_df("20250617")
    )
    DailyPartitionStore(tmp_path / "etf" / "fund_daily").write(
        "20250618", _make_fund_daily_df("20250618")
    )

    api = LocalPro(tmp_path)
    result = api.fund_daily(
        ts_code="510330.SH",
        start_date="20250618",
        end_date="20250618",
        fields="ts_code,trade_date,close,amount",
    )

    assert result.to_dict("records") == [
        {
            "ts_code": "510330.SH",
            "trade_date": "20250618",
            "close": 4.017,
            "amount": 153574.446,
        }
    ]


def test_fund_daily_query_supports_trade_date_limit_offset_and_dispatch(tmp_path):
    DailyPartitionStore(tmp_path / "etf" / "fund_daily").write(
        "20250618", _make_fund_daily_df("20250618")
    )

    api = LocalPro(tmp_path)
    result = api.query(
        "fund_daily",
        trade_date="20250618",
        offset=1,
        limit=1,
        fields=["ts_code", "trade_date", "close"],
    )

    assert result.to_dict("records") == [
        {"ts_code": "510330.SH", "trade_date": "20250618", "close": 4.017}
    ]


def test_fund_daily_validates_date_filters(tmp_path):
    DailyPartitionStore(tmp_path / "etf" / "fund_daily").write(
        "20250618", _make_fund_daily_df("20250618")
    )

    api = LocalPro(tmp_path)
    with pytest.raises(ValueError, match="YYYYMMDD"):
        api.fund_daily(start_date="2025-06-18")


def test_fund_daily_query_raises_when_no_data(tmp_path):
    api = LocalPro(tmp_path)

    with pytest.raises(FileNotFoundError, match="fund_daily"):
        api.fund_daily()
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 uv run pytest -p no:cacheprovider tests/test_api.py::test_fund_daily_query_filters_code_and_date_range tests/test_api.py::test_fund_daily_query_supports_trade_date_limit_offset_and_dispatch tests/test_api.py::test_fund_daily_validates_date_filters tests/test_api.py::test_fund_daily_query_raises_when_no_data -q
```

Expected: FAIL with `AttributeError: 'LocalPro' object has no attribute 'fund_daily'`.

- [ ] **Step 3: Add query function**

In `zer0share/query/etf.py`, change the imports to include `FUND_DAILY_SPEC` and `DailyPartitionRepository`:

```python
from zer0share.catalog import ETF_BASIC_SPEC, ETF_INDEX_SPEC, FUND_DAILY_SPEC
from zer0share.query.repository import (
    BaseParquetRepository,
    DailyPartitionRepository,
    eq_filter,
    in_filter,
)
```

Add this function after `etf_index`:

```python
def fund_daily(
    ctx: QueryContext,
    ts_code=None,
    trade_date=None,
    start_date=None,
    end_date=None,
    limit: int | None = None,
    offset: int | None = None,
    fields=None,
) -> pd.DataFrame:
    """Query ETF daily OHLCV bars."""
    return DailyPartitionRepository(ctx, FUND_DAILY_SPEC).query(
        ts_code,
        trade_date,
        start_date,
        end_date,
        fields,
        limit=limit,
        offset=offset,
    )
```

- [ ] **Step 4: Expose API method and dispatch**

In `zer0share/api.py`, add this method after `etf_index`:

```python
    def fund_daily(self, **kwargs):
        _check_dates(kwargs)
        return etf.fund_daily(self._ctx, **kwargs)
```

In `LocalPro.query`, add this dispatch entry after `"etf_index": self.etf_index,`:

```python
            "fund_daily": self.fund_daily,
```

- [ ] **Step 5: Run tests to verify they pass**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 uv run pytest -p no:cacheprovider tests/test_api.py::test_fund_daily_query_filters_code_and_date_range tests/test_api.py::test_fund_daily_query_supports_trade_date_limit_offset_and_dispatch tests/test_api.py::test_fund_daily_validates_date_filters tests/test_api.py::test_fund_daily_query_raises_when_no_data -q
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add zer0share/query/etf.py zer0share/api.py tests/test_api.py
git commit -m "feat: expose fund_daily local query"
```

---

### Task 3: Sync Pipeline And CLI

**Files:**
- Modify: `zer0share/sync/etf.py`
- Modify: `zer0share/cli.py`
- Test: `tests/test_pipeline.py`
- Test: `tests/test_cli.py`

- [ ] **Step 1: Write failing pipeline tests**

In `tests/test_pipeline.py`, update `test_pipeline_registry_contains_all_tables`:

```python
        "etf_basic", "etf_index", "fund_daily",
```

Change the final assertion in that test:

```python
    assert len(pipeline.registry) == 28
```

Add this helper after `_etf_index_df`:

```python
def _fund_daily_df(trade_date: str = "20240102") -> pd.DataFrame:
    return pd.DataFrame({
        "ts_code": ["510330.SH"],
        "trade_date": [trade_date],
        "open": [4.008],
        "high": [4.024],
        "low": [3.996],
        "close": [4.017],
        "pre_close": [4.006],
        "change": [0.011],
        "pct_chg": [0.2746],
        "vol": [382896.00],
        "amount": [153574.446],
    })
```

Add these tests after the ETF index sync tests:

```python
def test_fund_daily_spec_uses_etf_market_first_date(pipeline):
    assert pipeline.registry["fund_daily"].spec.first_date == "20100101"


def test_sync_fund_daily_writes_daily_partition(pipeline, cfg, fetcher):
    fetcher.fetch_fund_daily.return_value = _fund_daily_df("20240102")
    _setup_trade_cal_sse(pipeline, cfg)

    pipeline.run("fund_daily")

    assert (cfg.data_dir / "etf" / "fund_daily" / "date=20240102" / "data.parquet").exists()
    assert pipeline._runtime.meta.get_last_date("fund_daily") == "20240102"
    fetcher.fetch_fund_daily.assert_called_once_with("20240102")
```

- [ ] **Step 2: Write failing CLI tests**

In `tests/test_cli.py`, update `test_sync_etf_calls_etf_tables` expected calls to include `fund_daily`:

```python
    pipeline.run.assert_any_call(
        "fund_daily", start_date=None, end_date=None
    )
    assert pipeline.run.call_count == 3
```

Add these tests after `test_sync_etf_index_calls_pipeline`:

```python
def test_sync_fund_daily_calls_pipeline():
    runner = CliRunner()
    pipeline = _make_mock_pipeline()

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(cli, ["sync", "--table", "fund_daily"])

    assert result.exit_code == 0
    pipeline.run.assert_called_once_with("fund_daily", start_date=None, end_date=None)


def test_sync_fund_daily_accepts_date_range():
    runner = CliRunner()
    pipeline = _make_mock_pipeline()

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(
            cli,
            [
                "sync",
                "--table",
                "fund_daily",
                "--start-date",
                "20250101",
                "--end-date",
                "20250618",
            ],
        )

    assert result.exit_code == 0
    pipeline.run.assert_called_once_with(
        "fund_daily",
        start_date="20250101",
        end_date="20250618",
    )
```

- [ ] **Step 3: Run tests to verify they fail**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 uv run pytest -p no:cacheprovider tests/test_pipeline.py::test_pipeline_registry_contains_all_tables tests/test_pipeline.py::test_fund_daily_spec_uses_etf_market_first_date tests/test_pipeline.py::test_sync_fund_daily_writes_daily_partition tests/test_cli.py::test_sync_etf_calls_etf_tables tests/test_cli.py::test_sync_fund_daily_calls_pipeline tests/test_cli.py::test_sync_fund_daily_accepts_date_range -q
```

Expected: FAIL because `fund_daily` is not registered in the pipeline or CLI choices.

- [ ] **Step 4: Register sync job**

Replace the imports in `zer0share/sync/etf.py` with:

```python
from zer0share.catalog import ETF_BASIC_SPEC, ETF_INDEX_SPEC, FUND_DAILY_SPEC
from zer0share.storage import DailyPartitionStore, SnapshotStore
from zer0share.sync._jobs import DailySyncJob, SnapshotSyncJob, SyncJob
```

Add this job to the list returned by `build_jobs`, after the `etf_index` snapshot job:

```python
        DailySyncJob(
            table_name=FUND_DAILY_SPEC.name,
            spec=FUND_DAILY_SPEC,
            fetch=fetcher.fetch_fund_daily,
            store=DailyPartitionStore(etf_dir / "fund_daily"),
        ),
```

- [ ] **Step 5: Add CLI table choice**

In `zer0share/cli.py`, add `fund_daily` to `ETF_TABLES`:

```python
ETF_TABLES = [
    "etf_basic",
    "etf_index",
    "fund_daily",
]
```

- [ ] **Step 6: Run tests to verify they pass**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 uv run pytest -p no:cacheprovider tests/test_pipeline.py::test_pipeline_registry_contains_all_tables tests/test_pipeline.py::test_fund_daily_spec_uses_etf_market_first_date tests/test_pipeline.py::test_sync_fund_daily_writes_daily_partition tests/test_cli.py::test_sync_etf_calls_etf_tables tests/test_cli.py::test_sync_fund_daily_calls_pipeline tests/test_cli.py::test_sync_fund_daily_accepts_date_range -q
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add zer0share/sync/etf.py zer0share/cli.py tests/test_pipeline.py tests/test_cli.py
git commit -m "feat: sync fund_daily ETF bars"
```

---

### Task 4: Documentation And Smoke Example

**Files:**
- Modify: `README.md`
- Create: `examples/etf/fund_daily_query_smoke.py`
- Modify: `examples/README.md`

- [ ] **Step 1: Add smoke example script**

Create `examples/etf/fund_daily_query_smoke.py`:

```python
from __future__ import annotations

import argparse
import sys

from zer0share import pro_api


FIELDS = "ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount"


def _print_frame(title, df, rows: int = 5) -> None:
    print(f"\n## {title}")
    print(f"rows: {len(df)}")
    if df.empty:
        print("(empty)")
        return
    print(df.head(rows).to_string(index=False))


def _pick_sample(pro, ts_code: str, start_date: str, end_date: str):
    sample = pro.fund_daily(
        ts_code=ts_code,
        start_date=start_date,
        end_date=end_date,
        limit=1,
        fields=FIELDS,
    )
    if sample.empty:
        raise ValueError(
            "no fund_daily sample found; try a different --ts-code or date range"
        )
    return sample.iloc[0]


def run_smoke(ts_code: str, trade_date: str, start_date: str, end_date: str,
              offset: int, limit: int) -> None:
    pro = pro_api()
    sample = _pick_sample(pro, ts_code=ts_code, start_date=start_date, end_date=end_date)
    sample_trade_date = sample["trade_date"]

    print("Sample values")
    print(f"ts_code={ts_code}")
    print(f"sample_trade_date={sample_trade_date}")
    print(f"trade_date_arg={trade_date}")
    print(f"start_date={start_date}")
    print(f"end_date={end_date}")
    print(f"offset={offset}")
    print(f"limit={limit}")

    _print_frame(
        "filter_by_ts_code",
        pro.fund_daily(ts_code=ts_code, limit=limit, fields=FIELDS),
    )
    _print_frame(
        "filter_by_trade_date",
        pro.fund_daily(trade_date=trade_date, limit=limit, fields=FIELDS),
    )
    _print_frame(
        "filter_by_ts_code_and_trade_date",
        pro.fund_daily(ts_code=ts_code, trade_date=sample_trade_date, fields=FIELDS),
    )
    _print_frame(
        "filter_by_ts_code_and_date_range",
        pro.fund_daily(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
            fields=FIELDS,
        ),
    )
    _print_frame(
        "offset_and_limit",
        pro.fund_daily(trade_date=trade_date, offset=offset, limit=limit, fields=FIELDS),
    )
    _print_frame(
        "no_fields_filter",
        pro.fund_daily(ts_code=ts_code, start_date=start_date, end_date=end_date, limit=limit),
    )
    _print_frame(
        "query_dispatch",
        pro.query(
            "fund_daily",
            ts_code=ts_code,
            trade_date=sample_trade_date,
            offset=0,
            limit=1,
            fields=FIELDS,
        ),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Smoke-test fund_daily local queries against synced Parquet data."
    )
    parser.add_argument("--ts-code", default="510330.SH")
    parser.add_argument("--trade-date", default="20250618")
    parser.add_argument("--start-date", default="20250101")
    parser.add_argument("--end-date", default="20250618")
    parser.add_argument("--offset", type=int, default=1)
    parser.add_argument("--limit", type=int, default=5)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        run_smoke(
            ts_code=args.ts_code,
            trade_date=args.trade_date,
            start_date=args.start_date,
            end_date=args.end_date,
            offset=args.offset,
            limit=args.limit,
        )
    except FileNotFoundError as exc:
        print(f"Missing local data: {exc}", file=sys.stderr)
        print("Run the relevant sync command first:", file=sys.stderr)
        print(
            "  uv run python main.py sync --table fund_daily --start-date 20250101 --end-date 20250618",
            file=sys.stderr,
        )
        return 1
    except ValueError as exc:
        print(f"Invalid sample query: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 2: Update README sync commands**

In `README.md`, add this in the ETF sync command block after `etf_index`:

```bash
uv run python main.py sync --table fund_daily   # ETF 日线行情（需积分 >= 5000，8000 积分频次更高）
```

- [ ] **Step 3: Update README local API examples**

In `README.md`, add this after the existing `etf_index` local API example:

```python
fund_daily = pro.fund_daily(
    ts_code="510330.SH",
    start_date="20250101",
    end_date="20250618",
    fields="trade_date,open,high,low,close,vol,amount",
)
```

- [ ] **Step 4: Update README API and storage summaries**

In the README API table, add:

```markdown
| `fund_daily` | 查询已同步的 ETF 日线行情 |
```

In the README data directory tree under `data/etf/`, add:

```text
│   ├── fund_daily/
│   │   └── date=YYYYMMDD/data.parquet
```

In the README CLI command summary, add:

```markdown
| `sync --table fund_daily` | 同步 ETF 日线行情 |
```

- [ ] **Step 5: Update examples README**

In `examples/README.md`, add this row to the ETF table:

```markdown
| `fund_daily_query_smoke.py` | `--ts-code 510330.SH --start-date 20250101 --end-date 20250618` |
```

Add this command to the ETF command block:

```bash
uv run python examples/etf/fund_daily_query_smoke.py
```

- [ ] **Step 6: Run documentation checks**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 uv run python examples/etf/fund_daily_query_smoke.py --help
rg -n "fund_daily|ETF 日线" README.md examples/README.md examples/etf/fund_daily_query_smoke.py
```

Expected: the help command exits 0; `rg` prints the new documentation and script references.

- [ ] **Step 7: Commit**

```bash
git add README.md examples/README.md examples/etf/fund_daily_query_smoke.py
git commit -m "docs: document fund_daily ETF bars"
```

---

### Task 5: Focused And Full Verification

**Files:**
- Verify all modified files from Tasks 1-4.

- [ ] **Step 1: Run focused suite**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 uv run pytest -p no:cacheprovider tests/test_fetcher.py tests/test_api.py tests/test_cli.py tests/test_pipeline.py -q
```

Expected: PASS.

- [ ] **Step 2: Run full suite**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 uv run pytest -p no:cacheprovider tests -q
```

Expected: PASS.

- [ ] **Step 3: Inspect git status and recent commits**

Run:

```bash
git status --short
git log --oneline -5
```

Expected: working tree is clean. Recent commits include:

```text
feat: add fund_daily fetcher metadata
feat: expose fund_daily local query
feat: sync fund_daily ETF bars
docs: document fund_daily ETF bars
```

- [ ] **Step 4: Manual local sync smoke command**

Do not run this unless the environment has a valid Tushare token and enough points. The command is the intended manual verification path:

```bash
uv run python main.py sync --table fund_daily --start-date 20250101 --end-date 20250131
uv run python examples/etf/fund_daily_query_smoke.py --ts-code 510330.SH --start-date 20250101 --end-date 20250131
```

Expected: sync writes `data/etf/fund_daily/date=*/data.parquet`; the smoke script prints non-empty local query frames when data exists for the selected ETF.

---

## Self-Review

- Spec coverage: schema, catalog, fetcher, daily sync, CLI registration, local query API, README, smoke example, and focused tests are all covered by Tasks 1-5.
- Scope: the plan implements daily all-ETF `trade_date` sync only. It does not add per-code backfill loops, adjusted ETF prices, NAV, holdings, constituent weights, or creation/redemption basket data.
- Type consistency: the table name is consistently `fund_daily`; schema constant is `FUND_DAILY_COLS`; catalog spec is `FUND_DAILY_SPEC`; fetcher method is `fetch_fund_daily`; local API method is `fund_daily`.
- Verification: focused and full pytest commands are included, plus a manual online sync smoke path gated on local Tushare credentials.
