# ETF Share Size Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add local sync and query support for Tushare `etf_share_size`.

**Architecture:** Model `etf_share_size` as a standard ETF daily partition table, matching `fund_daily` and `fund_adj`. Fetch each trading day by calling Tushare for `SSE` and `SZSE`, merge the rows, store them under `data/etf/etf_share_size/date=YYYYMMDD/data.parquet`, and expose local filters through `LocalPro.etf_share_size(...)`.

**Tech Stack:** Python 3.11+, pandas, Tushare Pro, DuckDB, Parquet, pytest, Click.

---

## File Structure

- Modify `zer0share/schema.py`: add `ETF_SHARE_SIZE_COLS`.
- Modify `zer0share/catalog.py`: import `ETF_SHARE_SIZE_COLS` and add `ETF_SHARE_SIZE_SPEC`.
- Modify `zer0share/fetcher.py`: import `ETF_SHARE_SIZE_COLS` and add `TushareFetcher.fetch_etf_share_size`.
- Modify `zer0share/sync/etf.py`: import `ETF_SHARE_SIZE_SPEC` and register a `DailySyncJob`.
- Modify `zer0share/cli.py`: add `etf_share_size` to `ETF_TABLES`.
- Modify `zer0share/query/etf.py`: import `ETF_SHARE_SIZE_SPEC` and add local query function with `exchange` filtering.
- Modify `zer0share/api.py`: expose `LocalPro.etf_share_size` and `query("etf_share_size")`.
- Modify `tests/test_fetcher.py`: add fetcher tests.
- Modify `tests/test_pipeline.py`: add sync tests.
- Modify `tests/test_cli.py`: add CLI tests and update ETF batch expectation.
- Modify `tests/test_api.py`: add local query tests.
- Modify `README.md`: add sync command, local API example, API list, storage tree, and CLI summary row.
- Modify `skills/zer0share-data/references/api.md`: add local API reference.
- Create `examples/etf/etf_share_size_query_smoke.py`: add manual local smoke query script.

---

### Task 1: Schema, Catalog, And Fetcher

**Files:**
- Modify: `tests/test_fetcher.py`
- Modify: `zer0share/schema.py`
- Modify: `zer0share/catalog.py`
- Modify: `zer0share/fetcher.py`

- [ ] **Step 1: Add failing fetcher tests**

In `tests/test_fetcher.py`, update the mock import:

```python
from unittest.mock import call, patch
```

In `tests/test_fetcher.py`, add this constant after `FUND_ADJ_COLS`:

```python
ETF_SHARE_SIZE_COLS = [
    "trade_date",
    "ts_code",
    "etf_name",
    "total_share",
    "total_size",
    "nav",
    "close",
    "exchange",
]
```

Add this helper near the ETF/fund helpers:

```python
def _etf_share_size_row(
    *,
    ts_code: str = "510330.SH",
    trade_date: str = "20250102",
    exchange: str = "SSE",
) -> dict[str, object]:
    return {
        "trade_date": trade_date,
        "ts_code": ts_code,
        "etf_name": "沪深300ETF华夏",
        "total_share": 3986754.98,
        "total_size": 15939050.0,
        "nav": 4.0,
        "close": 4.01,
        "exchange": exchange,
    }
```

Add these tests near the `fund_adj` fetcher tests:

```python
def test_fetch_etf_share_size_calls_both_exchanges_with_expected_fields(mock_pro):
    mock_pro.etf_share_size.side_effect = [
        pd.DataFrame([_etf_share_size_row(exchange="SSE")]),
        pd.DataFrame([_etf_share_size_row(ts_code="159919.SZ", exchange="SZSE")]),
    ]
    fetcher = TushareFetcher("fake_token")

    fetcher.fetch_etf_share_size("20250102")

    assert mock_pro.etf_share_size.call_args_list == [
        call(
            trade_date="20250102",
            exchange="SSE",
            fields=",".join(ETF_SHARE_SIZE_COLS),
        ),
        call(
            trade_date="20250102",
            exchange="SZSE",
            fields=",".join(ETF_SHARE_SIZE_COLS),
        ),
    ]


def test_fetch_etf_share_size_combines_exchange_rows(mock_pro):
    mock_pro.etf_share_size.side_effect = [
        pd.DataFrame([_etf_share_size_row(exchange="SSE")]),
        pd.DataFrame([_etf_share_size_row(ts_code="159919.SZ", exchange="SZSE")]),
    ]
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_etf_share_size("20250102")

    assert list(df.columns) == ETF_SHARE_SIZE_COLS
    assert df[["ts_code", "exchange"]].to_dict("records") == [
        {"ts_code": "510330.SH", "exchange": "SSE"},
        {"ts_code": "159919.SZ", "exchange": "SZSE"},
    ]


def test_fetch_etf_share_size_returns_empty_when_all_exchanges_empty(mock_pro):
    mock_pro.etf_share_size.side_effect = [None, pd.DataFrame()]
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_etf_share_size("20250102")

    assert df.empty
    assert list(df.columns) == ETF_SHARE_SIZE_COLS
```

- [ ] **Step 2: Run the fetcher tests to verify they fail**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 uv run pytest -p no:cacheprovider tests/test_fetcher.py::test_fetch_etf_share_size_calls_both_exchanges_with_expected_fields tests/test_fetcher.py::test_fetch_etf_share_size_combines_exchange_rows tests/test_fetcher.py::test_fetch_etf_share_size_returns_empty_when_all_exchanges_empty -q
```

Expected: FAIL because `TushareFetcher.fetch_etf_share_size` is not defined.

- [ ] **Step 3: Add schema and catalog metadata**

In `zer0share/schema.py`, add this immediately after `FUND_ADJ_COLS`:

```python
ETF_SHARE_SIZE_COLS = [
    "trade_date",
    "ts_code",
    "etf_name",
    "total_share",
    "total_size",
    "nav",
    "close",
    "exchange",
]
```

In `zer0share/catalog.py`, add `ETF_SHARE_SIZE_COLS` to the `from zer0share.schema import (...)` import list.

Then add this spec immediately after `FUND_ADJ_SPEC`:

```python
ETF_SHARE_SIZE_SPEC = DailyTableSpec(
    name="etf_share_size",
    path_parts=("etf", "etf_share_size"),
    columns=ETF_SHARE_SIZE_COLS,
    parquet_pattern="date=*/data.parquet",
    sync_table="etf_share_size",
    order_by="ts_code, trade_date",
    hive_partitioning=True,
    union_by_name=True,
    first_date="20100101",
)
```

- [ ] **Step 4: Implement fetcher method**

In `zer0share/fetcher.py`, add `ETF_SHARE_SIZE_COLS` to the schema import list.

Insert this method immediately after `fetch_fund_adj`:

```python
    def fetch_etf_share_size(self, trade_date: str) -> pd.DataFrame:
        logger.debug(f"拉取ETF份额规模: {trade_date}")
        frames = []
        for exchange in ("SSE", "SZSE"):
            df = self._pro.etf_share_size(
                trade_date=trade_date,
                exchange=exchange,
                fields=",".join(ETF_SHARE_SIZE_COLS),
            )
            if df is not None and not df.empty:
                frames.append(df)
        combined = (
            pd.concat(frames, ignore_index=True)
            if frames
            else pd.DataFrame(columns=ETF_SHARE_SIZE_COLS)
        )
        return _select_columns_or_empty(combined, ETF_SHARE_SIZE_COLS)
```

- [ ] **Step 5: Run the fetcher tests to verify they pass**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 uv run pytest -p no:cacheprovider tests/test_fetcher.py::test_fetch_etf_share_size_calls_both_exchanges_with_expected_fields tests/test_fetcher.py::test_fetch_etf_share_size_combines_exchange_rows tests/test_fetcher.py::test_fetch_etf_share_size_returns_empty_when_all_exchanges_empty -q
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add tests/test_fetcher.py zer0share/schema.py zer0share/catalog.py zer0share/fetcher.py
git commit -m "feat: fetch etf_share_size data"
```

---

### Task 2: Sync Job And CLI Registration

**Files:**
- Modify: `tests/test_pipeline.py`
- Modify: `tests/test_cli.py`
- Modify: `zer0share/sync/etf.py`
- Modify: `zer0share/cli.py`

- [ ] **Step 1: Add failing pipeline tests**

In `tests/test_pipeline.py`, update the ETF section comment to:

```python
# ---------------------------------------------------------------------------
# 17. fund_daily / fund_adj / etf_share_size / etf_basic / etf_index
# ---------------------------------------------------------------------------
```

Add this helper after `_fund_adj_df`:

```python
def _etf_share_size_df(trade_date: str = "20240102") -> pd.DataFrame:
    return pd.DataFrame({
        "trade_date": [trade_date],
        "ts_code": ["510330.SH"],
        "etf_name": ["沪深300ETF华夏"],
        "total_share": [3986754.98],
        "total_size": [15939050.0],
        "nav": [4.0],
        "close": [4.01],
        "exchange": ["SSE"],
    })
```

Add these tests after `test_sync_fund_adj_fetches_for_trading_day`:

```python
def test_sync_etf_share_size_writes_to_etf_subdir(pipeline, cfg, fetcher):
    _setup_trade_cal(pipeline, cfg, "20240102", True)
    fetcher.fetch_etf_share_size.return_value = _etf_share_size_df()

    pipeline.run("etf_share_size")

    assert (cfg.data_dir / "etf" / "etf_share_size" / "date=20240102" / "data.parquet").exists()
    assert pipeline._runtime.meta.get_last_date("etf_share_size") == "20240102"


def test_sync_etf_share_size_fetches_for_trading_day(pipeline, cfg, fetcher):
    _setup_trade_cal(pipeline, cfg, "20240102", True)
    fetcher.fetch_etf_share_size.return_value = _etf_share_size_df()

    pipeline.run("etf_share_size")

    fetcher.fetch_etf_share_size.assert_called_once_with("20240102")
```

- [ ] **Step 2: Add failing CLI tests**

In `tests/test_cli.py`, add these tests after `test_sync_fund_adj_accepts_date_range`:

```python
def test_sync_etf_share_size_calls_pipeline():
    runner = CliRunner()
    pipeline = _make_mock_pipeline()

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(cli, ["sync", "--table", "etf_share_size"])

    assert result.exit_code == 0
    pipeline.run.assert_called_once_with("etf_share_size", start_date=None, end_date=None)


def test_sync_etf_share_size_accepts_date_range():
    runner = CliRunner()
    pipeline = _make_mock_pipeline()

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(
            cli,
            [
                "sync",
                "--table",
                "etf_share_size",
                "--start-date",
                "20240101",
                "--end-date",
                "20240131",
            ],
        )

    assert result.exit_code == 0
    pipeline.run.assert_called_once_with(
        "etf_share_size",
        start_date="20240101",
        end_date="20240131",
    )
```

Update `test_sync_etf_calls_etf_tables` expected list to:

```python
    assert [call.args[0] for call in pipeline.run.call_args_list] == [
        "fund_daily",
        "fund_adj",
        "etf_share_size",
        "etf_basic",
        "etf_index",
    ]
```

- [ ] **Step 3: Run sync/CLI tests to verify they fail**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 uv run pytest -p no:cacheprovider tests/test_pipeline.py::test_sync_etf_share_size_writes_to_etf_subdir tests/test_pipeline.py::test_sync_etf_share_size_fetches_for_trading_day tests/test_cli.py::test_sync_etf_share_size_calls_pipeline tests/test_cli.py::test_sync_etf_share_size_accepts_date_range tests/test_cli.py::test_sync_etf_calls_etf_tables -q
```

Expected: FAIL because `etf_share_size` is not registered as a sync job or CLI table choice.

- [ ] **Step 4: Register the sync job**

In `zer0share/sync/etf.py`, change the catalog import to include `ETF_SHARE_SIZE_SPEC`:

```python
from zer0share.catalog import ETF_BASIC_SPEC, ETF_INDEX_SPEC, ETF_SHARE_SIZE_SPEC, FUND_DAILY_SPEC
```

Keep the existing `FUND_ADJ_SPEC` fallback as-is.

Insert this `DailySyncJob` immediately after the `fund_adj` job:

```python
        DailySyncJob(
            table_name=ETF_SHARE_SIZE_SPEC.name,
            spec=ETF_SHARE_SIZE_SPEC,
            fetch=fetcher.fetch_etf_share_size,
            store=DailyPartitionStore(etf_dir / "etf_share_size"),
        ),
```

- [ ] **Step 5: Register the CLI table**

In `zer0share/cli.py`, update `ETF_TABLES` to:

```python
ETF_TABLES = [
    "fund_daily",
    "fund_adj",
    "etf_share_size",
    "etf_basic",
    "etf_index",
]
```

- [ ] **Step 6: Run sync/CLI tests to verify they pass**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 uv run pytest -p no:cacheprovider tests/test_pipeline.py::test_sync_etf_share_size_writes_to_etf_subdir tests/test_pipeline.py::test_sync_etf_share_size_fetches_for_trading_day tests/test_cli.py::test_sync_etf_share_size_calls_pipeline tests/test_cli.py::test_sync_etf_share_size_accepts_date_range tests/test_cli.py::test_sync_etf_calls_etf_tables -q
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add tests/test_pipeline.py tests/test_cli.py zer0share/sync/etf.py zer0share/cli.py
git commit -m "feat: sync etf_share_size table"
```

---

### Task 3: Local Query API

**Files:**
- Modify: `tests/test_api.py`
- Modify: `zer0share/query/etf.py`
- Modify: `zer0share/api.py`

- [ ] **Step 1: Add failing API tests**

In `tests/test_api.py`, add this helper after `_make_fund_adj_df`:

```python
def _make_etf_share_size_df():
    return pd.DataFrame(
        {
            "trade_date": ["20240102", "20240103", "20240102", "20240103"],
            "ts_code": ["510330.SH", "510330.SH", "159919.SZ", "159919.SZ"],
            "etf_name": ["沪深300ETF华夏", "沪深300ETF华夏", "沪深300ETF嘉实", "沪深300ETF嘉实"],
            "total_share": [3986754.98, 3994674.98, 1200000.0, 1210000.0],
            "total_size": [15939050.0, 15781760.0, 2500000.0, 2510000.0],
            "nav": [4.0, 3.95, 2.1, 2.11],
            "close": [4.01, 3.96, 2.12, 2.13],
            "exchange": ["SSE", "SSE", "SZSE", "SZSE"],
        }
    )
```

Add these tests after `test_fund_adj_query_raises_when_no_data`:

```python
def test_etf_share_size_query_returns_local_data_and_selected_fields(tmp_path):
    data = _make_etf_share_size_df()
    DailyPartitionStore(tmp_path / "etf" / "etf_share_size").write("20240102", data[data["trade_date"] == "20240102"])
    DailyPartitionStore(tmp_path / "etf" / "etf_share_size").write("20240103", data[data["trade_date"] == "20240103"])

    api = LocalPro(tmp_path)
    result = api.etf_share_size(fields="trade_date,ts_code,etf_name,total_share,total_size,exchange")

    assert result.to_dict("records") == [
        {"trade_date": "20240102", "ts_code": "159919.SZ", "etf_name": "沪深300ETF嘉实", "total_share": 1200000.0, "total_size": 2500000.0, "exchange": "SZSE"},
        {"trade_date": "20240103", "ts_code": "159919.SZ", "etf_name": "沪深300ETF嘉实", "total_share": 1210000.0, "total_size": 2510000.0, "exchange": "SZSE"},
        {"trade_date": "20240102", "ts_code": "510330.SH", "etf_name": "沪深300ETF华夏", "total_share": 3986754.98, "total_size": 15939050.0, "exchange": "SSE"},
        {"trade_date": "20240103", "ts_code": "510330.SH", "etf_name": "沪深300ETF华夏", "total_share": 3994674.98, "total_size": 15781760.0, "exchange": "SSE"},
    ]


def test_etf_share_size_filters_by_ts_code_date_range_and_exchange(tmp_path):
    data = _make_etf_share_size_df()
    DailyPartitionStore(tmp_path / "etf" / "etf_share_size").write("20240102", data[data["trade_date"] == "20240102"])
    DailyPartitionStore(tmp_path / "etf" / "etf_share_size").write("20240103", data[data["trade_date"] == "20240103"])

    api = LocalPro(tmp_path)
    result = api.etf_share_size(
        ts_code="510330.SH,159919.SZ",
        start_date="20240103",
        end_date="20240103",
        exchange="SSE",
        fields="trade_date,ts_code,total_share,exchange",
    )

    assert result.to_dict("records") == [
        {"trade_date": "20240103", "ts_code": "510330.SH", "total_share": 3994674.98, "exchange": "SSE"}
    ]


def test_etf_share_size_supports_trade_date_limit_offset_and_query_dispatch(tmp_path):
    data = _make_etf_share_size_df()
    DailyPartitionStore(tmp_path / "etf" / "etf_share_size").write("20240102", data[data["trade_date"] == "20240102"])
    DailyPartitionStore(tmp_path / "etf" / "etf_share_size").write("20240103", data[data["trade_date"] == "20240103"])

    api = LocalPro(tmp_path)
    result = api.query(
        "etf_share_size",
        trade_date="20240102",
        offset=1,
        limit=1,
        fields="trade_date,ts_code,total_size,exchange",
    )

    assert result.to_dict("records") == [
        {"trade_date": "20240102", "ts_code": "510330.SH", "total_size": 15939050.0, "exchange": "SSE"}
    ]


def test_etf_share_size_validates_date_filters(tmp_path):
    data = _make_etf_share_size_df()
    DailyPartitionStore(tmp_path / "etf" / "etf_share_size").write("20240102", data[data["trade_date"] == "20240102"])

    api = LocalPro(tmp_path)
    with pytest.raises(ValueError, match="YYYYMMDD"):
        api.etf_share_size(trade_date="2024-01-02")


def test_etf_share_size_query_raises_when_no_data(tmp_path):
    api = LocalPro(tmp_path)

    with pytest.raises(FileNotFoundError, match="etf_share_size"):
        api.etf_share_size()
```

- [ ] **Step 2: Run API tests to verify they fail**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 uv run pytest -p no:cacheprovider tests/test_api.py::test_etf_share_size_query_returns_local_data_and_selected_fields tests/test_api.py::test_etf_share_size_filters_by_ts_code_date_range_and_exchange tests/test_api.py::test_etf_share_size_supports_trade_date_limit_offset_and_query_dispatch tests/test_api.py::test_etf_share_size_validates_date_filters tests/test_api.py::test_etf_share_size_query_raises_when_no_data -q
```

Expected: FAIL because `LocalPro.etf_share_size` is not defined.

- [ ] **Step 3: Implement query function**

In `zer0share/query/etf.py`, update the catalog import to include `ETF_SHARE_SIZE_SPEC`.

Append this function after `fund_adj`:

```python
def etf_share_size(
    ctx: QueryContext,
    ts_code=None,
    trade_date=None,
    start_date=None,
    end_date=None,
    exchange=None,
    limit: int | None = None,
    offset: int | None = None,
    fields=None,
) -> pd.DataFrame:
    """Query ETF daily share and scale data."""
    filters = []
    if exchange is not None:
        filters.append(eq_filter("exchange", exchange, ETF_SHARE_SIZE_SPEC.columns))
    return DailyPartitionRepository(ctx, ETF_SHARE_SIZE_SPEC).query(
        ts_code,
        trade_date,
        start_date,
        end_date,
        fields,
        filters=filters,
        limit=limit,
        offset=offset,
    )
```

If the current `try/except ImportError` fallback block still exists for `FUND_ADJ_SPEC`, remove the fallback and use a direct import:

```python
from zer0share.catalog import (
    ETF_BASIC_SPEC,
    ETF_INDEX_SPEC,
    ETF_SHARE_SIZE_SPEC,
    FUND_ADJ_SPEC,
    FUND_DAILY_SPEC,
)
```

- [ ] **Step 4: Expose LocalPro method and dispatch**

In `zer0share/api.py`, insert this method immediately after `fund_adj`:

```python
    def etf_share_size(self, **kwargs):
        _check_dates(kwargs)
        return etf.etf_share_size(self._ctx, **kwargs)
```

In the `dispatch` dict, insert after `"fund_adj": self.fund_adj,`:

```python
            "etf_share_size": self.etf_share_size,
```

- [ ] **Step 5: Run API tests to verify they pass**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 uv run pytest -p no:cacheprovider tests/test_api.py::test_etf_share_size_query_returns_local_data_and_selected_fields tests/test_api.py::test_etf_share_size_filters_by_ts_code_date_range_and_exchange tests/test_api.py::test_etf_share_size_supports_trade_date_limit_offset_and_query_dispatch tests/test_api.py::test_etf_share_size_validates_date_filters tests/test_api.py::test_etf_share_size_query_raises_when_no_data -q
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add tests/test_api.py zer0share/query/etf.py zer0share/api.py
git commit -m "feat: query etf_share_size locally"
```

---

### Task 4: Documentation And Smoke Example

**Files:**
- Modify: `README.md`
- Modify: `skills/zer0share-data/references/api.md`
- Create: `examples/etf/etf_share_size_query_smoke.py`

- [ ] **Step 1: Update README ETF sync command list**

In `README.md`, update the ETF command block to include:

```bash
uv run python main.py sync --table etf_basic       # ETF 基础信息
uv run python main.py sync --table etf_index       # ETF 基准指数列表
uv run python main.py sync --table fund_daily      # ETF 日线行情（需积分 >= 5000，8000 积分频次更高）
uv run python main.py sync --table fund_adj        # 基金复权因子（需积分 >= 600，5000 积分以上频次更高）
uv run python main.py sync --table etf_share_size  # ETF 份额规模（需积分 >= 8000，通常次日 08:30 后更新）
```

- [ ] **Step 2: Update README local query examples**

In the local API example block, add this after the `fund_adj` example:

```python
etf_share_size = pro.etf_share_size(
    ts_code="510330.SH",
    start_date="20250101",
    end_date="20251224",
    fields="trade_date,ts_code,etf_name,total_share,total_size,exchange",
)
```

- [ ] **Step 3: Update README supported method table**

Add this row after `fund_adj`:

```markdown
| `etf_share_size` | 查询已同步的 ETF 份额和规模 |
```

- [ ] **Step 4: Update README storage tree**

In the `data/etf/` tree, include:

```text
│   ├── fund_daily/
│   │   └── date=YYYYMMDD/data.parquet
│   ├── fund_adj/
│   │   └── date=YYYYMMDD/data.parquet
│   └── etf_share_size/
│       └── date=YYYYMMDD/data.parquet
```

- [ ] **Step 5: Update README CLI summary**

In the CLI summary table, add:

```markdown
| `sync --table etf_share_size` | 同步 ETF 份额规模 |
```

- [ ] **Step 6: Update local skill API reference**

In `skills/zer0share-data/references/api.md`, add this entry after `fund_adj`:

```markdown
- `etf_share_size(ts_code=None, trade_date=None, start_date=None, end_date=None, exchange=None, fields=None, limit=None, offset=None)`
  - Local sync table: `etf_share_size`
  - ETF daily share and scale data, including total share, total size, NAV, close, ETF name, and exchange.
```

- [ ] **Step 7: Add smoke example**

Create `examples/etf/etf_share_size_query_smoke.py`:

```python
from zer0share import pro_api


FIELDS = "trade_date,ts_code,etf_name,total_share,total_size,nav,close,exchange"


def _print_frame(title, df):
    print(f"\n=== {title} ===")
    print(f"rows={len(df)} columns={list(df.columns)}")
    print(df.head(10).to_string(index=False))


def main():
    pro = pro_api()
    limit = 5

    sample = pro.etf_share_size(limit=1, fields=FIELDS)
    if sample.empty:
        print("No etf_share_size data found. Run: uv run python main.py sync --table etf_share_size")
        return

    row = sample.iloc[0]
    ts_code = row["ts_code"]
    trade_date = row["trade_date"]
    exchange = row["exchange"]

    _print_frame(
        "filter_by_ts_code",
        pro.etf_share_size(ts_code=ts_code, limit=limit, fields=FIELDS),
    )
    _print_frame(
        "filter_by_trade_date_exchange",
        pro.etf_share_size(trade_date=trade_date, exchange=exchange, limit=limit, fields=FIELDS),
    )
    _print_frame(
        "offset_and_limit",
        pro.etf_share_size(trade_date=trade_date, offset=1, limit=limit, fields=FIELDS),
    )
    _print_frame(
        "query_dispatch",
        pro.query("etf_share_size", ts_code=ts_code, limit=limit, fields=FIELDS),
    )


if __name__ == "__main__":
    main()
```

- [ ] **Step 8: Run formatting/lint check for the smoke example**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 uv run python -m py_compile examples/etf/etf_share_size_query_smoke.py
```

Expected: PASS with no output.

- [ ] **Step 9: Commit**

```bash
git add README.md skills/zer0share-data/references/api.md examples/etf/etf_share_size_query_smoke.py
git commit -m "docs: document etf_share_size interface"
```

---

### Task 5: Final Verification

**Files:**
- Review all changed files.

- [ ] **Step 1: Run focused suite**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 uv run pytest -p no:cacheprovider tests/test_fetcher.py tests/test_pipeline.py tests/test_cli.py tests/test_api.py -q
```

Expected: PASS.

- [ ] **Step 2: Run full suite**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 uv run pytest -p no:cacheprovider tests -q
```

Expected: PASS. If failures occur outside the touched ETF surface, inspect whether they are pre-existing before changing unrelated code.

- [ ] **Step 3: Check git status**

Run:

```bash
git status --short
```

Expected: no uncommitted changes.

- [ ] **Step 4: Review commit history**

Run:

```bash
git log --oneline -5
```

Expected: recent commits include:

```text
feat: fetch etf_share_size data
feat: sync etf_share_size table
feat: query etf_share_size locally
docs: document etf_share_size interface
```

## Self-Review

- Spec coverage: this plan covers schema, catalog, fetcher, sync, CLI, local query API, README, skill API reference, smoke example, and tests for all required behaviors.
- Placeholder scan: no unresolved marker text, incomplete sections, or copy-forward task instructions remain.
- Type consistency: the table name is consistently `etf_share_size`; schema constant is `ETF_SHARE_SIZE_COLS`; catalog spec is `ETF_SHARE_SIZE_SPEC`; fetcher method is `fetch_etf_share_size`; local API method is `LocalPro.etf_share_size`.
