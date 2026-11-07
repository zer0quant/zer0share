# ETF Fund Daily Interface Design

## Goal

Add Tushare `fund_daily` to `zer0share` so ETF daily OHLCV data can be synced after market close, stored locally by trading day, queried through the local Pro API, and included in ETF batch syncs.

## Scope

Included:

- Fetch `pro.fund_daily` by `trade_date` for all ETFs on each trading day.
- Store one Parquet partition per trading day under the ETF topic directory.
- Register `fund_daily` in schema/catalog, sync jobs, CLI table choices, local API dispatch, README, and examples.
- Support local query filters for `ts_code`, `trade_date`, `start_date`, and `end_date`.
- Support `fields`, `limit`, and `offset`.
- Add focused tests for fetcher, sync pipeline, CLI, local API, and query dispatch.

Excluded:

- Per-ETF backfill loops using `ts_code + start_date/end_date`.
- ETF adjusted prices or ETF `pro_bar` support.
- ETF NAV, holdings, constituent weights, or creation/redemption basket data.
- Online smoke execution in automated tests. Tests should use mocked Tushare clients and local Parquet fixtures.

## Data Model

Add `FUND_DAILY_COLS`:

- `ts_code`
- `trade_date`
- `open`
- `high`
- `low`
- `close`
- `pre_close`
- `change`
- `pct_chg`
- `vol`
- `amount`

Add `FUND_DAILY_SPEC`:

- `name`: `fund_daily`
- `path_parts`: `("etf", "fund_daily")`
- `parquet_pattern`: `date=*/data.parquet`
- `sync_table`: `fund_daily`
- `order_by`: `ts_code, trade_date`
- `hive_partitioning`: `True`
- `union_by_name`: `True`
- `first_date`: `20100101`

The field order follows the Tushare interface. Dates stay as Tushare-style `YYYYMMDD` strings.

## Storage

Persist ETF daily bars as a daily partitioned table:

```text
data/
+-- etf/
    +-- fund_daily/
        +-- date=20250102/
        |   +-- data.parquet
        +-- date=20250103/
            +-- data.parquet
```

The table uses the same storage shape as `daily_kline`, `fut_daily`, and `opt_daily`, which keeps date-range sync and local DuckDB queries consistent across daily bar tables.

## Fetching

Add `TushareFetcher.fetch_fund_daily(trade_date: str)`.

The method calls:

```python
self._pro.fund_daily(
    trade_date=trade_date,
    fields=",".join(FUND_DAILY_COLS),
)
```

Return handling uses the existing `_select_columns_or_empty` helper so `None` and empty upstream responses return an empty DataFrame with the documented columns.

## Sync Flow

Extend `zer0share/sync/etf.py` with a `DailySyncJob`.

The job:

- table name: `fund_daily`
- spec: `FUND_DAILY_SPEC`
- fetch callable: `fetcher.fetch_fund_daily`
- store: `DailyPartitionStore(etf_dir / "fund_daily")`
- exchange calendar: default `SSE`
- date range support: enabled through `DailySyncJob`

User-facing sync entry points:

- `uv run python main.py sync --table fund_daily`
- `uv run python main.py sync --table fund_daily --start-date 20250101 --end-date 20250618`
- `uv run python main.py sync --etf`
- `uv run python main.py sync --all`

`fund_daily` should be included in `status` output through the existing CLI table list.

## Local Query API

Add `zer0share.query.etf.fund_daily`.

Parameters:

- `ts_code=None`
- `trade_date=None`
- `start_date=None`
- `end_date=None`
- `limit=None`
- `offset=None`
- `fields=None`

Filtering:

- `ts_code` supports comma-separated multi-value filtering through `DailyPartitionRepository`.
- `trade_date` selects one partition date.
- `start_date` and `end_date` filter partition dates inclusively.
- `fields`, `limit`, and `offset` reuse existing repository behavior.

Expose it through:

- `LocalPro.fund_daily(**kwargs)`
- `LocalPro.query("fund_daily", **kwargs)`

`trade_date`, `start_date`, and `end_date` are validated as `YYYYMMDD` by the local API date checker.

## Documentation And Example

Update README:

- Add `fund_daily` to the ETF sync command list.
- Mention the Tushare permission requirement: points >= 5000, with higher frequency at 8000 points.
- Add a local Pro API example:

```python
fund_daily = pro.fund_daily(
    ts_code="510330.SH",
    start_date="20250101",
    end_date="20250618",
    fields="trade_date,open,high,low,close,vol,amount",
)
```

Add `examples/etf/fund_daily_query_smoke.py`.

The smoke example should mirror the daily-table examples and cover:

- sample row lookup
- filter by `ts_code`
- filter by `trade_date`
- filter by `ts_code + trade_date`
- filter by `ts_code + start_date/end_date`
- `limit`
- `offset`
- `query_dispatch`

Update `examples/README.md` if it has an ETF examples list.

## Testing

Add or extend tests:

- `tests/test_fetcher.py`
  - fetcher calls `pro.fund_daily` with `trade_date` and the expected fields
  - fetcher returns documented columns
  - fetcher returns empty columns for `None`
- `tests/test_api.py`
  - `LocalPro.fund_daily` returns local data
  - filters by `ts_code`, `trade_date`, and date range
  - supports `limit`, `offset`, and `fields`
  - `query("fund_daily")` dispatches
  - invalid date formats raise through `_check_dates`
  - missing data raises a message mentioning `fund_daily`
- `tests/test_cli.py`
  - `sync --table fund_daily` calls pipeline
  - `sync --table fund_daily --start-date ... --end-date ...` is accepted
  - `sync --etf` includes `fund_daily`
- `tests/test_pipeline.py`
  - pipeline registry contains `fund_daily`
  - sync writes to `data/etf/fund_daily/date=YYYYMMDD/data.parquet`
  - sync updates the `fund_daily` metastore date

## Verification

Run focused tests first:

```bash
PYTHONDONTWRITEBYTECODE=1 uv run pytest -p no:cacheprovider tests/test_fetcher.py tests/test_api.py tests/test_cli.py tests/test_pipeline.py -q
```

If the focused suite passes, run the full test suite if practical:

```bash
PYTHONDONTWRITEBYTECODE=1 uv run pytest -p no:cacheprovider tests -q
```

## Open Decisions

There are no open design decisions. The implementation should use daily all-ETF `trade_date` sync, not per-code historical loops.
