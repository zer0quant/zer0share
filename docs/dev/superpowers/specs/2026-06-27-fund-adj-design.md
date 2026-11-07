# Fund Adjustment Factor Interface Design

## Goal

Add Tushare `fund_adj` to `zer0share` so fund adjustment factors can be synced by trading day, stored locally, queried through the local Pro API, and included in ETF batch syncs.

## Scope

Included:

- Fetch `pro.fund_adj` by `trade_date` for all funds on each trading day.
- Store one Parquet partition per trading day under the ETF topic directory.
- Register `fund_adj` in schema/catalog, sync jobs, CLI table choices, local API dispatch, README, and local API references.
- Support local query filters for `ts_code`, `trade_date`, `start_date`, and `end_date`.
- Support `fields`, `limit`, and `offset`.
- Add focused tests for fetcher, sync pipeline, CLI, local API, and query dispatch.

Excluded:

- Per-fund backfill loops using `ts_code + start_date/end_date`.
- ETF or fund adjusted price calculation helpers.
- ETF `pro_bar` support.
- Online smoke execution in automated tests. Tests should use mocked Tushare clients and local Parquet fixtures.

## Data Model

Add `FUND_ADJ_COLS`:

- `ts_code`
- `trade_date`
- `adj_factor`
- `discount_rate`

The Tushare page lists `discount_rate` as a return field, even though the sample output only shows `adj_factor`. Persisting it keeps the local schema aligned with the documented interface.

Add `FUND_ADJ_SPEC`:

- `name`: `fund_adj`
- `path_parts`: `("etf", "fund_adj")`
- `parquet_pattern`: `date=*/data.parquet`
- `sync_table`: `fund_adj`
- `order_by`: `ts_code, trade_date`
- `hive_partitioning`: `True`
- `union_by_name`: `True`
- `first_date`: `20100101`

Dates stay as Tushare-style `YYYYMMDD` strings.

## Storage

Persist fund adjustment factors as a daily partitioned table:

```text
data/
+-- etf/
    +-- fund_adj/
        +-- date=20250102/
        |   +-- data.parquet
        +-- date=20250103/
            +-- data.parquet
```

The storage shape matches `fund_daily` and the other daily market data tables, which keeps date-range sync and local DuckDB queries consistent.

## Fetching

Add `TushareFetcher.fetch_fund_adj(trade_date: str)`.

The method calls:

```python
self._pro.fund_adj(
    trade_date=trade_date,
    fields=",".join(FUND_ADJ_COLS),
)
```

Return handling uses the existing `_select_columns_or_empty` helper so `None` and empty upstream responses return an empty DataFrame with the documented columns.

## Sync Flow

Extend `zer0share/sync/etf.py` with a `DailySyncJob`.

The job:

- table name: `fund_adj`
- spec: `FUND_ADJ_SPEC`
- fetch callable: `fetcher.fetch_fund_adj`
- store: `DailyPartitionStore(etf_dir / "fund_adj")`
- exchange calendar: default `SSE`
- date range support: enabled through `DailySyncJob`

User-facing sync entry points:

- `uv run python main.py sync --table fund_adj`
- `uv run python main.py sync --table fund_adj --start-date 20250101 --end-date 20250618`
- `uv run python main.py sync --etf`
- `uv run python main.py sync --all`

`fund_adj` should be included in `sync --etf`, `sync --all`, and `status` output through the existing CLI table lists.

## Local Query API

Add `zer0share.query.etf.fund_adj`.

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

- `LocalPro.fund_adj(**kwargs)`
- `LocalPro.query("fund_adj", **kwargs)`

`trade_date`, `start_date`, and `end_date` are validated as `YYYYMMDD` by the local API date checker.

## Documentation And Example

Update README:

- Add `fund_adj` to the ETF sync command list.
- Mention the Tushare permission requirement: user points >= 600, with higher frequency above 5000 points.
- Add a local Pro API example:

```python
fund_adj = pro.fund_adj(
    ts_code="513100.SH",
    start_date="20190101",
    end_date="20190926",
    fields="ts_code,trade_date,adj_factor,discount_rate",
)
```

Update `skills/zer0share-data/references/api.md` so the local data skill can discover and query the new table.

## Testing

Add or extend tests:

- `tests/test_fetcher.py`
  - fetcher calls `pro.fund_adj` with `trade_date` and the expected fields
  - fetcher returns documented columns
  - fetcher returns empty columns for `None`
- `tests/test_api.py`
  - `LocalPro.fund_adj` returns local data
  - filters by `ts_code`, `trade_date`, and date range
  - supports `limit`, `offset`, and `fields`
  - `query("fund_adj")` dispatches
  - invalid date formats raise through `_check_dates`
  - missing data raises a message mentioning `fund_adj`
- `tests/test_cli.py`
  - `sync --table fund_adj` calls pipeline
  - `sync --table fund_adj --start-date ... --end-date ...` is accepted
  - `sync --etf` includes `fund_adj`
- `tests/test_pipeline.py`
  - pipeline registry contains `fund_adj`
  - sync writes to `data/etf/fund_adj/date=YYYYMMDD/data.parquet`
  - sync updates the `fund_adj` metastore date

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

There are no open design decisions. The implementation should use daily all-fund `trade_date` sync, include `fund_adj` in ETF batch syncs, and preserve `discount_rate` in the schema.
