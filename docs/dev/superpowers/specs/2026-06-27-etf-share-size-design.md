# ETF Share Size Interface Design

## Goal

Add Tushare `etf_share_size` to `zer0share` so ETF daily share and scale data can be synced by trading day, stored locally, queried through the local Pro API, and included in ETF batch syncs.

## Scope

Included:

- Fetch `pro.etf_share_size` by `trade_date` for the supported沪深 ETF exchanges.
- Store one Parquet partition per trading day under the ETF topic directory.
- Register `etf_share_size` in schema/catalog, sync jobs, CLI table choices, local API dispatch, README, and local API references.
- Support local query filters for `ts_code`, `exchange`, `trade_date`, `start_date`, and `end_date`.
- Support `fields`, `limit`, and `offset`.
- Add focused tests for fetcher, sync pipeline, CLI, local API, and query dispatch.
- Add an ETF smoke example for local querying.

Excluded:

- Per-ETF historical sync loops using `ts_code + start_date/end_date`.
- A maintenance script for repairing a single ETF history.
- Derived ETF fund-flow metrics or factor calculations.
- Online smoke execution in automated tests. Tests should use mocked Tushare clients and local Parquet fixtures.

## Data Model

Add `ETF_SHARE_SIZE_COLS`:

- `trade_date`
- `ts_code`
- `etf_name`
- `total_share`
- `total_size`
- `nav`
- `close`
- `exchange`

These columns follow the documented Tushare return fields. `nav` and `close` are stored even though they are not default-displayed by Tushare, because local storage should preserve the complete documented interface and let callers choose fields at query time.

Add `ETF_SHARE_SIZE_SPEC`:

- `name`: `etf_share_size`
- `path_parts`: `("etf", "etf_share_size")`
- `parquet_pattern`: `date=*/data.parquet`
- `sync_table`: `etf_share_size`
- `order_by`: `ts_code, trade_date`
- `hive_partitioning`: `True`
- `union_by_name`: `True`
- `first_date`: `20100101`

Dates stay as Tushare-style `YYYYMMDD` strings.

## Storage

Persist ETF share-size snapshots as a daily partitioned table:

```text
data/
+-- etf/
    +-- etf_share_size/
        +-- date=20250102/
        |   +-- data.parquet
        +-- date=20250103/
            +-- data.parquet
```

Each date partition contains the rows returned for both `SSE` and `SZSE`. This storage shape matches `fund_daily` and `fund_adj`, keeping date-range sync and local DuckDB queries consistent.

## Fetching

Add `TushareFetcher.fetch_etf_share_size(trade_date: str)`.

The method calls Tushare once per supported exchange:

```python
self._pro.etf_share_size(
    trade_date=trade_date,
    exchange="SSE",
    fields=",".join(ETF_SHARE_SIZE_COLS),
)
self._pro.etf_share_size(
    trade_date=trade_date,
    exchange="SZSE",
    fields=",".join(ETF_SHARE_SIZE_COLS),
)
```

Non-empty responses are concatenated with `ignore_index=True`, then passed through the existing column selection helper so the result always uses `ETF_SHARE_SIZE_COLS`.

If both upstream responses are `None` or empty, return an empty DataFrame with `ETF_SHARE_SIZE_COLS`. This matches existing daily-table behavior and allows the sync job to write an empty but schema-correct partition for valid trading days.

The sync job fetches only `SSE` and `SZSE` because those are the documented input values. The output schema allows `BSE`, so local queries can still filter historical or manually loaded `BSE` rows if they exist.

## Sync Flow

Extend `zer0share/sync/etf.py` with a `DailySyncJob`.

The job:

- table name: `etf_share_size`
- spec: `ETF_SHARE_SIZE_SPEC`
- fetch callable: `fetcher.fetch_etf_share_size`
- store: `DailyPartitionStore(etf_dir / "etf_share_size")`
- exchange calendar: default `SSE`
- date range support: enabled through `DailySyncJob`

User-facing sync entry points:

- `uv run python main.py sync --table etf_share_size`
- `uv run python main.py sync --table etf_share_size --start-date 20250101 --end-date 20250618`
- `uv run python main.py sync --etf`
- `uv run python main.py sync --all`

`etf_share_size` should be included in `sync --etf`, `sync --all`, and `status` output through the existing CLI table lists.

Scheduler behavior remains config-driven. The implementation may document an example `[scheduler] etf_share_size = "08:45"` entry, but it should not require a hard-coded scheduler change.

## Local Query API

Add `zer0share.query.etf.etf_share_size`.

Parameters:

- `ts_code=None`
- `trade_date=None`
- `start_date=None`
- `end_date=None`
- `exchange=None`
- `limit=None`
- `offset=None`
- `fields=None`

Filtering:

- `ts_code` supports comma-separated multi-value filtering through `DailyPartitionRepository`.
- `trade_date` selects one partition date and cannot be combined with `start_date` or `end_date`.
- `start_date` and `end_date` filter partition dates inclusively.
- `exchange` applies an exact-value SQL filter on the stored `exchange` column.
- `fields`, `limit`, and `offset` reuse existing repository behavior.

Expose it through:

- `LocalPro.etf_share_size(**kwargs)`
- `LocalPro.query("etf_share_size", **kwargs)`

`trade_date`, `start_date`, and `end_date` are validated as `YYYYMMDD` by the local API date checker.

## Documentation And Example

Update README:

- Add `etf_share_size` to the ETF sync command list.
- Mention the Tushare permission requirement: user points >= 8000.
- Mention the source update timing: exchange data is generally available for the prior trading day around 08:30 the next day, while overseas-related ETFs can update later.
- Add a local Pro API example:

```python
etf_share_size = pro.etf_share_size(
    ts_code="510330.SH",
    start_date="20250101",
    end_date="20251224",
    fields="trade_date,ts_code,etf_name,total_share,total_size,exchange",
)
```

Add `etf_share_size` to:

- the local API table list
- the storage tree under `etf/`
- the CLI summary table
- `skills/zer0share-data/references/api.md`, so the local data skill can discover and query the new table

Add `examples/etf/etf_share_size_query_smoke.py`.

The smoke example should cover:

- lookup by `ts_code` and date range
- lookup by `trade_date` and `exchange`
- selected fields
- `offset` and `limit`
- `pro.query("etf_share_size", ...)`

## Testing

Add or extend tests:

- `tests/test_fetcher.py`
  - fetcher calls `pro.etf_share_size` for `SSE` and `SZSE`
  - each call includes `trade_date` and the expected fields
  - fetcher combines non-empty exchange responses
  - fetcher returns documented columns
  - fetcher returns empty columns when both exchange responses are `None` or empty
- `tests/test_api.py`
  - `LocalPro.etf_share_size` returns local data
  - filters by `ts_code`, `trade_date`, date range, and `exchange`
  - supports `limit`, `offset`, and `fields`
  - `query("etf_share_size")` dispatches
  - invalid date formats raise through `_check_dates`
  - missing data raises a message mentioning `etf_share_size`
- `tests/test_cli.py`
  - `sync --table etf_share_size` calls pipeline
  - `sync --table etf_share_size --start-date ... --end-date ...` is accepted
  - `sync --etf` includes `etf_share_size`
- `tests/test_pipeline.py`
  - pipeline registry contains `etf_share_size`
  - sync writes to `data/etf/etf_share_size/date=YYYYMMDD/data.parquet`
  - sync updates the `etf_share_size` metastore date
  - sync fetches only trading days through the existing daily job behavior

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

There are no open design decisions. The implementation should use daily all-market `trade_date` sync for `SSE` and `SZSE`, store one date partition containing both exchanges, and expose `exchange` as a local query filter.
