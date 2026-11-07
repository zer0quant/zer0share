# ETF SH Cons Design

## Goal

Add the Tushare `etf_sh_cons` interface to zer0share for Shanghai Stock Exchange ETF daily portfolio constituents. The feature should support local daily synchronization, Parquet storage, and Tushare-like local querying through `zer0share.api.LocalPro`.

The scope is intentionally limited to the Shanghai interface `etf_sh_cons`. Shenzhen ETF constituent support is out of scope until a concrete interface and requirements are provided.

## Data Model

Create a new daily partitioned table named `etf_sh_cons`.

Columns:

- `trade_date`: trading date, `YYYYMMDD`
- `ts_code`: ETF code
- `con_code`: constituent security code
- `con_name`: constituent name
- `qty`: share quantity
- `sub_flag`: cash substitution flag
- `cpr`: subscription cash substitution premium ratio, preserved as returned by Tushare because the upstream data may contain `-`
- `rdr`: redemption cash substitution discount ratio, preserved as returned by Tushare because the upstream data may contain `-`
- `sca`: substitution amount in CNY, preserved as returned by Tushare because the upstream data may contain `-`
- `exchange`: constituent exchange code such as `HK`, `SH`, `SZ`, or `OTH`

Storage path:

```text
data/etf/etf_sh_cons/date=YYYYMMDD/data.parquet
```

Catalog shape:

- Add `ETF_SH_CONS_COLS` in `zer0share/schema.py`.
- Add `ETF_SH_CONS_SPEC` as a `DailyTableSpec` in `zer0share/catalog.py`.
- Use `path_parts=("etf", "etf_sh_cons")`.
- Use `parquet_pattern="date=*/data.parquet"`.
- Use `sync_table="etf_sh_cons"`.
- Use `order_by="ts_code, trade_date, con_code"`.
- Enable `hive_partitioning=True` and `union_by_name=True`.
- Use `first_date="20100101"` to match the conservative ETF daily table default already used by `fund_daily`, `fund_adj`, and `etf_share_size`.

## Sync Flow

Add `TushareFetcher.fetch_etf_sh_cons(trade_date: str)`.

The fetcher should page through the Tushare endpoint by `limit` and `offset`, because the interface has a documented 3000-row maximum and a full trading day across all Shanghai ETFs can exceed one page.

For each page, call:

```python
self._pro.etf_sh_cons(
    trade_date=trade_date,
    fields=",".join(ETF_SH_CONS_COLS),
    limit=3000,
    offset=offset,
)
```

Stop when Tushare returns `None`, an empty frame, or fewer than 3000 rows. Concatenate non-empty pages, then return `_select_columns_or_empty(combined, ETF_SH_CONS_COLS)` so missing or empty upstream responses follow the existing project behavior.

Wire the table into ETF sync jobs:

- Import `ETF_SH_CONS_SPEC` in `zer0share/sync/etf.py`.
- Add a `DailySyncJob` with `table_name=ETF_SH_CONS_SPEC.name`.
- Store data with `DailyPartitionStore(etf_dir / "etf_sh_cons")`.

Wire the CLI:

- Add `"etf_sh_cons"` to `ETF_TABLES` in `zer0share/cli.py`.
- This enables `sync --table etf_sh_cons`, `sync --etf`, and `sync --all`.
- Date range support should work through the existing daily job path:

```bash
uv run python main.py sync --table etf_sh_cons --start-date 20260615 --end-date 20260615
```

## Query API

Add `etf_sh_cons` to the local query API.

Public method:

```python
pro.etf_sh_cons(
    ts_code=None,
    trade_date=None,
    con_code=None,
    start_date=None,
    end_date=None,
    limit=None,
    offset=None,
    fields=None,
)
```

Filtering behavior:

- `ts_code` should use the existing `DailyPartitionRepository` code filter behavior.
- `trade_date`, `start_date`, and `end_date` should use existing date partition behavior.
- `con_code` should add an equality filter on the `con_code` column.
- `fields`, `limit`, and `offset` should behave like other local ETF APIs.

Implementation wiring:

- Import `ETF_SH_CONS_SPEC` in `zer0share/query/etf.py`.
- Add `etf_sh_cons(...)` that delegates to `DailyPartitionRepository`.
- Add `LocalPro.etf_sh_cons(...)` in `zer0share/api.py`, with `_check_dates(kwargs)`.
- Add `"etf_sh_cons": self.etf_sh_cons` to `LocalPro.query(...)`.

## Error Handling

Use existing error behavior:

- Missing local data should raise `FileNotFoundError` through `DailyPartitionRepository`, with a message that points to `python main.py sync --table etf_sh_cons`.
- Invalid dates should raise `ValueError` through `_check_dates` in the local API and `click.BadParameter` in the CLI.
- Empty Tushare responses should produce an empty DataFrame with the expected columns.
- Paginated Tushare responses should be concatenated before the sync job writes the daily partition.

No new retry or partial-write behavior is required.

## Tests

Add focused coverage alongside the existing ETF tests.

Fetcher tests:

- `fetch_etf_sh_cons` calls `pro.etf_sh_cons` with `trade_date`, expected fields, `limit=3000`, and incrementing `offset` values.
- It concatenates multiple pages and stops after a short final page.
- It returns columns in `ETF_SH_CONS_COLS` order.
- It returns an empty DataFrame with expected columns when Tushare returns `None` or empty data.

Pipeline tests:

- `pipeline.run("etf_sh_cons")` writes to `data/etf/etf_sh_cons/date=YYYYMMDD/data.parquet`.
- Metadata records the last synced date.
- The fetcher is called for the expected trading day.
- The pipeline registry includes `etf_sh_cons`.

CLI tests:

- `sync --table etf_sh_cons` calls `pipeline.run("etf_sh_cons", start_date=None, end_date=None)`.
- `sync --table etf_sh_cons --start-date ... --end-date ...` passes the date range.
- `sync --etf` includes `etf_sh_cons` in ETF table order.

API tests:

- `pro.etf_sh_cons(fields=...)` returns selected local columns.
- It filters by `ts_code`, `trade_date`, date range, and `con_code`.
- It supports `limit`, `offset`, and `query("etf_sh_cons", ...)`.
- Invalid date formats are rejected.
- Missing data raises `FileNotFoundError` mentioning `etf_sh_cons`.

## Documentation

Update `README.md`:

- Add the sync command to the ETF sync section.
- Add a local API example.
- Add `etf_sh_cons` to the API table.
- Add the storage path to the directory layout.
- Add the command to the sync command reference.

Add a smoke script:

```text
examples/etf/etf_sh_cons_query_smoke.py
```

The smoke script should mirror existing ETF smoke tests and verify representative local queries against already-synced Parquet data.

## Acceptance Criteria

- `uv run python main.py sync --table etf_sh_cons --start-date 20260615 --end-date 20260615` is accepted by the CLI.
- Synced data is stored under `data/etf/etf_sh_cons/date=YYYYMMDD/data.parquet`.
- `pro.etf_sh_cons(trade_date="20260615", ts_code="517030.SH")` returns local constituent rows when data exists.
- `pro.query("etf_sh_cons", con_code="000001.SZ")` dispatches correctly.
- Unit tests cover fetcher, sync, CLI, and local API behavior.
- `examples/etf/etf_sh_cons_query_smoke.py` exercises representative local query parameters against synced data.
