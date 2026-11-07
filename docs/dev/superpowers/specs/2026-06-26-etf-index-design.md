# ETF Index Interface Design

## Goal

Add the Tushare `etf_index` interface to `zer0share` so ETF benchmark index metadata can be synced once, stored locally, queried through the local Pro API, and documented with a smoke example.

## Scope

This is a non-daily snapshot table, matching the shape of the existing `etf_basic` integration.

Included:

- Fetch `pro.etf_index` with documented fields.
- Store the snapshot at `data/etf/etf_index/data.parquet`.
- Register `etf_index` in schema/catalog, sync jobs, CLI table choices, local API dispatch, README, and examples.
- Support local query filters for `ts_code`, `pub_date`, and `base_date`.
- Support `fields`, `limit`, and `offset`.
- Add focused tests for fetcher, sync pipeline, CLI, local API, and query dispatch.

Excluded:

- Date-partitioned storage. The upstream interface is a small reference list and currently returns fewer than 2000 rows.
- Incremental merge logic. A full snapshot overwrite is sufficient and matches `etf_basic`.
- Online smoke execution in tests. Tests use mocked Tushare clients and local Parquet fixtures.

## Data Model

Add `ETF_INDEX_COLS`:

- `ts_code`
- `indx_name`
- `indx_csname`
- `pub_party_name`
- `pub_date`
- `base_date`
- `bp`
- `adj_circle`

Add `ETF_INDEX_SPEC`:

- `name`: `etf_index`
- `path_parts`: `("etf", "etf_index")`
- `parquet_pattern`: `data.parquet`
- `sync_table`: `etf_index`
- `order_by`: `ts_code`

## Fetching

Add `TushareFetcher.fetch_etf_index(ts_code=None, pub_date=None, base_date=None)`.

The method calls:

```python
self._pro.etf_index(
    ts_code=ts_code,
    pub_date=pub_date,
    base_date=base_date,
    fields=",".join(ETF_INDEX_COLS),
)
```

Return handling uses the existing `_select_columns_or_empty` helper so `None` and empty upstream responses return an empty DataFrame with the documented columns.

## Sync Flow

Extend `zer0share/sync/etf.py` with a second `SnapshotSyncJob`.

The job:

- table name: `etf_index`
- fetch callable: `fetcher.fetch_etf_index`
- store: `SnapshotStore(etf_dir / "etf_index" / "data.parquet")`
- `skip_non_trading`: `False`

The CLI includes `etf_index` in `ETF_TABLES`, so `--table etf_index`, `--etf`, `--all`, and `status` include it automatically.

## Local Query API

Add `zer0share.query.etf.etf_index`.

Parameters:

- `ts_code=None`
- `pub_date=None`
- `base_date=None`
- `limit=None`
- `offset=None`
- `fields=None`

Filtering:

- `ts_code` uses `in_filter` to support comma-separated or list inputs, matching common code filters elsewhere.
- `pub_date` and `base_date` use `eq_filter`.

Expose it through:

- `LocalPro.etf_index(**kwargs)`
- `LocalPro.query("etf_index", **kwargs)`

`pub_date` and `base_date` are validated as `YYYYMMDD` by the local API date checker.

## Documentation And Example

Update README:

- Sync command list.
- Local Pro API example or method table.
- Data directory tree.
- CLI sync table summary.

Add `examples/etf/etf_index_query_smoke.py`.

The smoke example should mirror `examples/etf/etf_basic_query_smoke.py` but use ETF index fields and filters:

- sample row lookup
- `ts_code`
- `pub_date`
- `base_date`
- `limit`
- `offset`
- `query_dispatch`

Update `examples/README.md` to list the new smoke script.

## Testing

Add or extend tests:

- `tests/test_fetcher.py`
  - fetcher returns documented columns
  - fetcher calls `pro.etf_index` with filters and fields
  - fetcher returns empty columns for `None`
- `tests/test_api.py`
  - `LocalPro.etf_index` returns data
  - filters by `ts_code`, `pub_date`, and `base_date`
  - supports `limit`, `offset`, and `fields`
  - `query("etf_index")` dispatches
  - invalid date format for `pub_date` or `base_date` raises
  - missing data raises a message mentioning `etf_index`
- `tests/test_cli.py`
  - `sync --table etf_index` calls pipeline
  - `sync --table etf_index --start-date ...` is rejected
  - `sync --etf` includes ETF tables through the existing table list behavior
- `tests/test_pipeline.py`
  - sync writes to `data/etf/etf_index/data.parquet`
  - sync runs on non-trading days

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

There are no open design decisions. The interface should follow the existing `etf_basic` integration pattern and stay within the ETF module.
