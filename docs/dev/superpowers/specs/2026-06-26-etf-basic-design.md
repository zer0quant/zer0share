# ETF Basic Design

## Goal

Add the first ETF topic table to zer0share: Tushare `etf_basic`.

The first slice should establish the ETF data domain in the existing public
Tushare-style data pipeline.

## Scope

Implement only the ETF basic information interface in this slice.

Tushare interface:

- API name: `etf_basic`
- Description: domestic ETF basic information, including QDII ETFs
- Request limit: up to 5000 rows per request
- Required permission: Tushare user points >= 8000

Input parameters supported by the fetcher:

- `ts_code`
- `index_code`
- `list_date`
- `list_status`
- `exchange`
- `mgr`

Local query parameters supported by `pro.etf_basic(...)`:

- `ts_code`
- `index_code`
- `list_date`
- `list_status`
- `exchange`
- `mgr`
- `mgr_name`
- `limit`
- `offset`
- `fields`

`mgr` is kept for compatibility with the Tushare request parameter. In local
queries it filters the stored `mgr_name` column. `mgr_name` is also supported so
callers can filter by the local column name directly.

## Output Schema

Persist these columns, in this order:

- `ts_code`
- `csname`
- `extname`
- `cname`
- `index_code`
- `index_name`
- `setup_date`
- `list_date`
- `list_status`
- `exchange`
- `mgr_name`
- `custod_name`
- `mgt_fee`
- `etf_type`

Dates stay as Tushare-style `YYYYMMDD` strings, matching the rest of the local
query API.

## Storage

ETF data gets its own top-level topic directory:

```text
data/
└── etf/
    └── etf_basic/
        └── data.parquet
```

`etf_basic` is a snapshot table, not a daily partitioned table. A sync rewrites
the snapshot with the latest complete response from Tushare.

## Sync Flow

Add `fetch_etf_basic(...)` to the Tushare fetcher. It calls:

```python
pro.etf_basic(fields=",".join(ETF_BASIC_COLS), ...)
```

The sync job writes `data/etf/etf_basic/data.parquet` through the existing
`SnapshotStore` and updates the metastore key `etf_basic` to the current date.

User-facing sync entry points:

- `uv run python main.py sync --table etf_basic`
- `uv run python main.py sync --etf`
- `uv run python main.py sync --all` includes ETF after the existing Tushare
  topics

## Query Flow

Add a new `zer0share.query.etf` module with:

```python
def etf_basic(ctx, ts_code=None, index_code=None, list_date=None,
              list_status=None, exchange=None, mgr=None, mgr_name=None,
              limit=None, offset=None, fields=None) -> pd.DataFrame:
    ...
```

The implementation uses `BaseParquetRepository` and `ETF_BASIC_SPEC`.

Filtering rules:

- `ts_code` supports comma-separated multi-value filtering, like other code
  columns in the project.
- `index_code`, `list_date`, `list_status`, and `exchange` use equality filters.
- `mgr` and `mgr_name` both filter the stored `mgr_name` field.
- `limit`, `offset`, and `fields` reuse the existing repository behavior.

Expose the query through:

- `pro.etf_basic(...)`
- `pro.query("etf_basic", ...)`

## Code Changes

- Add `ETF_BASIC_COLS` to `zer0share/schema.py`.
- Add `ETF_BASIC_SPEC` to `zer0share/catalog.py`.
- Add `fetch_etf_basic(...)` to `zer0share/sources/tushare.py`.
- Add `zer0share/sync/etf.py` with an `EtfBasicSyncJob`.
- Register ETF sync jobs in `zer0share/pipeline.py`.
- Add `ETF_TABLES = ["etf_basic"]`, `--etf`, and `etf_basic` to CLI table
  choices/status output.
- Add `zer0share/query/etf.py`.
- Add `LocalPro.etf_basic(...)` and query dispatch support in
  `zer0share/api.py`.
- Update README and `skills/zer0share-data/references/api.md`.

## Testing

Add focused tests for:

- fetcher calls `pro.etf_basic` with the expected fields and parameters;
- sync writes `data/etf/etf_basic/data.parquet` and updates the `etf_basic`
  metastore date;
- `pro.etf_basic(...)` filters by `ts_code`, `index_code`, `list_status`,
  `exchange`, `mgr`, `mgr_name`, `limit`, `offset`, and `fields`;
- `pro.query("etf_basic", ...)` dispatches correctly;
- CLI accepts `--table etf_basic` and `--etf`.

Run focused tests first:

```bash
PYTHONDONTWRITEBYTECODE=1 uv run pytest -p no:cacheprovider tests/test_fetcher.py tests/test_pipeline.py tests/test_api.py tests/test_cli.py -q
```

If practical, run the full suite before publishing the ETF feature branch.

## Non-Goals

This slice does not implement ETF daily行情, ETF复权因子, ETF成分/权重, 申赎清单,
fund NAV, or fund portfolio interfaces. Those should be added as later ETF-topic
slices after this table establishes the ETF module shape.
