# IDX Anns Design

## Goal

Add local synchronization and query support for the Tushare `idx_anns` interface in zer0share.

The feature should mirror index-company announcements locally so research workflows can query announcement metadata from Parquet through the existing Tushare-like local API without spending Tushare points.

## Scope

In scope:

- Pull `idx_anns` from Tushare by announcement date.
- Store results as daily Parquet partitions keyed by `ann_date`.
- Sync all sources by default.
- Page through the full upstream result set for each announcement date.
- Query locally through `pro.idx_anns(...)` and `pro.query("idx_anns", ...)`.
- Document sync and query usage.

Out of scope:

- Downloading or parsing announcement page contents from each `url`.
- Full-text search over announcement bodies.
- Source-specific sync commands.
- Non-Tushare scraping from index company websites.
- Extra local-only query parameters beyond the Tushare-like interface.

## Data Model

Create a new table named `idx_anns`.

Columns follow the Tushare `idx_anns` output:

- `ann_date`: announcement date, `YYYYMMDD`
- `title`: announcement title
- `url`: announcement URL
- `source`: source index company
- `type`: announcement type

Schema constant:

```python
IDX_ANNS_COLS = [
    "ann_date",
    "title",
    "url",
    "source",
    "type",
]
```

Catalog spec:

```python
IDX_ANNS_SPEC = DailyTableSpec(
    name="idx_anns",
    path_parts=("index", "idx_anns"),
    columns=IDX_ANNS_COLS,
    parquet_pattern="date=*/data.parquet",
    sync_table="idx_anns",
    order_by="ann_date DESC, source, title",
    hive_partitioning=True,
    union_by_name=True,
    first_date="20040101",
    date_column="ann_date",
    code_column=None,
)
```

Storage path:

```text
data/index/idx_anns/date=YYYYMMDD/data.parquet
```

`first_date` is `20040101` as a conservative default for historical index-company announcements. Users can override the starting point with `--start-date` for backfills.

## Sync Flow

`idx_anns` must use natural calendar dates, not trading days. Index announcements are date-stamped by `ann_date` and may be published on weekends or holidays.

Add a small `CalendarDateSyncJob` for natural-day partitioned tables. It should be separate from `DailySyncJob` so the existing trading-day job keeps its clear market-data semantics.

The job should:

- Loop over every natural date from `start_date` to `end_date`.
- Use `spec.first_date` when no metadata exists and no `start_date` is provided.
- Use today as the default end date.
- Skip a partition when the local date partition already exists.
- Advance metadata for skipped existing partitions when metadata is missing or behind that date.
- Support CLI date ranges like existing daily jobs.
- Update metadata after each processed date.
- Write empty partitions for empty upstream responses.
- Send success and failure notifications using the existing notifier conventions.

CLI usage:

```bash
uv run python main.py sync --table idx_anns --start-date 20260401 --end-date 20260430
```

Fetcher behavior:

```python
def fetch_idx_anns(self, ann_date: str) -> pd.DataFrame:
    ...
```

The fetcher should not pass `src` during sync. It should request all index-company sources for the date and let the local query API filter by `src`.

Pagination:

- Use `limit=1000`.
- Start with `offset=0`.
- Stop when Tushare returns `None`, an empty DataFrame, or fewer than 1000 rows.
- Concatenate all non-empty pages before returning.
- Return an empty DataFrame with `IDX_ANNS_COLS` when no data is returned.

Upstream call shape:

```python
self._pro.idx_anns(
    ann_date=ann_date,
    fields=",".join(IDX_ANNS_COLS),
    limit=1000,
    offset=offset,
)
```

## Query API

Expose a strict Tushare-like local API:

```python
pro.idx_anns(
    ann_date=None,
    start_date=None,
    end_date=None,
    src=None,
    limit=None,
    offset=None,
    fields=None,
)
```

Behavior:

- `ann_date` filters one announcement date.
- `start_date` and `end_date` filter an announcement date range.
- `ann_date` cannot be combined with `start_date` or `end_date`.
- `src` filters the `source` column by exact equality.
- `fields` may contain only `ann_date,title,url,source,type`.
- `limit` and `offset` apply to the local DuckDB query only.
- No extra local-only parameters are added.

Examples:

```python
from zer0share import pro_api

pro = pro_api()

df = pro.idx_anns(ann_date="20260416")

df = pro.idx_anns(
    src="中证指数",
    start_date="20260401",
    end_date="20260430",
    fields="ann_date,title,type",
)
```

Implementation wiring:

- Add `index.idx_anns(...)` in `zer0share/query/index.py`.
- Use `DailyPartitionRepository` with `IDX_ANNS_SPEC`.
- Add an equality filter from `src` to `source`.
- Add `LocalPro.idx_anns(...)` in `zer0share/api.py`.
- Add `"idx_anns": self.idx_anns` to `LocalPro.query(...)`.
- Extend `_check_dates` so it validates `ann_date`, then validate `ann_date`, `start_date`, and `end_date` for this API.

## CLI and Pipeline Wiring

Add `idx_anns` to the existing index-related sync wiring:

- `zer0share/schema.py`: add `IDX_ANNS_COLS`.
- `zer0share/catalog.py`: add `IDX_ANNS_SPEC`.
- `zer0share/fetcher.py`: add `fetch_idx_anns`.
- `zer0share/sync/_jobs.py`: add `CalendarDateSyncJob`.
- `zer0share/sync/index.py`: add a sync job for `idx_anns`.
- `zer0share/cli.py`: add `idx_anns` to `STOCK_TABLES`.
- `zer0share/api.py`: expose the local method and dispatch entry.

`idx_anns` should be listed under `STOCK_TABLES` because the current project groups index tables such as `index_daily` and `index_weight` there.

## Error Handling

Use existing zer0share behavior where possible:

- Missing local data raises `FileNotFoundError` through the repository layer and mentions `sync --table idx_anns`.
- Invalid date strings raise `ValueError` through `_check_dates`.
- Invalid CLI date strings raise `click.BadParameter`.
- `ann_date` mixed with `start_date` or `end_date` raises `ValueError`.
- Empty Tushare responses produce empty daily partitions and still advance metadata.
- Fetch failures use the existing retry delays from sync jobs; persistent failure should notify and re-raise.

## Tests

Fetcher tests:

- `fetch_idx_anns` calls `pro.idx_anns` with `ann_date`, expected fields, `limit=1000`, and `offset=0`.
- It concatenates multiple pages and increments offset by 1000.
- It stops on a short final page.
- It returns `IDX_ANNS_COLS` in order.
- It returns an empty DataFrame with expected columns when Tushare returns `None` or empty data.

Sync job tests:

- `CalendarDateSyncJob` loops over natural days, including weekends.
- It writes `data/index/idx_anns/date=YYYYMMDD/data.parquet`.
- It writes empty partitions when configured for `idx_anns`.
- It updates metadata after processed dates.
- It skips existing partitions and advances metadata for skipped dates when metadata is behind.
- It does not require `trade_cal` metadata.

Pipeline and CLI tests:

- Pipeline registry includes `idx_anns`.
- `pipeline.run("idx_anns", start_date=..., end_date=...)` invokes the index announcement job.
- `sync --table idx_anns` is accepted.
- `sync --table idx_anns --start-date ... --end-date ...` passes the date range through.
- `sync --stock` includes `idx_anns` in the grouped sync path.

API tests:

- `pro.idx_anns(ann_date=...)` returns matching local rows.
- Date range filtering works on `ann_date`.
- `src` filters by `source`.
- `fields`, `limit`, and `offset` work.
- `pro.query("idx_anns", ...)` dispatches correctly.
- Invalid date strings are rejected.
- Combining `ann_date` with a date range is rejected.
- Missing data raises `FileNotFoundError` mentioning `idx_anns`.

## Documentation

Update `README.md`:

- Add `idx_anns` to the index-related sync command list.
- Document that it uses natural-day announcement dates, not trading days.
- Add a local query example.
- Add the required Tushare points note: `idx_anns` needs 6000 points.

Add a smoke script:

```text
examples/index/idx_anns_query_smoke.py
```

The smoke script should query a representative date or range from already-synced local data and print a compact preview.

## Acceptance Criteria

- `uv run python main.py sync --table idx_anns --start-date 20260401 --end-date 20260430` is accepted.
- Sync loops over every natural day in the requested range.
- Synced data is stored under `data/index/idx_anns/date=YYYYMMDD/data.parquet`.
- Empty announcement dates are recorded as empty partitions and advance metadata.
- `pro.idx_anns(ann_date="20260416")` queries local data.
- `pro.idx_anns(src="中证指数", start_date="20260401", end_date="20260430", fields="ann_date,title,type")` filters locally.
- `pro.query("idx_anns", ann_date="20260416")` dispatches correctly.
- Unit tests cover fetcher pagination, natural-day sync, CLI/pipeline wiring, and local query behavior.
- README and smoke example document the new table.
