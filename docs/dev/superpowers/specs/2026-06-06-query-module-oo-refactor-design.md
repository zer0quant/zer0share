# Query Module OO Refactor Design

## Goal

Refactor `zer0share.query` from repeated function-local SQL construction into a small object-oriented query layer while preserving the public Tushare-like API exposed by `LocalPro`.

The refactor should make new local query APIs easier to add, reduce duplicated DuckDB SQL assembly, and centralize field/date/filter validation. It may break internal helper compatibility, especially `query_daily_partitioned()`, but should preserve the user-facing `LocalPro` API names, default parameters, result ordering, and tested error behavior.

## Current Context

The query module currently has one function-level abstraction: `query_daily_partitioned()` in `zer0share/query/_helpers.py`. Daily partitioned tables use it across equities, futures, and options, but its signature now exposes too many repository-level details.

The remaining query functions still duplicate the same pattern:

- validate selected fields
- validate optional dates
- build `WHERE` clauses and bound parameters
- check that a Parquet path exists
- call `duckdb.connect().execute(...).fetchdf()`
- append `ORDER BY`, `LIMIT`, and `OFFSET`

The duplication is visible in static tables such as `stock_basic`, `opt_basic`, industry member queries, `trade_cal`, `fut_basic`, `index_weight`, and `universe`.

There is also a current worktree syntax issue in `zer0share/query/_helpers.py` where the Parquet pattern line contains a stray `I`. Removing `query_daily_partitioned()` fixes that issue by deleting the broken code path.

## Design Principles

Use objects for stable responsibilities, not for their own sake.

- `TableSpec` describes a table.
- `DailyTableSpec` describes daily partition semantics.
- `ParquetQueryEngine` executes safe parameterized DuckDB queries.
- Repository classes adapt table specs into reusable query patterns.
- Domain modules keep explicit public functions so the API remains readable.
- `pro_bar` remains a business service because it performs adjustment calculations, not just table reads.

Avoid a single universal dynamic query dispatcher that hides every API behind metadata. That would reduce lines but make behavior, validation, and error messages harder to reason about.

Do not preserve `query_daily_partitioned()` as an internal compatibility wrapper. Domain query modules should depend on repositories directly.

## Core Types

### `TableSpec`

`TableSpec` is an immutable description of a local Parquet table.

Fields:

- `name`: logical table name for debugging and error messages.
- `path_parts`: path components relative to `ctx.data_dir`, such as `("futures", "fut_daily")`.
- `columns`: allowed query columns, usually from `zer0share.schema`.
- `parquet_pattern`: file pattern below the table directory, such as `data.parquet` or `date=*/data.parquet`.
- `sync_table`: sync command name to show when data is missing.
- `order_by`: default stable result ordering.
- `hive_partitioning`: whether DuckDB should infer partition columns from Hive-style paths.
- `union_by_name`: whether DuckDB should align columns by name across Parquet files.

Example:

```python
DAILY_SPEC = TableSpec(
    name="daily_kline",
    path_parts=("daily_kline",),
    columns=DAILY_COLS,
    parquet_pattern="date=*/data.parquet",
    sync_table="daily_kline",
    order_by="ts_code, trade_date",
    hive_partitioning=True,
    union_by_name=True,
)
```

### `SqlFilter`

`SqlFilter` represents one safe SQL predicate and its bound parameters.

It should be simple, for example:

```python
@dataclass(frozen=True)
class SqlFilter:
    clause: str
    params: tuple[object, ...]
```

Helpers should create filters for equality, `IN`, and date ranges. Column names must come from trusted code or be checked against `TableSpec.columns`; user values must always be bound as parameters.

### `DailyTableSpec`

`DailyTableSpec` extends `TableSpec` with date-specific semantics:

- `date_column`: the column used for `trade_date`, `start_date`, and `end_date` filters. Defaults to `trade_date`.
- `code_column`: the optional code column used by the common `ts_code`-style filter. Defaults to `ts_code`; can be `None` for tables such as `fut_holding`.

This removes the old need to pass `None` positionally into `query_daily_partitioned()` or to override the base data directory for futures and options.

### `ParquetQueryEngine`

`ParquetQueryEngine` owns SQL generation and DuckDB execution.

Responsibilities:

- build `SELECT ... FROM read_parquet(...)`
- bind all user values through DuckDB parameters
- validate trusted `ORDER BY` strings
- append `LIMIT` and `OFFSET`
- return a Pandas DataFrame

It should not know about Tushare APIs, sync commands, or business logic.

### `BaseParquetRepository`

`BaseParquetRepository` combines `QueryContext`, `TableSpec`, and `ParquetQueryEngine`.

Responsibilities:

- resolve table directory and Parquet pattern
- check data existence
- validate requested fields
- pass filters and pagination to the engine

It should expose a reusable `query()` method for non-date-partition-specific tables.

### `DailyPartitionRepository`

`DailyPartitionRepository` extends the base repository for tables keyed by a date column and stored under `date=*`.

Responsibilities:

- reject ambiguous `trade_date` plus date range input
- parse and validate `start_date` and `end_date`
- add code, exact date, start date, and end date filters according to `DailyTableSpec`
- accept extra `SqlFilter` objects such as `exchange`, `symbol`, or `prd`

This class replaces direct usage of `query_daily_partitioned()`.

### Domain Query Services

Domain modules should remain explicit:

- `EquityQueries`
- `FuturesQueries`
- `OptionsQueries`
- `IndustryQueries`
- `CalendarQueries`

These classes hold repository instances and expose methods matching existing functions. The current module-level functions may be removed as internal API if `LocalPro` instantiates the services directly. The important constraint is that `LocalPro.daily(...)`, `LocalPro.opt_basic(...)`, and `LocalPro.query("daily", ...)` keep working.

### `ProBarService`

`pro_bar` should be separate from repositories because it joins daily bars with adjustment factors and recalculates prices.

It should depend on the daily and adjustment-factor query methods rather than constructing SQL itself.

## Data Flow

For a simple table query:

1. Public API method receives user parameters.
2. Domain query method converts parameters into `SqlFilter` objects.
3. Repository validates fields and resolves the Parquet source.
4. Engine builds and executes parameterized SQL.
5. DataFrame is returned unchanged, except for existing date formatting behavior where already present.

For a daily partitioned query:

1. Public API method receives `ts_code`, `trade_date`, `start_date`, `end_date`, `fields`, `limit`, and `offset`.
2. `DailyPartitionRepository` validates date combinations and builds date filters using `DailyTableSpec`.
3. Extra domain filters are passed as `SqlFilter` objects.
4. Engine executes the query using Hive partitioning and `union_by_name` according to `TableSpec`.

## Error Handling

Keep current error classes:

- `FileNotFoundError` when the expected data path is missing.
- `ValueError` for unknown fields, invalid dates, invalid date ranges, invalid filter columns, and invalid order clauses.
- `NotImplementedError` for unsupported `pro_bar` options.

Error text should remain close to current messages to avoid surprising users and tests.

## Testing

Refactor tests should focus on behavior preservation:

- existing `tests/test_api.py` should pass
- unknown field validation still rejects invalid fields
- invalid date formats and reversed date ranges still raise `ValueError`
- daily partitioned queries still support `trade_date`, date ranges, multi-code code filters where previously supported, extra filters, `limit`, and `offset`
- static table queries such as `stock_basic`, `opt_basic`, industry queries, and `fut_basic` still return the same rows and ordering
- `LocalPro.query(api_name, **kwargs)` dispatch remains unchanged
- `pro_bar` adjustment behavior remains unchanged

Add focused unit tests for new helper classes only where existing API tests do not cover important edge cases, especially `ORDER BY` validation and filter construction.

## Migration Plan

Implement in small steps:

1. Add `TableSpec`, `DailyTableSpec`, `SqlFilter`, `ParquetQueryEngine`, `BaseParquetRepository`, and `DailyPartitionRepository`.
2. Remove `query_daily_partitioned()` and update all query modules to use repositories directly.
3. Migrate simple static queries to `BaseParquetRepository`.
4. Migrate daily partitioned queries to `DailyPartitionRepository`.
5. Migrate `index_weight`, `universe`, and `trade_cal` through the same repository primitives.
6. Keep `pro_bar` behavior unchanged, with only dependency wiring adjusted if needed.
7. Run targeted API tests after each migration step and the full test suite at the end.

## Non-Goals

This refactor will not:

- change the public API names or expected Tushare-like signatures
- add new data tables
- add new `pro_bar` features such as `ma`, non-daily frequency, or non-equity assets
- replace `LocalPro.query()` with a dynamic metadata-only dispatcher
- change the storage layout or sync behavior
- remove user or unrelated worktree changes
- preserve internal helper compatibility for `query_daily_partitioned()`

## Compatibility Choice

Internal compatibility is not required. The implementation should delete `query_daily_partitioned()` and remove `data_dir_override`/`extra_filters: dict` as concepts.

User-facing API compatibility is still required. `LocalPro` methods and `LocalPro.query(api_name, **kwargs)` should continue working.
