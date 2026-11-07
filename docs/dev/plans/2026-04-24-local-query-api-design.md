# Local Query API Design

## Context

`zer0share` already syncs these Tushare datasets into local Parquet files:

- `stock_basic` -> `data/basic/data.parquet`
- `trade_cal` -> `data/trade_cal/exchange=*/data.parquet`
- `daily` -> `data/daily_kline/date=*/data.parquet`
- `adj_factor` -> `data/adj_factor/date=*/data.parquet`

The next step is to make the synced data convenient for factor research. The
primary interface should be a Python API that feels close to Tushare Pro, not a
CLI query command. Researchers should be able to import a local `pro_api()`,
call familiar methods, and receive pandas DataFrames without consuming Tushare
quota.

## Goals

- Provide a local Python API similar to Tushare Pro.
- Query only local Parquet data through DuckDB.
- Return pandas DataFrames.
- Keep public parameter names aligned with Tushare where practical.
- Return date columns as `YYYYMMDD` strings, matching Tushare behavior.
- Support field projection to reduce memory use in notebooks and factor jobs.
- Return empty DataFrames for valid queries with no matching rows.

## Non-Goals

- Do not call Tushare during queries.
- Do not add a CLI query workflow as the main research interface.
- Do not calculate adjusted prices in the first version.
- Do not build a general SQL console.
- Do not add new synced datasets.

## Public API

Add `src/api.py` with a `LocalPro` class and expose `pro_api()` from
`src/__init__.py`.

Example usage:

```python
from zer0share import pro_api

pro = pro_api()

basic = pro.stock_basic(list_status="L")
cal = pro.trade_cal(exchange="SSE", start_date="20240101", end_date="20241231")
daily = pro.daily(
    ts_code="000001.SZ",
    start_date="20240101",
    end_date="20240331",
    fields="ts_code,trade_date,close,vol",
)
adj = pro.adj_factor(ts_code="000001.SZ", start_date="20240101", end_date="20240331")
```

### Constructor

```python
def pro_api(config_path: str | Path = "config/settings.toml") -> LocalPro:
    ...
```

`pro_api()` loads the existing project config and uses `paths.data_dir` as the
Parquet root. It should not require a Tushare token because local queries do
not access the network.

### Methods

```python
class LocalPro:
    def stock_basic(
        self,
        ts_code: str | None = None,
        name: str | None = None,
        market: str | None = None,
        list_status: str | None = "L",
        exchange: str | None = None,
        is_hs: str | None = None,
        fields: str | list[str] | None = None,
    ) -> pd.DataFrame: ...

    def trade_cal(
        self,
        exchange: str = "SSE",
        start_date: str | None = None,
        end_date: str | None = None,
        is_open: str | int | bool | None = None,
        fields: str | list[str] | None = None,
    ) -> pd.DataFrame: ...

    def daily(
        self,
        ts_code: str | None = None,
        trade_date: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        fields: str | list[str] | None = None,
    ) -> pd.DataFrame: ...

    def adj_factor(
        self,
        ts_code: str | None = None,
        trade_date: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        fields: str | list[str] | None = None,
    ) -> pd.DataFrame: ...

    def query(self, api_name: str, **kwargs) -> pd.DataFrame: ...
```

`query()` dispatches these Tushare-style names:

- `stock_basic`
- `trade_cal`
- `daily`
- `adj_factor`

## Parameter Behavior

Date inputs accept both `YYYYMMDD` and `YYYY-MM-DD`. Public examples should use
`YYYYMMDD` because this is the Tushare convention.

`daily()` and `adj_factor()` support:

- `trade_date="20240102"` for one full-market trading day.
- `ts_code="000001.SZ"` with `start_date` and `end_date` for one security.
- `ts_code="000001.SZ,600000.SH"` for multiple securities.

If `trade_date` is supplied together with `start_date` or `end_date`, raise
`ValueError` because the date intent is ambiguous.

`fields` accepts either a comma-separated string or a list of field names. The
API validates requested fields against the known schema for each dataset before
executing SQL. Unknown fields raise `ValueError`.

## Output Behavior

All methods return pandas DataFrames with stable column order.

Date columns are returned as strings:

- `stock_basic`: `list_date`, `delist_date`
- `trade_cal`: `cal_date`, `pretrade_date`
- `daily`: `trade_date`
- `adj_factor`: `trade_date`

The format is always `YYYYMMDD`. Missing dates should remain null-like values
instead of being converted to an empty string. This keeps pandas missing-value
handling usable while preserving Tushare-style date formatting for present
values.

## Query Implementation

Use DuckDB directly against Parquet files instead of reading full files into
pandas before filtering.

Suggested file patterns:

```sql
read_parquet('data/basic/data.parquet')
read_parquet('data/trade_cal/exchange=*/data.parquet', hive_partitioning=true)
read_parquet('data/daily_kline/date=*/data.parquet', hive_partitioning=true)
read_parquet('data/adj_factor/date=*/data.parquet', hive_partitioning=true)
```

The implementation should:

- Build SQL from a fixed table descriptor for each dataset.
- Only interpolate trusted column names and file paths controlled by the code.
- Bind user values through DuckDB parameters.
- Push filters into DuckDB for `ts_code`, date ranges, exchange, status, and
  `is_open`.
- Apply field projection in SQL.
- Sort results in a predictable order.

Default ordering:

- `stock_basic`: `ts_code`
- `trade_cal`: `exchange`, `cal_date`
- `daily`: `ts_code`, `trade_date`
- `adj_factor`: `ts_code`, `trade_date`

## Error Handling

Use ordinary Python exceptions with clear messages:

- Missing dataset file or directory: `FileNotFoundError` with a hint to run the
  matching `sync` command.
- Invalid date format: `ValueError`.
- Invalid date range: `ValueError`.
- Unknown field: `ValueError`.
- Unknown `query()` API name: `ValueError`.

Valid queries with no matching rows return an empty DataFrame with the requested
columns.

## Module Shape

Keep the public API small and put reusable query mechanics in a separate module
only if it keeps `src/api.py` readable.

Expected files:

- `src/api.py`: public `LocalPro` and `pro_api()`.
- `src/__init__.py`: exports `pro_api`.
- `tests/test_api.py`: API behavior tests.

If the DuckDB query builder grows beyond simple helper functions, introduce
`src/query.py` for internal table descriptors and SQL assembly.

## Testing Strategy

Add tests with temporary Parquet datasets so they do not depend on real synced
data.

Cover:

- `stock_basic()` filters by `ts_code`, `name`, `market`, `list_status`,
  `exchange`, and `is_hs`.
- `trade_cal()` filters by exchange, date range, and `is_open`.
- `daily()` filters by single `ts_code`, multiple `ts_code`, `trade_date`, and
  `start_date`/`end_date`.
- `adj_factor()` mirrors `daily()` query behavior.
- `fields` accepts strings and lists.
- Output date columns are `YYYYMMDD` strings.
- Missing data raises `FileNotFoundError` with the expected sync hint.
- `query("daily", ...)` dispatches correctly.
- Unknown fields and unknown API names raise `ValueError`.

## Implementation Notes

Prefer a table descriptor structure to avoid duplicated query code:

```python
TableSpec(
    name="daily",
    path=data_dir / "daily_kline" / "date=*" / "data.parquet",
    columns=DAILY_COLS,
    date_columns=["trade_date"],
    order_by=["ts_code", "trade_date"],
)
```

This keeps the first version focused while leaving room to add future datasets
with minimal boilerplate.
