# zer0share Data Quality Check Design

## Goal

Add a simple data quality check capability for zer0share. The first version checks local Parquet data only. It does not call Tushare, does not require network access, and does not consume Tushare credits.

The feature supports two usage scenarios:

- Historical full check: scan a user-specified date range after a backfill.
- Daily check: run after scheduled syncs and report problems without interrupting the scheduler.

## Scope

The first version covers only market data and adjustment factor tables:

- Stock: `daily_kline`, `adj_factor`
- Index: `index_daily`
- ETF: `fund_daily`, `fund_adj`
- Futures: `fut_daily`
- Options: `opt_daily`

Other tables such as basics, limits, holdings, industry data, ETF constituents, and universe outputs are intentionally out of scope for the first version.

## Architecture

Add a standalone `zer0share/quality/` module. Quality checks should not be scattered across scripts, and they should not be embedded into individual sync jobs.

Core concepts:

- `QualityTarget`: describes a checked table, including its catalog spec, market, primary key, price columns, volume columns, and related adjustment or market data table.
- `QualityRule`: evaluates one rule and emits pass, warning, or failure findings.
- `QualityRunner`: selects targets, applies rules for the requested mode and date range, and returns structured results.
- `QualityReporter`: prints a compact terminal summary and writes detailed local report files.

The module reads existing config and local Parquet data. It does not mutate data.

## CLI

Add a `quality check` command group:

```bash
uv run python main.py quality check --all --mode full --start-date 20200101 --end-date 20241231
uv run python main.py quality check --market stock --mode daily
uv run python main.py quality check --table daily_kline --mode daily --date 20240701
```

Target selection supports:

- `--table <name>`
- `--market stock|index|etf|futures|options`
- `--all`

Modes:

- `--mode full`: requires `--start-date` and `--end-date`.
- `--mode daily`: checks the latest synced trading day by default, or the date passed with `--date`.

For direct CLI usage, failures return a non-zero exit code. Warnings alone return zero.

## Rules

Structural checks:

- Table directory exists.
- Parquet files are readable.
- Required columns exist, based on the existing catalog specs.
- Primary key is unique. The first version uses `ts_code + trade_date` for all scoped tables.

Partition coverage:

- For trading-day tables, expected partitions use `date=YYYYMMDD/data.parquet`.
- When the trade calendar is available, check requested trading days for missing partitions.
- If the trade calendar is missing, downgrade coverage checking to existing partitions only and emit a warning.

Market data consistency:

- `open`, `high`, `low`, and `close` are present and greater than 0.
- `amount > 0` when the column exists.
- `vol > 0` when the column exists.
- `high >= max(open, close)`.
- `low <= min(open, close)`.
- `high >= low`.
- When `pre_close > 0` and `pct_chg` exists, validate:

```text
abs(((close / pre_close - 1) * 100) - pct_chg) <= 0.01
```

Adjustment factor checks:

- `adj_factor` is present, non-null, and greater than 0.
- `ts_code + trade_date` is unique.
- Large per-code factor jumps are warnings, not failures.
- Low same-date code coverage between adjustment factors and the corresponding market data table is a warning.

## Severity

Use three statuses:

- `pass`: no finding.
- `warn`: suspicious but not necessarily bad, such as missing partitions, empty partitions, zero volume or amount, missing trade calendar fallback, large adjustment factor jumps, and low coverage.
- `fail`: invalid local data, such as unreadable files, missing required columns, duplicate primary keys, non-positive prices, invalid OHLC relationships, invalid `pct_chg`, and non-positive adjustment factors.

Daily scheduled checks warn but do not interrupt sync or scheduler progress.

## Output

Terminal output should be a compact table-level summary.

Detailed reports are written under:

```text
reports/quality/YYYYMMDD_HHMMSS/
  summary.csv
  findings.csv
  metadata.json
```

`summary.csv` contains one row per table. `findings.csv` contains one row per rule finding with table, date, severity, rule, count, message, and sample data. `metadata.json` records run parameters, timestamps, data directory, and git commit when available.

## Scheduler Integration

Add optional config later in the implementation:

```toml
[quality]
enabled = false
mode = "daily"
markets = ["stock", "index", "etf", "futures", "options"]
notify_on = ["warn", "fail"]
```

When enabled, the scheduler runs `QualityRunner` after sync work finishes. It sends a notifier summary and report path for warnings or failures. Findings do not stop the scheduler.

## Testing

Tests should use temporary Parquet data and must not depend on Tushare or network access.

Cover:

- CLI target selection and mode/date validation.
- Missing table directory, missing partition, empty partition, unreadable or invalid Parquet file.
- Missing required columns.
- Duplicate `ts_code + trade_date`.
- OHLC and positive price rules.
- `vol`, `amount`, and `pct_chg` checks, including the 0.01 percentage-point tolerance.
- Adjustment factor positivity, duplicate keys, large jump warning, and market data coverage warning.
- Report file generation.
- Scheduler integration behavior where warnings and failures notify but do not interrupt scheduling.
