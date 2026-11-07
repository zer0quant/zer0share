# Small-Cap Universe Design

## Goal

Add a local zer0share stock universe for small-cap stocks. The universe should be available through the existing `build-universe` command and `pro.universe(...)` query path, just like the existing base and index universes.

## Definition

The new universe name is `univ_trade_smallcap`.

For each trade date:

1. Build the existing universe detail frame from local stock data.
2. Start from stocks where `in_trade_base` is true.
3. Among those stocks, rank `total_mv` ascending.
4. Include the bottom 20% by that rank.

The percentile is computed after trade-base filters. This keeps the pool usable for trading workflows by excluding ST, suspended, one-price limit, insufficient-liquidity, very new, delisted, and non-common A-share names before selecting small-cap names.

If no trade-base rows have valid `total_mv`, the small-cap universe is empty for that date.

## Code Changes

- Add `univ_trade_smallcap` to the central universe name list so range builds, completeness checks, and incremental skipping all treat it as a required universe partition.
- Add a helper for bottom-market-cap selection that mirrors the existing market-cap ranking style.
- Add the new universe to `build_universes(...)` output before writing partitions.
- Update docs to list the new universe and describe the 20% trade-base market-cap rule.

## Data Flow

`main.py build-universe` calls the existing CLI command, which calls `build_universes_range(...)` or `build_universes(...)`.

`build_universes(...)` creates the existing detail frame. It writes:

- `univ_research_base`
- `univ_trade_base`
- `univ_trade_hs300`
- `univ_trade_zz500`
- `univ_trade_zz1000`
- `univ_trade_smallcap`

The output schema stays unchanged: `trade_date`, `universe`, `ts_code`.

## Tests

Add or update focused tests in `tests/test_universe.py`:

- single-day build writes `univ_trade_smallcap`;
- count and members match the bottom 20% of trade-base rows by `total_mv`;
- range-build skip/completeness fixtures include the new universe name.

Run the universe tests after implementation. If practical, also run the full pytest suite.
