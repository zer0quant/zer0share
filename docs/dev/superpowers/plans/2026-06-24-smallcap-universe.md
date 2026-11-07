# Small-Cap Universe Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `univ_trade_smallcap`, a daily stock universe containing the bottom 20% by `total_mv` among `univ_trade_base` stocks.

**Architecture:** Reuse the existing `zer0share.universe` build path. Add the new universe to the central universe-name list so build output, range completeness checks, and incremental skipping all treat the new partition as required. Keep the persisted universe schema unchanged: `trade_date`, `universe`, `ts_code`.

**Tech Stack:** Python, pandas, pytest, existing zer0share Parquet stores.

---

## File Structure

- Modify `zer0share/universe.py`: add `univ_trade_smallcap` to `UNIVERSE_NAMES`, select bottom 20% of trade-base rows by `total_mv`, and write it with the existing universe writer.
- Modify `tests/test_universe.py`: add assertions for small-cap output and update completeness fixtures to include the new universe name.
- Modify `README.md`: document the new stock pool and storage partition.
- Modify `skills/zer0share-data/references/api.md`: list `univ_trade_smallcap` as a built universe.

### Task 1: Test Small-Cap Universe Output

**Files:**
- Modify: `tests/test_universe.py`

- [ ] **Step 1: Add assertions to the single-day universe build test**

In `tests/test_universe.py`, update `test_build_universes_writes_index_intersections` after `counts = build_universes(tmp_path, trade_date)`:

```python
    assert counts["univ_trade_smallcap"] == 3
    smallcap = pd.read_parquet(
        tmp_path
        / "stock"
        / "universe"
        / "name=univ_trade_smallcap"
        / "date=20240130"
        / "data.parquet"
    )
    assert smallcap["ts_code"].tolist() == ["000002.SZ", "000003.SZ", "000004.SZ"]
```

This fixture has 19 trade-base names after the existing bottom-5% market-cap filter removes `000001.SZ`; the bottom-20% selection should include the 3 smallest remaining `total_mv` names.

- [ ] **Step 2: Run the focused test and verify it fails**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 uv run pytest -p no:cacheprovider tests/test_universe.py::test_build_universes_writes_index_intersections -q
```

Expected: FAIL with a missing `univ_trade_smallcap` key or missing partition.

### Task 2: Implement `univ_trade_smallcap`

**Files:**
- Modify: `zer0share/universe.py`

- [ ] **Step 1: Add the universe name constant**

Change the constants at the top of `zer0share/universe.py` to include a derived trade universe:

```python
BASE_UNIVERSES = ("univ_research_base", "univ_trade_base")
DERIVED_TRADE_UNIVERSES = ("univ_trade_smallcap",)
INDEX_UNIVERSES = {
    "univ_trade_hs300": "399300.SZ",
    "univ_trade_zz500": "000905.SH",
    "univ_trade_zz1000": "000852.SH",
}
UNIVERSE_NAMES = (*BASE_UNIVERSES, *INDEX_UNIVERSES.keys(), *DERIVED_TRADE_UNIVERSES)
```

- [ ] **Step 2: Add the small-cap output**

In `build_universes(...)`, after the `outputs` dict is created and before the index-universe loop, add:

```python
    smallcap_mask = detail["in_trade_base"] & _bottom_market_cap(detail, 0.20)
    outputs["univ_trade_smallcap"] = detail.loc[
        smallcap_mask, ["trade_date", "ts_code"]
    ]
```

- [ ] **Step 3: Add the helper**

Below `_not_bottom_market_cap(...)`, add:

```python
def _bottom_market_cap(df: pd.DataFrame, pct: float) -> pd.Series:
    valid = df["total_mv"].notna()
    result = pd.Series(False, index=df.index)
    if not valid.any():
        return result
    ranks = df.loc[valid, "total_mv"].rank(method="first", ascending=True)
    cutoff = int(len(ranks) * pct)
    result.loc[valid] = ranks <= cutoff
    return result
```

- [ ] **Step 4: Run the focused test and verify it passes**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 uv run pytest -p no:cacheprovider tests/test_universe.py::test_build_universes_writes_index_intersections -q
```

Expected: PASS.

### Task 3: Update Completeness Fixtures

**Files:**
- Modify: `tests/test_universe.py`

- [ ] **Step 1: Add the new universe to skip/completeness fixture lists**

In every hard-coded list containing:

```python
[
    "univ_research_base",
    "univ_trade_base",
    "univ_trade_hs300",
    "univ_trade_zz500",
    "univ_trade_zz1000",
]
```

replace it with:

```python
[
    "univ_research_base",
    "univ_trade_base",
    "univ_trade_hs300",
    "univ_trade_zz500",
    "univ_trade_zz1000",
    "univ_trade_smallcap",
]
```

- [ ] **Step 2: Run universe tests**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 uv run pytest -p no:cacheprovider tests/test_universe.py -q
```

Expected: all `tests/test_universe.py` tests pass.

### Task 4: Update User-Facing Docs

**Files:**
- Modify: `README.md`
- Modify: `skills/zer0share-data/references/api.md`

- [ ] **Step 1: Update README stock-pool table**

In `README.md`, add this row to the generated universe table:

```markdown
| `univ_trade_smallcap` | 基础交易池中总市值倒数 20% 的小市值股票池 |
```

Also update the sentence that says `build-universe` builds 5 stock pools to say 6 stock pools.

- [ ] **Step 2: Update README storage tree**

In `README.md`, add this storage-tree line under `stock/universe`:

```markdown
│       └── name=univ_trade_smallcap/date=YYYYMMDD/data.parquet
```

If it is no longer the last line, use `├──` instead of `└──` for the preceding sibling line.

- [ ] **Step 3: Update API reference**

In `skills/zer0share-data/references/api.md`, update the available universe list to include `univ_trade_smallcap`.

### Task 5: Final Verification

**Files:**
- Verify only

- [ ] **Step 1: Run focused tests**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 uv run pytest -p no:cacheprovider tests/test_universe.py tests/test_api.py -q
```

Expected: all selected tests pass.

- [ ] **Step 2: Check diff**

Run:

```bash
git diff -- zer0share/universe.py tests/test_universe.py README.md skills/zer0share-data/references/api.md
```

Expected: only small-cap universe implementation, tests, and documentation changes appear.
