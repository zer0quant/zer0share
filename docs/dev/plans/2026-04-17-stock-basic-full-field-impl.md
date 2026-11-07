# Stock Basic Full-Field Mirror Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Update the `stock_basic` sync path so the local `basic` snapshot preserves all documented Tushare fields for `L,D,P,G` statuses while keeping existing date conversion for `list_date` and `delist_date`.

**Architecture:** Keep `basic/data.parquet` as a full snapshot mirror written by the existing storage layer. Make the fetcher explicitly request the documented `stock_basic` field set, convert only the two date columns, and update tests so schema loss or status regression is caught immediately.

**Tech Stack:** Python 3.11, pytest, pandas, Tushare, PyArrow Parquet

---

### Task 1: Lock in Fetcher Behavior With Tests

**Files:**
- Modify: `tests/test_fetcher.py`

**Step 1: Write the failing test for full-field fetch behavior**

Add a test that returns a DataFrame with all documented `stock_basic` columns:

```python
def test_fetch_basic_returns_all_documented_columns(mock_pro):
    mock_pro.stock_basic.return_value = pd.DataFrame({
        "ts_code": ["000001.SZ"],
        "symbol": ["000001"],
        "name": ["平安银行"],
        "area": ["深圳"],
        "industry": ["银行"],
        "fullname": ["平安银行股份有限公司"],
        "enname": ["Ping An Bank"],
        "cnspell": ["payh"],
        "market": ["主板"],
        "exchange": ["SZSE"],
        "curr_type": ["CNY"],
        "list_status": ["L"],
        "list_date": ["19910403"],
        "delist_date": [None],
        "is_hs": ["S"],
        "act_name": ["深圳市投资控股有限公司"],
        "act_ent_type": ["地方国企"],
    })
    fetcher = TushareFetcher("fake_token")
    df = fetcher.fetch_basic()
    assert list(df.columns) == [...]
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_fetcher.py::test_fetch_basic_returns_all_documented_columns -v`  
Expected: FAIL because current implementation only returns the smaller subset of columns.

**Step 3: Add a failing test for request parameters**

Add a test that verifies `stock_basic` is called with:

```python
mock_pro.stock_basic.assert_called_once_with(
    exchange="",
    list_status="L,D,P,G",
    fields="ts_code,symbol,name,area,industry,fullname,enname,cnspell,market,exchange,curr_type,list_status,list_date,delist_date,is_hs,act_name,act_ent_type",
)
```

**Step 4: Run test to verify it fails**

Run: `pytest tests/test_fetcher.py::test_fetch_basic_requests_all_statuses_and_fields -v`  
Expected: FAIL because current implementation still requests `L,D,P` and the truncated field list.

**Step 5: Add a focused date-conversion regression test**

Add a test asserting:

```python
assert df.iloc[0]["list_date"] == date(1991, 4, 3)
assert df.iloc[0]["delist_date"] == date(2024, 1, 31)
```

Use a non-null `delist_date` string so both conversions are exercised.

**Step 6: Run test to verify it fails or stays red for the right reason**

Run: `pytest tests/test_fetcher.py::test_fetch_basic_converts_only_date_fields -v`  
Expected: FAIL only if the fetcher still drops fields or does not preserve the expected conversions.

**Step 7: Commit after green**

```bash
git add tests/test_fetcher.py
git commit -m "test: cover full stock_basic field contract"
```

### Task 2: Lock in Storage Round-Trip Behavior

**Files:**
- Modify: `tests/test_storage.py`

**Step 1: Write the failing storage round-trip test**

Replace the narrow `basic` fixture with a full-field fixture:

```python
df = pd.DataFrame({
    "ts_code": ["000001.SZ"],
    "symbol": ["000001"],
    "name": ["平安银行"],
    "area": ["深圳"],
    "industry": ["银行"],
    "fullname": ["平安银行股份有限公司"],
    "enname": ["Ping An Bank"],
    "cnspell": ["payh"],
    "market": ["主板"],
    "exchange": ["SZSE"],
    "curr_type": ["CNY"],
    "list_status": ["L"],
    "list_date": [date(1991, 4, 3)],
    "delist_date": [None],
    "is_hs": ["S"],
    "act_name": ["深圳市投资控股有限公司"],
    "act_ent_type": ["地方国企"],
})
```

Assert that the read-back DataFrame still contains all columns.

**Step 2: Run test to verify current behavior**

Run: `pytest tests/test_storage.py::test_write_and_read_basic -v`  
Expected: PASS or FAIL. If it already passes, keep the stronger test because it protects the contract from future regressions.

**Step 3: Expand overwrite test fixture to the full schema**

Update `test_basic_overwrites_on_second_write` so both writes use the full field set and the final read preserves the extended columns.

**Step 4: Run storage tests**

Run: `pytest tests/test_storage.py -v`  
Expected: PASS once the fixtures reflect the full schema and storage remains schema-agnostic.

**Step 5: Commit after green**

```bash
git add tests/test_storage.py
git commit -m "test: preserve full stock_basic schema in parquet"
```

### Task 3: Implement Minimal Fetcher Changes

**Files:**
- Modify: `src/fetcher.py`

**Step 1: Update the documented field list**

Replace the current `BASIC_COLS` definition with:

```python
BASIC_COLS = [
    "ts_code",
    "symbol",
    "name",
    "area",
    "industry",
    "fullname",
    "enname",
    "cnspell",
    "market",
    "exchange",
    "curr_type",
    "list_status",
    "list_date",
    "delist_date",
    "is_hs",
    "act_name",
    "act_ent_type",
]
```

**Step 2: Update the `stock_basic` request**

Change the fetch call to:

```python
df = self._pro.stock_basic(
    exchange="",
    list_status="L,D,P,G",
    fields=",".join(BASIC_COLS),
)
```

**Step 3: Keep only the existing date conversions**

Retain conversion for:

```python
df["list_date"] = pd.to_datetime(...).dt.date
df["delist_date"] = pd.to_datetime(...).apply(...)
```

Do not add any other normalization.

**Step 4: Return the full schema**

Return:

```python
return df[BASIC_COLS]
```

**Step 5: Run targeted fetcher tests**

Run: `pytest tests/test_fetcher.py -v`  
Expected: PASS

**Step 6: Commit after green**

```bash
git add src/fetcher.py tests/test_fetcher.py
git commit -m "feat: keep full stock_basic field set"
```

### Task 4: Verify Broader Compatibility

**Files:**
- Check: `tests/test_pipeline.py`
- Check: `docs/plans/2026-04-17-tushare-pipeline-design.md`

**Step 1: Run pipeline tests that exercise basic sync**

Run: `pytest tests/test_pipeline.py -v`  
Expected: PASS without further production changes because the pipeline should already write arbitrary DataFrame columns.

**Step 2: If pipeline fixtures fail, update only test fixtures**

If tests fail because they still build the old narrow `basic` DataFrame, update those fixtures to use the full schema without changing pipeline behavior.

**Step 3: Run the relevant full test suite**

Run: `pytest tests/test_fetcher.py tests/test_storage.py tests/test_pipeline.py -v`  
Expected: PASS

**Step 4: Commit after green**

```bash
git add tests/test_pipeline.py
git commit -m "test: align pipeline fixtures with stock_basic schema"
```

