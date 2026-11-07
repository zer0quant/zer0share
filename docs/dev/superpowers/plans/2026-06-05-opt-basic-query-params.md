# Opt Basic Query Params Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend local `pro.opt_basic()` queries with `name`, `list_date`, `offset`, and `limit` parameters.

**Architecture:** Keep the change inside the existing local API query method. Add exact-match filters to the current DuckDB SQL builder, then apply stable pagination after `ORDER BY ts_code`.

**Tech Stack:** Python 3.11, pandas, DuckDB, pytest, local Parquet storage helpers.

---

## File Structure

- Modify `zer0share/api.py`: update `LocalPro.opt_basic()` signature and SQL construction.
- Modify `tests/test_api.py`: add focused tests using the existing `_make_opt_basic_df()` fixture and `write_opt_basic()` helper.

---

### Task 1: Add Failing Tests For New Filters

**Files:**
- Modify: `tests/test_api.py`
- Test: `tests/test_api.py`

- [ ] **Step 1: Add tests for `name` and `list_date`**

Insert these tests after `test_opt_basic_query_filters_by_call_put`:

```python
def test_opt_basic_filters_by_name_and_list_date(tmp_path):
    write_opt_basic(tmp_path / "options", _make_opt_basic_df())

    api = LocalPro(tmp_path)
    result = api.opt_basic(
        name="50ETF购4月2700",
        list_date="20240101",
        fields="ts_code,name,list_date",
    )

    assert result.to_dict("records") == [
        {
            "ts_code": "10004462.SH",
            "name": "50ETF购4月2700",
            "list_date": "20240101",
        }
    ]
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 uv run pytest -p no:cacheprovider tests/test_api.py::test_opt_basic_filters_by_name_and_list_date -q
```

Expected: FAIL with `TypeError: LocalPro.opt_basic() got an unexpected keyword argument 'name'`.

- [ ] **Step 3: Commit failing test**

```bash
git add tests/test_api.py
git commit -m "test: cover opt_basic name and list_date filters"
```

---

### Task 2: Implement Exact-Match Filters

**Files:**
- Modify: `zer0share/api.py`
- Test: `tests/test_api.py`

- [ ] **Step 1: Update `opt_basic()` signature**

Change the signature in `zer0share/api.py` to:

```python
def opt_basic(
    self,
    ts_code: str | None = None,
    exchange: str | None = None,
    opt_code: str | None = None,
    call_put: str | None = None,
    name: str | None = None,
    list_date: str | None = None,
    fields: str | list[str] | None = None,
) -> pd.DataFrame:
```

- [ ] **Step 2: Add SQL filters**

Add this block after the existing `call_put` filter:

```python
if name is not None:
    where.append("name = ?")
    params.append(name)
if list_date is not None:
    where.append("list_date = ?")
    params.append(list_date)
```

- [ ] **Step 3: Run filter test**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 uv run pytest -p no:cacheprovider tests/test_api.py::test_opt_basic_filters_by_name_and_list_date -q
```

Expected: PASS.

- [ ] **Step 4: Commit filter implementation**

```bash
git add zer0share/api.py tests/test_api.py
git commit -m "feat: add opt_basic name and list_date filters"
```

---

### Task 3: Add Failing Tests For Pagination

**Files:**
- Modify: `tests/test_api.py`
- Test: `tests/test_api.py`

- [ ] **Step 1: Add pagination test**

Insert this test after `test_opt_basic_filters_by_name_and_list_date`:

```python
def test_opt_basic_supports_limit_and_offset(tmp_path):
    write_opt_basic(tmp_path / "options", _make_opt_basic_df())

    api = LocalPro(tmp_path)
    result = api.opt_basic(limit=1, offset=1, fields="ts_code")

    assert result.to_dict("records") == [
        {"ts_code": "10004463.SH"}
    ]
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 uv run pytest -p no:cacheprovider tests/test_api.py::test_opt_basic_supports_limit_and_offset -q
```

Expected: FAIL with `TypeError: LocalPro.opt_basic() got an unexpected keyword argument 'limit'`.

- [ ] **Step 3: Commit failing test**

```bash
git add tests/test_api.py
git commit -m "test: cover opt_basic pagination"
```

---

### Task 4: Implement Pagination

**Files:**
- Modify: `zer0share/api.py`
- Test: `tests/test_api.py`

- [ ] **Step 1: Update `opt_basic()` signature**

Change the signature in `zer0share/api.py` to:

```python
def opt_basic(
    self,
    ts_code: str | None = None,
    exchange: str | None = None,
    opt_code: str | None = None,
    call_put: str | None = None,
    name: str | None = None,
    list_date: str | None = None,
    offset: int | None = None,
    limit: int | None = None,
    fields: str | list[str] | None = None,
) -> pd.DataFrame:
```

- [ ] **Step 2: Add SQL pagination**

Add this block after `sql += " ORDER BY ts_code"`:

```python
if limit is not None:
    sql += " LIMIT ?"
    params.append(limit)
if offset is not None:
    sql += " OFFSET ?"
    params.append(offset)
```

- [ ] **Step 3: Run pagination test**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 uv run pytest -p no:cacheprovider tests/test_api.py::test_opt_basic_supports_limit_and_offset -q
```

Expected: PASS.

- [ ] **Step 4: Commit pagination implementation**

```bash
git add zer0share/api.py tests/test_api.py
git commit -m "feat: add opt_basic pagination"
```

---

### Task 5: Verify Combined Fields And Full API Tests

**Files:**
- Modify: `tests/test_api.py`
- Test: `tests/test_api.py`

- [ ] **Step 1: Add combined filter and fields test**

Insert this test after `test_opt_basic_supports_limit_and_offset`:

```python
def test_opt_basic_combines_new_filters_with_fields(tmp_path):
    write_opt_basic(tmp_path / "options", _make_opt_basic_df())

    api = LocalPro(tmp_path)
    result = api.opt_basic(
        exchange="SSE",
        call_put="P",
        list_date="20240101",
        limit=1,
        fields=["ts_code", "call_put", "list_date"],
    )

    assert result.to_dict("records") == [
        {
            "ts_code": "10004463.SH",
            "call_put": "P",
            "list_date": "20240101",
        }
    ]
```

- [ ] **Step 2: Run all opt_basic tests**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 uv run pytest -p no:cacheprovider tests/test_api.py -q
```

Expected: PASS for the full `tests/test_api.py` file.

- [ ] **Step 3: Check worktree**

Run:

```bash
git status --short
```

Expected: only intended files changed before the final commit, or a clean worktree after the final commit.

- [ ] **Step 4: Commit final test coverage if needed**

If Step 1 was not included in an earlier commit, run:

```bash
git add tests/test_api.py
git commit -m "test: cover opt_basic combined query params"
```
