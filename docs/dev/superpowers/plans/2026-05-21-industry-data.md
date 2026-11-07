# Industry Data Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Shenwan (SW) and CITIC L1 industry stock-industry mapping data sync and local query support.

**Architecture:** Full-refresh sync of industry mapping data to single Parquet files under `data/industry/`. Each sync fetches all historical records (including `is_new='N'`). LocalPro API exposes 3 query methods reading directly from Parquet via DuckDB.

**Tech Stack:** Python, tushare, pandas, pyarrow, duckdb, click, pytest

---

## File Structure

| File | Responsibility |
|------|---------------|
| `zer0share/fetcher.py` | Add column defs + 3 fetch methods for SW classify, SW member, CI member |
| `zer0share/storage.py` | Add 6 read/write functions for industry parquet files |
| `zer0share/pipeline.py` | Add `sync_industry()` and `sync_ci_member()` methods |
| `zer0share/cli.py` | Add `"industry"` and `"ci_member"` to SYNC_TABLES + sync branches |
| `zer0share/api.py` | Add 3 query methods + dispatch registration |
| `tests/test_storage.py` | Test new read/write functions |
| `tests/test_fetcher.py` | Test new fetch methods |
| `tests/test_pipeline.py` | Test new sync methods |
| `tests/test_api.py` | Test new query methods |
| `tests/test_cli.py` | Test new CLI table options |

---

### Task 1: Storage Layer

**Files:**
- Modify: `zer0share/storage.py`
- Test: `tests/test_storage.py`

- [ ] **Step 1: Write failing tests for industry storage functions**

Add to `tests/test_storage.py`:

```python
from zer0share.storage import (
    write_sw_classify,
    read_sw_classify,
    write_sw_member,
    read_sw_member,
    write_ci_member,
    read_ci_member,
)


def test_write_and_read_sw_classify(tmp_path):
    df = pd.DataFrame(
        {
            "index_code": ["801010.SI", "801030.SI"],
            "industry_name": ["农林牧渔", "化工"],
            "level": ["L1", "L1"],
            "parent_code": ["0", "0"],
            "industry_code": ["110000", "220000"],
            "is_pub": ["1", "1"],
            "src": ["SW2021", "SW2021"],
        }
    )
    write_sw_classify(tmp_path, df)
    result = read_sw_classify(tmp_path)
    assert len(result) == 2
    assert result.iloc[0]["industry_name"] == "农林牧渔"
    assert (tmp_path / "industry" / "sw_classify" / "data.parquet").exists()


def test_sw_classify_overwrites_on_second_write(tmp_path):
    df1 = pd.DataFrame(
        {
            "index_code": ["801010.SI"],
            "industry_name": ["农林牧渔"],
            "level": ["L1"],
            "parent_code": ["0"],
            "industry_code": ["110000"],
            "is_pub": ["1"],
            "src": ["SW2021"],
        }
    )
    df2 = pd.DataFrame(
        {
            "index_code": ["801010.SI", "801030.SI"],
            "industry_name": ["农林牧渔", "化工"],
            "level": ["L1", "L1"],
            "parent_code": ["0", "0"],
            "industry_code": ["110000", "220000"],
            "is_pub": ["1", "1"],
            "src": ["SW2021", "SW2021"],
        }
    )
    write_sw_classify(tmp_path, df1)
    write_sw_classify(tmp_path, df2)
    result = read_sw_classify(tmp_path)
    assert len(result) == 2


def test_read_sw_classify_returns_empty_if_not_exists(tmp_path):
    result = read_sw_classify(tmp_path)
    assert result.empty


def test_write_and_read_sw_member(tmp_path):
    df = pd.DataFrame(
        {
            "l1_code": ["801010.SI", "801010.SI"],
            "l1_name": ["农林牧渔", "农林牧渔"],
            "l2_code": ["801016.SI", "801016.SI"],
            "l2_name": ["种植业", "种植业"],
            "l3_code": ["850111.SI", "850112.SI"],
            "l3_name": ["种子", "粮食种植"],
            "ts_code": ["002041.SZ", "600313.SH"],
            "name": ["登海种业", "农发种业"],
            "in_date": [date(2021, 12, 13), date(2021, 12, 13)],
            "out_date": [None, None],
            "is_new": ["Y", "Y"],
        }
    )
    write_sw_member(tmp_path, df)
    result = read_sw_member(tmp_path)
    assert len(result) == 2
    assert result.iloc[0]["ts_code"] == "002041.SZ"
    assert (tmp_path / "industry" / "sw_member" / "data.parquet").exists()


def test_sw_member_overwrites_on_second_write(tmp_path):
    df1 = pd.DataFrame(
        {
            "l1_code": ["801010.SI"],
            "l1_name": ["农林牧渔"],
            "l2_code": ["801016.SI"],
            "l2_name": ["种植业"],
            "l3_code": ["850111.SI"],
            "l3_name": ["种子"],
            "ts_code": ["002041.SZ"],
            "name": ["登海种业"],
            "in_date": [date(2021, 12, 13)],
            "out_date": [None],
            "is_new": ["Y"],
        }
    )
    df2 = pd.DataFrame(
        {
            "l1_code": ["801010.SI", "801010.SI"],
            "l1_name": ["农林牧渔", "农林牧渔"],
            "l2_code": ["801016.SI", "801016.SI"],
            "l2_name": ["种植业", "种植业"],
            "l3_code": ["850111.SI", "850112.SI"],
            "l3_name": ["种子", "粮食种植"],
            "ts_code": ["002041.SZ", "600313.SH"],
            "name": ["登海种业", "农发种业"],
            "in_date": [date(2021, 12, 13), date(2021, 12, 13)],
            "out_date": [None, None],
            "is_new": ["Y", "Y"],
        }
    )
    write_sw_member(tmp_path, df1)
    write_sw_member(tmp_path, df2)
    result = read_sw_member(tmp_path)
    assert len(result) == 2


def test_read_sw_member_returns_empty_if_not_exists(tmp_path):
    result = read_sw_member(tmp_path)
    assert result.empty


def test_write_and_read_ci_member(tmp_path):
    df = pd.DataFrame(
        {
            "l1_code": ["CI005001.CI", "CI005001.CI"],
            "l1_name": ["农林牧渔", "农林牧渔"],
            "l2_code": ["CI005005.CI", "CI005005.CI"],
            "l2_name": ["农产品加工", "农产品加工"],
            "l3_code": ["CI005006.CI", "CI005007.CI"],
            "l3_name": ["粮油加工", "果蔬加工"],
            "ts_code": ["000876.SZ", "600737.SH"],
            "name": ["新 希 望", "中粮糖业"],
            "in_date": [date(2020, 1, 1), date(2020, 1, 1)],
            "out_date": [None, None],
            "is_new": ["Y", "Y"],
        }
    )
    write_ci_member(tmp_path, df)
    result = read_ci_member(tmp_path)
    assert len(result) == 2
    assert result.iloc[0]["ts_code"] == "000876.SZ"
    assert (tmp_path / "industry" / "ci_member" / "data.parquet").exists()


def test_ci_member_overwrites_on_second_write(tmp_path):
    df1 = pd.DataFrame(
        {
            "l1_code": ["CI005001.CI"],
            "l1_name": ["农林牧渔"],
            "l2_code": ["CI005005.CI"],
            "l2_name": ["农产品加工"],
            "l3_code": ["CI005006.CI"],
            "l3_name": ["粮油加工"],
            "ts_code": ["000876.SZ"],
            "name": ["新 希 望"],
            "in_date": [date(2020, 1, 1)],
            "out_date": [None],
            "is_new": ["Y"],
        }
    )
    df2 = pd.DataFrame(
        {
            "l1_code": ["CI005001.CI", "CI005002.CI"],
            "l1_name": ["农林牧渔", "采掘"],
            "l2_code": ["CI005005.CI", "CI005010.CI"],
            "l2_name": ["农产品加工", "煤炭开采"],
            "l3_code": ["CI005006.CI", "CI005011.CI"],
            "l3_name": ["粮油加工", "动力煤"],
            "ts_code": ["000876.SZ", "601088.SH"],
            "name": ["新 希 望", "中国神华"],
            "in_date": [date(2020, 1, 1), date(2020, 1, 1)],
            "out_date": [None, None],
            "is_new": ["Y", "Y"],
        }
    )
    write_ci_member(tmp_path, df1)
    write_ci_member(tmp_path, df2)
    result = read_ci_member(tmp_path)
    assert len(result) == 2


def test_read_ci_member_returns_empty_if_not_exists(tmp_path):
    result = read_ci_member(tmp_path)
    assert result.empty
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /data/zer0share && python -m pytest tests/test_storage.py -k "sw_classify or sw_member or ci_member" -v`
Expected: FAIL with ImportError (functions not defined)

- [ ] **Step 3: Implement storage functions**

Add to `zer0share/storage.py` after the existing `read_trade_cal` function:

```python
def write_sw_classify(data_dir: Path, df: pd.DataFrame) -> None:
    classify_dir = data_dir / "industry" / "sw_classify"
    classify_dir.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, classify_dir / "data.parquet")


def read_sw_classify(data_dir: Path) -> pd.DataFrame:
    path = data_dir / "industry" / "sw_classify" / "data.parquet"
    if not path.exists():
        return pd.DataFrame()
    return pq.read_table(path).to_pandas()


def write_sw_member(data_dir: Path, df: pd.DataFrame) -> None:
    member_dir = data_dir / "industry" / "sw_member"
    member_dir.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, member_dir / "data.parquet")


def read_sw_member(data_dir: Path) -> pd.DataFrame:
    path = data_dir / "industry" / "sw_member" / "data.parquet"
    if not path.exists():
        return pd.DataFrame()
    return pq.read_table(path).to_pandas()


def write_ci_member(data_dir: Path, df: pd.DataFrame) -> None:
    member_dir = data_dir / "industry" / "ci_member"
    member_dir.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, member_dir / "data.parquet")


def read_ci_member(data_dir: Path) -> pd.DataFrame:
    path = data_dir / "industry" / "ci_member" / "data.parquet"
    if not path.exists():
        return pd.DataFrame()
    return pq.read_table(path).to_pandas()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /data/zer0share && python -m pytest tests/test_storage.py -k "sw_classify or sw_member or ci_member" -v`
Expected: All 9 tests PASS

- [ ] **Step 5: Commit**

```bash
git add zer0share/storage.py tests/test_storage.py
git commit -m "feat: add industry storage read/write functions"
```

---

### Task 2: Fetcher Layer

**Files:**
- Modify: `zer0share/fetcher.py`
- Test: `tests/test_fetcher.py`

- [ ] **Step 1: Write failing tests for fetch methods**

Add to `tests/test_fetcher.py`:

```python
SW_CLASSIFY_COLS = [
    "index_code", "industry_name", "level", "parent_code",
    "industry_code", "is_pub", "src",
]
SW_MEMBER_COLS = [
    "l1_code", "l1_name", "l2_code", "l2_name",
    "l3_code", "l3_name", "ts_code", "name",
    "in_date", "out_date", "is_new",
]
CI_MEMBER_COLS = [
    "l1_code", "l1_name", "l2_code", "l2_name",
    "l3_code", "l3_name", "ts_code", "name",
    "in_date", "out_date", "is_new",
]


def test_fetch_sw_classify_calls_all_levels(mock_pro):
    l1_df = pd.DataFrame({
        "index_code": ["801010.SI"],
        "industry_name": ["农林牧渔"],
        "level": ["L1"],
        "parent_code": ["0"],
        "industry_code": ["110000"],
        "is_pub": ["1"],
    })
    l2_df = pd.DataFrame({
        "index_code": ["801016.SI"],
        "industry_name": ["种植业"],
        "level": ["L2"],
        "parent_code": ["110000"],
        "industry_code": ["110100"],
        "is_pub": ["1"],
    })
    l3_df = pd.DataFrame({
        "index_code": ["850111.SI"],
        "industry_name": ["种子"],
        "level": ["L3"],
        "parent_code": ["110100"],
        "industry_code": ["110101"],
        "is_pub": ["1"],
    })
    mock_pro.index_classify.side_effect = [l1_df, l2_df, l3_df]
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_sw_classify()

    assert list(df.columns) == SW_CLASSIFY_COLS
    assert len(df) == 3
    mock_pro.index_classify.assert_any_call(level="L1", src="SW2021")
    mock_pro.index_classify.assert_any_call(level="L2", src="SW2021")
    mock_pro.index_classify.assert_any_call(level="L3", src="SW2021")


def test_fetch_sw_member_iterates_l1_codes(mock_pro):
    l1_df = pd.DataFrame({
        "index_code": ["801010.SI", "801030.SI"],
        "industry_name": ["农林牧渔", "化工"],
        "level": ["L1", "L1"],
    })
    mock_pro.index_classify.return_value = l1_df
    member_dfs = [
        pd.DataFrame({
            "l1_code": ["801010.SI"], "l1_name": ["农林牧渔"],
            "l2_code": ["801016.SI"], "l2_name": ["种植业"],
            "l3_code": ["850111.SI"], "l3_name": ["种子"],
            "ts_code": ["002041.SZ"], "name": ["登海种业"],
            "in_date": ["20211213"], "out_date": [None], "is_new": ["Y"],
        }),
        pd.DataFrame({
            "l1_code": ["801030.SI"], "l1_name": ["化工"],
            "l2_code": ["801033.SI"], "l2_name": ["化学原料"],
            "l3_code": ["850321.SI"], "l3_name": ["纯碱"],
            "ts_code": ["600291.SH"], "name": ["西水股份"],
            "in_date": ["20211213"], "out_date": [None], "is_new": ["Y"],
        }),
    ]
    mock_pro.index_member_all.side_effect = member_dfs
    fetcher = TushareFetcher("fake_token")

    with patch("zer0share.fetcher.time.sleep"):
        df = fetcher.fetch_sw_member()

    assert list(df.columns) == SW_MEMBER_COLS
    assert len(df) == 2
    mock_pro.index_member_all.assert_any_call(l1_code="801010.SI", is_new="")
    mock_pro.index_member_all.assert_any_call(l1_code="801030.SI", is_new="")


def test_fetch_sw_member_converts_dates(mock_pro):
    l1_df = pd.DataFrame({"index_code": ["801010.SI"], "industry_name": ["农林牧渔"], "level": ["L1"]})
    mock_pro.index_classify.return_value = l1_df
    mock_pro.index_member_all.return_value = pd.DataFrame({
        "l1_code": ["801010.SI"], "l1_name": ["农林牧渔"],
        "l2_code": ["801016.SI"], "l2_name": ["种植业"],
        "l3_code": ["850111.SI"], "l3_name": ["种子"],
        "ts_code": ["002041.SZ"], "name": ["登海种业"],
        "in_date": ["20211213"], "out_date": ["20220630"], "is_new": ["N"],
    })
    fetcher = TushareFetcher("fake_token")

    with patch("zer0share.fetcher.time.sleep"):
        df = fetcher.fetch_sw_member()

    assert df.iloc[0]["in_date"] == date(2021, 12, 13)
    assert df.iloc[0]["out_date"] == date(2022, 6, 30)


def test_fetch_ci_member_iterates_l1_codes(mock_pro):
    initial_df = pd.DataFrame({
        "l1_code": ["CI005001.CI", "CI005002.CI"],
        "l1_name": ["农林牧渔", "采掘"],
    })
    mock_pro.ci_index_member.return_value = initial_df
    member_dfs = [
        pd.DataFrame({
            "l1_code": ["CI005001.CI"], "l1_name": ["农林牧渔"],
            "l2_code": ["CI005005.CI"], "l2_name": ["农产品加工"],
            "l3_code": ["CI005006.CI"], "l3_name": ["粮油加工"],
            "ts_code": ["000876.SZ"], "name": ["新 希 望"],
            "in_date": ["20200101"], "out_date": [None], "is_new": ["Y"],
        }),
        pd.DataFrame({
            "l1_code": ["CI005002.CI"], "l1_name": ["采掘"],
            "l2_code": ["CI005010.CI"], "l2_name": ["煤炭开采"],
            "l3_code": ["CI005011.CI"], "l3_name": ["动力煤"],
            "ts_code": ["601088.SH"], "name": ["中国神华"],
            "in_date": ["20200101"], "out_date": [None], "is_new": ["Y"],
        }),
    ]
    mock_pro.ci_index_member.side_effect = [initial_df] + member_dfs
    fetcher = TushareFetcher("fake_token")

    with patch("zer0share.fetcher.time.sleep"):
        df = fetcher.fetch_ci_member()

    assert list(df.columns) == CI_MEMBER_COLS
    assert len(df) == 2
    mock_pro.ci_index_member.assert_any_call(l1_code="CI005001.CI", is_new="")
    mock_pro.ci_index_member.assert_any_call(l1_code="CI005002.CI", is_new="")


def test_fetch_ci_member_converts_dates(mock_pro):
    initial_df = pd.DataFrame({
        "l1_code": ["CI005001.CI"], "l1_name": ["农林牧渔"],
    })
    mock_pro.ci_index_member.side_effect = [
        initial_df,
        pd.DataFrame({
            "l1_code": ["CI005001.CI"], "l1_name": ["农林牧渔"],
            "l2_code": ["CI005005.CI"], "l2_name": ["农产品加工"],
            "l3_code": ["CI005006.CI"], "l3_name": ["粮油加工"],
            "ts_code": ["000876.SZ"], "name": ["新 希 望"],
            "in_date": ["20200101"], "out_date": ["20231231"], "is_new": ["N"],
        }),
    ]
    fetcher = TushareFetcher("fake_token")

    with patch("zer0share.fetcher.time.sleep"):
        df = fetcher.fetch_ci_member()

    assert df.iloc[0]["in_date"] == date(2020, 1, 1)
    assert df.iloc[0]["out_date"] == date(2023, 12, 31)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /data/zer0share && python -m pytest tests/test_fetcher.py -k "sw_classify or sw_member or ci_member" -v`
Expected: FAIL with AttributeError

- [ ] **Step 3: Implement fetcher methods**

Add column definitions to `zer0share/fetcher.py` after the existing `INDEX_WEIGHT_COLS`:

```python
SW_CLASSIFY_COLS = [
    "index_code", "industry_name", "level", "parent_code",
    "industry_code", "is_pub", "src",
]
SW_MEMBER_COLS = [
    "l1_code", "l1_name", "l2_code", "l2_name",
    "l3_code", "l3_name", "ts_code", "name",
    "in_date", "out_date", "is_new",
]
CI_MEMBER_COLS = [
    "l1_code", "l1_name", "l2_code", "l2_name",
    "l3_code", "l3_name", "ts_code", "name",
    "in_date", "out_date", "is_new",
]
```

Add 3 methods to `TushareFetcher` class, after `fetch_trade_cal`:

```python
def fetch_sw_classify(self, src: str = "SW2021") -> pd.DataFrame:
    logger.info(f"拉取申万行业分类: {src}")
    frames = []
    for level in ("L1", "L2", "L3"):
        df = self._pro.index_classify(level=level, src=src)
        if df is not None and not df.empty:
            df["src"] = src
            frames.append(df)
    if not frames:
        return pd.DataFrame(columns=SW_CLASSIFY_COLS)
    result = pd.concat(frames, ignore_index=True)
    return result[SW_CLASSIFY_COLS]


def fetch_sw_member(self) -> pd.DataFrame:
    l1_df = self._pro.index_classify(level="L1", src="SW2021")
    if l1_df is None or l1_df.empty:
        return pd.DataFrame(columns=SW_MEMBER_COLS)
    l1_codes = l1_df["index_code"].tolist()
    logger.info(f"拉取申万行业成分: {len(l1_codes)} 个一级行业")
    frames = []
    for l1_code in l1_codes:
        df = self._pro.index_member_all(l1_code=l1_code, is_new="")
        time.sleep(0.2)
        if df is not None and not df.empty:
            frames.append(df)
    if not frames:
        return pd.DataFrame(columns=SW_MEMBER_COLS)
    result = (
        pd.concat(frames, ignore_index=True)
        .drop_duplicates(subset=["ts_code", "l3_code", "in_date"], keep="last")
        .reset_index(drop=True)
    )
    return _format_industry_dates(result, SW_MEMBER_COLS)


def fetch_ci_member(self) -> pd.DataFrame:
    initial_df = self._pro.ci_index_member()
    if initial_df is None or initial_df.empty:
        return pd.DataFrame(columns=CI_MEMBER_COLS)
    l1_codes = initial_df["l1_code"].unique().tolist()
    logger.info(f"拉取中信行业成分: {len(l1_codes)} 个一级行业")
    frames = []
    for l1_code in l1_codes:
        df = self._pro.ci_index_member(l1_code=l1_code, is_new="")
        time.sleep(0.2)
        if df is not None and not df.empty:
            frames.append(df)
    if not frames:
        return pd.DataFrame(columns=CI_MEMBER_COLS)
    result = (
        pd.concat(frames, ignore_index=True)
        .drop_duplicates(subset=["ts_code", "l3_code", "in_date"], keep="last")
        .reset_index(drop=True)
    )
    return _format_industry_dates(result, CI_MEMBER_COLS)
```

Add helper function at the bottom of the file, after `_format_trade_date`:

```python
def _format_industry_dates(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=columns)
    for col in ("in_date", "out_date"):
        df[col] = pd.to_datetime(df[col], format="%Y%m%d", errors="coerce").apply(
            lambda x: x.date() if not pd.isna(x) and not pd.isnull(x) else None
        )
    return df[columns]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /data/zer0share && python -m pytest tests/test_fetcher.py -k "sw_classify or sw_member or ci_member" -v`
Expected: All 5 tests PASS

- [ ] **Step 5: Commit**

```bash
git add zer0share/fetcher.py tests/test_fetcher.py
git commit -m "feat: add SW and CITIC industry fetch methods"
```

---

### Task 3: Pipeline Layer

**Files:**
- Modify: `zer0share/pipeline.py`
- Test: `tests/test_pipeline.py`

- [ ] **Step 1: Write failing tests for pipeline sync methods**

Add to `tests/test_pipeline.py`:

```python
from zer0share.storage import read_sw_classify, read_sw_member, read_ci_member


def test_sync_industry_writes_sw_classify_and_member(pipeline, cfg):
    classify_df = pd.DataFrame({
        "index_code": ["801010.SI"],
        "industry_name": ["农林牧渔"],
        "level": ["L1"],
        "parent_code": ["0"],
        "industry_code": ["110000"],
        "is_pub": ["1"],
        "src": ["SW2021"],
    })
    member_df = pd.DataFrame({
        "l1_code": ["801010.SI"], "l1_name": ["农林牧渔"],
        "l2_code": ["801016.SI"], "l2_name": ["种植业"],
        "l3_code": ["850111.SI"], "l3_name": ["种子"],
        "ts_code": ["002041.SZ"], "name": ["登海种业"],
        "in_date": [date(2021, 12, 13)], "out_date": [None], "is_new": ["Y"],
    })
    pipeline._fetcher.fetch_sw_classify.return_value = classify_df
    pipeline._fetcher.fetch_sw_member.return_value = member_df

    with patch("zer0share.pipeline.date") as mock_date:
        mock_date.today.return_value = date(2024, 5, 18)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        pipeline.sync_industry()

    assert read_sw_classify(cfg.data_dir).equals(classify_df)
    assert read_sw_member(cfg.data_dir).equals(member_df)
    assert pipeline._meta.get_last_date("sw_classify") == date(2024, 5, 18)
    assert pipeline._meta.get_last_date("sw_member") == date(2024, 5, 18)


def test_sync_industry_failure_sends_alert_and_raises(pipeline):
    pipeline._fetcher.fetch_sw_classify.side_effect = RuntimeError("API error")
    with pytest.raises(RuntimeError):
        pipeline.sync_industry()
    pipeline._notifier.send.assert_called_once()
    msg = pipeline._notifier.send.call_args[0][0]
    assert "industry 同步失败" in msg


def test_sync_ci_member_writes_parquet(pipeline, cfg):
    member_df = pd.DataFrame({
        "l1_code": ["CI005001.CI"], "l1_name": ["农林牧渔"],
        "l2_code": ["CI005005.CI"], "l2_name": ["农产品加工"],
        "l3_code": ["CI005006.CI"], "l3_name": ["粮油加工"],
        "ts_code": ["000876.SZ"], "name": ["新 希 望"],
        "in_date": [date(2020, 1, 1)], "out_date": [None], "is_new": ["Y"],
    })
    pipeline._fetcher.fetch_ci_member.return_value = member_df

    with patch("zer0share.pipeline.date") as mock_date:
        mock_date.today.return_value = date(2024, 5, 18)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        pipeline.sync_ci_member()

    assert read_ci_member(cfg.data_dir).equals(member_df)
    assert pipeline._meta.get_last_date("ci_member") == date(2024, 5, 18)


def test_sync_ci_member_failure_sends_alert_and_raises(pipeline):
    pipeline._fetcher.fetch_ci_member.side_effect = RuntimeError("API error")
    with pytest.raises(RuntimeError):
        pipeline.sync_ci_member()
    pipeline._notifier.send.assert_called_once()
    msg = pipeline._notifier.send.call_args[0][0]
    assert "ci_member 同步失败" in msg
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /data/zer0share && python -m pytest tests/test_pipeline.py -k "sync_industry or sync_ci_member" -v`
Expected: FAIL with AttributeError

- [ ] **Step 3: Implement pipeline methods**

Add imports to `zer0share/pipeline.py` at the top, updating the existing import from storage:

```python
from zer0share.storage import (
    MetaStore,
    adj_factor_partition_exists,
    daily_partition_exists,
    daily_kline_partition_exists,
    index_weight_partition_exists,
    read_trade_cal,
    write_adj_factor,
    write_basic,
    write_daily_partition,
    write_daily_kline,
    write_index_weight,
    write_trade_cal,
    write_sw_classify,
    write_sw_member,
    write_ci_member,
)
```

Add 2 methods to the `Pipeline` class, after `sync_index_weight` and before `_sync_daily_partitioned`:

```python
def sync_industry(self) -> None:
    today = date.today()
    try:
        df = self._fetcher.fetch_sw_classify()
        write_sw_classify(self._cfg.data_dir, df)
        self._meta.update_last_date("sw_classify", today)
        logger.info(f"sw_classify 同步完成: {len(df)} 条")

        df = self._fetcher.fetch_sw_member()
        write_sw_member(self._cfg.data_dir, df)
        self._meta.update_last_date("sw_member", today)
        logger.info(f"sw_member 同步完成: {len(df)} 条")
    except Exception as e:
        logger.error(f"industry 同步失败: {e}")
        self._notifier.send(f"industry 同步失败: {e}")
        raise


def sync_ci_member(self) -> None:
    today = date.today()
    try:
        df = self._fetcher.fetch_ci_member()
        write_ci_member(self._cfg.data_dir, df)
        self._meta.update_last_date("ci_member", today)
        logger.info(f"ci_member 同步完成: {len(df)} 条")
    except Exception as e:
        logger.error(f"ci_member 同步失败: {e}")
        self._notifier.send(f"ci_member 同步失败: {e}")
        raise
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /data/zer0share && python -m pytest tests/test_pipeline.py -k "sync_industry or sync_ci_member" -v`
Expected: All 4 tests PASS

- [ ] **Step 5: Commit**

```bash
git add zer0share/pipeline.py tests/test_pipeline.py
git commit -m "feat: add industry and ci_member sync methods to pipeline"
```

---

### Task 4: CLI Layer

**Files:**
- Modify: `zer0share/cli.py`
- Test: `tests/test_cli.py`

- [ ] **Step 1: Write failing tests for CLI**

Add to `tests/test_cli.py`:

```python
def test_sync_industry_calls_pipeline():
    runner = CliRunner()
    pipeline = MagicMock()
    pipeline.__enter__.return_value = pipeline
    pipeline.__exit__.return_value = False

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(cli, ["sync", "--table", "industry"])

    assert result.exit_code == 0
    pipeline.sync_industry.assert_called_once()


def test_sync_ci_member_calls_pipeline():
    runner = CliRunner()
    pipeline = MagicMock()
    pipeline.__enter__.return_value = pipeline
    pipeline.__exit__.return_value = False

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(cli, ["sync", "--table", "ci_member"])

    assert result.exit_code == 0
    pipeline.sync_ci_member.assert_called_once()


def test_sync_all_includes_industry_and_ci_member():
    runner = CliRunner()
    pipeline = MagicMock()
    pipeline.__enter__.return_value = pipeline
    pipeline.__exit__.return_value = False

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(cli, ["sync", "--all"])

    assert result.exit_code == 0
    pipeline.sync_industry.assert_called_once()
    pipeline.sync_ci_member.assert_called_once()


def test_sync_industry_rejects_date_range():
    runner = CliRunner()
    pipeline = MagicMock()
    pipeline.__enter__.return_value = pipeline
    pipeline.__exit__.return_value = False

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(
            cli, ["sync", "--table", "industry", "--start-date", "2024-01-01"]
        )

    assert result.exit_code != 0
    assert "date range options" in result.output
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /data/zer0share && python -m pytest tests/test_cli.py -k "industry or ci_member" -v`
Expected: FAIL (table choices not in SYNC_TABLES)

- [ ] **Step 3: Implement CLI changes**

In `zer0share/cli.py`, update `SYNC_TABLES`:

```python
SYNC_TABLES = [
    "daily_kline",
    "basic",
    "trade_cal",
    "adj_factor",
    "daily_basic",
    "stock_st",
    "suspend_d",
    "stk_limit",
    "index_weight",
    "industry",
    "ci_member",
]
```

In the `sync` function, update `range_tables` and add the new branches. The `range_tables` set stays as-is (industry/ci_member are not in it, so date range is rejected). Add after the `index_weight` branch:

```python
if sync_all or table == "industry":
    pipeline.sync_industry()
if sync_all or table == "ci_member":
    pipeline.sync_ci_member()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /data/zer0share && python -m pytest tests/test_cli.py -k "industry or ci_member" -v`
Expected: All 4 tests PASS

- [ ] **Step 5: Run full CLI test suite to verify no regressions**

Run: `cd /data/zer0share && python -m pytest tests/test_cli.py -v`
Expected: All tests PASS

- [ ] **Step 6: Commit**

```bash
git add zer0share/cli.py tests/test_cli.py
git commit -m "feat: add industry and ci_member to CLI sync options"
```

---

### Task 5: LocalPro API Layer

**Files:**
- Modify: `zer0share/api.py`
- Test: `tests/test_api.py`

- [ ] **Step 1: Write failing tests for API query methods**

Add to `tests/test_api.py`:

```python
from zer0share.storage import write_sw_classify, write_sw_member, write_ci_member


def test_index_classify_filters_by_level(tmp_path):
    write_sw_classify(tmp_path, pd.DataFrame({
        "index_code": ["801010.SI", "801016.SI"],
        "industry_name": ["农林牧渔", "种植业"],
        "level": ["L1", "L2"],
        "parent_code": ["0", "110000"],
        "industry_code": ["110000", "110100"],
        "is_pub": ["1", "1"],
        "src": ["SW2021", "SW2021"],
    }))
    pro = LocalPro(tmp_path)
    result = pro.index_classify(level="L1")
    assert len(result) == 1
    assert result.iloc[0]["industry_name"] == "农林牧渔"


def test_index_classify_filters_by_src(tmp_path):
    write_sw_classify(tmp_path, pd.DataFrame({
        "index_code": ["801010.SI", "801010.SI"],
        "industry_name": ["农林牧渔", "农林牧渔"],
        "level": ["L1", "L1"],
        "parent_code": ["0", "0"],
        "industry_code": ["110000", "110000"],
        "is_pub": ["1", "1"],
        "src": ["SW2021", "SW2014"],
    }))
    pro = LocalPro(tmp_path)
    result = pro.index_classify(src="SW2014")
    assert len(result) == 1


def test_index_classify_no_filter_returns_all(tmp_path):
    write_sw_classify(tmp_path, pd.DataFrame({
        "index_code": ["801010.SI", "801016.SI"],
        "industry_name": ["农林牧渔", "种植业"],
        "level": ["L1", "L2"],
        "parent_code": ["0", "110000"],
        "industry_code": ["110000", "110100"],
        "is_pub": ["1", "1"],
        "src": ["SW2021", "SW2021"],
    }))
    pro = LocalPro(tmp_path)
    result = pro.index_classify()
    assert len(result) == 2


def test_index_member_all_filters_by_ts_code(tmp_path):
    write_sw_member(tmp_path, pd.DataFrame({
        "l1_code": ["801010.SI", "801030.SI"],
        "l1_name": ["农林牧渔", "化工"],
        "l2_code": ["801016.SI", "801033.SI"],
        "l2_name": ["种植业", "化学原料"],
        "l3_code": ["850111.SI", "850321.SI"],
        "l3_name": ["种子", "纯碱"],
        "ts_code": ["002041.SZ", "600291.SH"],
        "name": ["登海种业", "西水股份"],
        "in_date": [date(2021, 12, 13), date(2021, 12, 13)],
        "out_date": [None, None],
        "is_new": ["Y", "Y"],
    }))
    pro = LocalPro(tmp_path)
    result = pro.index_member_all(ts_code="002041.SZ")
    assert len(result) == 1
    assert result.iloc[0]["ts_code"] == "002041.SZ"


def test_index_member_all_filters_by_is_new(tmp_path):
    write_sw_member(tmp_path, pd.DataFrame({
        "l1_code": ["801010.SI", "801010.SI"],
        "l1_name": ["农林牧渔", "农林牧渔"],
        "l2_code": ["801016.SI", "801016.SI"],
        "l2_name": ["种植业", "种植业"],
        "l3_code": ["850111.SI", "850111.SI"],
        "l3_name": ["种子", "种子"],
        "ts_code": ["002041.SZ", "600313.SH"],
        "name": ["登海种业", "农发种业"],
        "in_date": [date(2021, 12, 13), date(2021, 12, 13)],
        "out_date": [date(2022, 6, 30), None],
        "is_new": ["N", "Y"],
    }))
    pro = LocalPro(tmp_path)
    result = pro.index_member_all(is_new="Y")
    assert len(result) == 1
    assert result.iloc[0]["ts_code"] == "600313.SH"


def test_index_member_all_filters_by_l1_code(tmp_path):
    write_sw_member(tmp_path, pd.DataFrame({
        "l1_code": ["801010.SI", "801030.SI"],
        "l1_name": ["农林牧渔", "化工"],
        "l2_code": ["801016.SI", "801033.SI"],
        "l2_name": ["种植业", "化学原料"],
        "l3_code": ["850111.SI", "850321.SI"],
        "l3_name": ["种子", "纯碱"],
        "ts_code": ["002041.SZ", "600291.SH"],
        "name": ["登海种业", "西水股份"],
        "in_date": [date(2021, 12, 13), date(2021, 12, 13)],
        "out_date": [None, None],
        "is_new": ["Y", "Y"],
    }))
    pro = LocalPro(tmp_path)
    result = pro.index_member_all(l1_code="801010.SI")
    assert len(result) == 1


def test_index_member_all_supports_multi_ts_code(tmp_path):
    write_sw_member(tmp_path, pd.DataFrame({
        "l1_code": ["801010.SI", "801030.SI", "801040.SI"],
        "l1_name": ["农林牧渔", "化工", "钢铁"],
        "l2_code": ["801016.SI", "801033.SI", "801043.SI"],
        "l2_name": ["种植业", "化学原料", "冶钢原料"],
        "l3_code": ["850111.SI", "850321.SI", "850431.SI"],
        "l3_name": ["种子", "纯碱", "铁矿石"],
        "ts_code": ["002041.SZ", "600291.SH", "000002.SZ"],
        "name": ["登海种业", "西水股份", "万科A"],
        "in_date": [date(2021, 12, 13), date(2021, 12, 13), date(2021, 12, 13)],
        "out_date": [None, None, None],
        "is_new": ["Y", "Y", "Y"],
    }))
    pro = LocalPro(tmp_path)
    result = pro.index_member_all(ts_code="002041.SZ,000002.SZ")
    assert len(result) == 2


def test_index_member_all_formats_dates(tmp_path):
    write_sw_member(tmp_path, pd.DataFrame({
        "l1_code": ["801010.SI"],
        "l1_name": ["农林牧渔"],
        "l2_code": ["801016.SI"],
        "l2_name": ["种植业"],
        "l3_code": ["850111.SI"],
        "l3_name": ["种子"],
        "ts_code": ["002041.SZ"],
        "name": ["登海种业"],
        "in_date": [date(2021, 12, 13)],
        "out_date": [date(2022, 6, 30)],
        "is_new": ["N"],
    }))
    pro = LocalPro(tmp_path)
    result = pro.index_member_all(ts_code="002041.SZ")
    assert result.iloc[0]["in_date"] == "20211213"
    assert result.iloc[0]["out_date"] == "20220630"


def test_ci_index_member_filters_by_ts_code(tmp_path):
    write_ci_member(tmp_path, pd.DataFrame({
        "l1_code": ["CI005001.CI", "CI005002.CI"],
        "l1_name": ["农林牧渔", "采掘"],
        "l2_code": ["CI005005.CI", "CI005010.CI"],
        "l2_name": ["农产品加工", "煤炭开采"],
        "l3_code": ["CI005006.CI", "CI005011.CI"],
        "l3_name": ["粮油加工", "动力煤"],
        "ts_code": ["000876.SZ", "601088.SH"],
        "name": ["新 希 望", "中国神华"],
        "in_date": [date(2020, 1, 1), date(2020, 1, 1)],
        "out_date": [None, None],
        "is_new": ["Y", "Y"],
    }))
    pro = LocalPro(tmp_path)
    result = pro.ci_index_member(ts_code="000876.SZ")
    assert len(result) == 1
    assert result.iloc[0]["ts_code"] == "000876.SZ"


def test_ci_index_member_filters_by_is_new(tmp_path):
    write_ci_member(tmp_path, pd.DataFrame({
        "l1_code": ["CI005001.CI", "CI005001.CI"],
        "l1_name": ["农林牧渔", "农林牧渔"],
        "l2_code": ["CI005005.CI", "CI005005.CI"],
        "l2_name": ["农产品加工", "农产品加工"],
        "l3_code": ["CI005006.CI", "CI005006.CI"],
        "l3_name": ["粮油加工", "粮油加工"],
        "ts_code": ["000876.SZ", "000877.SZ"],
        "name": ["新 希 望", "天山股份"],
        "in_date": [date(2020, 1, 1), date(2020, 1, 1)],
        "out_date": [date(2023, 12, 31), None],
        "is_new": ["N", "Y"],
    }))
    pro = LocalPro(tmp_path)
    result = pro.ci_index_member(is_new="Y")
    assert len(result) == 1


def test_ci_index_member_formats_dates(tmp_path):
    write_ci_member(tmp_path, pd.DataFrame({
        "l1_code": ["CI005001.CI"],
        "l1_name": ["农林牧渔"],
        "l2_code": ["CI005005.CI"],
        "l2_name": ["农产品加工"],
        "l3_code": ["CI005006.CI"],
        "l3_name": ["粮油加工"],
        "ts_code": ["000876.SZ"],
        "name": ["新 希 望"],
        "in_date": [date(2020, 1, 1)],
        "out_date": [date(2023, 12, 31)],
        "is_new": ["N"],
    }))
    pro = LocalPro(tmp_path)
    result = pro.ci_index_member(ts_code="000876.SZ")
    assert result.iloc[0]["in_date"] == "20200101"
    assert result.iloc[0]["out_date"] == "20231231"


def test_query_dispatches_index_classify(tmp_path):
    write_sw_classify(tmp_path, pd.DataFrame({
        "index_code": ["801010.SI"],
        "industry_name": ["农林牧渔"],
        "level": ["L1"],
        "parent_code": ["0"],
        "industry_code": ["110000"],
        "is_pub": ["1"],
        "src": ["SW2021"],
    }))
    pro = LocalPro(tmp_path)
    result = pro.query("index_classify")
    assert len(result) == 1


def test_query_dispatches_index_member_all(tmp_path):
    write_sw_member(tmp_path, pd.DataFrame({
        "l1_code": ["801010.SI"], "l1_name": ["农林牧渔"],
        "l2_code": ["801016.SI"], "l2_name": ["种植业"],
        "l3_code": ["850111.SI"], "l3_name": ["种子"],
        "ts_code": ["002041.SZ"], "name": ["登海种业"],
        "in_date": [date(2021, 12, 13)], "out_date": [None], "is_new": ["Y"],
    }))
    pro = LocalPro(tmp_path)
    result = pro.query("index_member_all", ts_code="002041.SZ")
    assert len(result) == 1


def test_query_dispatches_ci_index_member(tmp_path):
    write_ci_member(tmp_path, pd.DataFrame({
        "l1_code": ["CI005001.CI"], "l1_name": ["农林牧渔"],
        "l2_code": ["CI005005.CI"], "l2_name": ["农产品加工"],
        "l3_code": ["CI005006.CI"], "l3_name": ["粮油加工"],
        "ts_code": ["000876.SZ"], "name": ["新 希 望"],
        "in_date": [date(2020, 1, 1)], "out_date": [None], "is_new": ["Y"],
    }))
    pro = LocalPro(tmp_path)
    result = pro.query("ci_index_member", ts_code="000876.SZ")
    assert len(result) == 1


def test_index_classify_raises_file_not_found_with_sync_hint(tmp_path):
    pro = LocalPro(tmp_path)
    with pytest.raises(FileNotFoundError, match="sync --table industry"):
        pro.index_classify()


def test_index_member_all_raises_file_not_found_with_sync_hint(tmp_path):
    pro = LocalPro(tmp_path)
    with pytest.raises(FileNotFoundError, match="sync --table industry"):
        pro.index_member_all()


def test_ci_index_member_raises_file_not_found_with_sync_hint(tmp_path):
    pro = LocalPro(tmp_path)
    with pytest.raises(FileNotFoundError, match="sync --table ci_member"):
        pro.ci_index_member()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /data/zer0share && python -m pytest tests/test_api.py -k "index_classify or index_member_all or ci_index_member" -v`
Expected: FAIL with AttributeError

- [ ] **Step 3: Implement API methods**

Add column imports to `zer0share/api.py` by updating the import from fetcher:

```python
from zer0share.fetcher import (
    ADJ_FACTOR_COLS,
    BASIC_COLS,
    CI_MEMBER_COLS,
    DAILY_BASIC_COLS,
    DAILY_COLS,
    INDEX_WEIGHT_COLS,
    STOCK_ST_COLS,
    STK_LIMIT_COLS,
    SUSPEND_D_COLS,
    SW_CLASSIFY_COLS,
    SW_MEMBER_COLS,
    TRADE_CAL_COLS,
)
```

Add 3 methods to the `LocalPro` class. Add `index_classify` after `universe`:

```python
def index_classify(
    self,
    level: str | None = None,
    src: str | None = None,
    fields: str | list[str] | None = None,
) -> pd.DataFrame:
    path = self._data_dir / "industry" / "sw_classify" / "data.parquet"
    if not path.exists():
        raise FileNotFoundError(
            "sw_classify data not found; run `python main.py sync --table industry` first"
        )
    selected = _parse_fields(fields, SW_CLASSIFY_COLS)
    where = []
    params = []
    if level is not None:
        where.append("level = ?")
        params.append(level)
    if src is not None:
        where.append("src = ?")
        params.append(src)
    sql = f"SELECT {', '.join(selected)} FROM read_parquet(?)"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY industry_code"
    df = duckdb.connect().execute(sql, [str(path), *params]).fetchdf()
    return df
```

Add `index_member_all` after `index_classify`:

```python
def index_member_all(
    self,
    l1_code: str | None = None,
    ts_code: str | None = None,
    is_new: str | None = None,
    fields: str | list[str] | None = None,
) -> pd.DataFrame:
    path = self._data_dir / "industry" / "sw_member" / "data.parquet"
    if not path.exists():
        raise FileNotFoundError(
            "sw_member data not found; run `python main.py sync --table industry` first"
        )
    selected = _parse_fields(fields, SW_MEMBER_COLS)
    where = []
    params = []
    if l1_code is not None:
        where.append("l1_code = ?")
        params.append(l1_code)
    if ts_code is not None:
        codes = [code.strip() for code in ts_code.split(",") if code.strip()]
        placeholders = ", ".join("?" for _ in codes)
        where.append(f"ts_code IN ({placeholders})")
        params.extend(codes)
    if is_new is not None:
        where.append("is_new = ?")
        params.append(is_new)
    sql = f"SELECT {', '.join(selected)} FROM read_parquet(?)"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY ts_code, l1_code"
    df = duckdb.connect().execute(sql, [str(path), *params]).fetchdf()
    return _format_date_columns(df, ["in_date", "out_date"])
```

Add `ci_index_member` after `index_member_all`:

```python
def ci_index_member(
    self,
    l1_code: str | None = None,
    ts_code: str | None = None,
    is_new: str | None = None,
    fields: str | list[str] | None = None,
) -> pd.DataFrame:
    path = self._data_dir / "industry" / "ci_member" / "data.parquet"
    if not path.exists():
        raise FileNotFoundError(
            "ci_member data not found; run `python main.py sync --table ci_member` first"
        )
    selected = _parse_fields(fields, CI_MEMBER_COLS)
    where = []
    params = []
    if l1_code is not None:
        where.append("l1_code = ?")
        params.append(l1_code)
    if ts_code is not None:
        codes = [code.strip() for code in ts_code.split(",") if code.strip()]
        placeholders = ", ".join("?" for _ in codes)
        where.append(f"ts_code IN ({placeholders})")
        params.extend(codes)
    if is_new is not None:
        where.append("is_new = ?")
        params.append(is_new)
    sql = f"SELECT {', '.join(selected)} FROM read_parquet(?)"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY ts_code, l1_code"
    df = duckdb.connect().execute(sql, [str(path), *params]).fetchdf()
    return _format_date_columns(df, ["in_date", "out_date"])
```

Update the `query` dispatch dict:

```python
dispatch = {
    "stock_basic": self.stock_basic,
    "trade_cal": self.trade_cal,
    "daily": self.daily,
    "adj_factor": self.adj_factor,
    "daily_basic": self.daily_basic,
    "stock_st": self.stock_st,
    "suspend_d": self.suspend_d,
    "stk_limit": self.stk_limit,
    "index_weight": self.index_weight,
    "universe": self.universe,
    "pro_bar": self.pro_bar,
    "index_classify": self.index_classify,
    "index_member_all": self.index_member_all,
    "ci_index_member": self.ci_index_member,
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /data/zer0share && python -m pytest tests/test_api.py -k "index_classify or index_member_all or ci_index_member" -v`
Expected: All 17 tests PASS

- [ ] **Step 5: Run full API test suite to verify no regressions**

Run: `cd /data/zer0share && python -m pytest tests/test_api.py -v`
Expected: All tests PASS

- [ ] **Step 6: Commit**

```bash
git add zer0share/api.py tests/test_api.py
git commit -m "feat: add industry query methods to LocalPro API"
```

---

### Task 6: Final Validation

**Files:** All

- [ ] **Step 1: Run full test suite**

Run: `cd /data/zer0share && python -m pytest tests/ -v`
Expected: All tests PASS

- [ ] **Step 2: Verify CLI help shows new table options**

Run: `cd /data/zer0share && python main.py sync --help`
Expected: `--table` choices include `industry` and `ci_member`

- [ ] **Step 3: Verify status command includes new tables**

Run: `cd /data/zer0share && python main.py status`
Expected: Output includes `sw_classify`, `sw_member`, `ci_member` rows showing "从未同步"
