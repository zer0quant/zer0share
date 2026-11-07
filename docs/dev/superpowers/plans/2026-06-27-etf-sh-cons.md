# ETF SH Cons Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add local sync and query support for Tushare `etf_sh_cons`, the Shanghai ETF daily constituent portfolio interface.

**Architecture:** Follow the existing zer0share ETF path: schema constants define columns, `TushareFetcher` fetches paginated upstream data, ETF sync jobs write daily Parquet partitions, catalog specs expose table metadata, and `LocalPro` delegates local queries to `DailyPartitionRepository`. The implementation should mirror `etf_share_size` where possible, with an extra `con_code` query filter and endpoint pagination.

**Tech Stack:** Python 3.11, pandas, click, pytest, Tushare Pro, DuckDB-backed Parquet query layer.

---

## File Structure

- Modify `zer0share/schema.py`: add `ETF_SH_CONS_COLS`.
- Modify `zer0share/fetcher.py`: import `ETF_SH_CONS_COLS` and add paginated `fetch_etf_sh_cons`.
- Modify `zer0share/catalog.py`: add `ETF_SH_CONS_SPEC`.
- Modify `zer0share/sync/etf.py`: add a `DailySyncJob` for `etf_sh_cons`.
- Modify `zer0share/cli.py`: add `etf_sh_cons` to `ETF_TABLES`.
- Modify `zer0share/query/etf.py`: add local query function with `con_code` filter.
- Modify `zer0share/api.py`: expose `LocalPro.etf_sh_cons` and query dispatch.
- Modify `tests/test_fetcher.py`: add fetcher pagination tests.
- Modify `tests/test_pipeline.py`: add daily sync tests and registry assertion.
- Modify `tests/test_cli.py`: add CLI table/date-range tests and update `--etf` order.
- Modify `tests/test_api.py`: add local query and dispatch tests.
- Create `examples/etf/etf_sh_cons_query_smoke.py`: smoke-test local query parameters.
- Modify `README.md`: document sync, local API, storage layout, and command reference.

---

### Task 1: Schema, Catalog, and Fetcher Pagination

**Files:**
- Modify: `zer0share/schema.py`
- Modify: `zer0share/fetcher.py`
- Modify: `zer0share/catalog.py`
- Test: `tests/test_fetcher.py`

- [ ] **Step 1: Add failing fetcher tests**

In `tests/test_fetcher.py`, extend the ETF imports/constants section. Add this constant near `ETF_SHARE_SIZE_COLS`:

```python
ETF_SH_CONS_COLS = [
    "trade_date",
    "ts_code",
    "con_code",
    "con_name",
    "qty",
    "sub_flag",
    "cpr",
    "rdr",
    "sca",
    "exchange",
]
```

Add these tests after the `etf_share_size` tests:

```python
def _etf_sh_cons_row(
    *,
    ts_code: str = "517030.SH",
    con_code: str = "000001.SZ",
    trade_date: str = "20260615",
    con_name: str = "平安银行",
    exchange: str = "SZ",
) -> dict[str, object]:
    return {
        "trade_date": trade_date,
        "ts_code": ts_code,
        "con_code": con_code,
        "con_name": con_name,
        "qty": 1100,
        "sub_flag": "允许",
        "cpr": "15",
        "rdr": "60",
        "sca": "12364.000",
        "exchange": exchange,
    }


def test_fetch_etf_sh_cons_calls_api_with_pagination_fields(mock_pro):
    mock_pro.etf_sh_cons.return_value = pd.DataFrame([_etf_sh_cons_row()])
    fetcher = TushareFetcher("fake_token")

    fetcher.fetch_etf_sh_cons("20260615")

    mock_pro.etf_sh_cons.assert_called_once_with(
        trade_date="20260615",
        fields=",".join(ETF_SH_CONS_COLS),
        limit=3000,
        offset=0,
    )


def test_fetch_etf_sh_cons_combines_paginated_rows(mock_pro):
    first_page = pd.DataFrame(
        [_etf_sh_cons_row(con_code=f"{i:06d}.SH", con_name=f"成分{i}") for i in range(3000)]
    )
    second_page = pd.DataFrame([
        _etf_sh_cons_row(con_code="000001.SZ", con_name="平安银行", exchange="SZ")
    ])
    mock_pro.etf_sh_cons.side_effect = [first_page, second_page]
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_etf_sh_cons("20260615")

    assert list(df.columns) == ETF_SH_CONS_COLS
    assert len(df) == 3001
    assert mock_pro.etf_sh_cons.call_args_list == [
        call(trade_date="20260615", fields=",".join(ETF_SH_CONS_COLS), limit=3000, offset=0),
        call(trade_date="20260615", fields=",".join(ETF_SH_CONS_COLS), limit=3000, offset=3000),
    ]


def test_fetch_etf_sh_cons_returns_empty_when_none(mock_pro):
    mock_pro.etf_sh_cons.return_value = None
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_etf_sh_cons("20260615")

    assert df.empty
    assert list(df.columns) == ETF_SH_CONS_COLS
```

- [ ] **Step 2: Run fetcher tests and verify failure**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 uv run pytest -p no:cacheprovider tests/test_fetcher.py -q
```

Expected: FAIL because `TushareFetcher.fetch_etf_sh_cons` and `ETF_SH_CONS_COLS` are not implemented.

- [ ] **Step 3: Add schema columns**

In `zer0share/schema.py`, add after `ETF_SHARE_SIZE_COLS`:

```python
ETF_SH_CONS_COLS = [
    "trade_date",
    "ts_code",
    "con_code",
    "con_name",
    "qty",
    "sub_flag",
    "cpr",
    "rdr",
    "sca",
    "exchange",
]
```

- [ ] **Step 4: Add catalog spec**

In `zer0share/catalog.py`, import `ETF_SH_CONS_COLS` from `zer0share.schema` in the existing schema import list.

Add after `ETF_SHARE_SIZE_SPEC`:

```python
ETF_SH_CONS_SPEC = DailyTableSpec(
    name="etf_sh_cons",
    path_parts=("etf", "etf_sh_cons"),
    columns=ETF_SH_CONS_COLS,
    parquet_pattern="date=*/data.parquet",
    sync_table="etf_sh_cons",
    order_by="ts_code, trade_date, con_code",
    hive_partitioning=True,
    union_by_name=True,
    first_date="20100101",
)
```

- [ ] **Step 5: Implement paginated fetcher**

In `zer0share/fetcher.py`, add `ETF_SH_CONS_COLS` to the schema import list.

Add after `fetch_etf_share_size`:

```python
    def fetch_etf_sh_cons(self, trade_date: str) -> pd.DataFrame:
        logger.debug(f"拉取上交所ETF持仓组合: {trade_date}")
        frames = []
        limit = 3000
        offset = 0
        while True:
            df = self._pro.etf_sh_cons(
                trade_date=trade_date,
                fields=",".join(ETF_SH_CONS_COLS),
                limit=limit,
                offset=offset,
            )
            if df is None or df.empty:
                break
            frames.append(df)
            if len(df) < limit:
                break
            offset += limit
        combined = (
            pd.concat(frames, ignore_index=True)
            if frames
            else pd.DataFrame(columns=ETF_SH_CONS_COLS)
        )
        return _select_columns_or_empty(combined, ETF_SH_CONS_COLS)
```

- [ ] **Step 6: Run fetcher tests and verify pass**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 uv run pytest -p no:cacheprovider tests/test_fetcher.py -q
```

Expected: PASS.

- [ ] **Step 7: Commit schema/catalog/fetcher**

Run:

```bash
git add zer0share/schema.py zer0share/catalog.py zer0share/fetcher.py tests/test_fetcher.py
git commit -m "feat: fetch etf_sh_cons data"
```

---

### Task 2: Sync Job and CLI Wiring

**Files:**
- Modify: `zer0share/sync/etf.py`
- Modify: `zer0share/cli.py`
- Test: `tests/test_pipeline.py`
- Test: `tests/test_cli.py`

- [ ] **Step 1: Add failing pipeline tests**

In `tests/test_pipeline.py`, add after `_etf_share_size_df` and its tests:

```python
def _etf_sh_cons_df(trade_date: str = "20260615") -> pd.DataFrame:
    return pd.DataFrame({
        "trade_date": [trade_date],
        "ts_code": ["517030.SH"],
        "con_code": ["000001.SZ"],
        "con_name": ["平安银行"],
        "qty": [1100],
        "sub_flag": ["允许"],
        "cpr": ["15"],
        "rdr": ["60"],
        "sca": ["12364.000"],
        "exchange": ["SZ"],
    })


def test_sync_etf_sh_cons_writes_to_etf_subdir(pipeline, cfg, fetcher):
    _setup_trade_cal(pipeline, cfg, "20260615", True)
    fetcher.fetch_etf_sh_cons.return_value = _etf_sh_cons_df()

    pipeline.run("etf_sh_cons")

    assert (cfg.data_dir / "etf" / "etf_sh_cons" / "date=20260615" / "data.parquet").exists()
    assert pipeline._runtime.meta.get_last_date("etf_sh_cons") == "20260615"


def test_sync_etf_sh_cons_fetches_for_trading_day(pipeline, cfg, fetcher):
    _setup_trade_cal(pipeline, cfg, "20260615", True)
    fetcher.fetch_etf_sh_cons.return_value = _etf_sh_cons_df()

    pipeline.run("etf_sh_cons")

    fetcher.fetch_etf_sh_cons.assert_called_once_with("20260615")


def test_pipeline_registry_includes_etf_sh_cons(pipeline):
    assert "etf_sh_cons" in pipeline.registry
```

- [ ] **Step 2: Add failing CLI tests**

In `tests/test_cli.py`, add after `test_sync_etf_share_size_accepts_date_range`:

```python
def test_sync_etf_sh_cons_calls_pipeline():
    runner = CliRunner()
    pipeline = _make_mock_pipeline()

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(cli, ["sync", "--table", "etf_sh_cons"])

    assert result.exit_code == 0
    pipeline.run.assert_called_once_with("etf_sh_cons", start_date=None, end_date=None)


def test_sync_etf_sh_cons_accepts_date_range():
    runner = CliRunner()
    pipeline = _make_mock_pipeline()

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(
            cli,
            [
                "sync",
                "--table",
                "etf_sh_cons",
                "--start-date",
                "20260615",
                "--end-date",
                "20260615",
            ],
        )

    assert result.exit_code == 0
    pipeline.run.assert_called_once_with(
        "etf_sh_cons",
        start_date="20260615",
        end_date="20260615",
    )
```

Update `test_sync_etf_calls_etf_tables` expected order to:

```python
    assert [call.args[0] for call in pipeline.run.call_args_list] == [
        "fund_daily",
        "fund_adj",
        "etf_share_size",
        "etf_sh_cons",
        "etf_basic",
        "etf_index",
    ]
```

- [ ] **Step 3: Run pipeline and CLI tests and verify failure**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 uv run pytest -p no:cacheprovider tests/test_pipeline.py tests/test_cli.py -q
```

Expected: FAIL because the sync registry and CLI choices do not include `etf_sh_cons`.

- [ ] **Step 4: Wire ETF sync job**

In `zer0share/sync/etf.py`, import `ETF_SH_CONS_SPEC`:

```python
from zer0share.catalog import ETF_BASIC_SPEC, ETF_INDEX_SPEC, ETF_SHARE_SIZE_SPEC, ETF_SH_CONS_SPEC, FUND_DAILY_SPEC
```

Add this job after the `etf_share_size` job:

```python
        DailySyncJob(
            table_name=ETF_SH_CONS_SPEC.name,
            spec=ETF_SH_CONS_SPEC,
            fetch=fetcher.fetch_etf_sh_cons,
            store=DailyPartitionStore(etf_dir / "etf_sh_cons"),
        ),
```

- [ ] **Step 5: Wire CLI table list**

In `zer0share/cli.py`, add `"etf_sh_cons"` to `ETF_TABLES` after `"etf_share_size"`:

```python
ETF_TABLES = [
    "fund_daily",
    "fund_adj",
    "etf_share_size",
    "etf_sh_cons",
    "etf_basic",
    "etf_index",
]
```

- [ ] **Step 6: Run pipeline and CLI tests and verify pass**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 uv run pytest -p no:cacheprovider tests/test_pipeline.py tests/test_cli.py -q
```

Expected: PASS.

- [ ] **Step 7: Commit sync and CLI wiring**

Run:

```bash
git add zer0share/sync/etf.py zer0share/cli.py tests/test_pipeline.py tests/test_cli.py
git commit -m "feat: sync etf_sh_cons table"
```

---

### Task 3: Local Query API

**Files:**
- Modify: `zer0share/query/etf.py`
- Modify: `zer0share/api.py`
- Test: `tests/test_api.py`

- [ ] **Step 1: Add failing local API tests**

In `tests/test_api.py`, add after `_make_etf_share_size_df`:

```python
def _make_etf_sh_cons_df():
    return pd.DataFrame(
        {
            "trade_date": ["20260615", "20260615", "20260616", "20260616"],
            "ts_code": ["517030.SH", "517030.SH", "517030.SH", "510300.SH"],
            "con_code": ["000001.SZ", "00003.HK", "000001.SZ", "600519.SH"],
            "con_name": ["平安银行", "香港中华煤气", "平安银行", "贵州茅台"],
            "qty": [1100, 1000, 1200, 200],
            "sub_flag": ["允许", "允许", "允许", "必须"],
            "cpr": ["15", "30", "15", "-"],
            "rdr": ["60", "0", "60", "-"],
            "sca": ["12364.000", "5928.350", "12500.000", "0.000"],
            "exchange": ["SZ", "HK", "SZ", "SH"],
        }
    )
```

Add after the `etf_share_size` API tests:

```python
def test_etf_sh_cons_query_returns_local_data_and_selected_fields(tmp_path):
    data = _make_etf_sh_cons_df()
    DailyPartitionStore(tmp_path / "etf" / "etf_sh_cons").write("20260615", data[data["trade_date"] == "20260615"])
    DailyPartitionStore(tmp_path / "etf" / "etf_sh_cons").write("20260616", data[data["trade_date"] == "20260616"])

    api = LocalPro(tmp_path)
    result = api.etf_sh_cons(fields="trade_date,ts_code,con_code,con_name,qty,exchange")

    assert result.to_dict("records") == [
        {"trade_date": "20260615", "ts_code": "510300.SH", "con_code": "600519.SH", "con_name": "贵州茅台", "qty": 200, "exchange": "SH"},
        {"trade_date": "20260615", "ts_code": "517030.SH", "con_code": "000001.SZ", "con_name": "平安银行", "qty": 1100, "exchange": "SZ"},
        {"trade_date": "20260615", "ts_code": "517030.SH", "con_code": "00003.HK", "con_name": "香港中华煤气", "qty": 1000, "exchange": "HK"},
        {"trade_date": "20260616", "ts_code": "517030.SH", "con_code": "000001.SZ", "con_name": "平安银行", "qty": 1200, "exchange": "SZ"},
    ]


def test_etf_sh_cons_filters_by_ts_code_date_range_and_con_code(tmp_path):
    data = _make_etf_sh_cons_df()
    DailyPartitionStore(tmp_path / "etf" / "etf_sh_cons").write("20260615", data[data["trade_date"] == "20260615"])
    DailyPartitionStore(tmp_path / "etf" / "etf_sh_cons").write("20260616", data[data["trade_date"] == "20260616"])

    api = LocalPro(tmp_path)
    result = api.etf_sh_cons(
        ts_code="517030.SH",
        start_date="20260615",
        end_date="20260616",
        con_code="000001.SZ",
        fields="trade_date,ts_code,con_code,qty",
    )

    assert result.to_dict("records") == [
        {"trade_date": "20260615", "ts_code": "517030.SH", "con_code": "000001.SZ", "qty": 1100},
        {"trade_date": "20260616", "ts_code": "517030.SH", "con_code": "000001.SZ", "qty": 1200},
    ]


def test_etf_sh_cons_supports_trade_date_limit_offset_and_query_dispatch(tmp_path):
    data = _make_etf_sh_cons_df()
    DailyPartitionStore(tmp_path / "etf" / "etf_sh_cons").write("20260615", data[data["trade_date"] == "20260615"])

    api = LocalPro(tmp_path)
    result = api.query(
        "etf_sh_cons",
        trade_date="20260615",
        offset=1,
        limit=1,
        fields="trade_date,ts_code,con_code,exchange",
    )

    assert result.to_dict("records") == [
        {"trade_date": "20260615", "ts_code": "517030.SH", "con_code": "000001.SZ", "exchange": "SZ"}
    ]


def test_etf_sh_cons_validates_date_filters(tmp_path):
    data = _make_etf_sh_cons_df()
    DailyPartitionStore(tmp_path / "etf" / "etf_sh_cons").write("20260615", data[data["trade_date"] == "20260615"])

    api = LocalPro(tmp_path)
    with pytest.raises(ValueError, match="YYYYMMDD"):
        api.etf_sh_cons(trade_date="2026-06-15")


def test_etf_sh_cons_query_raises_when_no_data(tmp_path):
    api = LocalPro(tmp_path)

    with pytest.raises(FileNotFoundError, match="etf_sh_cons"):
        api.etf_sh_cons()
```

- [ ] **Step 2: Run API tests and verify failure**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 uv run pytest -p no:cacheprovider tests/test_api.py -q
```

Expected: FAIL because `LocalPro.etf_sh_cons` is not implemented.

- [ ] **Step 3: Implement query function**

In `zer0share/query/etf.py`, import `ETF_SH_CONS_SPEC` with the other ETF specs.

Add after `etf_share_size`:

```python
def etf_sh_cons(
    ctx: QueryContext,
    ts_code=None,
    trade_date=None,
    con_code=None,
    start_date=None,
    end_date=None,
    limit: int | None = None,
    offset: int | None = None,
    fields=None,
) -> pd.DataFrame:
    """Query Shanghai ETF daily constituent portfolio data."""
    filters = []
    if con_code is not None:
        filters.append(eq_filter("con_code", con_code, ETF_SH_CONS_SPEC.columns))
    return DailyPartitionRepository(ctx, ETF_SH_CONS_SPEC).query(
        ts_code,
        trade_date,
        start_date,
        end_date,
        fields,
        filters=filters,
        limit=limit,
        offset=offset,
    )
```

- [ ] **Step 4: Expose LocalPro method and dispatch**

In `zer0share/api.py`, add after `etf_share_size`:

```python
    def etf_sh_cons(self, **kwargs):
        _check_dates(kwargs)
        return etf.etf_sh_cons(self._ctx, **kwargs)
```

Add this entry to the `dispatch` dict:

```python
            "etf_sh_cons": self.etf_sh_cons,
```

- [ ] **Step 5: Run API tests and verify pass**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 uv run pytest -p no:cacheprovider tests/test_api.py -q
```

Expected: PASS.

- [ ] **Step 6: Commit local API**

Run:

```bash
git add zer0share/query/etf.py zer0share/api.py tests/test_api.py
git commit -m "feat: query etf_sh_cons locally"
```

---

### Task 4: Documentation and Smoke Script

**Files:**
- Create: `examples/etf/etf_sh_cons_query_smoke.py`
- Modify: `README.md`
- Modify: `examples/README.md`

- [ ] **Step 1: Add smoke script**

Create `examples/etf/etf_sh_cons_query_smoke.py`:

```python
from __future__ import annotations

import argparse
import sys

from zer0share import pro_api


FIELDS = "trade_date,ts_code,con_code,con_name,qty,sub_flag,cpr,rdr,sca,exchange"


def _print_frame(title, df, rows: int = 5) -> None:
    print(f"\n## {title}")
    print(f"rows: {len(df)}")
    if df.empty:
        print("(empty)")
        return
    print(df.head(rows).to_string(index=False))


def _pick_sample(pro):
    sample = pro.etf_sh_cons(limit=1, fields=FIELDS)
    if sample.empty:
        raise ValueError(
            "no etf_sh_cons sample found; run `uv run python main.py sync --table etf_sh_cons` first"
        )
    return sample.iloc[0]


def run_smoke(offset: int, limit: int) -> None:
    pro = pro_api()
    sample = _pick_sample(pro)

    ts_code = sample["ts_code"]
    trade_date = sample["trade_date"]
    con_code = sample["con_code"]

    print("Sample values")
    print(f"ts_code={ts_code}")
    print(f"trade_date={trade_date}")
    print(f"con_code={con_code}")
    print(f"offset={offset}")
    print(f"limit={limit}")

    _print_frame(
        "filter_by_ts_code",
        pro.etf_sh_cons(ts_code=ts_code, fields=FIELDS),
    )
    _print_frame(
        "filter_by_trade_date_and_con_code",
        pro.etf_sh_cons(trade_date=trade_date, con_code=con_code, limit=limit, fields=FIELDS),
    )
    _print_frame(
        "offset_and_limit",
        pro.etf_sh_cons(trade_date=trade_date, offset=offset, limit=limit, fields=FIELDS),
    )
    _print_frame(
        "query_dispatch",
        pro.query("etf_sh_cons", ts_code=ts_code, limit=limit, fields=FIELDS),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Smoke-test etf_sh_cons local query parameters against synced Parquet data."
    )
    parser.add_argument("--offset", type=int, default=1)
    parser.add_argument("--limit", type=int, default=5)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        run_smoke(offset=args.offset, limit=args.limit)
    except FileNotFoundError as exc:
        print(f"Missing local data: {exc}", file=sys.stderr)
        print("Run the relevant sync command first:", file=sys.stderr)
        print("  uv run python main.py sync --table etf_sh_cons", file=sys.stderr)
        return 1
    except ValueError as exc:
        print(f"Invalid sample query: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 2: Update README sync section**

In `README.md`, in the ETF sync block, add after `etf_share_size`:

```markdown
uv run python main.py sync --table etf_sh_cons  # 上交所 ETF 每日持仓组合（需积分 >= 8000）
```

- [ ] **Step 3: Update README local API example**

In the local API examples, add after `etf_share_size`:

```python
etf_sh_cons = pro.etf_sh_cons(
    trade_date="20260615",
    ts_code="517030.SH",
    fields="trade_date,ts_code,con_code,con_name,qty,sub_flag,cpr,rdr,sca,exchange",
)
```

- [ ] **Step 4: Update README API table**

In the API table, add after `etf_share_size`:

```markdown
| `etf_sh_cons` | 查询已同步的上交所 ETF 每日持仓组合 |
```

- [ ] **Step 5: Update README storage layout**

In the ETF storage layout, change:

```text
│   └── etf_share_size/
│       └── date=YYYYMMDD/data.parquet
```

to:

```text
│   ├── etf_share_size/
│   │   └── date=YYYYMMDD/data.parquet
│   └── etf_sh_cons/
│       └── date=YYYYMMDD/data.parquet
```

- [ ] **Step 6: Update README command reference**

In the CLI command table, add after `sync --table etf_share_size`:

```markdown
| `sync --table etf_sh_cons` | 同步上交所 ETF 每日持仓组合 |
```

- [ ] **Step 7: Update examples README**

In `examples/README.md`, add `etf_sh_cons_query_smoke.py` alongside the ETF examples:

```markdown
| `etf_sh_cons_query_smoke.py` | `--limit 5` |
```

Add a command example:

```bash
uv run python examples/etf/etf_sh_cons_query_smoke.py
```

- [ ] **Step 8: Run syntax check for smoke script**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 uv run python -m py_compile examples/etf/etf_sh_cons_query_smoke.py
```

Expected: exits with code 0.

- [ ] **Step 9: Commit docs and smoke script**

Run:

```bash
git add README.md examples/README.md examples/etf/etf_sh_cons_query_smoke.py
git commit -m "docs: add etf_sh_cons usage and smoke example"
```

---

### Task 5: Final Verification

**Files:**
- Verify all changed files.

- [ ] **Step 1: Run targeted tests**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 uv run pytest -p no:cacheprovider tests/test_fetcher.py tests/test_pipeline.py tests/test_cli.py tests/test_api.py -q
```

Expected: PASS.

- [ ] **Step 2: Run full test suite**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 uv run pytest -p no:cacheprovider tests -q
```

Expected: PASS.

- [ ] **Step 3: Verify CLI accepts new table**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 uv run python main.py sync --help
```

Expected: Help output includes `etf_sh_cons` in the `--table` choices.

- [ ] **Step 4: Review git status**

Run:

```bash
git status --short
```

Expected: no uncommitted changes.
