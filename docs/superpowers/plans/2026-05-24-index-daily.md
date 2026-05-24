# Index Daily 指数日线行情 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 新增 12 个宽基指数的日线行情同步（`index_daily`），包含 fetcher、pipeline、本地查询 API、CLI、定时调度，存储为按日期分区的 Parquet。

**Architecture:** 按 ts_code 逐个拉取 Tushare `index_daily` 接口（12 次调用），合并后按 trade_date 分组写入 `data/index_daily/date=YYYYMMDD/data.parquet`，复用现有的泛型 `write_daily_partition` / `daily_partition_exists` / `_query_daily_partitioned`，MetaStore 维护单条 `index_daily` last_sync_date。

**Tech Stack:** Python 3.11, tushare, pandas, pyarrow, duckdb, click, apscheduler

**Spec:** `docs/superpowers/specs/2026-05-24-index-daily-design.md`

---

### Task 1: fetcher — fetch_index_daily

**Files:**
- Modify: `zer0share/fetcher.py`
- Test: `tests/test_fetcher.py`

- [ ] **Step 1: Write failing tests**

Add to `tests/test_fetcher.py`:

```python
INDEX_DAILY_COLS = [
    "ts_code", "trade_date", "open", "high", "low",
    "close", "pre_close", "change", "pct_chg", "vol", "amount",
]

def _index_daily_row(ts_code: str = "000300.SH", trade_date: str = "20240102") -> dict:
    return {
        "ts_code": ts_code,
        "trade_date": trade_date,
        "open": 3500.0,
        "high": 3550.0,
        "low": 3480.0,
        "close": 3520.0,
        "pre_close": 3490.0,
        "change": 30.0,
        "pct_chg": 0.86,
        "vol": 50000000.0,
        "amount": 1750000000.0,
    }


def test_fetch_index_daily_returns_correct_columns(mock_pro):
    mock_pro.index_daily.return_value = pd.DataFrame([_index_daily_row()])
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_index_daily("000300.SH", date(2024, 1, 1), date(2024, 1, 31))

    assert list(df.columns) == INDEX_DAILY_COLS
    assert len(df) == 1


def test_fetch_index_daily_calls_api_with_correct_params(mock_pro):
    mock_pro.index_daily.return_value = pd.DataFrame([_index_daily_row()])
    fetcher = TushareFetcher("fake_token")

    fetcher.fetch_index_daily("000300.SH", date(2024, 1, 1), date(2024, 1, 31))

    mock_pro.index_daily.assert_called_once_with(
        ts_code="000300.SH",
        start_date="20240101",
        end_date="20240131",
        fields=",".join(INDEX_DAILY_COLS),
    )


def test_fetch_index_daily_converts_trade_date(mock_pro):
    mock_pro.index_daily.return_value = pd.DataFrame([_index_daily_row()])
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_index_daily("000300.SH", date(2024, 1, 1), date(2024, 1, 31))

    assert df.iloc[0]["trade_date"] == date(2024, 1, 2)


def test_fetch_index_daily_returns_empty_when_none(mock_pro):
    mock_pro.index_daily.return_value = None
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_index_daily("000300.SH", date(2024, 1, 1), date(2024, 1, 31))

    assert df.empty
    assert list(df.columns) == INDEX_DAILY_COLS


def test_fetch_index_daily_returns_empty_when_empty_df(mock_pro):
    mock_pro.index_daily.return_value = pd.DataFrame()
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_index_daily("000300.SH", date(2024, 1, 1), date(2024, 1, 31))

    assert df.empty
```

- [ ] **Step 2: Run to confirm failure**

```bash
cd /data/zer0share
uv run pytest tests/test_fetcher.py::test_fetch_index_daily_returns_correct_columns -v
```

Expected: `FAILED` — `AttributeError: 'TushareFetcher' object has no attribute 'fetch_index_daily'`

- [ ] **Step 3: Implement in fetcher.py**

After the existing column constant block (after `CI_MEMBER_COLS`, before `class TushareFetcher`), add:

```python
INDEX_DAILY_CODES = [
    "000001.SH",  # 上证指数
    "399001.SZ",  # 深证成指
    "000016.SH",  # 上证50
    "000300.SH",  # 沪深300
    "000905.SH",  # 中证500
    "000852.SH",  # 中证1000
    "000985.SH",  # 中证全指
    "399006.SZ",  # 创业板指
    "000688.SH",  # 科创50
    "399005.SZ",  # 中小板指
    "000922.SH",  # 中证红利
    "932000.CSI", # 中证2000（代码待实测确认）
]
INDEX_DAILY_COLS = [
    "ts_code", "trade_date", "open", "high", "low",
    "close", "pre_close", "change", "pct_chg", "vol", "amount",
]
```

Inside `class TushareFetcher`, after `fetch_index_weight`:

```python
def fetch_index_daily(
    self, ts_code: str, start_date: date, end_date: date
) -> pd.DataFrame:
    logger.debug(
        f"拉取指数日线: {ts_code} "
        f"{start_date.strftime('%Y%m%d')}~{end_date.strftime('%Y%m%d')}"
    )
    df = self._pro.index_daily(
        ts_code=ts_code,
        start_date=start_date.strftime("%Y%m%d"),
        end_date=end_date.strftime("%Y%m%d"),
        fields=",".join(INDEX_DAILY_COLS),
    )
    return _format_trade_date(df, INDEX_DAILY_COLS)
```

- [ ] **Step 4: Run all new fetcher tests**

```bash
uv run pytest tests/test_fetcher.py -k "index_daily" -v
```

Expected: all 5 tests `PASSED`

- [ ] **Step 5: Run full test suite to check regressions**

```bash
uv run pytest tests/test_fetcher.py -v
```

Expected: all tests `PASSED`

- [ ] **Step 6: Commit**

```bash
git add zer0share/fetcher.py tests/test_fetcher.py
git commit -m "feat: add INDEX_DAILY_CODES, INDEX_DAILY_COLS and fetch_index_daily to fetcher"
```

---

### Task 2: pipeline — sync_index_daily

**Files:**
- Modify: `zer0share/pipeline.py`
- Test: `tests/test_pipeline.py`

- [ ] **Step 1: Write failing tests**

Add imports at the top of `tests/test_pipeline.py`:

```python
from zer0share.fetcher import INDEX_DAILY_CODES
from zer0share.storage import daily_partition_exists
```

Add test helpers and tests:

```python
def _index_daily_df(ts_code: str = "000300.SH", trade_date: date = date(2024, 1, 2)) -> pd.DataFrame:
    return pd.DataFrame({
        "ts_code": [ts_code],
        "trade_date": [trade_date],
        "open": [3500.0],
        "high": [3550.0],
        "low": [3480.0],
        "close": [3520.0],
        "pre_close": [3490.0],
        "change": [30.0],
        "pct_chg": [0.86],
        "vol": [50000000.0],
        "amount": [1750000000.0],
    })


def test_sync_index_daily_fetches_all_codes(pipeline, cfg):
    pipeline._fetcher.fetch_index_daily.return_value = pd.DataFrame()

    with patch("zer0share.pipeline.date") as mock_date:
        mock_date.today.return_value = date(2024, 1, 2)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        pipeline.sync_index_daily()

    assert pipeline._fetcher.fetch_index_daily.call_count == len(INDEX_DAILY_CODES)
    for ts_code in INDEX_DAILY_CODES:
        pipeline._fetcher.fetch_index_daily.assert_any_call(
            ts_code,
            date(2016, 1, 1),
            date(2024, 1, 2),
        )


def test_sync_index_daily_writes_date_partitions(pipeline, cfg):
    pipeline._fetcher.fetch_index_daily.side_effect = [
        _index_daily_df(ts_code=ts_code, trade_date=date(2024, 1, 2))
        for ts_code in INDEX_DAILY_CODES
    ]

    with patch("zer0share.pipeline.date") as mock_date, \
         patch("zer0share.pipeline.time.sleep"):
        mock_date.today.return_value = date(2024, 1, 2)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        pipeline.sync_index_daily()

    assert daily_partition_exists(cfg.data_dir, "index_daily", date(2024, 1, 2))


def test_sync_index_daily_skips_existing_partitions(pipeline, cfg):
    from zer0share.storage import write_daily_partition
    existing = _index_daily_df(ts_code="000300.SH", trade_date=date(2024, 1, 2))
    write_daily_partition(cfg.data_dir, "index_daily", date(2024, 1, 2), existing)

    pipeline._fetcher.fetch_index_daily.side_effect = [
        _index_daily_df(ts_code=ts_code, trade_date=date(2024, 1, 2))
        for ts_code in INDEX_DAILY_CODES
    ]

    with patch("zer0share.pipeline.date") as mock_date, \
         patch("zer0share.pipeline.time.sleep"):
        mock_date.today.return_value = date(2024, 1, 2)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        pipeline.sync_index_daily()

    # Partition already existed — same file should still be there (written once, not twice)
    assert daily_partition_exists(cfg.data_dir, "index_daily", date(2024, 1, 2))


def test_sync_index_daily_up_to_date_skips_fetch(pipeline, cfg):
    pipeline._meta.update_last_date("index_daily", date(2024, 1, 2))

    with patch("zer0share.pipeline.date") as mock_date:
        mock_date.today.return_value = date(2024, 1, 2)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        pipeline.sync_index_daily()

    pipeline._fetcher.fetch_index_daily.assert_not_called()


def test_sync_index_daily_updates_metastore(pipeline, cfg):
    pipeline._fetcher.fetch_index_daily.side_effect = [
        _index_daily_df(ts_code=ts_code, trade_date=date(2024, 1, 2))
        for ts_code in INDEX_DAILY_CODES
    ]

    with patch("zer0share.pipeline.date") as mock_date, \
         patch("zer0share.pipeline.time.sleep"):
        mock_date.today.return_value = date(2024, 1, 2)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        pipeline.sync_index_daily()

    assert pipeline._meta.get_last_date("index_daily") == date(2024, 1, 2)
```

- [ ] **Step 2: Run to confirm failure**

```bash
uv run pytest tests/test_pipeline.py::test_sync_index_daily_fetches_all_codes -v
```

Expected: `FAILED` — `AttributeError: 'Pipeline' object has no attribute 'sync_index_daily'`

- [ ] **Step 3: Implement sync_index_daily in pipeline.py**

Add `INDEX_DAILY_CODES` to the import from `fetcher`:

```python
from zer0share.fetcher import TushareFetcher, INDEX_DAILY_CODES
```

Add `write_daily_partition` and `daily_partition_exists` to the import from `storage` (they are already imported — verify; if not, add them).

Add the method to `class Pipeline`, after `sync_index_weight` and before `sync_industry`:

```python
def sync_index_daily(
    self,
    start_date: date | None = None,
    end_date: date | None = None,
) -> None:
    today = date.today()
    last = self._meta.get_last_date("index_daily")

    if start_date is None:
        start = (last + timedelta(days=1)) if last else FIRST_DATE
        end = today
    else:
        start = start_date
        end = end_date or today

    if start_date is None and start > end:
        logger.info("index_daily 已是最新，无需同步")
        return
    if start > end:
        raise ValueError("start_date must be on or before end_date")

    logger.info(
        f"index_daily 同步开始: {start} ~ {end}, 共 {len(INDEX_DAILY_CODES)} 个指数"
    )
    all_frames = []
    for ts_code in INDEX_DAILY_CODES:
        try:
            df = self._fetcher.fetch_index_daily(ts_code, start, end)
            time.sleep(0.2)
            if not df.empty:
                all_frames.append(df)
        except Exception as e:
            logger.error(f"index_daily {ts_code} 拉取失败: {e}")
            self._notifier.send(f"index_daily {ts_code} 拉取失败: {e}")
            raise

    if not all_frames:
        logger.info("index_daily 无数据，跳过")
        return

    combined = pd.concat(all_frames, ignore_index=True)
    success = 0
    skipped_existing = 0
    frontier = last

    for trade_date, part in combined.groupby("trade_date"):
        if daily_partition_exists(self._cfg.data_dir, "index_daily", trade_date):
            skipped_existing += 1
            continue
        write_daily_partition(
            self._cfg.data_dir, "index_daily", trade_date, part.reset_index(drop=True)
        )
        if frontier is None or trade_date > frontier:
            self._meta.update_last_date("index_daily", trade_date)
            frontier = trade_date
        success += 1

    msg = (
        f"index_daily 同步完成: 成功 {success} 天, "
        f"跳过已存在 {skipped_existing} 天, 共 {len(INDEX_DAILY_CODES)} 个指数"
    )
    logger.info(msg)
    self._notifier.send(msg)
```

- [ ] **Step 4: Run all new pipeline tests**

```bash
uv run pytest tests/test_pipeline.py -k "index_daily" -v
```

Expected: all 5 tests `PASSED`

- [ ] **Step 5: Run full test suite to check regressions**

```bash
uv run pytest tests/test_pipeline.py -v
```

Expected: all tests `PASSED`

- [ ] **Step 6: Commit**

```bash
git add zer0share/pipeline.py tests/test_pipeline.py
git commit -m "feat: add sync_index_daily to pipeline"
```

---

### Task 3: api — index_daily query method

**Files:**
- Modify: `zer0share/api.py`
- Test: `tests/test_api.py`

- [ ] **Step 1: Write failing tests**

Add import at the top of `tests/test_api.py`:

```python
from zer0share.storage import write_daily_partition
```

Add tests:

```python
def _index_daily_partition(trade_date: date, ts_codes: list[str] | None = None) -> pd.DataFrame:
    codes = ts_codes or ["000300.SH", "000905.SH"]
    return pd.DataFrame({
        "ts_code": codes,
        "trade_date": [trade_date] * len(codes),
        "open": [3500.0] * len(codes),
        "high": [3550.0] * len(codes),
        "low": [3480.0] * len(codes),
        "close": [3520.0] * len(codes),
        "pre_close": [3490.0] * len(codes),
        "change": [30.0] * len(codes),
        "pct_chg": [0.86] * len(codes),
        "vol": [50000000.0] * len(codes),
        "amount": [1750000000.0] * len(codes),
    })


def test_index_daily_returns_all_on_no_filter(tmp_path):
    write_daily_partition(tmp_path, "index_daily", date(2024, 1, 2), _index_daily_partition(date(2024, 1, 2)))
    write_daily_partition(tmp_path, "index_daily", date(2024, 1, 3), _index_daily_partition(date(2024, 1, 3)))

    pro = LocalPro(tmp_path)
    result = pro.index_daily()

    assert len(result) == 4  # 2 dates × 2 codes


def test_index_daily_filters_by_ts_code(tmp_path):
    write_daily_partition(tmp_path, "index_daily", date(2024, 1, 2), _index_daily_partition(date(2024, 1, 2)))

    pro = LocalPro(tmp_path)
    result = pro.index_daily(ts_code="000300.SH")

    assert len(result) == 1
    assert result.iloc[0]["ts_code"] == "000300.SH"


def test_index_daily_filters_by_trade_date(tmp_path):
    write_daily_partition(tmp_path, "index_daily", date(2024, 1, 2), _index_daily_partition(date(2024, 1, 2)))
    write_daily_partition(tmp_path, "index_daily", date(2024, 1, 3), _index_daily_partition(date(2024, 1, 3)))

    pro = LocalPro(tmp_path)
    result = pro.index_daily(trade_date="20240102")

    assert len(result) == 2
    assert all(result["trade_date"] == "20240102")


def test_index_daily_filters_by_date_range(tmp_path):
    write_daily_partition(tmp_path, "index_daily", date(2024, 1, 2), _index_daily_partition(date(2024, 1, 2)))
    write_daily_partition(tmp_path, "index_daily", date(2024, 1, 3), _index_daily_partition(date(2024, 1, 3)))
    write_daily_partition(tmp_path, "index_daily", date(2024, 1, 4), _index_daily_partition(date(2024, 1, 4)))

    pro = LocalPro(tmp_path)
    result = pro.index_daily(start_date="20240102", end_date="20240103")

    assert len(result) == 4  # 2 dates × 2 codes
    dates = sorted(result["trade_date"].unique())
    assert dates == ["20240102", "20240103"]


def test_index_daily_raises_when_data_missing(tmp_path):
    pro = LocalPro(tmp_path)

    with pytest.raises(FileNotFoundError, match="index_daily"):
        pro.index_daily(ts_code="000300.SH")


def test_index_daily_fields_filter(tmp_path):
    write_daily_partition(tmp_path, "index_daily", date(2024, 1, 2), _index_daily_partition(date(2024, 1, 2)))

    pro = LocalPro(tmp_path)
    result = pro.index_daily(ts_code="000300.SH", fields="ts_code,trade_date,close")

    assert list(result.columns) == ["ts_code", "trade_date", "close"]


def test_index_daily_in_query_dispatch(tmp_path):
    write_daily_partition(tmp_path, "index_daily", date(2024, 1, 2), _index_daily_partition(date(2024, 1, 2)))

    pro = LocalPro(tmp_path)
    result = pro.query("index_daily", ts_code="000300.SH")

    assert len(result) == 1
```

- [ ] **Step 2: Run to confirm failure**

```bash
uv run pytest tests/test_api.py::test_index_daily_returns_all_on_no_filter -v
```

Expected: `FAILED` — `AttributeError: 'LocalPro' object has no attribute 'index_daily'`

- [ ] **Step 3: Implement in api.py**

Add `INDEX_DAILY_COLS` to the import from `fetcher` at the top of `api.py`:

```python
from zer0share.fetcher import (
    ADJ_FACTOR_COLS,
    BASIC_COLS,
    CI_MEMBER_COLS,
    DAILY_BASIC_COLS,
    DAILY_COLS,
    INDEX_DAILY_COLS,
    INDEX_WEIGHT_COLS,
    STOCK_ST_COLS,
    STK_LIMIT_COLS,
    SUSPEND_D_COLS,
    SW_CLASSIFY_COLS,
    SW_MEMBER_COLS,
    TRADE_CAL_COLS,
)
```

Add the method to `class LocalPro`, after `stk_limit` and before `index_weight`:

```python
def index_daily(
    self,
    ts_code: str | None = None,
    trade_date: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    fields: str | list[str] | None = None,
) -> pd.DataFrame:
    return self._query_daily_partitioned(
        table_name="index_daily",
        sync_table="index_daily",
        columns=INDEX_DAILY_COLS,
        ts_code=ts_code,
        trade_date=trade_date,
        start_date=start_date,
        end_date=end_date,
        fields=fields,
    )
```

In the `query()` method, add `"index_daily"` to the dispatch dict after `"stk_limit"`:

```python
"index_daily": self.index_daily,
```

- [ ] **Step 4: Run all new API tests**

```bash
uv run pytest tests/test_api.py -k "index_daily" -v
```

Expected: all 7 tests `PASSED`

- [ ] **Step 5: Run full test suite to check regressions**

```bash
uv run pytest tests/test_api.py -v
```

Expected: all tests `PASSED`

- [ ] **Step 6: Commit**

```bash
git add zer0share/api.py tests/test_api.py
git commit -m "feat: add index_daily query method to LocalPro API"
```

---

### Task 4: CLI — wire up sync command

**Files:**
- Modify: `zer0share/cli.py`
- Test: `tests/test_cli.py`

- [ ] **Step 1: Write failing tests**

Add to `tests/test_cli.py`:

```python
def test_sync_index_daily_accepts_date_range():
    runner = CliRunner()
    pipeline = MagicMock()
    pipeline.__enter__.return_value = pipeline
    pipeline.__exit__.return_value = False

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(
            cli,
            [
                "sync",
                "--table",
                "index_daily",
                "--start-date",
                "2024-01-01",
                "--end-date",
                "2024-01-31",
            ],
        )

    assert result.exit_code == 0
    pipeline.sync_index_daily.assert_called_once_with(
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 31),
    )


def test_sync_all_includes_index_daily():
    runner = CliRunner()
    pipeline = MagicMock()
    pipeline.__enter__.return_value = pipeline
    pipeline.__exit__.return_value = False

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(cli, ["sync", "--all"])

    assert result.exit_code == 0
    pipeline.sync_index_daily.assert_called_once()
```

- [ ] **Step 2: Run to confirm failure**

```bash
uv run pytest tests/test_cli.py::test_sync_index_daily_accepts_date_range -v
```

Expected: `FAILED` — `SystemExit` or `BadParameter: invalid choice: index_daily`

- [ ] **Step 3: Implement in cli.py**

In `SYNC_TABLES`, add `"index_daily"` after `"index_weight"`:

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
    "index_daily",
    "industry",
    "ci_member",
]
```

In the `sync` command, add `"index_daily"` to `range_tables`:

```python
range_tables = {
    "daily_kline",
    "adj_factor",
    "daily_basic",
    "stock_st",
    "suspend_d",
    "stk_limit",
    "index_weight",
    "index_daily",
}
```

After the `index_weight` block and before `industry`, add:

```python
        if sync_all or table == "index_daily":
            pipeline.sync_index_daily(
                start_date=parsed_start_date,
                end_date=parsed_end_date,
            )
```

- [ ] **Step 4: Run all new CLI tests**

```bash
uv run pytest tests/test_cli.py -k "index_daily" -v
```

Expected: all 2 tests `PASSED`

- [ ] **Step 5: Run full test suite to check regressions**

```bash
uv run pytest tests/test_cli.py -v
```

Expected: all tests `PASSED`

- [ ] **Step 6: Commit**

```bash
git add zer0share/cli.py tests/test_cli.py
git commit -m "feat: add index_daily to CLI sync command"
```

---

### Task 5: Scheduler — add index_daily job

**Files:**
- Modify: `zer0share/scheduler.py`

- [ ] **Step 1: Add index_daily job to scheduler**

In `zer0share/scheduler.py`, after the `daily_kline` job and before the `basic` job, add:

```python
        scheduler.add_job(
            pipeline.sync_index_daily,
            CronTrigger(
                hour=cfg.scheduler_daily_kline_hour,
                minute=cfg.scheduler_daily_kline_minute,
            ),
            id="index_daily",
        )
```

Update the `logger.info` message to include `index_daily`:

```python
        logger.info(
            f"调度器启动: daily_kline + index_daily 每天 "
            f"{cfg.scheduler_daily_kline_hour}:{cfg.scheduler_daily_kline_minute:02d}, "
            f"adj_factor 每天 "
            f"{cfg.scheduler_adj_factor_hour}:{cfg.scheduler_adj_factor_minute:02d}, "
            f"basic 每天 {cfg.scheduler_basic_hour}:00"
        )
```

- [ ] **Step 2: Run full test suite**

```bash
uv run pytest -v
```

Expected: all tests `PASSED`

- [ ] **Step 3: Commit**

```bash
git add zer0share/scheduler.py
git commit -m "feat: add index_daily to daily scheduler"
```

---

### Task 6: README update

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Add index_daily to the API table**

In the "支持的本地查询方法" table, add a row after `index_weight`:

```markdown
| `index_daily` | 查询已同步的宽基指数日线行情（12个指数） |
```

- [ ] **Step 2: Add CLI command entry**

In the CLI 命令表, add after `index_weight`:

```markdown
| `sync --table index_daily` | 增量同步12个宽基指数日线行情 |
```

- [ ] **Step 3: Add usage example**

In the 本地查询 API 代码块, add after `index_weight` example:

```python
# 指数日线行情（用于对冲基准收益率）
idx_daily = pro.index_daily(ts_code="000300.SH", start_date="20240101", end_date="20240131")
```

- [ ] **Step 4: Add index_daily to sync --all sequence**

In the 首次同步 section, add after `index_weight`:

```bash
uv run python main.py sync --table index_daily  # 宽基指数日线行情
```

- [ ] **Step 5: Add data structure entry**

In the 数据存储结构 code block, add after `index_weight/`:

```
├── index_daily/
│   ├── date=20160104/data.parquet   # 含当日全部12个宽基指数
│   ├── date=20160105/data.parquet
│   └── ...
```

- [ ] **Step 6: Commit**

```bash
git add README.md
git commit -m "docs: update README with index_daily support"
```

---

## Implementation Notes

**932000.CSI 代码验证：** 中证2000在Tushare中的实际代码需要在有真实 token 的环境下用 `pro.index_daily(ts_code="932000.CSI", start_date="20230101", end_date="20230110")` 实测确认。若返回空或报错，尝试 `932000.SH`。在 `INDEX_DAILY_CODES` 中修改对应条目即可，其他代码不受影响。
