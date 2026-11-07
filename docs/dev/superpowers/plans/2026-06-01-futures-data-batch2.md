# Futures Data Batch 2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add 5 extended futures data types (ft_limit, fut_weekly, fut_monthly, fut_index_daily, fut_weekly_detail) to zer0share, fully integrated across all layers.

**Architecture:** Direct extension of existing modules, identical to batch 1. Three sync patterns: standard daily (ft_limit, fut_weekly, fut_monthly via `_sync_daily_partitioned`), full-market daily (fut_index_daily, like existing `sync_index_daily`), and weekly iteration (fut_weekly_detail, new pattern).

**Tech Stack:** Python 3.11+, tushare, duckdb, pyarrow, apscheduler, click, pytest

---

### Task 1: Fetcher — Constants and Fetch Methods

**Files:**
- Modify: `zer0share/fetcher.py`
- Modify: `tests/test_fetcher.py`

- [ ] **Step 1: Add batch 2 column constants to fetcher.py**

Add after `FUT_MAPPING_COLS` (around line 131), before the `TushareFetcher` class:

```python
FT_LIMIT_COLS = [
    "trade_date", "ts_code", "name", "up_limit", "down_limit",
    "m_ratio", "cont", "exchange",
]

FUT_WEEKLY_COLS = [
    "ts_code", "trade_date", "freq", "open", "high", "low",
    "close", "pre_close", "settle", "pre_settle", "vol",
    "amount", "oi", "oi_chg", "exchange", "change1", "change2",
]

FUT_MONTHLY_COLS = FUT_WEEKLY_COLS

FUT_INDEX_DAILY_COLS = [
    "ts_code", "trade_date", "close", "open", "high", "low",
    "pre_close", "change", "pct_chg", "vol", "amount",
]

FUT_WEEKLY_DETAIL_COLS = [
    "exchange", "prd", "name", "vol", "vol_yoy", "amount",
    "amout_yoy", "cumvol", "cumvol_yoy", "cumamt", "cumamt_yoy",
    "open_interest", "interest_wow", "mc_close", "close_wow",
    "week", "week_date",
]
```

- [ ] **Step 2: Add 5 fetch methods to TushareFetcher class**

Add after `fetch_fut_mapping` (around line 306), before `SW_VERSIONS`:

```python
    def fetch_ft_limit(self, trade_date: date) -> pd.DataFrame:
        date_str = trade_date.strftime("%Y%m%d")
        logger.debug(f"拉取期货涨跌停: {date_str}")
        df = self._pro.ft_limit(trade_date=date_str, fields=",".join(FT_LIMIT_COLS))
        return _format_trade_date(df, FT_LIMIT_COLS)

    def fetch_fut_weekly(self, trade_date: date) -> pd.DataFrame:
        date_str = trade_date.strftime("%Y%m%d")
        logger.debug(f"拉取期货周线: {date_str}")
        df = self._pro.fut_weekly_monthly(
            trade_date=date_str, freq="week", fields=",".join(FUT_WEEKLY_COLS),
        )
        return _format_trade_date(df, FUT_WEEKLY_COLS)

    def fetch_fut_monthly(self, trade_date: date) -> pd.DataFrame:
        date_str = trade_date.strftime("%Y%m%d")
        logger.debug(f"拉取期货月线: {date_str}")
        df = self._pro.fut_weekly_monthly(
            trade_date=date_str, freq="month", fields=",".join(FUT_MONTHLY_COLS),
        )
        return _format_trade_date(df, FUT_MONTHLY_COLS)

    def fetch_fut_index_daily(self, trade_date: date) -> pd.DataFrame:
        date_str = trade_date.strftime("%Y%m%d")
        logger.debug(f"拉取南华期货指数: {date_str}")
        df = self._pro.fut_index_daily(
            trade_date=date_str, fields=",".join(FUT_INDEX_DAILY_COLS),
        )
        return _format_trade_date(df, FUT_INDEX_DAILY_COLS)

    def fetch_fut_weekly_detail(self, week: str) -> pd.DataFrame:
        logger.debug(f"拉取期货品种周报: {week}")
        df = self._pro.fut_weekly_detail(
            week=week, fields=",".join(FUT_WEEKLY_DETAIL_COLS),
        )
        if df is None or df.empty:
            return pd.DataFrame(columns=FUT_WEEKLY_DETAIL_COLS)
        if "week_date" in df.columns:
            df["week_date"] = pd.to_datetime(
                df["week_date"], format="%Y%m%d", errors="coerce"
            ).apply(lambda x: x.date() if not pd.isna(x) and not pd.isnull(x) else None)
        return df[FUT_WEEKLY_DETAIL_COLS]
```

- [ ] **Step 3: Add tests at end of `tests/test_fetcher.py`**

```python
# --- Futures batch 2 tests ---

from zer0share.fetcher import (
    FT_LIMIT_COLS, FUT_WEEKLY_COLS, FUT_MONTHLY_COLS,
    FUT_INDEX_DAILY_COLS, FUT_WEEKLY_DETAIL_COLS,
)


def test_fetch_ft_limit_returns_correct_columns(mock_pro):
    mock_pro.ft_limit.return_value = pd.DataFrame({
        "trade_date": ["20240102"],
        "ts_code": ["CU2401.SHF"],
        "name": ["沪铜2401"],
        "up_limit": [51000.0],
        "down_limit": [49000.0],
        "m_ratio": [0.10],
        "cont": ["CU"],
        "exchange": ["SHFE"],
    })
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_ft_limit(date(2024, 1, 2))

    assert list(df.columns) == FT_LIMIT_COLS
    assert len(df) == 1
    assert df.iloc[0]["trade_date"] == date(2024, 1, 2)


def test_fetch_ft_limit_calls_api_correctly(mock_pro):
    mock_pro.ft_limit.return_value = pd.DataFrame({
        "trade_date": ["20240102"],
        "ts_code": ["CU2401.SHF"], "name": ["沪铜2401"],
        "up_limit": [51000.0], "down_limit": [49000.0],
        "m_ratio": [0.10], "cont": ["CU"], "exchange": ["SHFE"],
    })
    fetcher = TushareFetcher("fake_token")

    fetcher.fetch_ft_limit(date(2024, 1, 2))

    mock_pro.ft_limit.assert_called_once_with(
        trade_date="20240102",
        fields=",".join(FT_LIMIT_COLS),
    )


def test_fetch_ft_limit_returns_empty_when_none(mock_pro):
    mock_pro.ft_limit.return_value = None
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_ft_limit(date(2024, 1, 1))

    assert df.empty
    assert list(df.columns) == FT_LIMIT_COLS


def test_fetch_fut_weekly_returns_correct_columns(mock_pro):
    mock_pro.fut_weekly_monthly.return_value = pd.DataFrame({
        "ts_code": ["CU2401.SHF"],
        "trade_date": ["20240102"],
        "freq": ["week"],
        "open": [50000.0], "high": [50500.0], "low": [49900.0],
        "close": [50300.0], "pre_close": [50000.0],
        "settle": [50250.0], "pre_settle": [50100.0],
        "vol": [10000.0], "amount": [251250.0],
        "oi": [50000.0], "oi_chg": [500.0],
        "exchange": ["SHFE"],
        "change1": [200.0], "change2": [150.0],
    })
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_fut_weekly(date(2024, 1, 2))

    assert list(df.columns) == FUT_WEEKLY_COLS
    assert df.iloc[0]["trade_date"] == date(2024, 1, 2)


def test_fetch_fut_weekly_calls_api_with_freq_week(mock_pro):
    mock_pro.fut_weekly_monthly.return_value = pd.DataFrame({
        "ts_code": ["CU2401.SHF"], "trade_date": ["20240102"],
        "freq": ["week"], "open": [50000.0], "high": [50500.0],
        "low": [49900.0], "close": [50300.0], "pre_close": [50000.0],
        "settle": [50250.0], "pre_settle": [50100.0], "vol": [10000.0],
        "amount": [251250.0], "oi": [50000.0], "oi_chg": [500.0],
        "exchange": ["SHFE"], "change1": [200.0], "change2": [150.0],
    })
    fetcher = TushareFetcher("fake_token")

    fetcher.fetch_fut_weekly(date(2024, 1, 2))

    mock_pro.fut_weekly_monthly.assert_called_once_with(
        trade_date="20240102", freq="week", fields=",".join(FUT_WEEKLY_COLS),
    )


def test_fetch_fut_monthly_calls_api_with_freq_month(mock_pro):
    mock_pro.fut_weekly_monthly.return_value = pd.DataFrame({
        "ts_code": ["CU2401.SHF"], "trade_date": ["20240102"],
        "freq": ["month"], "open": [50000.0], "high": [50500.0],
        "low": [49900.0], "close": [50300.0], "pre_close": [50000.0],
        "settle": [50250.0], "pre_settle": [50100.0], "vol": [10000.0],
        "amount": [251250.0], "oi": [50000.0], "oi_chg": [500.0],
        "exchange": ["SHFE"], "change1": [200.0], "change2": [150.0],
    })
    fetcher = TushareFetcher("fake_token")

    fetcher.fetch_fut_monthly(date(2024, 1, 2))

    mock_pro.fut_weekly_monthly.assert_called_once_with(
        trade_date="20240102", freq="month", fields=",".join(FUT_MONTHLY_COLS),
    )


def test_fetch_fut_index_daily_returns_correct_columns(mock_pro):
    mock_pro.fut_index_daily.return_value = pd.DataFrame({
        "ts_code": ["NHAI.NH"],
        "trade_date": ["20240102"],
        "close": [1000.0], "open": [998.0], "high": [1005.0], "low": [995.0],
        "pre_close": [998.0], "change": [2.0], "pct_chg": [0.2],
        "vol": [50000.0], "amount": [50000000.0],
    })
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_fut_index_daily(date(2024, 1, 2))

    assert list(df.columns) == FUT_INDEX_DAILY_COLS
    assert df.iloc[0]["trade_date"] == date(2024, 1, 2)


def test_fetch_fut_index_daily_calls_api_with_trade_date(mock_pro):
    mock_pro.fut_index_daily.return_value = pd.DataFrame({
        "ts_code": ["NHAI.NH"], "trade_date": ["20240102"],
        "close": [1000.0], "open": [998.0], "high": [1005.0], "low": [995.0],
        "pre_close": [998.0], "change": [2.0], "pct_chg": [0.2],
        "vol": [50000.0], "amount": [50000000.0],
    })
    fetcher = TushareFetcher("fake_token")

    fetcher.fetch_fut_index_daily(date(2024, 1, 2))

    mock_pro.fut_index_daily.assert_called_once_with(
        trade_date="20240102", fields=",".join(FUT_INDEX_DAILY_COLS),
    )


def test_fetch_fut_weekly_detail_returns_correct_columns(mock_pro):
    mock_pro.fut_weekly_detail.return_value = pd.DataFrame({
        "exchange": ["SHFE"], "prd": ["CU"], "name": ["沪铜"],
        "vol": [100000], "vol_yoy": [5.0], "amount": [250.0],
        "amout_yoy": [3.0], "cumvol": [5000000], "cumvol_yoy": [4.0],
        "cumamt": [12500.0], "cumamt_yoy": [2.0],
        "open_interest": [200000], "interest_wow": [1.0],
        "mc_close": [50300.0], "close_wow": [0.5],
        "week": ["202401"], "week_date": ["20240105"],
    })
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_fut_weekly_detail("202401")

    assert list(df.columns) == FUT_WEEKLY_DETAIL_COLS
    assert len(df) == 1
    assert df.iloc[0]["week_date"] == date(2024, 1, 5)


def test_fetch_fut_weekly_detail_calls_api_correctly(mock_pro):
    mock_pro.fut_weekly_detail.return_value = pd.DataFrame({
        "exchange": ["SHFE"], "prd": ["CU"], "name": ["沪铜"],
        "vol": [100000], "vol_yoy": [5.0], "amount": [250.0],
        "amout_yoy": [3.0], "cumvol": [5000000], "cumvol_yoy": [4.0],
        "cumamt": [12500.0], "cumamt_yoy": [2.0],
        "open_interest": [200000], "interest_wow": [1.0],
        "mc_close": [50300.0], "close_wow": [0.5],
        "week": ["202401"], "week_date": ["20240105"],
    })
    fetcher = TushareFetcher("fake_token")

    fetcher.fetch_fut_weekly_detail("202401")

    mock_pro.fut_weekly_detail.assert_called_once_with(
        week="202401", fields=",".join(FUT_WEEKLY_DETAIL_COLS),
    )


def test_fetch_fut_weekly_detail_returns_empty_when_none(mock_pro):
    mock_pro.fut_weekly_detail.return_value = None
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_fut_weekly_detail("202401")

    assert df.empty
    assert list(df.columns) == FUT_WEEKLY_DETAIL_COLS
```

- [ ] **Step 4: Run all fetcher tests**

Run: `cd /data/projects/zer0share && python -m pytest tests/test_fetcher.py -v 2>&1 | tail -40`
Expected: All tests PASS (existing + new)

- [ ] **Step 5: Commit**

```bash
git add zer0share/fetcher.py tests/test_fetcher.py
git commit -m "feat: add futures batch 2 fetcher constants and 5 fetch methods"
```

---

### Task 2: Pipeline — 5 Sync Methods

**Files:**
- Modify: `zer0share/pipeline.py`
- Modify: `tests/test_pipeline.py`

- [ ] **Step 1: Add 3 standard daily sync methods**

Add after `sync_fut_mapping` (around line 638), before `_sync_daily_partitioned`:

```python
    def sync_ft_limit(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> None:
        self._sync_daily_partitioned(
            table_name="ft_limit",
            fetch=self._fetcher.fetch_ft_limit,
            start_date=start_date,
            end_date=end_date,
            data_dir=self._cfg.data_dir / "futures",
        )

    def sync_fut_weekly(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> None:
        self._sync_daily_partitioned(
            table_name="fut_weekly",
            fetch=self._fetcher.fetch_fut_weekly,
            start_date=start_date,
            end_date=end_date,
            data_dir=self._cfg.data_dir / "futures",
        )

    def sync_fut_monthly(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> None:
        self._sync_daily_partitioned(
            table_name="fut_monthly",
            fetch=self._fetcher.fetch_fut_monthly,
            start_date=start_date,
            end_date=end_date,
            data_dir=self._cfg.data_dir / "futures",
        )
```

- [ ] **Step 2: Add sync_fut_index_daily (full-market daily pattern)**

Add after the 3 standard methods. This follows the same pattern as `sync_index_daily`:

```python
    def sync_fut_index_daily(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> None:
        today = date.today()
        last = self._meta.get_last_date("fut_index_daily")

        if start_date is None:
            start = (last + timedelta(days=1)) if last else FIRST_DATE
            end = today
        else:
            start = start_date
            end = end_date or today

        if start_date is None and start > end:
            logger.info("fut_index_daily 已是最新，无需同步")
            return
        if start > end:
            raise ValueError("start_date must be on or before end_date")

        logger.info(f"fut_index_daily 同步开始: {start} ~ {end}")

        all_frames = []
        current = start
        while current <= end:
            try:
                df = self._fetcher.fetch_fut_index_daily(current)
                time.sleep(0.2)
                if not df.empty:
                    all_frames.append(df)
            except Exception as e:
                logger.error(f"fut_index_daily {current} 拉取失败: {e}")
                self._notifier.send(f"fut_index_daily {current} 拉取失败: {e}")
            current += timedelta(days=1)

        if not all_frames:
            msg = "fut_index_daily 无数据，跳过"
            logger.info(msg)
            self._notifier.send(msg)
            return

        combined = pd.concat(all_frames, ignore_index=True)
        success = 0
        skipped_existing = 0
        frontier = last
        futures_dir = self._cfg.data_dir / "futures"

        for trade_date, part in combined.groupby("trade_date"):
            if daily_partition_exists(futures_dir, "fut_index_daily", trade_date):
                skipped_existing += 1
                continue
            write_daily_partition(futures_dir, "fut_index_daily", trade_date, part.reset_index(drop=True))
            if frontier is None or trade_date > frontier:
                self._meta.update_last_date("fut_index_daily", trade_date)
                frontier = trade_date
            success += 1

        msg = (
            f"fut_index_daily 同步完成: 成功 {success} 天, "
            f"跳过已存在 {skipped_existing} 天"
        )
        logger.info(msg)
        self._notifier.send(msg)
```

- [ ] **Step 3: Add sync_fut_weekly_detail (weekly iteration pattern)**

Add after `sync_fut_index_daily`:

```python
    def sync_fut_weekly_detail(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> None:
        today = date.today()
        last = self._meta.get_last_date("fut_weekly_detail")

        if start_date is None:
            start = (last + timedelta(days=1)) if last else FIRST_DATE
            end = today
        else:
            start = start_date
            end = end_date or today

        if start > end:
            raise ValueError("start_date must be on or before end_date")

        futures_dir = self._cfg.data_dir / "futures"
        success = 0
        skipped_existing = 0
        frontier = last

        weeks = _week_ranges(start, end)
        logger.info(
            f"fut_weekly_detail 同步开始: {start} ~ {end}, 共 {len(weeks)} 个周"
        )

        for week_num, week_start in weeks:
            try:
                df = self._fetcher.fetch_fut_weekly_detail(week_num)
                time.sleep(0.2)
                if df.empty:
                    continue
                # Use week_start as partition date
                if daily_partition_exists(futures_dir, "fut_weekly_detail", week_start):
                    skipped_existing += 1
                    continue
                write_daily_partition(futures_dir, "fut_weekly_detail", week_start, df)
                if frontier is None or week_start > frontier:
                    self._meta.update_last_date("fut_weekly_detail", week_start)
                    frontier = week_start
                success += 1
            except Exception as e:
                logger.error(f"fut_weekly_detail {week_num} 同步失败: {e}")
                self._notifier.send(f"fut_weekly_detail {week_num} 同步失败: {e}")
                raise

        msg = (
            f"fut_weekly_detail 同步完成: 成功 {success} 周, "
            f"跳过已存在 {skipped_existing} 周, "
            f"共 {len(weeks)} 周"
        )
        logger.info(msg)
        self._notifier.send(msg)
```

- [ ] **Step 4: Add `_week_ranges` helper function**

Add after `_month_ranges` (around line 83):

```python
def _week_ranges(start: date, end: date) -> list[tuple[str, date]]:
    """Generate (week_number, week_start_date) tuples for each ISO week in range.
    Returns list of (week_str like '202401', monday_of_that_week)."""
    weeks = []
    seen = set()
    current = start
    while current <= end:
        iso_year, iso_week, _ = current.isocalendar()
        week_key = (iso_year, iso_week)
        if week_key not in seen:
            seen.add(week_key)
            week_num = f"{iso_year}{iso_week:02d}"
            # Monday of this ISO week
            monday = current - timedelta(days=current.weekday())
            weeks.append((week_num, monday))
        current += timedelta(days=7)
    return weeks
```

- [ ] **Step 5: Add import for `pd` at top of pipeline.py**

`pd` is already imported. Verify it exists: `grep "import pandas" zer0share/pipeline.py`

- [ ] **Step 6: Add pipeline tests at end of `tests/test_pipeline.py`**

```python
# --- Futures batch 2 pipeline tests ---


def test_sync_ft_limit_writes_to_futures_subdir(pipeline, cfg):
    _setup_futures_trade_cal(pipeline, cfg)
    pipeline._fetcher.fetch_ft_limit.return_value = pd.DataFrame({
        "trade_date": [date(2024, 1, 2)],
        "ts_code": ["CU2401.SHF"], "name": ["沪铜2401"],
        "up_limit": [51000.0], "down_limit": [49000.0],
        "m_ratio": [0.10], "cont": ["CU"], "exchange": ["SHFE"],
    })
    pipeline._meta.update_last_date("ft_limit", date(2024, 1, 1))

    with patch("zer0share.pipeline.date") as mock_date, \
         patch("zer0share.pipeline.time.sleep"):
        mock_date.today.return_value = date(2024, 1, 2)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        pipeline.sync_ft_limit()

    assert (cfg.data_dir / "futures" / "ft_limit" / "date=20240102" / "data.parquet").exists()


def test_sync_fut_weekly_writes_to_futures_subdir(pipeline, cfg):
    _setup_futures_trade_cal(pipeline, cfg)
    pipeline._fetcher.fetch_fut_weekly.return_value = pd.DataFrame({
        "ts_code": ["CU2401.SHF"], "trade_date": [date(2024, 1, 2)],
        "freq": ["week"], "open": [50000.0], "high": [50500.0],
        "low": [49900.0], "close": [50300.0], "pre_close": [50000.0],
        "settle": [50250.0], "pre_settle": [50100.0], "vol": [10000.0],
        "amount": [251250.0], "oi": [50000.0], "oi_chg": [500.0],
        "exchange": ["SHFE"], "change1": [200.0], "change2": [150.0],
    })
    pipeline._meta.update_last_date("fut_weekly", date(2024, 1, 1))

    with patch("zer0share.pipeline.date") as mock_date, \
         patch("zer0share.pipeline.time.sleep"):
        mock_date.today.return_value = date(2024, 1, 2)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        pipeline.sync_fut_weekly()

    assert (cfg.data_dir / "futures" / "fut_weekly" / "date=20240102" / "data.parquet").exists()


def test_sync_fut_index_daily_writes_to_futures_subdir(pipeline, cfg):
    pipeline._fetcher.fetch_fut_index_daily.return_value = pd.DataFrame({
        "ts_code": ["NHAI.NH"], "trade_date": [date(2024, 1, 2)],
        "close": [1000.0], "open": [998.0], "high": [1005.0], "low": [995.0],
        "pre_close": [998.0], "change": [2.0], "pct_chg": [0.2],
        "vol": [50000.0], "amount": [50000000.0],
    })
    pipeline._meta.update_last_date("fut_index_daily", date(2024, 1, 1))

    with patch("zer0share.pipeline.date") as mock_date, \
         patch("zer0share.pipeline.time.sleep"):
        mock_date.today.return_value = date(2024, 1, 2)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        pipeline.sync_fut_index_daily()

    assert (cfg.data_dir / "futures" / "fut_index_daily" / "date=20240102" / "data.parquet").exists()


def test_sync_fut_weekly_detail_writes_to_futures_subdir(pipeline, cfg):
    pipeline._fetcher.fetch_fut_weekly_detail.return_value = pd.DataFrame({
        "exchange": ["SHFE"], "prd": ["CU"], "name": ["沪铜"],
        "vol": [100000], "vol_yoy": [5.0], "amount": [250.0],
        "amout_yoy": [3.0], "cumvol": [5000000], "cumvol_yoy": [4.0],
        "cumamt": [12500.0], "cumamt_yoy": [2.0],
        "open_interest": [200000], "interest_wow": [1.0],
        "mc_close": [50300.0], "close_wow": [0.5],
        "week": ["202401"], "week_date": [date(2024, 1, 1)],
    })
    pipeline._meta.update_last_date("fut_weekly_detail", date(2023, 12, 31))

    with patch("zer0share.pipeline.date") as mock_date, \
         patch("zer0share.pipeline.time.sleep"):
        mock_date.today.return_value = date(2024, 1, 7)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        pipeline.sync_fut_weekly_detail()

    # Should have written at least one partition
    futures_dir = cfg.data_dir / "futures" / "fut_weekly_detail"
    if futures_dir.exists():
        partitions = list(futures_dir.iterdir())
        assert len(partitions) >= 1
```

- [ ] **Step 7: Run all pipeline tests**

Run: `cd /data/projects/zer0share && python -m pytest tests/test_pipeline.py -v 2>&1 | tail -40`
Expected: All tests PASS

- [ ] **Step 8: Commit**

```bash
git add zer0share/pipeline.py tests/test_pipeline.py
git commit -m "feat: add futures batch 2 pipeline sync methods"
```

---

### Task 3: API — 5 Query Methods

**Files:**
- Modify: `zer0share/api.py`
- Modify: `tests/test_api.py`

- [ ] **Step 1: Add batch 2 column imports to api.py**

Add to the existing fetcher import block (after `FUT_MAPPING_COLS`):

```python
    FT_LIMIT_COLS,
    FUT_WEEKLY_COLS,
    FUT_MONTHLY_COLS,
    FUT_INDEX_DAILY_COLS,
    FUT_WEEKLY_DETAIL_COLS,
```

- [ ] **Step 2: Add 5 query methods to LocalPro**

Add after the batch 1 futures methods (after `fut_mapping`), before `pro_bar`:

```python
    def ft_limit(
        self,
        ts_code: str | None = None,
        trade_date: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        exchange: str | None = None,
        fields: str | list[str] | None = None,
    ) -> pd.DataFrame:
        extra = {}
        if exchange is not None:
            extra["exchange"] = exchange
        return self._query_daily_partitioned(
            table_name="ft_limit",
            sync_table="ft_limit",
            columns=FT_LIMIT_COLS,
            ts_code=ts_code,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
            fields=fields,
            extra_filters=extra or None,
            data_dir_override=self._data_dir / "futures",
        )

    def fut_weekly(
        self,
        ts_code: str | None = None,
        trade_date: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        exchange: str | None = None,
        fields: str | list[str] | None = None,
    ) -> pd.DataFrame:
        extra = {}
        if exchange is not None:
            extra["exchange"] = exchange
        return self._query_daily_partitioned(
            table_name="fut_weekly",
            sync_table="fut_weekly",
            columns=FUT_WEEKLY_COLS,
            ts_code=ts_code,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
            fields=fields,
            extra_filters=extra or None,
            data_dir_override=self._data_dir / "futures",
        )

    def fut_monthly(
        self,
        ts_code: str | None = None,
        trade_date: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        exchange: str | None = None,
        fields: str | list[str] | None = None,
    ) -> pd.DataFrame:
        extra = {}
        if exchange is not None:
            extra["exchange"] = exchange
        return self._query_daily_partitioned(
            table_name="fut_monthly",
            sync_table="fut_monthly",
            columns=FUT_MONTHLY_COLS,
            ts_code=ts_code,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
            fields=fields,
            extra_filters=extra or None,
            data_dir_override=self._data_dir / "futures",
        )

    def fut_index_daily(
        self,
        ts_code: str | None = None,
        trade_date: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        fields: str | list[str] | None = None,
    ) -> pd.DataFrame:
        return self._query_daily_partitioned(
            table_name="fut_index_daily",
            sync_table="fut_index_daily",
            columns=FUT_INDEX_DAILY_COLS,
            ts_code=ts_code,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
            fields=fields,
            data_dir_override=self._data_dir / "futures",
        )

    def fut_weekly_detail(
        self,
        exchange: str | None = None,
        prd: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        fields: str | list[str] | None = None,
    ) -> pd.DataFrame:
        extra = {}
        if exchange is not None:
            extra["exchange"] = exchange
        if prd is not None:
            extra["prd"] = prd
        return self._query_daily_partitioned(
            table_name="fut_weekly_detail",
            sync_table="fut_weekly_detail",
            columns=FUT_WEEKLY_DETAIL_COLS,
            ts_code=None,
            trade_date=None,
            start_date=start_date,
            end_date=end_date,
            fields=fields,
            extra_filters=extra or None,
            data_dir_override=self._data_dir / "futures",
            order_by="trade_date, exchange, prd",
        )
```

Note: `fut_weekly_detail` has no `trade_date` column in its query parameters — it uses `start_date`/`end_date` on the partition date. The actual data has a `week_date` column that gets stored as the partition date.

- [ ] **Step 3: Add 5 entries to query dispatch**

In the `query` method's `dispatch` dict, add:

```python
            "ft_limit": self.ft_limit,
            "fut_weekly": self.fut_weekly,
            "fut_monthly": self.fut_monthly,
            "fut_index_daily": self.fut_index_daily,
            "fut_weekly_detail": self.fut_weekly_detail,
```

- [ ] **Step 4: Add API tests at end of `tests/test_api.py`**

```python
# --- Futures batch 2 API tests ---


def test_ft_limit_query_returns_data(tmp_path):
    from datetime import date as dt
    api = LocalPro(tmp_path)
    df = pd.DataFrame({
        "trade_date": [dt(2024, 1, 2)],
        "ts_code": ["CU2401.SHF"], "name": ["沪铜2401"],
        "up_limit": [51000.0], "down_limit": [49000.0],
        "m_ratio": [0.10], "cont": ["CU"], "exchange": ["SHFE"],
    })
    write_daily_partition(tmp_path / "futures", "ft_limit", dt(2024, 1, 2), df)

    result = api.ft_limit(trade_date="20240102")
    assert len(result) == 1


def test_fut_weekly_query_returns_data(tmp_path):
    from datetime import date as dt
    api = LocalPro(tmp_path)
    df = pd.DataFrame({
        "ts_code": ["CU2401.SHF"], "trade_date": [dt(2024, 1, 2)],
        "freq": ["week"], "open": [50000.0], "high": [50500.0],
        "low": [49900.0], "close": [50300.0], "pre_close": [50000.0],
        "settle": [50250.0], "pre_settle": [50100.0], "vol": [10000.0],
        "amount": [251250.0], "oi": [50000.0], "oi_chg": [500.0],
        "exchange": ["SHFE"], "change1": [200.0], "change2": [150.0],
    })
    write_daily_partition(tmp_path / "futures", "fut_weekly", dt(2024, 1, 2), df)

    result = api.fut_weekly(trade_date="20240102")
    assert len(result) == 1


def test_fut_monthly_query_returns_data(tmp_path):
    from datetime import date as dt
    api = LocalPro(tmp_path)
    df = pd.DataFrame({
        "ts_code": ["CU2401.SHF"], "trade_date": [dt(2024, 1, 2)],
        "freq": ["month"], "open": [50000.0], "high": [50500.0],
        "low": [49900.0], "close": [50300.0], "pre_close": [50000.0],
        "settle": [50250.0], "pre_settle": [50100.0], "vol": [10000.0],
        "amount": [251250.0], "oi": [50000.0], "oi_chg": [500.0],
        "exchange": ["SHFE"], "change1": [200.0], "change2": [150.0],
    })
    write_daily_partition(tmp_path / "futures", "fut_monthly", dt(2024, 1, 2), df)

    result = api.fut_monthly(trade_date="20240102")
    assert len(result) == 1


def test_fut_index_daily_query_returns_data(tmp_path):
    from datetime import date as dt
    api = LocalPro(tmp_path)
    df = pd.DataFrame({
        "ts_code": ["NHAI.NH", "NHCI.NH"],
        "trade_date": [dt(2024, 1, 2), dt(2024, 1, 2)],
        "close": [1000.0, 800.0], "open": [998.0, 798.0],
        "high": [1005.0, 805.0], "low": [995.0, 795.0],
        "pre_close": [998.0, 798.0], "change": [2.0, 2.0],
        "pct_chg": [0.2, 0.25], "vol": [50000.0, 30000.0],
        "amount": [50000000.0, 24000000.0],
    })
    write_daily_partition(tmp_path / "futures", "fut_index_daily", dt(2024, 1, 2), df)

    result = api.fut_index_daily(trade_date="20240102")
    assert len(result) == 2


def test_fut_weekly_detail_query_returns_data(tmp_path):
    from datetime import date as dt
    api = LocalPro(tmp_path)
    df = pd.DataFrame({
        "exchange": ["SHFE", "DCE"],
        "prd": ["CU", "A"],
        "name": ["沪铜", "豆一"],
        "vol": [100000, 80000], "vol_yoy": [5.0, 3.0],
        "amount": [250.0, 400.0], "amout_yoy": [3.0, 2.0],
        "cumvol": [5000000, 4000000], "cumvol_yoy": [4.0, 3.0],
        "cumamt": [12500.0, 20000.0], "cumamt_yoy": [2.0, 1.0],
        "open_interest": [200000, 150000], "interest_wow": [1.0, -0.5],
        "mc_close": [50300.0, 5000.0], "close_wow": [0.5, -0.3],
        "week": ["202401", "202401"], "week_date": [dt(2024, 1, 1), dt(2024, 1, 1)],
    })
    write_daily_partition(tmp_path / "futures", "fut_weekly_detail", dt(2024, 1, 1), df)

    result = api.fut_weekly_detail()
    assert len(result) == 2


def test_batch2_query_dispatch(tmp_path):
    from datetime import date as dt
    api = LocalPro(tmp_path)
    df = pd.DataFrame({
        "trade_date": [dt(2024, 1, 2)], "ts_code": ["CU2401.SHF"],
        "name": ["沪铜2401"], "up_limit": [51000.0], "down_limit": [49000.0],
        "m_ratio": [0.10], "cont": ["CU"], "exchange": ["SHFE"],
    })
    write_daily_partition(tmp_path / "futures", "ft_limit", dt(2024, 1, 2), df)

    result = api.query("ft_limit", trade_date="20240102")
    assert len(result) == 1
```

- [ ] **Step 5: Run all API tests**

Run: `cd /data/projects/zer0share && python -m pytest tests/test_api.py -v 2>&1 | tail -40`
Expected: All tests PASS

- [ ] **Step 6: Commit**

```bash
git add zer0share/api.py tests/test_api.py
git commit -m "feat: add futures batch 2 query methods to LocalPro API"
```

---

### Task 4: Scheduler — Add 5 Futures Jobs

**Files:**
- Modify: `zer0share/scheduler.py`
- Modify: `tests/test_scheduler.py`

- [ ] **Step 1: Add 5 entries to `futures_tables` list**

In `zer0share/scheduler.py`, append to the `futures_tables` list (after `fut_mapping` at offset 50):

```python
        futures_tables = [
            ("fut_basic", pipeline.sync_fut_basic, 0),
            ("fut_daily", pipeline.sync_fut_daily, 10),
            ("fut_holding", pipeline.sync_fut_holding, 20),
            ("fut_wsr", pipeline.sync_fut_wsr, 30),
            ("fut_settle", pipeline.sync_fut_settle, 40),
            ("fut_mapping", pipeline.sync_fut_mapping, 50),
            ("ft_limit", pipeline.sync_ft_limit, 60),
            ("fut_weekly", pipeline.sync_fut_weekly, 70),
            ("fut_monthly", pipeline.sync_fut_monthly, 80),
            ("fut_index_daily", pipeline.sync_fut_index_daily, 90),
            ("fut_weekly_detail", pipeline.sync_fut_weekly_detail, 100),
        ]
```

- [ ] **Step 2: Update scheduler tests**

Update `tests/test_scheduler.py` to expect 15 total jobs (4 stock + 6 batch1 + 5 batch2) instead of 10.

- [ ] **Step 3: Run tests**

Run: `cd /data/projects/zer0share && python -m pytest tests/test_scheduler.py -v 2>&1 | tail -20`
Expected: All tests PASS

- [ ] **Step 4: Commit**

```bash
git add zer0share/scheduler.py tests/test_scheduler.py
git commit -m "feat: add futures batch 2 to scheduler"
```

---

### Task 5: CLI — Add 5 Tables to Sync Command

**Files:**
- Modify: `zer0share/cli.py`
- Modify: `tests/test_cli.py`

- [ ] **Step 1: Add 5 tables to SYNC_TABLES**

Append to the list in `zer0share/cli.py`:

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
    "fut_basic",
    "fut_daily",
    "fut_holding",
    "fut_wsr",
    "fut_settle",
    "fut_mapping",
    "ft_limit",
    "fut_weekly",
    "fut_monthly",
    "fut_index_daily",
    "fut_weekly_detail",
]
```

Add 5 to `range_tables`:

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
        "fut_daily",
        "fut_holding",
        "fut_wsr",
        "fut_settle",
        "fut_mapping",
        "ft_limit",
        "fut_weekly",
        "fut_monthly",
        "fut_index_daily",
        "fut_weekly_detail",
    }
```

- [ ] **Step 2: Add 5 dispatch blocks**

Add after the batch 1 dispatch blocks:

```python
        if sync_all or table == "ft_limit":
            pipeline.sync_ft_limit(
                start_date=parsed_start_date,
                end_date=parsed_end_date,
            )
        if sync_all or table == "fut_weekly":
            pipeline.sync_fut_weekly(
                start_date=parsed_start_date,
                end_date=parsed_end_date,
            )
        if sync_all or table == "fut_monthly":
            pipeline.sync_fut_monthly(
                start_date=parsed_start_date,
                end_date=parsed_end_date,
            )
        if sync_all or table == "fut_index_daily":
            pipeline.sync_fut_index_daily(
                start_date=parsed_start_date,
                end_date=parsed_end_date,
            )
        if sync_all or table == "fut_weekly_detail":
            pipeline.sync_fut_weekly_detail(
                start_date=parsed_start_date,
                end_date=parsed_end_date,
            )
```

- [ ] **Step 3: Update CLI tests**

Read `tests/test_cli.py` and update any table count checks. Add tests for the new tables if following existing patterns.

- [ ] **Step 4: Run full test suite**

Run: `cd /data/projects/zer0share && python -m pytest -v 2>&1 | tail -50`
Expected: ALL tests pass

- [ ] **Step 5: Verify CLI**

Run: `cd /data/projects/zer0share && python main.py sync --help 2>&1`
Expected: `--table` option includes all 23 tables

- [ ] **Step 6: Commit**

```bash
git add zer0share/cli.py tests/test_cli.py
git commit -m "feat: add futures batch 2 tables to CLI sync command"
```
