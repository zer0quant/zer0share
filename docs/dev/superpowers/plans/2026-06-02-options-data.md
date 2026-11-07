# Options Data Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 zer0share 中接入 Tushare Pro 期权数据，支持 `opt_basic`（合约基础信息）和 `opt_daily`（日线行情）的增量同步、本地查询和定时调度。

**Architecture:** 完全镜像现有期货模式——`opt_basic` 全量按交易所迭代写入 `data/options/`，`opt_daily` 复用 `_sync_daily_partitioned` 按日分区；API 新增对称的 `opt_basic()` / `opt_daily()` 查询方法；调度器在 `futures_hour + 110/120 min` 触发。

**Tech Stack:** Python 3.11, tushare, pandas, DuckDB, APScheduler, pytest, uv

---

## File Map

| 文件 | 变更类型 | 内容 |
|------|---------|------|
| `zer0share/fetcher.py` | Modify | 新增 `OPTIONS_EXCHANGES`, `OPT_BASIC_COLS`, `OPT_DAILY_COLS`, `fetch_opt_basic`, `fetch_opt_daily` |
| `zer0share/pipeline.py` | Modify | 新增 `sync_opt_basic`, `sync_opt_daily`；import `OPTIONS_EXCHANGES` |
| `zer0share/api.py` | Modify | 新增 `opt_basic`, `opt_daily` 方法；注册进 `query()` dispatch；import 新常量 |
| `zer0share/scheduler.py` | Modify | 新增 `options_tables` block |
| `zer0share/cli.py` | Modify | `SYNC_TABLES`、`range_tables`、`sync` 命令新增期权分支 |
| `tests/test_fetcher.py` | Modify | 新增 8 个期权 fetcher 测试 |
| `tests/test_pipeline.py` | Modify | 新增 6 个期权 pipeline 测试 |
| `tests/test_api.py` | Modify | 新增 6 个期权 API 测试 |
| `tests/test_scheduler.py` | Modify | 更新 job 注册数量断言，新增 options cron 时间校验 |
| `tests/test_cli.py` | Modify | 新增 4 个期权 CLI 测试 |

---

## Task 1: fetcher.py — 常量与 fetch 方法

**Files:**
- Modify: `zer0share/fetcher.py`
- Test: `tests/test_fetcher.py`

- [ ] **Step 1: 写失败测试（常量 + fetch_opt_basic）**

在 `tests/test_fetcher.py` 末尾追加：

```python
# --- Options fetcher tests ---

from zer0share.fetcher import (
    OPTIONS_EXCHANGES, OPT_BASIC_COLS, OPT_DAILY_COLS,
)


def _opt_basic_row(exchange: str = "SSE", list_date: str = "20240101") -> dict:
    return {
        "ts_code": "10004462.SH",
        "symbol": "10004462",
        "exchange": exchange,
        "name": "50ETF购4月2700",
        "per_unit": 10000.0,
        "opt_code": "OP510050",
        "opt_type": "E",
        "call_put": "C",
        "exercise_type": "E",
        "exercise_price": 2.7,
        "s_month": "202404",
        "maturity_date": "20240424",
        "list_date": list_date,
        "delist_date": "20240424",
    }


def _opt_daily_row(ts_code: str = "10004462.SH", trade_date: str = "20240102") -> dict:
    return {
        "ts_code": ts_code,
        "trade_date": trade_date,
        "exchange": "SSE",
        "pre_settle": 0.15,
        "pre_close": 0.148,
        "open": 0.152,
        "high": 0.16,
        "low": 0.148,
        "close": 0.155,
        "settle": 0.154,
        "vol": 5000.0,
        "amount": 7700000.0,
        "oi": 20000.0,
    }


def test_options_exchanges_has_six_entries():
    assert len(OPTIONS_EXCHANGES) == 6
    assert "SSE" in OPTIONS_EXCHANGES
    assert "SZSE" in OPTIONS_EXCHANGES
    assert "CFFEX" in OPTIONS_EXCHANGES
    assert "DCE" in OPTIONS_EXCHANGES
    assert "SHFE" in OPTIONS_EXCHANGES
    assert "CZCE" in OPTIONS_EXCHANGES


def test_fetch_opt_basic_returns_correct_columns(mock_pro):
    mock_pro.opt_basic.return_value = pd.DataFrame([_opt_basic_row()])
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_opt_basic("SSE")

    assert list(df.columns) == OPT_BASIC_COLS
    assert len(df) == 1


def test_fetch_opt_basic_converts_dates(mock_pro):
    mock_pro.opt_basic.return_value = pd.DataFrame([_opt_basic_row()])
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_opt_basic("SSE")

    assert df.iloc[0]["list_date"] == date(2024, 1, 1)
    assert df.iloc[0]["delist_date"] == date(2024, 4, 24)
    assert df.iloc[0]["maturity_date"] == date(2024, 4, 24)


def test_fetch_opt_basic_returns_empty_when_none(mock_pro):
    mock_pro.opt_basic.return_value = None
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_opt_basic("SSE")

    assert df.empty
    assert list(df.columns) == OPT_BASIC_COLS


def test_fetch_opt_basic_calls_api_correctly(mock_pro):
    mock_pro.opt_basic.return_value = pd.DataFrame([_opt_basic_row()])
    fetcher = TushareFetcher("fake_token")

    fetcher.fetch_opt_basic("CFFEX")

    mock_pro.opt_basic.assert_called_once_with(
        exchange="CFFEX",
        fields=",".join(OPT_BASIC_COLS),
    )


def test_fetch_opt_daily_returns_correct_columns(mock_pro):
    mock_pro.opt_daily.return_value = pd.DataFrame([_opt_daily_row()])
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_opt_daily(date(2024, 1, 2))

    assert list(df.columns) == OPT_DAILY_COLS
    assert len(df) == 1
    assert df.iloc[0]["trade_date"] == date(2024, 1, 2)


def test_fetch_opt_daily_returns_empty_when_none(mock_pro):
    mock_pro.opt_daily.return_value = None
    fetcher = TushareFetcher("fake_token")

    df = fetcher.fetch_opt_daily(date(2024, 1, 1))

    assert df.empty
    assert list(df.columns) == OPT_DAILY_COLS


def test_fetch_opt_daily_calls_api_with_date(mock_pro):
    mock_pro.opt_daily.return_value = pd.DataFrame([_opt_daily_row()])
    fetcher = TushareFetcher("fake_token")

    fetcher.fetch_opt_daily(date(2024, 1, 2))

    mock_pro.opt_daily.assert_called_once_with(
        trade_date="20240102",
        fields=",".join(OPT_DAILY_COLS),
    )
```

- [ ] **Step 2: 运行测试，确认失败**

```bash
uv run pytest tests/test_fetcher.py::test_options_exchanges_has_six_entries tests/test_fetcher.py::test_fetch_opt_basic_returns_correct_columns tests/test_fetcher.py::test_fetch_opt_daily_returns_correct_columns -v
```

Expected: FAIL with `ImportError: cannot import name 'OPTIONS_EXCHANGES'`

- [ ] **Step 3: 在 fetcher.py 中新增常量和方法**

在 `FUTURES_EXCHANGES = [...]` 行之后（第 91 行）插入：

```python
OPTIONS_EXCHANGES = ["SSE", "SZSE", "CFFEX", "DCE", "SHFE", "CZCE"]

OPT_BASIC_COLS = [
    "ts_code", "symbol", "exchange", "name", "per_unit",
    "opt_code", "opt_type", "call_put", "exercise_type",
    "exercise_price", "s_month", "maturity_date",
    "list_date", "delist_date",
]

OPT_DAILY_COLS = [
    "ts_code", "trade_date", "exchange",
    "pre_settle", "pre_close",
    "open", "high", "low", "close", "settle",
    "vol", "amount", "oi",
]
```

在 `TushareFetcher` 类末尾（`fetch_fut_weekly_detail` 方法之后，`SW_VERSIONS` 之前）插入：

```python
def fetch_opt_basic(self, exchange: str) -> pd.DataFrame:
    logger.debug(f"拉取期权合约: exchange={exchange}")
    df = self._pro.opt_basic(exchange=exchange, fields=",".join(OPT_BASIC_COLS))
    if df is None or df.empty:
        return pd.DataFrame(columns=OPT_BASIC_COLS)
    for col in ("list_date", "delist_date", "maturity_date"):
        if col in df.columns:
            df[col] = pd.to_datetime(
                df[col], format="%Y%m%d", errors="coerce"
            ).apply(lambda x: x.date() if not pd.isna(x) and not pd.isnull(x) else None)
    return df[OPT_BASIC_COLS]

def fetch_opt_daily(self, trade_date: date) -> pd.DataFrame:
    date_str = trade_date.strftime("%Y%m%d")
    logger.debug(f"拉取期权日线: {date_str}")
    df = self._pro.opt_daily(trade_date=date_str, fields=",".join(OPT_DAILY_COLS))
    return _format_trade_date(df, OPT_DAILY_COLS)
```

- [ ] **Step 4: 运行测试，确认通过**

```bash
uv run pytest tests/test_fetcher.py -k "opt" -v
```

Expected: 8 PASSED

- [ ] **Step 5: 全量测试无回归**

```bash
uv run pytest tests/test_fetcher.py -v
```

Expected: all PASSED

- [ ] **Step 6: Commit**

```bash
git add zer0share/fetcher.py tests/test_fetcher.py
git commit -m "feat: add options fetcher constants and fetch methods"
```

---

## Task 2: pipeline.py — sync_opt_basic + sync_opt_daily

**Files:**
- Modify: `zer0share/pipeline.py`
- Test: `tests/test_pipeline.py`

- [ ] **Step 1: 写失败测试**

在 `tests/test_pipeline.py` 末尾追加：

```python
# --- Options pipeline tests ---

from zer0share.fetcher import OPTIONS_EXCHANGES


def test_sync_opt_basic_writes_to_options_subdir(pipeline, cfg):
    def opt_basic_side_effect(exchange):
        return pd.DataFrame({
            "ts_code": [f"10004462.SH"],
            "symbol": ["10004462"],
            "exchange": [exchange],
            "name": ["50ETF购4月2700"],
            "per_unit": [10000.0],
            "opt_code": ["OP510050"],
            "opt_type": ["E"],
            "call_put": ["C"],
            "exercise_type": ["E"],
            "exercise_price": [2.7],
            "s_month": ["202404"],
            "maturity_date": [date(2024, 4, 24)],
            "list_date": [date(2024, 1, 1)],
            "delist_date": [date(2024, 4, 24)],
        })

    pipeline._fetcher.fetch_opt_basic.side_effect = opt_basic_side_effect

    with patch("zer0share.pipeline.time.sleep"), \
         patch("zer0share.pipeline.date") as mock_date:
        mock_date.today.return_value = date(2024, 1, 2)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        pipeline.sync_opt_basic()

    assert (cfg.data_dir / "options" / "opt_basic" / "date=20240102" / "data.parquet").exists()
    assert pipeline._meta.get_last_date("opt_basic") == date(2024, 1, 2)


def test_sync_opt_basic_calls_all_exchanges(pipeline, cfg):
    pipeline._fetcher.fetch_opt_basic.return_value = pd.DataFrame()

    with patch("zer0share.pipeline.time.sleep"), \
         patch("zer0share.pipeline.date") as mock_date:
        mock_date.today.return_value = date(2024, 1, 2)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        pipeline.sync_opt_basic()

    assert pipeline._fetcher.fetch_opt_basic.call_count == len(OPTIONS_EXCHANGES)
    for exchange in OPTIONS_EXCHANGES:
        pipeline._fetcher.fetch_opt_basic.assert_any_call(exchange)


def test_sync_opt_basic_failure_sends_alert_and_raises(pipeline, cfg):
    pipeline._fetcher.fetch_opt_basic.side_effect = RuntimeError("API error")
    with pytest.raises(RuntimeError):
        pipeline.sync_opt_basic()
    pipeline._notifier.send.assert_called_once()
    msg = pipeline._notifier.send.call_args[0][0]
    assert "opt_basic 同步失败" in msg


def _setup_options_trade_cal(pipeline, cfg):
    trade_cal = pd.DataFrame({
        "exchange": ["SSE"],
        "cal_date": [date(2024, 1, 2)],
        "is_open": [True],
        "pretrade_date": [date(2023, 12, 29)],
    })
    write_trade_cal(cfg.data_dir, "SSE", trade_cal)
    pipeline._meta.load_trade_cal_from_parquet(cfg.data_dir)
    pipeline._meta.update_last_date("trade_cal", date(2024, 1, 2))


def test_sync_opt_daily_writes_to_options_subdir(pipeline, cfg):
    _setup_options_trade_cal(pipeline, cfg)
    opt_df = pd.DataFrame({
        "ts_code": ["10004462.SH"],
        "trade_date": [date(2024, 1, 2)],
        "exchange": ["SSE"],
        "pre_settle": [0.15],
        "pre_close": [0.148],
        "open": [0.152],
        "high": [0.16],
        "low": [0.148],
        "close": [0.155],
        "settle": [0.154],
        "vol": [5000.0],
        "amount": [7700000.0],
        "oi": [20000.0],
    })
    pipeline._fetcher.fetch_opt_daily.return_value = opt_df
    pipeline._meta.update_last_date("opt_daily", date(2024, 1, 1))

    with patch("zer0share.pipeline.date") as mock_date, \
         patch("zer0share.pipeline.time.sleep"):
        mock_date.today.return_value = date(2024, 1, 2)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        pipeline.sync_opt_daily()

    assert (cfg.data_dir / "options" / "opt_daily" / "date=20240102" / "data.parquet").exists()


def test_sync_opt_daily_skips_existing_partitions(pipeline, cfg):
    _setup_options_trade_cal(pipeline, cfg)
    pipeline._fetcher.fetch_opt_daily.return_value = pd.DataFrame()
    pipeline._meta.update_last_date("opt_daily", date(2024, 1, 1))

    write_daily_partition(
        cfg.data_dir / "options", "opt_daily", date(2024, 1, 2),
        pd.DataFrame({"ts_code": ["10004462.SH"], "trade_date": [date(2024, 1, 2)]}),
    )

    with patch("zer0share.pipeline.date") as mock_date, \
         patch("zer0share.pipeline.time.sleep"):
        mock_date.today.return_value = date(2024, 1, 2)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        pipeline.sync_opt_daily()

    pipeline._fetcher.fetch_opt_daily.assert_not_called()


def test_sync_opt_daily_up_to_date(pipeline, cfg):
    pipeline._meta.update_last_date("opt_daily", date.today())
    pipeline.sync_opt_daily()
    pipeline._fetcher.fetch_opt_daily.assert_not_called()
```

- [ ] **Step 2: 运行测试，确认失败**

```bash
uv run pytest tests/test_pipeline.py -k "opt" -v
```

Expected: FAIL with `AttributeError: 'MagicMock' object has no attribute 'fetch_opt_basic'` 或类似错误

- [ ] **Step 3: 修改 pipeline.py**

在 `pipeline.py` 第 9 行的 import 中，把 `FUTURES_EXCHANGES` 改为同时导入 `OPTIONS_EXCHANGES`：

```python
from zer0share.fetcher import TushareFetcher, INDEX_DAILY_CODES, FUTURES_EXCHANGES, OPTIONS_EXCHANGES
```

在 `sync_fut_daily` 方法之后插入：

```python
def sync_opt_basic(self) -> None:
    today = date.today()
    options_dir = self._cfg.data_dir / "options"
    all_frames = []
    try:
        for exchange in OPTIONS_EXCHANGES:
            df = self._fetcher.fetch_opt_basic(exchange)
            time.sleep(0.2)
            if not df.empty:
                all_frames.append(df)
        if all_frames:
            combined = pd.concat(all_frames, ignore_index=True)
        else:
            combined = pd.DataFrame()
        write_daily_partition(options_dir, "opt_basic", today, combined)
        self._meta.update_last_date("opt_basic", today)
        logger.info(f"opt_basic 同步完成: {len(combined)} 条")
    except Exception as e:
        logger.error(f"opt_basic 同步失败: {e}")
        self._notifier.send(f"opt_basic 同步失败: {e}")
        raise

def sync_opt_daily(
    self,
    start_date: date | None = None,
    end_date: date | None = None,
) -> None:
    self._sync_daily_partitioned(
        table_name="opt_daily",
        fetch=self._fetcher.fetch_opt_daily,
        start_date=start_date,
        end_date=end_date,
        data_dir=self._cfg.data_dir / "options",
    )
```

- [ ] **Step 4: 运行测试，确认通过**

```bash
uv run pytest tests/test_pipeline.py -k "opt" -v
```

Expected: 6 PASSED

- [ ] **Step 5: 全量测试无回归**

```bash
uv run pytest tests/test_pipeline.py -v
```

Expected: all PASSED

- [ ] **Step 6: Commit**

```bash
git add zer0share/pipeline.py tests/test_pipeline.py
git commit -m "feat: add options pipeline sync methods"
```

---

## Task 3: api.py — opt_basic + opt_daily 查询方法

**Files:**
- Modify: `zer0share/api.py`
- Test: `tests/test_api.py`

- [ ] **Step 1: 写失败测试**

在 `tests/test_api.py` 末尾追加：

```python
# --- Options API tests ---

from zer0share.fetcher import OPT_BASIC_COLS, OPT_DAILY_COLS


def _write_opt_daily_data(data_dir, trade_date, ts_codes=None):
    ts_codes = ts_codes or ["10004462.SH", "10004463.SH"]
    rows = []
    for ts_code in ts_codes:
        rows.append({
            "ts_code": ts_code,
            "trade_date": trade_date,
            "exchange": "SSE",
            "pre_settle": 0.15,
            "pre_close": 0.148,
            "open": 0.152,
            "high": 0.16,
            "low": 0.148,
            "close": 0.155,
            "settle": 0.154,
            "vol": 5000.0,
            "amount": 7700000.0,
            "oi": 20000.0,
        })
    df = pd.DataFrame(rows)
    write_daily_partition(data_dir / "options", "opt_daily", trade_date, df)
    return df


def test_opt_basic_query_returns_data(tmp_path):
    from datetime import date as dt
    df = pd.DataFrame({
        "ts_code": ["10004462.SH", "10004463.SH"],
        "symbol": ["10004462", "10004463"],
        "exchange": ["SSE", "SSE"],
        "name": ["50ETF购4月2700", "50ETF沽4月2700"],
        "per_unit": [10000.0, 10000.0],
        "opt_code": ["OP510050", "OP510050"],
        "opt_type": ["E", "E"],
        "call_put": ["C", "P"],
        "exercise_type": ["E", "E"],
        "exercise_price": [2.7, 2.7],
        "s_month": ["202404", "202404"],
        "maturity_date": ["20240424", "20240424"],
        "list_date": ["20240101", "20240101"],
        "delist_date": ["20240424", "20240424"],
    })
    write_daily_partition(tmp_path / "options", "opt_basic", dt(2024, 1, 2), df)

    api = LocalPro(tmp_path)
    result = api.opt_basic()
    assert len(result) == 2
    assert set(result["ts_code"]) == {"10004462.SH", "10004463.SH"}


def test_opt_basic_query_filters_by_call_put(tmp_path):
    from datetime import date as dt
    df = pd.DataFrame({
        "ts_code": ["10004462.SH", "10004463.SH"],
        "symbol": ["10004462", "10004463"],
        "exchange": ["SSE", "SSE"],
        "name": ["50ETF购4月2700", "50ETF沽4月2700"],
        "per_unit": [10000.0, 10000.0],
        "opt_code": ["OP510050", "OP510050"],
        "opt_type": ["E", "E"],
        "call_put": ["C", "P"],
        "exercise_type": ["E", "E"],
        "exercise_price": [2.7, 2.7],
        "s_month": ["202404", "202404"],
        "maturity_date": ["20240424", "20240424"],
        "list_date": ["20240101", "20240101"],
        "delist_date": ["20240424", "20240424"],
    })
    write_daily_partition(tmp_path / "options", "opt_basic", dt(2024, 1, 2), df)

    api = LocalPro(tmp_path)
    result = api.opt_basic(call_put="C")
    assert len(result) == 1
    assert result.iloc[0]["ts_code"] == "10004462.SH"


def test_opt_basic_query_raises_when_no_data(tmp_path):
    api = LocalPro(tmp_path)
    with pytest.raises(FileNotFoundError):
        api.opt_basic()


def test_opt_daily_query_returns_data(tmp_path):
    from datetime import date as dt
    _write_opt_daily_data(tmp_path, dt(2024, 1, 2))

    api = LocalPro(tmp_path)
    result = api.opt_daily(trade_date="20240102")
    assert len(result) == 2


def test_opt_daily_query_filters_by_ts_code(tmp_path):
    from datetime import date as dt
    _write_opt_daily_data(tmp_path, dt(2024, 1, 2))

    api = LocalPro(tmp_path)
    result = api.opt_daily(ts_code="10004462.SH", trade_date="20240102")
    assert len(result) == 1
    assert result.iloc[0]["ts_code"] == "10004462.SH"


def test_opt_daily_query_raises_when_no_data(tmp_path):
    api = LocalPro(tmp_path)
    with pytest.raises(FileNotFoundError):
        api.opt_daily(trade_date="20240102")


def test_query_dispatch_supports_options(tmp_path):
    from datetime import date as dt
    _write_opt_daily_data(tmp_path, dt(2024, 1, 2))

    api = LocalPro(tmp_path)
    result = api.query("opt_daily", trade_date="20240102")
    assert len(result) == 2
```

- [ ] **Step 2: 运行测试，确认失败**

```bash
uv run pytest tests/test_api.py -k "opt" -v
```

Expected: FAIL with `AttributeError: 'LocalPro' object has no attribute 'opt_basic'`

- [ ] **Step 3: 修改 api.py**

在 api.py 顶部 import 块（第 8-33 行）中，`FUT_WEEKLY_DETAIL_COLS,` 之后插入：

```python
    OPT_BASIC_COLS,
    OPT_DAILY_COLS,
```

在 `fut_daily` 方法之后（`fut_holding` 方法之前）插入 `opt_basic` 和 `opt_daily` 方法：

```python
def opt_basic(
    self,
    ts_code: str | None = None,
    exchange: str | None = None,
    opt_code: str | None = None,
    call_put: str | None = None,
    fields: str | list[str] | None = None,
) -> pd.DataFrame:
    table_dir = self._data_dir / "options" / "opt_basic"
    if not table_dir.exists():
        raise FileNotFoundError(
            "opt_basic data not found; run `python main.py sync --table opt_basic` first"
        )
    selected = _parse_fields(fields, OPT_BASIC_COLS)
    where = []
    params = []
    if ts_code is not None:
        codes = [code.strip() for code in ts_code.split(",") if code.strip()]
        placeholders = ", ".join("?" for _ in codes)
        where.append(f"ts_code IN ({placeholders})")
        params.extend(codes)
    if exchange is not None:
        where.append("exchange = ?")
        params.append(exchange)
    if opt_code is not None:
        where.append("opt_code = ?")
        params.append(opt_code)
    if call_put is not None:
        where.append("call_put = ?")
        params.append(call_put)
    pattern = table_dir / "date=*" / "data.parquet"
    sql = (
        f"SELECT {', '.join(selected)} "
        "FROM read_parquet(?, hive_partitioning=true, union_by_name=true)"
    )
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY ts_code"
    df = duckdb.connect().execute(sql, [str(pattern), *params]).fetchdf()
    return _format_date_columns(df, ["list_date", "delist_date", "maturity_date"])

def opt_daily(
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
        table_name="opt_daily",
        sync_table="opt_daily",
        columns=OPT_DAILY_COLS,
        ts_code=ts_code,
        trade_date=trade_date,
        start_date=start_date,
        end_date=end_date,
        fields=fields,
        extra_filters=extra or None,
        data_dir_override=self._data_dir / "options",
    )
```

在 `query()` 方法的 dispatch 字典中（`"fut_weekly_detail": self.fut_weekly_detail,` 之后）添加：

```python
            "opt_basic": self.opt_basic,
            "opt_daily": self.opt_daily,
```

- [ ] **Step 4: 运行测试，确认通过**

```bash
uv run pytest tests/test_api.py -k "opt" -v
```

Expected: 7 PASSED

- [ ] **Step 5: 全量测试无回归**

```bash
uv run pytest tests/test_api.py -v
```

Expected: all PASSED

- [ ] **Step 6: Commit**

```bash
git add zer0share/api.py tests/test_api.py
git commit -m "feat: add options query methods to LocalPro API"
```

---

## Task 4: scheduler.py + cli.py — 调度与 CLI 接入

**Files:**
- Modify: `zer0share/scheduler.py`
- Modify: `zer0share/cli.py`
- Test: `tests/test_scheduler.py`
- Test: `tests/test_cli.py`

- [ ] **Step 1: 写失败的 scheduler 测试**

在 `tests/test_scheduler.py` 中，找到断言 `assert len(registered_jobs) == 15` 并改为：

```python
    assert set(registered_jobs) == {
        "daily_kline",
        "index_daily",
        "basic",
        "adj_factor",
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
        "opt_basic",
        "opt_daily",
    }
    assert len(registered_jobs) == 17
```

在现有的 `assert cron_calls[14] == {"hour": 18, "minute": 40}` 之后追加：

```python
    assert cron_calls[15] == {"hour": 18, "minute": 50}   # opt_basic
    assert cron_calls[16] == {"hour": 19, "minute": 0}    # opt_daily
```

- [ ] **Step 2: 写失败的 CLI 测试**

在 `tests/test_cli.py` 末尾追加：

```python
def test_sync_opt_basic_calls_pipeline():
    runner = CliRunner()
    pipeline = MagicMock()
    pipeline.__enter__.return_value = pipeline
    pipeline.__exit__.return_value = False

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(cli, ["sync", "--table", "opt_basic"])

    assert result.exit_code == 0
    pipeline.sync_opt_basic.assert_called_once()


def test_sync_opt_daily_accepts_date_range():
    runner = CliRunner()
    pipeline = MagicMock()
    pipeline.__enter__.return_value = pipeline
    pipeline.__exit__.return_value = False

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(
            cli,
            ["sync", "--table", "opt_daily", "--start-date", "2024-01-01", "--end-date", "2024-01-31"],
        )

    assert result.exit_code == 0
    pipeline.sync_opt_daily.assert_called_once_with(
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 31),
    )


def test_sync_opt_basic_rejects_date_range():
    runner = CliRunner()
    pipeline = MagicMock()
    pipeline.__enter__.return_value = pipeline
    pipeline.__exit__.return_value = False

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(
            cli, ["sync", "--table", "opt_basic", "--start-date", "2024-01-01"]
        )

    assert result.exit_code != 0
    assert "date range options" in result.output


def test_sync_all_includes_options_tables():
    runner = CliRunner()
    pipeline = MagicMock()
    pipeline.__enter__.return_value = pipeline
    pipeline.__exit__.return_value = False

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(cli, ["sync", "--all"])

    assert result.exit_code == 0
    pipeline.sync_opt_basic.assert_called_once()
    pipeline.sync_opt_daily.assert_called_once()
```

- [ ] **Step 3: 运行测试，确认失败**

```bash
uv run pytest tests/test_scheduler.py tests/test_cli.py -k "opt" -v
```

Expected: FAIL（scheduler job 数量断言失败、CLI 调用失败）

- [ ] **Step 4: 修改 scheduler.py**

在 `futures_tables` 的 for 循环结束之后（`logger.info(...)` 之前）插入：

```python
        options_tables = [
            ("opt_basic", pipeline.sync_opt_basic, 110),
            ("opt_daily", pipeline.sync_opt_daily, 120),
        ]
        for job_id, func, offset in options_tables:
            total_min = cfg.scheduler_futures_start_minute + offset
            job_hour = cfg.scheduler_futures_hour + total_min // 60
            job_minute = total_min % 60
            scheduler.add_job(
                func,
                CronTrigger(hour=job_hour, minute=job_minute),
                id=job_id,
            )
```

- [ ] **Step 5: 修改 cli.py**

在 `SYNC_TABLES` 列表中，`"fut_weekly_detail",` 之后添加：

```python
    "opt_basic",
    "opt_daily",
```

在 `range_tables` 集合中，`"fut_weekly_detail",` 之后添加：

```python
        "opt_daily",
```

在 `sync` 命令函数体末尾（期货相关 if 块之后）添加：

```python
    if sync_all or table == "opt_basic":
        pipeline.sync_opt_basic()
    if sync_all or table == "opt_daily":
        pipeline.sync_opt_daily(
            start_date=parsed_start_date,
            end_date=parsed_end_date,
        )
```

- [ ] **Step 6: 运行 scheduler + CLI 测试，确认通过**

```bash
uv run pytest tests/test_scheduler.py tests/test_cli.py -v
```

Expected: all PASSED

- [ ] **Step 7: 全量测试无回归**

```bash
uv run pytest -v
```

Expected: all PASSED

- [ ] **Step 8: Commit**

```bash
git add zer0share/scheduler.py zer0share/cli.py tests/test_scheduler.py tests/test_cli.py
git commit -m "feat: wire options data into scheduler and CLI"
```
