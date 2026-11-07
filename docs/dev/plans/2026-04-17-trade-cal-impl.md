# 交易日历模块 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 新增交易日历同步功能，并改造 sync_daily_kline 只对真实交易日调用 API，跳过周末和节假日。

**Architecture:** TushareFetcher 新增 fetch_trade_cal；storage.py 新增 write_trade_cal/read_trade_cal；MetaStore 新增 load_trade_cal_from_parquet 和 get_trading_days；Pipeline 新增 sync_trade_cal 并改造 sync_daily_kline；CLI 的 sync --table 选项加入 trade_cal，sync --all 顺序改为 trade_cal → basic → daily_kline。

**Tech Stack:** Python 3.11+, tushare, duckdb, pyarrow, pandas, click

---

## Task 1: storage.py — 新增 trade_cal Parquet 读写 + MetaStore 方法

**Files:**
- Modify: `src/storage.py`
- Modify: `tests/test_storage.py`

**背景：**
`src/storage.py` 已有 `write_basic`/`read_basic`/`write_daily_kline`/`read_daily_kline` 函数和 `MetaStore` 类。
本任务在此基础上追加：
1. `write_trade_cal(data_dir, exchange, df)` — 写 `data/trade_cal/exchange=XXX/data.parquet`
2. `read_trade_cal(data_dir, exchange)` — 读取，不存在返回空 DataFrame
3. `MetaStore._init_schema` 中创建 `trade_cal` DuckDB 表
4. `MetaStore.load_trade_cal_from_parquet(data_dir)` — 从所有分区 Parquet 加载到 DuckDB
5. `MetaStore.get_trading_days(exchange, start, end)` — 查询交易日列表

**Step 1: 在 tests/test_storage.py 末尾追加测试**

```python
from src.storage import write_trade_cal, read_trade_cal


def test_write_and_read_trade_cal(tmp_path):
    df = pd.DataFrame({
        "exchange": ["SSE", "SSE"],
        "cal_date": [date(2024, 1, 2), date(2024, 1, 3)],
        "is_open": [True, False],
        "pretrade_date": [date(2023, 12, 29), date(2024, 1, 2)],
    })
    write_trade_cal(tmp_path, "SSE", df)
    result = read_trade_cal(tmp_path, "SSE")
    assert len(result) == 2
    assert (tmp_path / "trade_cal" / "exchange=SSE" / "data.parquet").exists()


def test_read_trade_cal_returns_empty_if_not_exists(tmp_path):
    result = read_trade_cal(tmp_path, "SSE")
    assert result.empty


def test_load_trade_cal_from_parquet(tmp_path):
    db_path = tmp_path / "meta.duckdb"
    df = pd.DataFrame({
        "exchange": ["SSE", "SSE"],
        "cal_date": [date(2024, 1, 2), date(2024, 1, 3)],
        "is_open": [True, False],
        "pretrade_date": [date(2023, 12, 29), date(2024, 1, 2)],
    })
    write_trade_cal(tmp_path, "SSE", df)
    with MetaStore(db_path) as store:
        store.load_trade_cal_from_parquet(tmp_path)
        row = store._conn.execute(
            "SELECT COUNT(*) FROM trade_cal WHERE exchange='SSE'"
        ).fetchone()
        assert row[0] == 2


def test_get_trading_days(tmp_path):
    db_path = tmp_path / "meta.duckdb"
    df = pd.DataFrame({
        "exchange": ["SSE"] * 5,
        "cal_date": [
            date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4),
            date(2024, 1, 5), date(2024, 1, 6),
        ],
        "is_open": [True, False, True, False, True],
        "pretrade_date": [
            date(2023, 12, 29), date(2024, 1, 2), date(2024, 1, 2),
            date(2024, 1, 4), date(2024, 1, 4),
        ],
    })
    write_trade_cal(tmp_path, "SSE", df)
    with MetaStore(db_path) as store:
        store.load_trade_cal_from_parquet(tmp_path)
        days = store.get_trading_days("SSE", date(2024, 1, 1), date(2024, 1, 6))
    assert days == [date(2024, 1, 2), date(2024, 1, 4), date(2024, 1, 6)]


def test_get_trading_days_returns_empty_when_no_cal(tmp_path):
    db_path = tmp_path / "meta.duckdb"
    with MetaStore(db_path) as store:
        days = store.get_trading_days("SSE", date(2024, 1, 1), date(2024, 1, 6))
    assert days == []
```

**Step 2: 运行测试确认失败**

```bash
cd D:/Project/zer0share && uv run pytest tests/test_storage.py::test_write_and_read_trade_cal -v
```

Expected: FAIL with "cannot import name 'write_trade_cal'"

**Step 3: 在 src/storage.py 中追加函数和修改 MetaStore**

在 `_init_schema` 方法中追加 trade_cal 表创建：

```python
def _init_schema(self):
    self._conn.execute("""
        CREATE TABLE IF NOT EXISTS sync_meta (
            table_name  VARCHAR PRIMARY KEY,
            last_date   DATE,
            updated_at  TIMESTAMP
        )
    """)
    self._conn.execute("""
        CREATE TABLE IF NOT EXISTS trade_cal (
            exchange      VARCHAR,
            cal_date      DATE,
            is_open       BOOLEAN,
            pretrade_date DATE,
            PRIMARY KEY (exchange, cal_date)
        )
    """)
```

在 `close()` 方法前追加两个新方法：

```python
def load_trade_cal_from_parquet(self, data_dir: Path) -> None:
    self._conn.execute("DELETE FROM trade_cal")
    trade_cal_dir = data_dir / "trade_cal"
    if not trade_cal_dir.exists():
        return
    for exchange_dir in sorted(trade_cal_dir.iterdir()):
        parquet_path = exchange_dir / "data.parquet"
        if not parquet_path.exists():
            continue
        self._conn.execute(
            "INSERT INTO trade_cal SELECT * FROM read_parquet(?)",
            [str(parquet_path)]
        )

def get_trading_days(
    self, exchange: str, start: date, end: date
) -> list[date]:
    rows = self._conn.execute(
        """
        SELECT cal_date FROM trade_cal
        WHERE exchange = ?
          AND cal_date >= ?
          AND cal_date <= ?
          AND is_open = TRUE
        ORDER BY cal_date
        """,
        [exchange, start, end]
    ).fetchall()
    return [row[0] for row in rows]
```

在文件末尾追加两个 Parquet 函数：

```python
def write_trade_cal(data_dir: Path, exchange: str, df: pd.DataFrame) -> None:
    partition_dir = data_dir / "trade_cal" / f"exchange={exchange}"
    partition_dir.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, partition_dir / "data.parquet")


def read_trade_cal(data_dir: Path, exchange: str) -> pd.DataFrame:
    path = data_dir / "trade_cal" / f"exchange={exchange}" / "data.parquet"
    if not path.exists():
        return pd.DataFrame()
    return pq.read_table(path).to_pandas()
```

**Step 4: 运行全量 storage 测试**

```bash
cd D:/Project/zer0share && uv run pytest tests/test_storage.py -v
```

Expected: 全部 PASS（约 16 个测试）

**Step 5: Commit**

```bash
cd D:/Project/zer0share
git add src/storage.py tests/test_storage.py
git commit -m "feat: add trade_cal parquet read/write and MetaStore methods"
```

---

## Task 2: fetcher.py — 新增 fetch_trade_cal

**Files:**
- Modify: `src/fetcher.py`
- Modify: `tests/test_fetcher.py`

**背景：**
`src/fetcher.py` 已有 `TushareFetcher` 类，含 `fetch_basic` 和 `fetch_daily_kline`。
本任务追加 `fetch_trade_cal(exchange)` 方法。

**Step 1: 在 tests/test_fetcher.py 末尾追加测试**

```python
def test_fetch_trade_cal_returns_correct_columns(mock_pro):
    mock_pro.trade_cal.return_value = pd.DataFrame({
        "exchange": ["SSE", "SSE"],
        "cal_date": ["20240102", "20240103"],
        "is_open": ["1", "0"],
        "pretrade_date": ["20231229", "20240102"],
    })
    fetcher = TushareFetcher("fake_token")
    df = fetcher.fetch_trade_cal("SSE")
    assert list(df.columns) == ["exchange", "cal_date", "is_open", "pretrade_date"]
    assert len(df) == 2


def test_fetch_trade_cal_converts_types(mock_pro):
    mock_pro.trade_cal.return_value = pd.DataFrame({
        "exchange": ["SSE", "SSE"],
        "cal_date": ["20240102", "20240103"],
        "is_open": ["1", "0"],
        "pretrade_date": ["20231229", "20240102"],
    })
    fetcher = TushareFetcher("fake_token")
    df = fetcher.fetch_trade_cal("SSE")
    assert df.iloc[0]["cal_date"] == date(2024, 1, 2)
    assert df.iloc[0]["is_open"] is True
    assert df.iloc[1]["is_open"] is False
    assert df.iloc[0]["pretrade_date"] == date(2023, 12, 29)
```

**Step 2: 运行测试确认失败**

```bash
cd D:/Project/zer0share && uv run pytest tests/test_fetcher.py::test_fetch_trade_cal_returns_correct_columns -v
```

Expected: FAIL with "TushareFetcher has no attribute fetch_trade_cal"

**Step 3: 在 src/fetcher.py 中追加常量和方法**

在 `DAILY_COLS` 常量后追加：

```python
TRADE_CAL_COLS = ["exchange", "cal_date", "is_open", "pretrade_date"]
```

在 `fetch_daily_kline` 方法后追加：

```python
def fetch_trade_cal(self, exchange: str) -> pd.DataFrame:
    today = date.today().strftime("%Y%m%d")
    logger.info(f"拉取交易日历: {exchange}")
    df = self._pro.trade_cal(
        exchange=exchange,
        start_date="19900101",
        end_date=today,
        fields=",".join(TRADE_CAL_COLS),
    )
    if df is None or df.empty:
        return pd.DataFrame(columns=TRADE_CAL_COLS)
    df["cal_date"] = pd.to_datetime(
        df["cal_date"], format="%Y%m%d", errors="coerce"
    ).dt.date
    df["pretrade_date"] = pd.to_datetime(
        df["pretrade_date"], format="%Y%m%d", errors="coerce"
    ).apply(lambda x: x.date() if not pd.isnull(x) else None)
    df["is_open"] = df["is_open"].astype(str).map({"1": True, "0": False})
    return df[TRADE_CAL_COLS]
```

**Step 4: 运行全量 fetcher 测试**

```bash
cd D:/Project/zer0share && uv run pytest tests/test_fetcher.py -v
```

Expected: 全部 PASS（约 7 个测试）

**Step 5: Commit**

```bash
cd D:/Project/zer0share
git add src/fetcher.py tests/test_fetcher.py
git commit -m "feat: add fetch_trade_cal to TushareFetcher"
```

---

## Task 3: pipeline.py — 新增 sync_trade_cal + 改造 sync_daily_kline

**Files:**
- Modify: `src/pipeline.py`
- Modify: `tests/test_pipeline.py`

**背景：**
`src/pipeline.py` 已有 `Pipeline` 类，含 `sync_basic` 和 `sync_daily_kline`。
本任务：
1. 新增 `sync_trade_cal()` 方法
2. 改造 `sync_daily_kline()` 使用 DuckDB 交易日历过滤

当前 `sync_daily_kline` 逐日遍历 last_date+1 到 today。
改造后：查询 SSE 交易日列表，只对交易日调用 API；若 trade_cal 未加载则抛出 `RuntimeError`。

同时需在 `src/pipeline.py` 顶部导入中加入：
```python
from src.storage import MetaStore, write_basic, write_daily_kline, write_trade_cal
```

**Step 1: 在 tests/test_pipeline.py 末尾追加测试**

```python
from src.storage import write_trade_cal


EXCHANGES = ["SSE", "SZSE", "CFFEX", "SHFE", "CZCE", "DCE", "INE"]


def _trade_cal_df(exchange: str):
    return pd.DataFrame({
        "exchange": [exchange] * 3,
        "cal_date": [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)],
        "is_open": [True, False, True],
        "pretrade_date": [date(2023, 12, 29), date(2024, 1, 2), date(2024, 1, 2)],
    })


def test_sync_trade_cal_writes_all_exchanges(pipeline, cfg):
    for ex in EXCHANGES:
        pipeline._fetcher.fetch_trade_cal.return_value = _trade_cal_df(ex)
    pipeline.sync_trade_cal()
    for ex in EXCHANGES:
        assert (cfg.data_dir / "trade_cal" / f"exchange={ex}" / "data.parquet").exists()


def test_sync_trade_cal_loads_to_duckdb(pipeline, cfg):
    for ex in EXCHANGES:
        pipeline._fetcher.fetch_trade_cal.return_value = _trade_cal_df(ex)
    pipeline.sync_trade_cal()
    days = pipeline._meta.get_trading_days("SSE", date(2024, 1, 1), date(2024, 1, 5))
    assert date(2024, 1, 2) in days
    assert date(2024, 1, 3) not in days


def test_sync_trade_cal_updates_meta(pipeline, cfg):
    for ex in EXCHANGES:
        pipeline._fetcher.fetch_trade_cal.return_value = _trade_cal_df(ex)
    pipeline.sync_trade_cal()
    assert pipeline._meta.get_last_date("trade_cal") is not None


def test_sync_daily_kline_uses_trading_calendar(pipeline, cfg):
    # 准备交易日历（SSE: 2024-01-02 交易，2024-01-03 休市，2024-01-04 交易）
    write_trade_cal(cfg.data_dir, "SSE", _trade_cal_df("SSE"))
    pipeline._meta.load_trade_cal_from_parquet(cfg.data_dir)

    kline_df = pd.DataFrame({
        "ts_code": ["000001.SZ"], "trade_date": [date(2024, 1, 2)],
        "open": [10.0], "high": [11.0], "low": [9.5], "close": [10.5],
        "pre_close": [10.0], "change": [0.5], "pct_chg": [5.0],
        "vol": [100000.0], "amount": [1050000.0],
    })
    pipeline._fetcher.fetch_daily_kline.return_value = kline_df
    pipeline._meta.update_last_date("daily_kline", date(2024, 1, 1))

    with patch("src.pipeline.date") as mock_date:
        mock_date.today.return_value = date(2024, 1, 4)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        pipeline.sync_daily_kline()

    # 应只调用 2 次（1/2 和 1/4），不调用 1/3（休市）
    assert pipeline._fetcher.fetch_daily_kline.call_count == 2


def test_sync_daily_kline_raises_if_no_trade_cal(pipeline, cfg):
    pipeline._meta.update_last_date("daily_kline", date(2024, 1, 1))
    with patch("src.pipeline.date") as mock_date:
        mock_date.today.return_value = date(2024, 1, 4)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        with pytest.raises(RuntimeError, match="trade_cal"):
            pipeline.sync_daily_kline()
```

**Step 2: 运行测试确认失败**

```bash
cd D:/Project/zer0share && uv run pytest tests/test_pipeline.py::test_sync_trade_cal_writes_all_exchanges -v
```

Expected: FAIL with "Pipeline has no attribute sync_trade_cal"

**Step 3: 修改 src/pipeline.py**

修改顶部 import：

```python
from src.storage import MetaStore, write_basic, write_daily_kline, write_trade_cal
```

在 `sync_basic` 方法前追加常量：

```python
EXCHANGES = ["SSE", "SZSE", "CFFEX", "SHFE", "CZCE", "DCE", "INE"]
```

在 `sync_basic` 方法后追加 `sync_trade_cal`：

```python
def sync_trade_cal(self) -> None:
    try:
        for exchange in EXCHANGES:
            df = self._fetcher.fetch_trade_cal(exchange)
            write_trade_cal(self._cfg.data_dir, exchange, df)
            logger.info(f"trade_cal {exchange} 写入完成: {len(df)} 条")
        self._meta.load_trade_cal_from_parquet(self._cfg.data_dir)
        self._meta.update_last_date("trade_cal", date.today())
        logger.info("trade_cal 全部同步完成")
    except Exception as e:
        logger.error(f"trade_cal 同步失败: {e}")
        self._notifier.send(f"trade_cal 同步失败: {e}")
        raise
```

将 `sync_daily_kline` 方法完整替换为：

```python
def sync_daily_kline(self) -> None:
    last = self._meta.get_last_date("daily_kline")
    start = (last + timedelta(days=1)) if last else FIRST_DATE
    today = date.today()

    if start > today:
        logger.info("daily_kline 已是最新，无需同步")
        return

    trading_days = self._meta.get_trading_days("SSE", start, today)
    if not trading_days and self._meta.get_last_date("trade_cal") is None:
        raise RuntimeError(
            "DuckDB 中无 SSE trade_cal 数据，请先运行: "
            "python main.py sync --table trade_cal"
        )

    if not trading_days:
        logger.info("指定范围内无交易日，无需同步")
        return

    success = 0
    for trade_date in trading_days:
        try:
            df = self._fetcher.fetch_daily_kline(trade_date)
            if not df.empty:
                write_daily_kline(self._cfg.data_dir, trade_date, df)
                self._meta.update_last_date("daily_kline", trade_date)
                success += 1
        except Exception as e:
            logger.error(f"daily_kline {trade_date} 同步失败: {e}")
            self._notifier.send(f"daily_kline {trade_date} 同步失败: {e}")
            raise

    msg = f"daily_kline 同步完成: 成功 {success} 天（共 {len(trading_days)} 个交易日）"
    logger.info(msg)
    self._notifier.send(msg)
```

**Step 4: 运行全量 pipeline 测试**

```bash
cd D:/Project/zer0share && uv run pytest tests/test_pipeline.py -v
```

Expected: 全部 PASS（约 17 个测试）

**Step 5: Commit**

```bash
cd D:/Project/zer0share
git add src/pipeline.py tests/test_pipeline.py
git commit -m "feat: add sync_trade_cal and refactor sync_daily_kline to use trading calendar"
```

---

## Task 4: cli.py — 更新 sync 命令 + status 命令

**Files:**
- Modify: `src/cli.py`

**背景：**
`src/cli.py` 的 `sync` 命令目前只支持 `--table [daily_kline|basic]`。
本任务：
1. 在 `--table` 选项中加入 `trade_cal`
2. `sync --all` 顺序改为 trade_cal → basic → daily_kline
3. `status` 命令加入 trade_cal 状态显示

**Step 1: 修改 src/cli.py**

将 `sync` 命令修改为：

```python
@cli.command()
@click.option(
    "--table",
    type=click.Choice(["daily_kline", "basic", "trade_cal"]),
    default=None,
)
@click.option("--all", "sync_all", is_flag=True, default=False)
def sync(table: str | None, sync_all: bool) -> None:
    """增量同步数据"""
    with _make_pipeline() as pipeline:
        if sync_all or table == "trade_cal":
            pipeline.sync_trade_cal()
        if sync_all or table == "basic":
            pipeline.sync_basic()
        if sync_all or table == "daily_kline":
            pipeline.sync_daily_kline()
```

将 `status` 命令修改为：

```python
@cli.command()
def status() -> None:
    """显示各表最后更新时间"""
    cfg = load_config(Path("config/settings.toml"))
    with MetaStore(cfg.db_path) as store:
        for table in ["trade_cal", "daily_kline", "basic"]:
            last = store.get_last_date(table)
            click.echo(f"{table}: {last or '从未同步'}")
```

**Step 2: 验证 CLI 帮助正常**

```bash
cd D:/Project/zer0share && uv run python main.py sync --help
```

Expected: 输出包含 `trade_cal` 选项

**Step 3: 运行全量测试确认无回归**

```bash
cd D:/Project/zer0share && uv run pytest tests/ -v
```

Expected: 全部 PASS

**Step 4: Commit**

```bash
cd D:/Project/zer0share
git add src/cli.py
git commit -m "feat: add trade_cal to CLI sync and status commands"
```
