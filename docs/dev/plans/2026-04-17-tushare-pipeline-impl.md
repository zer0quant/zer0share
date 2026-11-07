# Tushare 数据管道 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 构建基于 Tushare 的 A 股数据管道，增量拉取日线行情和基础信息，以 Parquet 按日期分区存储，DuckDB 管理元数据，支持 CLI 和定时调度，企业微信告警。

**Architecture:** Tushare → fetcher.py 拉取 → storage.py 写 Parquet + 更新 DuckDB 元数据 → cli.py 手动触发 / scheduler.py 定时调度 → notifier.py 企业微信推送。

**Tech Stack:** Python 3.11+, tushare, duckdb, pyarrow, apscheduler, click, loguru, httpx, tomllib

---

## Task 1: 项目初始化 & 依赖配置

**Files:**
- Create: `pyproject.toml`
- Create: `config/settings.toml`
- Create: `config/settings.example.toml`
- Create: `src/__init__.py`
- Create: `tests/__init__.py`

**Step 1: 创建 pyproject.toml**

```toml
[project]
name = "zer0share"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = [
    "tushare>=1.4.0",
    "duckdb>=1.1.0",
    "pyarrow>=17.0.0",
    "apscheduler>=3.10.0",
    "click>=8.1.0",
    "loguru>=0.7.0",
    "httpx>=0.27.0",
]

[project.optional-dependencies]
dev = ["pytest>=8.0.0", "pytest-mock>=3.14.0"]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

**Step 2: 创建 config/settings.example.toml**

```toml
[tushare]
token = "your_tushare_token_here"

[paths]
data_dir = "data"
db_path = "db/meta.duckdb"
log_path = "logs/pipeline.log"

[basic]
refresh_days = 7          # basic 表超过 N 天未更新则刷新

[scheduler]
daily_kline_hour = 18
daily_kline_minute = 0
basic_day_of_week = "mon"
basic_hour = 8

[notifier]
wecom_webhook_url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=YOUR_KEY"
enabled = true
```

**Step 3: 复制为 config/settings.toml（填写真实 token 和 webhook）**

```bash
cp config/settings.example.toml config/settings.toml
# 编辑 config/settings.toml 填入真实配置
```

**Step 4: 创建空的 __init__.py**

```bash
mkdir -p src tests logs db data/daily_kline data/basic
touch src/__init__.py tests/__init__.py
```

**Step 5: 安装依赖**

```bash
pip install -e ".[dev]"
```

Expected: 所有包安装成功，无报错。

**Step 6: Commit**

```bash
git add pyproject.toml config/settings.example.toml src/__init__.py tests/__init__.py
git commit -m "chore: project init with dependencies"
```

---

## Task 2: 配置加载模块

**Files:**
- Create: `src/config.py`
- Create: `tests/test_config.py`

**Step 1: 写失败测试**

```python
# tests/test_config.py
import pytest
from pathlib import Path
from src.config import load_config, Config

def test_load_config(tmp_path):
    cfg_file = tmp_path / "settings.toml"
    cfg_file.write_text("""
[tushare]
token = "test_token"

[paths]
data_dir = "data"
db_path = "db/meta.duckdb"
log_path = "logs/pipeline.log"

[basic]
refresh_days = 7

[scheduler]
daily_kline_hour = 18
daily_kline_minute = 0
basic_day_of_week = "mon"
basic_hour = 8

[notifier]
wecom_webhook_url = "https://example.com/webhook"
enabled = false
""")
    cfg = load_config(cfg_file)
    assert cfg.tushare_token == "test_token"
    assert cfg.data_dir == Path("data")
    assert cfg.basic_refresh_days == 7
    assert cfg.notifier_enabled is False
```

**Step 2: 运行测试确认失败**

```bash
pytest tests/test_config.py -v
```

Expected: FAIL with "cannot import name 'load_config'"

**Step 3: 实现 src/config.py**

```python
from dataclasses import dataclass
from pathlib import Path
import tomllib


@dataclass(frozen=True)
class Config:
    tushare_token: str
    data_dir: Path
    db_path: Path
    log_path: Path
    basic_refresh_days: int
    scheduler_daily_kline_hour: int
    scheduler_daily_kline_minute: int
    scheduler_basic_day_of_week: str
    scheduler_basic_hour: int
    wecom_webhook_url: str
    notifier_enabled: bool


def load_config(path: Path = Path("config/settings.toml")) -> Config:
    with open(path, "rb") as f:
        raw = tomllib.load(f)
    return Config(
        tushare_token=raw["tushare"]["token"],
        data_dir=Path(raw["paths"]["data_dir"]),
        db_path=Path(raw["paths"]["db_path"]),
        log_path=Path(raw["paths"]["log_path"]),
        basic_refresh_days=raw["basic"]["refresh_days"],
        scheduler_daily_kline_hour=raw["scheduler"]["daily_kline_hour"],
        scheduler_daily_kline_minute=raw["scheduler"]["daily_kline_minute"],
        scheduler_basic_day_of_week=raw["scheduler"]["basic_day_of_week"],
        scheduler_basic_hour=raw["scheduler"]["basic_hour"],
        wecom_webhook_url=raw["notifier"]["wecom_webhook_url"],
        notifier_enabled=raw["notifier"]["enabled"],
    )
```

**Step 4: 运行测试确认通过**

```bash
pytest tests/test_config.py -v
```

Expected: PASS

**Step 5: Commit**

```bash
git add src/config.py tests/test_config.py
git commit -m "feat: add config loader"
```

---

## Task 3: DuckDB 元数据管理

**Files:**
- Create: `src/storage.py`
- Create: `tests/test_storage.py`

**Step 1: 写失败测试**

```python
# tests/test_storage.py
import pytest
from pathlib import Path
from datetime import date
from src.storage import MetaStore

def test_init_creates_table(tmp_path):
    db_path = tmp_path / "meta.duckdb"
    store = MetaStore(db_path)
    assert store.get_last_date("daily_kline") is None

def test_update_and_get_last_date(tmp_path):
    db_path = tmp_path / "meta.duckdb"
    store = MetaStore(db_path)
    store.update_last_date("daily_kline", date(2024, 1, 15))
    assert store.get_last_date("daily_kline") == date(2024, 1, 15)

def test_update_overwrites_previous(tmp_path):
    db_path = tmp_path / "meta.duckdb"
    store = MetaStore(db_path)
    store.update_last_date("daily_kline", date(2024, 1, 1))
    store.update_last_date("daily_kline", date(2024, 1, 31))
    assert store.get_last_date("daily_kline") == date(2024, 1, 31)
```

**Step 2: 运行测试确认失败**

```bash
pytest tests/test_storage.py -v
```

Expected: FAIL with "cannot import name 'MetaStore'"

**Step 3: 实现 src/storage.py（MetaStore 部分）**

```python
import duckdb
from datetime import date, datetime
from pathlib import Path


class MetaStore:
    def __init__(self, db_path: Path):
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = duckdb.connect(str(db_path))
        self._init_schema()

    def _init_schema(self):
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS sync_meta (
                table_name  VARCHAR PRIMARY KEY,
                last_date   DATE,
                updated_at  TIMESTAMP
            )
        """)

    def get_last_date(self, table_name: str) -> date | None:
        row = self._conn.execute(
            "SELECT last_date FROM sync_meta WHERE table_name = ?",
            [table_name]
        ).fetchone()
        return row[0] if row else None

    def update_last_date(self, table_name: str, last_date: date):
        self._conn.execute("""
            INSERT INTO sync_meta (table_name, last_date, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT (table_name) DO UPDATE SET
                last_date = excluded.last_date,
                updated_at = excluded.updated_at
        """, [table_name, last_date, datetime.now()])

    def close(self):
        self._conn.close()
```

**Step 4: 运行测试确认通过**

```bash
pytest tests/test_storage.py -v
```

Expected: PASS

**Step 5: Commit**

```bash
git add src/storage.py tests/test_storage.py
git commit -m "feat: add DuckDB meta store"
```

---

## Task 4: Parquet 读写（storage.py 扩展）

**Files:**
- Modify: `src/storage.py`
- Modify: `tests/test_storage.py`

**Step 1: 添加失败测试**

```python
# 追加到 tests/test_storage.py
import pandas as pd
from src.storage import write_daily_kline, read_daily_kline, write_basic, read_basic

def test_write_and_read_daily_kline(tmp_path):
    df = pd.DataFrame({
        "ts_code": ["000001.SZ", "000002.SZ"],
        "trade_date": [date(2024, 1, 2), date(2024, 1, 2)],
        "open": [10.0, 20.0],
        "high": [11.0, 21.0],
        "low": [9.5, 19.5],
        "close": [10.5, 20.5],
        "pre_close": [10.0, 20.0],
        "change": [0.5, 0.5],
        "pct_chg": [5.0, 2.5],
        "vol": [100000.0, 200000.0],
        "amount": [1050000.0, 4100000.0],
    })
    write_daily_kline(tmp_path, date(2024, 1, 2), df)
    result = read_daily_kline(tmp_path, date(2024, 1, 2))
    assert len(result) == 2
    assert set(result["ts_code"]) == {"000001.SZ", "000002.SZ"}

def test_write_and_read_basic(tmp_path):
    df = pd.DataFrame({
        "ts_code": ["000001.SZ"],
        "symbol": ["000001"],
        "name": ["平安银行"],
        "area": ["深圳"],
        "industry": ["银行"],
        "market": ["主板"],
        "list_status": ["L"],
        "list_date": [date(1991, 4, 3)],
        "delist_date": [None],
    })
    write_basic(tmp_path, df)
    result = read_basic(tmp_path)
    assert len(result) == 1
    assert result.iloc[0]["name"] == "平安银行"
```

**Step 2: 运行测试确认失败**

```bash
pytest tests/test_storage.py::test_write_and_read_daily_kline -v
```

Expected: FAIL with "cannot import name 'write_daily_kline'"

**Step 3: 在 src/storage.py 追加 Parquet 函数**

```python
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import date
from pathlib import Path


def write_daily_kline(data_dir: Path, trade_date: date, df: pd.DataFrame):
    partition_dir = data_dir / "daily_kline" / f"date={trade_date.strftime('%Y%m%d')}"
    partition_dir.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, partition_dir / "data.parquet")


def read_daily_kline(data_dir: Path, trade_date: date) -> pd.DataFrame:
    path = data_dir / "daily_kline" / f"date={trade_date.strftime('%Y%m%d')}" / "data.parquet"
    return pq.read_table(path).to_pandas()


def write_basic(data_dir: Path, df: pd.DataFrame):
    basic_dir = data_dir / "basic"
    basic_dir.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, basic_dir / "data.parquet")


def read_basic(data_dir: Path) -> pd.DataFrame:
    path = data_dir / "basic" / "data.parquet"
    return pq.read_table(path).to_pandas()
```

**Step 4: 运行全部 storage 测试**

```bash
pytest tests/test_storage.py -v
```

Expected: 全部 PASS

**Step 5: Commit**

```bash
git add src/storage.py tests/test_storage.py
git commit -m "feat: add parquet read/write for daily_kline and basic"
```

---

## Task 5: 企业微信告警模块

**Files:**
- Create: `src/notifier.py`
- Create: `tests/test_notifier.py`

**Step 1: 写失败测试**

```python
# tests/test_notifier.py
import pytest
from unittest.mock import patch, MagicMock
from src.notifier import Notifier

def test_send_success_disabled():
    n = Notifier(webhook_url="https://example.com", enabled=False)
    # 禁用时不应发送请求
    with patch("httpx.post") as mock_post:
        n.send("test message")
        mock_post.assert_not_called()

def test_send_success_enabled():
    n = Notifier(webhook_url="https://example.com/hook", enabled=True)
    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    with patch("httpx.post", return_value=mock_response) as mock_post:
        n.send("同步完成：成功 5 天")
        mock_post.assert_called_once()
        payload = mock_post.call_args[1]["json"]
        assert payload["msgtype"] == "text"
        assert "同步完成" in payload["text"]["content"]

def test_send_failure_logs_error():
    import httpx
    n = Notifier(webhook_url="https://example.com/hook", enabled=True)
    with patch("httpx.post", side_effect=httpx.RequestError("network error")):
        # 不应抛出异常，只记录日志
        n.send("告警消息")
```

**Step 2: 运行测试确认失败**

```bash
pytest tests/test_notifier.py -v
```

Expected: FAIL with "cannot import name 'Notifier'"

**Step 3: 实现 src/notifier.py**

```python
import httpx
from loguru import logger


class Notifier:
    def __init__(self, webhook_url: str, enabled: bool):
        self._url = webhook_url
        self._enabled = enabled

    def send(self, message: str):
        if not self._enabled:
            return
        payload = {
            "msgtype": "text",
            "text": {"content": f"[zer0share] {message}"}
        }
        try:
            resp = httpx.post(self._url, json=payload, timeout=10)
            resp.raise_for_status()
        except httpx.RequestError as e:
            logger.error(f"企业微信推送失败: {e}")
        except httpx.HTTPStatusError as e:
            logger.error(f"企业微信返回错误: {e.response.status_code}")
```

**Step 4: 运行测试确认通过**

```bash
pytest tests/test_notifier.py -v
```

Expected: 全部 PASS

**Step 5: Commit**

```bash
git add src/notifier.py tests/test_notifier.py
git commit -m "feat: add wecom notifier"
```

---

## Task 6: Tushare 数据拉取模块

**Files:**
- Create: `src/fetcher.py`
- Create: `tests/test_fetcher.py`

**Step 1: 写失败测试**

```python
# tests/test_fetcher.py
import pytest
import pandas as pd
from datetime import date
from unittest.mock import patch, MagicMock
from src.fetcher import TushareFetcher

@pytest.fixture
def mock_pro():
    with patch("tushare.pro_api") as mock:
        yield mock.return_value

def test_fetch_basic(mock_pro):
    mock_pro.stock_basic.return_value = pd.DataFrame({
        "ts_code": ["000001.SZ"],
        "symbol": ["000001"],
        "name": ["平安银行"],
        "area": ["深圳"],
        "industry": ["银行"],
        "market": ["主板"],
        "list_status": ["L"],
        "list_date": ["19910403"],
        "delist_date": [None],
        "fullname": ["平安银行股份有限公司"],
        "enname": ["Ping An Bank"],
        "cnspell": ["payh"],
        "exchange": ["SZSE"],
        "curr_type": ["CNY"],
        "is_hs": ["S"],
        "act_name": ["深圳市投资控股有限公司"],
        "act_ent_type": ["1"],
    })
    fetcher = TushareFetcher("fake_token")
    df = fetcher.fetch_basic()
    assert len(df) == 1
    assert list(df.columns) == ["ts_code","symbol","name","area","industry","market","list_status","list_date","delist_date"]

def test_fetch_daily_kline(mock_pro):
    mock_pro.daily.return_value = pd.DataFrame({
        "ts_code": ["000001.SZ"],
        "trade_date": ["20240102"],
        "open": [10.0], "high": [11.0], "low": [9.5],
        "close": [10.5], "pre_close": [10.0],
        "change": [0.5], "pct_chg": [5.0],
        "vol": [100000.0], "amount": [1050000.0],
    })
    fetcher = TushareFetcher("fake_token")
    df = fetcher.fetch_daily_kline(date(2024, 1, 2))
    assert len(df) == 1
    assert df.iloc[0]["ts_code"] == "000001.SZ"
    assert df.iloc[0]["trade_date"] == date(2024, 1, 2)
```

**Step 2: 运行测试确认失败**

```bash
pytest tests/test_fetcher.py -v
```

Expected: FAIL with "cannot import name 'TushareFetcher'"

**Step 3: 实现 src/fetcher.py**

```python
import tushare as ts
import pandas as pd
from datetime import date
from loguru import logger

BASIC_COLS = ["ts_code","symbol","name","area","industry","market","list_status","list_date","delist_date"]
DAILY_COLS = ["ts_code","trade_date","open","high","low","close","pre_close","change","pct_chg","vol","amount"]


class TushareFetcher:
    def __init__(self, token: str):
        self._pro = ts.pro_api(token)

    def fetch_basic(self) -> pd.DataFrame:
        logger.info("拉取 stock_basic")
        df = self._pro.stock_basic(
            exchange="",
            list_status="L,D,P",
            fields=",".join(BASIC_COLS)
        )
        df["list_date"] = pd.to_datetime(df["list_date"], format="%Y%m%d", errors="coerce").dt.date
        df["delist_date"] = pd.to_datetime(df["delist_date"], format="%Y%m%d", errors="coerce").dt.date
        return df[BASIC_COLS]

    def fetch_daily_kline(self, trade_date: date) -> pd.DataFrame:
        date_str = trade_date.strftime("%Y%m%d")
        logger.info(f"拉取日线行情: {date_str}")
        df = self._pro.daily(trade_date=date_str, fields=",".join(DAILY_COLS))
        if df is None or df.empty:
            return pd.DataFrame(columns=DAILY_COLS)
        df["trade_date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d").dt.date
        return df[DAILY_COLS]
```

**Step 4: 运行测试确认通过**

```bash
pytest tests/test_fetcher.py -v
```

Expected: 全部 PASS

**Step 5: Commit**

```bash
git add src/fetcher.py tests/test_fetcher.py
git commit -m "feat: add tushare fetcher for basic and daily_kline"
```

---

## Task 7: 同步业务逻辑（Pipeline）

**Files:**
- Create: `src/pipeline.py`
- Create: `tests/test_pipeline.py`

**Step 1: 写失败测试**

```python
# tests/test_pipeline.py
import pytest
import pandas as pd
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch
from src.pipeline import Pipeline

@pytest.fixture
def tmp_pipeline(tmp_path):
    cfg = MagicMock()
    cfg.data_dir = tmp_path
    cfg.db_path = tmp_path / "meta.duckdb"
    cfg.basic_refresh_days = 7
    fetcher = MagicMock()
    notifier = MagicMock()
    return Pipeline(cfg, fetcher, notifier)

def test_sync_basic_first_run(tmp_pipeline, tmp_path):
    basic_df = pd.DataFrame({
        "ts_code": ["000001.SZ"], "symbol": ["000001"],
        "name": ["平安银行"], "area": ["深圳"], "industry": ["银行"],
        "market": ["主板"], "list_status": ["L"],
        "list_date": [date(1991, 4, 3)], "delist_date": [None],
    })
    tmp_pipeline._fetcher.fetch_basic.return_value = basic_df
    tmp_pipeline.sync_basic()
    assert (tmp_path / "basic" / "data.parquet").exists()

def test_sync_daily_kline_incremental(tmp_pipeline, tmp_path):
    basic_df = pd.DataFrame({
        "ts_code": ["000001.SZ"], "symbol": ["000001"],
        "name": ["平安银行"], "area": ["深圳"], "industry": ["银行"],
        "market": ["主板"], "list_status": ["L"],
        "list_date": [date(1991, 4, 3)], "delist_date": [None],
    })
    from src.storage import write_basic
    write_basic(tmp_path, basic_df)

    kline_df = pd.DataFrame({
        "ts_code": ["000001.SZ"], "trade_date": [date(2024, 1, 2)],
        "open": [10.0], "high": [11.0], "low": [9.5], "close": [10.5],
        "pre_close": [10.0], "change": [0.5], "pct_chg": [5.0],
        "vol": [100000.0], "amount": [1050000.0],
    })
    tmp_pipeline._fetcher.fetch_daily_kline.return_value = kline_df
    tmp_pipeline._meta.update_last_date("daily_kline", date(2024, 1, 1))

    with patch("src.pipeline.date") as mock_date:
        mock_date.today.return_value = date(2024, 1, 2)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
        tmp_pipeline.sync_daily_kline()

    assert (tmp_path / "daily_kline" / "date=20240102" / "data.parquet").exists()
```

**Step 2: 运行测试确认失败**

```bash
pytest tests/test_pipeline.py -v
```

Expected: FAIL with "cannot import name 'Pipeline'"

**Step 3: 实现 src/pipeline.py**

```python
from datetime import date, timedelta
from pathlib import Path
from loguru import logger

from src.config import Config
from src.fetcher import TushareFetcher
from src.notifier import Notifier
from src.storage import (
    MetaStore, write_basic, read_basic,
    write_daily_kline
)

FIRST_DATE = date(2010, 1, 4)


class Pipeline:
    def __init__(self, cfg: Config, fetcher: TushareFetcher, notifier: Notifier):
        self._cfg = cfg
        self._fetcher = fetcher
        self._notifier = notifier
        self._meta = MetaStore(cfg.db_path)

    def sync_basic(self):
        last = self._meta.get_last_date("basic")
        today = date.today()
        if last and (today - last).days < self._cfg.basic_refresh_days:
            logger.info(f"basic 距上次更新 {(today - last).days} 天，跳过")
            return
        try:
            df = self._fetcher.fetch_basic()
            write_basic(self._cfg.data_dir, df)
            self._meta.update_last_date("basic", today)
            logger.info(f"basic 同步完成: {len(df)} 条")
        except Exception as e:
            logger.error(f"basic 同步失败: {e}")
            self._notifier.send(f"basic 同步失败: {e}")
            raise

    def sync_daily_kline(self):
        last = self._meta.get_last_date("daily_kline")
        start = (last + timedelta(days=1)) if last else FIRST_DATE
        today = date.today()

        if start > today:
            logger.info("daily_kline 已是最新，无需同步")
            return

        success, skipped = 0, 0
        current = start
        while current <= today:
            try:
                df = self._fetcher.fetch_daily_kline(current)
                if df.empty:
                    skipped += 1
                else:
                    write_daily_kline(self._cfg.data_dir, current, df)
                    self._meta.update_last_date("daily_kline", current)
                    success += 1
            except Exception as e:
                logger.error(f"daily_kline {current} 同步失败: {e}")
                self._notifier.send(f"daily_kline {current} 同步失败: {e}")
                raise
            current += timedelta(days=1)

        msg = f"daily_kline 同步完成: 成功 {success} 天，跳过 {skipped} 天（非交易日）"
        logger.info(msg)
        self._notifier.send(msg)
```

**Step 4: 运行测试确认通过**

```bash
pytest tests/test_pipeline.py -v
```

Expected: 全部 PASS

**Step 5: Commit**

```bash
git add src/pipeline.py tests/test_pipeline.py
git commit -m "feat: add pipeline sync logic with incremental update"
```

---

## Task 8: CLI 入口

**Files:**
- Create: `src/cli.py`
- Create: `main.py`

**Step 1: 实现 src/cli.py**

```python
import click
from loguru import logger
from src.config import load_config
from src.fetcher import TushareFetcher
from src.notifier import Notifier
from src.pipeline import Pipeline
from src.storage import MetaStore


def _make_pipeline(config_path="config/settings.toml") -> Pipeline:
    cfg = load_config(config_path)
    logger.add(cfg.log_path, rotation="10 MB", retention="30 days")
    fetcher = TushareFetcher(cfg.tushare_token)
    notifier = Notifier(cfg.wecom_webhook_url, cfg.notifier_enabled)
    return Pipeline(cfg, fetcher, notifier)


@click.group()
def cli():
    pass


@cli.command()
@click.option("--table", type=click.Choice(["daily_kline", "basic"]), default=None)
@click.option("--all", "sync_all", is_flag=True, default=False)
def sync(table, sync_all):
    """增量同步数据"""
    pipeline = _make_pipeline()
    if sync_all or table == "basic":
        pipeline.sync_basic()
    if sync_all or table == "daily_kline":
        pipeline.sync_daily_kline()


@cli.command()
def status():
    """显示各表最后更新时间"""
    from pathlib import Path
    cfg = load_config()
    store = MetaStore(cfg.db_path)
    for table in ["daily_kline", "basic"]:
        last = store.get_last_date(table)
        click.echo(f"{table}: {last or '从未同步'}")


@cli.command("scheduler")
@click.argument("action", type=click.Choice(["start"]))
def scheduler_cmd(action):
    """启动定时调度"""
    from src.scheduler import start_scheduler
    start_scheduler()
```

**Step 2: 实现 main.py**

```python
from src.cli import cli

if __name__ == "__main__":
    cli()
```

**Step 3: 手动验证 CLI**

```bash
python main.py --help
python main.py status
```

Expected: 显示帮助信息和各表状态（首次为"从未同步"）

**Step 4: Commit**

```bash
git add src/cli.py main.py
git commit -m "feat: add CLI with sync and status commands"
```

---

## Task 9: 定时调度模块

**Files:**
- Create: `src/scheduler.py`

**Step 1: 实现 src/scheduler.py**

```python
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from loguru import logger

from src.config import load_config
from src.fetcher import TushareFetcher
from src.notifier import Notifier
from src.pipeline import Pipeline


def start_scheduler(config_path="config/settings.toml"):
    cfg = load_config(config_path)
    logger.add(cfg.log_path, rotation="10 MB", retention="30 days")
    fetcher = TushareFetcher(cfg.tushare_token)
    notifier = Notifier(cfg.wecom_webhook_url, cfg.notifier_enabled)
    pipeline = Pipeline(cfg, fetcher, notifier)

    scheduler = BlockingScheduler()
    scheduler.add_job(
        pipeline.sync_daily_kline,
        CronTrigger(hour=cfg.scheduler_daily_kline_hour, minute=cfg.scheduler_daily_kline_minute),
        id="daily_kline",
    )
    scheduler.add_job(
        pipeline.sync_basic,
        CronTrigger(day_of_week=cfg.scheduler_basic_day_of_week, hour=cfg.scheduler_basic_hour),
        id="basic",
    )
    logger.info(
        f"调度器启动: daily_kline 每天 {cfg.scheduler_daily_kline_hour}:{cfg.scheduler_daily_kline_minute:02d}, "
        f"basic 每周{cfg.scheduler_basic_day_of_week} {cfg.scheduler_basic_hour}:00"
    )
    scheduler.start()
```

**Step 2: 验证调度器可启动（Ctrl+C 退出）**

```bash
python main.py scheduler start
```

Expected: 显示调度器启动日志，Ctrl+C 退出无报错

**Step 3: Commit**

```bash
git add src/scheduler.py
git commit -m "feat: add APScheduler cron jobs"
```

---

## Task 10: 运行全量测试 & .gitignore

**Files:**
- Create: `.gitignore`

**Step 1: 创建 .gitignore**

```
config/settings.toml
data/
db/
logs/
__pycache__/
*.pyc
.pytest_cache/
*.egg-info/
dist/
```

**Step 2: 运行全量测试**

```bash
pytest tests/ -v --tb=short
```

Expected: 全部 PASS

**Step 3: 最终 Commit**

```bash
git add .gitignore
git commit -m "chore: add gitignore"
```
