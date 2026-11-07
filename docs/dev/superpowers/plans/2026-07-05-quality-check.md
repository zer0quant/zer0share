# Data Quality Check Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build local Parquet quality checks for zer0share market data and adjustment factor tables.

**Architecture:** Add a focused `zer0share/quality/` package with target definitions, result models, rules, runner, and reporting. Expose it through `main.py quality check`, then optionally call the same runner from the existing scheduler without interrupting sync progress.

**Tech Stack:** Python 3.11, click, pandas, pyarrow, pytest, existing zer0share catalog/config/scheduler/notifier modules.

---

## File Structure

- Create `zer0share/quality/__init__.py`: public exports for the quality package.
- Create `zer0share/quality/models.py`: dataclasses and enums for severity, findings, summaries, run options, and run reports.
- Create `zer0share/quality/targets.py`: registry mapping scoped table names to catalog specs, markets, table type, and related tables.
- Create `zer0share/quality/rules.py`: pure rule functions that inspect DataFrames and partition paths.
- Create `zer0share/quality/runner.py`: selects targets, resolves dates/partitions, reads Parquet, applies rules, and returns a `QualityRunReport`.
- Create `zer0share/quality/reporter.py`: writes `summary.csv`, `findings.csv`, `metadata.json`, and formats terminal summaries.
- Modify `zer0share/cli.py`: add `quality check` command group.
- Modify `zer0share/config.py`: add parsed `quality` config with defaults.
- Modify `config/settings.example.toml`: document `[quality]` defaults.
- Modify `zer0share/scheduler.py`: run daily table-level quality checks after scheduled syncs when enabled; notify warn/fail without raising.
- Create `tests/test_quality_targets.py`: target registry tests.
- Create `tests/test_quality_rules.py`: rule-level tests using small DataFrames.
- Create `tests/test_quality_runner.py`: temporary Parquet integration tests.
- Create `tests/test_quality_reporter.py`: report file tests.
- Modify `tests/test_cli.py`: quality CLI tests.
- Create `tests/test_scheduler_quality.py`: scheduler integration unit tests.

---

### Task 1: Quality Result Models

**Files:**
- Create: `zer0share/quality/__init__.py`
- Create: `zer0share/quality/models.py`
- Test: `tests/test_quality_rules.py`

- [ ] **Step 1: Write the failing model test**

Add to `tests/test_quality_rules.py`:

```python
from zer0share.quality.models import QualityFinding, Severity


def test_quality_finding_serializes_sample_as_json_text():
    finding = QualityFinding(
        table="daily_kline",
        date="20240102",
        severity=Severity.FAIL,
        rule="positive_prices",
        count=2,
        message="non-positive prices",
        sample=[{"ts_code": "000001.SZ", "close": 0}],
    )

    row = finding.to_row()

    assert row["table"] == "daily_kline"
    assert row["date"] == "20240102"
    assert row["severity"] == "fail"
    assert row["rule"] == "positive_prices"
    assert row["count"] == 2
    assert '"000001.SZ"' in row["sample"]
```

- [ ] **Step 2: Run the failing test**

Run:

```bash
uv run pytest tests/test_quality_rules.py::test_quality_finding_serializes_sample_as_json_text -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'zer0share.quality'`.

- [ ] **Step 3: Implement the models**

Create `zer0share/quality/models.py`:

```python
from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path


class Severity(str, Enum):
    PASS = "pass"
    WARN = "warn"
    FAIL = "fail"


@dataclass(frozen=True)
class QualityFinding:
    table: str
    date: str | None
    severity: Severity
    rule: str
    count: int
    message: str
    sample: list[dict] = field(default_factory=list)

    def to_row(self) -> dict[str, object]:
        return {
            "table": self.table,
            "date": self.date or "",
            "severity": self.severity.value,
            "rule": self.rule,
            "count": self.count,
            "message": self.message,
            "sample": json.dumps(self.sample, ensure_ascii=False, default=str),
        }


@dataclass(frozen=True)
class TableSummary:
    table: str
    market: str
    partitions: int
    rows: int
    pass_count: int
    warn_count: int
    fail_count: int

    def to_row(self) -> dict[str, object]:
        return {
            "table": self.table,
            "market": self.market,
            "partitions": self.partitions,
            "rows": self.rows,
            "pass": self.pass_count,
            "warn": self.warn_count,
            "fail": self.fail_count,
        }


@dataclass(frozen=True)
class QualityRunOptions:
    mode: str
    tables: tuple[str, ...]
    start_date: str | None = None
    end_date: str | None = None
    date: str | None = None
    report_dir: Path | None = None


@dataclass(frozen=True)
class QualityRunReport:
    options: QualityRunOptions
    summaries: list[TableSummary]
    findings: list[QualityFinding]
    output_dir: Path | None = None

    @property
    def fail_count(self) -> int:
        return sum(1 for finding in self.findings if finding.severity == Severity.FAIL)

    @property
    def warn_count(self) -> int:
        return sum(1 for finding in self.findings if finding.severity == Severity.WARN)
```

Create `zer0share/quality/__init__.py`:

```python
from zer0share.quality.models import (
    QualityFinding,
    QualityRunOptions,
    QualityRunReport,
    Severity,
    TableSummary,
)

__all__ = [
    "QualityFinding",
    "QualityRunOptions",
    "QualityRunReport",
    "Severity",
    "TableSummary",
]
```

- [ ] **Step 4: Run the model test**

Run:

```bash
uv run pytest tests/test_quality_rules.py::test_quality_finding_serializes_sample_as_json_text -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add zer0share/quality/__init__.py zer0share/quality/models.py tests/test_quality_rules.py
git commit -m "feat: add quality result models"
```

---

### Task 2: Quality Target Registry

**Files:**
- Create: `zer0share/quality/targets.py`
- Modify: `zer0share/quality/__init__.py`
- Test: `tests/test_quality_targets.py`

- [ ] **Step 1: Write target registry tests**

Create `tests/test_quality_targets.py`:

```python
import pytest

from zer0share.quality.targets import (
    QUALITY_TARGETS,
    get_targets,
    select_targets,
)


def test_quality_targets_cover_first_version_scope():
    assert set(QUALITY_TARGETS) == {
        "daily_kline",
        "adj_factor",
        "index_daily",
        "fund_daily",
        "fund_adj",
        "fut_daily",
        "opt_daily",
    }


def test_select_targets_by_market():
    targets = select_targets(market="etf")

    assert [target.table for target in targets] == ["fund_daily", "fund_adj"]


def test_select_targets_by_table():
    targets = select_targets(table="daily_kline")

    assert len(targets) == 1
    assert targets[0].market == "stock"
    assert targets[0].primary_key == ("ts_code", "trade_date")


def test_select_all_targets_keeps_registry_order():
    targets = select_targets(all_targets=True)

    assert [target.table for target in targets] == list(QUALITY_TARGETS)


def test_select_targets_rejects_unknown_table():
    with pytest.raises(ValueError, match="unknown quality table"):
        select_targets(table="daily_basic")


def test_get_targets_rejects_unknown_names():
    with pytest.raises(ValueError, match="unknown quality table"):
        get_targets(["missing"])
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
uv run pytest tests/test_quality_targets.py -v
```

Expected: FAIL with `ModuleNotFoundError` for `zer0share.quality.targets`.

- [ ] **Step 3: Implement target registry**

Create `zer0share/quality/targets.py`:

```python
from __future__ import annotations

from dataclasses import dataclass

from zer0share.catalog import (
    ADJ_FACTOR_SPEC,
    DAILY_KLINE_SPEC,
    FUND_ADJ_SPEC,
    FUND_DAILY_SPEC,
    FUT_DAILY_SPEC,
    INDEX_DAILY_SPEC,
    OPT_DAILY_SPEC,
)
from zer0share.query.repository import DailyTableSpec


@dataclass(frozen=True)
class QualityTarget:
    table: str
    market: str
    kind: str
    spec: DailyTableSpec
    primary_key: tuple[str, ...] = ("ts_code", "trade_date")
    price_columns: tuple[str, ...] = ("open", "high", "low", "close")
    volume_columns: tuple[str, ...] = ("vol", "amount")
    related_table: str | None = None


QUALITY_TARGETS: dict[str, QualityTarget] = {
    "daily_kline": QualityTarget("daily_kline", "stock", "market_data", DAILY_KLINE_SPEC),
    "adj_factor": QualityTarget(
        "adj_factor",
        "stock",
        "adjustment",
        ADJ_FACTOR_SPEC,
        price_columns=(),
        volume_columns=(),
        related_table="daily_kline",
    ),
    "index_daily": QualityTarget("index_daily", "index", "market_data", INDEX_DAILY_SPEC),
    "fund_daily": QualityTarget("fund_daily", "etf", "market_data", FUND_DAILY_SPEC),
    "fund_adj": QualityTarget(
        "fund_adj",
        "etf",
        "adjustment",
        FUND_ADJ_SPEC,
        price_columns=(),
        volume_columns=(),
        related_table="fund_daily",
    ),
    "fut_daily": QualityTarget("fut_daily", "futures", "market_data", FUT_DAILY_SPEC),
    "opt_daily": QualityTarget("opt_daily", "options", "market_data", OPT_DAILY_SPEC),
}


def get_targets(names: list[str]) -> list[QualityTarget]:
    unknown = [name for name in names if name not in QUALITY_TARGETS]
    if unknown:
        raise ValueError(f"unknown quality table: {', '.join(unknown)}")
    return [QUALITY_TARGETS[name] for name in names]


def select_targets(
    *,
    table: str | None = None,
    market: str | None = None,
    all_targets: bool = False,
) -> list[QualityTarget]:
    selectors = [table is not None, market is not None, all_targets]
    if sum(selectors) != 1:
        raise ValueError("select exactly one of table, market, or all_targets")
    if table is not None:
        return get_targets([table])
    if market is not None:
        targets = [target for target in QUALITY_TARGETS.values() if target.market == market]
        if not targets:
            raise ValueError(f"unknown quality market: {market}")
        return targets
    return list(QUALITY_TARGETS.values())
```

Modify `zer0share/quality/__init__.py`:

```python
from zer0share.quality.models import (
    QualityFinding,
    QualityRunOptions,
    QualityRunReport,
    Severity,
    TableSummary,
)
from zer0share.quality.targets import QualityTarget

__all__ = [
    "QualityFinding",
    "QualityRunOptions",
    "QualityRunReport",
    "QualityTarget",
    "Severity",
    "TableSummary",
]
```

- [ ] **Step 4: Run target tests**

Run:

```bash
uv run pytest tests/test_quality_targets.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add zer0share/quality/__init__.py zer0share/quality/targets.py tests/test_quality_targets.py
git commit -m "feat: add quality target registry"
```

---

### Task 3: DataFrame Rule Functions

**Files:**
- Create: `zer0share/quality/rules.py`
- Test: `tests/test_quality_rules.py`

- [ ] **Step 1: Add failing rule tests**

Append to `tests/test_quality_rules.py`:

```python
import pandas as pd

from zer0share.quality.rules import (
    check_adjustment_factor_values,
    check_duplicate_key,
    check_market_data_values,
    check_required_columns,
)


def test_check_required_columns_finds_missing_columns():
    df = pd.DataFrame({"ts_code": ["000001.SZ"]})

    findings = check_required_columns("daily_kline", "20240102", df, ["ts_code", "trade_date"])

    assert findings[0].severity.value == "fail"
    assert findings[0].rule == "required_columns"
    assert "trade_date" in findings[0].message


def test_check_duplicate_key_finds_duplicate_rows():
    df = pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "trade_date": "20240102"},
            {"ts_code": "000001.SZ", "trade_date": "20240102"},
        ]
    )

    findings = check_duplicate_key("daily_kline", "20240102", df, ("ts_code", "trade_date"))

    assert findings[0].severity.value == "fail"
    assert findings[0].count == 2


def test_check_market_data_values_validates_prices_and_pct_chg():
    df = pd.DataFrame(
        [
            {
                "ts_code": "000001.SZ",
                "trade_date": "20240102",
                "open": 10.0,
                "high": 9.5,
                "low": 9.0,
                "close": 10.0,
                "pre_close": 9.0,
                "pct_chg": 99.0,
                "vol": 0,
                "amount": 100.0,
            }
        ]
    )

    findings = check_market_data_values("daily_kline", "20240102", df)

    assert {finding.rule for finding in findings} == {
        "ohlc_relationship",
        "positive_volume",
        "pct_chg_consistency",
    }
    assert {finding.severity.value for finding in findings} == {"fail", "warn"}


def test_check_adjustment_factor_values_finds_non_positive_factor():
    df = pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "trade_date": "20240102", "adj_factor": 0.0},
        ]
    )

    findings = check_adjustment_factor_values("adj_factor", "20240102", df)

    assert findings[0].severity.value == "fail"
    assert findings[0].rule == "positive_adj_factor"
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
uv run pytest tests/test_quality_rules.py -v
```

Expected: FAIL with import errors for rule functions.

- [ ] **Step 3: Implement rule functions**

Create `zer0share/quality/rules.py`:

```python
from __future__ import annotations

import pandas as pd

from zer0share.quality.models import QualityFinding, Severity


def _sample(df: pd.DataFrame, columns: list[str] | None = None, limit: int = 5) -> list[dict]:
    if columns is not None:
        columns = [column for column in columns if column in df.columns]
        df = df[columns]
    return df.head(limit).to_dict("records")


def check_required_columns(
    table: str,
    date: str | None,
    df: pd.DataFrame,
    required_columns: list[str],
) -> list[QualityFinding]:
    missing = [column for column in required_columns if column not in df.columns]
    if not missing:
        return []
    return [
        QualityFinding(
            table=table,
            date=date,
            severity=Severity.FAIL,
            rule="required_columns",
            count=len(missing),
            message=f"missing required columns: {', '.join(missing)}",
        )
    ]


def check_duplicate_key(
    table: str,
    date: str | None,
    df: pd.DataFrame,
    key_columns: tuple[str, ...],
) -> list[QualityFinding]:
    if any(column not in df.columns for column in key_columns):
        return []
    duplicated = df[df.duplicated(list(key_columns), keep=False)]
    if duplicated.empty:
        return []
    return [
        QualityFinding(
            table=table,
            date=date,
            severity=Severity.FAIL,
            rule="duplicate_key",
            count=len(duplicated),
            message=f"duplicate primary key: {' + '.join(key_columns)}",
            sample=_sample(duplicated, list(key_columns)),
        )
    ]


def check_market_data_values(
    table: str,
    date: str | None,
    df: pd.DataFrame,
) -> list[QualityFinding]:
    findings: list[QualityFinding] = []
    price_columns = [column for column in ("open", "high", "low", "close") if column in df.columns]
    if price_columns:
        bad_prices = df[df[price_columns].isna().any(axis=1) | (df[price_columns] <= 0).any(axis=1)]
        if not bad_prices.empty:
            findings.append(
                QualityFinding(
                    table=table,
                    date=date,
                    severity=Severity.FAIL,
                    rule="positive_prices",
                    count=len(bad_prices),
                    message="open, high, low, and close must be non-null and greater than 0",
                    sample=_sample(bad_prices, ["ts_code", "trade_date", *price_columns]),
                )
            )

    if {"open", "high", "low", "close"}.issubset(df.columns):
        bad_ohlc = df[
            (df["high"] < df[["open", "close"]].max(axis=1))
            | (df["low"] > df[["open", "close"]].min(axis=1))
            | (df["high"] < df["low"])
        ]
        if not bad_ohlc.empty:
            findings.append(
                QualityFinding(
                    table=table,
                    date=date,
                    severity=Severity.FAIL,
                    rule="ohlc_relationship",
                    count=len(bad_ohlc),
                    message="OHLC relationship is invalid",
                    sample=_sample(bad_ohlc, ["ts_code", "trade_date", "open", "high", "low", "close"]),
                )
            )

    for column in ("vol", "amount"):
        if column not in df.columns:
            continue
        bad_volume = df[df[column].isna() | (df[column] <= 0)]
        if not bad_volume.empty:
            findings.append(
                QualityFinding(
                    table=table,
                    date=date,
                    severity=Severity.WARN,
                    rule=f"positive_{column}",
                    count=len(bad_volume),
                    message=f"{column} should be greater than 0",
                    sample=_sample(bad_volume, ["ts_code", "trade_date", column]),
                )
            )

    if {"close", "pre_close", "pct_chg"}.issubset(df.columns):
        valid_base = df["pre_close"] > 0
        expected = ((df["close"] / df["pre_close"] - 1) * 100)
        bad_pct = df[valid_base & ((expected - df["pct_chg"]).abs() > 0.01)]
        if not bad_pct.empty:
            findings.append(
                QualityFinding(
                    table=table,
                    date=date,
                    severity=Severity.FAIL,
                    rule="pct_chg_consistency",
                    count=len(bad_pct),
                    message="pct_chg differs from close/pre_close by more than 0.01 percentage points",
                    sample=_sample(bad_pct, ["ts_code", "trade_date", "close", "pre_close", "pct_chg"]),
                )
            )

    return findings


def check_adjustment_factor_values(
    table: str,
    date: str | None,
    df: pd.DataFrame,
) -> list[QualityFinding]:
    if "adj_factor" not in df.columns:
        return []
    bad = df[df["adj_factor"].isna() | (df["adj_factor"] <= 0)]
    if bad.empty:
        return []
    return [
        QualityFinding(
            table=table,
            date=date,
            severity=Severity.FAIL,
            rule="positive_adj_factor",
            count=len(bad),
            message="adj_factor must be non-null and greater than 0",
            sample=_sample(bad, ["ts_code", "trade_date", "adj_factor"]),
        )
    ]
```

- [ ] **Step 4: Run rule tests**

Run:

```bash
uv run pytest tests/test_quality_rules.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add zer0share/quality/rules.py tests/test_quality_rules.py
git commit -m "feat: add quality dataframe rules"
```

---

### Task 4: Quality Runner Partition Checks

**Files:**
- Create: `zer0share/quality/runner.py`
- Test: `tests/test_quality_runner.py`

- [ ] **Step 1: Write runner tests for missing and valid partitions**

Create `tests/test_quality_runner.py`:

```python
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from zer0share.quality.models import QualityRunOptions
from zer0share.quality.runner import QualityRunner


def _write_parquet(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pandas(pd.DataFrame(rows), preserve_index=False), path)


def _write_trade_cal(data_dir: Path, dates: list[str]) -> None:
    _write_parquet(
        data_dir / "stock" / "trade_cal" / "exchange=SSE" / "data.parquet",
        [
            {"exchange": "SSE", "cal_date": date, "is_open": True, "pretrade_date": date}
            for date in dates
        ],
    )


def test_runner_reports_missing_partition_from_trade_calendar(tmp_path):
    _write_trade_cal(tmp_path, ["20240102"])
    runner = QualityRunner(tmp_path)

    report = runner.run(
        QualityRunOptions(
            mode="full",
            tables=("daily_kline",),
            start_date="20240102",
            end_date="20240102",
        )
    )

    assert report.fail_count == 0
    assert report.warn_count == 1
    assert report.findings[0].rule == "missing_partition"
    assert report.findings[0].date == "20240102"


def test_runner_passes_valid_daily_kline_partition(tmp_path):
    _write_trade_cal(tmp_path, ["20240102"])
    _write_parquet(
        tmp_path / "stock" / "daily_kline" / "date=20240102" / "data.parquet",
        [
            {
                "ts_code": "000001.SZ",
                "trade_date": "20240102",
                "open": 10.0,
                "high": 11.0,
                "low": 9.0,
                "close": 10.5,
                "pre_close": 10.0,
                "change": 0.5,
                "pct_chg": 5.0,
                "vol": 100.0,
                "amount": 1000.0,
            }
        ],
    )
    runner = QualityRunner(tmp_path)

    report = runner.run(
        QualityRunOptions(
            mode="full",
            tables=("daily_kline",),
            start_date="20240102",
            end_date="20240102",
        )
    )

    assert report.fail_count == 0
    assert report.warn_count == 0
    assert report.summaries[0].rows == 1
    assert report.summaries[0].partitions == 1
```

- [ ] **Step 2: Run runner tests to verify failure**

Run:

```bash
uv run pytest tests/test_quality_runner.py -v
```

Expected: FAIL with `ModuleNotFoundError` for `zer0share.quality.runner`.

- [ ] **Step 3: Implement runner basics**

Create `zer0share/quality/runner.py`:

```python
from __future__ import annotations

import datetime as dt
from pathlib import Path

import pandas as pd

from zer0share.quality.models import (
    QualityFinding,
    QualityRunOptions,
    QualityRunReport,
    Severity,
    TableSummary,
)
from zer0share.quality.rules import (
    check_adjustment_factor_values,
    check_duplicate_key,
    check_market_data_values,
    check_required_columns,
)
from zer0share.quality.targets import QualityTarget, get_targets


def _date_range(start_date: str, end_date: str) -> list[str]:
    start = dt.datetime.strptime(start_date, "%Y%m%d").date()
    end = dt.datetime.strptime(end_date, "%Y%m%d").date()
    if end < start:
        raise ValueError("end_date must be on or after start_date")
    days = []
    current = start
    while current <= end:
        days.append(current.strftime("%Y%m%d"))
        current += dt.timedelta(days=1)
    return days


class QualityRunner:
    def __init__(self, data_dir: Path):
        self.data_dir = Path(data_dir)

    def run(self, options: QualityRunOptions) -> QualityRunReport:
        targets = get_targets(list(options.tables))
        summaries: list[TableSummary] = []
        findings: list[QualityFinding] = []
        for target in targets:
            summary, target_findings = self._run_target(target, options)
            summaries.append(summary)
            findings.extend(target_findings)
        return QualityRunReport(options=options, summaries=summaries, findings=findings)

    def _run_target(
        self,
        target: QualityTarget,
        options: QualityRunOptions,
    ) -> tuple[TableSummary, list[QualityFinding]]:
        target_dir = self._target_dir(target)
        findings: list[QualityFinding] = []
        if not target_dir.exists():
            findings.append(
                QualityFinding(
                    table=target.table,
                    date=None,
                    severity=Severity.FAIL,
                    rule="table_directory_exists",
                    count=1,
                    message=f"table directory does not exist: {target_dir}",
                )
            )
            return self._summary(target, 0, 0, findings), findings

        expected_dates = self._expected_dates(options)
        partitions = self._partition_paths(target, expected_dates)
        rows = 0
        existing_partitions = 0
        for date_value, parquet_path in partitions:
            if not parquet_path.exists():
                findings.append(
                    QualityFinding(
                        table=target.table,
                        date=date_value,
                        severity=Severity.WARN,
                        rule="missing_partition",
                        count=1,
                        message=f"missing partition: {parquet_path}",
                    )
                )
                continue
            existing_partitions += 1
            try:
                df = pd.read_parquet(parquet_path)
            except Exception as exc:
                findings.append(
                    QualityFinding(
                        table=target.table,
                        date=date_value,
                        severity=Severity.FAIL,
                        rule="readable_parquet",
                        count=1,
                        message=f"failed to read parquet: {exc}",
                    )
                )
                continue
            rows += len(df)
            if df.empty:
                findings.append(
                    QualityFinding(
                        table=target.table,
                        date=date_value,
                        severity=Severity.WARN,
                        rule="empty_partition",
                        count=1,
                        message="partition contains zero rows",
                    )
                )
            findings.extend(self._check_frame(target, date_value, df))
        return self._summary(target, existing_partitions, rows, findings), findings

    def _target_dir(self, target: QualityTarget) -> Path:
        path = self.data_dir
        for part in target.spec.path_parts:
            path = path / part
        return path

    def _expected_dates(self, options: QualityRunOptions) -> list[str]:
        if options.mode == "full":
            if options.start_date is None or options.end_date is None:
                raise ValueError("full mode requires start_date and end_date")
            return self._trading_dates(options.start_date, options.end_date)
        if options.mode == "daily":
            if options.date is not None:
                return [options.date]
            if options.end_date is not None:
                return [options.end_date]
            if options.start_date is not None:
                return [options.start_date]
            raise ValueError("daily mode requires date when latest synced date is not provided")
        raise ValueError(f"unknown quality mode: {options.mode}")

    def _trading_dates(self, start_date: str, end_date: str) -> list[str]:
        cal_path = self.data_dir / "stock" / "trade_cal" / "exchange=SSE" / "data.parquet"
        if not cal_path.exists():
            return _date_range(start_date, end_date)
        cal = pd.read_parquet(cal_path)
        cal_dates = cal["cal_date"].astype(str).str.replace("-", "", regex=False)
        mask = (cal_dates >= start_date) & (cal_dates <= end_date) & (cal["is_open"].astype(bool))
        return sorted(cal_dates[mask].tolist())

    def _partition_paths(self, target: QualityTarget, dates: list[str]) -> list[tuple[str, Path]]:
        target_dir = self._target_dir(target)
        return [(date_value, target_dir / f"date={date_value}" / "data.parquet") for date_value in dates]

    def _check_frame(
        self,
        target: QualityTarget,
        date_value: str,
        df: pd.DataFrame,
    ) -> list[QualityFinding]:
        findings = []
        findings.extend(check_required_columns(target.table, date_value, df, target.spec.columns))
        findings.extend(check_duplicate_key(target.table, date_value, df, target.primary_key))
        if target.kind == "market_data":
            findings.extend(check_market_data_values(target.table, date_value, df))
        if target.kind == "adjustment":
            findings.extend(check_adjustment_factor_values(target.table, date_value, df))
        return findings

    def _summary(
        self,
        target: QualityTarget,
        partitions: int,
        rows: int,
        findings: list[QualityFinding],
    ) -> TableSummary:
        target_findings = [finding for finding in findings if finding.table == target.table]
        warn_count = sum(1 for finding in target_findings if finding.severity == Severity.WARN)
        fail_count = sum(1 for finding in target_findings if finding.severity == Severity.FAIL)
        pass_count = 1 if warn_count == 0 and fail_count == 0 else 0
        return TableSummary(
            table=target.table,
            market=target.market,
            partitions=partitions,
            rows=rows,
            pass_count=pass_count,
            warn_count=warn_count,
            fail_count=fail_count,
        )
```

- [ ] **Step 4: Run runner tests**

Run:

```bash
uv run pytest tests/test_quality_runner.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add zer0share/quality/runner.py tests/test_quality_runner.py
git commit -m "feat: add quality runner partition checks"
```

---

### Task 5: Adjustment Coverage and Jump Warnings

**Files:**
- Modify: `zer0share/quality/rules.py`
- Modify: `zer0share/quality/runner.py`
- Test: `tests/test_quality_rules.py`
- Test: `tests/test_quality_runner.py`

- [ ] **Step 1: Add failing tests for factor jumps and coverage**

Append to `tests/test_quality_rules.py`:

```python
from zer0share.quality.rules import check_adjustment_factor_jumps


def test_check_adjustment_factor_jumps_warns_on_large_change():
    df = pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "trade_date": "20240102", "adj_factor": 1.0},
            {"ts_code": "000001.SZ", "trade_date": "20240103", "adj_factor": 2.0},
        ]
    )

    findings = check_adjustment_factor_jumps("adj_factor", df, threshold=0.5)

    assert findings[0].severity.value == "warn"
    assert findings[0].rule == "adj_factor_jump"
```

Append to `tests/test_quality_runner.py`:

```python
def test_runner_warns_when_adjustment_coverage_is_low(tmp_path):
    _write_trade_cal(tmp_path, ["20240102"])
    _write_parquet(
        tmp_path / "stock" / "daily_kline" / "date=20240102" / "data.parquet",
        [
            {"ts_code": "000001.SZ", "trade_date": "20240102", "open": 10, "high": 10, "low": 10, "close": 10, "pre_close": 10, "change": 0, "pct_chg": 0, "vol": 1, "amount": 1},
            {"ts_code": "000002.SZ", "trade_date": "20240102", "open": 10, "high": 10, "low": 10, "close": 10, "pre_close": 10, "change": 0, "pct_chg": 0, "vol": 1, "amount": 1},
        ],
    )
    _write_parquet(
        tmp_path / "stock" / "adj_factor" / "date=20240102" / "data.parquet",
        [
            {"ts_code": "000001.SZ", "trade_date": "20240102", "adj_factor": 1.0},
        ],
    )
    runner = QualityRunner(tmp_path)

    report = runner.run(
        QualityRunOptions(
            mode="full",
            tables=("adj_factor",),
            start_date="20240102",
            end_date="20240102",
        )
    )

    assert any(finding.rule == "adjustment_market_coverage" for finding in report.findings)
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
uv run pytest tests/test_quality_rules.py tests/test_quality_runner.py -v
```

Expected: FAIL for missing `check_adjustment_factor_jumps` and missing coverage warning.

- [ ] **Step 3: Implement jump and coverage checks**

Append to `zer0share/quality/rules.py`:

```python
def check_adjustment_factor_jumps(
    table: str,
    df: pd.DataFrame,
    threshold: float = 0.5,
) -> list[QualityFinding]:
    required = {"ts_code", "trade_date", "adj_factor"}
    if not required.issubset(df.columns) or df.empty:
        return []
    ordered = df.sort_values(["ts_code", "trade_date"]).copy()
    ordered["prev_adj_factor"] = ordered.groupby("ts_code")["adj_factor"].shift(1)
    valid = ordered["prev_adj_factor"] > 0
    ordered["jump_ratio"] = (ordered["adj_factor"] / ordered["prev_adj_factor"] - 1).abs()
    jumps = ordered[valid & (ordered["jump_ratio"] > threshold)]
    if jumps.empty:
        return []
    return [
        QualityFinding(
            table=table,
            date=None,
            severity=Severity.WARN,
            rule="adj_factor_jump",
            count=len(jumps),
            message=f"adj_factor changed by more than {threshold:.0%} for a code",
            sample=_sample(jumps, ["ts_code", "trade_date", "adj_factor", "prev_adj_factor", "jump_ratio"]),
        )
    ]
```

Modify imports in `zer0share/quality/runner.py`:

```python
from zer0share.quality.rules import (
    check_adjustment_factor_jumps,
    check_adjustment_factor_values,
    check_duplicate_key,
    check_market_data_values,
    check_required_columns,
)
```

In `QualityRunner._run_target`, collect frames and call table-level checks after the loop:

```python
        frames: list[pd.DataFrame] = []
        for date_value, parquet_path in partitions:
            ...
            rows += len(df)
            frames.append(df)
            ...
        if target.kind == "adjustment" and frames:
            combined = pd.concat(frames, ignore_index=True)
            findings.extend(check_adjustment_factor_jumps(target.table, combined))
            findings.extend(self._check_adjustment_coverage(target, combined))
        return self._summary(target, existing_partitions, rows, findings), findings
```

Add this method to `QualityRunner`:

```python
    def _check_adjustment_coverage(
        self,
        target: QualityTarget,
        adjustment_df: pd.DataFrame,
    ) -> list[QualityFinding]:
        if target.related_table is None or not {"ts_code", "trade_date"}.issubset(adjustment_df.columns):
            return []
        related_target = get_targets([target.related_table])[0]
        related_dir = self._target_dir(related_target)
        findings: list[QualityFinding] = []
        for trade_date, adj_day in adjustment_df.groupby("trade_date"):
            related_path = related_dir / f"date={trade_date}" / "data.parquet"
            if not related_path.exists():
                continue
            market_df = pd.read_parquet(related_path, columns=["ts_code", "trade_date"])
            market_codes = set(market_df["ts_code"].astype(str))
            if not market_codes:
                continue
            adj_codes = set(adj_day["ts_code"].astype(str))
            coverage = len(adj_codes & market_codes) / len(market_codes)
            if coverage < 0.95:
                findings.append(
                    QualityFinding(
                        table=target.table,
                        date=str(trade_date),
                        severity=Severity.WARN,
                        rule="adjustment_market_coverage",
                        count=len(market_codes) - len(adj_codes & market_codes),
                        message=f"adjustment coverage is {coverage:.2%}, below 95.00%",
                    )
                )
        return findings
```

- [ ] **Step 4: Run tests**

Run:

```bash
uv run pytest tests/test_quality_rules.py tests/test_quality_runner.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add zer0share/quality/rules.py zer0share/quality/runner.py tests/test_quality_rules.py tests/test_quality_runner.py
git commit -m "feat: add adjustment quality warnings"
```

---

### Task 6: Quality Reporter

**Files:**
- Create: `zer0share/quality/reporter.py`
- Test: `tests/test_quality_reporter.py`

- [ ] **Step 1: Write reporter tests**

Create `tests/test_quality_reporter.py`:

```python
import json

from zer0share.quality.models import (
    QualityFinding,
    QualityRunOptions,
    QualityRunReport,
    Severity,
    TableSummary,
)
from zer0share.quality.reporter import QualityReporter, format_summary


def test_reporter_writes_summary_findings_and_metadata(tmp_path):
    report = QualityRunReport(
        options=QualityRunOptions(mode="daily", tables=("daily_kline",), date="20240102"),
        summaries=[TableSummary("daily_kline", "stock", 1, 1, 0, 0, 1)],
        findings=[
            QualityFinding("daily_kline", "20240102", Severity.FAIL, "positive_prices", 1, "bad price")
        ],
    )
    reporter = QualityReporter(tmp_path)

    output_dir = reporter.write(report)

    assert (output_dir / "summary.csv").exists()
    assert (output_dir / "findings.csv").exists()
    metadata = json.loads((output_dir / "metadata.json").read_text())
    assert metadata["mode"] == "daily"
    assert metadata["tables"] == ["daily_kline"]


def test_format_summary_includes_table_counts():
    report = QualityRunReport(
        options=QualityRunOptions(mode="daily", tables=("daily_kline",), date="20240102"),
        summaries=[TableSummary("daily_kline", "stock", 1, 1, 1, 0, 0)],
        findings=[],
    )

    text = format_summary(report)

    assert "daily_kline" in text
    assert "partitions" in text
    assert "rows" in text
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
uv run pytest tests/test_quality_reporter.py -v
```

Expected: FAIL with `ModuleNotFoundError` for `zer0share.quality.reporter`.

- [ ] **Step 3: Implement reporter**

Create `zer0share/quality/reporter.py`:

```python
from __future__ import annotations

import datetime as dt
import json
import subprocess
from pathlib import Path

import pandas as pd

from zer0share.quality.models import QualityRunReport


def _git_commit() -> str | None:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    except Exception:
        return None


def format_summary(report: QualityRunReport) -> str:
    headers = ["table", "market", "partitions", "rows", "pass", "warn", "fail"]
    rows = [summary.to_row() for summary in report.summaries]
    widths = {
        header: max(len(header), *(len(str(row[header])) for row in rows)) if rows else len(header)
        for header in headers
    }
    lines = [
        f"quality check: {report.options.mode}",
        " ".join(header.ljust(widths[header]) for header in headers),
    ]
    for row in rows:
        lines.append(" ".join(str(row[header]).ljust(widths[header]) for header in headers))
    if report.output_dir is not None:
        lines.append(f"report: {report.output_dir}")
    return "\n".join(lines)


class QualityReporter:
    def __init__(self, base_dir: Path):
        self.base_dir = Path(base_dir)

    def write(self, report: QualityRunReport) -> Path:
        timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = self.base_dir / timestamp
        output_dir.mkdir(parents=True, exist_ok=True)

        pd.DataFrame([summary.to_row() for summary in report.summaries]).to_csv(
            output_dir / "summary.csv",
            index=False,
        )
        pd.DataFrame([finding.to_row() for finding in report.findings]).to_csv(
            output_dir / "findings.csv",
            index=False,
        )
        metadata = {
            "mode": report.options.mode,
            "tables": list(report.options.tables),
            "start_date": report.options.start_date,
            "end_date": report.options.end_date,
            "date": report.options.date,
            "git_commit": _git_commit(),
            "created_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        }
        (output_dir / "metadata.json").write_text(
            json.dumps(metadata, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return output_dir
```

- [ ] **Step 4: Run reporter tests**

Run:

```bash
uv run pytest tests/test_quality_reporter.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add zer0share/quality/reporter.py tests/test_quality_reporter.py
git commit -m "feat: add quality reports"
```

---

### Task 7: CLI Integration

**Files:**
- Modify: `zer0share/cli.py`
- Test: `tests/test_cli.py`

- [ ] **Step 1: Add CLI tests**

Append to `tests/test_cli.py`:

```python
def test_quality_check_full_requires_date_range():
    runner = CliRunner()

    result = runner.invoke(cli, ["quality", "check", "--all", "--mode", "full"])

    assert result.exit_code != 0
    assert "full mode requires --start-date and --end-date" in result.output


def test_quality_check_runs_selected_table(tmp_path):
    runner = CliRunner()
    cfg = MagicMock()
    cfg.data_dir = tmp_path

    fake_report = MagicMock()
    fake_report.fail_count = 0
    fake_report.warn_count = 0
    fake_report.output_dir = tmp_path / "reports"

    with (
        patch("zer0share.cli.load_config", return_value=cfg),
        patch("zer0share.cli.QualityRunner") as runner_cls,
        patch("zer0share.cli.QualityReporter") as reporter_cls,
        patch("zer0share.cli.format_summary", return_value="quality summary"),
    ):
        runner_cls.return_value.run.return_value = fake_report
        reporter_cls.return_value.write.return_value = tmp_path / "reports"
        result = runner.invoke(
            cli,
            ["quality", "check", "--table", "daily_kline", "--mode", "daily", "--date", "20240102"],
        )

    assert result.exit_code == 0
    assert "quality summary" in result.output
    runner_cls.return_value.run.assert_called_once()
```

- [ ] **Step 2: Run CLI tests to verify failure**

Run:

```bash
uv run pytest tests/test_cli.py::test_quality_check_full_requires_date_range tests/test_cli.py::test_quality_check_runs_selected_table -v
```

Expected: FAIL because the `quality` command does not exist.

- [ ] **Step 3: Implement CLI command**

Modify imports near the top of `zer0share/cli.py`:

```python
from zer0share.quality.models import QualityRunOptions
from zer0share.quality.reporter import QualityReporter, format_summary
from zer0share.quality.runner import QualityRunner
from zer0share.quality.targets import select_targets
```

Add below the existing `sync` command:

```python
@cli.group()
def quality() -> None:
    """数据质检。"""


@quality.command("check")
@click.option("--table", "table_name", default=None)
@click.option(
    "--market",
    type=click.Choice(["stock", "index", "etf", "futures", "options"]),
    default=None,
)
@click.option("--all", "all_targets", is_flag=True, default=False)
@click.option("--mode", type=click.Choice(["full", "daily"]), default="daily", show_default=True)
@click.option("--start-date", default=None, callback=_validate_date)
@click.option("--end-date", default=None, callback=_validate_date)
@click.option("--date", "single_date", default=None, callback=_validate_date)
def quality_check(
    table_name: str | None,
    market: str | None,
    all_targets: bool,
    mode: str,
    start_date: str | None,
    end_date: str | None,
    single_date: str | None,
) -> None:
    """检查本地 Parquet 数据质量。"""
    if mode == "full" and (start_date is None or end_date is None):
        raise click.UsageError("full mode requires --start-date and --end-date")
    if mode == "daily" and (start_date is not None or end_date is not None):
        raise click.UsageError("daily mode uses --date, not --start-date/--end-date")

    try:
        targets = select_targets(table=table_name, market=market, all_targets=all_targets)
    except ValueError as exc:
        raise click.UsageError(str(exc)) from exc

    cfg = load_config(Path("config/settings.toml"))
    options = QualityRunOptions(
        mode=mode,
        tables=tuple(target.table for target in targets),
        start_date=start_date,
        end_date=end_date,
        date=single_date,
    )
    report = QualityRunner(cfg.data_dir).run(options)
    output_dir = QualityReporter(Path("reports") / "quality").write(report)
    report = QualityRunReport(
        options=report.options,
        summaries=report.summaries,
        findings=report.findings,
        output_dir=output_dir,
    )
    click.echo(format_summary(report))
    if report.fail_count:
        raise click.exceptions.Exit(1)
```

Also add `QualityRunReport` to the imports from `zer0share.quality.models`.

- [ ] **Step 4: Run CLI tests**

Run:

```bash
uv run pytest tests/test_cli.py::test_quality_check_full_requires_date_range tests/test_cli.py::test_quality_check_runs_selected_table -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add zer0share/cli.py tests/test_cli.py
git commit -m "feat: add quality check cli"
```

---

### Task 8: Config and Scheduler Integration

**Files:**
- Modify: `zer0share/config.py`
- Modify: `config/settings.example.toml`
- Modify: `zer0share/scheduler.py`
- Test: `tests/test_config.py`
- Test: `tests/test_scheduler_quality.py`

- [ ] **Step 1: Add config tests**

Append to `tests/test_config.py`:

```python
def test_load_config_defaults_quality_disabled(tmp_path):
    path = tmp_path / "settings.toml"
    path.write_text(
        """
[tushare]
token = "token"

[paths]
data_dir = "data"
db_path = "data/meta.duckdb"
log_path = "logs/app.log"

[scheduler]
daily_kline = "18:00"

[notifier]
enabled = false
wecom_webhook_url = ""
""",
        encoding="utf-8",
    )

    cfg = load_config(path)

    assert cfg.quality["enabled"] is False
    assert cfg.quality["mode"] == "daily"
    assert cfg.quality["markets"] == ["stock", "index", "etf", "futures", "options"]
    assert cfg.quality["notify_on"] == ["warn", "fail"]
```

Create `tests/test_scheduler_quality.py`:

```python
from unittest.mock import MagicMock, patch

from zer0share.scheduler import run_scheduled_table


def test_run_scheduled_table_runs_quality_without_raising(tmp_path):
    cfg = MagicMock()
    cfg.data_dir = tmp_path
    cfg.quality = {
        "enabled": True,
        "mode": "daily",
        "markets": ["stock"],
        "notify_on": ["warn", "fail"],
    }
    notifier = MagicMock()
    fetcher = MagicMock()
    report = MagicMock()
    report.warn_count = 1
    report.fail_count = 0
    report.output_dir = tmp_path / "reports"

    with (
        patch("zer0share.scheduler.Pipeline") as pipeline_cls,
        patch("zer0share.scheduler.QualityRunner") as runner_cls,
        patch("zer0share.scheduler.QualityReporter") as reporter_cls,
        patch("zer0share.scheduler.format_summary", return_value="quality summary"),
    ):
        pipeline_cls.return_value.__enter__.return_value = pipeline_cls.return_value
        pipeline_cls.return_value.__exit__.return_value = False
        runner_cls.return_value.run.return_value = report
        reporter_cls.return_value.write.return_value = tmp_path / "reports"

        run_scheduled_table(cfg, fetcher, notifier, "daily_kline")

    pipeline_cls.return_value.run.assert_called_once_with("daily_kline")
    notifier.send.assert_called()
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
uv run pytest tests/test_config.py::test_load_config_defaults_quality_disabled tests/test_scheduler_quality.py -v
```

Expected: FAIL because `Config.quality` and `run_scheduled_table` do not exist.

- [ ] **Step 3: Implement config parsing**

Modify `zer0share/config.py`:

```python
@dataclass(frozen=True)
class Config:
    tushare_token: str
    data_dir: Path
    db_path: Path
    log_path: Path
    schedule: dict[str, str]
    wecom_webhook_url: str
    notifier_enabled: bool
    quality: dict
```

Add helper:

```python
def _parse_quality(raw_quality: dict | None) -> dict:
    raw_quality = raw_quality or {}
    return {
        "enabled": bool(raw_quality.get("enabled", False)),
        "mode": raw_quality.get("mode", "daily"),
        "markets": list(raw_quality.get("markets", ["stock", "index", "etf", "futures", "options"])),
        "notify_on": list(raw_quality.get("notify_on", ["warn", "fail"])),
    }
```

Pass it from `load_config`:

```python
quality=_parse_quality(raw.get("quality")),
```

- [ ] **Step 4: Implement scheduler hook**

Modify `zer0share/scheduler.py` imports:

```python
from zer0share.quality.models import QualityRunOptions, QualityRunReport
from zer0share.quality.reporter import QualityReporter, format_summary
from zer0share.quality.runner import QualityRunner
from zer0share.quality.targets import QUALITY_TARGETS
```

Add top-level helper:

```python
def _quality_tables_for_sync(table_name: str, markets: list[str]) -> tuple[str, ...]:
    target = QUALITY_TARGETS.get(table_name)
    if target is None or target.market not in markets:
        return ()
    return (target.table,)


def run_scheduled_table(cfg, fetcher, notifier: Notifier, table_name: str) -> None:
    with Pipeline(cfg, fetcher, notifier) as pipeline:
        pipeline.run(table_name)

    quality_cfg = getattr(cfg, "quality", {})
    if not quality_cfg.get("enabled", False):
        return

    tables = _quality_tables_for_sync(table_name, quality_cfg.get("markets", []))
    if not tables:
        return

    try:
        options = QualityRunOptions(mode="daily", tables=tables)
        report = QualityRunner(cfg.data_dir).run(options)
        output_dir = QualityReporter(Path("reports") / "quality").write(report)
        report = QualityRunReport(
            options=report.options,
            summaries=report.summaries,
            findings=report.findings,
            output_dir=output_dir,
        )
        notify_on = set(quality_cfg.get("notify_on", ["warn", "fail"]))
        should_notify = ("fail" in notify_on and report.fail_count) or ("warn" in notify_on and report.warn_count)
        if should_notify:
            notifier.send(f"数据质检发现问题\n{format_summary(report)}")
    except Exception as exc:
        logger.error(f"quality check failed after {table_name}: {exc}")
        notifier.send(f"数据质检执行失败\n表：{table_name}\n错误：{exc}")
```

In `start_scheduler`, replace the nested `run_table` body with:

```python
    def run_table(table_name: str) -> None:
        run_scheduled_table(cfg, fetcher, notifier, table_name)
```

- [ ] **Step 5: Update example config**

Append to `config/settings.example.toml`:

```toml
[quality]
enabled = false
mode = "daily"
markets = ["stock", "index", "etf", "futures", "options"]
notify_on = ["warn", "fail"]
```

- [ ] **Step 6: Run config and scheduler tests**

Run:

```bash
uv run pytest tests/test_config.py::test_load_config_defaults_quality_disabled tests/test_scheduler_quality.py -v
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add zer0share/config.py config/settings.example.toml zer0share/scheduler.py tests/test_config.py tests/test_scheduler_quality.py
git commit -m "feat: run quality checks from scheduler"
```

---

### Task 9: End-to-End Verification and Documentation

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Add README usage section**

Add a short section after the status command section in `README.md`:

```markdown
### 数据质检

本地质检只读取 Parquet 文件，不访问 Tushare，也不消耗积分。

```bash
uv run python main.py quality check --all --mode full --start-date 20200101 --end-date 20241231
uv run python main.py quality check --market stock --mode daily
uv run python main.py quality check --table daily_kline --mode daily --date 20240701
```

终端输出表级摘要，明细报告写入 `reports/quality/YYYYMMDD_HHMMSS/`。
```

- [ ] **Step 2: Run focused quality tests**

Run:

```bash
uv run pytest tests/test_quality_targets.py tests/test_quality_rules.py tests/test_quality_runner.py tests/test_quality_reporter.py -v
```

Expected: PASS.

- [ ] **Step 3: Run CLI/config/scheduler tests**

Run:

```bash
uv run pytest tests/test_cli.py tests/test_config.py tests/test_scheduler_quality.py -v
```

Expected: PASS.

- [ ] **Step 4: Run full test suite**

Run:

```bash
uv run pytest -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add README.md
git commit -m "docs: document quality checks"
```

---

## Self-Review

Spec coverage:

- Local-only quality checks are covered by Tasks 1 through 7.
- Scoped tables are covered by Task 2.
- Structural, partition, market data, and adjustment rules are covered by Tasks 3 through 5.
- CLI usage and exit behavior are covered by Task 7.
- Report files are covered by Task 6.
- Scheduler notification without interruption is covered by Task 8.
- Tests and documentation are covered by Task 9.

Placeholder scan:

- The plan contains no placeholder markers or unspecified implementation steps.
- Each code-changing step includes concrete code or exact file text to add.

Type consistency:

- `QualityFinding`, `QualityRunOptions`, `QualityRunReport`, and `TableSummary` are defined in Task 1 before use.
- `QualityTarget`, `QUALITY_TARGETS`, `get_targets`, and `select_targets` are defined in Task 2 before use.
- Rule function names used by the runner match Task 3 and Task 5 definitions.
- CLI imports match the files created by earlier tasks.
