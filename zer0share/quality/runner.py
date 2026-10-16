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
    check_adjustment_factor_jumps,
    check_coverage,
    check_duplicate_key,
    check_market_data_values,
    WELL_KNOWN_ETF_CODES,
    check_required_columns,
)
from zer0share.quality.targets import QualityTarget, get_targets

AdjustedReturnState = dict[str, tuple[str, float]]


def _date_range(start_date: str, end_date: str) -> list[str]:
    start = dt.datetime.strptime(start_date, "%Y%m%d").date()
    end = dt.datetime.strptime(end_date, "%Y%m%d").date()
    if end < start:
        raise ValueError("end_date must be on or after start_date")
    days: list[str] = []
    current = start
    while current <= end:
        days.append(current.strftime("%Y%m%d"))
        current += dt.timedelta(days=1)
    return days


def _normalize_dates(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series.astype(str), errors="coerce").dt.strftime("%Y%m%d")


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

        expected_dates = self._expected_dates(target, options)
        rows = 0
        partitions = 0
        adjusted_return_state: AdjustedReturnState = {}

        # load basic reference table for coverage checks (once per target)
        self._basic_df: pd.DataFrame | None = None
        if target.table == "fund_daily":
            basic_path = self.data_dir / "etf" / "etf_basic" / "data.parquet"
        elif target.table == "daily_kline":
            basic_path = self.data_dir / "stock" / "basic" / "data.parquet"
        else:
            basic_path = None

        if basic_path is not None and basic_path.exists():
            try:
                self._basic_df = pd.read_parquet(basic_path)
            except Exception:
                self._basic_df = None

        for date_value in expected_dates:
            parquet_path = target_dir / f"date={date_value}" / "data.parquet"
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

            try:
                df = pd.read_parquet(parquet_path)
            except Exception as exc:  # pragma: no cover - exercised through integration tests
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

            partitions += 1
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
            if target.kind == "adjustment":
                findings.extend(self._check_adjustment_market_coverage(target, date_value, df))
                findings.extend(
                    self._check_adjusted_return_jumps(
                        target,
                        date_value,
                        df,
                        adjusted_return_state,
                    )
                )

        return self._summary(target, partitions, rows, findings), findings

    def _target_dir(self, target: QualityTarget) -> Path:
        path = self.data_dir
        for part in target.spec.path_parts:
            path = path / part
        return path

    def _expected_dates(self, target: QualityTarget, options: QualityRunOptions) -> list[str]:
        if options.mode == "full":
            if options.start_date is None or options.end_date is None:
                raise ValueError("full mode requires start_date and end_date")
            return self._trading_dates(target, options.start_date, options.end_date)

        if options.mode == "daily":
            if options.date is not None:
                return [options.date]
            if options.end_date is not None:
                return [options.end_date]
            if options.start_date is not None:
                return [options.start_date]
            raise ValueError("daily mode requires date, end_date, or start_date")

        raise ValueError(f"unknown quality mode: {options.mode}")

    def _calendar_exchanges(self, target: QualityTarget) -> set[str] | None:
        if target.market in {"stock", "index", "etf"}:
            return {"SSE", "SZSE"}
        if target.market == "futures":
            return {"CFFEX", "CZCE", "DCE", "GFEX", "INE", "SHFE"}
        return None

    def _trading_dates(self, target: QualityTarget, start_date: str, end_date: str) -> list[str]:
        trade_cal_dir = self.data_dir / "stock" / "trade_cal"
        parquet_paths = sorted(trade_cal_dir.glob("exchange=*/data.parquet"))
        if not parquet_paths:
            return _date_range(start_date, end_date)

        allowed_exchanges = self._calendar_exchanges(target)
        frames: list[pd.DataFrame] = []
        for parquet_path in parquet_paths:
            exchange = parquet_path.parent.name.removeprefix("exchange=")
            if allowed_exchanges is not None and exchange not in allowed_exchanges:
                continue
            try:
                frame = pd.read_parquet(parquet_path)
            except Exception:
                continue
            if not frame.empty:
                frames.append(frame)

        if not frames:
            return _date_range(start_date, end_date)

        cal = pd.concat(frames, ignore_index=True)
        if "cal_date" not in cal.columns or "is_open" not in cal.columns:
            return _date_range(start_date, end_date)

        cal_dates = _normalize_dates(cal["cal_date"])
        is_open = cal["is_open"].fillna(False).astype(bool)
        mask = (
            cal_dates.notna()
            & (cal_dates >= start_date)
            & (cal_dates <= end_date)
            & is_open
        )
        return sorted(set(cal_dates[mask].tolist()))

    def _expected_codes_for_date(self, target: QualityTarget, date_value: str) -> set[str]:
        """Return the set of ts_code expected to exist for this target on date_value."""
        if self._basic_df is None:
            return set()

        df = self._basic_df
        if "ts_code" not in df.columns or "list_date" not in df.columns:
            return set()

        if target.table == "fund_daily":
            mask = pd.Series(True, index=df.index)
            if "list_status" in df.columns:
                mask = mask & (df["list_status"] != "D")
            mask = mask & df["list_date"].notna()
            mask = mask & (df["list_date"].astype(str).str.strip() <= date_value)
        elif target.table == "daily_kline":
            mask = pd.Series(True, index=df.index)
            if "list_status" in df.columns:
                mask = mask & (df["list_status"] == "L")
            mask = mask & df["list_date"].notna()
            mask = mask & (df["list_date"].astype(str).str.strip() <= date_value)
            if "delist_date" in df.columns:
                delist_na = df["delist_date"].isna() | (df["delist_date"].astype(str).str.strip() == "")
                delist_future = df["delist_date"].astype(str).str.strip() > date_value
                mask = mask & (delist_na | delist_future)
        else:
            return set()

        return set(df.loc[mask, "ts_code"].dropna().astype(str))

    def _check_frame(
        self,
        target: QualityTarget,
        date_value: str,
        df: pd.DataFrame,
    ) -> list[QualityFinding]:
        findings: list[QualityFinding] = []
        findings.extend(check_required_columns(target.table, date_value, df, list(target.spec.columns)))
        findings.extend(check_duplicate_key(target.table, date_value, df, target.primary_key))
        if target.kind == "market_data":
            findings.extend(check_market_data_values(target.table, date_value, df))
            findings.extend(
                check_coverage(
                    target.table,
                    date_value,
                    df,
                    self._expected_codes_for_date(target, date_value),
                    WELL_KNOWN_ETF_CODES if target.table == "fund_daily" else None,
                )
            )
        elif target.kind == "adjustment":
            findings.extend(check_adjustment_factor_values(target.table, date_value, df))
            findings.extend(check_adjustment_factor_jumps(target.table, df))
        return findings

    def _check_adjustment_market_coverage(
        self,
        target: QualityTarget,
        date_value: str,
        adjustment_df: pd.DataFrame,
    ) -> list[QualityFinding]:
        if target.related_table is None:
            return []

        related_target = get_targets([target.related_table])[0]
        related_dir = self._target_dir(related_target)
        related_path = related_dir / f"date={date_value}" / "data.parquet"
        if not related_path.exists():
            return []

        try:
            market_df = pd.read_parquet(related_path)
        except Exception:  # pragma: no cover - exercised through integration tests
            return []

        if "ts_code" not in adjustment_df.columns or "ts_code" not in market_df.columns:
            return []

        adjustment_codes = set(adjustment_df["ts_code"].dropna().astype(str))
        market_codes = set(market_df["ts_code"].dropna().astype(str))
        if not adjustment_codes or not market_codes:
            return []

        shared_codes = adjustment_codes & market_codes
        coverage = len(shared_codes) / len(market_codes)
        if coverage >= 0.95:
            return []

        missing_count = len(market_codes - adjustment_codes)
        return [
            QualityFinding(
                table=target.table,
                date=date_value,
                severity=Severity.WARN,
                rule="adjustment_market_coverage",
                count=missing_count,
                message=(
                    f"distinct ts_code coverage against {related_target.table} is {coverage:.1%}, "
                    "below the 95% warning threshold"
                ),
                sample=[
                    {
                        "adjustment_ts_codes": len(adjustment_codes),
                        "market_ts_codes": len(market_codes),
                        "shared_ts_codes": len(shared_codes),
                        "missing_market_ts_codes": missing_count,
                    }
                ],
            )
        ]

    def _check_adjusted_return_jumps(
        self,
        target: QualityTarget,
        date_value: str,
        adjustment_df: pd.DataFrame,
        state: AdjustedReturnState,
    ) -> list[QualityFinding]:
        if target.related_table is None:
            return []

        related_target = get_targets([target.related_table])[0]
        related_dir = self._target_dir(related_target)
        related_path = related_dir / f"date={date_value}" / "data.parquet"
        if not related_path.exists():
            return []

        try:
            market_df = pd.read_parquet(related_path, columns=["ts_code", "trade_date", "close"])
        except Exception:  # pragma: no cover - exercised through integration tests
            return []

        required_adjustment_columns = {"ts_code", "trade_date", "adj_factor"}
        if not required_adjustment_columns.issubset(adjustment_df.columns):
            return []
        if not {"ts_code", "trade_date", "close"}.issubset(market_df.columns):
            return []

        current = market_df.merge(
            adjustment_df[["ts_code", "trade_date", "adj_factor"]],
            on=["ts_code", "trade_date"],
            how="inner",
        )
        if current.empty:
            return []

        current = current[~current["ts_code"].astype(str).str.endswith(".BJ")]
        if current.empty:
            return []

        current["close"] = pd.to_numeric(current["close"], errors="coerce")
        current["adj_factor"] = pd.to_numeric(current["adj_factor"], errors="coerce")
        current["adj_close"] = current["close"] * current["adj_factor"]
        current = current[current["adj_close"].notna() & (current["adj_close"] > 0)]
        if current.empty:
            return []

        threshold = self._adjusted_return_threshold(target)
        jumps: list[dict[str, object]] = []
        for row in current.itertuples(index=False):
            ts_code = str(row.ts_code)
            adj_close = float(row.adj_close)
            previous = state.get(ts_code)
            if previous is not None:
                prev_date, prev_adj_close = previous
                adjusted_return = adj_close / prev_adj_close - 1
                if abs(adjusted_return) > threshold:
                    jumps.append(
                        {
                            "ts_code": ts_code,
                            "trade_date": str(row.trade_date),
                            "close": float(row.close),
                            "adj_factor": float(row.adj_factor),
                            "adj_close": adj_close,
                            "prev_trade_date": prev_date,
                            "prev_adj_close": prev_adj_close,
                            "adjusted_return": round(adjusted_return, 6),
                        }
                    )
            state[ts_code] = (str(row.trade_date), adj_close)

        if not jumps:
            return []

        return [
            QualityFinding(
                table=target.table,
                date=date_value,
                severity=Severity.WARN,
                rule="adjusted_return_jump",
                count=len(jumps),
                message=(
                    f"absolute adjusted close return exceeds {threshold:.0%}; "
                    "check market close and adjustment factor continuity"
                ),
                sample=jumps[:5],
            )
        ]

    def _adjusted_return_threshold(self, target: QualityTarget) -> float:
        if target.market == "stock":
            return 0.35
        if target.market == "etf":
            return 0.50
        return 0.50

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
        pass_count = 1 if warn_count == 0 and fail_count == 0 and partitions > 0 else 0
        return TableSummary(
            table=target.table,
            market=target.market,
            partitions=partitions,
            rows=rows,
            pass_count=pass_count,
            warn_count=warn_count,
            fail_count=fail_count,
        )
