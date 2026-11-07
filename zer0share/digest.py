"""Daily digest for scheduled syncs.

Failures are forwarded to the real notifier immediately; success messages are
buffered and sent as a single summary per day. The summary also doubles as a
heartbeat: if it does not arrive, the scheduler itself is in trouble.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from loguru import logger

from zer0share.notifier import NotifierProtocol

_OK = "ok"
_FAILED = "failed"
_RUNNING = "running"
_WARN = "warn"


@dataclass
class _TableRun:
    status: str
    detail: str = ""


def _condense(message: str) -> str:
    """Drop the '<table> 同步完成' headline, keep the details on one line."""
    lines = [line.strip() for line in message.splitlines() if line.strip()]
    if len(lines) > 1:
        lines = lines[1:]
    return "｜".join(lines)


def _first_line(text: str, limit: int = 200) -> str:
    line = text.strip().splitlines()[0] if text.strip() else ""
    return line if len(line) <= limit else line[: limit - 1] + "…"


def _first_due(hhmm: str, start: dt.datetime, end: dt.datetime) -> dt.datetime | None:
    """First occurrence of HH:MM in (start, end], or None."""
    hour, minute = (int(x) for x in hhmm.split(":"))
    day = start.date()
    while day <= end.date():
        at = dt.datetime.combine(day, dt.time(hour, minute))
        if start < at <= end:
            return at
        day += dt.timedelta(days=1)
    return None


def _due_between(hhmm: str, start: dt.datetime, end: dt.datetime) -> bool:
    return _first_due(hhmm, start, end) is not None


class DigestNotifier:
    def __init__(
        self,
        inner: NotifierProtocol,
        schedule: dict[str, str],
        *,
        clock: Callable[[], dt.datetime] = dt.datetime.now,
        window_start: dt.datetime | None = None,
        state_path: Path | None = None,
        digest_time: str | None = None,
    ):
        self._inner = inner
        self._schedule = dict(schedule)
        self._clock = clock
        self._window_start = window_start or clock()
        self._runs: dict[str, _TableRun] = {}
        self._lock = threading.Lock()
        self._local = threading.local()
        self._state_path = Path(state_path) if state_path is not None else None

        if self._restore():
            now = clock()
            missed = _first_due(digest_time, self._window_start, now) if digest_time else None
            if missed is not None:
                self.flush(label_date=missed.date(), overdue=True)

    # --- NotifierProtocol -------------------------------------------------

    def send(self, message: str) -> None:
        self._local.alerted = True
        self._inner.send(message)

    def send_success(self, message: str) -> None:
        table = getattr(self._local, "table", None)
        if table is None:
            self._inner.send_success(message)
            return
        self._local.success_detail = _condense(message)

    def notify_start(self, stage: str, details: dict | None = None) -> None:
        self._inner.notify_start(stage, details)

    def notify_progress(self, stage: str, done: int, total: int) -> None:
        self._inner.notify_progress(stage, done, total)

    def notify_stage_done(self, stage: str, summary: dict, elapsed: float) -> None:
        self._inner.notify_stage_done(stage, summary, elapsed)

    def notify_error(self, stage: str, error) -> None:
        self._local.alerted = True
        self._inner.notify_error(stage, error)

    # --- scheduling hooks -------------------------------------------------

    def run_table(self, table_name: str, fn: Callable[[], None]) -> None:
        self._local.table = table_name
        self._local.alerted = False
        self._local.success_detail = ""
        self._set_run(table_name, _TableRun(_RUNNING))
        try:
            fn()
        except Exception as exc:
            detail = f"{type(exc).__name__}: {exc}"
            self._set_run(table_name, _TableRun(_FAILED, _first_line(detail)))
            if not self._local.alerted:
                self._inner.send(f"{table_name} 同步失败\n{detail}")
            raise
        else:
            status = _WARN if self._local.alerted else _OK
            self._set_run(table_name, _TableRun(status, self._local.success_detail or "无新数据"))
        finally:
            self._local.table = None

    def flush(self, *, label_date: dt.date | None = None, overdue: bool = False) -> None:
        now = self._clock()
        with self._lock:
            runs = dict(self._runs)
            window_start = self._window_start
            self._runs = {k: v for k, v in runs.items() if v.status == _RUNNING}
            self._window_start = now
            self._save()
        self._inner.send(self._format(runs, window_start, now, label_date or now.date(), overdue))

    # --- persistence ------------------------------------------------------

    def _set_run(self, table_name: str, run: _TableRun) -> None:
        with self._lock:
            self._runs[table_name] = run
            self._save()

    def _save(self) -> None:
        """Write state atomically. Caller must hold the lock."""
        if self._state_path is None:
            return
        payload = {
            "window_start": self._window_start.isoformat(),
            "runs": {t: {"status": r.status, "detail": r.detail} for t, r in self._runs.items()},
        }
        tmp = self._state_path.with_name(self._state_path.name + ".tmp")
        try:
            self._state_path.parent.mkdir(parents=True, exist_ok=True)
            tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            os.replace(tmp, self._state_path)
        except OSError as exc:
            logger.warning(f"digest 状态写入失败 {self._state_path}: {exc}")

    def _restore(self) -> bool:
        """Load state left by a previous process. Returns True if anything was restored."""
        if self._state_path is None or not self._state_path.exists():
            return False
        try:
            payload = json.loads(self._state_path.read_text(encoding="utf-8"))
            window_start = dt.datetime.fromisoformat(payload["window_start"])
            runs = {
                str(t): _TableRun(str(r["status"]), str(r.get("detail", "")))
                for t, r in payload.get("runs", {}).items()
            }
        except (OSError, ValueError, KeyError, TypeError, AttributeError) as exc:
            logger.warning(f"digest 状态文件无法读取，忽略 {self._state_path}: {exc}")
            return False
        for table, run in runs.items():
            if run.status == _RUNNING:
                runs[table] = _TableRun(_FAILED, "调度器重启，运行被中断")
        with self._lock:
            self._window_start = window_start
            self._runs = runs
            self._save()
        logger.info(f"digest 状态已恢复: {len(runs)} 条记录，周期起点 {window_start:%Y-%m-%d %H:%M}")
        return True

    def _format(
        self,
        runs: dict[str, _TableRun],
        start: dt.datetime,
        end: dt.datetime,
        label_date: dt.date,
        overdue: bool = False,
    ) -> str:
        expected = [t for t, hhmm in self._schedule.items() if _due_between(hhmm, start, end)]
        ok = [(t, r) for t, r in runs.items() if r.status == _OK]
        warned = [(t, r) for t, r in runs.items() if r.status == _WARN]
        failed = [(t, r) for t, r in runs.items() if r.status == _FAILED]
        running = [t for t, r in runs.items() if r.status == _RUNNING]
        missing = [t for t in expected if t not in runs]
        total = len(set(expected) | set(runs))

        lines = [
            f"每日同步汇总 {label_date:%Y-%m-%d}" + ("（补发）" if overdue else ""),
            f"成功 {len(ok)}/{total}｜失败 {len(failed)}｜未运行 {len(missing)}"
            + (f"｜有告警 {len(warned)}" if warned else "")
            + (f"｜运行中 {len(running)}" if running else ""),
        ]
        if failed:
            lines.append("")
            lines.append("失败：")
            lines.extend(f"- {t}: {r.detail}" for t, r in failed)
        if warned:
            lines.append("")
            lines.append("完成但有告警：")
            lines.extend(f"- {t}: {r.detail}" for t, r in warned)
        if missing:
            lines.append("")
            lines.append("未运行：")
            lines.extend(f"- {t}" for t in missing)
        if running:
            lines.append("")
            lines.append("运行中：")
            lines.extend(f"- {t}" for t in running)
        if ok:
            lines.append("")
            lines.append("成功：")
            lines.extend(f"- {t}: {r.detail}" for t, r in ok)
        return "\n".join(lines)
