import datetime as dt
from unittest.mock import MagicMock

import pytest

from zer0share.digest import DigestNotifier


class FakeClock:
    def __init__(self, now: dt.datetime):
        self.now = now

    def __call__(self) -> dt.datetime:
        return self.now


@pytest.fixture
def clock():
    return FakeClock(dt.datetime(2026, 11, 6, 18, 10))


def make_digest(clock, schedule=None):
    inner = MagicMock()
    schedule = schedule or {"daily_kline": "16:10", "fut_wsr": "17:32", "opt_daily": "17:56"}
    digest = DigestNotifier(inner, schedule, clock=clock, window_start=dt.datetime(2026, 11, 6, 0, 0))
    return digest, inner


def test_success_messages_are_buffered_not_sent(clock):
    digest, inner = make_digest(clock)

    digest.run_table("daily_kline", lambda: digest.send_success("daily_kline 同步完成\n日期：20261106｜5300 行"))

    inner.send.assert_not_called()
    inner.send_success.assert_not_called()


def test_failure_messages_are_forwarded_immediately(clock):
    digest, inner = make_digest(clock)

    def job():
        digest.send("fut_wsr 同步失败\nboom")
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        digest.run_table("fut_wsr", job)

    inner.send.assert_called_once_with("fut_wsr 同步失败\nboom")


def test_unannounced_exception_triggers_an_alert(clock):
    digest, inner = make_digest(clock)

    def job():
        raise RuntimeError("connection reset")

    with pytest.raises(RuntimeError):
        digest.run_table("opt_daily", job)

    inner.send.assert_called_once()
    msg = inner.send.call_args[0][0]
    assert "opt_daily" in msg
    assert "connection reset" in msg


def test_flush_summarises_success_failure_and_missing(clock):
    digest, inner = make_digest(clock)
    digest.run_table("daily_kline", lambda: digest.send_success("daily_kline 同步完成\n日期：20261106｜5300 行"))

    def failing():
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        digest.run_table("fut_wsr", failing)
    inner.reset_mock()

    digest.flush()

    inner.send.assert_called_once()
    msg = inner.send.call_args[0][0]
    assert "每日同步汇总" in msg
    assert "成功 1/3" in msg
    assert "失败 1" in msg
    assert "未运行 1" in msg
    assert "fut_wsr" in msg and "boom" in msg
    assert "opt_daily" in msg
    assert "daily_kline" in msg and "5300 行" in msg


def test_job_without_success_message_counts_as_no_new_data(clock):
    digest, inner = make_digest(clock, schedule={"adj_factor": "09:25"})
    digest.run_table("adj_factor", lambda: None)

    digest.flush()

    msg = inner.send.call_args[0][0]
    assert "成功 1/1" in msg
    assert "无新数据" in msg


def test_flush_resets_window(clock):
    digest, inner = make_digest(clock, schedule={"daily_kline": "16:10"})
    digest.run_table("daily_kline", lambda: digest.send_success("daily_kline 同步完成"))
    digest.flush()
    inner.reset_mock()

    clock.now = dt.datetime(2026, 11, 7, 18, 10)
    digest.flush()

    msg = inner.send.call_args[0][0]
    assert "成功 0/1" in msg
    assert "未运行 1" in msg


def test_tables_not_due_yet_are_not_reported_missing():
    clock = FakeClock(dt.datetime(2026, 11, 6, 18, 10))
    inner = MagicMock()
    digest = DigestNotifier(
        inner,
        {"basic": "02:05", "daily_kline": "16:10"},
        clock=clock,
        window_start=dt.datetime(2026, 11, 6, 12, 0),  # scheduler started at noon
    )
    digest.run_table("daily_kline", lambda: None)

    digest.flush()

    msg = inner.send.call_args[0][0]
    assert "成功 1/1" in msg
    assert "basic" not in msg


def test_running_table_is_reported_as_running(clock):
    digest, inner = make_digest(clock, schedule={"ricequant_stock_minute": "16:30"})

    def job():
        digest.flush()

    digest.run_table("ricequant_stock_minute", job)

    msg = inner.send.call_args_list[0][0][0]
    assert "运行中 1" in msg


def test_alert_without_exception_is_reported_as_warning(clock):
    digest, inner = make_digest(clock, schedule={"index_daily": "17:11"})

    def job():
        digest.send("index_daily 拉取失败\n指数：000300.SH｜timeout")
        digest.send_success("index_daily 同步完成\n写入 1 天")

    digest.run_table("index_daily", job)
    inner.reset_mock()
    digest.flush()

    msg = inner.send.call_args[0][0]
    assert "成功 0/1" in msg
    assert "有告警 1" in msg
    assert "完成但有告警" in msg


def test_send_success_outside_scheduled_run_passes_through(clock):
    digest, inner = make_digest(clock)
    digest.send_success("manual run 同步完成")
    inner.send_success.assert_called_once_with("manual run 同步完成")


# --- persistence ----------------------------------------------------------

def make_persistent(clock, state_path, schedule=None, digest_time="18:10"):
    inner = MagicMock()
    digest = DigestNotifier(
        inner,
        schedule or {"daily_kline": "16:10", "fut_wsr": "17:32"},
        clock=clock,
        window_start=dt.datetime(2026, 11, 6, 0, 0),
        state_path=state_path,
        digest_time=digest_time,
    )
    return digest, inner


def test_state_survives_restart(tmp_path):
    state = tmp_path / "digest_state.json"
    clock = FakeClock(dt.datetime(2026, 11, 6, 16, 20))
    digest, _ = make_persistent(clock, state)
    digest.run_table("daily_kline", lambda: digest.send_success("daily_kline 同步完成\n写入 5300 条"))
    assert state.exists()

    clock.now = dt.datetime(2026, 11, 6, 17, 0)  # restarted before digest time
    restarted, inner = make_persistent(clock, state)
    inner.send.assert_not_called()
    clock.now = dt.datetime(2026, 11, 6, 18, 10)
    restarted.flush()

    msg = inner.send.call_args[0][0]
    assert "daily_kline: 写入 5300 条" in msg
    assert "未运行 1" in msg  # fut_wsr


def test_restart_after_digest_time_sends_overdue_digest(tmp_path):
    state = tmp_path / "digest_state.json"
    clock = FakeClock(dt.datetime(2026, 11, 6, 16, 20))
    digest, _ = make_persistent(clock, state)
    digest.run_table("daily_kline", lambda: digest.send_success("daily_kline 同步完成\n写入 5300 条"))

    clock.now = dt.datetime(2026, 11, 6, 19, 0)  # crashed before 18:10, back at 19:00
    restarted, inner = make_persistent(clock, state)

    inner.send.assert_called_once()
    msg = inner.send.call_args[0][0]
    assert "补发" in msg
    assert "每日同步汇总 2026-11-06" in msg
    assert "daily_kline: 写入 5300 条" in msg

    inner.reset_mock()
    clock.now = dt.datetime(2026, 11, 7, 18, 10)
    restarted.flush()
    assert "daily_kline: 写入" not in inner.send.call_args[0][0]


def test_run_interrupted_by_restart_is_reported_failed(tmp_path):
    state = tmp_path / "digest_state.json"
    clock = FakeClock(dt.datetime(2026, 11, 6, 16, 20))
    digest, _ = make_persistent(clock, state)

    class Crash(BaseException):
        pass

    def job():
        raise Crash()  # simulates the process dying mid-run

    with pytest.raises(Crash):
        digest.run_table("daily_kline", job)

    restarted, inner = make_persistent(clock, state)
    restarted.flush()
    msg = inner.send.call_args[0][0]
    assert "失败 1" in msg
    assert "中断" in msg


def test_corrupt_state_file_is_ignored(tmp_path):
    state = tmp_path / "digest_state.json"
    state.write_text("{not json", encoding="utf-8")
    clock = FakeClock(dt.datetime(2026, 11, 6, 18, 10))

    digest, inner = make_persistent(clock, state)
    digest.run_table("daily_kline", lambda: None)
    digest.flush()

    assert "成功 1/" in inner.send.call_args[0][0]


def test_flush_persists_cleared_state(tmp_path):
    state = tmp_path / "digest_state.json"
    clock = FakeClock(dt.datetime(2026, 11, 6, 18, 10))
    digest, _ = make_persistent(clock, state)
    digest.run_table("daily_kline", lambda: None)
    digest.flush()

    import json
    saved = json.loads(state.read_text(encoding="utf-8"))
    assert saved["runs"] == {}
    assert saved["window_start"] == "2026-11-06T18:10:00"
