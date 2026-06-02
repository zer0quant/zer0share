from unittest.mock import MagicMock, patch


VALID_CONFIG = """
[tushare]
token = "test"

[paths]
data_dir = "data"
db_path = "db/meta.duckdb"
log_path = "logs/pipeline.log"

[scheduler]
daily_kline_hour = 18
daily_kline_minute = 0
basic_hour = 8
adj_factor_hour = 18
adj_factor_minute = 5
futures_hour = 17
futures_start_minute = 0

[notifier]
wecom_webhook_url = "https://example.com"
enabled = false
"""


def test_start_scheduler_registers_two_jobs(tmp_path):
    cfg_file = tmp_path / "settings.toml"
    cfg_file.write_text(VALID_CONFIG, encoding="utf-8")

    registered_jobs = []

    def fake_add_job(func, trigger, id=None, **kwargs):
        registered_jobs.append(id)

    with (
        patch("tushare.pro_api"),
        patch("apscheduler.schedulers.blocking.BlockingScheduler.start"),
        patch(
            "apscheduler.schedulers.blocking.BlockingScheduler.add_job",
            side_effect=fake_add_job,
        ),
        patch("zer0share.scheduler.Pipeline") as mock_pipeline_cls,
    ):
        mock_pipeline_cls.return_value.__enter__ = lambda s: s
        mock_pipeline_cls.return_value.__exit__ = MagicMock(return_value=False)
        from zer0share.scheduler import start_scheduler

        start_scheduler(str(cfg_file))

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


def test_start_scheduler_registers_basic_job_as_daily(tmp_path):
    cfg_file = tmp_path / "settings.toml"
    cfg_file.write_text(VALID_CONFIG, encoding="utf-8")

    cron_calls = []

    def fake_cron_trigger(**kwargs):
        cron_calls.append(kwargs)
        return MagicMock()

    with (
        patch("tushare.pro_api"),
        patch("zer0share.scheduler.CronTrigger", side_effect=fake_cron_trigger),
        patch("apscheduler.schedulers.blocking.BlockingScheduler.start"),
        patch("apscheduler.schedulers.blocking.BlockingScheduler.add_job"),
        patch("zer0share.scheduler.Pipeline") as mock_pipeline_cls,
    ):
        mock_pipeline_cls.return_value.__enter__ = lambda s: s
        mock_pipeline_cls.return_value.__exit__ = MagicMock(return_value=False)
        from zer0share.scheduler import start_scheduler

        start_scheduler(str(cfg_file))

    assert cron_calls[0] == {"hour": 18, "minute": 0}
    assert cron_calls[1] == {"hour": 18, "minute": 0}
    assert cron_calls[2] == {"hour": 8}
    assert cron_calls[3] == {"hour": 18, "minute": 5}
    assert cron_calls[4] == {"hour": 17, "minute": 0}
    assert cron_calls[5] == {"hour": 17, "minute": 10}
    assert cron_calls[6] == {"hour": 17, "minute": 20}
    assert cron_calls[7] == {"hour": 17, "minute": 30}
    assert cron_calls[8] == {"hour": 17, "minute": 40}
    assert cron_calls[9] == {"hour": 17, "minute": 50}
    assert cron_calls[10] == {"hour": 18, "minute": 0}
    assert cron_calls[11] == {"hour": 18, "minute": 10}
    assert cron_calls[12] == {"hour": 18, "minute": 20}
    assert cron_calls[13] == {"hour": 18, "minute": 30}
    assert cron_calls[14] == {"hour": 18, "minute": 40}
    assert cron_calls[15] == {"hour": 18, "minute": 50}   # opt_basic (offset 110)
    assert cron_calls[16] == {"hour": 19, "minute": 0}    # opt_daily (offset 120)
