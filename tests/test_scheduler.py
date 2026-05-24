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

    assert set(registered_jobs) == {"daily_kline", "index_daily", "basic", "adj_factor"}
    assert len(registered_jobs) == 4


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
