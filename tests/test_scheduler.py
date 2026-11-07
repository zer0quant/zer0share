from unittest.mock import MagicMock, patch


VALID_CONFIG = """
[tushare]
token = "test"

[paths]
data_dir = "data"
db_path = "db/meta.duckdb"
log_path = "logs/pipeline.log"

[scheduler]
trade_cal   = "09:00"
basic       = "09:10"
daily_kline = "16:30"
adj_factor  = "16:35"

[notifier]
wecom_webhook_url = "https://example.com"
enabled = false
"""


def test_start_scheduler_registers_all_configured_jobs(tmp_path):
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

    assert set(registered_jobs) == {"trade_cal", "basic", "daily_kline", "adj_factor"}
    assert len(registered_jobs) == 4


def test_start_scheduler_sends_startup_notification(tmp_path):
    cfg_file = tmp_path / "settings.toml"
    cfg_file.write_text(VALID_CONFIG, encoding="utf-8")
    notifier = MagicMock()

    with (
        patch("tushare.pro_api"),
        patch("zer0share.scheduler.build_notifier", return_value=notifier),
        patch("apscheduler.schedulers.blocking.BlockingScheduler.start"),
        patch("apscheduler.schedulers.blocking.BlockingScheduler.add_job"),
        patch("zer0share.scheduler.Pipeline"),
    ):
        from zer0share.scheduler import start_scheduler

        start_scheduler(str(cfg_file))

    notifier.send.assert_called_once()
    message = notifier.send.call_args[0][0]
    assert "调度器已启动" in message
    assert "已调度：4 个表" in message
    assert str(cfg_file) in message


def test_start_scheduler_opens_pipeline_only_when_job_runs(tmp_path):
    cfg_file = tmp_path / "settings.toml"
    cfg_file.write_text(VALID_CONFIG, encoding="utf-8")

    registered_jobs = {}

    def fake_add_job(func, trigger, id=None, **kwargs):
        registered_jobs[id] = func

    with (
        patch("tushare.pro_api"),
        patch("apscheduler.schedulers.blocking.BlockingScheduler.start"),
        patch(
            "apscheduler.schedulers.blocking.BlockingScheduler.add_job",
            side_effect=fake_add_job,
        ),
        patch("zer0share.scheduler.Pipeline") as mock_pipeline_cls,
    ):
        mock_pipeline = mock_pipeline_cls.return_value
        mock_pipeline.__enter__.return_value = mock_pipeline
        mock_pipeline.__exit__.return_value = False

        from zer0share.scheduler import start_scheduler

        start_scheduler(str(cfg_file))

        mock_pipeline_cls.assert_not_called()

        registered_jobs["basic"]()

    mock_pipeline_cls.assert_called_once()
    mock_pipeline.run.assert_called_once_with("basic")
    mock_pipeline.__exit__.assert_called_once()


def test_start_scheduler_uses_correct_cron_times(tmp_path):
    cfg_file = tmp_path / "settings.toml"
    cfg_file.write_text(VALID_CONFIG, encoding="utf-8")

    job_cron_map = {}  # id → {"hour": ..., "minute": ...}
    last_cron_kwargs = {}

    def fake_cron_trigger(**kwargs):
        last_cron_kwargs.update(kwargs)
        return MagicMock()

    def fake_add_job(func, trigger, id=None, **kwargs):
        job_cron_map[id] = dict(last_cron_kwargs)
        last_cron_kwargs.clear()

    with (
        patch("tushare.pro_api"),
        patch("zer0share.scheduler.CronTrigger", side_effect=fake_cron_trigger),
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

    assert job_cron_map["trade_cal"] == {"hour": 9, "minute": 0}
    assert job_cron_map["basic"] == {"hour": 9, "minute": 10}
    assert job_cron_map["daily_kline"] == {"hour": 16, "minute": 30}
    assert job_cron_map["adj_factor"] == {"hour": 16, "minute": 35}


ENABLED_CONFIG = VALID_CONFIG.replace(
    '[notifier]\nwecom_webhook_url = "https://example.com"\nenabled = false',
    '[notifier]\nenabled = true\ndigest_time = "18:10"\n\n[notifier.wecom]\nenabled = true\nwebhook_url = "https://example.com"',
)


def _start_with_fake_jobs(cfg_file, notifier):
    registered_jobs = {}
    cron_kwargs = {}

    def fake_cron_trigger(**kwargs):
        return dict(kwargs)

    def fake_add_job(func, trigger, id=None, **kwargs):
        registered_jobs[id] = func
        cron_kwargs[id] = trigger

    patches = (
        patch("tushare.pro_api"),
        patch("zer0share.scheduler.build_notifier", return_value=notifier),
        patch("zer0share.scheduler.CronTrigger", side_effect=fake_cron_trigger),
        patch("apscheduler.schedulers.blocking.BlockingScheduler.start"),
        patch("apscheduler.schedulers.blocking.BlockingScheduler.add_job", side_effect=fake_add_job),
    )
    return registered_jobs, cron_kwargs, patches


def test_start_scheduler_registers_daily_digest_and_buffers_success(tmp_path):
    from contextlib import ExitStack

    cfg_file = tmp_path / "settings.toml"
    cfg_file.write_text(ENABLED_CONFIG.replace('db_path = "db/meta.duckdb"', f'db_path = "{tmp_path}/db/meta.duckdb"'), encoding="utf-8")
    notifier = MagicMock()
    jobs, crons, patches = _start_with_fake_jobs(cfg_file, notifier)

    with ExitStack() as stack:
        for p in patches:
            stack.enter_context(p)
        mock_pipeline_cls = stack.enter_context(patch("zer0share.scheduler.Pipeline"))
        mock_pipeline = mock_pipeline_cls.return_value
        mock_pipeline.__enter__.return_value = mock_pipeline
        mock_pipeline.__exit__.return_value = False

        def fake_run(table_name):
            job_notifier = mock_pipeline_cls.call_args[0][2]
            job_notifier.send_success(f"{table_name} 同步完成\n写入 1 天")

        mock_pipeline.run.side_effect = fake_run

        from zer0share.scheduler import start_scheduler
        start_scheduler(str(cfg_file))

        assert crons["__daily_digest__"] == {"hour": 18, "minute": 10}
        notifier.reset_mock()

        jobs["basic"]()
        notifier.send.assert_not_called()
        notifier.send_success.assert_not_called()

        jobs["__daily_digest__"]()

    notifier.send.assert_called_once()
    msg = notifier.send.call_args[0][0]
    assert "每日同步汇总" in msg
    assert "basic: 写入 1 天" in msg
    assert (tmp_path / "db" / "digest_state.json").exists()


def test_start_scheduler_without_digest_time_sends_success_immediately(tmp_path):
    from contextlib import ExitStack

    cfg_file = tmp_path / "settings.toml"
    cfg_file.write_text(
        ENABLED_CONFIG.replace('digest_time = "18:10"', 'digest_time = ""')
        .replace('db_path = "db/meta.duckdb"', f'db_path = "{tmp_path}/db/meta.duckdb"'),
        encoding="utf-8",
    )
    notifier = MagicMock()
    jobs, _, patches = _start_with_fake_jobs(cfg_file, notifier)

    with ExitStack() as stack:
        for p in patches:
            stack.enter_context(p)
        mock_pipeline_cls = stack.enter_context(patch("zer0share.scheduler.Pipeline"))
        from zer0share.scheduler import start_scheduler
        start_scheduler(str(cfg_file))

        assert "__daily_digest__" not in jobs
        jobs["basic"]()
        assert mock_pipeline_cls.call_args[0][2] is notifier
