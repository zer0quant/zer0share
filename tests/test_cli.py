from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from zer0share.cli import cli


def _make_mock_pipeline(supports_date_range_for=None):
    """Return a MagicMock pipeline (from upstream PR #1's tests).

    supports_date_range_for: set of table names that support date range.
    All others return a job with supports_date_range=False.
    If None, all tables return supports_date_range=True (default MagicMock truthy).
    """
    pipeline = MagicMock()
    pipeline.__enter__.return_value = pipeline
    pipeline.__exit__.return_value = False

    if supports_date_range_for is not None:
        def fake_registry_get(table):
            job = MagicMock()
            job.supports_date_range = table in supports_date_range_for
            return job
        pipeline.registry.get.side_effect = fake_registry_get
    else:
        # 我们的 run_sync 在 runner 层用 pipeline.registry 校验表是否注册;
        # 这里给一个"全部认可"的 registry, 让测试聚焦于 CLI 是否把表路由到
        # pipeline.run, 而不是被注册校验挡下 (真实 pipeline 自带完整 registry)。
        reg = MagicMock()
        reg.__contains__ = lambda self, k: True

        def _get(k):
            job = MagicMock()
            job.supports_date_range = True
            return job
        reg.get = _get
        pipeline.registry = reg

    return pipeline


class _Registry(dict):
    """支持 `in` / `[]` / `.get()` 的假 registry。

    MagicMock 的 `__contains__` 恒为 True, 会让 runner 把任何表名都当成已注册,
    掩盖"表名写错"这类问题。这里用真实的 dict 语义: 未注册的表直接 KeyError。
    """

    def __init__(self, tables, supports_date_range_for=None):
        super().__init__()
        for t in tables:
            job = MagicMock()
            job.supports_date_range = (
                True if supports_date_range_for is None
                else t in supports_date_range_for
            )
            self[t] = job


def _mock_pipeline_for(tables, supports_date_range_for=None):
    """构造一个 registry 真实、run 可断言的 mock pipeline。"""
    pipeline = MagicMock()
    pipeline.__enter__.return_value = pipeline
    pipeline.__exit__.return_value = False
    pipeline.registry = _Registry(tables, supports_date_range_for)
    return pipeline


def _job_tables():
    """逐日 / 快照型 job 表 (catalog 规范名, 不含别名)。"""
    from zer0share import sync_plan as SP

    return sorted(t for t, p in SP.PLANS.items() if p.is_job)


def _bulk_tables():
    """批量取数型表 (吸收原 bulk 引擎, 现为 BatchSyncJob)。"""
    from zer0share import sync_plan as SP

    return sorted(t for t, p in SP.PLANS.items() if p.is_batch)


def _sync_env(pipeline, tmp_path=None):
    """构造统一 sync 需要的 patch 环境。

    - `_make_pipeline` → mock pipeline (所有表都走它, 包括批量取数表)
    - `load_config` → mock cfg (避免依赖真实 settings.toml)
    """
    cfg = MagicMock()
    cfg.log_path = Path(tmp_path or "/tmp") / "zer0share-test.log"
    cfg.data_dir = str(tmp_path or "/tmp")

    patches = [
        patch("zer0share.cli._make_pipeline", return_value=pipeline),
        patch("zer0share.cli.load_config", return_value=cfg),
    ]
    return patches


def _invoke_sync(argv, pipeline, tmp_path=None):
    """跑一次 sync 命令, 返回 result。"""
    patches = _sync_env(pipeline, tmp_path)
    with patches[0], patches[1]:
        result = CliRunner().invoke(cli, ["sync", *argv])
    return result


# ------------------------------------------------------------------ 参数校验
def test_sync_end_date_requires_start_date():
    result = CliRunner().invoke(
        cli, ["sync", "--table", "daily_kline", "--end-date", "20160131"]
    )

    assert result.exit_code != 0
    assert "--end-date requires --start-date" in result.output


def test_sync_requires_a_target():
    result = CliRunner().invoke(cli, ["sync"])

    assert result.exit_code != 0
    assert "--table" in result.output


def test_sync_rejects_unknown_table():
    result = CliRunner().invoke(cli, ["sync", "--table", "no_such_table"])

    assert result.exit_code != 0
    assert "未知表" in result.output


def test_sync_rejects_unknown_topic():
    result = CliRunner().invoke(cli, ["sync", "--topic", "no_such_topic"])

    assert result.exit_code != 0
    assert "未知主题" in result.output


# ------------------------------------------------------------------ 统一引擎
def test_sync_batch_table_goes_through_pipeline():
    """daily_kline 现为 job 引擎的表 — 与其它表一样走 pipeline.run。"""
    pipeline = _mock_pipeline_for(_job_tables() + _bulk_tables())
    result = _invoke_sync(["--table", "daily_kline"], pipeline)

    assert result.exit_code == 0
    pipeline.run.assert_called_once_with(
        "daily_kline", start_date=None, end_date=None)


def test_sync_job_table_goes_through_pipeline():
    """industry 逐日型 — 通过 pipeline.run 执行。"""
    pipeline = _mock_pipeline_for(_job_tables())
    result = _invoke_sync(["--table", "industry"], pipeline)

    assert result.exit_code == 0
    pipeline.run.assert_called_once_with("industry", start_date=None, end_date=None)


def test_sync_mixed_tables_all_go_through_pipeline():
    """批量取数表 + 逐日表 → 都走同一个 pipeline, 不再分引擎。"""
    pipeline = _mock_pipeline_for(_job_tables() + _bulk_tables())
    result = _invoke_sync(["--tables", "daily_kline,industry"], pipeline)

    assert result.exit_code == 0
    called = [c.args[0] for c in pipeline.run.call_args_list]
    assert set(called) == {"daily_kline", "industry"}


def test_sync_all_covers_every_table():
    pipeline = _mock_pipeline_for(_job_tables() + _bulk_tables())
    result = _invoke_sync(["--all"], pipeline)

    assert result.exit_code == 0
    called = {c.args[0] for c in pipeline.run.call_args_list}
    assert "daily_kline" in called
    assert "industry" in called


# ------------------------------------------------------------------ 别名
def test_sync_accepts_legacy_table_alias():
    """旧 bulk 表名 `daily` 归一到 catalog 名 `daily_kline`。"""
    pipeline = _mock_pipeline_for(_job_tables() + _bulk_tables())
    result = _invoke_sync(["--table", "daily"], pipeline)

    assert result.exit_code == 0
    pipeline.run.assert_called_once_with(
        "daily_kline", start_date=None, end_date=None)


def test_sync_accepts_legacy_stock_basic_alias():
    pipeline = _mock_pipeline_for(_job_tables() + _bulk_tables())
    result = _invoke_sync(["--table", "stock_basic"], pipeline)

    assert result.exit_code == 0
    # basic 是快照表
    pipeline.run.assert_called_once_with("basic", start_date=None, end_date=None)


# ------------------------------------------------------------------ 主题 / 旧开关
def test_sync_stock_flag_includes_idx_anns():
    """旧 `--stock` 开关保持原语义 (含 idx_anns)。"""
    pipeline = _mock_pipeline_for(_job_tables())
    result = _invoke_sync(["--stock"], pipeline)

    assert result.exit_code == 0
    called = [c.args[0] for c in pipeline.run.call_args_list]
    assert "idx_anns" in called


def test_sync_etf_flag_calls_etf_tables():
    """旧 `--etf` 开关保持原表清单与顺序。"""
    pipeline = _mock_pipeline_for(_job_tables())
    result = _invoke_sync(["--etf"], pipeline)

    assert result.exit_code == 0
    assert [c.args[0] for c in pipeline.run.call_args_list] == [
        "fund_daily",
        "fund_adj",
        "etf_share_size",
        "etf_sh_cons",
        "etf_basic",
        "etf_index",
    ]


def test_sync_topic_and_table_are_unioned():
    pipeline = _mock_pipeline_for(_job_tables() + _bulk_tables())
    result = _invoke_sync(
        ["--topic", "finance", "--table", "industry"], pipeline
    )

    assert result.exit_code == 0
    called = {c.args[0] for c in pipeline.run.call_args_list}
    assert "income" in called
    assert "industry" in called


def test_sync_topic_all_expands_all_topics():
    pipeline = _mock_pipeline_for(_job_tables() + _bulk_tables())
    result = _invoke_sync(["--topic", "all"], pipeline)

    assert result.exit_code == 0
    called = {c.args[0] for c in pipeline.run.call_args_list}
    assert set(_bulk_tables()) <= called


# ------------------------------------------------------------------ 日期区间
def test_sync_daily_kline_accepts_date_range():
    pipeline = _mock_pipeline_for(_job_tables() + _bulk_tables())
    result = _invoke_sync(
        ["--table", "daily_kline", "--start-date", "20160101", "--end-date", "20160131"],
        pipeline,
    )

    assert result.exit_code == 0
    pipeline.run.assert_called_once_with(
        "daily_kline", start_date="20160101", end_date="20160131"
    )


def test_sync_industry_rejects_date_range():
    pipeline = _mock_pipeline_for(_job_tables(), supports_date_range_for=set())
    result = _invoke_sync(
        ["--table", "industry", "--start-date", "20240101"], pipeline
    )

    assert result.exit_code != 0
    assert "date range options" in result.output


def test_sync_etf_basic_rejects_date_range():
    pipeline = _mock_pipeline_for(_job_tables(), supports_date_range_for=set())
    result = _invoke_sync(
        ["--table", "etf_basic", "--start-date", "20240101"], pipeline
    )

    assert result.exit_code != 0
    assert "date range options" in result.output


def test_sync_etf_index_rejects_date_range():
    pipeline = _mock_pipeline_for(_job_tables(), supports_date_range_for=set())
    result = _invoke_sync(
        ["--table", "etf_index", "--start-date", "20240101"], pipeline
    )

    assert result.exit_code != 0
    assert "date range options" in result.output


def test_sync_fut_basic_rejects_date_range():
    pipeline = _mock_pipeline_for(_job_tables(), supports_date_range_for=set())
    result = _invoke_sync(
        ["--table", "fut_basic", "--start-date", "20240101"], pipeline
    )

    assert result.exit_code != 0
    assert "date range options" in result.output


def test_sync_opt_basic_rejects_date_range():
    pipeline = _mock_pipeline_for(_job_tables(), supports_date_range_for=set())
    result = _invoke_sync(
        ["--table", "opt_basic", "--start-date", "20240101"], pipeline
    )

    assert result.exit_code != 0
    assert "date range options" in result.output


def test_sync_index_daily_accepts_date_range():
    pipeline = _mock_pipeline_for(_job_tables())
    result = _invoke_sync(
        ["--table", "index_daily", "--start-date", "20240101", "--end-date", "20240131"],
        pipeline,
    )

    assert result.exit_code == 0
    pipeline.run.assert_called_once_with(
        "index_daily", start_date="20240101", end_date="20240131"
    )


def test_sync_idx_anns_accepts_date_range():
    pipeline = _mock_pipeline_for(_job_tables())
    result = _invoke_sync(
        ["--table", "idx_anns", "--start-date", "20260401", "--end-date", "20260430"],
        pipeline,
    )

    assert result.exit_code == 0
    pipeline.run.assert_called_once_with(
        "idx_anns", start_date="20260401", end_date="20260430"
    )


def test_sync_fut_daily_accepts_date_range():
    pipeline = _mock_pipeline_for(_job_tables())
    result = _invoke_sync(
        ["--table", "fut_daily", "--start-date", "20240101", "--end-date", "20240131"],
        pipeline,
    )

    assert result.exit_code == 0
    pipeline.run.assert_called_once_with(
        "fut_daily", start_date="20240101", end_date="20240131"
    )


def test_sync_ft_limit_accepts_date_range():
    pipeline = _mock_pipeline_for(_job_tables())
    result = _invoke_sync(
        ["--table", "ft_limit", "--start-date", "20240101", "--end-date", "20240131"],
        pipeline,
    )

    assert result.exit_code == 0
    pipeline.run.assert_called_once_with(
        "ft_limit", start_date="20240101", end_date="20240131"
    )


def test_sync_fut_weekly_accepts_date_range():
    pipeline = _mock_pipeline_for(_job_tables())
    result = _invoke_sync(
        ["--table", "fut_weekly", "--start-date", "20240101", "--end-date", "20240131"],
        pipeline,
    )

    assert result.exit_code == 0
    pipeline.run.assert_called_once_with(
        "fut_weekly", start_date="20240101", end_date="20240131"
    )


def test_sync_fut_weekly_detail_accepts_date_range():
    pipeline = _mock_pipeline_for(_job_tables())
    result = _invoke_sync(
        ["--table", "fut_weekly_detail", "--start-date", "20240101", "--end-date", "20240131"],
        pipeline,
    )

    assert result.exit_code == 0
    pipeline.run.assert_called_once_with(
        "fut_weekly_detail", start_date="20240101", end_date="20240131"
    )


def test_sync_opt_daily_accepts_date_range():
    pipeline = _mock_pipeline_for(_job_tables())
    result = _invoke_sync(
        ["--table", "opt_daily", "--start-date", "20240101", "--end-date", "20240131"],
        pipeline,
    )

    assert result.exit_code == 0
    pipeline.run.assert_called_once_with(
        "opt_daily", start_date="20240101", end_date="20240131"
    )


def test_sync_fund_daily_accepts_date_range():
    pipeline = _mock_pipeline_for(_job_tables())
    result = _invoke_sync(
        ["--table", "fund_daily", "--start-date", "20240101", "--end-date", "20240131"],
        pipeline,
    )

    assert result.exit_code == 0
    pipeline.run.assert_called_once_with(
        "fund_daily", start_date="20240101", end_date="20240131"
    )


def test_sync_fund_adj_accepts_date_range():
    pipeline = _mock_pipeline_for(_job_tables())
    result = _invoke_sync(
        ["--table", "fund_adj", "--start-date", "20240101", "--end-date", "20240131"],
        pipeline,
    )

    assert result.exit_code == 0
    pipeline.run.assert_called_once_with(
        "fund_adj", start_date="20240101", end_date="20240131"
    )


def test_sync_etf_share_size_accepts_date_range():
    pipeline = _mock_pipeline_for(_job_tables())
    result = _invoke_sync(
        ["--table", "etf_share_size", "--start-date", "20240101", "--end-date", "20240131"],
        pipeline,
    )

    assert result.exit_code == 0
    pipeline.run.assert_called_once_with(
        "etf_share_size", start_date="20240101", end_date="20240131"
    )


def test_sync_etf_sh_cons_accepts_date_range():
    pipeline = _mock_pipeline_for(_job_tables())
    result = _invoke_sync(
        ["--table", "etf_sh_cons", "--start-date", "20240101", "--end-date", "20240131"],
        pipeline,
    )

    assert result.exit_code == 0
    pipeline.run.assert_called_once_with(
        "etf_sh_cons", start_date="20240101", end_date="20240131"
    )


# ------------------------------------------------------------------ 其它命令
def test_build_universe_accepts_date_range(tmp_path):
    runner = CliRunner()
    cfg = MagicMock()
    cfg.data_dir = "data"
    cfg.log_path = tmp_path / "pipeline.log"

    with (
        patch("zer0share.cli.load_config", return_value=cfg),
        patch("zer0share.cli.build_universes_range") as mock_build_range,
    ):
        mock_build_range.return_value = {
            "start_date": date(2024, 1, 1),
            "end_date": date(2024, 1, 31),
            "trading_days": 22,
            "built_days": 20,
            "skipped_days": 2,
            "counts": {"univ_trade_base": 100},
        }
        result = runner.invoke(
            cli,
            [
                "build-universe",
                "--start-date",
                "20240101",
                "--end-date",
                "20240131",
            ],
        )

    assert result.exit_code == 0
    mock_build_range.assert_called_once_with(
        "data",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 31),
    )
    assert "built: 20, skipped: 2" in result.output


def test_build_universe_rejects_date_with_range():
    runner = CliRunner()

    result = runner.invoke(
        cli,
        [
            "build-universe",
            "--date",
            "20240131",
            "--start-date",
            "20240101",
        ],
    )

    assert result.exit_code != 0
    assert "--date cannot be used with --start-date or --end-date" in result.output


def test_sync_rejects_table_missing_from_registry():
    """显式 --table 指定了 registry 里没有的表时必须报错。

    回归用例: [ricequant] enabled=false 时 build_jobs 返回空, 旧实现只打印
    "无注册项, 跳过" 并返回 exit 0, 让调用方以为同步成功了。
    """
    registry = [t for t in _job_tables() if not t.startswith("ricequant")]
    pipeline = _mock_pipeline_for(registry)

    result = _invoke_sync(
        ["--table", "ricequant_basic", "--dry-run"], pipeline
    )

    assert result.exit_code != 0
    assert "ricequant_basic" in result.output
    assert "enabled" in result.output


def test_sync_rejects_missing_registry_table_without_dry_run():
    """真实执行路径同样要拦: 静默跳过会掩盖"一张表都没写"。"""
    registry = [t for t in _job_tables() if not t.startswith("ricequant")]
    pipeline = _mock_pipeline_for(registry)

    result = _invoke_sync(["--table", "ricequant_basic"], pipeline)

    assert result.exit_code != 0
    assert "ricequant_basic" in result.output
    pipeline.run.assert_not_called()


def test_job_tables_skip_unregistered_when_not_strict(tmp_path):
    """非 strict: registry 里没有的表被剔除, 返回空表清单而不报错。"""
    from zer0share.config import load_config
    from zer0share import sync_runner as SR

    # 用真实 config: [ricequant] enabled=false, 轻量 registry 因此不含
    # ricequant_* 表 —— 这正是要覆盖的场景。
    cfg = load_config(Path("config/settings.toml"))
    reg = SR._lightweight_registry(cfg)
    assert "ricequant_basic" not in reg

    present = sorted(reg)[0]
    done, failed = SR.run_job_tables(
        cfg, [present, "ricequant_basic"], dry_run=True, strict=False
    )

    assert done == [present]
    assert failed == []


def test_job_tables_raise_on_unregistered_when_strict(tmp_path):
    """strict: registry 里没有的表必须抛 UnknownTable。"""
    import pytest
    from zer0share.config import load_config
    from zer0share import sync_runner as SR

    cfg = load_config(Path("config/settings.toml"))

    with pytest.raises(SR.UnknownTable) as exc:
        SR.run_job_tables(cfg, ["ricequant_basic"], dry_run=True, strict=True)

    assert "enabled" in str(exc.value)


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
            [
                "quality",
                "check",
                "--table",
                "daily_kline",
                "--mode",
                "daily",
                "--date",
                "20240102",
            ],
        )

    assert result.exit_code == 0
    assert "quality summary" in result.output
    runner_cls.return_value.run.assert_called_once()


def test_sync_industry_calls_pipeline():
    runner = CliRunner()
    pipeline = _make_mock_pipeline()

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(cli, ["sync", "--table", "industry"])

    assert result.exit_code == 0
    pipeline.run.assert_called_once_with("industry", start_date=None, end_date=None)


def test_sync_ci_member_calls_pipeline():
    runner = CliRunner()
    pipeline = _make_mock_pipeline()

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(cli, ["sync", "--table", "ci_member"])

    assert result.exit_code == 0
    pipeline.run.assert_called_once_with("ci_member", start_date=None, end_date=None)


def test_sync_all_includes_industry_and_ci_member():
    runner = CliRunner()
    pipeline = _make_mock_pipeline()

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        with patch("zer0share.cli.run_sync") as run_sync:
            run_sync.return_value = MagicMock(failed=[], summary=lambda: "")
            result = runner.invoke(cli, ["sync", "--all"])

    assert result.exit_code == 0
    # 重构后统一走 run_sync(整批表名), 不再走 pipeline.run_all()
    run_sync.assert_called_once()
    called_tables = run_sync.call_args.args[1]
    assert "industry" in called_tables
    assert "ci_member" in called_tables


def test_sync_industry_rejects_date_range():
    runner = CliRunner()
    # industry does not support date range
    pipeline = _make_mock_pipeline(supports_date_range_for=set())

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(
            cli, ["sync", "--table", "industry", "--start-date", "20240101"]
        )

    assert result.exit_code != 0
    assert "date range options" in result.output


def test_sync_index_daily_accepts_date_range():
    runner = CliRunner()
    pipeline = _make_mock_pipeline()

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(
            cli,
            [
                "sync",
                "--table",
                "index_daily",
                "--start-date",
                "20240101",
                "--end-date",
                "20240131",
            ],
        )

    assert result.exit_code == 0
    pipeline.run.assert_called_once_with(
        "index_daily",
        start_date="20240101",
        end_date="20240131",
    )


def test_sync_all_includes_index_daily():
    runner = CliRunner()
    pipeline = _make_mock_pipeline()

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        with patch("zer0share.cli.run_sync") as run_sync:
            run_sync.return_value = MagicMock(failed=[], summary=lambda: "")
            result = runner.invoke(cli, ["sync", "--all"])

    assert result.exit_code == 0
    run_sync.assert_called_once()
    assert "index_daily" in run_sync.call_args.args[1]


def test_sync_idx_anns_accepts_date_range():
    runner = CliRunner()
    pipeline = _make_mock_pipeline()

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(
            cli,
            [
                "sync",
                "--table",
                "idx_anns",
                "--start-date",
                "20260401",
                "--end-date",
                "20260430",
            ],
        )

    assert result.exit_code == 0
    pipeline.run.assert_called_once_with(
        "idx_anns",
        start_date="20260401",
        end_date="20260430",
    )


def test_sync_fut_basic_calls_pipeline():
    runner = CliRunner()
    pipeline = _make_mock_pipeline()

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(cli, ["sync", "--table", "fut_basic"])

    assert result.exit_code == 0
    pipeline.run.assert_called_once_with("fut_basic", start_date=None, end_date=None)


def test_sync_fut_daily_accepts_date_range():
    runner = CliRunner()
    pipeline = _make_mock_pipeline()

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(
            cli,
            [
                "sync",
                "--table",
                "fut_daily",
                "--start-date",
                "20240101",
                "--end-date",
                "20240131",
            ],
        )

    assert result.exit_code == 0
    pipeline.run.assert_called_once_with(
        "fut_daily",
        start_date="20240101",
        end_date="20240131",
    )


def test_sync_fut_basic_rejects_date_range():
    runner = CliRunner()
    # fut_basic does not support date range
    pipeline = _make_mock_pipeline(supports_date_range_for=set())

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(
            cli, ["sync", "--table", "fut_basic", "--start-date", "20240101"]
        )

    assert result.exit_code != 0
    assert "date range options" in result.output


def test_sync_all_includes_futures_tables():
    runner = CliRunner()
    pipeline = _make_mock_pipeline()

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        with patch("zer0share.cli.run_sync") as run_sync:
            run_sync.return_value = MagicMock(failed=[], summary=lambda: "")
            result = runner.invoke(cli, ["sync", "--all"])

    assert result.exit_code == 0
    run_sync.assert_called_once()
    tbls = run_sync.call_args.args[1]
    for t in ["fut_basic", "fut_daily", "fut_holding", "fut_wsr", "fut_settle",
              "fut_mapping", "ft_limit", "fut_weekly", "fut_monthly",
              "fut_index_daily", "fut_weekly_detail"]:
        assert t in tbls


def test_sync_stock_includes_idx_anns():
    runner = CliRunner()
    pipeline = _make_mock_pipeline()

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        with patch("zer0share.cli.run_sync") as run_sync:
            run_sync.return_value = MagicMock(failed=[], summary=lambda: "")
            result = runner.invoke(cli, ["sync", "--stock"])

    assert result.exit_code == 0
    run_sync.assert_called_once()
    assert "idx_anns" in run_sync.call_args.args[1]


def test_sync_ft_limit_accepts_date_range():
    runner = CliRunner()
    pipeline = _make_mock_pipeline()

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(
            cli,
            [
                "sync",
                "--table",
                "ft_limit",
                "--start-date",
                "20240101",
                "--end-date",
                "20240131",
            ],
        )

    assert result.exit_code == 0
    pipeline.run.assert_called_once_with(
        "ft_limit",
        start_date="20240101",
        end_date="20240131",
    )


def test_sync_fut_weekly_accepts_date_range():
    runner = CliRunner()
    pipeline = _make_mock_pipeline()

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(
            cli,
            [
                "sync",
                "--table",
                "fut_weekly",
                "--start-date",
                "20240101",
                "--end-date",
                "20240131",
            ],
        )

    assert result.exit_code == 0
    pipeline.run.assert_called_once_with(
        "fut_weekly",
        start_date="20240101",
        end_date="20240131",
    )


def test_sync_fut_weekly_detail_accepts_date_range():
    runner = CliRunner()
    pipeline = _make_mock_pipeline()

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(
            cli,
            [
                "sync",
                "--table",
                "fut_weekly_detail",
                "--start-date",
                "20240101",
                "--end-date",
                "20240131",
            ],
        )

    assert result.exit_code == 0
    pipeline.run.assert_called_once_with(
        "fut_weekly_detail",
        start_date="20240101",
        end_date="20240131",
    )


def test_sync_all_includes_futures_batch2_tables():
    runner = CliRunner()
    pipeline = _make_mock_pipeline()

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        with patch("zer0share.cli.run_sync") as run_sync:
            run_sync.return_value = MagicMock(failed=[], summary=lambda: "")
            result = runner.invoke(cli, ["sync", "--all"])

    assert result.exit_code == 0
    run_sync.assert_called_once()
    tbls = run_sync.call_args.args[1]
    for t in ["fut_basic", "fut_daily", "fut_holding", "fut_wsr", "fut_settle",
              "fut_mapping", "ft_limit", "fut_weekly", "fut_monthly",
              "fut_index_daily", "fut_weekly_detail"]:
        assert t in tbls


def test_sync_opt_basic_calls_pipeline():
    runner = CliRunner()
    pipeline = _make_mock_pipeline()

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(cli, ["sync", "--table", "opt_basic"])

    assert result.exit_code == 0
    pipeline.run.assert_called_once_with("opt_basic", start_date=None, end_date=None)


def test_sync_opt_daily_accepts_date_range():
    runner = CliRunner()
    pipeline = _make_mock_pipeline()

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(
            cli,
            ["sync", "--table", "opt_daily", "--start-date", "20240101", "--end-date", "20240131"],
        )

    assert result.exit_code == 0
    pipeline.run.assert_called_once_with(
        "opt_daily",
        start_date="20240101",
        end_date="20240131",
    )


def test_sync_opt_basic_rejects_date_range():
    runner = CliRunner()
    # opt_basic does not support date range
    pipeline = _make_mock_pipeline(supports_date_range_for=set())

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(
            cli, ["sync", "--table", "opt_basic", "--start-date", "20240101"]
        )

    assert result.exit_code != 0
    assert "date range options" in result.output


def test_sync_all_includes_options_tables():
    runner = CliRunner()
    pipeline = _make_mock_pipeline()

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        with patch("zer0share.cli.run_sync") as run_sync:
            run_sync.return_value = MagicMock(failed=[], summary=lambda: "")
            result = runner.invoke(cli, ["sync", "--all"])

    assert result.exit_code == 0
    run_sync.assert_called_once()
    tbls = run_sync.call_args.args[1]
    assert "opt_basic" in tbls
    assert "opt_daily" in tbls


def test_sync_etf_basic_calls_pipeline():
    runner = CliRunner()
    pipeline = _make_mock_pipeline()

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(cli, ["sync", "--table", "etf_basic"])

    assert result.exit_code == 0
    pipeline.run.assert_called_once_with("etf_basic", start_date=None, end_date=None)


def test_sync_fund_daily_calls_pipeline():
    runner = CliRunner()
    pipeline = _make_mock_pipeline()

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(cli, ["sync", "--table", "fund_daily"])

    assert result.exit_code == 0
    pipeline.run.assert_called_once_with("fund_daily", start_date=None, end_date=None)


def test_sync_fund_daily_accepts_date_range():
    runner = CliRunner()
    pipeline = _make_mock_pipeline()

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(
            cli,
            [
                "sync",
                "--table",
                "fund_daily",
                "--start-date",
                "20240101",
                "--end-date",
                "20240131",
            ],
        )

    assert result.exit_code == 0
    pipeline.run.assert_called_once_with(
        "fund_daily",
        start_date="20240101",
        end_date="20240131",
    )


def test_sync_fund_adj_calls_pipeline():
    runner = CliRunner()
    pipeline = _make_mock_pipeline()

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(cli, ["sync", "--table", "fund_adj"])

    assert result.exit_code == 0
    pipeline.run.assert_called_once_with("fund_adj", start_date=None, end_date=None)


def test_sync_fund_adj_accepts_date_range():
    runner = CliRunner()
    pipeline = _make_mock_pipeline()

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(
            cli,
            [
                "sync",
                "--table",
                "fund_adj",
                "--start-date",
                "20240101",
                "--end-date",
                "20240131",
            ],
        )

    assert result.exit_code == 0
    pipeline.run.assert_called_once_with(
        "fund_adj",
        start_date="20240101",
        end_date="20240131",
    )


def test_sync_etf_share_size_calls_pipeline():
    runner = CliRunner()
    pipeline = _make_mock_pipeline()

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(cli, ["sync", "--table", "etf_share_size"])

    assert result.exit_code == 0
    pipeline.run.assert_called_once_with("etf_share_size", start_date=None, end_date=None)


def test_sync_etf_share_size_accepts_date_range():
    runner = CliRunner()
    pipeline = _make_mock_pipeline()

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(
            cli,
            [
                "sync",
                "--table",
                "etf_share_size",
                "--start-date",
                "20240101",
                "--end-date",
                "20240131",
            ],
        )

    assert result.exit_code == 0
    pipeline.run.assert_called_once_with(
        "etf_share_size",
        start_date="20240101",
        end_date="20240131",
    )


def test_sync_etf_sh_cons_calls_pipeline():
    runner = CliRunner()
    pipeline = _make_mock_pipeline()

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(cli, ["sync", "--table", "etf_sh_cons"])

    assert result.exit_code == 0
    pipeline.run.assert_called_once_with("etf_sh_cons", start_date=None, end_date=None)


def test_sync_etf_sh_cons_accepts_date_range():
    runner = CliRunner()
    pipeline = _make_mock_pipeline()

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(
            cli,
            [
                "sync",
                "--table",
                "etf_sh_cons",
                "--start-date",
                "20240101",
                "--end-date",
                "20240131",
            ],
        )

    assert result.exit_code == 0
    pipeline.run.assert_called_once_with(
        "etf_sh_cons",
        start_date="20240101",
        end_date="20240131",
    )


def test_sync_etf_calls_etf_tables():
    runner = CliRunner()
    pipeline = _make_mock_pipeline()

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(cli, ["sync", "--etf"])

    assert result.exit_code == 0
    assert [call.args[0] for call in pipeline.run.call_args_list] == [
        "fund_daily",
        "fund_adj",
        "etf_share_size",
        "etf_sh_cons",
        "etf_basic",
        "etf_index",
    ]


def test_sync_etf_index_calls_pipeline():
    runner = CliRunner()
    pipeline = _make_mock_pipeline()

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(cli, ["sync", "--table", "etf_index"])

    assert result.exit_code == 0
    pipeline.run.assert_called_once_with("etf_index", start_date=None, end_date=None)


def test_sync_etf_basic_rejects_date_range():
    runner = CliRunner()
    pipeline = _make_mock_pipeline(supports_date_range_for=set())

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(
            cli, ["sync", "--table", "etf_basic", "--start-date", "20240101"]
        )

    assert result.exit_code != 0
    assert "date range options" in result.output


def test_sync_etf_index_rejects_date_range():
    runner = CliRunner()
    pipeline = _make_mock_pipeline(supports_date_range_for=set())

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(
            cli, ["sync", "--table", "etf_index", "--start-date", "20240101"]
        )

    assert result.exit_code != 0
    assert "date range options" in result.output


def test_sync_stock_includes_stk_holdertrade():
    runner = CliRunner()
    pipeline = _make_mock_pipeline()

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        with patch("zer0share.cli.run_sync") as run_sync:
            run_sync.return_value = MagicMock(failed=[], summary=lambda: "")
            result = runner.invoke(cli, ["sync", "--stock"])

    assert result.exit_code == 0
    run_sync.assert_called_once()
    assert "stk_holdertrade" in run_sync.call_args.args[1]
