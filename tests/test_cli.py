from datetime import date
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from zer0share.cli import cli


def test_sync_daily_kline_accepts_date_range():
    runner = CliRunner()
    pipeline = MagicMock()
    pipeline.__enter__.return_value = pipeline
    pipeline.__exit__.return_value = False

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(
            cli,
            [
                "sync",
                "--table",
                "daily_kline",
                "--start-date",
                "2016-01-01",
                "--end-date",
                "2016-01-31",
            ],
        )

    assert result.exit_code == 0
    pipeline.sync_daily_kline.assert_called_once_with(
        start_date=date(2016, 1, 1),
        end_date=date(2016, 1, 31),
    )


def test_sync_end_date_requires_start_date():
    runner = CliRunner()

    result = runner.invoke(
        cli,
        ["sync", "--table", "daily_kline", "--end-date", "2016-01-31"],
    )

    assert result.exit_code != 0
    assert "--end-date requires --start-date" in result.output


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
                "2024-01-01",
                "--end-date",
                "2024-01-31",
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
            "2024-01-31",
            "--start-date",
            "2024-01-01",
        ],
    )

    assert result.exit_code != 0
    assert "--date cannot be used with --start-date or --end-date" in result.output


def test_sync_industry_calls_pipeline():
    runner = CliRunner()
    pipeline = MagicMock()
    pipeline.__enter__.return_value = pipeline
    pipeline.__exit__.return_value = False

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(cli, ["sync", "--table", "industry"])

    assert result.exit_code == 0
    pipeline.sync_industry.assert_called_once()


def test_sync_ci_member_calls_pipeline():
    runner = CliRunner()
    pipeline = MagicMock()
    pipeline.__enter__.return_value = pipeline
    pipeline.__exit__.return_value = False

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(cli, ["sync", "--table", "ci_member"])

    assert result.exit_code == 0
    pipeline.sync_ci_member.assert_called_once()


def test_sync_all_includes_industry_and_ci_member():
    runner = CliRunner()
    pipeline = MagicMock()
    pipeline.__enter__.return_value = pipeline
    pipeline.__exit__.return_value = False

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(cli, ["sync", "--all"])

    assert result.exit_code == 0
    pipeline.sync_industry.assert_called_once()
    pipeline.sync_ci_member.assert_called_once()


def test_sync_industry_rejects_date_range():
    runner = CliRunner()
    pipeline = MagicMock()
    pipeline.__enter__.return_value = pipeline
    pipeline.__exit__.return_value = False

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(
            cli, ["sync", "--table", "industry", "--start-date", "2024-01-01"]
        )

    assert result.exit_code != 0
    assert "date range options" in result.output


def test_sync_index_daily_accepts_date_range():
    runner = CliRunner()
    pipeline = MagicMock()
    pipeline.__enter__.return_value = pipeline
    pipeline.__exit__.return_value = False

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(
            cli,
            [
                "sync",
                "--table",
                "index_daily",
                "--start-date",
                "2024-01-01",
                "--end-date",
                "2024-01-31",
            ],
        )

    assert result.exit_code == 0
    pipeline.sync_index_daily.assert_called_once_with(
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 31),
    )


def test_sync_all_includes_index_daily():
    runner = CliRunner()
    pipeline = MagicMock()
    pipeline.__enter__.return_value = pipeline
    pipeline.__exit__.return_value = False

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(cli, ["sync", "--all"])

    assert result.exit_code == 0
    pipeline.sync_index_daily.assert_called_once()


def test_sync_fut_basic_calls_pipeline():
    runner = CliRunner()
    pipeline = MagicMock()
    pipeline.__enter__.return_value = pipeline
    pipeline.__exit__.return_value = False

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(cli, ["sync", "--table", "fut_basic"])

    assert result.exit_code == 0
    pipeline.sync_fut_basic.assert_called_once()


def test_sync_fut_daily_accepts_date_range():
    runner = CliRunner()
    pipeline = MagicMock()
    pipeline.__enter__.return_value = pipeline
    pipeline.__exit__.return_value = False

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(
            cli,
            [
                "sync",
                "--table",
                "fut_daily",
                "--start-date",
                "2024-01-01",
                "--end-date",
                "2024-01-31",
            ],
        )

    assert result.exit_code == 0
    pipeline.sync_fut_daily.assert_called_once_with(
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 31),
    )


def test_sync_fut_basic_rejects_date_range():
    runner = CliRunner()
    pipeline = MagicMock()
    pipeline.__enter__.return_value = pipeline
    pipeline.__exit__.return_value = False

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(
            cli, ["sync", "--table", "fut_basic", "--start-date", "2024-01-01"]
        )

    assert result.exit_code != 0
    assert "date range options" in result.output


def test_sync_all_includes_futures_tables():
    runner = CliRunner()
    pipeline = MagicMock()
    pipeline.__enter__.return_value = pipeline
    pipeline.__exit__.return_value = False

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(cli, ["sync", "--all"])

    assert result.exit_code == 0
    pipeline.sync_fut_basic.assert_called_once()
    pipeline.sync_fut_daily.assert_called_once()
    pipeline.sync_fut_holding.assert_called_once()
    pipeline.sync_fut_wsr.assert_called_once()
    pipeline.sync_fut_settle.assert_called_once()
    pipeline.sync_fut_mapping.assert_called_once()


def test_sync_ft_limit_accepts_date_range():
    runner = CliRunner()
    pipeline = MagicMock()
    pipeline.__enter__.return_value = pipeline
    pipeline.__exit__.return_value = False

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(
            cli,
            [
                "sync",
                "--table",
                "ft_limit",
                "--start-date",
                "2024-01-01",
                "--end-date",
                "2024-01-31",
            ],
        )

    assert result.exit_code == 0
    pipeline.sync_ft_limit.assert_called_once_with(
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 31),
    )


def test_sync_fut_weekly_detail_accepts_date_range():
    runner = CliRunner()
    pipeline = MagicMock()
    pipeline.__enter__.return_value = pipeline
    pipeline.__exit__.return_value = False

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(
            cli,
            [
                "sync",
                "--table",
                "fut_weekly_detail",
                "--start-date",
                "2024-01-01",
                "--end-date",
                "2024-01-31",
            ],
        )

    assert result.exit_code == 0
    pipeline.sync_fut_weekly_detail.assert_called_once_with(
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 31),
    )


def test_sync_all_includes_futures_batch2_tables():
    runner = CliRunner()
    pipeline = MagicMock()
    pipeline.__enter__.return_value = pipeline
    pipeline.__exit__.return_value = False

    with patch("zer0share.cli._make_pipeline", return_value=pipeline):
        result = runner.invoke(cli, ["sync", "--all"])

    assert result.exit_code == 0
    pipeline.sync_ft_limit.assert_called_once()
    pipeline.sync_fut_weekly.assert_called_once()
    pipeline.sync_fut_monthly.assert_called_once()
    pipeline.sync_fut_index_daily.assert_called_once()
    pipeline.sync_fut_weekly_detail.assert_called_once()
