from pathlib import Path

import pytest

from zer0share.config import load_config


VALID_TOML = """
[tushare]
token = "test_token"

[paths]
data_dir = "data"
db_path = "db/meta.duckdb"
log_path = "logs/pipeline.log"

[scheduler]
trade_cal   = "09:00"
basic       = "09:10"
daily_kline = "16:30"

[notifier]
wecom_webhook_url = "https://example.com/webhook"
enabled = false
"""


def test_load_config_returns_schedule_dict(tmp_path):
    cfg_file = tmp_path / "settings.toml"
    cfg_file.write_text(VALID_TOML, encoding="utf-8")

    cfg = load_config(cfg_file)

    assert cfg.tushare_token == "test_token"
    assert cfg.data_dir == Path("data")
    assert cfg.schedule == {
        "trade_cal": "09:00",
        "basic": "09:10",
        "daily_kline": "16:30",
    }
    assert cfg.wecom_webhook_url == "https://example.com/webhook"
    assert cfg.notifier_enabled is False
    assert cfg.notifier.enabled is False
    assert cfg.notifier.wecom.webhook_url == "https://example.com/webhook"
    assert cfg.notifier.wecom.enabled is False
    assert cfg.notifier.feishu.webhook_url == ""
    assert cfg.notifier.feishu.enabled is False


def test_load_config_defaults_tushare_http_url_empty(tmp_path):
    cfg_file = tmp_path / "settings.toml"
    cfg_file.write_text(VALID_TOML, encoding="utf-8")

    cfg = load_config(cfg_file)

    assert cfg.tushare_http_url == ""


def test_load_config_parses_tushare_http_url(tmp_path):
    cfg_file = tmp_path / "settings.toml"
    cfg_file.write_text(
        VALID_TOML.replace(
            'token = "test_token"',
            'token = "test_token"\nhttp_url = "https://ts.gyzcloud.top/api"',
        ),
        encoding="utf-8",
    )

    cfg = load_config(cfg_file)

    assert cfg.tushare_http_url == "https://ts.gyzcloud.top/api"


def test_load_config_notifier_enabled_true(tmp_path):
    cfg_file = tmp_path / "settings.toml"
    cfg_file.write_text(
        VALID_TOML.replace("enabled = false", "enabled = true"),
        encoding="utf-8",
    )
    cfg = load_config(cfg_file)
    assert cfg.notifier_enabled is True


def test_load_config_accepts_nested_wecom_notifier(tmp_path):
    cfg_file = tmp_path / "settings.toml"
    cfg_file.write_text(
        """
[tushare]
token = "test_token"

[paths]
data_dir = "data"
db_path = "db/meta.duckdb"
log_path = "logs/pipeline.log"

[scheduler]
daily_kline = "16:30"

[notifier]
enabled = true

[notifier.wecom]
enabled = true
webhook_url = "https://example.com/wecom"
""",
        encoding="utf-8",
    )

    cfg = load_config(cfg_file)

    assert cfg.wecom_webhook_url == "https://example.com/wecom"
    assert cfg.notifier_enabled is True


def test_load_config_disables_wecom_when_nested_wecom_disabled(tmp_path):
    cfg_file = tmp_path / "settings.toml"
    cfg_file.write_text(
        """
[tushare]
token = "test_token"

[paths]
data_dir = "data"
db_path = "db/meta.duckdb"
log_path = "logs/pipeline.log"

[scheduler]
daily_kline = "16:30"

[notifier]
enabled = true

[notifier.wecom]
enabled = false
webhook_url = "https://example.com/wecom"
""",
        encoding="utf-8",
    )

    cfg = load_config(cfg_file)

    assert cfg.wecom_webhook_url == "https://example.com/wecom"
    assert cfg.notifier_enabled is False


def test_load_config_defaults_quality_disabled(tmp_path):
    cfg_file = tmp_path / "settings.toml"
    cfg_file.write_text(
        """
[tushare]
token = "token"

[paths]
data_dir = "data"
db_path = "db/meta.duckdb"
log_path = "logs/pipeline.log"

[scheduler]
daily_kline = "18:00"

[notifier]
wecom_webhook_url = ""
enabled = false
""",
        encoding="utf-8",
    )

    cfg = load_config(cfg_file)

    assert cfg.quality.enabled is False
    assert cfg.quality.mode == "daily"
    assert cfg.quality.markets == ["stock", "index", "etf", "futures", "options"]
    assert cfg.quality.notify_on == ["warn", "fail"]


def test_load_config_file_not_found():
    with pytest.raises(FileNotFoundError, match="配置文件不存在"):
        load_config(Path("nonexistent/settings.toml"))


def test_load_config_missing_key(tmp_path):
    cfg_file = tmp_path / "settings.toml"
    cfg_file.write_text(
        "[tushare]\n"
        "# token missing\n"
        "[paths]\n"
        "data_dir='data'\n"
        "db_path='db/meta.duckdb'\n"
        "log_path='logs/pipeline.log'\n"
        "[scheduler]\n"
        "trade_cal = '09:00'\n"
        "[notifier]\n"
        "wecom_webhook_url='https://x.com'\n"
        "enabled=false\n",
        encoding="utf-8",
    )
    with pytest.raises(KeyError, match="配置文件缺少必要字段"):
        load_config(cfg_file)


def test_load_config_invalid_schedule_format(tmp_path):
    cfg_file = tmp_path / "settings.toml"
    cfg_file.write_text(
        "[tushare]\n"
        "token = 'test'\n"
        "[paths]\n"
        "data_dir='data'\n"
        "db_path='db/meta.duckdb'\n"
        "log_path='logs/pipeline.log'\n"
        "[scheduler]\n"
        "trade_cal = 'not_a_time'\n"
        "[notifier]\n"
        "wecom_webhook_url='https://x.com'\n"
        "enabled=false\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="调度时间格式错误"):
        load_config(cfg_file)


def test_load_config_out_of_range_schedule_time(tmp_path):
    cfg_file = tmp_path / "settings.toml"
    cfg_file.write_text(
        "[tushare]\n"
        "token = 'test'\n"
        "[paths]\n"
        "data_dir='data'\n"
        "db_path='db/meta.duckdb'\n"
        "log_path='logs/pipeline.log'\n"
        "[scheduler]\n"
        "trade_cal = '25:00'\n"
        "[notifier]\n"
        "wecom_webhook_url='https://x.com'\n"
        "enabled=false\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="调度时间格式错误"):
        load_config(cfg_file)


def test_config_is_immutable(tmp_path):
    cfg_file = tmp_path / "settings.toml"
    cfg_file.write_text(VALID_TOML, encoding="utf-8")
    cfg = load_config(cfg_file)
    with pytest.raises(Exception):
        cfg.tushare_token = "hacked"


def test_load_config_defaults_ricequant_disabled(tmp_path):
    cfg_file = tmp_path / "settings.toml"
    cfg_file.write_text(VALID_TOML, encoding="utf-8")

    cfg = load_config(cfg_file)

    assert cfg.ricequant.enabled is False
    assert cfg.ricequant.username == ""
    assert cfg.ricequant.password == ""
    assert cfg.ricequant.license_key == ""
    assert cfg.ricequant.stock_minute.request_sleep_seconds == 0.2
    assert cfg.ricequant.stock_minute.batch_size == 1000
    assert cfg.ricequant.stock_minute.adjust_type == "none"
    assert cfg.ricequant.stock_minute.skip_suspended is True


def test_load_config_parses_ricequant_section(tmp_path):
    cfg_file = tmp_path / "settings.toml"
    cfg_file.write_text(
        VALID_TOML
        + """

[ricequant]
enabled = true
username = "rq_user"
password = "rq_password"
license_key = ""

[ricequant.stock_minute]
request_sleep_seconds = 0.5
batch_size = 500
adjust_type = "none"
skip_suspended = false
""",
        encoding="utf-8",
    )

    cfg = load_config(cfg_file)

    assert cfg.ricequant.enabled is True
    assert cfg.ricequant.username == "rq_user"
    assert cfg.ricequant.password == "rq_password"
    assert cfg.ricequant.license_key == ""
    assert cfg.ricequant.stock_minute.request_sleep_seconds == 0.5
    assert cfg.ricequant.stock_minute.batch_size == 500
    assert cfg.ricequant.stock_minute.adjust_type == "none"
    assert cfg.ricequant.stock_minute.skip_suspended is False


def test_load_config_parses_ricequant_license_key(tmp_path):
    cfg_file = tmp_path / "settings.toml"
    cfg_file.write_text(
        VALID_TOML
        + """

[ricequant]
enabled = true
license_key = "rq_license_key"
""",
        encoding="utf-8",
    )

    cfg = load_config(cfg_file)

    assert cfg.ricequant.enabled is True
    assert cfg.ricequant.username == ""
    assert cfg.ricequant.password == ""
    assert cfg.ricequant.license_key == "rq_license_key"


def test_load_config_rejects_ambiguous_ricequant_credentials(tmp_path):
    cfg_file = tmp_path / "settings.toml"
    cfg_file.write_text(
        VALID_TOML
        + """

[ricequant]
enabled = true
username = "rq_user"
password = "rq_password"
license_key = "rq_license_key"
""",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="ricequant credentials"):
        load_config(cfg_file)


def test_load_config_rejects_enabled_ricequant_without_credentials(tmp_path):
    cfg_file = tmp_path / "settings.toml"
    cfg_file.write_text(
        VALID_TOML
        + """

[ricequant]
enabled = true
""",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="ricequant credentials"):
        load_config(cfg_file)


def test_load_config_rejects_unsupported_ricequant_adjust_type(tmp_path):
    cfg_file = tmp_path / "settings.toml"
    cfg_file.write_text(
        VALID_TOML
        + """

[ricequant]
enabled = true
username = "rq_user"
password = "rq_password"

[ricequant.stock_minute]
adjust_type = "pre"
""",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="ricequant.stock_minute.adjust_type"):
        load_config(cfg_file)


def test_load_config_parses_wecom_and_feishu_notifier_sections(tmp_path):
    cfg_file = tmp_path / "settings.toml"
    cfg_file.write_text(
        VALID_TOML
        + """

[notifier.wecom]
enabled = true
webhook_url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=example"

[notifier.feishu]
enabled = true
webhook_url = "https://open.feishu.cn/open-apis/bot/v2/hook/example"
""",
        encoding="utf-8",
    )
    cfg = load_config(cfg_file)
    assert cfg.notifier.enabled is False
    assert cfg.notifier.wecom.enabled is True
    assert cfg.notifier.wecom.webhook_url == "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=example"
    assert cfg.notifier.feishu.enabled is True
    assert cfg.notifier.feishu.webhook_url == "https://open.feishu.cn/open-apis/bot/v2/hook/example"


def test_load_config_keeps_legacy_wecom_webhook_url_compatibility(tmp_path):
    cfg_file = tmp_path / "settings.toml"
    cfg_file.write_text(VALID_TOML, encoding="utf-8")
    cfg = load_config(cfg_file)
    assert cfg.wecom_webhook_url == "https://example.com/webhook"
    assert cfg.notifier.wecom.webhook_url == "https://example.com/webhook"
