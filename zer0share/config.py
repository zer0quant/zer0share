from dataclasses import dataclass
from pathlib import Path
import re
import tomllib


@dataclass(frozen=True)
class RiceQuantStockMinuteConfig:
    request_sleep_seconds: float
    batch_size: int
    adjust_type: str
    skip_suspended: bool


@dataclass(frozen=True)
class RiceQuantETFMinuteConfig:
    enabled: bool
    request_sleep_seconds: float
    batch_size: int
    adjust_type: str
    skip_suspended: bool


@dataclass(frozen=True)
class WeComNotifierConfig:
    enabled: bool
    webhook_url: str


@dataclass(frozen=True)
class FeishuNotifierConfig:
    enabled: bool
    webhook_url: str


@dataclass(frozen=True)
class NotifierConfig:
    enabled: bool
    wecom: WeComNotifierConfig
    feishu: FeishuNotifierConfig


@dataclass(frozen=True)
class RiceQuantConfig:
    enabled: bool
    username: str
    password: str
    license_key: str
    stock_minute: RiceQuantStockMinuteConfig
    etf_minute: RiceQuantETFMinuteConfig


@dataclass(frozen=True)
class QualityConfig:
    enabled: bool
    mode: str
    markets: list[str]
    notify_on: list[str]


@dataclass(frozen=True)
class Config:
    tushare_token: str
    tushare_http_url: str
    data_dir: Path
    db_path: Path
    log_path: Path
    schedule: dict[str, str]  # table_name → "HH:MM"
    wecom_webhook_url: str
    notifier_enabled: bool
    notifier: NotifierConfig
    ricequant: RiceQuantConfig
    quality: QualityConfig


def _parse_ricequant(raw: dict) -> RiceQuantConfig:
    raw_rq = raw.get("ricequant", {})
    raw_stock_minute = raw_rq.get("stock_minute", {})
    adjust_type = raw_stock_minute.get("adjust_type", "none")
    if adjust_type != "none":
        raise ValueError("ricequant.stock_minute.adjust_type currently only supports 'none'")

    raw_etf_minute = raw_rq.get("etf_minute", {})
    etf_adjust_type = raw_etf_minute.get("adjust_type", "none")
    if etf_adjust_type != "none":
        raise ValueError("ricequant.etf_minute.adjust_type currently only supports 'none'")

    enabled = bool(raw_rq.get("enabled", False))
    username = str(raw_rq.get("username", ""))
    password = str(raw_rq.get("password", ""))
    license_key = str(raw_rq.get("license_key", ""))
    has_user_password = bool(username or password)
    if enabled and license_key and has_user_password:
        raise ValueError("ricequant credentials must use either license_key or username/password, not both")
    if enabled and license_key == "" and not (username and password):
        raise ValueError("ricequant credentials require license_key or both username and password")
    return RiceQuantConfig(
        enabled=enabled,
        username=username,
        password=password,
        license_key=license_key,
        stock_minute=RiceQuantStockMinuteConfig(
            request_sleep_seconds=float(raw_stock_minute.get("request_sleep_seconds", 0.2)),
            batch_size=int(raw_stock_minute.get("batch_size", 1000)),
            adjust_type=adjust_type,
            skip_suspended=bool(raw_stock_minute.get("skip_suspended", True)),
        ),
        etf_minute=RiceQuantETFMinuteConfig(
            enabled=bool(raw_etf_minute.get("enabled", False)),
            request_sleep_seconds=float(raw_etf_minute.get("request_sleep_seconds", 0.2)),
            batch_size=int(raw_etf_minute.get("batch_size", 500)),
            adjust_type=etf_adjust_type,
            skip_suspended=bool(raw_etf_minute.get("skip_suspended", True)),
        ),
    )


def _parse_schedule(raw_scheduler: dict) -> dict[str, str]:
    schedule = {}
    pattern = re.compile(r"^\d{1,2}:\d{2}$")
    for table, time_str in raw_scheduler.items():
        if not isinstance(time_str, str) or not pattern.match(time_str):
            raise ValueError(f"调度时间格式错误 ({table}): 期望 'HH:MM', 得到 {time_str!r}")
        hour, minute = int(time_str.split(":")[0]), int(time_str.split(":")[1])
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise ValueError(f"调度时间格式错误 ({table}): 期望 'HH:MM', 得到 {time_str!r}")
        schedule[table] = time_str
    return schedule


def _parse_notifier(raw: dict) -> NotifierConfig:
    raw_notifier = raw.get("notifier", {})
    enabled = bool(raw_notifier.get("enabled", False))
    raw_wecom = raw_notifier.get("wecom", {})
    raw_feishu = raw_notifier.get("feishu", {})
    legacy_wecom_url = str(raw_notifier.get("wecom_webhook_url", ""))
    legacy_webhook_url = str(raw_notifier.get("webhook_url", ""))

    wecom_url = str(raw_wecom.get("webhook_url", legacy_wecom_url))
    feishu_url = str(raw_feishu.get("webhook_url", legacy_webhook_url))
    return NotifierConfig(
        enabled=enabled,
        wecom=WeComNotifierConfig(
            enabled=bool(raw_wecom.get("enabled", enabled and bool(wecom_url))),
            webhook_url=wecom_url,
        ),
        feishu=FeishuNotifierConfig(
            enabled=bool(raw_feishu.get("enabled", enabled and bool(feishu_url))),
            webhook_url=feishu_url,
        ),
    )


def _parse_quality(raw_quality: dict | None) -> QualityConfig:
    raw_quality = raw_quality or {}
    return QualityConfig(
        enabled=bool(raw_quality.get("enabled", False)),
        mode=str(raw_quality.get("mode", "daily")),
        markets=list(raw_quality.get("markets", ["stock", "index", "etf", "futures", "options"])),
        notify_on=list(raw_quality.get("notify_on", ["warn", "fail"])),
    )


def _parse_wecom_notifier(raw_notifier: dict) -> tuple[str, bool]:
    if "wecom_webhook_url" in raw_notifier:
        return raw_notifier["wecom_webhook_url"], bool(raw_notifier["enabled"])

    raw_wecom = raw_notifier.get("wecom", {})
    webhook_url = raw_wecom.get("webhook_url", "")
    enabled = bool(raw_notifier.get("enabled", False)) and bool(raw_wecom.get("enabled", False))
    return webhook_url, enabled
def load_config(path: Path = Path("config/settings.toml")) -> Config:
    if not path.exists():
        raise FileNotFoundError(f"配置文件不存在: {path}")
    try:
        with open(path, "rb") as f:
            raw = tomllib.load(f)
    except tomllib.TOMLDecodeError as e:
        raise ValueError(f"配置文件格式错误: {e}") from e
    try:
        notifier = _parse_notifier(raw)
        wecom_webhook_url, notifier_enabled = _parse_wecom_notifier(raw["notifier"])
        return Config(
            tushare_http_url=raw["tushare"].get("http_url", ""),
            tushare_token=raw["tushare"]["token"],
            data_dir=Path(raw["paths"]["data_dir"]),
            db_path=Path(raw["paths"]["db_path"]),
            log_path=Path(raw["paths"]["log_path"]),
            schedule=_parse_schedule(raw["scheduler"]),
            wecom_webhook_url=wecom_webhook_url,
            notifier_enabled=notifier_enabled,
            notifier=notifier,
            ricequant=_parse_ricequant(raw),
            quality=_parse_quality(raw.get("quality")),
        )
    except KeyError as e:
        raise KeyError(f"配置文件缺少必要字段: {e}") from e
