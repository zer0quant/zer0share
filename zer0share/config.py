from dataclasses import dataclass
from pathlib import Path
import tomllib


@dataclass(frozen=True)
class Config:
    tushare_token: str
    data_dir: Path
    db_path: Path
    log_path: Path
    scheduler_daily_kline_hour: int
    scheduler_daily_kline_minute: int
    scheduler_basic_hour: int
    scheduler_adj_factor_hour: int
    scheduler_adj_factor_minute: int
    scheduler_futures_hour: int
    scheduler_futures_start_minute: int
    wecom_webhook_url: str
    notifier_enabled: bool


def load_config(path: Path = Path("config/settings.toml")) -> Config:
    if not path.exists():
        raise FileNotFoundError(f"配置文件不存在: {path}")
    try:
        with open(path, "rb") as f:
            raw = tomllib.load(f)
    except tomllib.TOMLDecodeError as e:
        raise ValueError(f"配置文件格式错误: {e}") from e
    try:
        return Config(
            tushare_token=raw["tushare"]["token"],
            data_dir=Path(raw["paths"]["data_dir"]),
            db_path=Path(raw["paths"]["db_path"]),
            log_path=Path(raw["paths"]["log_path"]),
            scheduler_daily_kline_hour=raw["scheduler"]["daily_kline_hour"],
            scheduler_daily_kline_minute=raw["scheduler"]["daily_kline_minute"],
            scheduler_basic_hour=raw["scheduler"]["basic_hour"],
            scheduler_adj_factor_hour=raw["scheduler"]["adj_factor_hour"],
            scheduler_adj_factor_minute=raw["scheduler"]["adj_factor_minute"],
            scheduler_futures_hour=raw["scheduler"]["futures_hour"],
            scheduler_futures_start_minute=raw["scheduler"]["futures_start_minute"],
            wecom_webhook_url=raw["notifier"]["wecom_webhook_url"],
            notifier_enabled=raw["notifier"]["enabled"],
        )
    except KeyError as e:
        raise KeyError(f"配置文件缺少必要字段: {e}") from e
