#!/usr/bin/env python3
"""CLI entrypoint for long-running RiceQuant historical minute sync.

Usage:
    uv run python scripts/sync_ricequant_history.py \\
        --start-date 20160101 --end-date 20161231 \\
        --max-bytes 50G --stop-remaining-below 8G
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Ensure the package root is on sys.path when run as a standalone script.
_PROJECT_ROOT = Path(__file__).parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="RiceQuant historical minute sync")
    parser.add_argument("--start-date", required=True, help="YYYYMMDD")
    parser.add_argument("--end-date", required=True, help="YYYYMMDD")
    parser.add_argument("--chunk", default="month", choices=["month"], help="Chunk strategy")
    parser.add_argument("--max-bytes", default=None, help="Max quota bytes to use (e.g. 50G)")
    parser.add_argument("--stop-remaining-below", default=None, help="Stop if remaining below (e.g. 8G)")
    parser.add_argument("--retries", type=int, default=3, help="Retry count per day")
    parser.add_argument("--config", default="config/settings.toml", help="Config file path")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    from zer0share.config import load_config
    from zer0share.logging import init_logger
    from zer0share.notifier import build_notifier
    from zer0share.pipeline import Pipeline
    from zer0share.sources import DataSources, RiceQuantFetcher, TushareFetcher
    from zer0share.storage import MetaStore
    from zer0share.trading_calendar import TradingCalendar
    from zer0share.ricequant_history import (
        RiceQuantHistoryManifest,
        RiceQuantHistoryRunner,
        parse_bytes,
    )

    cfg = load_config(Path(args.config))
    init_logger(cfg.log_path)

    print(f"[sync_ricequant_history] {args.start_date}~{args.end_date}")

    notifier = build_notifier(cfg.notifier)

    ricequant_fetcher = (
        RiceQuantFetcher(
            username=cfg.ricequant.username,
            password=cfg.ricequant.password,
            license_key=cfg.ricequant.license_key,
        )
        if cfg.ricequant.enabled
        else None
    )
    sources = DataSources(tushare=TushareFetcher(cfg.tushare_token, cfg.tushare_http_url), ricequant=ricequant_fetcher)

    pipeline = Pipeline(cfg, sources, notifier)
    manifest = RiceQuantHistoryManifest(cfg.db_path)
    meta = MetaStore(cfg.db_path)
    calendar = TradingCalendar(meta)

    runner = RiceQuantHistoryRunner(
        pipeline=pipeline,
        manifest=manifest,
        calendar=calendar,
        data_dir=cfg.data_dir,
        notifier=notifier,
    )

    runner.run(
        args.start_date,
        args.end_date,
        chunk=args.chunk,
        max_bytes=parse_bytes(args.max_bytes),
        stop_remaining_below=parse_bytes(args.stop_remaining_below),
        retries=args.retries,
    )

    print(
        f"[sync_ricequant_history] 完成 {args.start_date}~{args.end_date}"
    )


if __name__ == "__main__":
    main()
