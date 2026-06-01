from __future__ import annotations

import argparse
import calendar
from datetime import date, timedelta
from html import escape
from pathlib import Path

import duckdb


def main() -> None:
    parser = argparse.ArgumentParser(description="Plot a stock daily return calendar heatmap as SVG.")
    parser.add_argument("--ts-code", required=True, help="Stock code, e.g. 688585.SH")
    parser.add_argument("--data-dir", default="data", help="Local zer0share data directory")
    parser.add_argument("--output", required=True, help="Output SVG path")
    parser.add_argument("--cap", type=float, default=20.0, help="Color cap for pct_chg, in percent")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    rows = load_daily_rows(data_dir, args.ts_code)
    if not rows:
        raise SystemExit(f"No daily data found for {args.ts_code}")

    stock_name = load_stock_name(data_dir, args.ts_code) or args.ts_code
    svg = render_svg(args.ts_code, stock_name, rows, args.cap)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(svg, encoding="utf-8")
    print(f"Wrote {output}")


def load_daily_rows(data_dir: Path, ts_code: str) -> list[tuple[date, float, float]]:
    pattern = data_dir / "daily_kline" / "date=*" / "data.parquet"
    sql = """
        SELECT trade_date, close, pct_chg
        FROM read_parquet(?, hive_partitioning=true)
        WHERE ts_code = ?
        ORDER BY trade_date
    """
    return duckdb.connect().execute(sql, [str(pattern), ts_code]).fetchall()


def load_stock_name(data_dir: Path, ts_code: str) -> str | None:
    path = data_dir / "basic" / "data.parquet"
    if not path.exists():
        return None
    row = duckdb.connect().execute(
        "SELECT name FROM read_parquet(?) WHERE ts_code = ? LIMIT 1",
        [str(path), ts_code],
    ).fetchone()
    return row[0] if row else None


def render_svg(ts_code: str, stock_name: str, rows: list[tuple[date, float, float]], cap: float) -> str:
    by_date = {trade_date: (close, pct_chg) for trade_date, close, pct_chg in rows}
    years = sorted({trade_date.year for trade_date, _, _ in rows})
    first_date = rows[0][0]
    last_date = rows[-1][0]

    cell = 13
    gap = 3
    label_w = 58
    top = 98
    year_gap = 34
    grid_w = 53 * (cell + gap)
    grid_h = 7 * (cell + gap) - gap
    width = label_w + grid_w + 44
    height = top + len(years) * (grid_h + year_gap) + 78

    parts = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        "<style>",
        "text{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI','Microsoft YaHei',Arial,sans-serif;fill:#27313f}",
        ".title{font-size:24px;font-weight:700}.sub{font-size:13px;fill:#667085}.axis{font-size:11px;fill:#7a8594}",
        ".year{font-size:15px;font-weight:700}.cell{stroke:#ffffff;stroke-width:1}.legend{font-size:11px;fill:#667085}",
        "</style>",
        '<rect x="0" y="0" width="100%" height="100%" fill="#ffffff"/>',
        f'<text class="title" x="24" y="34">{escape(stock_name)} {escape(ts_code)} 日历热力图</text>',
        f'<text class="sub" x="24" y="58">单日涨跌幅，红色上涨、绿色下跌；颜色按 ±{cap:g}% 截断。数据区间：{first_date:%Y-%m-%d} 至 {last_date:%Y-%m-%d}</text>',
    ]

    month_labels = "一月 二月 三月 四月 五月 六月 七月 八月 九月 十月 十一月 十二月".split()
    weekday_labels = ["一", "二", "三", "四", "五", "六", "日"]

    for i, year in enumerate(years):
        y0 = top + i * (grid_h + year_gap)
        x0 = label_w
        parts.append(f'<text class="year" x="24" y="{y0 + 18}">{year}</text>')

        for weekday, label in enumerate(weekday_labels):
            if weekday in (0, 2, 4):
                y = y0 + weekday * (cell + gap) + 11
                parts.append(f'<text class="axis" x="{x0 - 24}" y="{y}">周{label}</text>')

        for month in range(1, 13):
            month_start = date(year, month, 1)
            week = week_index(month_start)
            x = x0 + week * (cell + gap)
            parts.append(f'<text class="axis" x="{x}" y="{y0 - 8}">{month_labels[month - 1]}</text>')

        start = date(year, 1, 1)
        end = date(year, 12, 31)
        cur = start
        while cur <= end:
            week = week_index(cur)
            weekday = cur.weekday()
            x = x0 + week * (cell + gap)
            y = y0 + weekday * (cell + gap)
            if cur in by_date:
                close, pct_chg = by_date[cur]
                color = return_color(pct_chg, cap)
                title = f"{cur:%Y-%m-%d} close={close:.2f} pct_chg={pct_chg:.2f}%"
                parts.append(
                    f'<rect class="cell" x="{x}" y="{y}" width="{cell}" height="{cell}" rx="2" '
                    f'fill="{color}"><title>{escape(title)}</title></rect>'
                )
            else:
                parts.append(
                    f'<rect class="cell" x="{x}" y="{y}" width="{cell}" height="{cell}" rx="2" fill="#f1f4f8"/>'
                )
            cur += timedelta(days=1)

    legend_y = height - 42
    legend_x = label_w
    parts.append(f'<text class="legend" x="24" y="{legend_y + 10}">涨跌幅</text>')
    for i, value in enumerate([-cap, -cap / 2, 0, cap / 2, cap]):
        x = legend_x + i * 62
        parts.append(f'<rect x="{x}" y="{legend_y}" width="34" height="13" rx="2" fill="{return_color(value, cap)}"/>')
        parts.append(f'<text class="legend" x="{x}" y="{legend_y + 29}">{value:g}%</text>')
    parts.append("</svg>")
    return "\n".join(parts)


def week_index(day: date) -> int:
    first = date(day.year, 1, 1)
    return ((day - first).days + first.weekday()) // 7


def return_color(pct_chg: float, cap: float) -> str:
    value = max(-cap, min(cap, pct_chg)) / cap
    if value > 0:
        return blend((255, 245, 242), (198, 40, 40), value)
    if value < 0:
        return blend((241, 248, 244), (46, 125, 50), -value)
    return "#eef2f6"


def blend(start: tuple[int, int, int], end: tuple[int, int, int], t: float) -> str:
    rgb = [round(a + (b - a) * t) for a, b in zip(start, end, strict=True)]
    return "#" + "".join(f"{channel:02x}" for channel in rgb)


if __name__ == "__main__":
    main()
