# Examples

Run these examples from the repository root after syncing local data.

所有脚本只读取本地 Parquet 文件，通过 DuckDB 执行，不访问 Tushare，不消耗积分。

---

## 股票（`examples/stock/`）

| 脚本 | 默认参数 |
|------|------|
| `basic_query_smoke.py` | `--ts-code 000001.SZ --market 主板 --exchange SZSE` |
| `daily_query_smoke.py` | `--ts-code 000001.SZ --start-date 20260518 --end-date 20260605` |
| `daily_kline_integrity_smoke.py` | *(数据完整性检查，无过滤参数)* |
| `adj_factor_query_smoke.py` | `--ts-code 000001.SZ --start-date 19901219 --end-date 19911231` |
| `daily_basic_query_smoke.py` | `--ts-code 000001.SZ --start-date 19901219 --end-date 19911231` |
| `stock_st_query_smoke.py` | `--ts-code 000001.SZ --trade-date 20260605` |
| `stk_limit_query_smoke.py` | `--ts-code 000001.SZ --trade-date 20260605` |
| `suspend_d_query_smoke.py` | `--ts-code 000001.SZ --trade-date 20260605` |
| `sw_classify_query_smoke.py` | `--index-code 801020.SI --level L1 --src SW2021` |
| `sw_member_query_smoke.py` | `--ts-code 000001.SZ --l1-code 801780.SI --is-new Y` |
| `ci_member_query_smoke.py` | `--ts-code 000001.SZ --l1-code CI005021.CI --is-new Y` |

```bash
uv run python examples/stock/basic_query_smoke.py
uv run python examples/stock/daily_query_smoke.py
uv run python examples/stock/daily_kline_integrity_smoke.py
uv run python examples/stock/adj_factor_query_smoke.py
uv run python examples/stock/daily_basic_query_smoke.py
uv run python examples/stock/stock_st_query_smoke.py
uv run python examples/stock/stk_limit_query_smoke.py
uv run python examples/stock/suspend_d_query_smoke.py
uv run python examples/stock/sw_classify_query_smoke.py
uv run python examples/stock/sw_member_query_smoke.py
uv run python examples/stock/ci_member_query_smoke.py
```

---

## 指数（`examples/index/`）

| 脚本 | 默认参数 |
|------|------|
| `index_daily_query_smoke.py` | `--ts-code 000001.SH --start-date 19930702 --end-date 19930831` |
| `index_weight_query_smoke.py` | `--index-code 399300.SZ --start-date 20160101 --end-date 20160331` |

```bash
uv run python examples/index/index_daily_query_smoke.py
uv run python examples/index/index_weight_query_smoke.py
```

---

## ETF（`examples/etf/`）

| 脚本 | 默认参数 |
|------|------|
| `etf_basic_query_smoke.py` | `--exchange SH --index-code 000300.SH --list-status L` |
| `etf_index_query_smoke.py` | `--ts-code 000300.SH` |
| `fund_daily_query_smoke.py` | `--ts-code 510330.SH --start-date 20250101 --end-date 20250618` |
| `etf_sh_cons_query_smoke.py` | `--limit 5` |

```bash
uv run python examples/etf/etf_basic_query_smoke.py
uv run python examples/etf/etf_index_query_smoke.py
uv run python examples/etf/fund_daily_query_smoke.py
uv run python examples/etf/etf_sh_cons_query_smoke.py
```

---

## 交易日历（`examples/calendar/`）

| 脚本 | 默认参数 |
|------|------|
| `trade_cal_smoke.py` | `--exchange SSE --start-date 20240101 --end-date 20240131` |

```bash
uv run python examples/calendar/trade_cal_smoke.py
```

---

## 期货（`examples/futures/`）

| 脚本 | 默认参数 |
|------|------|
| `fut_basic_query_smoke.py` | `--exchange SHFE` |
| `fut_daily_query_smoke.py` | `--ts-code RB2501.SHFE --start-date 20240101` |
| `fut_holding_query_smoke.py` | `--symbol CU0201 --exchange SHFE --start-date 20020107` |
| `fut_wsr_query_smoke.py` | `--symbol A --exchange DCE --start-date 20070101` |
| `fut_settle_query_smoke.py` | `--ts-code CF1201.ZCE --exchange CZCE --start-date 20120104` |
| `fut_mapping_query_smoke.py` | `--ts-code RB.SHFE --start-date 20240101` |
| `ft_limit_query_smoke.py` | `--ts-code CU0502.SHF --exchange SHFE --start-date 20050104` |
| `fut_weekly_monthly_query_smoke.py` | `--ts-code RB2501.SHFE --start-date 20240101` |
| `fut_index_daily_query_smoke.py` | `--ts-code NHCI.NH --start-date 20060104` |
| `fut_weekly_detail_query_smoke.py` | `--exchange SHFE --prd CU --start-date 20160101` |

```bash
uv run python examples/futures/fut_basic_query_smoke.py
uv run python examples/futures/fut_daily_query_smoke.py
uv run python examples/futures/fut_holding_query_smoke.py
uv run python examples/futures/fut_settle_query_smoke.py
uv run python examples/futures/fut_wsr_query_smoke.py
uv run python examples/futures/ft_limit_query_smoke.py
uv run python examples/futures/fut_index_daily_query_smoke.py
uv run python examples/futures/fut_weekly_detail_query_smoke.py
```

---

## 期权（`examples/options/`）

| 脚本 | 默认参数 |
|------|------|
| `opt_basic_query_smoke.py` | `--exchange SSE` |
| `opt_daily_query_smoke.py` | `--ts-code 10000001.SH --exchange SSE --start-date 20150209` |

```bash
uv run python examples/options/opt_basic_query_smoke.py
uv run python examples/options/opt_daily_query_smoke.py
```
