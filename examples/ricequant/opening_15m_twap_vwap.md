# 开盘后 15 分钟 TWAP / VWAP 示例

脚本：`examples/ricequant/opening_15m_twap_vwap.py`。在项目根目录运行。

适用于中国内地 A 股、ETF，不适用于期货、可转债或指数。默认读取已经同步的本地米筐分钟数据；`--source online` 直接访问 RiceQuant，会消耗账户数据配额。

## 计算口径

- 时间窗口：北京时间 09:30 开盘后的 15 分钟，取结束时间标签为 **09:31—09:45（含两端）** 的 15 根一分钟 K 线。
- `twap_15m`：15 个不复权分钟收盘价的算术平均。这是分钟采样 TWAP，不是逐 tick 时间积分，也不保证与米筐算法订单撮合价一致。
- `vwap_15m`：窗口内 `sum(total_turnover) / sum(volume)`。A 股、ETF 的成交额单位为元、成交量单位为股/份，结果为元/股或元/份。不能对分钟 VWAP 直接取算术平均得到窗口 VWAP。
- 不复权，保留零成交量分钟的有效收盘价参与 TWAP；不自动补缺失分钟。缺失/重复分钟或异常数值时，均价输出为空；总成交量为零时 VWAP 为空。
- 按证券、日期分别输出，不把多日混在一起计算。

09:31 分钟线是否包含开盘集合竞价成交，取决于供应商分钟线的合成口径。本示例保留其原始成交量/额，未单独扣除竞价。若要求严格排除竞价，需要核实该口径或改用带交易阶段信息的 tick 数据。

## 本地取数

先同步需要的日期（已有数据可直接跳过）：

```bash
uv run python main.py sync --table ricequant_basic
uv run python main.py sync --table ricequant_stock_minute --start-date 20240102 --end-date 20240105
```

计算多只股票、多个交易日：

```bash
uv run python examples/ricequant/opening_15m_twap_vwap.py \
  --order-book-ids 000001.XSHE 600000.XSHG \
  --start-date 20240102 --end-date 20240105
```

本地 ETF 数据使用 `--asset etf --order-book-ids 510050.XSHG`，需要先同步 `ricequant_etf_basic` 和 `ricequant_etf_minute`。

## 米筐在线取数

在 `config/settings.toml` 的 `[ricequant]` 中配置 `license_key`，或配置 `username` / `password`（二选一）。使用现有项目配置，不要把凭据写入示例脚本。

```bash
uv run python examples/ricequant/opening_15m_twap_vwap.py \
  --source online --order-book-ids 000001.XSHE \
  --start-date 20240102 --end-date 20240105 \
  --output data/analysis/pingan_opening_15m.csv
```

在线示例按日期拉取分钟行情，然后筛选窗口；账户需要相应品种的分钟行情权限。

## 输出

默认输出 `data/analysis/opening_15m_twap_vwap.csv`，字段包括证券代码、日期、窗口分钟数、完整性、TWAP、VWAP、成交量、成交额和 `status`。

只有 `status=ok` 才表示两种均价均有效。成交量/额是实际收到的窗口数据之和；`complete=False` 时它们也是不完整的。没有返回任何行情的证券/日期（例如停牌、休市或本地完全缺数据）不会生成结果行。

接口参考：[RiceQuant get_price / get_vwap 官方文档](https://www.ricequant.com/doc/rqdata/python/generic-api)。
