# RiceQuant 分钟线

RiceQuant 数据源通过独立入口同步和查询，不混入 `pro_api()`。

认证支持 license key 或用户名/密码二选一：

```toml
[ricequant]
enabled = true
license_key = "your_ricequant_license_key"
```

或：

```toml
[ricequant]
enabled = true
username = "your_username"
password = "your_password"
```

同步基础信息和分钟线：

```bash
uv run python main.py sync --table ricequant_basic
uv run python main.py sync --table ricequant_stock_minute --start-date 20240102 --end-date 20240102
```

本地查询：

```python
from zer0share import rq_api

rq = rq_api()
basic = rq.all_instruments(type="CS", market="cn")
df = rq.get_price(
    "000001.XSHE",
    start_date="20240102",
    end_date="20240102",
    frequency="1m",
)
```
