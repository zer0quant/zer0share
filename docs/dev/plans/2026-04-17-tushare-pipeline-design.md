# Tushare 数据管道设计

**日期:** 2026-04-17  
**状态:** 已确认

## 概述

轻量级本地金融数据管道，通过 Tushare 拉取 A 股日线行情和股票基础信息，以 Parquet 格式按日期分区存储，使用 DuckDB 管理增量同步元数据，支持 CLI 手动触发和 APScheduler 定时调度，失败时推送企业微信告警。

## 技术栈

- Python 3.11+
- Tushare（数据源）
- DuckDB（元数据管理 + 查询引擎）
- PyArrow / Parquet（本地存储）
- APScheduler（定时调度）
- Click（CLI）
- Loguru（日志）
- httpx（企业微信 Webhook）

## 项目结构

```
zer0share/
├── config/
│   └── settings.toml        # token、路径、企业微信配置
├── data/
│   ├── daily_kline/         # 日线行情，按日期分区
│   │   ├── date=20240101/
│   │   │   └── data.parquet
│   │   └── date=20240102/
│   │       └── data.parquet
│   └── basic/
│       └── data.parquet     # stock_basic 全字段快照，全量覆盖
├── db/
│   └── meta.duckdb          # 增量同步元数据
├── src/
│   ├── fetcher.py           # Tushare 拉取逻辑
│   ├── storage.py           # DuckDB + Parquet 读写
│   ├── scheduler.py         # APScheduler 定时任务
│   ├── notifier.py          # 企业微信 Webhook
│   └── cli.py               # Click CLI 入口
├── logs/
│   └── pipeline.log
└── main.py
```

## 数据表设计

### daily_kline 字段

| 字段 | 类型 | 说明 |
|------|------|------|
| ts_code | VARCHAR | 股票代码 |
| trade_date | DATE | 交易日期 |
| open | DOUBLE | 开盘价 |
| high | DOUBLE | 最高价 |
| low | DOUBLE | 最低价 |
| close | DOUBLE | 收盘价 |
| pre_close | DOUBLE | 昨收价 |
| change | DOUBLE | 涨跌额 |
| pct_chg | DOUBLE | 涨跌幅 |
| vol | DOUBLE | 成交量（手）|
| amount | DOUBLE | 成交额（千元）|

### basic 字段

`basic` 的设计原则不是精选字段表，而是 `stock_basic` 的本地镜像层。落库时保留官方接口全部字段，仅对 `list_date` 和 `delist_date` 做最小必要的日期类型转换，其余字段保持原样。

| 字段 | 类型 | 说明 |
|------|------|------|
| ts_code | VARCHAR | 股票代码 |
| symbol | VARCHAR | 股票简码 |
| name | VARCHAR | 股票名称 |
| area | VARCHAR | 地区 |
| industry | VARCHAR | 行业 |
| fullname | VARCHAR | 股票全称 |
| enname | VARCHAR | 英文全称 |
| cnspell | VARCHAR | 拼音缩写 |
| market | VARCHAR | 市场类型 |
| exchange | VARCHAR | 交易所代码 |
| curr_type | VARCHAR | 交易货币 |
| list_status | VARCHAR | 上市状态 L/D/P/G |
| list_date | DATE | 上市日期 |
| delist_date | DATE | 退市日期 |
| is_hs | VARCHAR | 是否沪深港通标的 |
| act_name | VARCHAR | 实控人名称 |
| act_ent_type | VARCHAR | 实控人企业性质 |

### DuckDB 元数据表

```sql
CREATE TABLE sync_meta (
    table_name  VARCHAR PRIMARY KEY,  -- 'daily_kline' / 'basic'
    last_date   DATE,
    updated_at  TIMESTAMP
);
```

## 数据流 & 增量逻辑

```
1. 读取 meta.duckdb 中 daily_kline 的 last_date
2. 读取 basic/data.parquet 获取全量 ts_code 列表
3. 按 last_date+1 → 今天，逐日请求 Tushare pro_bar 接口
4. 每天数据写入 data/daily_kline/date=YYYYMMDD/data.parquet
5. 更新 sync_meta 中的 last_date
6. 任何步骤失败 → 记录日志 + 推送企业微信告警

首次运行: start_date=20100104（A股最早交易日）
```

**Basic 同步策略：**
- 每次检查距上次更新是否超过 7 天
- 全量拉取 stock_basic（list_status=L,D,P,G）
- 显式请求官方文档中的全部字段，不做字段裁剪
- 保留 `list_date` / `delist_date` 的日期转换，其余字段原样保留
- 覆盖写入 data/basic/data.parquet

## 设计约束

- `basic/data.parquet` 表示当前 `stock_basic` 的全量快照，而不是当前上市股票子集
- 任何新增分析字段、衍生字段、清洗字段都不直接写入镜像层，应放在下游查询或派生层处理
- 若 Tushare `stock_basic` 字段定义变更，需要显式更新 `fetcher` 中的字段列表和测试用例

## CLI 命令

```bash
python main.py sync --table daily_kline   # 增量同步日线
python main.py sync --table basic         # 全量刷新基础信息
python main.py sync --all                 # 同步全部
python main.py status                     # 显示各表最后更新时间
python main.py scheduler start            # 启动定时调度
```

## 定时任务

```python
# 每天 18:00 自动同步日线（收盘后）
scheduler.add_job(sync_daily_kline, CronTrigger(hour=18, minute=0))

# 每周一 08:00 刷新 basic
scheduler.add_job(sync_basic, CronTrigger(day_of_week='mon', hour=8))
```

## 告警策略

企业微信 Webhook 触发条件：
- Tushare API 请求失败（网络异常 / 积分限流）
- Parquet 写入失败
- 同步完成摘要推送（成功 N 天，跳过 N 天）

## DuckDB 查询示例

```sql
-- 查询某股票历史行情
SELECT * FROM read_parquet('data/daily_kline/date=*/data.parquet')
WHERE ts_code = '000001.SZ'
ORDER BY trade_date;

-- 查询某日全市场数据
SELECT * FROM read_parquet('data/daily_kline/date=20240101/data.parquet');

-- 联表查询（行情 + 基础信息）
SELECT k.*, b.name, b.industry
FROM read_parquet('data/daily_kline/date=*/data.parquet') k
JOIN read_parquet('data/basic/data.parquet') b ON k.ts_code = b.ts_code
WHERE k.trade_date = '2024-01-01';
```
