# 行业数据同步与本地查询设计

## 目标

新增申万一级行业和中信一级行业的股票-行业映射数据，支持全量历史同步和本地零积分查询，用于行业中性化。

## 数据源

| 分类体系 | 接口 | 积分 | 数据内容 |
|---------|------|------|---------|
| 申万行业分类 | `index_classify` | 2000 | 行业分类树（L1/L2/L3） |
| 申万行业成分 | `index_member_all` | 2000 | 股票-行业映射 + 历史变更（`in_date`/`out_date`/`is_new`） |
| 中信行业成分 | `ci_index_member` | 5000 | 股票-行业映射 + 历史变更（同上） |

同步全量历史（`is_new='Y'` + `is_new='N'`），不做行业指数日线行情。

## 方案

全量覆盖，单 Parquet 存储。行业映射数据量小（每套约 1-2 万条历史记录），变动不频繁，每次同步全量拉取覆盖。与现有 `basic` 表模式一致。

## 存储结构

```
data/industry/
  sw_classify/data.parquet    — 申万行业分类树
  sw_member/data.parquet      — 申万股票-行业映射（全量历史）
  ci_member/data.parquet      — 中信股票-行业映射（全量历史）
```

每次同步全量覆盖写，无分区。数据本身通过 `in_date`/`out_date` 描述时效性。

## 各层设计

### Fetcher 层

`TushareFetcher` 新增 3 个方法：

**列定义：**
- `SW_CLASSIFY_COLS`: `index_code`, `industry_name`, `level`, `parent_code`, `industry_code`, `is_pub`, `src`
- `SW_MEMBER_COLS`: `l1_code`, `l1_name`, `l2_code`, `l2_name`, `l3_code`, `l3_name`, `ts_code`, `name`, `in_date`, `out_date`, `is_new`
- `CI_MEMBER_COLS`: `l1_code`, `l1_name`, `l2_code`, `l2_name`, `l3_code`, `l3_name`, `ts_code`, `name`, `in_date`, `out_date`, `is_new`

**方法：**

`fetch_sw_classify(src="SW2021")` — 分别调用 `pro.index_classify(level='L1')`, `L2`, `L3`，合并返回全部分类。

`fetch_sw_member()` — 获取 L1 行业列表后，按每个 L1 行业代码调用 `pro.index_member_all(l1_code=..., is_new='')` 获取当前和历史成分，合并去重。每次调用间隔 0.2s 限速。

`fetch_ci_member()` — 类似 `fetch_sw_member`，调用 `pro.ci_index_member(l1_code=..., is_new='')`。需要先通过不带参数的调用获取所有 L1 代码列表，再逐个拉取。

`in_date`/`out_date` 转为 `date` 类型。

### Storage 层

`storage.py` 新增 6 个函数（每组读写各一）：

- `write_sw_classify` / `read_sw_classify` — `data/industry/sw_classify/data.parquet`
- `write_sw_member` / `read_sw_member` — `data/industry/sw_member/data.parquet`
- `write_ci_member` / `read_ci_member` — `data/industry/ci_member/data.parquet`

写入时 `mkdir(parents=True, exist_ok=True)`，用 PyArrow 写 Parquet。读取时检查文件是否存在，不存在返回空 DataFrame。

### Pipeline 层

`Pipeline` 新增 2 个同步方法：

`sync_industry()` — 全量同步申万数据：
1. 调 `fetch_sw_classify()` → `write_sw_classify()`
2. 调 `fetch_sw_member()` → `write_sw_member()`
3. 更新 `sync_meta`: `sw_classify`, `sw_member`
4. 异常时 logger.error + 通知

`sync_ci_member()` — 全量同步中信数据：
1. 调 `fetch_ci_member()` → `write_ci_member()`
2. 更新 `sync_meta`: `ci_member`
3. 异常时 logger.error + 通知

### CLI 层

`SYNC_TABLES` 新增 `"industry"` 和 `"ci_member"`。

`sync` 命令新增分支：
- `--table industry` → `pipeline.sync_industry()`
- `--table ci_member` → `pipeline.sync_ci_member()`
- `--all` 时自动包含两者

两个方法均不支持 `--start-date`/`--end-date`，属于 `date range options` 限制范围之外。

`status` 命令自动展示 `sw_classify`、`sw_member`、`ci_member` 的最后同步时间。

### LocalPro API 层

`LocalPro` 新增 3 个查询方法：

`index_classify(level=None, src=None, fields=None)` — 读 `sw_classify/data.parquet`，按 `level`/`src` 过滤。

`index_member_all(l1_code=None, ts_code=None, is_new=None, fields=None)` — 读 `sw_member/data.parquet`，按参数过滤。`ts_code` 支持多代码逗号分隔（与 `universe` 一致）。

`ci_index_member(l1_code=None, ts_code=None, is_new=None, fields=None)` — 读 `ci_member/data.parquet`，按参数过滤。

三个方法注册到 `query` dispatch：
```python
"index_classify": self.index_classify,
"index_member_all": self.index_member_all,
"ci_index_member": self.ci_index_member,
```

查询走 DuckDB 直读 Parquet，`in_date`/`out_date` 返回 `%Y%m%d` 字符串格式。

## 使用示例

```python
from zer0share import pro_api
pro = pro_api()

# 申万一级行业列表
pro.index_classify(level='L1', src='SW2021')

# 查某股票当前所属申万一级行业
pro.index_member_all(ts_code='000001.SZ', is_new='Y')

# 查某股票当前所属中信一级行业
pro.ci_index_member(ts_code='000001.SZ', is_new='Y')

# 查某行业所有历史成分
pro.index_member_all(l1_code='801010.SI')

# 通用 dispatch 调用
pro.query("index_member_all", ts_code='000001.SZ')
```

## 涉及文件

| 文件 | 变更 |
|------|------|
| `zer0share/fetcher.py` | 新增 3 个列定义 + 3 个 fetch 方法 |
| `zer0share/storage.py` | 新增 6 个读写函数 |
| `zer0share/pipeline.py` | 新增 2 个 sync 方法 + 对应 import |
| `zer0share/cli.py` | SYNC_TABLES 新增 2 项 + sync 分支 |
| `zer0share/api.py` | 新增 3 个列定义 + 3 个查询方法 + dispatch 注册 |
| `tests/test_fetcher.py` | 新增 fetch 方法测试 |
| `tests/test_storage.py` | 新增读写函数测试 |
| `tests/test_pipeline.py` | 新增 sync 方法测试 |
| `tests/test_api.py` | 新增查询方法测试 |
| `tests/test_cli.py` | 新增 CLI 命令测试 |
