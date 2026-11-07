# 期权基础信息查询参数设计

## 背景

`opt_basic` 同步已经改为拉取全部期权基础信息并覆盖写入 `data/options/opt_basic/data.parquet`。本地查询 API 当前支持 `ts_code`、`exchange`、`opt_code`、`call_put` 和 `fields`，但还没有覆盖 Tushare 常用查询参数 `name`、`list_date`、`offset`、`limit`。

## 需求

- `pro.opt_basic()` 支持按 `name` 精确过滤
- `pro.opt_basic()` 支持按 `list_date` 精确过滤，输入格式为 `YYYYMMDD`
- `pro.opt_basic()` 支持 `limit` 和 `offset` 分页
- 保持现有参数和字段返回行为不变
- 查询仍只访问本地 Parquet，不访问 Tushare

## 设计

### 1. API 参数扩展

**文件**：`zer0share/api.py`

`LocalProAPI.opt_basic()` 新增参数：

```python
name: str | None = None
list_date: str | None = None
offset: int | None = None
limit: int | None = None
```

现有参数保留：

```python
ts_code: str | None = None
exchange: str | None = None
opt_code: str | None = None
call_put: str | None = None
fields: str | list[str] | None = None
```

### 2. 过滤规则

- `name` 使用 `name = ?` 精确匹配
- `list_date` 使用 `list_date = ?` 精确匹配
- `ts_code` 保持现有逗号分隔多代码查询能力
- `exchange`、`opt_code`、`call_put` 保持现有精确匹配能力
- 多个过滤条件之间使用 `AND`

### 3. 分页规则

结果继续先 `ORDER BY ts_code`，再应用分页：

```sql
ORDER BY ts_code
LIMIT ?
OFFSET ?
```

`limit` 为 `None` 时不加 `LIMIT`。`offset` 为 `None` 时不加 `OFFSET`；如果只传 `offset` 不传 `limit`，使用 DuckDB 支持的 `OFFSET ?`，仍保持全量结果跳过前 N 行。

### 4. 日期返回格式

查询结果继续调用 `_format_date_columns()`，把 `list_date`、`delist_date`、`maturity_date`、`last_edate`、`last_ddate` 格式化为 `YYYYMMDD` 字符串。

### 5. 测试

**文件**：`tests/test_api.py`

新增或扩展 `opt_basic` 相关测试：

| 测试 | 覆盖内容 |
|------|---------|
| `test_opt_basic_filters_by_name_and_list_date` | 验证 `name` 与 `list_date` 精确过滤 |
| `test_opt_basic_supports_limit_and_offset` | 验证分页在 `ORDER BY ts_code` 后生效 |
| `test_opt_basic_combines_new_filters_with_fields` | 验证新过滤参数与 `fields` 可组合使用 |

## 改动清单

| 文件 | 改动 |
|------|------|
| `zer0share/api.py` | `opt_basic()` 新增 `name`、`list_date`、`offset`、`limit` 参数与 SQL 处理 |
| `tests/test_api.py` | 增加本地 `opt_basic` 查询参数测试 |
