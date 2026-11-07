# 开发笔记

按时间顺序记录 `zer0share` 的设计过程、踩坑记录和后续扩展。文章发布在知乎，微信公众号「极客投研笔记」同步更新。

| 文章 | 内容 |
|---|---|
| [我用 Tushare + Claude Code，手搓了一套本地股票数据同步系统（已开源）](https://zhuanlan.zhihu.com/p/2028821029410154089) | 项目起点：Tushare Pro 拉取、Parquet 分区存储、DuckDB 本地查询和增量同步的整体设计 |
| [我用 Tushare + Codex，把本地股票数据库补上了前后复权行情（已开源）](https://zhuanlan.zhihu.com/p/2031374938871944513) | 复权因子同步与本地 `pro_bar()` 前复权 / 后复权行情查询 |
| [我用 Hermes Agent + zer0share-data skill，把本地数据库变成了可对话的投研助手](https://zhuanlan.zhihu.com/p/2049206897111196557) | `zer0share-data` AI Skill：让智能体把中文自然语言数据请求转成本地查询流程 |

新文章会持续追加到这个列表。因子研究相关的文章见 [zer0factor 的开发笔记](https://github.com/zer0quant/zer0factor/blob/main/docs/devlog.md)。
