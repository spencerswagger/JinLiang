# 未决问题（需要拍板或实测后才能定）

| # | 问题 | 影响 | 倾向 |
|---|---|---|---|
| 1 | ~~MySQL 无主键表~~ | 已定：**允许**，提示 insert-only、不可块级续跑；**不因表大而拒绝**，只建议加主键。后续口径（`unique_key` 声明、TARGET-02、E1017）见 `DESIGN-NO-PRIMARY-KEY.md` |
| 2 | ~~`_jl_*` 落哪个 schema~~ | 已定：默认与目标表同 schema，可配 |
| 3 | ~~视图作为源~~ | 已定：快照 ✓、定时增量 ✓（需水位列）、**实时 CDC ✗**（视图没有自己的变更流） |
| 4 | 大事务 spill 目录默认位置与磁盘上限 | 稳定性 | 与状态目录同盘，上限可配，默认 5 GB |
| 5 | `interval` 模式的水位存哪 | 需要 `_jl_meta.watermark` 字段 | 加一列 `watermark text`（DDL 待补） |
| 6 | `newest` 冲突策略在 PG `track_commit_timestamp=off` 时的口径 | 准确性 | 只用列上的时间戳，拿不到就退化为 manual |
| 7 | 是否暴露 REST/gRPC 控制面 | 部署复杂度 | 不做，CLI + 指标端点足够 |
| 8 | ~~死信 `raw_row`~~ | 已定：默认存（便于重放），可配 `none` 只存哈希 |
| 9 | ~~独立文档站~~ | 已定：不做。提示自解释（what/why/do/risk 四段），文档只给开发者 |
| 10 | MariaDB 何时接 | binlog 同族，成本中等 | 二期，先看 MySQL 稳定性 |
