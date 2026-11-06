// Package mysqlsink 是 MySQL 作为「目标端」的写入侧：建表/加列、批量 upsert、
// 位点与数据同事务、`on_exists` 三策略、DDL 白名单，以及 utf8mb4 行宽预检。
//
// 与 internal/mysqlsource 对称：那个读源端变更流，这个写目标端行。
//
// 四条设计口径（照 docs/ 定，不在这里另立）：
//   - upsert 用 `INSERT … ON DUPLICATE KEY UPDATE`，**没有唯一键就明确报错**——
//     退化成纯 INSERT 会在重放时造重复行，那是静默的数据损坏（docs/CONFLICT.md）。
//   - 位点写在 `_jl_meta.position`。无 DDL 的批次是「数据 + 位点同一事务」；含 DDL/TRUNCATE
//     的批次拆三段（数据+旧位点 → DDL → 新位点），因为 MySQL 的 DDL 隐式提交（docs/STATE-SCHEMA.md §1）。
//
// 两条运维须知（写在这里，因为它们会造成「看起来正常」的假象）：
//   - **一次 prepare 失败也会在目标库留下 `_jl_*` 状态表**（租约/元数据/对象/块断点四张）：
//     建表在 Open 里幂等执行，失败发生在建表之前才不留痕；这些表是 `_jl_` 前缀，gc 只清它们，不碰用户表。
//   - **`instance_id` 必须按实例唯一，不要直接拷 data-dir 起第二个实例**：同 id 的两个容器
//     会让租约判定进入「同机」分支，而 pid 在容器间不可比，于是出现双写窗口。
//   - `on_exists` 三策略：`error`（默认，撞表就停）/ `recreate`（先 DROP）/ `append`（建表用
//     已存在的表，**源端删过的行会在目标端残留**——这是语义选择，写进文档，不是缺陷）。
//   - DDL 只有白名单四类跟随：加列、放宽长度、加索引、可选删列（删列要确认）；
//     其余一律停并报错，绝不静默跳过或照搬（docs/SPEC.md `content.schema_follow`）。
package mysqlsink
