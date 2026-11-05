// Package mysqlsource reads MySQL change streams: it parses binlog events
// (table map / rows / XID transaction boundaries), keeps two kinds of
// checkpoint (file+pos and GTID), probes which server capabilities are really
// there, and decompresses 8.0.20+ compressed transactions.
//
// 三个必须守住的点（docs/STREAMING.md §1、§3、docs/SNAPSHOT.md §3）：
//   - 事务完整性：一个源事务要么整段交给下游，要么一条都不给；
//   - 位点双套：file+pos 与 GTID 都写、都读、都能恢复；取位点记到毫秒；
//   - 缺能力不静默：探测不到就给 source.Compensation，说清降级代价。
//
// 明确的行为边界（不留未定义）：
//   - JSON 列：原样透传 MySQL 的二进制 JSON（source.Value.Kind == KindBytes），
//     既不假装解成文本、也不报错丢数据。文本映射归类型字典（internal/catalog，Ark 的）。
//   - binlog_row_value_options=PARTIAL_JSON、旧式 DECIMAL、未知列类型：明确报错并给动作，
//     不静默兜底。
//   - 无主键表全量：退化成单连接顺序扫（整表扫完），只有表级断点；代价写进 SnapshotNote。
//
// 这里只放“读 MySQL 变更流”的实现；位点落库、批提交、冲突处理在
// internal/core（别人的地盘），共享类型在 internal/source（Ark 的）。
package mysqlsource
