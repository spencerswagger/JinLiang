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
// 这里只放“读 MySQL 变更流”的实现；位点落库、批提交、冲突处理在
// internal/core（别人的地盘），共享类型在 internal/source（Ark 的）。
package mysqlsource
