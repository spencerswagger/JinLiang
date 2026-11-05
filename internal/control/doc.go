// Package control owns the control plane's local state: the embedded SQLite
// database that holds connections, job definitions, run history, settings and
// the audit trail.
//
// 分工（docs/CONTROL-PLANE.md §2）：
//
//	控制库（本包）= 「应该做什么」      —— 连接、任务定义、运行摘要、凭证密文
//	运行时库（internal/state）= 「已经到哪了」 —— 位点、租约、块断点，永远在目标库里
//
// 红线：位点永不进控制库。本包只保存 UI 展示用的运行摘要。
package control
