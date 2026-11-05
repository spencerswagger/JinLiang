//go:build !unix

package pgsink

// processAlive 在没有 unix 进程信号语义的平台上无法判断，一律返回 false：
// 宁可接管（崩溃恢复优先），也不要把用户永久挡在门外。跨机器的排他仍由
// instance_id 保证。
func processAlive(pid int) bool { return false }
