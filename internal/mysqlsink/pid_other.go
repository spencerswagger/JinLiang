//go:build !unix

package mysqlsink

// processAlive 在非 unix 平台上保守返回 true：拿不到 pid 状态时**绝不接管**，
// 宁可让用户等租约自然过期，也不要两个进程同时写同一张目标表。
func processAlive(pid int) bool { return pid > 0 }
