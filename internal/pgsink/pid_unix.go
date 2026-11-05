//go:build unix

package pgsink

import "syscall"

// processAlive reports whether pid is a live process we can see.
// EPERM 说明进程存在、只是不归我们管，也算活着。
func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	return err == nil || err == syscall.EPERM
}
