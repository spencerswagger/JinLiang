//go:build unix

package mysqlsink

import (
	"errors"
	"syscall"
)

// processAlive 判断本机某 pid 还在不在（同机崩溃接管的判据）。
func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	if err == nil {
		return true
	}
	// EPERM：进程在，只是不归我们管——仍算活着，绝不接管。
	return errors.Is(err, syscall.EPERM)
}
