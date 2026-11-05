package pgsink

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// 租约的语义（docs/STATE-SCHEMA.md §3）：同一个 job_key 同时只允许一个实例写。
//
// 光用「instance_id 相同就允许接管」是有代价的：同一台机器上跑两个进程会拿到
// 同一个 instance_id（它标识的是这台机器，不是进程），第二个进程会把第一个
// 踢掉，于是两份写入并行跑。
//
// 所以租约的 label 里带上 pid：同一台机器上接管前先看那个 pid 还活着没有——
//   - 进程已死（kill -9 之后的重启）→ 立刻接管，不用等 30 秒过期；
//   - 进程还活着（用户又点了一次 run）→ 拒绝，告诉他是哪个 pid 在跑。
type leaseRow struct {
	InstanceID string
	Label      string // "<job>#<pid>"，老数据可能没有 pid
	Expires    time.Time
}

// leaseOwnerLabel renders the human-readable holder: 任务名#pid。
func leaseOwnerLabel(job string, pid int) string {
	job = strings.TrimSpace(job)
	if job == "" {
		job = "job"
	}
	return fmt.Sprintf("%s#%d", job, pid)
}

// parseLeasePID pulls the pid out of a label. ok=false 表示这条记录没有 pid
// （老版本写的，或者被人手工改过）：当成「进程状态未知」处理。
func parseLeasePID(label string) (int, bool) {
	i := strings.LastIndex(label, "#")
	if i < 0 || i == len(label)-1 {
		return 0, false
	}
	pid, err := strconv.Atoi(label[i+1:])
	if err != nil || pid <= 0 {
		return 0, false
	}
	return pid, true
}

// leaseTakeover decides whether this process may take the lease.
//
// 纯函数：把「能不能接管」的判断从 SQL 里拿出来，好写单测，也好让评审一眼看完
// 所有分支（Owl 关心的正是「同机重启会不会自己挡自己」）。
func leaseTakeover(cur leaseRow, exists bool, now time.Time, instanceID string, myPID int,
	pidAlive func(int) bool) (ok bool, why string) {

	if !exists {
		return true, ""
	}
	if cur.Expires.Before(now) {
		return true, "上一个租约已经过期"
	}
	if cur.InstanceID != instanceID {
		return false, fmt.Sprintf("另一个实例 %s 正在跑（租约到 %s）",
			cur.InstanceID, cur.Expires.UTC().Format(time.RFC3339))
	}
	pid, hasPID := parseLeasePID(cur.Label)
	if !hasPID {
		// 没有 pid 的老记录：只能按「本机的」处理，允许接管（否则老数据会永久挡住）。
		return true, "接管了本机上一次运行留下的租约（那条记录没有 pid）"
	}
	if pid == myPID {
		return true, "" // 自己续租
	}
	if pidAlive(pid) {
		return false, fmt.Sprintf("同一台机器上还有一个进程在跑这个任务（pid %d）", pid)
	}
	return true, fmt.Sprintf("接管了本机崩溃进程（pid %d）留下的租约", pid)
}
