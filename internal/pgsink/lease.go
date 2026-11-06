package pgsink

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/spencerswagger/jinliang/internal/dsn"
	"github.com/spencerswagger/jinliang/internal/pgwire"
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

// leaseOwnerLabel renders the holder: 任务名#pid#token。
//
// token 是每个 Sink（= 每次 run）独有的：同一个进程里也可能有两个句柄
// （测试、或用户在同一进程里连开两次），光靠 pid 分不开它们。
func leaseOwnerLabel(job string, pid int, token string) string {
	job = strings.TrimSpace(job)
	if job == "" {
		job = "job"
	}
	if token == "" {
		return fmt.Sprintf("%s#%d", job, pid)
	}
	return fmt.Sprintf("%s#%d#%s", job, pid, token)
}

// parseLeaseLabel splits "<job>#<pid>[#<token>]". 从右往左切，任务名里带 # 也不怕。
// ok=false 表示这条记录没有 pid（老版本写的或手工改过）：当成「进程状态未知」。
func parseLeaseLabel(label string) (pid int, token string, ok bool) {
	i := strings.LastIndex(label, "#")
	if i < 0 || i == len(label)-1 {
		return 0, "", false
	}
	last := label[i+1:]
	rest := label[:i]
	if p, err := strconv.Atoi(last); err == nil && p > 0 {
		return p, "", true
	}
	token = last
	j := strings.LastIndex(rest, "#")
	if j < 0 || j == len(rest)-1 {
		return 0, "", false
	}
	p, err := strconv.Atoi(rest[j+1:])
	if err != nil || p <= 0 {
		return 0, "", false
	}
	return p, token, true
}

// leaseTakeover decides whether this process may take the lease.
//
// 纯函数：把「能不能接管」的判断从 SQL 里拿出来，好写单测，也好让评审一眼看完
// 所有分支（Owl 关心的正是「同机重启会不会自己挡自己」）。
func leaseTakeover(cur leaseRow, exists bool, now time.Time, instanceID string,
	myPID int, myToken string, pidAlive func(int) bool) (ok bool, why string) {

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
	pid, token, hasPID := parseLeaseLabel(cur.Label)
	if !hasPID {
		// 没有 pid 的老记录：只能按「本机的」处理，允许接管（否则老数据会永久挡住）。
		return true, "接管了本机上一次运行留下的租约（那条记录没有 pid）"
	}
	if pid == myPID && (token == "" || token == myToken) {
		return true, "" // 就是自己：续租
	}
	// 同一个 instance_id 不代表同一个进程：这台机器上可能有另一个 run 在跑。
	// 它活着就绝不接管（否则两个进程同时写同一张目标表）。
	if pidAlive(pid) {
		return false, fmt.Sprintf("同一台机器上还有一个进程在跑这个任务（pid %d）："+
			"等它结束，或在那边先停掉", pid)
	}
	return true, fmt.Sprintf("接管了本机崩溃进程（pid %d）留下的租约", pid)
}

// LeaseHolder is who holds (or last held) a job's lease — read-only view.
type LeaseHolder struct {
	Exists     bool
	InstanceID string
	Label      string
	Expires    time.Time
}

// ReadLease reads the lease row without taking it. 用来回答一个问题：
// 「这个任务现在到底还有没有活着的进程在跑」——崩溃重启时，控制库里的
// runs 行是死的、看不出来，目标端的租约才是可信的。
func ReadLease(ctx context.Context, ep dsn.Endpoint, schema, jobKey string) (LeaseHolder, error) {
	if schema == "" {
		schema = "public"
	}
	conn, err := pgwire.Connect(ctx, ep)
	if err != nil {
		return LeaseHolder{}, err
	}
	defer conn.Close(ctx)
	q := fmt.Sprintf(`SELECT instance_id, COALESCE(label,''), expires_at FROM %s.%s WHERE job_key=$1`,
		pgwire.Quote(schema), pgwire.Quote("_jl_lease"))
	var h LeaseHolder
	err = conn.QueryRow(ctx, q, jobKey).Scan(&h.InstanceID, &h.Label, &h.Expires)
	if errors.Is(err, pgx.ErrNoRows) {
		return LeaseHolder{}, nil
	}
	if err != nil {
		return LeaseHolder{}, err
	}
	h.Exists = true
	return h, nil
}

// Abandoned reports whether nobody is actually running under this lease:
// 过期了，或者 label 里的 pid 已经不在（kill -9 之后就是这种）。
func (h LeaseHolder) Abandoned(now time.Time, alive func(int) bool) bool {
	if !h.Exists {
		return true
	}
	if h.Expires.Before(now) {
		return true
	}
	pid, _, ok := parseLeaseLabel(h.Label)
	if !ok {
		return false // 说不清就不动
	}
	return !alive(pid)
}

// ProcessAlive exposes the platform liveness probe for callers that need to
// judge a lease holder (engine's crash recovery path).
func ProcessAlive(pid int) bool { return processAlive(pid) }

// ForceReleaseLease 删掉这个任务在目标端的租约行，**不管它现在被谁持有**。
//
// 只有一个调用场景：用户删任务。那时租约属于一个可能已经没人管的进程，
// 而它还在续期、还在写——同目标新建任务会一直撞 E1005，用户没有办法。
//
// 安全性：租约行一消失，**正在跑的那个进程下一次续租就会失败两次 → loseLease →
// cancelRun 自己停下来**（#30 的语义），所以这不是「从别的进程手里硬抢」，而是
// 「通知它别再写了」。返回被删掉的那一行的持有者描述（空表示本来就没有）。
// 返回值：(被删掉那一行的持有者 label, 该持有者的进程是否还活着, error)。
// 「还活着」与「已经是尸体」对用户是两件事：前者是**强制踢掉一个正在写的进程**，
// 后者只是清理一条过期记录。
func ForceReleaseLease(ctx context.Context, ep dsn.Endpoint, schema, jobKey string) (string, bool, error) {
	s, err := Open(ctx, Options{Target: ep, Schema: schema, JobKey: jobKey, JobName: jobKey,
		InstanceID: "gc-force-release", OnExists: "append"})
	if err != nil {
		return "", false, err
	}
	defer s.Close()
	var label string
	// 用 CTE 先读 label 再删，避免「删完就查不到持有者」。
	q := fmt.Sprintf(`WITH gone AS (DELETE FROM %s WHERE job_key=$1 RETURNING label)
		SELECT label FROM gone`, s.stateTable("_jl_lease"))
	err = s.conn.QueryRow(ctx, q, jobKey).Scan(&label)
	if err != nil {
		if strings.Contains(err.Error(), "no rows") {
			return "", false, nil // 本来就没有
		}
		return "", false, err
	}
	return label, labelHolderAlive(label), nil
}

// labelHolderAlive 从租约 label（`<job>#<pid>#<token>`）里取 pid，判断那个进程还在不在。
// 解析不出来就按「不确定」处理 → 当作还活着（宁可提示得严重一点，也不要让人以为可以立刻重建）。
func labelHolderAlive(label string) bool {
	parts := strings.Split(label, "#")
	if len(parts) < 2 {
		return true
	}
	pid, err := strconv.Atoi(parts[len(parts)-2])
	if err != nil || pid <= 0 {
		return true
	}
	return processAlive(pid)
}
