// Package state manages job state: leases, positions, conflict log and dead
// letters. Positions commit in the same transaction as data, so replays are
// naturally idempotent.
package state

// Phase is the lifecycle state of one job, persisted in _ds_meta.phase.
type Phase string

// Job lifecycle phases. The order matters: only the transitions in the
// table below are legal, and every illegal transition is a bug, not a
// warning — the runtime asserts this instead of writing whatever it feels.
const (
	Idle      Phase = "idle"      // 还没开始
	Preflight Phase = "preflight" // 探测与预检
	Prepare   Phase = "prepare"   // 执行代做动作（建账号/表/书签）
	Snapshot  Phase = "snapshot"  // 全量初始化读数
	Catchup   Phase = "catchup"   // 快照已完，追增量到快照起点之后
	Streaming Phase = "streaming" // 常态增量
	Paused    Phase = "paused"    // 因错误/冲突/限流暂停，可恢复
	Draining  Phase = "draining"  // 收尾：停读、刷批、提交位点、清理
	Done      Phase = "done"      // 正常结束（保留 meta 供查历史）
	Failed    Phase = "failed"    // 不可恢复失败
)

var transitions = map[Phase][]Phase{
	Idle:      {Preflight, Failed},
	Preflight: {Prepare, Idle, Failed},       // 预检不过 → 回 idle 等用户修
	Prepare:   {Snapshot, Streaming, Failed}, // 只做增量时可以跳过快照
	Snapshot:  {Catchup, Paused, Draining, Failed},
	Catchup:   {Streaming, Paused, Draining, Failed},
	Streaming: {Paused, Draining, Failed},
	Paused:    {Streaming, Snapshot, Draining, Failed}, // Paused 恢复时可整体回退重来
	Draining:  {Done, Failed},
	Done:      {Idle}, // 允许同一任务重新跑
	Failed:    {Idle}, // 修好后重来
}

// CanTransition reports whether from -> to is legal.
func CanTransition(from, to Phase) bool {
	if from == to {
		return true // 幂等重写（例如恢复时重写当前阶段）
	}
	for _, p := range transitions[from] {
		if p == to {
			return true
		}
	}
	return false
}

// Terminal reports whether the phase ends the run.
func (p Phase) Terminal() bool { return p == Done || p == Failed }

// HoldsLease reports whether this phase must keep the target lease alive.
// A job in Done/Failed/Idle must not block another instance.
func (p Phase) HoldsLease() bool {
	switch p {
	case Preflight, Prepare, Snapshot, Catchup, Streaming, Paused, Draining:
		return true
	}
	return false
}
