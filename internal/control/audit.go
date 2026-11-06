package control

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/spencerswagger/jinliang/internal/state"
)

// Audit entries: 我们替用户做了什么（docs/SECURITY.md §5）。
// 控制面的每个写操作都记一条，这样"系统为什么动了"永远可回溯。
const (
	ActorUI   = "ui"
	ActorAPI  = "api"
	ActorCLI  = "cli"
	ActorStub = "runner"
	// ActorSystem 是进程自己的动作（迁移收尾、启动恢复），不是某个用户点的按钮。
	ActorSystem      = "system"
	ActionConnCreate = "connection.create"
	ActionConnUpdate = "connection.update"
	ActionConnDelete = "connection.delete"
	ActionConnTest   = "connection.test"
	ActionJobCreate  = "job.create"
	ActionJobUpdate  = "job.update"
	ActionJobDelete  = "job.delete"
	ActionJobImport  = "job.import"
	ActionJobExport  = "job.export"
	ActionJobRun     = "job.run"
	ActionRunStub    = "run.stub"
	// ActionRunRejected：run 被接受（HTTP 202）但引擎在「还没建出 run 行」之前就失败。
	// 这条必须落控制库——事件只活在环形缓冲里，进程一重启就查不到了，而用户回来
	// 第一个问题正是「它说已接受，为什么什么都没发生」。
	ActionRunRejected = "job.run.rejected"
	ActionAdminSet    = "admin.set"
)

// AuditEntry is one row of the audit trail.
type AuditEntry struct {
	ID     string `json:"id"`
	At     string `json:"at"`
	Actor  string `json:"actor"`
	Action string `json:"action"`
	Detail string `json:"detail_json,omitempty"`
}

// Audit appends an entry. detail is marshalled to JSON; nil means "no detail".
// Failing to audit is not fatal for the operation, but it is loud: the caller
// gets the error and the API surfaces it as a warning in the response envelope.
func (s *Store) Audit(ctx context.Context, actor, action string, detail any) error {
	id, err := NewID()
	if err != nil {
		return err
	}
	detailJSON := ""
	if detail != nil {
		b, err := json.Marshal(detail)
		if err != nil {
			return err
		}
		detailJSON = string(b)
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO audit (id, at, actor, action, detail_json) VALUES (?, ?, ?, ?, ?)`,
		id, s.nowText(), actor, action, nullIfEmpty(detailJSON))
	return err
}

// ListAudit returns the newest entries first.
// 审计窗口大小抽成常量：诊断页的表格与「被拒绝的开始」区块各自写一个 50/10，
// 哪天窗口改了，用例里那个写死的数字就会悄悄失去意义。
const (
	// AuditWindow 是诊断页「最近动作」表格的行数。
	AuditWindow = 50
	// RejectedWindow 是「被拒绝的『开始』」区块的行数。
	RejectedWindow = 10
)

func (s *Store) ListAudit(ctx context.Context, limit int) ([]AuditEntry, error) {
	if limit <= 0 {
		limit = AuditWindow
	}
	if limit > 500 {
		limit = 500
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, at, actor, action, COALESCE(detail_json,'') FROM audit ORDER BY at DESC, id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []AuditEntry
	for rows.Next() {
		var a AuditEntry
		if err := rows.Scan(&a.ID, &a.At, &a.Actor, &a.Action, &a.Detail); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// ListAuditByAction 按 action 过滤取最近的审计记录。
//
// 为什么必须有这个方法：诊断页原来取「最近 50 条 audit」再在页面里筛 rejected。
// 忙时一小时的运行/登录就能把窗口填满，拒绝记录会掉出去——用户点开诊断页看到
// 「没有被拒绝的记录」，而实际刚刚发生过。那和「没有痕迹」是同一个后果，
// 所以过滤必须下推到 SQL。
func (s *Store) ListAuditByAction(ctx context.Context, action string, limit int) ([]AuditEntry, error) {
	// 钳制要「按方向」做：非法下限回落默认，超上限截到上限。
	// 早先写成 `<=0 || >100 → 10`，于是 limit=500 会静默拿到 10 条而不是 100 条。
	if limit <= 0 {
		limit = RejectedWindow
	}
	if limit > 100 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, at, actor, action, COALESCE(detail_json,'') FROM audit
		 WHERE action = ? ORDER BY at DESC, id DESC LIMIT ?`, action, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []AuditEntry
	for rows.Next() {
		var a AuditEntry
		if err := rows.Scan(&a.ID, &a.At, &a.Actor, &a.Action, &a.Detail); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// PhaseLabel is the human word for a phase. The UI never shows raw phase codes
// and never shows WAL/slot/LSN (docs/DESIGN.md: 用户不懂这些).
// RunStateLabel is what the UI shows for one run. finish_reason wins over phase:
// a run that ended with phase=done but reason=stopped is "已停止", not "已完成".
func RunStateLabel(r Run) string {
	switch r.FinishReason {
	case FinishStopped:
		return "已停止"
	case FinishInterrupted:
		return "已中断"
	case FinishFailed:
		return "失败"
	case FinishCompleted:
		return "已完成"
	}
	if r.EndedAt == "" {
		return PhaseLabel(r.Phase)
	}
	return PhaseLabel(r.Phase)
}

// RunBadgeClass 是同一套口径的「颜色」：文案说「已停止」，颜色就不能是绿色的 ok。
//
// 规则：只有 completed 才是 ok；stopped 是中性偏警示（warn）；interrupted/failed 是 bad。
// 没结束的运行按 phase 粗分（paused 用 warn，failed 用 bad，其余不染色）。
func RunBadgeClass(r Run) string {
	switch r.FinishReason {
	case FinishCompleted:
		return "ok"
	case FinishStopped:
		return "warn"
	case FinishInterrupted, FinishFailed:
		return "bad"
	}
	switch r.Phase {
	case state.Failed:
		return "bad"
	case state.Paused:
		return "warn"
	}
	return ""
}

func PhaseLabel(p state.Phase) string {
	switch p {
	case state.Idle:
		return "未开始"
	case state.Preflight:
		return "预检中"
	case state.Prepare:
		return "准备对象"
	case state.Snapshot:
		return "全量初始化"
	case state.Catchup:
		return "追增量"
	case state.Streaming:
		return "实时同步中"
	case state.Paused:
		return "已暂停"
	case state.Draining:
		return "收尾提交"
	case state.Done:
		return "已完成"
	case state.Failed:
		return "失败"
	}
	return string(p)
}

// PhaseHint is one short sentence explaining what the phase means in practice,
// so the detail page can answer "现在到底在干嘛" without a glossary.
func PhaseHint(p state.Phase) string {
	switch p {
	case state.Preflight:
		return "正在只读地检查两端能力、表结构与冲突，不会写任何数据"
	case state.Prepare:
		return "正在目标端准备表与同步元数据"
	case state.Snapshot:
		return "正在把历史数据整块搬过去，可以随时暂停"
	case state.Catchup:
		return "历史数据搬完了，正在把期间发生的变更补上"
	case state.Streaming:
		return "已经追上，之后每次变更都会在秒级内同步"
	case state.Paused:
		return "暂停中，位点已经提交，恢复会从断点继续"
	case state.Draining:
		return "正在提交最后一批并释放占用"
	case state.Done:
		return "这一轮结束了，元数据保留着，可以随时再来一轮"
	case state.Failed:
		return "停下了，错误卡片里有下一步"
	}
	return ""
}

// knownPhases is the closed set from internal/state. Used to reject unknown
// phase strings coming in from the outside (yaml, API filters).
var knownPhases = []state.Phase{
	state.Idle, state.Preflight, state.Prepare, state.Snapshot, state.Catchup,
	state.Streaming, state.Paused, state.Draining, state.Done, state.Failed,
}

// ValidatePhase ensures an externally supplied phase string is one we know.
func ValidatePhase(s string) error {
	for _, p := range knownPhases {
		if string(p) == s {
			return nil
		}
	}
	return fmt.Errorf("未知阶段 %q", s)
}
