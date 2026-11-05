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
	ActorUI          = "ui"
	ActorAPI         = "api"
	ActorCLI         = "cli"
	ActorStub        = "runner"
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
	ActionAdminSet   = "admin.set"
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
func (s *Store) ListAudit(ctx context.Context, limit int) ([]AuditEntry, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
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

// PhaseLabel is the human word for a phase. The UI never shows raw phase codes
// and never shows WAL/slot/LSN (docs/DESIGN.md: 用户不懂这些).
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
