package control

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/spencerswagger/jinliang/internal/state"
)

// Run is one execution of a job: the summary the UI shows, not the source of
// truth for positions (those live in the target database, CONTROL-PLANE §2).
// Run 的种类：这一轮是什么运行。布尔 stub 的反面是「默认真跑」，所以用枚举。
const (
	RunKindSync   = "sync"   // 正常同步（列表页的「最近一次运行」只认这个）
	RunKindVerify = "verify" // 校验运行（B1 的 verify，不是同步，别混进历史主视图）
	RunKindStub   = "stub"   // B0 骨架运行：只推进阶段、不搬数据；B1 起不再产生
)

// ValidRunKind reports whether k is one of the three known kinds.
func ValidRunKind(k string) bool {
	return k == RunKindSync || k == RunKindVerify || k == RunKindStub
}

type Run struct {
	ID        string      `json:"id"`
	JobID     string      `json:"job_id"`
	Phase     state.Phase `json:"phase"`
	StartedAt string      `json:"started_at"`
	EndedAt   string      `json:"ended_at,omitempty"`
	RowsDone  int64       `json:"rows_done"`
	LagBytes  *int64      `json:"lag_bytes,omitempty"`
	LagMs     *int64      `json:"lag_ms,omitempty"`
	LastError string      `json:"last_error,omitempty"`
	ExitCode  *int        `json:"exit_code,omitempty"`
	// Kind 是这一轮的种类（sync | verify | stub）。B0 之后新代码必须显式传，
	// 不靠列默认值。
	Kind string `json:"kind"`
	// Stub 是 B0 的旧列，保留一版做兼容；新代码以 Kind 为准，两者应一致。
	Stub bool `json:"stub"`
	// Code 是这一轮结束时的错误码（E1xxx/E3xxx…），空表示没有错误。
	// 有了它，列表页只显示「徽标 + 一句摘要」，全文留给详情页——
	// 这比把 250–300 字的 last_error 截断更根治（#41 只是止血）。
	Code string `json:"code,omitempty"`
	// HeartbeatAt is the last liveness ping from the process that owns this run.
	// A crashed process leaves ended_at NULL forever unless someone notices —
	// that is exactly what this column and RecoverStaleRuns are for.
	HeartbeatAt string `json:"heartbeat_at,omitempty"`
	// OwnerInstance is the control-plane instance that started the run.
	OwnerInstance string `json:"owner_instance,omitempty"`
	// PausedTables 是这一轮结束时仍暂停的表（JSON 数组；空表示没有）。
	// 退出码 3 只是粗信号，这一列才是「哪几张表、欠多少行」的权威。
	PausedTables string `json:"paused_tables,omitempty"`
	// FinishReason explains how it ended: completed | failed | stopped | interrupted.
	FinishReason string `json:"finish_reason,omitempty"`
}

// DefaultHeartbeatInterval is how often a running job refreshes heartbeat_at.
// 进程被杀时没有心跳，`serve` 启动时按 StaleAfter(3×) 判死并收尾。
const DefaultHeartbeatInterval = 30 * time.Second

// Finish reasons. "stopped" must never look like a successful completion.
const (
	FinishCompleted   = "completed"
	FinishFailed      = "failed"
	FinishStopped     = "stopped"
	FinishInterrupted = "interrupted"
	// FinishPausedTables 表示「跑完了，但有表因结构不一致被暂停」（退出码 3）。
	// 它与 completed 的区别只在「数据全不全」，不是失败——phase 仍是 done。
	FinishPausedTables = "paused_tables"
)

// StartRun inserts a run row in the given phase and kind.
//
// kind 必须显式传（sync / verify / stub）：列的 DEFAULT 'sync' 只服务历史行兼容，
// 不是给新代码偷懒用的——漏写会让一次 verify 被当成真同步展示。
func (s *Store) StartRun(ctx context.Context, jobID string, phase state.Phase, kind string) (Run, error) {
	if !ValidRunKind(kind) {
		return Run{}, fmt.Errorf("未知的运行种类 %q（只接受 sync / verify / stub）", kind)
	}
	id, err := NewID()
	if err != nil {
		return Run{}, err
	}
	now := s.nowText()
	legacyStub := 0
	if kind == RunKindStub {
		legacyStub = 1 // 兼容旧列：老读者仍看 stub=1
	}
	r := Run{ID: id, JobID: jobID, Phase: phase, StartedAt: now, HeartbeatAt: now,
		OwnerInstance: s.instanceID, Kind: kind}
	if _, err := s.db.ExecContext(ctx,
		`INSERT INTO runs (id, job_id, phase, started_at, heartbeat_at, owner_instance, rows_done, stub, kind)
		 VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?)`,
		r.ID, r.JobID, string(r.Phase), r.StartedAt, r.HeartbeatAt, r.OwnerInstance, legacyStub, r.Kind); err != nil {
		// 唯一索引 idx_runs_one_active 把「一个任务两个活跃 run」关死在库层面。
		if strings.Contains(err.Error(), "UNIQUE") {
			return Run{}, fmt.Errorf("%w: 这个任务已经有一条未结束的运行（多半是上次进程被杀的残留）。跑一次 `jinliang serve` 会替它收尾（心跳过期后标成 interrupted）", ErrConflict)
		}
		return Run{}, fmt.Errorf("写入运行记录失败: %w", err)
	}
	r.Stub = legacyStub == 1
	return r, nil
}

// SetRunStub flags a run as a skeleton run.
func (s *Store) SetRunStub(ctx context.Context, runID string, stub bool) error {
	v := 0
	if stub {
		v = 1
	}
	_, err := s.db.ExecContext(ctx, `UPDATE runs SET stub = ? WHERE id = ?`, v, runID)
	return err
}

// UpdateRunPhase writes a phase transition plus the metrics that came with it.
// Illegal transitions are refused here rather than written "because we can":
// an impossible phase change is a bug, not a warning (internal/state/phase.go).
func (s *Store) UpdateRunPhase(ctx context.Context, runID string, to state.Phase) (Run, error) {
	cur, err := s.GetRun(ctx, runID)
	if err != nil {
		return Run{}, err
	}
	if !state.CanTransition(cur.Phase, to) {
		return Run{}, fmt.Errorf("非法的阶段迁移 %s → %s（这是程序错误）", cur.Phase, to)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE runs SET phase = ? WHERE id = ?`, string(to), runID); err != nil {
		return Run{}, err
	}
	return s.GetRun(ctx, runID)
}

// TouchRun refreshes the liveness heartbeat for a running run.
// 只更新未结束的行：收尾与心跳是并发的，别让一个迟到的 tick 把 ended_at 的行改活。
func (s *Store) TouchRun(ctx context.Context, runID string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE runs SET heartbeat_at = ? WHERE id = ? AND ended_at IS NULL`, s.nowText(), runID)
	return err
}

// RecoverStaleRuns closes runs whose owning process stopped heart-beating
// (killed -9,机器断电). Called by `serve` at startup only: CLI commands open the
// same database and must not "recover" a run that a live server is driving.
//
// 判据用「最后心跳早于 staleAfter」而不是「owner 不是我」——同一台机器上重启
// 会拿到新的 boot epoch，但 run 的 owner_instance 是部署身份，重启后不变。
func (s *Store) RecoverStaleRuns(ctx context.Context, staleAfter time.Duration) (int, error) {
	if staleAfter <= 0 {
		staleAfter = StaleAfter(s.heartbeat)
	}
	cutoff := s.now().Add(-staleAfter).UTC().Format(time.RFC3339)
	// phase 也要落到可表达的终态：否则历史页会永久显示「增量中」「已暂停」。
	// interrupted 记成 failed（它确实没跑完），靠 finish_reason 区分「失败」和「被中断」。
	res, err := s.db.ExecContext(ctx,
		`UPDATE runs
		    SET ended_at = ?, finish_reason = ?,
		        phase = CASE WHEN phase IN ('done','failed') THEN phase ELSE 'failed' END,
		        last_error = COALESCE(last_error, '进程在运行中被终止，心跳已过期；这一轮没有正常收尾')
		  WHERE ended_at IS NULL
		    AND COALESCE(heartbeat_at, started_at) < ?`,
		s.nowText(), FinishInterrupted, cutoff)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// FinishRun closes a run with its terminal phase, error/exit code and why.
func (s *Store) FinishRun(ctx context.Context, runID string, to state.Phase, lastError string, exitCode int, finishReason string) (Run, error) {
	cur, err := s.GetRun(ctx, runID)
	if err != nil {
		return Run{}, err
	}
	// 手动停止是唯一允许「不走阶段表」的收尾：用户可以在 preflight（还没动过数据）
	// 就按停止，而阶段表里 preflight 只能去 prepare/idle/failed。把它记成 failed
	// 会在历史页显示「失败」，那是撒谎。停止的语义由 finish_reason=stopped 表达，
	// 阶段统一落 done。
	if !state.CanTransition(cur.Phase, to) && finishReason != FinishStopped {
		return Run{}, fmt.Errorf("非法的阶段迁移 %s → %s（这是程序错误）", cur.Phase, to)
	}
	if _, err := s.db.ExecContext(ctx,
		`UPDATE runs SET phase=?, ended_at=?, last_error=?, exit_code=?, finish_reason=?, heartbeat_at=? WHERE id=?`,
		string(to), s.nowText(), nullIfEmpty(lastError), exitCode, nullIfEmpty(finishReason), s.nowText(), runID); err != nil {
		return Run{}, err
	}
	return s.GetRun(ctx, runID)
}

// FinishRunWithCode 与 FinishRun 相同，但把错误码一起写进 runs.code。
//
// 单独的入口而不是改 FinishRun 的签名：FinishRun 已被引擎多处调用，加参数会逼着
// 每个调用点都改；引擎按需切到这个入口即可。errCode 为空时与 FinishRun 等价。
func (s *Store) FinishRunWithCode(ctx context.Context, runID string, to state.Phase, lastError string, exitCode int, finishReason, errCode string) (Run, error) {
	run, err := s.FinishRun(ctx, runID, to, lastError, exitCode, finishReason)
	if err != nil {
		return run, err
	}
	if errCode != "" {
		if _, err := s.db.ExecContext(ctx, `UPDATE runs SET code = ? WHERE id = ?`, errCode, runID); err != nil {
			return run, err
		}
		return s.GetRun(ctx, runID)
	}
	return run, nil
}

// FinishRunWithPauses 收尾并落「本轮仍暂停的表」（JSON 数组；nil 表示没有）。
//
// 照 `FinishRunWithCode` 的形状：先正常收尾，再补一列。分两步是因为 paused_tables
// 只是一份**展示/审计**数据，不该让收尾本身因为它失败而失败。
func (s *Store) FinishRunWithPauses(ctx context.Context, runID string, to state.Phase,
	lastError string, exitCode int, finishReason string, pauses any) (Run, error) {
	run, err := s.FinishRun(ctx, runID, to, lastError, exitCode, finishReason)
	if err != nil {
		return run, err
	}
	if pauses == nil {
		return run, nil
	}
	blob, jerr := json.Marshal(pauses)
	if jerr != nil {
		return run, jerr
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE runs SET paused_tables = ? WHERE id = ?`,
		string(blob), runID); err != nil {
		return run, err
	}
	return s.GetRun(ctx, runID)
}

// SetRunCode 只补错误码（用于「先记错误、后确定分类」的场景）。
func (s *Store) SetRunCode(ctx context.Context, runID, code string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE runs SET code = ? WHERE id = ?`, nullIfEmpty(code), runID)
	return err
}

// SetRunProgress records rows moved and lag, for the number wall.
func (s *Store) SetRunProgress(ctx context.Context, runID string, rows int64, lagBytes, lagMs *int64) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE runs SET rows_done = ?, lag_bytes = ?, lag_ms = ? WHERE id = ?`,
		rows, lagBytes, lagMs, runID)
	return err
}

// SetRunError records a one-line error on a still-running run.
func (s *Store) SetRunError(ctx context.Context, runID, msg string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE runs SET last_error = ? WHERE id = ?`, nullIfEmpty(msg), runID)
	return err
}

const runCols = `id, job_id, phase, started_at, COALESCE(ended_at,''), rows_done,
	lag_bytes, lag_ms, COALESCE(last_error,''), exit_code, stub,
	COALESCE(heartbeat_at,''), COALESCE(owner_instance,''), COALESCE(finish_reason,''), kind,
	COALESCE(code,'')`

func scanRun(sc interface{ Scan(...any) error }) (Run, error) {
	var r Run
	var phase string
	var lagBytes, lagMs sql.NullInt64
	var exit sql.NullInt64
	var stub int
	if err := sc.Scan(&r.ID, &r.JobID, &phase, &r.StartedAt, &r.EndedAt, &r.RowsDone,
		&lagBytes, &lagMs, &r.LastError, &exit, &stub,
		&r.HeartbeatAt, &r.OwnerInstance, &r.FinishReason, &r.Kind, &r.Code); err != nil {
		return Run{}, err
	}
	r.Phase = state.Phase(phase)
	if lagBytes.Valid {
		v := lagBytes.Int64
		r.LagBytes = &v
	}
	if lagMs.Valid {
		v := lagMs.Int64
		r.LagMs = &v
	}
	if exit.Valid {
		v := int(exit.Int64)
		r.ExitCode = &v
	}
	r.Stub = stub != 0
	return r, nil
}

// GetRun loads one run.
func (s *Store) GetRun(ctx context.Context, id string) (Run, error) {
	r, err := scanRun(s.db.QueryRowContext(ctx, `SELECT `+runCols+` FROM runs WHERE id = ?`, id))
	if isNoRows(err) {
		return Run{}, ErrNotFound
	}
	return r, err
}

// LatestRun returns the most recent run of any kind.
func (s *Store) LatestRun(ctx context.Context, jobID string) (Run, error) {
	r, err := scanRun(s.db.QueryRowContext(ctx,
		`SELECT `+runCols+` FROM runs WHERE job_id = ? ORDER BY started_at DESC, id DESC LIMIT 1`, jobID))
	if isNoRows(err) {
		return Run{}, ErrNotFound
	}
	return r, err
}

// LatestRunOfKind returns the most recent run of one kind.
//
// 「最近一次运行」在任务列表/详情页必须用 kind='sync' 取：verify 运行会被
// 写成一行 run，但它不是同步，混进主视图会让人以为同步跑过。
func (s *Store) LatestRunOfKind(ctx context.Context, jobID, kind string) (Run, error) {
	r, err := scanRun(s.db.QueryRowContext(ctx,
		`SELECT `+runCols+` FROM runs WHERE job_id = ? AND kind = ?
		 ORDER BY started_at DESC, id DESC LIMIT 1`, jobID, kind))
	if isNoRows(err) {
		return Run{}, ErrNotFound
	}
	return r, err
}

// LatestSyncRun 是「最近一次同步运行」，列表页与任务摘要都用它。
func (s *Store) LatestSyncRun(ctx context.Context, jobID string) (Run, error) {
	return s.LatestRunOfKind(ctx, jobID, RunKindSync)
}

// ListRuns returns recent runs, newest first. jobID "" means all jobs.
func (s *Store) ListRuns(ctx context.Context, jobID string, limit int) ([]Run, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	var (
		rows *sql.Rows
		err  error
	)
	if jobID == "" {
		rows, err = s.db.QueryContext(ctx,
			`SELECT `+runCols+` FROM runs ORDER BY started_at DESC, id DESC LIMIT ?`, limit)
	} else {
		rows, err = s.db.QueryContext(ctx,
			`SELECT `+runCols+` FROM runs WHERE job_id = ? ORDER BY started_at DESC, id DESC LIMIT ?`, jobID, limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Run
	for rows.Next() {
		r, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ActiveRuns returns runs that have not reached a terminal phase.
func (s *Store) ActiveRuns(ctx context.Context) ([]Run, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+runCols+` FROM runs WHERE ended_at IS NULL ORDER BY started_at DESC LIMIT 50`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Run
	for rows.Next() {
		r, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		if !r.Phase.Terminal() {
			out = append(out, r)
		}
	}
	return out, rows.Err()
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}
