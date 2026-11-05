package control

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/spencerswagger/jinliang/internal/state"
)

// Run is one execution of a job: the summary the UI shows, not the source of
// truth for positions (those live in the target database, CONTROL-PLANE §2).
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
	// Stub marks a B0 skeleton run: no data was moved. The UI must say so
	// instead of pretending a real sync happened.
	Stub bool `json:"stub"`
}

// StartRun inserts a run row in the given phase.
func (s *Store) StartRun(ctx context.Context, jobID string, phase state.Phase) (Run, error) {
	id, err := NewID()
	if err != nil {
		return Run{}, err
	}
	r := Run{ID: id, JobID: jobID, Phase: phase, StartedAt: s.nowText()}
	if _, err := s.db.ExecContext(ctx,
		`INSERT INTO runs (id, job_id, phase, started_at, rows_done, stub) VALUES (?, ?, ?, ?, 0, 0)`,
		r.ID, r.JobID, string(r.Phase), r.StartedAt); err != nil {
		return Run{}, fmt.Errorf("写入运行记录失败: %w", err)
	}
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

// FinishRun closes a run with its terminal phase and error/exit code.
func (s *Store) FinishRun(ctx context.Context, runID string, to state.Phase, lastError string, exitCode int) (Run, error) {
	cur, err := s.GetRun(ctx, runID)
	if err != nil {
		return Run{}, err
	}
	if !state.CanTransition(cur.Phase, to) {
		return Run{}, fmt.Errorf("非法的阶段迁移 %s → %s（这是程序错误）", cur.Phase, to)
	}
	if _, err := s.db.ExecContext(ctx,
		`UPDATE runs SET phase=?, ended_at=?, last_error=?, exit_code=? WHERE id=?`,
		string(to), s.nowText(), nullIfEmpty(lastError), exitCode, runID); err != nil {
		return Run{}, err
	}
	return s.GetRun(ctx, runID)
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
	lag_bytes, lag_ms, COALESCE(last_error,''), exit_code, stub`

func scanRun(sc interface{ Scan(...any) error }) (Run, error) {
	var r Run
	var phase string
	var lagBytes, lagMs sql.NullInt64
	var exit sql.NullInt64
	var stub int
	if err := sc.Scan(&r.ID, &r.JobID, &phase, &r.StartedAt, &r.EndedAt, &r.RowsDone,
		&lagBytes, &lagMs, &r.LastError, &exit, &stub); err != nil {
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

// LatestRun returns the most recent run for a job.
func (s *Store) LatestRun(ctx context.Context, jobID string) (Run, error) {
	r, err := scanRun(s.db.QueryRowContext(ctx,
		`SELECT `+runCols+` FROM runs WHERE job_id = ? ORDER BY started_at DESC, id DESC LIMIT 1`, jobID))
	if isNoRows(err) {
		return Run{}, ErrNotFound
	}
	return r, err
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
