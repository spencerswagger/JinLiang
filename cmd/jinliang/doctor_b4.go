package main

import (
	"context"

	"github.com/spencerswagger/jinliang/internal/control"
	"github.com/spencerswagger/jinliang/internal/doctor"
)

// storeControl adapts the control store to doctor.ControlProbe. Keeping the
// adapter here means internal/doctor does not depend on internal/control
// (another track owns it), and a change there breaks at one small seam.
type storeControl struct {
	s *control.Store
}

func (c storeControl) InstanceID(ctx context.Context) (string, error) {
	return c.s.InstanceID(ctx)
}

func (c storeControl) IntegrityCheck(ctx context.Context) (string, error) {
	return c.s.IntegrityCheck(ctx)
}

func (c storeControl) SchemaVersion(ctx context.Context) (int, error) {
	return c.s.SchemaVersion(ctx)
}

func (c storeControl) AdminConfigured(ctx context.Context) (bool, error) {
	return c.s.AdminConfigured(ctx)
}

// OpenRuns reads open runs plus their last heartbeat.
//
// runs-liveness（PR #5）给控制库加了 heartbeat_at；本分支的基线还没有这一列。
// 直接查列而不是依赖 control.Run 的字段，可以同时支持两种库：有列就用心跳，
// 没列就把 HeartbeatKnown=false 交给 doctor —— 后者只提醒、不判红，避免
// 「正常长跑被当成幽灵」。
func (c storeControl) OpenRuns(ctx context.Context) ([]doctor.RunInfo, error) {
	runs, err := c.s.ActiveRuns(ctx)
	if err != nil {
		return nil, err
	}
	heartbeats, hasHeartbeat := c.heartbeats(ctx)
	out := make([]doctor.RunInfo, 0, len(runs))
	for _, r := range runs {
		info := doctor.RunInfo{
			ID: r.ID, JobID: r.JobID, Phase: string(r.Phase),
			StartedAt: r.StartedAt, Stub: r.Stub,
			HeartbeatKnown: hasHeartbeat,
		}
		if hasHeartbeat {
			info.HeartbeatAt = heartbeats[r.ID]
		}
		out = append(out, info)
	}
	return out, nil
}

// heartbeats returns id→heartbeat_at when the column exists. The column probe
// is cheap and keeps this adapter compatible across the #5 merge boundary.
func (c storeControl) heartbeats(ctx context.Context) (map[string]string, bool) {
	db := c.s.DB()
	if db == nil {
		return nil, false
	}
	var n int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pragma_table_info('runs') WHERE name = 'heartbeat_at'`).Scan(&n); err != nil || n == 0 {
		return nil, false
	}
	rows, err := db.QueryContext(ctx,
		`SELECT id, COALESCE(heartbeat_at, '') FROM runs WHERE ended_at IS NULL`)
	if err != nil {
		return nil, false
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var id, hb string
		if err := rows.Scan(&id, &hb); err != nil {
			return nil, false
		}
		out[id] = hb
	}
	return out, true
}
