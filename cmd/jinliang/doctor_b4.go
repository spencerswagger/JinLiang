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

func (c storeControl) OpenRuns(ctx context.Context) ([]doctor.RunInfo, error) {
	runs, err := c.s.ActiveRuns(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]doctor.RunInfo, 0, len(runs))
	for _, r := range runs {
		out = append(out, doctor.RunInfo{
			ID: r.ID, JobID: r.JobID, Phase: string(r.Phase),
			StartedAt: r.StartedAt, Stub: r.Stub,
		})
	}
	return out, nil
}
