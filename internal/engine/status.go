package engine

import (
	"context"
	"time"

	"github.com/spencerswagger/jinliang/internal/control"
	"github.com/spencerswagger/jinliang/internal/pgsink"
)

// Status is one job's "where is it now", assembled from three places:
// the control database (what should happen), the target `_jl_meta` (what did
// happen, authoritative), and the source slot (how far behind we are).
type Status struct {
	JobID     string       `json:"job_id"`
	Name      string       `json:"name"`
	JobKey    string       `json:"job_key"`
	Source    string       `json:"source"` // 已脱敏：不带口令
	Target    string       `json:"target"`
	Mode      string       `json:"mode"`
	Tables    []string     `json:"tables"`
	Phase     string       `json:"phase"`
	Position  string       `json:"position,omitempty"`
	UpdatedAt string       `json:"position_at,omitempty"`
	LagBytes  int64        `json:"lag_bytes"`
	SlotBytes int64        `json:"slot_wal_bytes"`
	LastRun   *control.Run `json:"last_run,omitempty"`
	Note      string       `json:"note,omitempty"`
}

// StatusFor resolves a job (id or name) and reports its current state.
func StatusFor(ctx context.Context, opt Options) (Status, error) {
	job, err := opt.Store.ResolveJob(ctx, opt.JobID)
	if err != nil {
		return Status{}, err
	}
	doc, err := opt.Store.JobDoc(ctx, job)
	if err != nil {
		return Status{}, err
	}
	srcConn, err := opt.Store.GetConnection(ctx, job.SourceID)
	if err != nil {
		return Status{}, err
	}
	tgtConn, err := opt.Store.GetConnection(ctx, job.TargetID)
	if err != nil {
		return Status{}, err
	}
	ss, err := opt.Store.Secret(ctx, job.SourceID)
	if err != nil {
		return Status{}, err
	}
	ts, err := opt.Store.Secret(ctx, job.TargetID)
	if err != nil {
		return Status{}, err
	}
	st := Status{
		JobID:  job.ID,
		Name:   job.Name,
		JobKey: job.JobKey,
		Mode:   doc.Mode.Frequency,
		Source: endpointOf(srcConn, ss).Redacted(),
		Target: endpointOf(tgtConn, ts).Redacted(),
	}
	if run, err := opt.Store.LatestRun(ctx, job.ID); err == nil {
		st.LastRun = &run
	}
	schema := doc.Target.Schema
	if schema == "" {
		schema = tgtConn.Database
	}
	meta, err := pgsink.ReadState(ctx, endpointOf(tgtConn, ts), schema, job.JobKey)
	if err != nil {
		st.Note = "目标端还没有运行状态（还没跑过）"
	} else {
		st.Phase = meta.Phase
		st.Position = meta.Position
		st.UpdatedAt = meta.UpdatedAt.UTC().Format(time.RFC3339)
	}
	r := &runner{opt: opt, job: job, doc: doc,
		srcEP: endpointOf(srcConn, ss), tgtEP: endpointOf(tgtConn, ts),
		instID: instanceID(ctx, opt.Store)}
	if err := r.loadCatalog(ctx); err != nil {
		st.Note = "读源端表清单失败：" + err.Error()
		return st, nil
	}
	st.Tables = tableNames(r.tables)
	src, err := r.openSource(ctx)
	if err != nil {
		st.Note = "连不上源库：" + err.Error()
		return st, nil
	}
	defer src.Close()
	if st.Position != "" {
		parsed, err := src.ParsePosition(st.Position)
		switch {
		case err != nil:
			// 不静默：位点解析不了说明目标库里的进度标记被改过或版本不认，
			// 这种情况必须让人看见（以前这里直接忽略，看起来像「没有位点」）。
			st.Note = "目标库里的进度标记解析失败：" + err.Error()
		default:
			if !src.IsZero(parsed) {
				// 落后量由源端自己算（PG 是 LSN 差，MySQL 是 binlog 文件大小差）；
				// 拿不到就不给数（0 会被当成「已经追平」）。
				if n, err := src.Backlog(ctx, parsed); err == nil && n >= 0 {
					st.LagBytes = n
				}
			}
		}
	}
	// 源端侧积压（PG：槽位 WAL 差；MySQL：位点之后还剩多少 binlog）。
	if n, err := src.Backlog(ctx, nil); err == nil && n >= 0 {
		st.SlotBytes = n
	}
	return st, nil
}
