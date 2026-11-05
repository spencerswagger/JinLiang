package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/spencerswagger/jinliang/internal/connect"
	"github.com/spencerswagger/jinliang/internal/control"
	"github.com/spencerswagger/jinliang/internal/pgsink"
	"github.com/spencerswagger/jinliang/internal/pgsource"
	"github.com/spencerswagger/jinliang/internal/source"
)

// verify compares every selected table pair by row count + server-side
// fingerprint. It never pulls rows across the wire (docs/SPEC.md §6).
func (r *runner) verify(ctx context.Context) ([]string, error) {
	if r.sink == nil || r.src == nil {
		return nil, fmt.Errorf("还没准备好两端，不能校验")
	}
	r.sink.CountSource = r.src.Count
	r.sink.FingerprintSource = r.src.Fingerprint
	var diffs []string
	for _, t := range r.tables {
		n, err := r.sink.Verify(ctx, t)
		if err != nil {
			return diffs, fmt.Errorf("校验 %s 失败: %w", t.Table, err)
		}
		switch {
		case n == 0:
			r.emit(ctx, control.Event{Type: "note", Message: "校验 " + t.Table + "：一致"})
		case n < 0:
			msg := "校验 " + t.Table + "：行数一致但内容不同"
			diffs = append(diffs, msg)
			r.emit(ctx, control.Event{Type: "error", Message: msg})
		default:
			msg := fmt.Sprintf("校验 %s：差 %d 行", t.Table, n)
			diffs = append(diffs, msg)
			r.emit(ctx, control.Event{Type: "error", Message: msg})
		}
	}
	return diffs, nil
}

// loadCatalog reads the source catalog and resolves the selected tables without
// writing anything (verify path).
func (r *runner) loadCatalog(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	c, err := connect.Open(r.srcEP)
	if err != nil {
		return err
	}
	defer c.Close()
	cat, err := c.Catalog(ctx, scopeOf(r.doc))
	if err != nil {
		return err
	}
	r.cat = cat
	tables, err := r.selectTables()
	if err != nil {
		return err
	}
	r.tables = tables
	return nil
}

// openSink / openSource build the two halves for a run or a dry verify.
func (r *runner) openSink(ctx context.Context) (*pgsink.Sink, error) {
	return pgsink.Open(ctx, pgsink.Options{
		Target:     r.tgtEP,
		JobKey:     r.job.JobKey,
		JobName:    r.job.Name,
		InstanceID: r.instID,
		Schema:     r.targetSchema(),
		OnExists:   r.doc.Target.OnExists,
		Catalog:    source.Catalog{Tables: r.selectedTables()},
	})
}

func (r *runner) openSource(ctx context.Context) (*pgsource.Source, error) {
	return pgsource.Open(ctx, pgsource.Options{
		Source:      r.srcEP,
		JobKey:      r.job.JobKey,
		Publication: r.publication(),
		Slot:        r.slot(),
		Catalog:     source.Catalog{Tables: r.selectedTables()},
	})
}

// VerifyJob re-checks an already-synced job without moving any data. `jinliang
// status --verify` runs it; it creates nothing and takes no lease.
func VerifyJob(ctx context.Context, opt Options) ([]string, error) {
	job, err := opt.Store.GetJob(ctx, opt.JobID)
	if err != nil {
		return nil, err
	}
	doc, err := opt.Store.JobDoc(ctx, job)
	if err != nil {
		return nil, err
	}
	srcConn, err := opt.Store.GetConnection(ctx, job.SourceID)
	if err != nil {
		return nil, err
	}
	tgtConn, err := opt.Store.GetConnection(ctx, job.TargetID)
	if err != nil {
		return nil, err
	}
	ss, err := opt.Store.Secret(ctx, job.SourceID)
	if err != nil {
		return nil, err
	}
	ts, err := opt.Store.Secret(ctx, job.TargetID)
	if err != nil {
		return nil, err
	}
	r := &runner{
		opt:    opt,
		job:    job,
		doc:    doc,
		srcEP:  endpointOf(srcConn, ss),
		tgtEP:  endpointOf(tgtConn, ts),
		instID: instanceID(ctx, opt.Store),
	}
	if err := r.loadCatalog(ctx); err != nil {
		return nil, err
	}
	sink, err := r.openSink(ctx)
	if err != nil {
		return nil, err
	}
	defer sink.Close()
	r.sink = sink
	src, err := r.openSource(ctx)
	if err != nil {
		return nil, err
	}
	defer src.Close()
	r.src = src
	return r.verify(ctx)
}
