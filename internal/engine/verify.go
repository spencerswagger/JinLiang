package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/spencerswagger/jinliang/internal/connect"
	"github.com/spencerswagger/jinliang/internal/control"
	"github.com/spencerswagger/jinliang/internal/dsn"
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
	// 指纹口径不同就**不能直接比**：MySQL 侧是客户端规范行 sha256（mysql-canon-v1），
	// PG 侧是服务端 sum(hashtextextended(t::text,0))——两者永远不相等，直接比等于
	// 每条都假红。跨引擎的内容比对走 `jinliang verify` 的逐行/分块路径（见 internal/verify）。
	if scope := r.src.FingerprintScope(); scope != "" {
		return r.verifyCountOnly(ctx, scope)
	}
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

// verifyCountOnly 是「源与目标指纹口径不同」时的校验：只比行数，内容比对交给
// 逐行路径。**不报「内容不同」**——那是拿两套不可比的指纹硬比出来的假红，
// 比不校验更坏（用户会去查一个不存在的问题）。
func (r *runner) verifyCountOnly(ctx context.Context, srcScope string) ([]string, error) {
	var diffs []string
	for _, t := range r.tables {
		srcN, err := r.src.Count(ctx, t)
		if err != nil {
			return diffs, fmt.Errorf("数源端 %s 的行数失败: %w", t.Table, err)
		}
		dstN, err := r.sink.Count(ctx, r.targetRef(t))
		if err != nil {
			return diffs, fmt.Errorf("数目标端 %s 的行数失败: %w", t.Table, err)
		}
		if srcN == dstN {
			r.emit(ctx, control.Event{Type: "note",
				Message: fmt.Sprintf("校验 %s：行数一致（%d）", t.Table, srcN)})
			continue
		}
		d := srcN - dstN
		if d < 0 {
			d = -d
		}
		msg := fmt.Sprintf("校验 %s：差 %d 行（源 %d / 目标 %d）", t.Table, d, srcN, dstN)
		diffs = append(diffs, msg)
		r.emit(ctx, control.Event{Type: "error", Message: msg})
	}
	r.note(ctx, "本次只比了行数：源端指纹口径是 "+srcScope+"，目标端是 PG 服务端指纹，两者不可直接比。"+
		"要比内容请跑 jinliang verify（逐行/分块比对，跨引擎也成立）")
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
	cat, err := c.Catalog(ctx, r.scope())
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
		// 目标端看到的表定义必须是**目标端**的（schema 是目标 schema）：
		// pgsink 用这份 catalog 建表、找唯一键，拿源端 schema 去建会报「没有这个 schema」。
		Catalog:    r.targetCatalog(),
		SourceKind: catalogEngine(r.srcEP.Kind),
	})
}

// openSource 在构造阶段把源端定下来：PG（逻辑复制槽）或 MySQL（binlog/GTID）。
// 之后引擎只认 runSource 接口，阶段机里不再出现「哪种源」的分支。
func (r *runner) openSource(ctx context.Context) (runSource, error) {
	switch r.srcEP.Kind {
	case dsn.KindPostgres:
		s, err := pgsource.Open(ctx, pgsource.Options{
			Source:      r.srcEP,
			JobKey:      r.job.JobKey,
			Publication: r.publication(),
			Slot:        r.slot(),
			Catalog:     source.Catalog{Tables: r.selectedTables()},
		})
		if err != nil {
			return nil, err
		}
		return pgRunSource{s: s}, nil
	case dsn.KindMySQL:
		src, err := newMyRunSource(ctx, r.srcEP, r.selectedTables())
		if err != nil {
			return nil, err
		}
		// 适配器丢掉的东西（DDL）必须留痕：note 走引擎的事件流，用户看得见。
		src.note = func(msg string) { r.note(ctx, msg) }
		return src, nil
	}
	return nil, fmt.Errorf("源端类型 %q 还没有读实现（现在支持 postgres / mysql）。"+
		"你现在能做什么：把源连接的 kind 改成 postgres 或 mysql。风险：没有（什么都没动）", r.srcEP.Kind)
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
