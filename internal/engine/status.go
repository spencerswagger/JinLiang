package engine

import (
	"context"
	"time"

	"github.com/spencerswagger/jinliang/internal/control"
	"github.com/spencerswagger/jinliang/internal/dsn"
	"github.com/spencerswagger/jinliang/internal/pgsink"
	"github.com/spencerswagger/jinliang/internal/state"
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
	// PausedTables 是「现在还停着哪几张表」。**读不到时是 null + PausedNote**，
	// 不是空数组——空数组的意思是「没有暂停」，两者在「要不要处理」上是相反的结论。
	PausedTables []PausedTableStat `json:"paused_tables,omitempty"`
	PausedNote   string            `json:"paused_note,omitempty"`
	Note         string            `json:"note,omitempty"`
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
	// 暂停表：读不到就明说「未知」——空数组的意思是「没有暂停」，两者结论相反。
	if reg := r.pauseRegistryOf(); reg != nil {
		if rec, perr := reg.PausedTables(ctx); perr != nil {
			st.PausedNote = "读不到暂停记录（不是「没有暂停」）：" + perr.Error()
		} else {
			st.PausedTables = pausedStatsOf(rec)
		}
	} else if r.tgtEP.Kind == dsn.KindMySQL {
		st.PausedNote = "这个目标端还没有实现暂停记录（按表暂停在 MySQL 目标端暂不支持）"
	}
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

// PausedTableStat 是一张暂停表对外的形状（status / 事件 / Result 共用一份）。
//
// 为什么不做成 `map[string]pgsink.PausedTable` 直接透出：那是**存储**的形状（key 是裸表名），
// 对外的形状要带表名本身，且不能随存储层加字段就抖动（外部是 JSON 契约）。
type PausedTableStat struct {
	Table          string `json:"table"`
	Reason         string `json:"reason"`
	Detail         string `json:"detail,omitempty"`
	SkippedRows    int64  `json:"skipped_rows,omitempty"`
	SkippedFrom    string `json:"skipped_from,omitempty"`
	LastSkippedPos string `json:"last_skipped_pos,omitempty"`
}

// DecideExit 是退出码的**唯一派生点**（Panel 裁决：失败→1 > 用户停止→130 > 有暂停→3 > 0）。
//
// 为什么必须单点：这个码会被 CLI、容器、CI 三处读，散着算迟早不一致；
// 而且「有暂停表却退 0」正是这一版要修的 bug——只要派生只有一处，就不可能漏。
//
// 注意：`phase` 参数目前不参与判定（有暂停时 phase 仍是 done，见 drain 的注释），
// 留着是为了将来「blocked 复用 3」时有地方表达；**不要**在这里判 `state.Paused`：
// 那个阶段 `HoldsLease()=true`，会把「停一张表」变成「整个任务停摆」。
func DecideExit(phase state.Phase, exit int, paused []PausedTableStat) int {
	_ = phase
	if exit == ExitFailed {
		return ExitFailed // 失败优先：真错误比「有欠账」重要
	}
	if exit == ExitStopped {
		return ExitStopped // 用户按停：语义是「没跑完」，不是「跑完了有欠账」
	}
	if len(paused) > 0 {
		return ExitNeedsAction
	}
	return ExitOK
}

// pausedStatsOf 把存储层的暂停记录转成对外形状（排序稳定）。
func pausedStatsOf(rec map[string]pgsink.PausedTable) []PausedTableStat {
	if len(rec) == 0 {
		return nil
	}
	out := make([]PausedTableStat, 0, len(rec))
	for _, key := range pgsink.PausedKeys(rec) {
		one := rec[key]
		out = append(out, PausedTableStat{
			Table: key, Reason: one.Reason, Detail: one.Detail, SkippedRows: one.SkippedRows,
			SkippedFrom: one.SkippedFrom, LastSkippedPos: one.LastSkippedPos,
		})
	}
	return out
}
