package engine

import (
	"context"
	"time"

	"github.com/spencerswagger/jinliang/internal/control"
	"github.com/spencerswagger/jinliang/internal/dsn"
	"github.com/spencerswagger/jinliang/internal/mysqlsink"
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
	// 所以这里**不能**加 omitempty：加上之后「读不到」与「没有暂停」在 JSON 里都没了这个键，
	// 消费方再也分不开（`null` = 未知、`[]` = 确实没有）。
	PausedTables []PausedTableStat `json:"paused_tables"`
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
	st.PausedTables, st.PausedNote = statusPaused(ctx, r.tgtEP, schema, job.JobKey)

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

// statusPaused 按目标端 kind **直读** `_jl_meta.paused_tables`，给出对外形状 + 说明。
//
// 为什么直读、而不是走 `runner.pauseRegistryOf()`（这里原来就是那么写的，是个真 bug）：
// `StatusFor` 造的 runner 只用来探源端表清单，**没有** sink / pauseReg —— 那两样是执行
// 路径（prepare）才构造的。于是 `pauseRegistryOf()` 恒 nil，`status --json` 的
// `paused_tables` 永远为空；而那条 else 分支还在打印「按表暂停在 MySQL 目标端暂不支持」
// （#102 早就支持了，`mysqlsink` 的六个方法都在）。两件事叠加的结果是：
// 真停了表的人打开 status，看到的是「没有暂停」+ 一句过期的话。
//
// 为什么不干脆 Open 一个 sink 再问它：status 是只读命令，Open 会建 `_jl_meta`/`_jl_chunk`、
// 探方言、MySQL 侧还可能补列 —— 「看一眼状态」不该动目标库。这里只连一次、只读一列。
//
// 三种结论必须能分开（对外是三种不同的字段组合，见 Status.PausedTables 的注释）：
//   - 读到、有记录 → PausedTables 非空，note 为空；
//   - 读到、没有记录（含「目标端还没跑过」）→ **空数组**（`[]`，不是 null）+ note 为空；
//   - 读不到 / 读到的不是合法 JSON → `null` + note，意思是**未知**（不是没有暂停）。
func statusPaused(ctx context.Context, tgtEP dsn.Endpoint, schema, jobKey string) ([]PausedTableStat, string) {
	switch tgtEP.Kind {
	case dsn.KindPostgres:
		rec, dirty, err := pgsink.ReadPausedTables(ctx, tgtEP, schema, jobKey)
		if err != nil {
			return nil, "读不到暂停记录（不是「没有暂停」）：" + err.Error()
		}
		if dirty {
			return nil, pausedDirtyNote
		}
		return emptyInsteadOfNil(pausedStatsOf(rec)), ""
	case dsn.KindMySQL:
		rec, dirty, err := mysqlsink.ReadPausedTables(ctx, tgtEP, jobKey)
		if err != nil {
			return nil, "读不到暂停记录（不是「没有暂停」）：" + err.Error()
		}
		if dirty {
			return nil, pausedDirtyNote
		}
		return emptyInsteadOfNil(pausedStatsOf(rec)), ""
	}
	// 目标端 kind 不认识：这段代码还没学会读它的暂停记录。**不许**静默返回空
	// （空数组的意思是「没有暂停」，那是相反的结论）。
	return nil, "这个目标端（" + tgtEP.Kind + "）的暂停记录还不支持读取：这次没读到，" +
		"别把空当成「没有暂停」。你能做什么：用 PostgreSQL 或 MySQL 目标端，或先看目标库的 _jl_meta.paused_tables。" +
		"风险：没有（status 只读，什么都没动）"
}

// emptyInsteadOfNil 把「读到了、没有暂停」变成空数组而不是 nil。
//
// 为什么要较这个真：`null` 与 `[]` 在这条路径上是**相反**的结论（未知 vs 没有暂停），
// 而 Go 的 nil slice 序列化成 `null`、空 slice 序列化成 `[]` —— 不显式区分，
// 消费方只能把「读失败」当成「没事」，那正是这次要修的 bug 的另一种形态。
func emptyInsteadOfNil(s []PausedTableStat) []PausedTableStat {
	if s == nil {
		return []PausedTableStat{}
	}
	return s
}

// pausedDirtyNote 是「那一列不是合法 JSON」的说明（两份目标端共用一句，免得文案漂）。
const pausedDirtyNote = "目标端 `_jl_meta.paused_tables` 不是合法 JSON：这一列已经按「没有暂停」处理，" +
	"但它是脏数据，请人看一眼那一列（可能是手改过或被截断）。风险：本次只是读，没有写任何东西"

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
