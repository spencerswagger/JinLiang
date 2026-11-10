package engine

// 非 PG 目标端（MySQL）的**按表暂停恢复路径**。
//
// PG 目标端的恢复长在 `prepare()` + `snapshot()` 里（清块标记 → planCopy 排进重搬 →
// 搬成解除）。通用路径没有 `snapshot()` 那一套，所以这里按**同样的四步**做一遍：
//
//	① 对齐判定   —— `EnsureSchema` 通过（它由 `executeGeneric` 在 needCopy 时调用）
//	② 标成待重搬 —— 不需要额外的标记：本函数把表收进 `forceRecopy`
//	③ 整表重搬   —— 清空目标表（本任务建的才准清）+ 从表头读（`copyGeneric` 本来就是从头读）
//	④ 搬成才解除 —— `copyGeneric` 盖完 `chunk_no=0` 之后 `finishGenericRecopy` 删记录
//
// 为什么不能只绑 `pauseReg` 不做这段（Owl II 必改 2）：只停不恢复 = 那张表被静默跳过、
// `skipped_rows` 一直涨、而这一轮"成功"——正是「静默缺数据」。

import (
	"context"
	"fmt"
	"strings"

	"github.com/spencerswagger/jinliang/internal/dsn"
	"github.com/spencerswagger/jinliang/internal/pgsink"
	"github.com/spencerswagger/jinliang/internal/source"
)

// planGenericRecopy 把「欠一次整表重搬」的暂停表收进 `r.forceRecopy`，并在搬之前清空它们。
//
// 判据（与 PG 侧同源）：记录里 `resume != manual`（本任务建的表）∧ 该表仍在本轮范围内。
// `resume=manual` 的用户表**不清也不搬**，只发一条 note 要人决定（清它就是删用户数据，
// 不清就 upsert 又会把暂停期间被删的行留成幻行）。
func (r *runner) planGenericRecopy(ctx context.Context) {
	if r.pauseReg == nil {
		return
	}
	rec := r.pausedSnapshot()
	if len(rec) == 0 {
		return
	}
	inScope := map[string]bool{}
	for _, t := range r.tables {
		inScope[t.Table] = true
	}
	for _, key := range pgsink.PausedKeys(rec) {
		one := rec[key]
		if !inScope[key] {
			continue
		}
		if pgsink.NormalizedResume(one.Resume) == pgsink.ResumeManual {
			r.note(ctx, "表 "+key+" 暂停中，且它是目标端既有表（不是本任务建的）：不会自动清空重搬。"+
				"请在 `jinliang status` 里按提示选一条——重建这张表 / 手工清空 / 把它移出同步范围")
			continue
		}
		if r.forceRecopy == nil {
			r.forceRecopy = map[string]source.TableRef{}
		}
		ref := source.TableRef{Schema: r.targetSchema(), Table: key}
		for _, t := range r.tables {
			if t.Table == key {
				ref = t
				break
			}
		}
		r.forceRecopy[key] = ref
	}
}

// truncateForcedRecopy 在搬之前清空「欠一次重搬」的表。
//
// 只清**本任务建的表**（`_jl_object` 有登记）：用户自己的表里全是他们的行，清它就是删数据。
// 拿不到所有权答案时**停下来**（返回错误），不猜。
func (r *runner) truncateForcedRecopy(ctx context.Context, st stateCapable) error {
	for key, ref := range r.forceRecopy {
		tref := r.targetRef(ref)
		ours, err := r.didCreateTable(ctx, tref)
		if err != nil {
			return fmt.Errorf("查不到目标表 %s 是谁建的（无法判断能不能清空重搬）: %w", key, err)
		}
		if !ours {
			// 不自动清：把它从没重搬的名单里摘掉，保留暂停记录（下面还会发 note）。
			delete(r.forceRecopy, key)
			r.note(ctx, "表 "+key+" 是目标端既有表：不自动清空重搬（需要你决定：重建 / 手工清空 / 移出范围）")
			continue
		}
		if err := st.TruncateTable(ctx, tref); err != nil {
			return fmt.Errorf("清空目标表 %s 准备整表重搬失败: %w", key, err)
		}
		// 块标记一并清掉：不清的话 `copyGeneric` 会以为这张表已经搬过（续跑语义）。
		if reg, ok := r.pauseReg.(interface {
			MarkTableRecopyPending(context.Context, source.TableRef) error
		}); ok {
			if err := reg.MarkTableRecopyPending(ctx, tref); err != nil {
				return fmt.Errorf("清掉 %s 的块标记失败: %w", key, err)
			}
		}
		r.clearedTables = append(r.clearedTables, key)
		r.note(ctx, "表 "+key+" 整表重搬：先清空目标端再从头读（暂停窗口里的删除只能这样补掉）")
	}
	return nil
}

// finishGenericRecopy 在整表重搬搬成之后解除暂停（只删这几个 key，别的记录留着）。
//
// 顺序不可换：`copyGeneric` 末尾已经盖了 `chunk_no=0`；这里再删记录。
// 反过来的话崩在中间就是「记录没了、块标记说没搬完」——下一轮又重搬一遍（幂等，但白搬）。
func (r *runner) finishGenericRecopy(ctx context.Context) error {
	if r.pauseReg == nil || len(r.forceRecopy) == 0 {
		return nil
	}
	for key, ref := range r.forceRecopy {
		tref := r.targetRef(ref)
		if err := r.pauseReg.ClearPausedTable(ctx, tref); err != nil {
			return fmt.Errorf("解除 %s 的暂停失败: %w", key, err)
		}
		rec, _ := r.pausedRecord(key)
		r.forgetPaused(key)
		msg := "表 " + key + " 已恢复：整表重搬完成"
		if rec.SkippedRows > 0 {
			msg += fmt.Sprintf("，跳过窗口的 %d 行已补齐", rec.SkippedRows)
		}
		r.note(ctx, msg)
	}
	return nil
}

// detectRelationDrift 在**通用目标端**（`r.sink == nil`）上补一层结构变化检测。
//
// 为什么需要：PG 目标端那边是 `pgsink` 自己比「事件带的 Relation vs 它缓存的表定义」，
// 不一致就返回 `TypeChangedError`（也是 `PausableError`）。MySQL 目标端**没有**这层
// （Data 的 PauseRegistry 只做了存储），于是「源端改了列类型」在 MySQL 目标端表现为一条
// 裸的驱动错误（1264/1366 之类），`pauseSink` 认不出它是「这张表结构对不上」→ 整轮失败，
// 表级暂停永远走不到。
//
// 判据：事件带的 `Relation` 与**本轮 prepare 读到的源端 catalog** 比列名与列类型。
// 只对 PG 源开启：PG 的 Relation 与 information_schema 都是源端类型文本，可比；
// MySQL 源在没开 `binlog_row_metadata=FULL` 时根本不给 Relation，类型文本口径也不一致，
// 硬比会误报（那是 Data 侧要补的能力，不是这里瞎猜）。
func (r *runner) detectRelationDrift(evs []source.Event) pgsink.PausableError {
	if r.sink != nil || r.srcEP.Kind != dsn.KindPostgres {
		return nil
	}
	for _, ev := range evs {
		rel := ev.Relation
		if rel == nil || rel.TableRef.Table == "" {
			continue
		}
		want, ok := r.catalogTable(rel.TableRef)
		if !ok {
			continue // 不在本任务范围 / 缓存里没有：不在这里判（别的检查会挡住）
		}
		if diff, changed := relationDiff(&want, rel); changed {
			return &pgsink.TypeChangedError{TableName: rel.TableRef.Table, Detail: diff}
		}
	}
	return nil
}

// catalogTable 在 prepare 读到的 catalog 里找这张表。
func (r *runner) catalogTable(ref source.TableRef) (source.Table, bool) {
	for _, t := range r.cat.Tables {
		if t.Table == ref.Table {
			return t, true
		}
	}
	return source.Table{}, false
}

// relationShape 把一份表定义压成可比的字符串（列名:类型，按顺序）。
func relationShape(t *source.Table) string {
	var b strings.Builder
	for i, c := range t.Columns {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(c.Name)
		b.WriteByte(':')
		b.WriteString(canonType(c.Type))
	}
	return b.String()
}

// canonType 把类型文本归一（去掉空格与大小写差异）：`NUMERIC(12, 2)` 与 `numeric(12,2)` 是同一个。
func canonType(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = strings.ReplaceAll(s, " ", "")
	return s
}

// relationDiff 比较 catalog 与事件里的 Relation，返回人话差异与「是否变了」。
func relationDiff(want *source.Table, got *source.Table) (string, bool) {
	if relationShape(want) == relationShape(got) {
		return "", false
	}
	oldTypes := map[string]string{}
	for _, c := range want.Columns {
		oldTypes[c.Name] = canonType(c.Type)
	}
	var diffs []string
	for _, c := range got.Columns {
		if t, ok := oldTypes[c.Name]; ok && t != canonType(c.Type) {
			diffs = append(diffs, fmt.Sprintf("列 %s：源端原来是 %s，现在是 %s", c.Name, t, canonType(c.Type)))
		}
	}
	if len(diffs) == 0 {
		// 列名变了（改名/删列）：列数可能一样，必须单独报，不能当成「没变」。
		return "列名在源端变了（改名或删列）：" + relationShape(got), true
	}
	return strings.Join(diffs, "；"), true
}

// pausedSnapshot 返回内存里的暂停集合副本（避免调用方在遍历时与写路径抢锁）。
func (r *runner) pausedSnapshot() map[string]pgsink.PausedTable {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.paused) == 0 {
		return nil
	}
	out := make(map[string]pgsink.PausedTable, len(r.paused))
	for k, v := range r.paused {
		out[k] = v
	}
	return out
}
