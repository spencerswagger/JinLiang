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
		srcRef := source.TableRef{Schema: r.targetSchema(), Table: key}
		for _, t := range r.tables {
			if t.Table == key {
				srcRef = t
				break
			}
		}
		// **先确认目标端列类型真的对齐了**，再决定清空重搬（Owl II P1）。
		//
		// 为什么不能省：MySQL 的 `EnsureSchema` 在 append 下只补缺列、不改列类型，
		// 所以「prepare 跑到这里」并不代表对齐。没对齐就清空重搬 = 把源端的数值写进
		// 类型不匹配的列，MySQL 会**静默收敛**（1.25 → 1），然后我们发「已恢复：整表重搬完成」。
		// 那是假恢复：比不恢复坏得多。
		if ok, why, err := r.targetColumnsAligned(ctx, srcRef); err != nil {
			return // 读不到就什么都不做：留着暂停记录，人来看（不猜）
		} else if !ok {
			r.note(ctx, "表 "+key+" 仍暂停：目标端列类型还没对齐（"+why+"）。"+
				"对齐之后重跑会自动整表重搬补齐")
			continue
		}
		if r.forceRecopy == nil {
			r.forceRecopy = map[string]source.TableRef{}
		}
		if r.recopyRecords == nil {
			r.recopyRecords = map[string]pgsink.PausedTable{}
		}
		r.recopyRecords[key] = one
		ref := srcRef
		r.forceRecopy[key] = ref
	}
}

// targetColumnsAligned 问目标端：这张表的列类型是不是源端这批列该有的类型。
//
// 能力做成**可选接口**：目标端能回答才自动重搬。回答不了就返回 (false, 原因, nil)，
// 让调用方留着暂停记录——「不知道对齐没对齐就清空重搬」是最坏的组合。
func (r *runner) targetColumnsAligned(ctx context.Context, ref source.TableRef) (bool, string, error) {
	matcher, ok := r.pauseReg.(interface {
		ColumnsMatch(context.Context, source.TableRef, []source.Column) (bool, string, error)
	})
	if !ok {
		return false, "这个目标端还不能回答「列类型对齐了吗」（需要 ColumnsMatch 能力），本版不自动清空重搬", nil
	}
	want, ok := r.catalogTable(ref)
	if !ok {
		return false, "本轮 catalog 里没有这张表的列定义", nil
	}
	// 目标端的 schema 与源端可能不同：按目标端 ref 问。
	return matcher.ColumnsMatch(ctx, r.targetRef(ref), want.Columns)
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
		if err := r.pauseReg.MarkTableRecopyPending(ctx, tref); err != nil {
			return fmt.Errorf("清掉 %s 的块标记失败: %w", key, err)
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
		// 搬成才解除（契约 §1.6 步 4）：记录一直留到这一步，中途崩了下一轮还会重搬。
		if err := r.pauseReg.ClearPausedTable(ctx, tref); err != nil {
			return fmt.Errorf("解除 %s 的暂停失败: %w", key, err)
		}
		rec := r.recopyRecords[key]
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
func (r *runner) detectRelationDrift(ctx context.Context, evs []source.Event) pgsink.PausableError {
	if r.sink != nil || r.srcEP.Kind != dsn.KindPostgres {
		return nil
	}
	seen := map[string]bool{}
	for _, ev := range evs {
		rel := ev.Relation
		if rel == nil || rel.TableRef.Table == "" || seen[rel.TableRef.Table] {
			continue
		}
		seen[rel.TableRef.Table] = true
		if _, paused := r.pausedRecord(rel.TableRef.Table); paused {
			continue
		}
		want, ok := r.catalogTable(rel.TableRef)
		if !ok {
			continue
		}
		// **触发信号**：事件里的 Relation 与 catalog 的类型身份不同 = 源端这一版变了。
		// （注意：catalog 是**本轮 prepare** 读的，所以「源端上一轮改的」在这里比不出来——
		// 那种情况由 prepare 期的对齐检查兜住。这里管的是「跑起来之后才改的」。）
		if _, changed := relationDiff(&want, rel); !changed {
			continue
		}
		// **判定**：源端变了，目标端对齐了吗？问目标端（ColumnsMatch 用目标端自己那份类型字典）。
		ok2, why, err := r.targetColumnsAligned(ctx, rel.TableRef)
		if err != nil {
			continue // 读不到就不在这里判（下一个检查会响亮失败），绝不猜
		}
		if ok2 {
			// 目标端已经是对的类型（例如用户手工对齐了）：这是「追平」，不是「暂停」。
			continue
		}
		return &pgsink.TypeChangedError{TableName: rel.TableRef.Table,
			Detail: "源端这一版列类型变了，而目标端还没对齐：" + why}
	}
	return nil
}

// sameOIDShape 报告两份表定义的列名与列数是否一致（列名一致才谈得上「比同一个位置的 OID」）。
func sameOIDShape(want, got *source.Table) bool {
	if len(want.Columns) != len(got.Columns) {
		return false
	}
	for i := range want.Columns {
		if want.Columns[i].Name != got.Columns[i].Name {
			return false
		}
		if want.Columns[i].TypeOID == 0 || got.Columns[i].TypeOID == 0 {
			return false // 有一边拿不到身份：退回文本比较
		}
	}
	return true
}

func sameOIDs(want, got *source.Table) bool {
	for i := range want.Columns {
		if want.Columns[i].TypeOID != got.Columns[i].TypeOID {
			return false
		}
	}
	return true
}

// oidDiff 用人话说明哪几列的类型身份变了（带上两边的类型文本，方便用户对照）。
func oidDiff(want, got *source.Table) string {
	var diffs []string
	for i := range want.Columns {
		w, g := want.Columns[i], got.Columns[i]
		if w.TypeOID != g.TypeOID {
			diffs = append(diffs, fmt.Sprintf("列 %s：源端原来是 %s(oid=%d)，现在是 %s(oid=%d)",
				g.Name, w.Type, w.TypeOID, g.Type, g.TypeOID))
		}
	}
	return strings.Join(diffs, "；")
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
//
// **优先比 TypeOID**（类型身份）：pgoutput 的 Relation 消息只带 OID，`buildRelation` 把它填进
// `Column.TypeOID`；容器 catalog 也从 `pg_type.oid` 取。两边都有 OID 时只比 OID——
// 这样 `int8`/`bigint`、`numeric`/`numeric(12,2)` 的文本差异不会假红，而「放宽 varchar 长度」
// （OID 不变）天然放行（契约里那本来就属于「追平」）。OID 拿不到时退回文本比较，并在
// detail 里注明——这种写法会把别名差异当成「改类型」，宁可停一下，也不静默写错。
func relationDiff(want *source.Table, got *source.Table) (string, bool) {
	oidComparable := sameOIDShape(want, got)
	if oidComparable {
		if sameOIDs(want, got) {
			return "", false
		}
		return oidDiff(want, got), true
	}
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

// restorePausedAfterFailedRecopy 在整表重搬失败时把暂停记录**放回去**。
//
// 为什么必须还原：`truncateForcedRecopy` 已经把记录摘掉（否则目标端的「最后一道防线」
// 会拦住重搬的写入）。如果重搬失败就停在这里，记录也没了 → 下一轮既不会重搬、也不会暂停，
// 目标端停在一张**清空了一半**的表上，而退出码、界面一切正常。那是最坏的一种静默缺数据。
func (r *runner) restorePausedAfterFailedRecopy(ctx context.Context) {
	if r.pauseReg == nil {
		return
	}
	for key, rec := range r.recopyRecords {
		ref := source.TableRef{Schema: r.targetSchema(), Table: key}
		if err := r.pauseReg.PauseTable(ctx, ref, rec); err != nil {
			r.note(ctx, "重搬失败后还原 "+key+" 的暂停记录也失败了："+err.Error()+
				"（下一轮不会自动重搬这张表，请手工确认它的数据）")
			continue
		}
		r.rememberPaused(key, rec)
	}
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

// detectPrepareDrift 在**通用目标端** prepare 期做一次「源端列 vs 目标端实际列」的对账。
//
// 这是 PG 目标端那条 `TakePendingPauses` 的对应物：PG 的 `EnsureSchema` 会自己比类型并返回
// `PausableError`；MySQL 的 `EnsureSchema` 只补缺列、**不改列类型**，所以「上次跑完之后源端
// 改了列类型」在 MySQL 目标端没有任何信号——只有把行写进去才炸，而 MySQL 会**静默收敛**
// （1.25 存进 bigint 变成 1），连炸都不会炸。
//
// 判据：目标端存在这张表 ∧ 按类型字典映射过来的列类型对不上 → 登记暂停 + 本轮不搬它的存量。
// 目标端还没有这张表（首次）→ 不算漂移（EnsureSchema 会建）。读不到 → 不猜，交给写路径响亮失败。
func (r *runner) detectPrepareDrift(ctx context.Context) error {
	if r.pauseReg == nil || r.sink != nil {
		return nil // 只服务「目标端没有 sink 侧对账」的通用路径
	}
	exists, ok := r.pauseReg.(interface {
		TableExists(context.Context, source.TableRef) (bool, error)
	})
	matcher, ok2 := r.pauseReg.(interface {
		ColumnsMatch(context.Context, source.TableRef, []source.Column) (bool, string, error)
	})
	if !ok || !ok2 {
		return nil // 目标端还不具备对账能力：不在这里判（写路径会响亮失败）
	}
	for _, t := range r.tables {
		if _, paused := r.pausedRecord(t.Table); paused {
			continue // 已经在暂停里：由 planGenericRecopy 判能不能恢复
		}
		tref := r.targetRef(t)
		has, err := exists.TableExists(ctx, tref)
		if err != nil {
			return fmt.Errorf("查目标表 %s 是否存在失败: %w", t.Table, err)
		}
		if !has {
			continue // 首次搬：EnsureSchema 会建，不算漂移
		}
		want, okc := r.catalogTable(t)
		if !okc {
			continue
		}
		aligned, why, err := matcher.ColumnsMatch(ctx, tref, want.Columns)
		if err != nil {
			return fmt.Errorf("对账目标表 %s 的列类型失败: %w", t.Table, err)
		}
		if aligned {
			continue
		}
		// 登记先行（契约 §1.5 第 2 条）：先落库成功，才允许本轮不搬它。
		rec := pgsink.PausedTable{
			Reason: pgsink.PauseReasonTypeChanged,
			Detail: "源端列的当前类型与目标端不一致：" + why,
			Resume: r.resumeModeFor(ctx, tref),
		}
		if err := r.pauseReg.PauseTable(ctx, tref, rec); err != nil {
			return fmt.Errorf("登记表 %s 的暂停失败（本轮一行都没搬）: %w", t.Table, err)
		}
		r.rememberPaused(t.Table, rec)
		if r.skippedTables == nil {
			r.skippedTables = map[string]bool{}
		}
		r.skippedTables[t.Table] = true
		r.note(ctx, "表 "+t.Table+" 的结构和源端对不上（"+rec.Reason+"）：暂停这张表，"+
			"本轮不搬它的存量，其它表照常跑；结构对齐后重跑会整表重搬补齐")
	}
	return nil
}
