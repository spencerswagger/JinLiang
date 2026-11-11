//go:build integration

// MySQL 目标端的按表暂停（契约 §4.2 六个方法 + §1.4 记录形状 + §1.6 恢复第 2 步）。
//
// 每条用例都打在**真 MySQL** 上：这一层的正确性全在 SQL 细节里
// （FOR UPDATE、影响 0 行、老库补列、单 key 删除），mock 掉就等于没测。
package mysqlsink

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/spencerswagger/jinliang/internal/pgsink"
	"github.com/spencerswagger/jinliang/internal/source"
)

// ensureMetaRow 把任务状态行准备好（暂停记录挂在它上面）。
func ensureMetaRow(t *testing.T, s *Sink) {
	t.Helper()
	if err := s.EnsureMeta(context.Background(), "hash", "streaming", "", ""); err != nil {
		t.Fatalf("EnsureMeta: %v", err)
	}
}

func refOf(name string) source.TableRef { return source.TableRef{Table: name} }

// ① 登记 → 读回 → 解除：形状与 PG 侧逐字同源（key = 裸表名，字段一一对应）。
func TestIntegrationPauseRegisterThenClear(t *testing.T) {
	s := openSink(t, nil)
	ensureMetaRow(t, s)
	ctx := context.Background()

	// 先清干净，免得上一次跑的记录串味。
	if err := s.ClearPausedTable(ctx, refOf("jl_b3_pause_a")); err != nil {
		t.Fatalf("清理旧记录: %v", err)
	}

	rec := pgsink.PausedTable{
		Reason: pgsink.PauseReasonTypeChanged, Detail: "列 v：源端 bigint → varchar(64)",
		Resume: pgsink.ResumeRecopy,
	}
	if err := s.PauseTable(ctx, refOf("jl_b3_pause_a"), rec); err != nil {
		t.Fatalf("登记暂停失败: %v", err)
	}
	got, err := s.PausedTables(ctx)
	if err != nil {
		t.Fatalf("读暂停记录失败: %v", err)
	}
	r, ok := got["jl_b3_pause_a"]
	if !ok {
		t.Fatalf("登记后读不到这张表：%v", got)
	}
	if r.Reason != pgsink.PauseReasonTypeChanged || r.Detail == "" || r.Resume != pgsink.ResumeRecopy {
		t.Fatalf("记录字段不对：%#v", r)
	}
	if r.At == "" {
		t.Fatal("at 必须写上（第一次暂停时刻）")
	}
	// at 永不被覆盖：第二次登记（重搬失败又发现不一致）仍用第一次的时刻。
	first := r.At
	incremental := r
	incremental.At = ""
	incremental.Reason = pgsink.PauseReasonSchemaDrift // 第二次是另一个原因
	incremental.Detail = "第二次"
	if err := s.PauseTable(ctx, refOf("jl_b3_pause_a"), incremental); err != nil {
		t.Fatalf("第二次登记失败: %v", err)
	}
	got2, _ := s.PausedTables(ctx)
	if got2["jl_b3_pause_a"].At != first {
		t.Fatalf("at 被覆盖了：%q → %q（第一次的时刻最有价值）", first, got2["jl_b3_pause_a"].At)
	}
	// reason / detail 用**最新**这一次（「这张表现在为什么停着」），与 PG 侧同一口径。
	// 早先这里保留过第一次的 reason，会让 reason 与 detail 互相矛盾
	// （reason=type_changed，detail 却写着「列被改名」）。
	if got2["jl_b3_pause_a"].Reason != pgsink.PauseReasonSchemaDrift {
		t.Fatalf("reason 应跟随最新一次（schema_drift），实际 %q", got2["jl_b3_pause_a"].Reason)
	}
	if got2["jl_b3_pause_a"].Detail != "第二次" {
		t.Fatalf("detail 应更新成最新一次，实际 %q", got2["jl_b3_pause_a"].Detail)
	}

	// resume 未知值一律按 recopy（宁可多搬一次，不可留下一张永远不一致的表）。
	if err := s.PauseTable(ctx, refOf("jl_b3_resume_weird"), pgsink.PausedTable{
		Reason: "schema_drift", Detail: "x", Resume: "weird-value",
	}); err != nil {
		t.Fatalf("登记失败: %v", err)
	}
	gotW, _ := s.PausedTables(ctx)
	if gotW["jl_b3_resume_weird"].Resume != pgsink.ResumeRecopy {
		t.Fatalf("未知 resume 应归一化成 recopy，实际 %q", gotW["jl_b3_resume_weird"].Resume)
	}
	_ = s.ClearPausedTable(ctx, refOf("jl_b3_resume_weird"))

	// 解除：只删这一个 key。
	if err := s.ClearPausedTable(ctx, refOf("jl_b3_pause_a")); err != nil {
		t.Fatalf("解除失败: %v", err)
	}
	got3, _ := s.PausedTables(ctx)
	if _, ok := got3["jl_b3_pause_a"]; ok {
		t.Fatalf("解除后还在：%v", got3)
	}
	// 幂等：再解除一次不算错（「搬成清该表，搬败留记录」，重复清不是失败）。
	if err := s.ClearPausedTable(ctx, refOf("jl_b3_pause_a")); err != nil {
		t.Fatalf("重复解除应幂等，实际报错: %v", err)
	}
}

// ② 两张表互不影响：登记/解除 A 不许碰 B（单 key 读改写，不是整列重建）。
func TestIntegrationPauseOneTableKeepsOthers(t *testing.T) {
	s := openSink(t, nil)
	ensureMetaRow(t, s)
	ctx := context.Background()
	b := refOf("jl_b3_pause_b")
	_ = s.ClearPausedTable(ctx, b)
	_ = s.ClearPausedTable(ctx, refOf("jl_b3_pause_a2"))

	if err := s.PauseTable(ctx, b, pgsink.PausedTable{Reason: "schema_drift", Detail: "b"}); err != nil {
		t.Fatalf("登记 B 失败: %v", err)
	}
	if err := s.PauseTable(ctx, refOf("jl_b3_pause_a2"),
		pgsink.PausedTable{Reason: "type_changed", Detail: "a"}); err != nil {
		t.Fatalf("登记 A 失败: %v", err)
	}
	if err := s.ClearPausedTable(ctx, refOf("jl_b3_pause_a2")); err != nil {
		t.Fatalf("解除 A 失败: %v", err)
	}
	got, _ := s.PausedTables(ctx)
	if _, ok := got["jl_b3_pause_a2"]; ok {
		t.Fatalf("A 没被清掉：%v", got)
	}
	if got["jl_b3_pause_b"].Reason != "schema_drift" {
		t.Fatalf("B 的记录被 A 的操作带走了：%v", got)
	}
}

// ③ 跳过窗口记账：行数累加、skipped_from 首次不覆盖、last_skipped_pos 跟着走。
func TestIntegrationNoteSkippedAccounting(t *testing.T) {
	s := openSink(t, nil)
	ensureMetaRow(t, s)
	ctx := context.Background()
	ref := refOf("jl_b3_pause_skip")
	_ = s.ClearPausedTable(ctx, ref)

	if err := s.PauseTable(ctx, ref, pgsink.PausedTable{Reason: "type_changed", Detail: "skip"}); err != nil {
		t.Fatalf("登记失败: %v", err)
	}
	if err := s.NoteSkipped(ctx, ref, 100, "pos-1"); err != nil {
		t.Fatalf("记第一笔失败: %v", err)
	}
	if err := s.NoteSkipped(ctx, ref, 28, "pos-2"); err != nil {
		t.Fatalf("记第二笔失败: %v", err)
	}
	got, _ := s.PausedTables(ctx)
	r := got["jl_b3_pause_skip"]
	if r.SkippedRows != 128 {
		t.Fatalf("跳过行数应累加到 128，实际 %d", r.SkippedRows)
	}
	if r.SkippedFrom != "pos-1" {
		t.Fatalf("skipped_from 应是第一次的 pos-1（不覆盖），实际 %q", r.SkippedFrom)
	}
	if r.LastSkippedPos != "pos-2" {
		t.Fatalf("last_skipped_pos 应是最近一次的 pos-2，实际 %q", r.LastSkippedPos)
	}
	// 没登记的表不许被凭空记一笔。
	if err := s.NoteSkipped(ctx, refOf("jl_b3_never_paused"), 5, "pos-x"); err != nil {
		t.Fatalf("未登记表记账应静默跳过（不报错），实际 %v", err)
	}
	got2, _ := s.PausedTables(ctx)
	if _, ok := got2["jl_b3_never_paused"]; ok {
		t.Fatalf("凭空造出了一条暂停记录：%v", got2)
	}
	_ = s.ClearPausedTable(ctx, ref)
}

// ④ MarkTableRecopyPending：清该表**全部**块标记（含 chunk_no=0），别的表不动。
func TestIntegrationMarkTableRecopyPendingClearsChunks(t *testing.T) {
	s := openSink(t, nil)
	ensureMetaRow(t, s)
	ctx := context.Background()
	a, b := refOf("jl_b3_recopy_a"), refOf("jl_b3_recopy_b")

	// 两张表各记两块 + 一个完成标记。
	for _, ref := range []source.TableRef{a, b} {
		for no := 1; no <= 2; no++ {
			if err := s.MarkChunkDone(ctx, ref, no, "from", "to", 10); err != nil {
				t.Fatalf("记块失败: %v", err)
			}
		}
		if err := s.MarkTableDone(ctx, ref); err != nil {
			t.Fatalf("盖完成标记失败: %v", err)
		}
	}
	prog, err := s.ChunkProgress(ctx)
	if err != nil {
		t.Fatalf("读块进度失败: %v", err)
	}
	if !prog[a.Table].Done || prog[a.Table].LastNo != 2 || prog[a.Table].Rows != 20 {
		t.Fatalf("A 的进度不对：%#v", prog[a.Table])
	}

	if err := s.MarkTableRecopyPending(ctx, a); err != nil {
		t.Fatalf("标待重搬失败: %v", err)
	}
	prog2, _ := s.ChunkProgress(ctx)
	if _, ok := prog2[a.Table]; ok {
		t.Fatalf("A 的块标记（含完成标记）应该全被清掉，实际 %#v", prog2[a.Table])
	}
	if !prog2[b.Table].Done || prog2[b.Table].LastNo != 2 {
		t.Fatalf("B 的块标记不该被动：%#v", prog2[b.Table])
	}
	// 清完之后 planCopy 会把它当「存量没搬完」→ 排进整表重搬，这正是契约 §1.6 步 2 要的。
	_ = s.MarkTableRecopyPending(ctx, b)
}

// ⑤ 老库缺列：补得上就补（不能依赖 ADD COLUMN IF NOT EXISTS —— MySQL 没这个语法），
// 补不上时必须是**明确失败**，不许「UPDATE 影响 0 行当成功」。
func TestIntegrationPausedColumnBackfillAndExplicitFailure(t *testing.T) {
	s := openSink(t, nil)
	ctx := context.Background()
	ensureMetaRow(t, s)

	// 模拟老库：把列删掉。
	if _, err := s.db.ExecContext(ctx, "ALTER TABLE `_jl_meta` DROP COLUMN `paused_tables`"); err != nil {
		t.Fatalf("模拟老库（删列）失败: %v", err)
	}
	s.pausedColMu.Lock()
	s.pausedColOK = false // 绕开“这一版已经补过”的缓存，模拟进程刚起来
	s.pausedColMu.Unlock()

	// 第一次用到它：应该自动补列并成功。
	if err := s.PauseTable(ctx, refOf("jl_b3_backfill"),
		pgsink.PausedTable{Reason: "type_changed", Detail: "补列后登记"}); err != nil {
		t.Fatalf("缺列时应自动补列再登记，实际失败: %v", err)
	}
	var n int
	if err := s.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() "+
			"AND table_name='_jl_meta' AND column_name='paused_tables'").Scan(&n); err != nil {
		t.Fatalf("查列失败: %v", err)
	}
	if n != 1 {
		t.Fatalf("补列没成功（information_schema 里没有 paused_tables）")
	}

	// 状态行不存在：必须报错说清「任务状态行不在」，不许静默。
	other := openSink(t, func(o *Options) { o.JobKey = "zzzzzzzzzz" })
	err := other.PauseTable(ctx, refOf("jl_b3_norow"), pgsink.PausedTable{Reason: "type_changed"})
	if err == nil {
		t.Fatal("状态行不存在时登记暂停必须报错（否则就是「跳过了但没记下」）")
	}
	if !strings.Contains(err.Error(), "任务状态行不存在") {
		t.Fatalf("错误文案要点名「状态行不存在」：%v", err)
	}

	_ = s.ClearPausedTable(ctx, refOf("jl_b3_backfill"))
}

// ⑥ 记录的 JSON 形状必须与契约 §1.4 一致（换实现也不能换形状）。
func TestIntegrationPausedJSONShape(t *testing.T) {
	s := openSink(t, nil)
	ensureMetaRow(t, s)
	ctx := context.Background()
	ref := refOf("jl_b3_shape")
	_ = s.ClearPausedTable(ctx, ref)
	if err := s.PauseTable(ctx, ref, pgsink.PausedTable{
		Reason: "type_changed", Detail: "列 v：目标 text / 源 bigint", Resume: pgsink.ResumeRecopy,
	}); err != nil {
		t.Fatalf("登记失败: %v", err)
	}
	if err := s.NoteSkipped(ctx, ref, 7, "mysql:v1:file=binlog.000001;pos=100"); err != nil {
		t.Fatalf("记账失败: %v", err)
	}
	var raw string
	if err := s.db.QueryRowContext(ctx,
		"SELECT `paused_tables` FROM `_jl_meta` WHERE job_key = ?", s.opt.JobKey).Scan(&raw); err != nil {
		t.Fatalf("读原始列失败: %v", err)
	}
	var shape map[string]map[string]any
	if err := json.Unmarshal([]byte(raw), &shape); err != nil {
		t.Fatalf("列里不是合法 JSON：%q", raw)
	}
	rec, ok := shape["jl_b3_shape"]
	if !ok {
		t.Fatalf("key 必须是目标端裸表名：%q", raw)
	}
	for _, field := range []string{"reason", "at", "detail", "resume", "skipped_rows", "skipped_from"} {
		if _, ok := rec[field]; !ok {
			t.Fatalf("记录里缺字段 %q：%q", field, raw)
		}
	}
	_ = s.ClearPausedTable(ctx, ref)
}
