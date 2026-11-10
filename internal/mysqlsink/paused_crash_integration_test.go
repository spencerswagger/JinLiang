//go:build integration

// P2：「登记暂停」与「跳过该表的那一批」之间被 kill -9，重启后状态必须收敛。
//
// 两步写（契约 §1.5 第 2/3 条）：
//
//	① `PauseTable` 单独一个事务把记录落库；
//	② 同一批的「数据 + 位点」在另一个事务里提交（该表的事件已被引擎丢掉）。
//
// 崩在 ①②之间会留下「记录在、位点没动、那批数据没写」——这必须是**可恢复**的形态：
// 重启后位点没越过那条事件，重放时该表已登记暂停 → 继续跳过 → 恢复时整表重搬。
// 反过来（②先落、①没落）才是灾难：跳过了但没记下 = 永久静默缺失，所以顺序被钉死。
//
// 这组用例不加锁、不用 mock：两个 Sink 实例共享同一个目标库，就是「进程崩了再起来」。
package mysqlsink

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/spencerswagger/jinliang/internal/pgsink"
	"github.com/spencerswagger/jinliang/internal/source"
)

// pauseCrashFixture 造一张「本任务建的」表 + 已搬完的块标记 + 一个已提交的位点。
func pauseCrashFixture(t *testing.T, s *Sink, table string, rows int) (source.TableRef, source.Position) {
	t.Helper()
	ctx := context.Background()
	ref := refOf(table)
	if _, err := s.db.ExecContext(ctx, "DROP TABLE IF EXISTS `"+table+"`"); err != nil {
		t.Fatalf("清表失败: %v", err)
	}
	if _, err := s.db.ExecContext(ctx,
		"CREATE TABLE `"+table+"` (id bigint PRIMARY KEY, payload varchar(32)) ENGINE=InnoDB"); err != nil {
		t.Fatalf("建表失败: %v", err)
	}
	for i := 1; i <= rows; i++ {
		if _, err := s.db.ExecContext(ctx,
			"INSERT INTO `"+table+"` (id,payload) VALUES (?,?)", i, "v"); err != nil {
			t.Fatalf("灌数失败: %v", err)
		}
	}
	if err := s.RecordObject(ctx, s.tableObjectName(ref), "table"); err != nil {
		t.Fatalf("登记所有权失败: %v", err)
	}
	if _, err := s.db.ExecContext(ctx,
		"DELETE FROM `_jl_chunk` WHERE job_key = ? AND table_name = ?", s.opt.JobKey, table); err != nil {
		t.Fatalf("清块标记失败: %v", err)
	}
	if err := s.MarkChunkDone(ctx, ref, 1, "", "k1", int64(rows)); err != nil {
		t.Fatalf("记块失败: %v", err)
	}
	if err := s.MarkTableDone(ctx, ref); err != nil {
		t.Fatalf("盖完成标记失败: %v", err)
	}
	pos := posOf(t, "mysql:v1:file=binlog.000001;pos=500")
	if err := s.Apply(ctx, nil, pos); err != nil { // 空批只推位点：首搬结束时的形态
		t.Fatalf("写位点失败: %v", err)
	}
	return ref, pos
}

// 崩在 ①②之间：记录在、位点没动、那批数据没写 → 重启后必须能整表重搬收敛，
// 且**不允许**出现「暂停已登记、该表的行却被写进去」。
func TestIntegrationPauseCrashBetweenRegisterAndSkip(t *testing.T) {
	ctx := context.Background()
	a := openSink(t, nil)
	ensureMetaRow(t, a)
	const tbl = "jl_b3_crash_between"
	ref, posBefore := pauseCrashFixture(t, a, tbl, 10)
	t.Cleanup(func() { dropTable(t, a, tbl); _ = a.ClearPausedTable(ctx, ref) })

	// ① 登记暂停（独立事务）——这一步必须成功，成功之后才允许跳过。
	if err := a.PauseTable(ctx, ref, pgsink.PausedTable{
		Reason: pgsink.PauseReasonTypeChanged, Detail: "列 v：源端 bigint → varchar(64)",
		Resume: pgsink.ResumeRecopy,
	}); err != nil {
		t.Fatalf("登记暂停失败: %v", err)
	}
	// ② 还没提交那一批：kill -9 了。用 Close 模拟进程消失（连接断掉、内存没了）。
	a.Close()

	// ── 重启：新进程、同一个目标库 ──────────────────────────────
	b := openSink(t, nil)
	ensureMetaRow(t, b)
	if err := b.LoadPausedTables(ctx); err != nil {
		t.Fatalf("重启后读暂停记录失败: %v", err)
	}
	paused, err := b.PausedTables(ctx)
	if err != nil {
		t.Fatalf("读暂停记录失败: %v", err)
	}
	if _, ok := paused[tbl]; !ok {
		t.Fatalf("重启后暂停记录丢了（登记是先落库的，不该丢）：%v", paused)
	}
	posAfter, err := b.Position(ctx)
	if err != nil {
		t.Fatalf("读位点失败: %v", err)
	}
	if posAfter != posBefore.String() {
		t.Fatalf("崩在两步之间，位点不该前进：%s → %s", posBefore, posAfter)
	}

	// 不变量①：暂停登记之后，这张表**一行都不许再写进去**（写路径的闸）。
	err = b.Apply(ctx, []source.Event{{
		Kind:  source.EvInsert,
		Table: ref,
		New: source.Row{Table: ref, Values: []source.Value{
			{Kind: source.KindInt, I: 11}, {Kind: source.KindString, S: "late"}}},
	}}, nil)
	if err == nil {
		t.Fatal("暂停已登记，写路径必须拒绝这张表的事件（否则「跳过了但没记下」反过来也会发生）")
	}
	if !strings.Contains(err.Error(), "引擎没有过滤掉暂停表") {
		t.Fatalf("拒绝原因要说清是「引擎没过滤」而不是别的：%v", err)
	}
	if n := countRows(t, b, tbl); n != 10 {
		t.Fatalf("暂停期间该表不许被写：应有 10 行，实际 %d", n)
	}

	// 不变量②：恢复走契约 §1.6 的四步（清块标记 → 清表 → 重搬 → 解除），收敛到源端。
	if err := b.MarkTableRecopyPending(ctx, ref); err != nil {
		t.Fatalf("标待重搬失败: %v", err)
	}
	if err := b.TruncateTable(ctx, ref); err != nil {
		t.Fatalf("清表失败: %v", err)
	}
	for i := 1; i <= 12; i++ { // 源端在暂停窗口里长到 12 行
		if _, err := b.db.ExecContext(ctx,
			"INSERT INTO `"+tbl+"` (id,payload) VALUES (?,?)", i, "v"); err != nil {
			t.Fatalf("重搬写行失败: %v", err)
		}
	}
	if err := b.MarkTableDone(ctx, ref); err != nil {
		t.Fatalf("盖完成标记失败: %v", err)
	}
	if err := b.ClearPausedTable(ctx, ref); err != nil {
		t.Fatalf("解除暂停失败: %v", err)
	}
	if n := countRows(t, b, tbl); n != 12 {
		t.Fatalf("整表重搬后应有 12 行，实际 %d", n)
	}
	if paused, _ := b.PausedTables(ctx); len(paused) != 0 {
		t.Fatalf("重搬搬成后记录应解除，实际 %v", paused)
	}
}

// 崩在 ②之后（该批已提交、位点已前进、该表被跳过）：重启后**块标记仍是「搬完」**，
// 这是危险态——必须靠 reconcile 的 `MarkTableRecopyPending` 把它翻回「欠一次重搬」。
// 这条用例把「谁负责翻」钉在契约上：不翻就会「暂停解除了、数据其实缺着」。
func TestIntegrationPauseCrashAfterSkipKeepsRecoverable(t *testing.T) {
	ctx := context.Background()
	a := openSink(t, nil)
	ensureMetaRow(t, a)
	const tbl = "jl_b3_crash_after"
	ref, _ := pauseCrashFixture(t, a, tbl, 10)
	t.Cleanup(func() { dropTable(t, a, tbl); _ = a.ClearPausedTable(ctx, ref) })

	if err := a.PauseTable(ctx, ref, pgsink.PausedTable{
		Reason: pgsink.PauseReasonTypeChanged, Detail: "暂停后该表被跳过", Resume: pgsink.ResumeRecopy,
	}); err != nil {
		t.Fatalf("登记暂停失败: %v", err)
	}
	// ② 该批已提交：只剩别的表的事件，位点照常前进（契约 §1.5 第 3 条）。
	pos2 := posOf(t, "mysql:v1:file=binlog.000001;pos=900")
	if err := a.Apply(ctx, nil, pos2); err != nil {
		t.Fatalf("提交空批失败: %v", err)
	}
	a.Close()

	b := openSink(t, nil)
	ensureMetaRow(t, b)
	if err := b.LoadPausedTables(ctx); err != nil {
		t.Fatalf("重启后读暂停记录失败: %v", err)
	}
	paused, _ := b.PausedTables(ctx)
	if _, ok := paused[tbl]; !ok {
		t.Fatal("记录不该因为位点前进而消失")
	}
	prog, err := b.ChunkProgress(ctx)
	if err != nil {
		t.Fatalf("读块进度失败: %v", err)
	}
	// 危险态的准确形状：块标记说「搬完了」，但暂停窗口里的行没有。
	if !prog[tbl].Done {
		t.Fatalf("这个场景的前提是「块标记仍说搬完」，实际 %#v", prog[tbl])
	}
	// 契约 §1.6 步 2：reconcile 必须把它翻回「欠一次重搬」，否则永远不会重搬。
	if err := b.MarkTableRecopyPending(ctx, ref); err != nil {
		t.Fatalf("标待重搬失败: %v", err)
	}
	prog, err = b.ChunkProgress(ctx)
	if err != nil {
		t.Fatalf("读块进度失败: %v", err)
	}
	if _, ok := prog[tbl]; ok {
		t.Fatalf("标待重搬之后不该还有块标记（否则 planCopy 判成已搬完、永远不重搬）：%#v", prog[tbl])
	}
	// 恢复后记录才允许解除（先重搬、后解除）。
	if err := b.ClearPausedTable(ctx, ref); err != nil {
		t.Fatalf("解除失败: %v", err)
	}
	if _, ok := readPausedMap(t, b, mustJobKey(t, b))[tbl]; ok {
		t.Fatal("解除后记录应消失")
	}
}

// mustJobKey 读本任务在 `_jl_meta` 的 job_key（与 readPaused 的形状对齐）。
func mustJobKey(t *testing.T, s *Sink) string {
	t.Helper()
	var k string
	if err := s.db.QueryRowContext(context.Background(),
		"SELECT job_key FROM `_jl_meta` WHERE job_key = ?", s.opt.JobKey).Scan(&k); err != nil {
		t.Fatalf("读 job_key 失败: %v", err)
	}
	return k
}

func readPausedMap(t *testing.T, s *Sink, jobKey string) map[string]pgsink.PausedTable {
	t.Helper()
	m, err := s.PausedTables(context.Background())
	if err != nil {
		t.Fatalf("读暂停记录失败: %v", err)
	}
	_ = jobKey
	return m
}

var _ = errors.Is
