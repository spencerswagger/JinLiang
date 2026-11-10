//go:build integration

// 块记账的一致性（P1）：`chunk_no=0`（完成标记）的 `rows` 必须跟着块重放刷新。
//
// 为什么这条从「无所谓」升级成必改：块记账现在真的写了（#95），于是
// `chunk_no=0.rows` 变成「这张表一共搬进去多少行」的唯一权威。块重放（续跑重读、
// 重搬后重盖、崩溃后重放同一块）会改 `chunk_no>0` 的行数，而完成标记如果不刷新，
// 两个数就会**永久脱节**：
//
//   - 展示：`status` / `verify` 拿它当「已搬行数」会少报；
//   - 判据：任何「按行数判断搬没搬完 / 要不要重搬」的逻辑都会被这个陈旧值带偏
//     （把该重搬的判成已搬完 = 静默缺行）。
//
// 修复方向是「刷新」而不是「忽略」：完成标记本来就该等于各块之和。
package mysqlsink

import (
	"context"
	"testing"

	"github.com/spencerswagger/jinliang/internal/source"
)

// doneRowRows 读完成标记那一行的 rows（不走 ChunkProgress 的求和，直接读原始行）。
func doneRowRows(t *testing.T, s *Sink, table string) int64 {
	t.Helper()
	var n int64
	if err := s.db.QueryRowContext(context.Background(),
		"SELECT `rows` FROM `_jl_chunk` WHERE job_key = ? AND table_name = ? AND chunk_no = 0",
		s.opt.JobKey, table).Scan(&n); err != nil {
		t.Fatalf("读完成标记失败: %v", err)
	}
	return n
}

// 复现：重放第 1 块（2000 → 2500 行），完成标记必须跟着变。
func TestIntegrationChunkStampRefreshesOnReplay(t *testing.T) {
	s := openSink(t, nil)
	ensureMetaRow(t, s)
	ctx := context.Background()
	ref := refOf("jl_b3_chunk_replay")

	// 清干净，免得上一次跑的标记串味。
	if _, err := s.db.ExecContext(ctx,
		"DELETE FROM `_jl_chunk` WHERE job_key = ? AND table_name = ?", s.opt.JobKey, ref.Table); err != nil {
		t.Fatalf("清块标记失败: %v", err)
	}

	// 首搬：两块 + 完成标记。
	if err := s.MarkChunkDone(ctx, ref, 1, "", "k1", 2000); err != nil {
		t.Fatalf("记第 1 块失败: %v", err)
	}
	if err := s.MarkChunkDone(ctx, ref, 2, "k1", "k2", 500); err != nil {
		t.Fatalf("记第 2 块失败: %v", err)
	}
	if err := s.MarkTableDone(ctx, ref); err != nil {
		t.Fatalf("盖完成标记失败: %v", err)
	}
	if got := doneRowRows(t, s, ref.Table); got != 2500 {
		t.Fatalf("首次盖章后完成标记应是 2500，实际 %d", got)
	}

	// 重放第 1 块：同一块重读，行数变大（续跑/重搬后重读都会这样）。
	if err := s.MarkChunkDone(ctx, ref, 1, "", "k1", 3000); err != nil {
		t.Fatalf("重放第 1 块失败: %v", err)
	}
	if err := s.MarkTableDone(ctx, ref); err != nil { // 重搬完成时再盖一次
		t.Fatalf("重新盖章失败: %v", err)
	}

	prog, err := s.ChunkProgress(ctx)
	if err != nil {
		t.Fatalf("读块进度失败: %v", err)
	}
	if prog[ref.Table].Rows != 3500 {
		t.Fatalf("ChunkProgress.Rows（各块之和）应是 3500，实际 %d", prog[ref.Table].Rows)
	}
	if got := doneRowRows(t, s, ref.Table); got != 3500 {
		t.Fatalf("完成标记的 rows 没跟着重放刷新：%d，应为 3500。\n"+
			"两个数脱节之后，任何「按已搬行数判断搬没搬完」的逻辑都会被陈旧值带偏"+
			"（把该重搬的判成已搬完 = 静默缺行）", got)
	}

	if _, err := s.db.ExecContext(ctx,
		"DELETE FROM `_jl_chunk` WHERE job_key = ? AND table_name = ?", s.opt.JobKey, ref.Table); err != nil {
		t.Fatalf("清理失败: %v", err)
	}
}

// 自查：`MarkTableRecopyPending` 清掉该表全部标记（含完成标记）之后，
// 重搬的**第一块**必须从块号 1、从表头（空游标）开始，且不会凭空出现完成标记。
func TestIntegrationRecopyPendingRestartsAtChunkOne(t *testing.T) {
	s := openSink(t, nil)
	ensureMetaRow(t, s)
	ctx := context.Background()
	ref := refOf("jl_b3_chunk_restart")
	if _, err := s.db.ExecContext(ctx,
		"DELETE FROM `_jl_chunk` WHERE job_key = ? AND table_name = ?", s.opt.JobKey, ref.Table); err != nil {
		t.Fatalf("清块标记失败: %v", err)
	}

	if err := s.MarkChunkDone(ctx, ref, 1, "", "k1", 2000); err != nil {
		t.Fatalf("记块失败: %v", err)
	}
	if err := s.MarkTableDone(ctx, ref); err != nil {
		t.Fatalf("盖章失败: %v", err)
	}
	if err := s.MarkTableRecopyPending(ctx, ref); err != nil {
		t.Fatalf("标待重搬失败: %v", err)
	}

	prog, err := s.ChunkProgress(ctx)
	if err != nil {
		t.Fatalf("读块进度失败: %v", err)
	}
	if p, ok := prog[ref.Table]; ok {
		t.Fatalf("清完之后不该有任何记录（含完成标记），实际 %#v", p)
	}

	// 重搬的第一块：块号从 1 起、游标从表头。
	if err := s.MarkChunkDone(ctx, ref, 1, "", "n1", 1200); err != nil {
		t.Fatalf("重搬第一块失败: %v", err)
	}
	prog, err = s.ChunkProgress(ctx)
	if err != nil {
		t.Fatalf("读块进度失败: %v", err)
	}
	p := prog[ref.Table]
	if p.Done {
		t.Fatalf("只写了一块就被当成搬完：%#v（Done 只能来自 chunk_no=0）", p)
	}
	if p.LastNo != 1 || p.Cursor != "n1" || p.Rows != 1200 {
		t.Fatalf("重搬第一块的进度不对：%#v（应为 LastNo=1 / Cursor=n1 / Rows=1200）", p)
	}
	if _, err := s.db.ExecContext(ctx,
		"DELETE FROM `_jl_chunk` WHERE job_key = ? AND table_name = ?", s.opt.JobKey, ref.Table); err != nil {
		t.Fatalf("清理失败: %v", err)
	}
	_ = source.TableRef{}
}
