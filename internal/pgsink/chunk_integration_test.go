//go:build integration

package pgsink

// 块进度是续跑的凭据，所以单独测它：读回来的边界必须能原样喂给下一块、
// 完成标记必须和数据分开、ResetTableChunks 必须「清数据 + 清标记」一起生效。

import (
	"context"
	"strconv"
	"testing"

	"github.com/spencerswagger/jinliang/internal/source"
)

func TestChunkProgressRoundTrip(t *testing.T) {
	ctx := context.Background()
	table := "chunk_progress"
	s := openSinkFor(t, batchCatalog(table), nil)
	// 先把表建出来（Open 只建状态表，业务表由 EnsureSchema 建）。
	if _, err := s.EnsureSchema(ctx, batchCatalog(table)); err != nil {
		t.Fatalf("建目标表失败: %v", err)
	}
	ref := source.TableRef{Schema: "public", Table: table}
	ti := s.Table(ref)
	if ti == nil {
		t.Fatalf("目标端没有 %s 的定义（EnsureSchema 没建表？）", table)
	}

	rows := func(from, to int) [][]any {
		out := make([][]any, 0, to-from+1)
		for i := from; i <= to; i++ {
			out = append(out, []any{strconv.Itoa(i), "p" + strconv.Itoa(i)})
		}
		return out
	}

	// 空进度：一块都没搬
	if got, err := s.ChunkProgress(ctx); err != nil {
		t.Fatal(err)
	} else if p := got[table]; p.LastNo != 0 || p.Done || p.Cursor != "" {
		t.Fatalf("还没搬过时进度应为空，得到 %+v", p)
	}

	// 两块：边界与合计行数必须对得上
	if err := s.BulkApply(ctx, ti, 1, rows(1, 3), "pos1", "", "3"); err != nil {
		t.Fatal(err)
	}
	if err := s.BulkApply(ctx, ti, 2, rows(4, 6), "pos1", "3", "6"); err != nil {
		t.Fatal(err)
	}
	got, err := s.ChunkProgress(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if p := got[table]; p.LastNo != 2 || p.Cursor != "6" || p.Rows != 6 || p.Done {
		t.Fatalf("进度应为 lastNo=2 cursor=6 rows=6 done=false，得到 %+v", p)
	}

	// 完成标记：Done 变 true，但**不能**冒充一个块（LastNo/Cursor 不许被它改掉）
	if err := s.MarkTableDone(ctx, ref); err != nil {
		t.Fatal(err)
	}
	got, err = s.ChunkProgress(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if p := got[table]; !p.Done || p.LastNo != 2 || p.Cursor != "6" || p.Rows != 6 {
		t.Fatalf("完成标记后应为 done=true 且块边界不变（lastNo=2 cursor=6），得到 %+v", p)
	}
	// 幂等：再盖一次不报错、不改行数
	if err := s.MarkTableDone(ctx, ref); err != nil {
		t.Fatal(err)
	}

	// 续跑从 6 之后接着写：块号接着排，数据不重复
	if err := s.BulkApply(ctx, ti, 3, rows(7, 9), "pos1", "6", "9"); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := s.conn.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 9 {
		t.Fatalf("续写后应有 9 行（不重不漏），实际 %d", n)
	}

	// StartFreshCopy：**清标记 + 推位点，同一个事务**（Owl 必改 1）。
	// 判据：调完之后「没有块进度」且「位点就是新位点」——两个状态不可能只成立一半。
	if err := s.StartFreshCopy(ctx, "pos-fresh"); err != nil {
		t.Fatal(err)
	}
	got, err = s.ChunkProgress(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if p, ok := got[table]; ok && (p.LastNo != 0 || p.Done) {
		t.Fatalf("StartFreshCopy 之后不该再有块进度，得到 %+v", p)
	}
	if pos, err := s.Position(ctx); err != nil {
		t.Fatal(err)
	} else if pos != "pos-fresh" {
		t.Fatalf("StartFreshCopy 之后位点应为 pos-fresh，得到 %q", pos)
	}
	// 它不是「清表」：数据还在（清不清表由调用方按表决定，无主键表那套走 ResetTableChunks）。
	if err := s.conn.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 9 {
		t.Fatalf("StartFreshCopy 不该动数据，实际 %d 行", n)
	}

	// ResetTableChunks：数据和标记一起回到起点
	if err := s.ResetTableChunks(ctx, ref); err != nil {
		t.Fatal(err)
	}
	if err := s.conn.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("ResetTableChunks 之后表应为空，实际 %d 行", n)
	}
	got, err = s.ChunkProgress(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if p, ok := got[table]; ok && (p.LastNo != 0 || p.Done) {
		t.Fatalf("ResetTableChunks 之后不该再有进度，得到 %+v", p)
	}
}

// 完成标记（chunk_no=0）的 rows 必须跟着**块重放**刷新（P1）。
//
// 与 MySQL 目标端同一个缺口（两边 SQL 不同、坑一样）：块重放会改各块之和，
// 完成标记若停在旧值，它和 `ChunkProgress.Rows` 就永久脱节——任何「按已搬行数
// 判断搬没搬完」的逻辑都会被这个陈旧值带偏（该重搬判成已搬完 = 静默缺行）。
func TestChunkStampRefreshesOnReplay(t *testing.T) {
	ctx := context.Background()
	table := "chunk_stamp_replay"
	s := openSinkFor(t, batchCatalog(table), nil)
	if _, err := s.EnsureSchema(ctx, batchCatalog(table)); err != nil {
		t.Fatalf("建目标表失败: %v", err)
	}
	ref := source.TableRef{Schema: "public", Table: table}
	ti := s.Table(ref)
	if ti == nil {
		t.Fatal("目标端没有表定义")
	}
	rows := func(from, to int) [][]any {
		out := make([][]any, 0, to-from+1)
		for i := from; i <= to; i++ {
			out = append(out, []any{strconv.Itoa(i), "p" + strconv.Itoa(i)})
		}
		return out
	}
	stampRows := func() int64 {
		var n int64
		if err := s.conn.QueryRow(ctx,
			"SELECT rows FROM _jl_chunk WHERE job_key=$1 AND table_name=$2 AND chunk_no=0",
			s.opt.JobKey, table).Scan(&n); err != nil {
			t.Fatalf("读完成标记失败: %v", err)
		}
		return n
	}

	if err := s.BulkApply(ctx, ti, 1, rows(1, 3), "pos1", "", "3"); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkTableDone(ctx, ref); err != nil {
		t.Fatal(err)
	}
	if got := stampRows(); got != 3 {
		t.Fatalf("首次盖章应是 3 行，实际 %d", got)
	}

	// 重放同一块（续跑重读 / 重搬后重读）：行数变大，完成标记必须跟着变。
	if err := s.BulkApply(ctx, ti, 1, rows(1, 5), "pos1", "", "5"); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkTableDone(ctx, ref); err != nil {
		t.Fatal(err)
	}
	got, err := s.ChunkProgress(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got[table].Rows != 5 {
		t.Fatalf("ChunkProgress.Rows 应是 5，实际 %d", got[table].Rows)
	}
	if n := stampRows(); n != 5 {
		t.Fatalf("完成标记的 rows 没跟着重放刷新：%d，应为 5（与 ChunkProgress.Rows 永久脱节）", n)
	}
}
