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
