//go:build integration

package pgsink

// 「位点与数据同事务」在目标端（pgsink）这一侧的最小契约用例。
//
// 为什么单独立一条：位点持久化是撤销成本最高的一处（位点丢了 → 重启整轮重搬；
// 位点写早了 → 崩在中途就漏一段变更，且**没有任何报错**）。原来的证据都散在
// engine 的重型用例里，注入「把 position 写入删掉」时跑一条要几十秒；这里锁住
// 最小可证伪的形状，秒级跑完，`scripts/mutation_gate.py` 拿它当检测器。
//
// 三条判据：
//   1. 成功的一批：数据与位点一起落库（读回来逐字节相等）；
//   2. 失败的一批：位点**一点都不动**（数据事务回滚不许把位点带走）；
//   3. 位点只由 Apply 推进：不经 Apply 的路径不改 `_jl_meta.position`。

import (
	"context"
	"testing"
	"time"

	"github.com/spencerswagger/jinliang/internal/source"
)

func TestPositionPersistedWithData(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const table = "jl_position_atomic"
	s := openSinkFor(t, batchCatalog(table), nil)
	_, _ = s.conn.Exec(ctx, "DROP TABLE IF EXISTS public."+table)
	if _, err := s.EnsureSchema(ctx, batchCatalog(table)); err != nil {
		t.Fatalf("建表: %v", err)
	}
	defer func() { _, _ = s.conn.Exec(ctx, "DROP TABLE IF EXISTS public."+table) }()

	// ① 成功的一批：3 行 + 位点一起落库。
	if err := s.Apply(ctx, insertEvents(table, 3, "0/AA"), testPosition{"postgres:v1:0/AA"}); err != nil {
		t.Fatalf("第一批发写入失败: %v", err)
	}
	pos, err := s.Position(ctx)
	if err != nil {
		t.Fatalf("读位点: %v", err)
	}
	if pos != "postgres:v1:0/AA" {
		t.Fatalf("位点没落盘：期望 postgres:v1:0/AA，实际 %q（位点丢了 → 重启整轮重搬）", pos)
	}
	n, err := s.Count(ctx, source.TableRef{Schema: "public", Table: table})
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("第一批应写 3 行，实际 %d", n)
	}

	// ② 失败的一批：第 2 行主键不是数字（bigint 转不过去）→ 整批回滚，位点不动。
	ref := source.TableRef{Schema: "public", Table: table}
	bad := []source.Event{
		{Kind: source.EvBegin, TxID: "tx-bad"},
		{Kind: source.EvInsert, TxID: "tx-bad", Table: ref, Relation: relationOf(table),
			New: source.Row{Table: ref, Values: []source.Value{
				{Kind: source.KindString, S: "4"}, {Kind: source.KindString, S: "p4"}}}},
		{Kind: source.EvInsert, TxID: "tx-bad", Table: ref, Relation: relationOf(table),
			New: source.Row{Table: ref, Values: []source.Value{
				{Kind: source.KindString, S: "不是数字"}, {Kind: source.KindString, S: "p5"}}}},
		{Kind: source.EvCommit, TxID: "tx-bad"},
	}
	if err := s.Apply(ctx, bad, testPosition{"postgres:v1:0/BB"}); err == nil {
		t.Fatal("坏行没有让这一批失败：位点会被推到一个没写成功的位置上")
	}
	pos2, err := s.Position(ctx)
	if err != nil {
		t.Fatalf("读位点: %v", err)
	}
	if pos2 != "postgres:v1:0/AA" {
		t.Fatalf("失败的一批把位点推走了：期望仍是 postgres:v1:0/AA，实际 %q", pos2)
	}
	n2, err := s.Count(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if n2 != 3 {
		t.Fatalf("失败的一批落进去 %d 行（应整批回滚，仍是 3 行）：数据与位点不同事务", n2-3)
	}

	// ③ 再来一批成功的：位点前进到新值，行数跟着涨（insertEvents 产的是 id 1..N，
	// 所以第二批要比第一批大，多出来的才是新行）。
	if err := s.Apply(ctx, insertEvents(table, 5, "0/CC"), testPosition{"postgres:v1:0/CC"}); err != nil {
		t.Fatalf("第二批写入失败: %v", err)
	}
	pos3, err := s.Position(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if pos3 != "postgres:v1:0/CC" {
		t.Fatalf("位点没有前进到 postgres:v1:0/CC，实际 %q", pos3)
	}
	if n3, _ := s.Count(ctx, ref); n3 != 5 {
		t.Fatalf("两批之后应有 5 行，实际 %d", n3)
	}
	t.Logf("位点：0/AA →（坏批失败，不动）→ 0/CC；行数 3 → 3 → 5")
}
