//go:build integration

// 集成测试：需要真 MySQL。
//
// 缺 DSN 时**直接失败**（不 skip）：静默跳过会让「矩阵全绿」变成假象，
// 而这条链是数据面最容易悄悄退化的地方。
package mysqlsource

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/spencerswagger/jinliang/internal/source"
)

func requireDSN(t *testing.T) string {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv("JINLIANG_TEST_MYSQL_SRC_DSN"))
	if raw == "" {
		t.Fatal("缺少 JINLIANG_TEST_MYSQL_SRC_DSN：集成测试必须跑在真库上，" +
			"没有 DSN 就是失败，不是跳过（矩阵见 .github/workflows/mysql-matrix.yml）")
	}
	return raw
}

func openSource(t *testing.T) *Source {
	t.Helper()
	s, err := Open(context.Background(), requireDSN(t))
	if err != nil {
		t.Fatalf("连接源库失败: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestIntegrationProbe(t *testing.T) {
	s := openSource(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	res, err := s.ProbeDetail(ctx)
	if err != nil {
		t.Fatalf("探测失败: %v", err)
	}
	t.Logf("源库 %s（%s），位点语句 %q", res.Server.Version, res.Server.Flavor, res.StatusStatement)
	if res.StatusStatement == "" {
		t.Fatal("读不出位点语句")
	}
	if !res.Caps[source.CapCDC].Supported {
		t.Fatalf("测试库必须开 log_bin + ROW 才能跑后面几条: %#v", res.Caps[source.CapCDC])
	}
	for _, ch := range res.Checks {
		if ch.Status == source.StatusBlocked {
			t.Logf("预检 blocked：%s — %s", ch.ID, ch.Detail)
		}
	}
}

func TestIntegrationStatusRoundTrip(t *testing.T) {
	s := openSource(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pos, err := s.Status(ctx)
	if err != nil {
		t.Fatalf("取位点失败: %v", err)
	}
	if pos.File == "" || pos.Pos == 0 {
		t.Fatalf("位点不完整: %#v", pos)
	}
	if pos.TakenAt.IsZero() {
		t.Fatal("位点必须带取的那一刻")
	}
	if pos.TakenAt.Nanosecond()%int(time.Millisecond) != 0 {
		t.Fatalf("取位点时间应截到毫秒，拿到 %s", pos.TakenAt)
	}

	// 写 → 读 → 恢复：走一遍落库时的字符串往返。
	back, err := ParsePosition(pos.String())
	if err != nil {
		t.Fatalf("回读位点失败: %v", err)
	}
	if back.File != pos.File || back.Pos != pos.Pos {
		t.Fatalf("文件名+偏移往返不一致: %#v vs %#v", back, pos)
	}
	if !back.GTID.Equal(pos.GTID) {
		t.Fatalf("GTID 往返不一致: %q vs %q", back.GTID.String(), pos.GTID.String())
	}
	if back.TakenAt.UnixMilli() != pos.TakenAt.UnixMilli() {
		t.Fatalf("取位点毫秒往返不一致")
	}
}

func TestIntegrationSnapshotChunked(t *testing.T) {
	s := openSource(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	table := "jl_it_snapshot"
	exec(t, s, "DROP TABLE IF EXISTS "+quoteIdent(table))
	exec(t, s, "CREATE TABLE "+quoteIdent(table)+" (id INT PRIMARY KEY, note VARCHAR(32) NULL)")
	for i := 1; i <= 7; i++ {
		exec(t, s, "INSERT INTO "+quoteIdent(table)+" (id, note) VALUES (?, ?)", i, nil)
	}
	exec(t, s, "INSERT INTO "+quoteIdent(table)+" (id, note) VALUES (8, 'eight')")

	var rows int
	var sawText bool
	pos, err := s.Snapshot(ctx, source.SnapshotOpt{ChunkRows: 3}, func(ctx context.Context, row source.Row) error {
		if row.Table.Table != table {
			return nil
		}
		rows++
		if v := row.Values[1]; v.Kind == source.KindString && v.S == "eight" {
			sawText = true
		}
		return nil
	})
	if err != nil {
		t.Fatalf("全量读取失败: %v", err)
	}
	if rows != 8 {
		t.Fatalf("应读到 8 行，拿到 %d（分块游标可能漏行/重复）", rows)
	}
	if !sawText {
		t.Fatal("VARCHAR 列没读出来")
	}
	mp, ok := pos.(Position)
	if !ok || mp.File == "" {
		t.Fatalf("全量结束必须交出增量起点: %#v", pos)
	}
	if note := s.LastSnapshotNote(); note.Detail == "" {
		t.Fatal("取一致性点的方式必须可说明（锁没锁、代价是什么）")
	}
	exec(t, s, "DROP TABLE IF EXISTS "+quoteIdent(table))
}

func TestIntegrationStreamDeliversWholeTransactions(t *testing.T) {
	s := openSource(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	table := "jl_it_stream"
	exec(t, s, "DROP TABLE IF EXISTS "+quoteIdent(table))
	exec(t, s, "CREATE TABLE "+quoteIdent(table)+" (id INT PRIMARY KEY, note VARCHAR(32))")

	start, err := s.Status(ctx)
	if err != nil {
		t.Fatalf("取起点失败: %v", err)
	}

	batches := make(chan Tx, 16)
	asm := NewAssembler(func(ctx context.Context, tx Tx) error {
		batches <- tx
		return nil
	})
	streamCtx, stop := context.WithCancel(ctx)
	defer stop()
	streamErr := make(chan error, 1)
	go func() { streamErr <- s.RunStream(streamCtx, start, asm) }()

	// 等取流连上再写数据：一条单行事务 + 一个 4 行事务，用来验「整段交付」。
	time.Sleep(700 * time.Millisecond)
	exec(t, s, "INSERT INTO "+quoteIdent(table)+" (id, note) VALUES (1, 'one')")
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("开事务: %v", err)
	}
	for i := 2; i <= 5; i++ {
		if _, err := tx.ExecContext(ctx, "INSERT INTO "+quoteIdent(table)+" (id, note) VALUES (?, ?)", i, "many"); err != nil {
			t.Fatalf("事务内写入: %v", err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("提交: %v", err)
	}

	counts := map[string]int{}
	deadline := time.After(30 * time.Second)
	for len(counts) < 2 {
		select {
		case b := <-batches:
			want := 0
			for _, ev := range b.Events {
				if ev.Table.Table != table {
					continue
				}
				want++
				if ev.Kind != source.EvInsert {
					t.Fatalf("期望插入事件，拿到 %v", ev.Kind)
				}
			}
			if want > 0 {
				counts[b.ID] = want
			}
			if b.Commit.File == "" || b.Commit.Pos == 0 {
				t.Fatalf("提交位点不完整: %#v", b.Commit)
			}
			// 真库上的批次也要能被契约的机械校验放行（EvBegin/EvCommit 配对）。
			if err := source.ValidateBatch(b.Events); err != nil {
				t.Fatalf("真库批次没通过 ValidateBatch: %v", err)
			}
		case err := <-streamErr:
			t.Fatalf("取流提前结束: %v", err)
		case <-deadline:
			t.Fatalf("30 秒内没收到两个事务，收到=%v", counts)
		}
	}

	var sawFour bool
	for id, n := range counts {
		t.Logf("事务 %q 交付 %d 行", id, n)
		if n == 4 {
			sawFour = true
		}
		if n == 0 {
			t.Fatalf("事务 %s 没有事件却被交付了", id)
		}
	}
	if !sawFour {
		t.Fatalf("4 行的源事务必须整段交付（不能拆成多个批次），实际=%v", counts)
	}
	exec(t, s, "DROP TABLE IF EXISTS "+quoteIdent(table))
}

// TestIntegrationRestartFromSavedPosition 是「崩溃恢复」的连接器级验证：
// 拿到位点之后不消费，继续写数据，然后从那个位点重新起流 —— 期间提交的
// 事务必须能完整重放出来（这就是进程被杀后重启要走的路）。
func TestIntegrationRestartFromSavedPosition(t *testing.T) {
	s := openSource(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	table := "jl_it_restart"
	exec(t, s, "DROP TABLE IF EXISTS "+quoteIdent(table))
	exec(t, s, "CREATE TABLE "+quoteIdent(table)+" (id INT PRIMARY KEY)")

	checkpoint, err := s.Status(ctx)
	if err != nil {
		t.Fatalf("取位点失败: %v", err)
	}
	// 模拟「位点已落库、进程随后被杀」：位点之后才发生的事务。
	exec(t, s, "INSERT INTO "+quoteIdent(table)+" (id) VALUES (101)")
	exec(t, s, "INSERT INTO "+quoteIdent(table)+" (id) VALUES (102)")

	// 按落库的字符串恢复（真实重启也是读这一串）。
	saved, err := ParsePosition(checkpoint.String())
	if err != nil {
		t.Fatalf("回读位点失败: %v", err)
	}

	batches := make(chan Tx, 8)
	asm := NewAssembler(func(ctx context.Context, tx Tx) error {
		batches <- tx
		return nil
	})
	streamCtx, stop := context.WithCancel(ctx)
	defer stop()
	go func() { _ = s.RunStream(streamCtx, saved, asm) }()

	got := map[int64]bool{}
	deadline := time.After(30 * time.Second)
	for len(got) < 2 {
		select {
		case b := <-batches:
			for _, ev := range b.Events {
				if ev.Table.Table != table || len(ev.New.Values) == 0 {
					continue
				}
				if v := ev.New.Values[0]; v.Kind == source.KindInt {
					got[v.I] = true
				}
			}
		case <-deadline:
			t.Fatalf("从位点恢复后没重放到预期事务，收到=%v", got)
		}
	}
	if !got[101] || !got[102] {
		t.Fatalf("位点之后的两笔事务必须都能重放，收到=%v", got)
	}
	exec(t, s, "DROP TABLE IF EXISTS "+quoteIdent(table))
}

// 边界 1：无主键表。最常见翻车点——不能只读第一块，也不能静默漏行。
func TestIntegrationSnapshotNoPrimaryKeyReadsEverything(t *testing.T) {
	s := openSource(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	table := "jl_it_nopk"
	exec(t, s, "DROP TABLE IF EXISTS "+quoteIdent(table))
	exec(t, s, "CREATE TABLE "+quoteIdent(table)+" (note VARCHAR(16), n INT)")
	for i := 1; i <= 7; i++ {
		exec(t, s, "INSERT INTO "+quoteIdent(table)+" (note, n) VALUES (?, ?)", fmt.Sprintf("r%d", i), i)
	}

	var got int
	_, err := s.Snapshot(ctx, source.SnapshotOpt{ChunkRows: 2}, func(ctx context.Context, row source.Row) error {
		if row.Table.Table == table {
			got++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("全量读取失败: %v", err)
	}
	if got != 7 {
		t.Fatalf("无主键表必须整表扫完：应读 7 行，实际 %d 行（少了就是静默漏行）", got)
	}
	note := s.LastSnapshotNote()
	found := false
	for _, name := range note.NoPKTables {
		if strings.HasSuffix(name, "."+table) {
			found = true
		}
	}
	if !found {
		t.Fatalf("无主键表必须在 SnapshotNote 里如实报出来（只有表级断点），实际=%#v", note)
	}
	exec(t, s, "DROP TABLE IF EXISTS "+quoteIdent(table))
}

// 边界 2：复合主键。keyset 游标要用 (a,b) > (?,?) 逐块推进，跨块不能重不能漏。
func TestIntegrationSnapshotCompositePrimaryKey(t *testing.T) {
	s := openSource(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	table := "jl_it_composite"
	exec(t, s, "DROP TABLE IF EXISTS "+quoteIdent(table))
	exec(t, s, "CREATE TABLE "+quoteIdent(table)+" (a INT, b VARCHAR(8), v INT, PRIMARY KEY (a, b))")
	for a := 1; a <= 2; a++ {
		for _, b := range []string{"a", "b", "c"} {
			exec(t, s, "INSERT INTO "+quoteIdent(table)+" (a, b, v) VALUES (?, ?, ?)", a, b, a*10)
		}
	}

	seen := map[string]bool{}
	_, err := s.Snapshot(ctx, source.SnapshotOpt{ChunkRows: 2}, func(ctx context.Context, row source.Row) error {
		if row.Table.Table != table {
			return nil
		}
		key := fmt.Sprintf("%v/%v", row.Values[0].I, row.Values[1].S)
		if seen[key] {
			t.Fatalf("复合主键翻页出现重复行: %s", key)
		}
		seen[key] = true
		return nil
	})
	if err != nil {
		t.Fatalf("全量读取失败: %v", err)
	}
	if len(seen) != 6 {
		t.Fatalf("复合主键应读到 6 行（3 块），实际 %d: %v", len(seen), seen)
	}
	exec(t, s, "DROP TABLE IF EXISTS "+quoteIdent(table))
}

// 边界 3：稀疏/大偏移主键（如 1 和 10^12）。keyset 不用 OFFSET，所以不应衰减也不应漏。
func TestIntegrationSnapshotSparseLargeKeys(t *testing.T) {
	s := openSource(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	table := "jl_it_sparse"
	exec(t, s, "DROP TABLE IF EXISTS "+quoteIdent(table))
	exec(t, s, "CREATE TABLE "+quoteIdent(table)+" (id BIGINT PRIMARY KEY, v INT)")
	ids := []int64{1, 2, 1_000_000_000_000, 1_000_000_000_005, 9_000_000_000_000_000}
	for _, id := range ids {
		exec(t, s, "INSERT INTO "+quoteIdent(table)+" (id, v) VALUES (?, ?)", id, 1)
	}

	seen := map[int64]bool{}
	_, err := s.Snapshot(ctx, source.SnapshotOpt{ChunkRows: 2}, func(ctx context.Context, row source.Row) error {
		if row.Table.Table != table {
			return nil
		}
		seen[row.Values[0].I] = true
		return nil
	})
	if err != nil {
		t.Fatalf("全量读取失败: %v", err)
	}
	if len(seen) != len(ids) {
		t.Fatalf("稀疏大主键应读到 %d 行，实际 %d: %v", len(ids), len(seen), seen)
	}
	for _, id := range ids {
		if !seen[id] {
			t.Fatalf("大偏移下漏了主键 %d", id)
		}
	}
	exec(t, s, "DROP TABLE IF EXISTS "+quoteIdent(table))
}

func exec(t *testing.T, s *Source, query string, args ...any) {
	t.Helper()
	if _, err := s.db.ExecContext(context.Background(), query, args...); err != nil {
		t.Fatalf("执行 %q 失败: %v", query, err)
	}
}
