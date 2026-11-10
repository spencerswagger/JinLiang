//go:build integration

package mysqlsource

import (
	"context"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"github.com/spencerswagger/jinliang/internal/source"
)

// 从块边界续跑必须**不重不漏**：这是「静默少行」的唯一真库证据。
//
// 用 BINARY(16) 主键（UUID 存二进制的常见形态）：块边界一旦丢了类型、按文本去比，
// 二进制列会按字节比大小，续跑直接跳到表尾——行数少了、退出码还是 0。
// 这条用例在修复前是红的（续跑读到 0 行），修复后读到剩下的一半。
func TestIntegrationResumeFromBinaryPKCursor(t *testing.T) {
	s := openSource(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	const rows = 30
	exec(t, s, "DROP TABLE IF EXISTS bin_pk")
	exec(t, s, "CREATE TABLE bin_pk (id BINARY(16) NOT NULL PRIMARY KEY, payload VARCHAR(32))")
	t.Cleanup(func() { exec(t, s, "DROP TABLE IF EXISTS bin_pk") })
	// id = 前 14 字节是序号 + 末尾 0xFFFF：按字节序单调（方便核对续跑起点），
	// 而且**不是合法 UTF-8**——真实 UUID 存二进制时大多如此。合法 UTF-8 的二进制
	// 主键会「碰巧」按原样往返，测不出丢类型；这一条必须用非 UTF-8 的字节。
	for i := 0; i < rows; i++ {
		exec(t, s, fmt.Sprintf(
			"INSERT INTO bin_pk (id, payload) VALUES (UNHEX(CONCAT(LPAD('%x', 28, '0'), 'FFFF')), 'row-%d')", i, i))
	}

	table := binPKTable(t, s, ctx)
	s.SetKnownTables([]source.Table{table})

	opt := source.SnapshotOpt{ChunkRows: 5, Conns: 1}

	// 第一遍：从头读，记下第 2 块的右边界（= 续跑起点）。
	type chunkInfo struct {
		no   int
		to   string
		rows int
	}
	var first []chunkInfo
	if _, err := s.SnapshotChunksFrom(ctx, opt, nil, func(c context.Context, ch Chunk) error {
		first = append(first, chunkInfo{no: ch.No, to: ch.To, rows: len(ch.Rows)})
		return nil
	}); err != nil {
		t.Fatalf("第一遍分块读失败: %v", err)
	}
	if len(first) < 3 {
		t.Fatalf("30 行 / 每块 5 行应至少 6 块，实际 %d 块", len(first))
	}
	if first[0].rows != 5 || first[1].rows != 5 {
		t.Fatalf("每块应是 5 行，实际 %d/%d", first[0].rows, first[1].rows)
	}
	cut := first[1] // 第 2 块（块号 2）
	t.Logf("cut.to=%q rows=%d", cut.to, cut.rows)
	if cut.to == "" {
		t.Fatal("第 2 块必须带右边界（没有边界就没法续跑）")
	}

	// 第二遍：从第 2 块的右边界续跑，块号接着往上排。
	var resumed []chunkInfo
	var ids []string
	if _, err := s.SnapshotChunksFrom(ctx, opt,
		map[string]ChunkResume{"bin_pk": {Cursor: cut.to, LastNo: cut.no}},
		func(c context.Context, ch Chunk) error {
			resumed = append(resumed, chunkInfo{no: ch.No, to: ch.To, rows: len(ch.Rows)})
			for _, r := range ch.Rows {
				ids = append(ids, idHex(r[0]))
			}
			return nil
		}); err != nil {
		t.Fatalf("续跑失败: %v", err)
	}

	want := rows - cut.no*5 // 已提交 cut.no 块，每块 5 行
	if len(ids) != want {
		t.Fatalf("续跑应读 %d 行（%d 行 - 已提交 %d 块×5），实际 %d 行——"+
			"少读就是静默丢数据（块边界丢类型 → 二进制列按文本比 → 跳到表尾）",
			want, rows, cut.no, len(ids))
	}
	// 续跑的第一行必须是第 10 条（0 起算），最后一行是第 29 条。
	if want := fmt.Sprintf("%028xffff", cut.no*5); ids[0] != want {
		t.Fatalf("续跑起点不对：应从 %s 开始，实际 %s", want, ids[0])
	}
	if want := fmt.Sprintf("%028xffff", rows-1); ids[len(ids)-1] != want {
		t.Fatalf("续跑终点不对：应到 %s，实际 %s", want, ids[len(ids)-1])
	}
	// 块号必须接着旧编号往上排（否则 _jl_chunk 主键会撞车、把旧块边界覆盖掉）。
	if resumed[0].no != cut.no+1 {
		t.Fatalf("续跑第一块的块号应是 %d，实际 %d", cut.no+1, resumed[0].no)
	}
}

// 时间主键同样要能续跑：DATETIME(6) 的边界必须还原成 MySQL 认的字面量（含微秒）。
func TestIntegrationResumeFromTimePKCursor(t *testing.T) {
	s := openSource(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	exec(t, s, "DROP TABLE IF EXISTS time_pk")
	exec(t, s, "CREATE TABLE time_pk (id DATETIME(6) NOT NULL PRIMARY KEY, payload VARCHAR(32))")
	t.Cleanup(func() { exec(t, s, "DROP TABLE IF EXISTS time_pk") })
	const rows = 12
	for i := 0; i < rows; i++ {
		exec(t, s, fmt.Sprintf(
			"INSERT INTO time_pk (id, payload) VALUES ('2026-11-01 10:00:%02d.%06d', 'row-%d')", i, i, i))
	}

	table := namedTable(t, s, ctx, "time_pk")
	s.SetKnownTables([]source.Table{table})

	opt := source.SnapshotOpt{ChunkRows: 4, Conns: 1}
	var to string
	var cut int
	if _, err := s.SnapshotChunksFrom(ctx, opt, nil, func(c context.Context, ch Chunk) error {
		if ch.No == 2 {
			to, cut = ch.To, ch.No
		}
		return nil
	}); err != nil {
		t.Fatalf("第一遍失败: %v", err)
	}
	if to == "" {
		t.Fatal("第 2 块必须带右边界")
	}
	var got int
	if _, err := s.SnapshotChunksFrom(ctx, opt,
		map[string]ChunkResume{"time_pk": {Cursor: to, LastNo: cut}},
		func(c context.Context, ch Chunk) error {
			got += len(ch.Rows)
			return nil
		}); err != nil {
		t.Fatalf("续跑失败（时间边界还原不回去时会报 1525 或截断到秒）: %v", err)
	}
	if want := rows - cut*4; got != want {
		t.Fatalf("续跑应读 %d 行，实际 %d 行", want, got)
	}
}

// idHex 把一列主键渲染成十六进制（驱动可能给 []byte 或 string，两种都认）。
func idHex(v any) string {
	switch x := v.(type) {
	case []byte:
		return hex.EncodeToString(x)
	case string:
		return hex.EncodeToString([]byte(x))
	}
	return fmt.Sprintf("%v", v)
}

// binPKTable 从 catalog 里挑出 bin_pk 的定义（列类型按 information_schema 的真实值）。
func binPKTable(t *testing.T, s *Source, ctx context.Context) source.Table {
	return namedTable(t, s, ctx, "bin_pk")
}

func namedTable(t *testing.T, s *Source, ctx context.Context, name string) source.Table {
	t.Helper()
	cat, err := s.Catalog(ctx, source.Scope{Schema: s.ep.Database})
	if err != nil {
		t.Fatalf("读 catalog 失败: %v", err)
	}
	for _, tb := range cat.Tables {
		if tb.Table == name {
			return tb
		}
	}
	t.Fatalf("catalog 里没有 %s", name)
	return source.Table{}
}
