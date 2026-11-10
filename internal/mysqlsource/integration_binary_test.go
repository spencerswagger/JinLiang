//go:build integration

// 二进制列 vs 字符列：**只能靠排序规则分**（契约 docs/TYPE-MAPPING.md）。
//
// 为什么必须在真库上验：判据是 TABLE_MAP 的排序规则（`binary` = id 63），
// 而 `BINARY(4)='abcd'` 这种「内容恰好是合法 UTF-8」的列，用内容启发式永远判错——
// 判错的后果不是显示难看，而是 pgsink 侧走 `$n::text::bytea`：
// 值以反斜杠开头时 PG 会把它当十六进制**静默改写**（不报错、数据已变）。
package mysqlsource

import (
	"context"
	"testing"
	"time"

	"github.com/spencerswagger/jinliang/internal/source"
)

func TestIntegrationBinaryVsTextDecoding(t *testing.T) {
	s := openSource(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	table := "jl_it_bintext"
	exec(t, s, "DROP TABLE IF EXISTS "+quoteIdent(table))
	exec(t, s, "CREATE TABLE "+quoteIdent(table)+` (
		id  INT NOT NULL PRIMARY KEY,
		b4  BINARY(4)     NULL,
		vb  VARBINARY(8)  NULL,
		bl  BLOB          NULL,
		c10 CHAR(10)      NULL,
		v16 VARCHAR(16)   NULL,
		tx  TEXT          NULL
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`)

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
	go func() { _ = s.RunStream(streamCtx, start, asm) }()
	time.Sleep(700 * time.Millisecond)

	// 两行：一行内容是可打印 ASCII（内容启发式最容易判错的那种），
	// 一行是反斜杠开头的二进制（pgsink 侧会被 `::text::bytea` 静默改写的那种）。
	exec(t, s, "INSERT INTO "+quoteIdent(table)+
		" (id,b4,vb,bl,c10,v16,tx) VALUES (1,'abcd','abcd','abcd','中文','hello','文本')")
	exec(t, s, "INSERT INTO "+quoteIdent(table)+
		" (id,b4,vb,bl,c10,v16,tx) VALUES (2,X'5C313233',X'5C313233',X'00FF','ab','xyz','t')")

	got := map[int][]source.Value{}
	var rel *source.Table
	deadline := time.After(30 * time.Second)
	for len(got) < 2 {
		select {
		case b := <-batches:
			for _, ev := range b.Events {
				if ev.Table.Table != table || ev.Kind != source.EvInsert {
					continue
				}
				if ev.Relation != nil {
					rel = ev.Relation
				}
				got[int(ev.New.Values[0].I)] = append([]source.Value(nil), ev.New.Values...)
			}
		case <-deadline:
			t.Fatalf("30s 内没读到两行（拿到 %d 行：%v）", len(got), keysOf(got))
		}
	}

	// ── 判据①：按字节存的列必须交 KindBytes，哪怕内容看起来是纯 ASCII ──
	for _, c := range []struct {
		name string
		idx  int
	}{{"BINARY(4)", 1}, {"VARBINARY(8)", 2}, {"BLOB", 3}} {
		v := got[1][c.idx]
		if v.Kind != source.KindBytes {
			t.Fatalf("%s（第 %d 列）应按字节交，实际 Kind=%v 值=%q。"+
				"内容启发式会把 'abcd' 判成字符串，进而 pgsink 走 ::text::bytea 静默改写", c.name, c.idx+1, v.Kind, v.S)
		}
		if string(v.B) != "abcd" {
			t.Fatalf("%s 的内容不该被改写：%q", c.name, string(v.B))
		}
	}
	// 反斜杠开头的那一行同样是字节（这条就是被静默改写的那个形态）。
	if v := got[2][1]; v.Kind != source.KindBytes || len(v.B) != 4 || v.B[0] != 0x5C {
		t.Fatalf("BINARY(4)=X'5C313233' 应按字节原样交，实际 Kind=%v 值=%q", v.Kind, v.B)
	}
	if v := got[2][3]; v.Kind != source.KindBytes || len(v.B) != 2 || v.B[0] != 0x00 {
		t.Fatalf("BLOB=X'00FF' 应按字节原样交，实际 Kind=%v 值=%q", v.Kind, v.B)
	}

	// ── 判据②：按字符存的列必须交 KindString ──
	for _, c := range []struct {
		name string
		idx  int
		want string
	}{{"CHAR(10)", 4, "中文"}, {"VARCHAR(16)", 5, "hello"}, {"TEXT", 6, "文本"}} {
		v := got[1][c.idx]
		if v.Kind != source.KindString || v.S != c.want {
			t.Fatalf("%s（第 %d 列）应按字符交 %q，实际 Kind=%v 值=%q", c.name, c.idx+1, c.want, v.Kind, v.S)
		}
	}

	// ── 判据③：类型串也必须分开（否则「改类型」判定会把 binary↔char 看漏）──
	if rel == nil {
		t.Fatal("事件没带 Relation：类型串没得验")
	}
	want := []string{"int", "binary(4)", "varbinary(8)", "blob", "char(10)", "varchar(16)", "text"}
	for i, w := range want {
		if i >= len(rel.Columns) {
			t.Fatalf("Relation 只有 %d 列，缺第 %d 列（%s）", len(rel.Columns), i+1, w)
		}
		if rel.Columns[i].Type != w {
			t.Fatalf("第 %d 列类型应是 %q，实际 %q（BINARY/VARBINARY/BLOB 与 CHAR/VARCHAR/TEXT "+
				"共用 binlog 类型字节，只有排序规则能分）", i+1, w, rel.Columns[i].Type)
		}
	}
	t.Logf("二进制/字符判定：%v", func() []string {
		var out []string
		for _, c := range rel.Columns {
			out = append(out, c.Type)
		}
		return out
	}())
}

// keysOf 只为报错时看得见拿到哪几行。
func keysOf(m map[int][]source.Value) []int {
	out := make([]int, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
