package engine

import (
	"context"
	"testing"

	"github.com/spencerswagger/jinliang/internal/mysqlsource"
	"github.com/spencerswagger/jinliang/internal/pgsource"
	"github.com/spencerswagger/jinliang/internal/source"
)

// refMappedSink 必须把事件里的表名换成目标端的，而且**同一版 Relation 复用同一个指针**。
//
// 为什么这两条都要钉住：
//   - 不换表名：目标端报「收到未知表 orders 的变更（发布里多了表？）」，文案把人引向
//     源端配置，实际是引擎没做映射（MySQL 源的 schema 是库名，PG 目标是 public）；
//   - 每批新建 Relation：契约规定「指针相等即同一版」，目标端会以为源端每批都在改表结构。
func TestRefMappedSinkRewritesTableAndKeepsRelationIdentity(t *testing.T) {
	inner := &recordingSink{}
	mapped := newRefMappedSink(inner, func(ref source.TableRef) source.TableRef {
		return source.TableRef{Schema: "public", Table: ref.Table}
	})
	rel := &source.Table{TableRef: source.TableRef{Schema: "jinliang_src", Table: "orders"}}
	evs := []source.Event{
		{Kind: source.EvBegin},
		{Kind: source.EvInsert,
			Table:    source.TableRef{Schema: "jinliang_src", Table: "orders"},
			New:      source.Row{Table: source.TableRef{Schema: "jinliang_src", Table: "orders"}},
			Relation: rel},
		{Kind: source.EvCommit},
	}
	pos, _ := pgsource.ParsePayload("0/100")
	if err := mapped.Apply(context.Background(), evs, pos); err != nil {
		t.Fatalf("Apply 失败: %v", err)
	}
	if len(inner.evs) != 3 {
		t.Fatalf("应转发 3 条事件，实际 %d", len(inner.evs))
	}
	if got := inner.evs[1].Table; got.Schema != "public" || got.Table != "orders" {
		t.Fatalf("表引用应换成目标端（public.orders），实际 %v", got)
	}
	if got := inner.evs[1].New.Table; got.Schema != "public" {
		t.Fatalf("行里的表引用也要换，实际 %v", got)
	}
	if got := inner.evs[1].Relation.TableRef; got.Schema != "public" {
		t.Fatalf("Relation 的表引用也要换，实际 %v", got)
	}

	// 第二批发同一版 Relation：必须是同一个指针（契约：指针相等即同一版）。
	if err := mapped.Apply(context.Background(), evs, pos); err != nil {
		t.Fatalf("第二批失败: %v", err)
	}
	first := inner.evs[1].Relation
	second := inner.evs[4].Relation
	if first != second {
		t.Fatal("同一版 Relation 必须复用同一个指针，否则目标端会以为每批都在改表结构")
	}
	// 原始事件不能被改写（调用方可能还要用）。
	if evs[1].Table.Schema != "jinliang_src" {
		t.Fatalf("不该改写调用方的事件切片，实际 %v", evs[1].Table)
	}
}

type recordingSink struct {
	evs  []source.Event
	poss []string
}

func (r *recordingSink) Apply(_ context.Context, evs []source.Event, pos source.Position) error {
	r.evs = append(r.evs, evs...)
	if pos != nil {
		r.poss = append(r.poss, pos.String())
	}
	return nil
}

// 位点顺序由连接器自己给：MySQL 的序是「文件名数字段 → 偏移」，跨文件时按文件号比。
// 引擎的位点回退保护与追平判断全靠它（契约只保证可判等）。
func TestMyRunSourceOrdersBinlogPositions(t *testing.T) {
	src := myRunSource{}
	earlier := mysqlsource.Position{File: "mysql-bin.000009", Pos: 900}
	later := mysqlsource.Position{File: "mysql-bin.000010", Pos: 4}
	sameFileSmall := mysqlsource.Position{File: "mysql-bin.000009", Pos: 100}

	if !src.Before(earlier, later) {
		t.Fatal("跨文件时 .000009 必须早于 .000010")
	}
	if !src.Before(sameFileSmall, earlier) {
		t.Fatal("同一文件里偏移小的更早")
	}
	if src.Before(earlier, earlier) {
		t.Fatal("同一个位点不算更早（否则回退保护会失效）")
	}
	if src.Before(later, earlier) {
		t.Fatal("更晚的位点不能被判成更早")
	}
	// 外来位点（PG 的）不参与比较：保守返回 false，宁可不动位点。
	pg, _ := pgsource.ParsePayload("0/16B6C50")
	if src.Before(earlier, pg) || src.Before(pg, earlier) {
		t.Fatal("跨类型的位点不该比出先后（保守返回 false）")
	}
	if !src.IsZero(nil) || !src.IsZero(mysqlsource.Position{}) {
		t.Fatal("空位点必须被判成 zero")
	}
	if src.IsZero(earlier) {
		t.Fatal("有内容的位点不该被判成 zero")
	}
}

// MySQL 源没有「导出的快照名」：引擎不能拿 PG 那条判据（没快照名 = 失败）卡它。
func TestMyRunSourceDoesNotRequireExportedSnapshot(t *testing.T) {
	if (myRunSource{}).RequiresExportedSnapshot() {
		t.Fatal("MySQL 源的一致性视图在同一个连接的事务里，没有快照名")
	}
}
