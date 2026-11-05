package mysqlsource

import (
	"context"
	"testing"
	"time"

	"github.com/spencerswagger/jinliang/internal/source"
)

var txSID = [16]byte{0x3e, 0x11, 0xfa, 0x47, 0x71, 0xca, 0x11, 0xe1, 0x9e, 0x33, 0xc8, 0x0a, 0xa9, 0x42, 0x95, 0x62}

func feedTxn(t *testing.T, a *Assembler, gno int64, rows int, commit bool) error {
	t.Helper()
	ctx := context.Background()
	steps := []*Event{
		{Header: Header{Type: evGTID, LogPos: 100}, File: "mysql-bin.000001", GTID: &GTIDEvent{SID: txSID, GNO: gno}},
		{Header: Header{Type: evQuery, LogPos: 120}, File: "mysql-bin.000001", Query: &QueryEvent{Schema: "s", Query: "BEGIN"}},
		{Header: Header{Type: evTableMap, LogPos: 140}, File: "mysql-bin.000001", TableMap: &TableMap{TableID: 1, Schema: "s", Table: "t",
			Columns: 1, Types: []byte{typeLong}, Meta: []uint16{0}, Nullable: []bool{false}, Names: []string{"id"}, Unsigned: []bool{false}}},
	}
	for i := 0; i < rows; i++ {
		steps = append(steps, &Event{Header: Header{Type: evWriteRowsV2, LogPos: uint32(160 + i)}, File: "mysql-bin.000001",
			Rows: &RowsEvent{Table: steps[2].TableMap, Version: 2, Changes: []Change{{
				Kind: source.EvInsert,
				New: Image{
					Row:     source.Row{Table: source.TableRef{Schema: "s", Table: "t"}, Values: []source.Value{{Kind: source.KindInt, I: int64(i)}}},
					Present: []bool{true},
				},
			}}}})
	}
	if commit {
		steps = append(steps, &Event{Header: Header{Type: evXID, LogPos: 400}, File: "mysql-bin.000001", XID: &XIDEvent{XID: 1}})
	}
	for _, ev := range steps {
		if err := a.Feed(ctx, ev); err != nil {
			return err
		}
	}
	return nil
}

func TestAssemblerEmitsWholeTransactionOnly(t *testing.T) {
	var got []Tx
	a := NewAssembler(func(ctx context.Context, tx Tx) error {
		got = append(got, tx)
		return nil
	})
	if err := feedTxn(t, a, 5, 3, true); err != nil {
		t.Fatalf("喂事件失败: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("应只交付 1 个事务，拿到 %d", len(got))
	}
	if len(got[0].Events) != 3 {
		t.Fatalf("事务里应有 3 条变更，拿到 %d", len(got[0].Events))
	}
	if got[0].ID != "3e11fa47-71ca-11e1-9e33-c80aa9429562:5" {
		t.Fatalf("事务号不对: %q", got[0].ID)
	}
	if got[0].Commit.Pos != 400 {
		t.Fatalf("提交位点应是提交事件的结束位置，拿到 %d", got[0].Commit.Pos)
	}
	if !got[0].Commit.GTID.Contains(mustSet(t, "3e11fa47-71ca-11e1-9e33-c80aa9429562:5")) {
		t.Fatalf("提交位点里应包含本事务的事务号: %q", got[0].Commit.GTID.String())
	}
}

func TestAssemblerWithholdsUncommittedTransaction(t *testing.T) {
	var got []Tx
	a := NewAssembler(func(ctx context.Context, tx Tx) error {
		got = append(got, tx)
		return nil
	})
	if err := feedTxn(t, a, 6, 2, false); err != nil { // 没有 XID
		t.Fatalf("喂事件失败: %v", err)
	}

	if len(got) != 0 {
		t.Fatalf("没有提交的事务一条都不许交付，拿到了 %d 个", len(got))
	}
	open, id, n := a.Flush()
	if !open || n != 2 {
		t.Fatalf("应当处于「事务未提交」状态: open=%v id=%q events=%d", open, id, n)
	}
	if !a.Position().IsZero() && a.Position().Pos >= 400 {
		t.Fatalf("未提交的事务不能推进位点: %#v", a.Position())
	}
}

func TestAssemblerRollbackDiscardsEverything(t *testing.T) {
	var got []Tx
	a := NewAssembler(func(ctx context.Context, tx Tx) error {
		got = append(got, tx)
		return nil
	})
	if err := feedTxn(t, a, 7, 2, false); err != nil {
		t.Fatalf("喂事件失败: %v", err)
	}
	if err := a.Feed(context.Background(), &Event{Header: Header{Type: evQuery, LogPos: 300},
		File: "mysql-bin.000001", Query: &QueryEvent{Schema: "s", Query: "ROLLBACK"}}); err != nil {
		t.Fatalf("回滚: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("源端回滚了，下游一条都不该看到，拿到 %d", len(got))
	}
	open, _, n := a.Flush()
	if open || n != 0 {
		t.Fatalf("回滚后缓冲应清空: open=%v events=%d", open, n)
	}
}

func TestAssemblerRejectsRowsOutsideTransaction(t *testing.T) {
	a := NewAssembler(func(ctx context.Context, tx Tx) error { return nil })
	tm := &TableMap{TableID: 1, Schema: "s", Table: "t", Columns: 1, Types: []byte{typeLong}}
	err := a.Feed(context.Background(), &Event{Header: Header{Type: evWriteRowsV2, LogPos: 10}, File: "mysql-bin.000001",
		Rows: &RowsEvent{Table: tm, Version: 2, Changes: []Change{{Kind: source.EvInsert,
			New: Image{Row: source.Row{Values: []source.Value{{Kind: source.KindInt, I: 1}}}, Present: []bool{true}}}}}})
	if err == nil {
		t.Fatal("事务外的行变更必须报错（否则会半截应用）")
	}
}

func TestAssemblerRejectsPartialImage(t *testing.T) {
	a := NewAssembler(func(ctx context.Context, tx Tx) error { return nil })
	tm := &TableMap{TableID: 1, Schema: "s", Table: "t", Columns: 2, Types: []byte{typeLong, typeVarchar}, Names: []string{"id", "note"}}
	feed := func(ev *Event) error { return a.Feed(context.Background(), ev) }
	if err := feed(&Event{Header: Header{Type: evGTID, LogPos: 10}, File: "f", GTID: &GTIDEvent{SID: txSID, GNO: 9}}); err != nil {
		t.Fatal(err)
	}
	if err := feed(&Event{Header: Header{Type: evQuery, LogPos: 20}, File: "f", Query: &QueryEvent{Query: "BEGIN"}}); err != nil {
		t.Fatal(err)
	}
	err := feed(&Event{Header: Header{Type: evUpdateRowsV2, LogPos: 30}, File: "f", Rows: &RowsEvent{Table: tm, Version: 2,
		Changes: []Change{{Kind: source.EvUpdate,
			Old: Image{Row: source.Row{Values: []source.Value{{Kind: source.KindInt, I: 1}}}, Present: []bool{true, false}},
			New: Image{Row: source.Row{Values: []source.Value{{Kind: source.KindInt, I: 1}}}, Present: []bool{true, false}}}}}})
	if err == nil {
		t.Fatal("binlog_row_image 不是 FULL 时必须报错，不能把缺列当成 NULL 写下去")
	}
	if !contains(err.Error(), "binlog_row_image") {
		t.Fatalf("错误信息要指出 binlog_row_image: %v", err)
	}
}

func TestAssemblerEmptyTransactionDoesNotEmitBatch(t *testing.T) {
	var got []Tx
	a := NewAssembler(func(ctx context.Context, tx Tx) error {
		got = append(got, tx)
		return nil
	})
	if err := feedTxn(t, a, 8, 0, true); err != nil {
		t.Fatalf("喂事件失败: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("空事务不该产生批次，拿到 %d", len(got))
	}
	if a.Position().Pos != 400 {
		t.Fatalf("空事务也要推进位点，拿到 %d", a.Position().Pos)
	}
}

func TestAssemblerHandlerErrorKeepsCheckpoint(t *testing.T) {
	boom := errorString("target down")
	var calls int
	a := NewAssembler(func(ctx context.Context, tx Tx) error {
		calls++
		return boom
	})
	if err := feedTxn(t, a, 10, 1, true); err == nil {
		t.Fatal("下游写失败时，Assembler 必须把错误抛出去，不能假装成功")
	}
	if calls != 1 {
		t.Fatalf("处理器应被调用一次，拿到 %d", calls)
	}
	if a.Position().Pos >= 400 {
		t.Fatalf("下游写失败时位点不能前进，否则会丢事务: %#v", a.Position())
	}
}

func TestAssemblerDDLOutsideTransaction(t *testing.T) {
	var got []Tx
	a := NewAssembler(func(ctx context.Context, tx Tx) error {
		got = append(got, tx)
		return nil
	})
	err := a.Feed(context.Background(), &Event{Header: Header{Type: evQuery, LogPos: 50, Timestamp: 1_700_000_000},
		File: "mysql-bin.000001", Query: &QueryEvent{Schema: "s", Query: "CREATE TABLE x (id int)"}})
	if err != nil {
		t.Fatalf("独立 DDL: %v", err)
	}
	if len(got) != 1 || got[0].Events[0].Kind != source.EvDDL {
		t.Fatalf("独立 DDL 应作为一个单事件事务交付: %#v", got)
	}
}

func TestAssemblerHeartbeatDoesNotOpenTransaction(t *testing.T) {
	var beats int
	a := NewAssembler(func(ctx context.Context, tx Tx) error { return nil })
	a.OnHeartbeat = func(hb Heartbeat, at time.Time) { beats++ }
	if err := a.Feed(context.Background(), &Event{Header: Header{Type: evHeartbeatV2}, File: "f",
		Heartbeat: &Heartbeat{File: "mysql-bin.000001", Pos: 4096}}); err != nil {
		t.Fatal(err)
	}
	if beats != 1 {
		t.Fatalf("心跳应回调一次，拿到 %d", beats)
	}
	if open, _, _ := a.Flush(); open {
		t.Fatal("心跳不能开事务")
	}
}

func mustSet(t *testing.T, s string) GTIDSet {
	t.Helper()
	g, err := ParseGTIDSet(s)
	if err != nil {
		t.Fatalf("%q: %v", s, err)
	}
	return g
}

type errorString string

func (e errorString) Error() string { return string(e) }
