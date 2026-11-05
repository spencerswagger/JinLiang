package mysqlsource

import (
	"context"
	"errors"
	"strings"
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
	data := dataEvents(got[0].Events)
	if len(data) != 3 {
		t.Fatalf("事务里应有 3 条变更，拿到 %d", len(data))
	}
	if got[0].Events[0].Kind != source.EvBegin {
		t.Fatalf("事务必须以 EvBegin 开头，拿到 %v", got[0].Events[0].Kind)
	}
	if last := got[0].Events[len(got[0].Events)-1]; last.Kind != source.EvCommit {
		t.Fatalf("事务必须以 EvCommit 结尾（这是「已提交」的唯一信号），拿到 %v", last.Kind)
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
	for _, tx := range got {
		for _, ev := range tx.Events {
			if ev.Kind == source.EvCommit {
				t.Fatal("源端回滚的事务绝不能带 EvCommit（没有 rollback 事件，不交付就是不提交）")
			}
		}
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
	if len(got) != 1 {
		t.Fatalf("独立 DDL 应作为一笔事务交付: %#v", got)
	}
	if got[0].Events[0].Kind != source.EvBegin || got[0].Events[len(got[0].Events)-1].Kind != source.EvCommit {
		t.Fatalf("DDL 也要有事务边界: %#v", got[0].Events)
	}
	data := dataEvents(got[0].Events)
	if len(data) != 1 || data[0].Kind != source.EvDDL {
		t.Fatalf("应有一条 DDL: %#v", data)
	}
	if data[0].TxID == "" {
		t.Fatal("DDL 事件必须带事务号（否则死信里认不出它属于谁）")
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

// dataEvents drops the transaction-boundary markers.
func dataEvents(evs []source.Event) []source.Event {
	out := make([]source.Event, 0, len(evs))
	for _, ev := range evs {
		if ev.Kind == source.EvBegin || ev.Kind == source.EvCommit {
			continue
		}
		out = append(out, ev)
	}
	return out
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

func TestAssemblerImplicitCommitOnDDL(t *testing.T) {
	// MySQL 的 DDL 会隐式提交当前事务，而且它自己那笔事务没有 XID。
	// 不先结掉已缓冲的行变更，就会一直等一个不会来的提交事件（9.x 矩阵炸出来的）。
	var got []Tx
	a := NewAssembler(func(ctx context.Context, tx Tx) error { got = append(got, tx); return nil })
	ctx := context.Background()
	tm := &TableMap{TableID: 1, Schema: "s", Table: "t", Columns: 1, Types: []byte{typeLong}, Names: []string{"id"}}

	if err := feedTxnPrefix(t, a, 11, tm, 1); err != nil {
		t.Fatalf("喂事件失败: %v", err)
	}
	// DDL 在事务中间到来：先隐式提交前面的行，再自成一件。
	if err := a.Feed(ctx, &Event{Header: Header{Type: evQuery, LogPos: 500, Timestamp: 1_700_000_000},
		File: "mysql-bin.000001", Query: &QueryEvent{Schema: "s", Query: "ALTER TABLE t ADD COLUMN x INT"}}); err != nil {
		t.Fatalf("DDL 不该报错: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("应交付 2 笔事务（行变更 + DDL），拿到 %d", len(got))
	}
	if dataEvents(got[0].Events)[0].Kind != source.EvInsert {
		t.Fatalf("第一笔应是行变更: %#v", got[0].Events)
	}
	if dataEvents(got[1].Events)[0].Kind != source.EvDDL {
		t.Fatalf("第二笔应是 DDL: %#v", got[1].Events)
	}
	if got[1].Events[len(got[1].Events)-1].Kind != source.EvCommit {
		t.Fatal("DDL 那笔也要以 EvCommit 结尾")
	}
}

func TestAssemblerAcceptsNewGTIDAfterEmptyOpenTransaction(t *testing.T) {
	// 从半截位置起流时可能只看到一个空 GTID 占位；下一个 GTID 到来不该判定「流不可信」。
	a := NewAssembler(func(ctx context.Context, tx Tx) error { return nil })
	ctx := context.Background()
	if err := a.Feed(ctx, &Event{Header: Header{Type: evGTID, LogPos: 10}, File: "f",
		GTID: &GTIDEvent{SID: txSID, GNO: 3}}); err != nil {
		t.Fatal(err)
	}
	if err := a.Feed(ctx, &Event{Header: Header{Type: evGTID, LogPos: 20}, File: "f",
		GTID: &GTIDEvent{SID: txSID, GNO: 4}}); err != nil {
		t.Fatalf("空事务之后的新 GTID 不该报错: %v", err)
	}
}

// feedTxnPrefix feeds GTID + BEGIN + TABLE_MAP + one insert, leaving the
// transaction open (no XID).
func feedTxnPrefix(t *testing.T, a *Assembler, gno int64, tm *TableMap, id int64) error {
	t.Helper()
	ctx := context.Background()
	steps := []*Event{
		{Header: Header{Type: evGTID, LogPos: 100}, File: "mysql-bin.000001", GTID: &GTIDEvent{SID: txSID, GNO: gno}},
		{Header: Header{Type: evQuery, LogPos: 110}, File: "mysql-bin.000001", Query: &QueryEvent{Schema: "s", Query: "BEGIN"}},
		{Header: Header{Type: evTableMap, LogPos: 120}, File: "mysql-bin.000001", TableMap: tm},
		{Header: Header{Type: evWriteRowsV2, LogPos: 130}, File: "mysql-bin.000001", Rows: &RowsEvent{Table: tm, Version: 2,
			Changes: []Change{{Kind: source.EvInsert, New: Image{
				Row:     source.Row{Table: source.TableRef{Schema: "s", Table: "t"}, Values: []source.Value{{Kind: source.KindInt, I: id}}},
				Present: []bool{true},
			}}}}},
	}
	for _, ev := range steps {
		if err := a.Feed(ctx, ev); err != nil {
			return err
		}
	}
	return nil
}

func TestBatchesPassValidateBatch(t *testing.T) {
	// 契约要求：连接器产出的事件流要能被 source.ValidateBatch 机械校验通过。
	var batches [][]source.Event
	a := NewAssembler(func(ctx context.Context, tx Tx) error {
		batches = append(batches, tx.Events)
		return nil
	})
	tm := &TableMap{TableID: 1, Schema: "s", Table: "t", Columns: 1, Types: []byte{typeLong}, Names: []string{"id"}}

	if err := feedTxn(t, a, 20, 2, true); err != nil {
		t.Fatalf("行事务: %v", err)
	}
	if err := a.Feed(context.Background(), &Event{Header: Header{Type: evQuery, LogPos: 900}, File: "f",
		Query: &QueryEvent{Schema: "s", Query: "ALTER TABLE t ADD COLUMN y INT"}}); err != nil {
		t.Fatalf("DDL: %v", err)
	}
	if len(batches) != 2 {
		t.Fatalf("应有 2 个批次，拿到 %d", len(batches))
	}
	for i, evs := range batches {
		if err := source.ValidateBatch(evs); err != nil {
			t.Fatalf("第 %d 批没能通过 ValidateBatch: %v（事件=%v）", i, err, kinds(evs))
		}
	}
	_ = tm
}

func TestHalfTransactionFailsValidateBatch(t *testing.T) {
	// 半个事务的批次必须被校验拦住：这正是我们「未提交不交付」的同一条不变量。
	half := []source.Event{
		{Kind: source.EvBegin, TxID: "t1"},
		{Kind: source.EvInsert, TxID: "t1"},
	}
	if err := source.ValidateBatch(half); err == nil {
		t.Fatal("没有 EvCommit 收尾的批次必须被 ValidateBatch 拒绝")
	}
	// 而我们的装配器在没提交时根本不会交付这种批次（回滚/未提交 → 零批次）。
	var batches int
	a := NewAssembler(func(ctx context.Context, tx Tx) error { batches++; return nil })
	if err := feedTxn(t, a, 21, 1, false); err != nil { // 没有 XID
		t.Fatalf("喂事件失败: %v", err)
	}
	if batches != 0 {
		t.Fatalf("未提交的事务不该产生批次，拿到 %d", batches)
	}
}

func kinds(evs []source.Event) []source.EventKind {
	out := make([]source.EventKind, len(evs))
	for i, ev := range evs {
		out[i] = ev.Kind
	}
	return out
}

func TestAssemblerRejectsOversizedTransaction(t *testing.T) {
	// 大事务必须有上限：整段缓冲一个事务，内存 ≈ 行数 × 行宽。
	// 超限要报一条能读的错误（带做法与风险），不是 OOM。
	var batches int
	a := NewAssembler(func(ctx context.Context, tx Tx) error { batches++; return nil })
	a.MaxBytes = 1024 // 1 KB：几行大字段就超

	tm := &TableMap{TableID: 1, Schema: "s", Table: "t", Columns: 1,
		Types: []byte{typeBlob}, Meta: []uint16{2}, Names: []string{"payload"}}
	if err := feedTxnPrefix(t, a, 30, tm, 1); err != nil {
		t.Fatalf("第一行不该超限: %v", err)
	}
	big := strings.Repeat("x", 2048)
	err := a.Feed(context.Background(), &Event{Header: Header{Type: evWriteRowsV2, LogPos: 200}, File: "f",
		Rows: &RowsEvent{Table: tm, Version: 2, Changes: []Change{{Kind: source.EvInsert, New: Image{
			Row:     source.Row{Table: source.TableRef{Schema: "s", Table: "t"}, Values: []source.Value{{Kind: source.KindString, S: big}}},
			Present: []bool{true},
		}}}}})
	if err == nil {
		t.Fatal("超过上限必须报错，不能继续吃内存")
	}
	if !errors.Is(err, ErrTxOversize) {
		t.Fatalf("超限错误要能被 errors.Is(ErrTxOversize) 判定: %v", err)
	}
	// 用户能做的路必须是真能生效的：`reliability.tx_oversize` 在 job schema 里只有
	// spill|fail，写成「调高 reliability.tx_oversize.bytes」会让用户被 schema 拒绝。
	if contains(err.Error(), "reliability.tx_oversize.") {
		t.Fatalf("不能建议用户改 job yaml 里不存在的数值键: %v", err)
	}
	// 用户能做的两条路必须是真能生效的：拆小事务 / 环境变量。
	// `reliability.tx_oversize` 在 job schema 里只有 spill|fail，不能写成「调高它」。
	for _, want := range []string{"E1011", "JINLIANG_TX_MAX_BYTES", "拆小事务", "风险"} {
		if !contains(err.Error(), want) {
			t.Fatalf("错误里应包含 %q（四段式文案），拿到: %v", want, err)
		}
	}
	if contains(err.Error(), "reliability.tx_oversize.events") || contains(err.Error(), "reliability.tx_oversize.bytes") {
		t.Fatalf("不能建议用户改 job yaml 里不存在的数值键（schema 只有 spill|fail）: %v", err)
	}
	if batches != 0 {
		t.Fatalf("超限时不该交付任何批次，拿到 %d", batches)
	}
	if a.Position().Pos >= 200 {
		t.Fatalf("超限时位点不能前进: %#v", a.Position())
	}
}

func TestAssemblerEventCountLimit(t *testing.T) {
	a := NewAssembler(func(ctx context.Context, tx Tx) error { return nil })
	a.MaxEvents = 2
	tm := &TableMap{TableID: 1, Schema: "s", Table: "t", Columns: 1, Types: []byte{typeLong}, Names: []string{"id"}}
	err := feedTxnPrefix(t, a, 31, tm, 1)
	if err != nil {
		t.Fatalf("第一行: %v", err)
	}
	for i := 0; i < 2; i++ {
		err = a.Feed(context.Background(), &Event{Header: Header{Type: evWriteRowsV2, LogPos: uint32(300 + i)}, File: "f",
			Rows: &RowsEvent{Table: tm, Version: 2, Changes: []Change{{Kind: source.EvInsert, New: Image{
				Row:     source.Row{Table: source.TableRef{Schema: "s", Table: "t"}, Values: []source.Value{{Kind: source.KindInt, I: int64(i)}}},
				Present: []bool{true},
			}}}}})
		if err != nil {
			break
		}
	}
	if err == nil {
		t.Fatal("事件数超过上限必须报错")
	}
	if !errors.Is(err, ErrTxOversize) {
		t.Fatalf("错误要能被 errors.Is(ErrTxOversize) 判定（调用方靠它区分「过大」与「流坏了」）: %v", err)
	}
	if !contains(err.Error(), "E1011") {
		t.Fatalf("错误里要带码表里的码 E1011（user_action），拿到: %v", err)
	}
	if !contains(err.Error(), "JINLIANG_TX_MAX_EVENTS") {
		t.Fatalf("错误文案要给出可配置项: %v", err)
	}
}
