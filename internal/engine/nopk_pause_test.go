package engine

// 无主键表收到改/删：**复用**现有的按表暂停通道，不新增接口
// （docs/DESIGN-NO-PRIMARY-KEY.md §4.4：引擎只看原因码；形状按 Ark 裁决复用 pgsink.PausableError）。
//
// 判据三件（S2 验收里引擎侧那一半，不需要真库）：
//  1. 登记先行的记录里 `Reason = no_pk_write`（不是 type_changed：恢复/展示都按稳定码判等）；
//  2. 这一批重试后**其它表照常写进去**（「只停这张表」）；
//  3. 跳过窗口有记账（`NoteSkipped` 被调用），否则就是静默缺失。
//
// 变异（任一处拿掉即红）：
//   - `pauseSink.Apply` 不把 NoPKWriteError 当 PausableError → 整批失败、其它表也停；
//   - `registerPause` 写死 type_changed → 第 1 条红；
//   - `noteSkipped` 不调 → 第 3 条红。

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/spencerswagger/jinliang/internal/control"
	"github.com/spencerswagger/jinliang/internal/pgsink"
	"github.com/spencerswagger/jinliang/internal/source"
)

// nopkPauseReg 记录登记时的完整记录与记账行数（fakePauseReg 只数次数，看不出口径）。
type nopkPauseReg struct {
	failPause bool
	pauses    int
	rec       map[string]pgsink.PausedTable
	skipped   map[string]int64
	skipCalls int
}

func newNoPKPauseReg() *nopkPauseReg {
	return &nopkPauseReg{rec: map[string]pgsink.PausedTable{}, skipped: map[string]int64{}}
}

func (n *nopkPauseReg) LoadPausedTables(context.Context) error { return nil }
func (n *nopkPauseReg) PausedTables(context.Context) (map[string]pgsink.PausedTable, error) {
	return map[string]pgsink.PausedTable{}, nil
}
func (n *nopkPauseReg) PauseTable(_ context.Context, ref source.TableRef, rec pgsink.PausedTable) error {
	n.pauses++
	if n.failPause {
		return errors.New("模拟登记失败：目标端状态行不在了")
	}
	n.rec[ref.Table] = rec
	return nil
}
func (n *nopkPauseReg) NoteSkipped(_ context.Context, ref source.TableRef, rows int64, _ string) error {
	n.skipCalls++
	n.skipped[ref.Table] += rows
	return nil
}
func (n *nopkPauseReg) ClearPausedTable(context.Context, source.TableRef) error       { return nil }
func (n *nopkPauseReg) MarkTableRecopyPending(context.Context, source.TableRef) error { return nil }

// nopkInner 第一次返回 NoPKWriteError（模拟「这批里有这张表的改/删」），之后成功。
type nopkInner struct {
	calls   int
	batches [][]source.Event
}

func (s *nopkInner) Apply(_ context.Context, evs []source.Event, _ source.Position) error {
	s.calls++
	if s.calls == 1 {
		return &pgsink.NoPKWriteError{TableName: "events", Detail: "收到一次改动"}
	}
	s.batches = append(s.batches, evs)
	return nil
}

func TestNoPKWritePausesOnlyThatTable(t *testing.T) {
	reg := newNoPKPauseReg()
	inner := &nopkInner{}
	r := pausedRunner() // 这一批才发现，所以 events 还不在内存暂停集合里
	r.pauseReg = reg
	r.doc = control.JobDoc{Target: control.TargetDoc{Schema: "public"}}
	p := newPauseSink(inner, r)

	evs := []source.Event{
		{Kind: source.EvBegin},
		{Kind: source.EvUpdate, Table: source.TableRef{Schema: "public", Table: "events"}},
		{Kind: source.EvInsert, Table: source.TableRef{Schema: "public", Table: "orders"}},
		{Kind: source.EvCommit},
	}
	if err := p.Apply(context.Background(), evs, nil); err != nil {
		t.Fatalf("无主键表的改/删只该停它自己，不该打死整轮：%v", err)
	}

	// ① 登记：原因码是 no_pk_write（稳定机器码），不是 type_changed。
	rec, ok := reg.rec["events"]
	if !ok {
		t.Fatalf("必须登记 events 的暂停，实际登记了 %v", reg.rec)
	}
	if rec.Reason != pgsink.PauseReasonNoPKWrite {
		t.Fatalf("原因码应是 %q，得到 %q（写死 type_changed 会让恢复/展示按错的口径判）",
			pgsink.PauseReasonNoPKWrite, rec.Reason)
	}
	if rec.Detail == "" {
		t.Fatal("Detail 要写人话（哪张表、哪条变更），否则用户只看到一个码")
	}

	// ② 其它表照常：重试后这一批里只有 orders 的 INSERT（events 的改/删被整条丢掉）。
	if inner.calls != 2 {
		t.Fatalf("应「先失败 → 登记 → 重试」共 2 次，实际 %d 次", inner.calls)
	}
	if len(inner.batches) != 1 {
		t.Fatalf("重试应恰好提交 1 批，实际 %d 批", len(inner.batches))
	}
	got := inner.batches[0]
	var sawOrders, sawEvents, sawBegin, sawCommit bool
	for _, ev := range got {
		switch {
		case ev.Kind == source.EvBegin:
			sawBegin = true
		case ev.Kind == source.EvCommit:
			sawCommit = true
		case ev.Table.Table == "orders":
			sawOrders = true
		case ev.Table.Table == "events":
			sawEvents = true
		}
	}
	if !sawOrders || !sawBegin || !sawCommit {
		t.Fatalf("其它表与事务边界都必须留下（只停该表）：%+v", got)
	}
	if sawEvents {
		t.Fatalf("暂停表的事件不许交到写路径：%+v", got)
	}

	// ③ 跳过窗口要有账（可枚举、可补偿），否则就是静默缺失。
	if reg.skipCalls == 0 {
		t.Fatal("跳过窗口必须记账（NoteSkipped）")
	}
	if reg.skipped["events"] < 1 {
		t.Fatalf("events 的 skipped_rows 应 ≥1，得到 %d", reg.skipped["events"])
	}
	// 引擎内存集合要记得它：重搬窗口内写路径继续跳过这张表。
	if _, ok := r.pausedRecord("events"); !ok {
		t.Fatal("登记成功后，引擎的暂停集合里必须有 events")
	}
}

// 登记失败 = 整批失败（沿用 pause_skip 红线 1）。新原因码不许破例：
// 「跳过了但没记下」是永久静默缺失。
func TestNoPKWriteRegistrationFailureFailsBatch(t *testing.T) {
	reg := newNoPKPauseReg()
	reg.failPause = true
	inner := &nopkInner{}
	r := pausedRunner()
	r.pauseReg = reg
	r.doc = control.JobDoc{Target: control.TargetDoc{Schema: "public"}}
	p := newPauseSink(inner, r)

	err := p.Apply(context.Background(), []source.Event{
		{Kind: source.EvBegin},
		{Kind: source.EvUpdate, Table: source.TableRef{Schema: "public", Table: "events"}},
		{Kind: source.EvCommit},
	}, nil)
	if err == nil {
		t.Fatal("登记失败时这一批必须失败（不许跳过不留痕）")
	}
	if !strings.Contains(err.Error(), "登记") {
		t.Fatalf("错误要指向「登记暂停失败」，得到：%v", err)
	}
	if inner.calls != 1 {
		t.Fatalf("登记失败后不许再重试这一批：inner 只该被调 1 次，实际 %d 次", inner.calls)
	}
	if reg.skipCalls != 0 {
		t.Fatalf("没提交成功的批次不许记跳过账，实际记了 %d 次", reg.skipCalls)
	}
}
