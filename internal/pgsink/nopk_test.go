package pgsink

// 无主键表的写路径（docs/DESIGN-NO-PRIMARY-KEY.md §2.3 / §6 红线 2、6）：
//   - `INSERT` 照跑（insert-only：纯插入不需要行身份）；
//   - 同一批里出现 `UPDATE`/`DELETE` → **写任何一行之前**整批返回
//     `NoPKWriteError`（PausableError，原因码 `no_pk_write`），引擎据此只停这张表、
//     其它表照写、位点照常前进，欠的账等整表重搬。
//
// 为什么在 pgsink 侧钉：这个判定是「目标端能不能定位这一行」的事实，只有写路径知道；
// 引擎侧的「按表暂停」只是一条既有的通用通道（`pause_skip.go`），不该为它新开接口。
// 变异：把 `checkNoIdentityWrites` 从 `Apply` 里拿掉 → 本用例红，且 UPDATE 会被
// 当成 INSERT 写进去（多一行，`upsertBatchTx` 对无主键表拼的就是裸 INSERT）。

import (
	"errors"
	"testing"

	"github.com/spencerswagger/jinliang/internal/catalog"
	"github.com/spencerswagger/jinliang/internal/source"
)

// nopkSink 只装一份 catalog：这条判定不碰数据库（纯内存查表）。
func nopkSink(t *testing.T) *Sink {
	t.Helper()
	cat := source.Catalog{Tables: []source.Table{
		{
			TableRef: source.TableRef{Schema: "public", Table: "no_pk"},
			Columns: []source.Column{
				{Name: "id", Type: "bigint"},
				{Name: "v", Type: "text"},
			},
		},
		{
			TableRef:   source.TableRef{Schema: "public", Table: "with_pk"},
			PrimaryKey: []string{"id"},
			Columns: []source.Column{
				{Name: "id", Type: "bigint"},
				{Name: "v", Type: "text"},
			},
		},
	}}
	tables, order := buildTables(cat, catalog.Postgres)
	return &Sink{tables: tables, order: order}
}

func nopkEvent(kind source.EventKind, table string) source.Event {
	return source.Event{
		Kind:  kind,
		Table: source.TableRef{Schema: "public", Table: table},
	}
}

func TestNoIdentityWritesArePausableBeforeAnyWrite(t *testing.T) {
	s := nopkSink(t)

	for _, kind := range []source.EventKind{source.EvUpdate, source.EvDelete} {
		err := s.checkNoIdentityWrites([]source.Event{
			nopkEvent(source.EvInsert, "with_pk"), // 同批的其它表：不该被牵连
			nopkEvent(kind, "no_pk"),
		})
		if err == nil {
			t.Fatalf("%v 无主键表：必须整批返回 PausableError（不许写半批）", kind)
		}
		var pe PausableError
		if !errors.As(err, &pe) {
			t.Fatalf("%v：错误必须是 PausableError，得到 %T：%v", kind, err, err)
		}
		if pe.Table() != "no_pk" {
			t.Fatalf("%v：暂停/拒绝的应是 no_pk，得到 %q", kind, pe.Table())
		}
		if pe.Reason() != PauseReasonNoPKWrite {
			t.Fatalf("%v：原因码应是 %q（稳定机器码），得到 %q", kind, PauseReasonNoPKWrite, pe.Reason())
		}
		var nopk *NoPKWriteError
		if !errors.As(err, &nopk) {
			t.Fatalf("%v：应是 *NoPKWriteError（引擎按它挂 E1017，不能落到 E1015）", kind)
		}
	}
}

func TestNoIdentityInsertsStillRun(t *testing.T) {
	s := nopkSink(t)
	// insert-only 是默认口径：无主键表纯插入照跑，不许因为「没有身份」被拦。
	if err := s.checkNoIdentityWrites([]source.Event{
		nopkEvent(source.EvInsert, "no_pk"),
		nopkEvent(source.EvInsert, "no_pk"),
		nopkEvent(source.EvTruncate, "no_pk"),
	}); err != nil {
		t.Fatalf("无主键表的 INSERT/TRUNCATE 不需要行身份，不该被拦：%v", err)
	}
	// 有主键的表：改/删都照旧（定位得）。
	if err := s.checkNoIdentityWrites([]source.Event{
		nopkEvent(source.EvUpdate, "with_pk"),
		nopkEvent(source.EvDelete, "with_pk"),
	}); err != nil {
		t.Fatalf("有主键的表不该被拦：%v", err)
	}
}

// 第二道防线：即使某条新写路径没经过 checkNoIdentityWrites，
// deleteTx 也必须给 PausableError（按表暂停），而不是一句裸错误打死整轮。
func TestDeleteTxOnNoIdentityTableStaysPausable(t *testing.T) {
	s := nopkSink(t)
	ti := s.tables["public.no_pk"]
	if ti == nil {
		t.Fatal("用例前提：no_pk 在表字典里")
	}
	// 不连库，只走判定分支：ti.hasPK=false 时 deleteTx 在第一行 SQL 之前就返回。
	err := s.deleteTx(nil, nil, nopkEvent(source.EvDelete, "no_pk")) //nolint:staticcheck // 判定分支不碰 tx/ctx
	if err == nil {
		t.Fatal("无主键表的删除必须返回错误")
	}
	var pe PausableError
	if !errors.As(err, &pe) || pe.Reason() != PauseReasonNoPKWrite {
		t.Fatalf("应是 PausableError(no_pk_write)，得到 %T：%v", err, err)
	}
}
