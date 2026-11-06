package verify

import (
	"context"
	"strings"
	"testing"

	"github.com/spencerswagger/jinliang/internal/source"
)

// 这四条是「校验器会不会说谎」的唯一验收标准：目标端被动过，verify 必须判失败
// 并指到具体行/列。先写测试、确认现版本会红，再改实现。
//
// 注意 SampleRows=1：故意让抽样只覆盖第 1 行，证明「抽样通过」不能当判据。

func dataOnly(t *testing.T, src, tgt *fakeSide) Options {
	t.Helper()
	opts := baseOptions(src, tgt)
	opts.Level = LevelData
	opts.SampleRows = 1
	return opts
}

func TestAggregateCatchesOneValueChange(t *testing.T) {
	src, tgt := newFake("postgres"), newFake("postgres")
	src.add(ordersTable(), ordersRows()...)
	rows := cloneRows(ordersRows())
	rows[2]["amount"] = "31.00" // 第 3 行被改，抽样只覆盖第 1 行
	tgt.add(ordersTable(), rows...)

	rep, err := Run(context.Background(), dataOnly(t, src, tgt))
	if err != nil {
		t.Fatal(err)
	}
	if rep.OK {
		t.Fatalf("目标端改了 1 行，verify 仍报通过：%+v", rep.Levels[0])
	}
	f := findFinding(rep, "row_mismatch")
	if f == nil || f.Table != "orders" || f.Column != "amount" || !strings.Contains(f.Detail, "主键 3") {
		t.Fatalf("必须指到具体行/列（orders.amount, 主键 3），实际 findings=%+v", rep.Levels[0].Findings)
	}
}

func TestAggregateCatchesDeletedRow(t *testing.T) {
	src, tgt := newFake("postgres"), newFake("postgres")
	src.add(ordersTable(), ordersRows()...)
	tgt.add(ordersTable(), ordersRows()[:2]...) // 删掉第 3 行

	rep, err := Run(context.Background(), dataOnly(t, src, tgt))
	if err != nil {
		t.Fatal(err)
	}
	if rep.OK || !hasCode(rep, "row_count_mismatch") {
		t.Fatalf("删 1 行必须判失败并指出行数差异：%+v", rep.Levels[0])
	}
	f := findFinding(rep, "missing_row")
	if f == nil || f.Table != "orders" || !strings.Contains(f.Detail, "主键 3") {
		t.Fatalf("必须指到缺的那一行（主键 3），实际 findings=%+v", rep.Levels[0].Findings)
	}
}

func TestAggregateCatchesPrecisionChange(t *testing.T) {
	src, tgt := newFake("postgres"), newFake("postgres")
	src.add(ordersTable(), ordersRows()...)
	rows := cloneRows(ordersRows())
	rows[2]["amount"] = "30.0" // 1.234 -> 1.23 同类：值变了但行数不变
	tgt.add(ordersTable(), rows...)

	rep, err := Run(context.Background(), dataOnly(t, src, tgt))
	if err != nil {
		t.Fatal(err)
	}
	if rep.OK {
		t.Fatalf("精度变化没被发现：%+v", rep.Levels[0])
	}
	f := findFinding(rep, "row_mismatch")
	if f == nil || f.Table != "orders" || f.Column != "amount" || !strings.Contains(f.Detail, "主键 3") {
		t.Fatalf("必须指到具体行/列（orders.amount, 主键 3），实际 findings=%+v", rep.Levels[0].Findings)
	}
}

func TestAggregateCatchesSwappedRows(t *testing.T) {
	src, tgt := newFake("postgres"), newFake("postgres")
	src.add(ordersTable(), ordersRows()...)
	rows := cloneRows(ordersRows())
	// 两行互换：行数不变，count/sum/min/max 也可能完全不变——只有行级/顺序敏感
	// 的证据才能发现。第 2、3 行互换 name/amount。
	rows[1]["name"], rows[2]["name"] = rows[2]["name"], rows[1]["name"]
	rows[1]["amount"], rows[2]["amount"] = rows[2]["amount"], rows[1]["amount"]
	tgt.add(ordersTable(), rows...)

	rep, err := Run(context.Background(), dataOnly(t, src, tgt))
	if err != nil {
		t.Fatal(err)
	}
	if rep.OK {
		t.Fatalf("两行互换没被发现（count/sum/min/max 都不变）：%+v", rep.Levels[0])
	}
	if findFinding(rep, "row_mismatch") == nil {
		t.Fatalf("必须指到互换的两行，实际 findings=%+v", rep.Levels[0].Findings)
	}
	b := findFinding(rep, "block_checksum_mismatch")
	if b == nil || b.Table != "orders" || !strings.Contains(b.Detail, "第 0 块") {
		t.Fatalf("必须指到具体块（第 0 块），实际 findings=%+v", rep.Levels[0].Findings)
	}
}

func findFinding(rep Report, code string) *Finding {
	for _, lr := range rep.Levels {
		for i := range lr.Findings {
			if lr.Findings[i].Code == code {
				return &lr.Findings[i]
			}
		}
	}
	return nil
}

var _ = source.TableRef{}
