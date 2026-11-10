package engine

// 「变更流只交付本次选中的表」这条过滤的判等键必须是 **schema + 表名**。
//
// 踩过的坑（MySQL → MySQL，源库与目标库在同一个实例上）：
// 过滤只比表名时，目标库里的同名表（`jinliang_test.m2m_orders`）会从 binlog 里
// 漏进来——引擎读到自己刚写下去的行，再把它当成源端变更写一遍。当时的表现是
// `Unknown column 'col_1'`：binlog_row_metadata=MINIMAL 时 binlog 没给列名，而目标库同名表
// 不在「已知表」里，补不出真名，只能拿占位名拼语句。真实用户也会遇到（本地验收、
// 小规模自建经常把源库与目标库放同一个实例），而且它不会报「配置错」，只会报一个
// 看起来像 bug 的列名错误。

import (
	"testing"

	"github.com/spencerswagger/jinliang/internal/source"
)

func TestTableKeySeparatesSameNameAcrossSchemas(t *testing.T) {
	keep := map[string]bool{}
	for _, tbl := range []source.Table{
		{TableRef: source.TableRef{Schema: "biz", Table: "orders"}},
	} {
		keep[tableKey(tbl.TableRef)] = true
	}

	inScope := source.TableRef{Schema: "biz", Table: "orders"}
	if !keep[tableKey(inScope)] {
		t.Fatalf("本次选中的表被过滤掉了：%s", tableKey(inScope))
	}
	// 同一个实例上「另一个库里的同名表」不属于本次范围——这是那条 bug 的形状。
	otherSchema := source.TableRef{Schema: "target_db", Table: "orders"}
	if keep[tableKey(otherSchema)] {
		t.Fatalf("同名但不同库的表不该被交付：%s（只按表名比就会漏进来）", tableKey(otherSchema))
	}
	// 同库不同表也要分开。
	otherTable := source.TableRef{Schema: "biz", Table: "orders_archive"}
	if keep[tableKey(otherTable)] {
		t.Fatalf("同库不同表不该被交付：%s", tableKey(otherTable))
	}
}
