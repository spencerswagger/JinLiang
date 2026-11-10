package mysqlsource

import (
	"testing"

	"github.com/spencerswagger/jinliang/internal/source"
)

// binlog_row_metadata=MINIMAL（8.0/8.4/9 的默认值）时，binlog 里只有列类型、没有列名，
// TABLE_MAP 只能给出 col_1..col_n 占位名。目标端比对的是**列名**，拿到占位名会判成
// 「源端列改名/删列」并整轮失败。这里按序号把 catalog 的名字补回去——这条用例钉住它。
func TestEnrichRelationFillsNamesFromCatalog(t *testing.T) {
	s := &Source{}
	s.SetKnownTables([]source.Table{{
		TableRef:    source.TableRef{Schema: "app", Table: "orders"},
		Columns:     []source.Column{{Name: "id", Type: "bigint"}, {Name: "payload", Type: "varchar(64)"}},
		PrimaryKey:  []string{"id"},
		HasStablePK: true,
	}})

	// binlog 侧：只有类型与占位名（MINIMAL）。
	rel := &source.Table{
		TableRef: source.TableRef{Schema: "app", Table: "orders"},
		Columns: []source.Column{
			{Name: "col_1", Type: "bigint"},
			{Name: "col_2", Type: "varchar(64)"},
		},
	}
	got := s.enrichRelation(rel)
	if got.Columns[0].Name != "id" || got.Columns[1].Name != "payload" {
		t.Fatalf("列名没补回来：%v", []string{got.Columns[0].Name, got.Columns[1].Name})
	}
	if len(got.PrimaryKey) != 1 || got.PrimaryKey[0] != "id" {
		t.Fatalf("主键没补回来：%v", got.PrimaryKey)
	}
	// 同一版定义必须复用同一个指针（契约：指针相等即同一版）。
	if again := s.enrichRelation(rel); again != got {
		t.Fatal("同一个 Relation 指针必须复用同一个补好的副本，否则目标端会以为每批都在改结构")
	}
	// 原对象不能被改写（解码器可能还在用）。
	if rel.Columns[0].Name != "col_1" {
		t.Fatalf("不该改写调用方的 Relation：%v", rel.Columns[0].Name)
	}
}

// 列数对不上（源端刚 DDL 过）时**不许**硬套 catalog：名字能按序号补就补，
// 补不了的保留占位名，让上层照常报「结构不一致」——静默套用会让行值与列错位。
func TestEnrichRelationKeepsPlaceholdersWhenColumnCountDiffers(t *testing.T) {
	s := &Source{}
	s.SetKnownTables([]source.Table{{
		TableRef: source.TableRef{Schema: "app", Table: "orders"},
		Columns:  []source.Column{{Name: "id", Type: "bigint"}},
	}})
	rel := &source.Table{
		TableRef: source.TableRef{Schema: "app", Table: "orders"},
		Columns: []source.Column{
			{Name: "col_1", Type: "bigint"},
			{Name: "col_2", Type: "varchar(64)"},
		},
	}
	got := s.enrichRelation(rel)
	if got.Columns[0].Name != "id" {
		t.Fatalf("能按序号对上的列要补：%v", got.Columns[0].Name)
	}
	if got.Columns[1].Name != "col_2" {
		t.Fatalf("对不上的列保留占位名（让上层报结构不一致）：%v", got.Columns[1].Name)
	}
	if got.Columns[1].Type != "varchar(64)" {
		t.Fatalf("列数不一致时类型必须沿用 binlog 的（catalog 可能已经旧了）：%v", got.Columns[1].Type)
	}
}

// binlog 已经给了真列名（binlog_row_metadata=FULL）时原样返回，不做任何猜测。
func TestEnrichRelationLeavesFullMetadataAlone(t *testing.T) {
	s := &Source{}
	s.SetKnownTables([]source.Table{{
		TableRef: source.TableRef{Schema: "app", Table: "orders"},
		Columns:  []source.Column{{Name: "wrong", Type: "bigint"}},
	}})
	rel := &source.Table{
		TableRef: source.TableRef{Schema: "app", Table: "orders"},
		Columns:  []source.Column{{Name: "id", Type: "bigint"}},
	}
	if got := s.enrichRelation(rel); got != rel {
		t.Fatalf("FULL 元数据必须原样使用，实际改成了 %v", got.Columns)
	}
}
