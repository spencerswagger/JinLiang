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

// MINIMAL 元数据下「列数没变、只改了类型」：修复前会被 catalog **整体盖回旧类型**，
// 改类型在流上彻底看不见（目标端按旧口径继续写，直到下次 prepare 才发现）。
//
// 判据：类型串一变，就不许再拿 catalog 的类型覆盖；列名照旧按序号补齐。
// 这是 Owl II 在 #94 评审里指出的必改项（列数不变的 MODIFY COLUMN 会被盖掉）。
func TestEnrichRelationKeepsBinlogTypeWhenTypeChanged(t *testing.T) {
	s := &Source{}
	s.SetKnownTables([]source.Table{{
		TableRef:   source.TableRef{Schema: "app", Table: "orders"},
		Columns:    []source.Column{{Name: "id", Type: "bigint"}, {Name: "v", Type: "bigint"}},
		PrimaryKey: []string{"id"}, HasStablePK: true,
	}})

	// 第一版：与 catalog 一致（MINIMAL 的占位名）。
	rel1 := &source.Table{
		TableRef: source.TableRef{Schema: "app", Table: "orders"},
		Columns:  []source.Column{{Name: "col_1", Type: "bigint"}, {Name: "col_2", Type: "bigint"}},
	}
	got1 := s.enrichRelation(rel1)
	if got1.Columns[1].Type != "bigint" || got1.Columns[1].Name != "v" {
		t.Fatalf("第一版应沿用 catalog：%#v", got1.Columns[1])
	}

	// 第二版：源端 MODIFY COLUMN v VARCHAR(64)（列数不变，只有类型变了）。
	rel2 := &source.Table{
		TableRef: source.TableRef{Schema: "app", Table: "orders"},
		Columns:  []source.Column{{Name: "col_1", Type: "bigint"}, {Name: "col_2", Type: "varchar(64)"}},
	}
	got2 := s.enrichRelation(rel2)
	if got2.Columns[1].Type != "varchar(64)" {
		t.Fatalf("类型变了却仍被 catalog 盖成 %q：改类型在流上看不见（MINIMAL 的默认路径）",
			got2.Columns[1].Type)
	}
	if got2.Columns[1].Name != "v" {
		t.Fatalf("名字仍要按序号补齐，实际 %q", got2.Columns[1].Name)
	}
	// 第三版回到 bigint（再改一次）也要看得见。
	rel3 := &source.Table{
		TableRef: source.TableRef{Schema: "app", Table: "orders"},
		Columns:  []source.Column{{Name: "col_1", Type: "bigint"}, {Name: "col_2", Type: "bigint"}},
	}
	if got3 := s.enrichRelation(rel3); got3.Columns[1].Type != "bigint" {
		t.Fatalf("改回去也要看得见，实际 %q", got3.Columns[1].Type)
	}
}

// looseSameType：MINIMAL 元数据下「同一列」的两种写法必须判成同一列，
// 而**真的改了类型**必须判成不同——两边都会出事：
// 判太严 → 每一张带 varchar/char/text 的表都在第一条 Relation 上误暂停；
// 判太松 → 改类型被 catalog 盖回去，目标端按旧口径继续写（静默写错）。
func TestLooseSameType(t *testing.T) {
	cases := []struct {
		binlog, catalog string
		same            bool
		why             string
	}{
		{"bigint", "bigint", true, "一模一样"},
		{"varchar(64)", "varchar(64)", true, "换算之后两边都是字符，逐字相等"},
		// 没换算的字节串**不许**当成同一列：允许多倍等于给「真改了长度」开静默通道。
		// （换算在 enrichRelation 里按 catalog 字符集做：`bytesToCharsType`）
		{"varchar(256)", "varchar(64)", false, "未换算的字节 vs 字符：宁可多暂停一次"},
		{"varchar(192)", "varchar(64)", false, "同上（utf8mb3 ×3 也不放行）"},
		{"char(40)", "char(10)", false, "CHAR 未换算时同样不放行"},
		{"varchar(128)", "varchar(64)", false, "latin1 的字节恰好 ×2 —— 最容易被误放行的形态"},
		{"text", "text", true, "一致"},
		{"blob", "text", true, "TEXT 与 BLOB 共用类型字节，MINIMAL 下分不开——不能因此判成改类型"},
		{"mediumblob", "mediumtext", true, "同尺寸档"},
		{"varchar(32)", "varbinary(32)", true, "binary 字符集在 MINIMAL 里拿不到，名字会变"},
		{"varchar(64)", "bigint", false, "**真的改类型**：必须判不同，否则改类型看不见"},
		{"int", "bigint", false, "整型宽度变了"},
		{"mediumtext", "text", false, "尺寸档变了"},
		{"varchar(512)", "varchar(64)", false, "×8 不在任何字符集的可能里（utf8mb4 最多 ×4）"},
		{"varchar(20)", "varchar(64)", false, "收窄"},
	}
	for _, c := range cases {
		if got := looseSameType(c.binlog, c.catalog); got != c.same {
			t.Errorf("looseSameType(%q, %q) = %v，应为 %v —— %s", c.binlog, c.catalog, got, c.same, c.why)
		}
	}
}

// 5.7 的 TABLE_MAP 不带列字符集，只能按字节报长度；换算必须靠 catalog 的字符集。
// 这一格钉住换算本身与误差方向（宁可多暂停，不可漏暂停）。
func TestBytesToCharsType(t *testing.T) {
	cases := []struct {
		in, charset, want, why string
	}{
		{"varchar(256)", "utf8mb4", "varchar(64)", "utf8mb4 的 VARCHAR(64)"},
		{"varchar(192)", "utf8mb3", "varchar(64)", "utf8mb3 的 VARCHAR(64)"},
		{"char(40)", "utf8mb4", "char(10)", "CHAR 的长度也是字节"},
		{"varchar(64)", "latin1", "varchar(64)", "latin1 一字一字节，换算后不变"},
		{"varchar(256)", "", "varchar(256)", "拿不到字符集：原样返回，不猜"},
		{"varbinary(32)", "utf8mb4", "varbinary(32)", "VARBINARY 本来就是字节计，不许除"},
		{"text", "utf8mb4", "text", "TEXT 的长度是尺寸档，不是字符数"},
		{"varchar(255)", "utf8mb4", "varchar(255)", "除不尽（不该发生）：原样返回，不猜"},
	}
	for _, c := range cases {
		if got := bytesToCharsType(c.in, c.charset); got != c.want {
			t.Errorf("bytesToCharsType(%q, %q) = %q，应为 %q —— %s", c.in, c.charset, got, c.want, c.why)
		}
	}
}

// enrichRelation 的 5.7 路径：binlog 没有字符集、catalog 有 → 按 catalog 换算成字符，
// 且换算后与 catalog 一致（不触发假暂停）。
func TestEnrichRelationConverts5_7ByteLengths(t *testing.T) {
	s := &Source{}
	s.SetKnownTables([]source.Table{{
		TableRef: source.TableRef{Schema: "app", Table: "orders"},
		Columns: []source.Column{
			{Name: "id", Type: "bigint"},
			{Name: "name", Type: "varchar(64)", Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"},
		},
		PrimaryKey: []string{"id"}, HasStablePK: true,
	}})

	// 5.7：只有字节长度，没有字符集（Charset 为空）。
	rel := &source.Table{
		TableRef: source.TableRef{Schema: "app", Table: "orders"},
		Columns: []source.Column{
			{Name: "col_1", Type: "bigint"},
			{Name: "col_2", Type: "varchar(256)"},
		},
	}
	got := s.enrichRelation(rel)
	if got.Columns[1].Type != "varchar(64)" {
		t.Fatalf("应按 catalog 字符集把字节换算成字符（varchar(256) → varchar(64)），实际 %q", got.Columns[1].Type)
	}
	if got.Columns[1].Charset != "utf8mb4" {
		t.Fatalf("字符集应补自 catalog，实际 %q", got.Columns[1].Charset)
	}
	// 换算后与 catalog 同型 → 不该判成改类型（否则 5.7 上每张多字节字符列都假暂停）。
	if !columnsLooseSame(got.Columns, []source.Column{
		{Name: "id", Type: "bigint"},
		{Name: "name", Type: "varchar(64)"},
	}) {
		t.Fatal("换算之后应判成同一列（否则 5.7 上会一直假暂停）")
	}
}

// 5.7 上 BINARY 与 CHAR 在 binlog 里分不开（共用类型字节、没有排序规则），
// 类型串只能靠 catalog 纠正基名——否则「改类型」判定会把 binary 与 char 看成同一列。
// 长度仍按 binlog（这样长度变化照样看得见）。
func TestEnrichRelationFixesBinaryBaseOn57(t *testing.T) {
	s := &Source{}
	s.SetKnownTables([]source.Table{{
		TableRef: source.TableRef{Schema: "app", Table: "t"},
		Columns: []source.Column{
			{Name: "b4", Type: "binary(4)"},                     // MySQL 不给字符集（NULL）
			{Name: "vb", Type: "varbinary(8)"},                  // 同上
			{Name: "bl", Type: "blob"},                          // 同上
			{Name: "c10", Type: "char(10)", Charset: "utf8mb4"}, // 有字符集
			{Name: "tx", Type: "text", Charset: "utf8mb4"},      // 有字符集
		},
		PrimaryKey: []string{"b4"}, HasStablePK: true,
	}})
	// 5.7 的 binlog：全部落在「字符家族」的基名上，长度是字节。
	rel := &source.Table{
		TableRef: source.TableRef{Schema: "app", Table: "t"},
		Columns: []source.Column{
			{Name: "col_1", Type: "char(4)"},
			{Name: "col_2", Type: "varchar(8)"},
			{Name: "col_3", Type: "text"},
			{Name: "col_4", Type: "char(40)"},
			{Name: "col_5", Type: "text"},
		},
	}
	got := s.enrichRelation(rel)
	want := []string{"binary(4)", "varbinary(8)", "blob", "char(10)", "text"}
	for i, w := range want {
		if got.Columns[i].Type != w {
			t.Errorf("第 %d 列应是 %q，实际 %q（catalog 的 binary/字符家族是权威基名）", i+1, w, got.Columns[i].Type)
		}
	}
}
