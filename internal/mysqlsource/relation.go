package mysqlsource

import (
	"fmt"
	"strings"

	"github.com/spencerswagger/jinliang/internal/source"
)

// enrichRelation 用**已知的 catalog** 补上 binlog 没给的列名。
//
// 为什么必须有：`binlog_row_metadata` 的默认值是 MINIMAL，此时 TABLE_MAP 里没有
// 列名，只有列类型——行镜像仍然是「按表定义序」，但列名只能是 col_1..col_n 占位。
// 目标端拿到这种 Relation 会判成「源端列改名/删列」并整轮失败（它比对的是列名），
// 而真实原因只是元数据级别不够。这里按序号把 catalog 的名字补回去：
//   - 列数与 catalog 一致 → 名字、类型都用 catalog（行镜像顺序 = 表定义序）；
//   - 列数不一致（源端刚 DDL 过）→ 保留 binlog 的类型，名字能对上的按序号补，
//     对不上的保留占位名，让上层照常报「结构不一致」。
//
// 同一版 Relation（同一个指针）只算一次：契约要求「指针相等即同一版」。
func (s *Source) enrichRelation(rel *source.Table) *source.Table {
	if rel == nil {
		return nil
	}
	s.knownMu.Lock()
	defer s.knownMu.Unlock()
	if s.enriched == nil {
		s.enriched = map[*source.Table]*source.Table{}
	}
	if cp, ok := s.enriched[rel]; ok {
		return cp
	}
	known, has := s.known[rel.Schema+"."+rel.Table]
	placeholder := true
	for i, c := range rel.Columns {
		if c.Name != fmt.Sprintf("col_%d", i+1) {
			placeholder = false
			break
		}
	}
	if !has || !placeholder {
		s.enriched[rel] = rel
		return rel
	}
	cp := *rel
	cp.Columns = append([]source.Column(nil), rel.Columns...)
	if len(known.Columns) == len(cp.Columns) {
		cp.Columns = append([]source.Column(nil), known.Columns...)
		cp.PrimaryKey = append([]string(nil), known.PrimaryKey...)
		cp.HasStablePK = known.HasStablePK
	} else {
		for i := range cp.Columns {
			if i < len(known.Columns) && known.Columns[i].Name != "" {
				cp.Columns[i].Name = known.Columns[i].Name
			}
		}
	}
	s.enriched[rel] = &cp
	return &cp
}

// SetKnownTables 告诉连接器「本次同步的这些表长什么样」（来自 information_schema）。
// 只用于补 binlog 缺失的列名，不改变行镜像的顺序或数量。
func (s *Source) SetKnownTables(tables []source.Table) {
	s.knownMu.Lock()
	defer s.knownMu.Unlock()
	s.known = make(map[string]source.Table, len(tables))
	s.knownList = append([]source.Table(nil), tables...)
	for _, t := range tables {
		s.known[t.Schema+"."+t.Table] = t
	}
	s.enriched = map[*source.Table]*source.Table{}
}

// tableFromMap turns a TABLE_MAP event into the column definition the engine
// aligns row images against (source.Event.Relation).
//
// 列顺序 = binlog 行镜像的顺序 = Values 的顺序，三者必须一致，所以这里不做任何
// 重排/过滤（不可见列也在里面，索引仍对得上）。
//
// 名字缺失时（binlog_row_metadata=MINIMAL）用 col_1..col_n 占位并在类型串上保留
// 真实类型：让引擎能对齐，又不假装知道列名。
func tableFromMap(tm *TableMap) *source.Table {
	return tableFromMapWith(tm, staticCollations)
}

// tableFromMapWith is tableFromMap with an explicit collation table (the real
// one comes from information_schema.COLLATIONS at stream start).
func tableFromMapWith(tm *TableMap, collations map[uint64]collation) *source.Table {
	t := &source.Table{
		TableRef:    source.TableRef{Schema: tm.Schema, Table: tm.Table},
		Columns:     make([]source.Column, tm.Columns),
		HasStablePK: len(tm.PrimaryKey) > 0,
	}
	for i := 0; i < tm.Columns; i++ {
		name := fmt.Sprintf("col_%d", i+1)
		if i < len(tm.Names) && tm.Names[i] != "" {
			name = tm.Names[i]
		}
		unsigned := i < len(tm.Unsigned) && tm.Unsigned[i]
		nullable := i < len(tm.Nullable) && tm.Nullable[i]
		var typ byte = typeInvalid
		if i < len(tm.Types) {
			typ = tm.Types[i]
		}
		var meta uint16
		if i < len(tm.Meta) {
			meta = tm.Meta[i]
		}
		col := source.Column{Name: name, Nullable: nullable}
		if id := tm.collationFor(i); id != 0 {
			col.Charset, col.Collation = lookupCollation(collations, id)
		}
		// **先拿字符集再渲染类型**：binlog 里 VARCHAR 的长度是**字节**上限
		// （utf8mb4 的 VARCHAR(64) 记的是 256），按字节报给用户就是 `varchar(256)` ——
		// 用户明明写的是 64。类型字典比对与暂停提示都吃这个字符串，错一个字就全错。
		col.Type = mysqlTypeName(typ, meta, unsigned, charsetMaxLen(col.Charset))
		t.Columns[i] = col
	}
	for _, idx := range tm.PrimaryKey {
		if idx >= 0 && idx < len(t.Columns) {
			t.PrimaryKey = append(t.PrimaryKey, t.Columns[idx].Name)
		}
	}
	return t
}

// collationFor returns the collation id that applies to column i, walking the
// character columns in order (the metadata only names character columns).
func (tm *TableMap) collationFor(i int) uint64 {
	if i >= len(tm.Types) || !isCharacterType(tm.Types[i]) {
		return 0
	}
	ordinal := 0
	for j := 0; j < i; j++ {
		if j < len(tm.Types) && isCharacterType(tm.Types[j]) {
			ordinal++
		}
	}
	if ordinal < len(tm.ColumnCollations) {
		return tm.ColumnCollations[ordinal]
	}
	if v, ok := tm.CollationOverrides[ordinal]; ok {
		return v
	}
	return tm.DefaultCollation
}

// isCharacterType reports whether the column carries a charset/collation.
func isCharacterType(t byte) bool {
	switch t {
	case typeString, typeVarString, typeVarchar, typeBlob:
		return true
	}
	return false
}

// relationSignature fingerprints the definition so a new *source.Table is only
// allocated when the definition really changed — the contract asks connectors
// to reuse the same pointer for the same version (pointer equality = same
// version), so the engine can cache decoded column plans per pointer.
func relationSignature(tm *TableMap) string {
	var b strings.Builder
	b.WriteString(tm.Schema)
	b.WriteByte('.')
	b.WriteString(tm.Table)
	for i := 0; i < tm.Columns; i++ {
		b.WriteByte('|')
		var typ byte
		if i < len(tm.Types) {
			typ = tm.Types[i]
		}
		var meta uint16
		if i < len(tm.Meta) {
			meta = tm.Meta[i]
		}
		b.WriteString(fmt.Sprint(typ, "/", meta, "@", tm.collationFor(i)))
		if i < len(tm.Unsigned) && tm.Unsigned[i] {
			b.WriteByte('u')
		}
		if i < len(tm.Nullable) && tm.Nullable[i] {
			b.WriteByte('n')
		}
		if i < len(tm.Names) {
			b.WriteByte(':')
			b.WriteString(tm.Names[i])
		}
	}
	fmt.Fprintf(&b, "#pk%v", tm.PrimaryKey)
	return b.String()
}

// mysqlTypeName renders the column type the way a DBA would write it
// ("bigint unsigned", "varchar(64)", "datetime(6)").
//
// 这里只做「照实描述」，不做跨库映射：映射归类型字典（internal/catalog，Ark 的）。
func mysqlTypeName(t byte, meta uint16, unsigned bool, maxLen int) string {
	name, args := typeNameAndArgs(t, meta, maxLen)
	if unsigned {
		switch name {
		case "tinyint", "smallint", "mediumint", "int", "bigint", "decimal", "float", "double":
			return name + args + " unsigned"
		}
	}
	return name + args
}

// charsetMaxLen 是字符集的**单字符最大字节数**（mbmaxlen）。
//
// 只用来把 binlog 的「字节上限」换算回「字符数」。取不到字符集时返回 1：
// 字节数当字符数报，与 5.7/8.0 的 latin1 列一致；宁可保守，不要凭猜除出来一个假长度。
func charsetMaxLen(charset string) int {
	switch strings.ToLower(charset) {
	case "utf8mb4":
		return 4
	case "utf8mb3", "utf8":
		return 3
	case "ucs2", "utf16", "utf16le", "utf32":
		if strings.EqualFold(charset, "ucs2") {
			return 2
		}
		return 4
	case "gbk", "big5", "sjis", "cp932", "euckr", "gb2312":
		return 2
	case "gb18030":
		return 4
	default:
		return 1
	}
}

func typeNameAndArgs(t byte, meta uint16, maxLen int) (string, string) {
	fsp := int(meta & 0xff)
	switch t {
	case typeTiny:
		return "tinyint", ""
	case typeShort:
		return "smallint", ""
	case typeInt24:
		return "mediumint", ""
	case typeLong:
		return "int", ""
	case typeLongLong:
		return "bigint", ""
	case typeFloat:
		return "float", ""
	case typeDouble:
		return "double", ""
	case typeNewDecimal:
		precision := int(meta >> 8)
		scale := int(meta & 0xff)
		return "decimal", fmt.Sprintf("(%d,%d)", precision, scale)
	case typeBit:
		nbits := int(meta>>8)*8 + int(meta&0xff)
		return "bit", fmt.Sprintf("(%d)", nbits)
	case typeYear:
		return "year", ""
	case typeDate, typeNewDate:
		return "date", ""
	case typeTime:
		return "time", ""
	case typeTime2:
		return "time", fracArg(fsp)
	case typeDateTime:
		return "datetime", ""
	case typeDateTime2:
		return "datetime", fracArg(fsp)
	case typeTimestamp:
		return "timestamp", ""
	case typeTimestamp2:
		return "timestamp", fracArg(fsp)
	case typeBlob, typeTinyBlob, typeMediumBlob, typeLongBlob:
		switch meta {
		case 1:
			return "tinyblob", ""
		case 3:
			return "mediumblob", ""
		case 4:
			return "longblob", ""
		default:
			return "blob", ""
		}
	case typeVarchar:
		// meta 是**字节**上限：utf8mb4 的 VARCHAR(64) 在这里是 256。
		// 除不尽（理论上不该发生）就按字节报，并让调用方从字符串里看得出来。
		if maxLen > 1 && int(meta)%maxLen == 0 {
			return "varchar", fmt.Sprintf("(%d)", int(meta)/maxLen)
		}
		return "varchar", fmt.Sprintf("(%d)", meta)
	case typeVarString:
		// VARBINARY 没有字符集（按字节），不许除。
		return "varbinary", fmt.Sprintf("(%d)", meta)
	case typeString:
		realTyp, length := realType(typeString, meta)
		switch realTyp {
		case typeEnum:
			return "enum", ""
		case typeSet:
			return "set", ""
		default:
			return "char", fmt.Sprintf("(%d)", length)
		}
	case typeJSON:
		return "json", ""
	case typeGeometry:
		return "geometry", ""
	case typeVector:
		return "vector", ""
	case typeEnum:
		return "enum", ""
	case typeSet:
		return "set", ""
	case typeNull:
		return "null", ""
	default:
		return fmt.Sprintf("type_%d", t), ""
	}
}

func fracArg(fsp int) string {
	if fsp <= 0 {
		return ""
	}
	return fmt.Sprintf("(%d)", fsp)
}
