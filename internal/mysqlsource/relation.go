package mysqlsource

import (
	"fmt"
	"strings"

	"github.com/spencerswagger/jinliang/internal/source"
)

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
		col := source.Column{
			Name:     name,
			Type:     mysqlTypeName(typ, meta, unsigned),
			Nullable: nullable,
		}
		if id := tm.collationFor(i); id != 0 {
			col.Charset, col.Collation = lookupCollation(collations, id)
		}
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
func mysqlTypeName(t byte, meta uint16, unsigned bool) string {
	name, args := typeNameAndArgs(t, meta)
	if unsigned {
		switch name {
		case "tinyint", "smallint", "mediumint", "int", "bigint", "decimal", "float", "double":
			return name + args + " unsigned"
		}
	}
	return name + args
}

func typeNameAndArgs(t byte, meta uint16) (string, string) {
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
		return "varchar", fmt.Sprintf("(%d)", meta)
	case typeVarString:
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
