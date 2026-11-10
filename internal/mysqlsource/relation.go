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
	// 5.7 的 TABLE_MAP **不带列字符集**（没有可选元数据那一套），于是 VARCHAR/CHAR 的长度
	// 只能按字节报（utf8mb4 的 VARCHAR(64) → `varchar(256)`）。这会让：
	//   - 提示文案说错长度；
	//   - 与 catalog（字符）比对时「同一列」被判成改类型 → 每张多字节字符列都假暂停。
	//
	// 处理：binlog 没给字符集、而 catalog 给了的时候，**按 catalog 的字符集把字节换算回字符**
	// （`charsetMaxLen`）。误差方向要写清：换算用的是「流开始那一刻」的字符集——
	// 若源端在流中把列字符集也改了，我们会按旧字符集算，长度对不上 → **多暂停一次**（安全方向），
	// 不会出现「该停没停」。catalog 也对不上（列数变了）时不做换算，保持字节原样，同样走「暂停」。
	for i := range cp.Columns {
		if cp.Columns[i].Charset != "" || i >= len(known.Columns) {
			continue
		}
		kind := catalogBinaryKind(known.Columns[i])
		if cs := known.Columns[i].Charset; cs != "" {
			cp.Columns[i].Charset = cs
			cp.Columns[i].Collation = known.Columns[i].Collation
		}
		if kind != binUnknown {
			// 二进制/字符的**基名**也按 catalog 纠正：5.7 的 binlog 分不开 CHAR 与 BINARY，
			// 渲染出来是 `char(4)`，而 catalog 明明白白写着 `binary(4)`。
			// 只纠基名、长度仍来自 binlog（长度变化照样能看见）。
			cp.Columns[i].Type = rerenderForKind(cp.Columns[i].Type, kind)
		}
		if cp.Columns[i].Charset != "" {
			cp.Columns[i].Type = bytesToCharsType(cp.Columns[i].Type, cp.Columns[i].Charset)
		}
	}
	// 「还能不能当成同一个类型」而不是「列数一样就覆盖」：
	//
	// 列数一样时整体套 catalog 会把**改类型**盖掉（`MODIFY COLUMN v VARCHAR(64)` 看不见），
	// 而一律用 binlog 的类型又会在 MINIMAL 下把 `char(10)` 说成 `char(40)`、
	// 把 `text` 说成 `blob`（binlog 里没有字符集，只有字节上限），反过来制造假暂停。
	//
	// 所以中间这条路：**宽松同型**（见 looseSameType）就信 catalog（它带字符集、是权威），
	// 不同型就说明结构真的变了 → 保留 binlog 的新类型，让引擎按表暂停。
	if len(known.Columns) == len(cp.Columns) && columnsLooseSame(cp.Columns, known.Columns) {
		cp.Columns = append([]source.Column(nil), known.Columns...)
		cp.PrimaryKey = append([]string(nil), known.PrimaryKey...)
		cp.HasStablePK = known.HasStablePK
	} else {
		for i := range cp.Columns {
			if i < len(known.Columns) && known.Columns[i].Name != "" {
				cp.Columns[i].Name = known.Columns[i].Name
			}
		}
		if len(known.Columns) == len(cp.Columns) {
			// 列数没变但类型不同型：名字能对上的按序号补，类型**保留 binlog 的新值**。
			cp.PrimaryKey = append([]string(nil), known.PrimaryKey...)
			cp.HasStablePK = known.HasStablePK
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

// bytesToCharsType 把按字节渲染的类型换成按字符（`varchar(256)` + utf8mb4 → `varchar(64)`）。
//
// 只动 CHAR / VARCHAR：BINARY / VARBINARY / BLOB 本来就是字节计（charset=binary 的
// mbmaxlen 是 1，除下去也不变），TEXT 家族的长度在 binlog 里是**尺寸档**不是字符数。
// 除不尽（不该发生）时原样返回——不猜，留给比对去判「变了」。
func bytesToCharsType(t, charset string) string {
	base, args := splitTypeString(t)
	if base != "varchar" && base != "char" {
		return t
	}
	n, ok := atoiArgs(args)
	if !ok {
		return t
	}
	mb := charsetMaxLen(charset)
	if mb <= 1 || n%mb != 0 {
		return t
	}
	return fmt.Sprintf("%s(%d)", base, n/mb)
}

// columnsLooseSame 逐列判断「binlog 给的类型」与「catalog 的类型」还能不能算同一个。
func columnsLooseSame(binlogCols, catalogCols []source.Column) bool {
	if len(binlogCols) != len(catalogCols) {
		return false
	}
	for i := range binlogCols {
		if !looseSameType(binlogCols[i].Type, catalogCols[i].Type) {
			return false
		}
	}
	return true
}

// looseSameType 判断两个类型串「在 binlog 元数据不全的前提下」是不是同一个类型。
//
// 为什么需要它：MINIMAL 元数据里 VARCHAR/CHAR 的长度是**字节**、BLOB 与 TEXT 共用
// 一个类型字节，所以 `varchar(64)`（catalog，字符）与 `varchar(256)`（binlog，字节）
// 说的是同一列。直接字符串比对会把「同一列」判成「改类型」，反过来把「改类型」判成
// 「没变」——两头都是错，所以这里按**可能的等价形态**放行：
//
//	varbinary(N)          ≈ varchar(N)（二进制改字符集/反过来，长度单位相同）
//	tinytext/text/…       ≈ 同尺寸档的 blob 家族（TEXT 与 BLOB 共用类型字节）
//
// **长度不再放行任何倍数**：字节→字符的换算已经在 enrichRelation 里按 catalog 字符集做完了
// （5.7 那条路）。剩下的「未知」场景宁可判不同 → 多暂停一次（安全方向），
// 也不许把 latin1 的 `varchar(64)→varchar(128)`（字节恰好 ×2）当成「utf8mb3 的同一列」放过。
//
// 除此之外一律「不同型」——宁可让上层报一次结构变化，也不要把新类型盖回旧的。
func looseSameType(binlogType, catalogType string) bool {
	bBase, bArgs := splitTypeString(binlogType)
	cBase, cArgs := splitTypeString(catalogType)
	if bBase == cBase {
		if bArgs == cArgs || bArgs == "" || cArgs == "" {
			return bArgs == cArgs
		}
		// 参数不同：**长度必须逐字相等**才算同一列。
		// 字节↔字符的换算已经在 enrichRelation 里按 catalog 字符集做完了（5.7 那条路），
		// 这里再放行任何倍数都等于给「真改了长度」开一条静默通道——宁可多暂停一次。
		return false
	}
	// varbinary(N) vs varchar(N)：binary 字符集在 MINIMAL 里拿不到，名字会变。
	if (bBase == "varbinary" && cBase == "varchar") || (bBase == "varchar" && cBase == "varbinary") {
		return bArgs == cArgs
	}
	// TEXT 与 BLOB 共用类型字节：只有尺寸档能信。
	if textBlobClass(bBase) != "" && textBlobClass(bBase) == textBlobClass(cBase) {
		return true
	}
	return false
}

// textBlobClass 返回尺寸档（tiny/medium/long/空），text 与 blob 归到同一档。
func textBlobClass(base string) string {
	switch base {
	case "tinytext", "tinyblob":
		return "tiny"
	case "text", "blob":
		return "normal"
	case "mediumtext", "mediumblob":
		return "medium"
	case "longtext", "longblob":
		return "long"
	}
	return ""
}

// splitTypeString 把 `varchar(64)` 拆成 base 与 args（args 去掉括号）。
func splitTypeString(t string) (base, args string) {
	i := strings.IndexByte(t, '(')
	if i < 0 {
		return t, ""
	}
	base = t[:i]
	args = strings.TrimSuffix(t[i+1:], ")")
	return base, args
}

func atoiArgs(args string) (int, bool) {
	n := 0
	if args == "" {
		return 0, false
	}
	for _, r := range args {
		if r < '0' || r > '9' {
			return 0, false
		}
		n = n*10 + int(r-'0')
	}
	return n, true
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
		col.Type = mysqlTypeName(typ, meta, unsigned, col.Charset)
		// 基名也要对：5.7 的 binlog 分不开 CHAR/BINARY、VARCHAR/VARBINARY、TEXT/BLOB
		// （共用类型字节，又没有排序规则）。catalog 的 kind 已经补进 ColumnKinds，
		// 这里就地纠正——否则所有拿到 Relation 的路径（引擎、测试、将来的调用方）
		// 都会看到 `char(4)` 而 catalog 是 `binary(4)`。
		col.Type = rerenderForKind(col.Type, tm.columnBinaryKind(i))
		t.Columns[i] = col
	}
	for _, idx := range tm.PrimaryKey {
		if idx >= 0 && idx < len(t.Columns) {
			t.PrimaryKey = append(t.PrimaryKey, t.Columns[idx].Name)
		}
	}
	return t
}

// binaryKind 是「这一列按字节存还是按字符存」的三态。
//
// 为什么不是 bool：5.7 的 TABLE_MAP 没有可选元数据（不给排序规则），此时**说不上来**。
// 说不上来 ≠ 按字符存——那会把 BINARY 当成文本；也 ≠ 按字节存——那会把中文 VARCHAR
// 变成 KindBytes，pgsink 写 text 列会报编码错。这种情况只能退回内容启发式。
type binaryKind uint8

const (
	binUnknown binaryKind = iota // 排序规则拿不到（5.7 / 非字符列）
	binText                      // 按字符存：CHAR / VARCHAR / TEXT
	binBinary                    // 按字节存：BINARY / VARBINARY / BLOB
)

// binaryCollationID 是 MySQL 的 `binary` 排序规则（字符集 `binary`）的固定 id。
//
// 用它当判据而不是「字符集名字 == binary」：名字要先查 collation 表，而 id 就在
// TABLE_MAP 里（FULL 元数据）。分不开的后果不是显示错：
//   - `BINARY(4)='abcd'` 被当成 KindString → pgsink 走 `$n::text::bytea`，
//     值以反斜杠开头时会被 PG **静默改写**（不是报错，是错值）；
//   - 类型串把 binary 写成 varchar → 「改类型」判定把 binary↔char 的改动看漏。
const binaryCollationID = 63

// columnBinaryKind 判断第 i 列按字节存还是按字符存。
//
// 顺序很关键：**先信 catalog 补进来的 ColumnKinds**（5.7 只有它有），再退回
// TABLE_MAP 的排序规则（8.0+），最后才是「不知道」。
func (tm *TableMap) columnBinaryKind(i int) binaryKind {
	if i < len(tm.ColumnKinds) && tm.ColumnKinds[i] != binUnknown {
		return tm.ColumnKinds[i]
	}
	switch tm.collationFor(i) {
	case 0:
		return binUnknown
	case binaryCollationID:
		return binBinary
	default:
		return binText
	}
}

// FillColumnKinds 用 catalog 的字符集补「哪一列按字节存」。
//
// 只在 TABLE_MAP 给不出排序规则时（5.7）才需要，但因为写入的是 ColumnKinds、
// 而 columnBinaryKind 优先看它，8.0 上补了也无害（两边结论一致）。
// 对不上的情形一律留 binUnknown：宁可退回内容启发式，也不要凭序号猜错列。
func FillColumnKinds(tm *TableMap, known *source.Table) {
	if tm == nil || known == nil || len(known.Columns) != tm.Columns {
		return
	}
	kinds := make([]binaryKind, tm.Columns)
	any := false
	for i := range kinds {
		kinds[i] = catalogBinaryKind(known.Columns[i])
		if kinds[i] != binUnknown {
			any = true
		}
	}
	if any {
		tm.ColumnKinds = kinds
	}
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
func mysqlTypeName(t byte, meta uint16, unsigned bool, charset string) string {
	name, args := typeNameAndArgs(t, meta, charset)
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
	case "gbk", "big5", "sjis", "cp932", "euckr", "gb2312", "eucjpms", "ujis":
		if strings.EqualFold(charset, "ujis") || strings.EqualFold(charset, "eucjpms") {
			return 3 // ujis / eucjpms 是 3 字节字符集（漏了会把 varchar(50) 报成 varchar(150)）
		}
		return 2
	case "gb18030":
		return 4
	default:
		return 1
	}
}

func typeNameAndArgs(t byte, meta uint16, charset string) (string, string) {
	fsp := int(meta & 0xff)
	// binary 字符集 = 按字节存的列（VARBINARY / BLOB）；非 binary = 按字符存的（VARCHAR / TEXT）。
	// 两者在 binlog 里共用同一个类型字节，只能靠字符集分开——这也是 TEXT 之前被报成 blob 的原因。
	binary := strings.EqualFold(charset, "binary")
	maxLen := charsetMaxLen(charset)
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
		// BLOB 与 TEXT 共用 MYSQL_TYPE_BLOB，只有字符集能分开：
		// 报错方向会让「改类型」判定凭空触发（catalog 是 text、binlog 说 blob → 停在第一行）。
		fam := [4]string{"blob", "tinyblob", "mediumblob", "longblob"}
		if !binary {
			fam = [4]string{"text", "tinytext", "mediumtext", "longtext"}
		}
		switch meta {
		case 1:
			return fam[1], ""
		case 3:
			return fam[2], ""
		case 4:
			return fam[3], ""
		default:
			return fam[0], ""
		}
	case typeVarchar:
		// meta 是**字节**上限：utf8mb4 的 VARCHAR(64) 在这里是 256。
		// binary 字符集的列是真·VARBINARY（MySQL 也是用 VARCHAR 的类型字节存的），
		// 名字得跟着走，否则 catalog 的 varbinary 和 binlog 的 varchar 会互相误判。
		if binary {
			return "varbinary", fmt.Sprintf("(%d)", meta)
		}
		// 除不尽（理论上不该发生）就按字节报，不猜。
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
			// CHAR 的长度同样是**字节**（utf8mb4 的 CHAR(10) 记 40 → 之前报 char(40)）。
			if binary {
				return "binary", fmt.Sprintf("(%d)", length)
			}
			if maxLen > 1 && length%maxLen == 0 {
				return "char", fmt.Sprintf("(%d)", length/maxLen)
			}
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

// catalogBinaryKind 从 catalog 的一列判断「按字节存还是按字符存」。
//
// 两个来源，按可靠性排序：
//  1. `CHARACTER_SET_NAME`：非 binary 的字符集 → 按字符存（utf8mb4/latin1/…）。
//     **但 MySQL 对 BINARY/VARBINARY/BLOB 列不给字符集（NULL）**——所以「空」不等于
//     「不知道」，它恰好是二进制列最常见的形态（真库实测：`binary(4)` 的
//     CHARACTER_SET_NAME 是 NULL，而 `char(10)` 是 utf8mb4）。
//  2. 因此空字符集时看**类型名**：binary/varbinary/blob 家族 → 按字节；
//     char/varchar/text 家族 → 按字符；其余（数值/时间）与行列镜像无关，留 binUnknown。
func catalogBinaryKind(c source.Column) binaryKind {
	if cs := c.Charset; cs != "" {
		if strings.EqualFold(cs, "binary") {
			return binBinary
		}
		return binText
	}
	base := strings.ToLower(c.Type)
	if i := strings.IndexAny(base, "( "); i >= 0 {
		base = base[:i]
	}
	switch base {
	case "binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob", "geometry":
		return binBinary
	case "char", "varchar", "tinytext", "text", "mediumtext", "longtext", "enum", "set":
		return binText
	}
	return binUnknown
}

// rerenderForKind 用「按字节存 / 按字符存」把类型的**基名**换成对的那个。
//
// 为什么需要：binlog 里 CHAR 与 BINARY、VARCHAR 与 VARBINARY、TEXT 与 BLOB 共用同一个
// 类型字节，5.7 又没有排序规则，渲染出来一律是字符家族的名字（`char(4)`）。
// catalog 知道真相（`binary(4)`），把基名换掉，长度保持 binlog 的——这样
// 「长度真的变了」仍然看得见，只有「binary↔char 换了」在 5.7 上看不见（已知边界）。
func rerenderForKind(t string, kind binaryKind) string {
	base, args := splitTypeString(t)
	var to string
	switch kind {
	case binBinary:
		to = map[string]string{"char": "binary", "varchar": "varbinary",
			"tinytext": "tinyblob", "text": "blob", "mediumtext": "mediumblob", "longtext": "longblob"}[base]
	case binText:
		to = map[string]string{"binary": "char", "varbinary": "varchar",
			"tinyblob": "tinytext", "blob": "text", "mediumblob": "mediumtext", "longblob": "longtext"}[base]
	}
	if to == "" {
		return t
	}
	if args == "" {
		return to
	}
	return to + "(" + args + ")"
}
