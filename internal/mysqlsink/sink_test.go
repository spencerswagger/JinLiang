package mysqlsink

import (
	"strings"
	"testing"
	"time"

	"github.com/spencerswagger/jinliang/internal/source"
)

func TestClassifyDDLWhitelist(t *testing.T) {
	cases := []struct {
		stmt  string
		class DDLClass
		why   string
	}{
		// 白名单四类
		{"ALTER TABLE `shop`.`orders` ADD COLUMN note VARCHAR(64) NULL", DDLAllow, "加列"},
		{"ALTER TABLE t ADD INDEX idx_note (note)", DDLAllow, "加普通索引"},
		{"CREATE INDEX idx_x ON t (x)", DDLAllow, "CREATE INDEX"},
		// MODIFY/CHANGE 需要先读目标端旧类型，纯分类只能给「需确认」，由 Sink.classify 补齐
		{"ALTER TABLE t MODIFY COLUMN note VARCHAR(128) NULL", DDLConfirm, "改列类型要先比旧类型"},

		// 需确认
		{"ALTER TABLE t ADD UNIQUE KEY uk (a)", DDLConfirm, "加唯一索引会改 upsert 判据"},
		{"ALTER TABLE t DROP COLUMN note", DDLConfirm, "删列"},

		// 跳过
		{"ALTER TABLE t DROP INDEX idx", DDLSkip, "删索引不影响行数据"},
		{"ALTER TABLE t COMMENT = 'x'", DDLSkip, "表属性"},
		{"ALTER TABLE t ENGINE = InnoDB", DDLSkip, "表属性"},

		// 拒绝
		{"DROP TABLE t", DDLReject, "DROP 丢数据"},
		{"TRUNCATE TABLE t", DDLReject, "TRUNCATE 不在白名单"},
		{"RENAME TABLE a TO b", DDLReject, "重命名"},
		{"CREATE TABLE x (id int)", DDLReject, "建表由 EnsureSchema 负责"},
		{"ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY (a) REFERENCES b(id)", DDLReject, "外键"},
		{"GRANT SELECT ON *.* TO u", DDLReject, "未知语句类型"},
	}
	for _, c := range cases {
		got := ClassifyDDL(c.stmt)
		if got.Class != c.class {
			t.Fatalf("%q 判成 %s，期望 %s（%s）；理由=%q", c.stmt, got.Class, c.class, c.why, got.Reason)
		}
		if got.Reason == "" {
			t.Fatalf("%q 的判定必须带人话理由", c.stmt)
		}
	}
}

func TestDecideColumnChange(t *testing.T) {
	cases := []struct {
		oldType, newType string
		want             DDLClass
	}{
		{"varchar(64)", "varchar(128)", DDLAllow},  // 放宽
		{"varchar(128)", "varchar(64)", DDLReject}, // 改窄
		{"int", "bigint", DDLAllow},                // 整型放大
		{"bigint", "int", DDLReject},               // 整型缩小
		{"varchar(64)", "int", DDLReject},          // 跨家族
		{"varchar(64)", "varchar(64)", DDLAllow},   // 只改 NOT NULL/DEFAULT
	}
	for _, c := range cases {
		d := DecideColumnChange(c.oldType, c.newType)
		if d.Class != c.want {
			t.Fatalf("%s → %s 判成 %s，期望 %s（%s）", c.oldType, c.newType, d.Class, c.want, d.Reason)
		}
	}
}

func TestParseModify(t *testing.T) {
	col, typ, renamed, ok := parseModify("MODIFY COLUMN note VARCHAR(128) NOT NULL")
	if !ok || col != "note" || typ != "VARCHAR(128)" || renamed {
		t.Fatalf("MODIFY 解析 = (%q,%q,%v,%v)", col, typ, renamed, ok)
	}
	col, typ, renamed, ok = parseModify("CHANGE COLUMN old_name new_name VARCHAR(64) NULL")
	if !ok || col != "new_name" || typ != "VARCHAR(64)" || !renamed {
		t.Fatalf("CHANGE 改名解析 = (%q,%q,%v,%v)", col, typ, renamed, ok)
	}
	col, typ, renamed, ok = parseModify("CHANGE COLUMN note note VARCHAR(64)")
	if !ok || col != "note" || renamed {
		t.Fatalf("CHANGE 不改名应判成未改名: (%q,%q,%v,%v)", col, typ, renamed, ok)
	}
}

func TestClassifyDDLExtractsTable(t *testing.T) {
	d := ClassifyDDL("ALTER TABLE `shop`.`orders` ADD COLUMN note VARCHAR(64)")
	if d.Table.Schema != "shop" || d.Table.Table != "orders" {
		t.Fatalf("表名解析错了: %#v", d.Table)
	}
	d = ClassifyDDL("CREATE INDEX i ON `t2` (x)")
	if d.Table.Table != "t2" {
		t.Fatalf("CREATE INDEX 的表名解析错了: %#v", d.Table)
	}
}

func TestUpsertSQLForm(t *testing.T) {
	table := source.TableRef{Schema: "shop", Table: "orders"}
	cols := []string{"id", "note"}
	update := []string{"note"}

	legacy := &Sink{}
	got := legacy.upsertSQL(table, cols, 2, update)
	want := "INSERT INTO `shop`.`orders` (`id`,`note`) VALUES (?,?),(?,?) ON DUPLICATE KEY UPDATE `note`=VALUES(`note`)"
	if got != want {
		t.Fatalf("5.7/8.0.18 形态不对:\n got %s\nwant %s", got, want)
	}
	if strings.Contains(got, "IGNORE") {
		t.Fatal("upsert 不能退化成 INSERT IGNORE（遇到重复键会丢掉更新）")
	}

	alias := &Sink{aliasUpsert: true}
	got = alias.upsertSQL(table, cols, 1, update)
	want = "INSERT INTO `shop`.`orders` (`id`,`note`) VALUES (?,?) AS new ON DUPLICATE KEY UPDATE `note`=new.`note`"
	if got != want {
		t.Fatalf("8.0.19+ 形态不对:\n got %s\nwant %s", got, want)
	}
}

func TestUpsertSQLOnlyPrimaryKeyStillValid(t *testing.T) {
	// 只有主键列时不能生成 "... ON DUPLICATE KEY UPDATE " 空尾巴（语法错误）。
	s := &Sink{}
	got := s.upsertSQL(source.TableRef{Table: "t"}, []string{"id"}, 1, nil)
	if strings.HasSuffix(strings.TrimSpace(got), "UPDATE") {
		t.Fatalf("update 列表为空时会生成非法 SQL: %s", got)
	}
}

func TestInsertSQLForm(t *testing.T) {
	// 无主键表的 insert-only 路径：多行纯 INSERT，**不能**出现 ON DUPLICATE KEY
	//（那需要唯一键；设计 §1.2 路线 A）。
	table := source.TableRef{Schema: "shop", Table: "no_pk"}
	got := insertSQL(table, []string{"a", "b"}, 2)
	want := "INSERT INTO `shop`.`no_pk` (`a`,`b`) VALUES (?,?),(?,?)"
	if got != want {
		t.Fatalf("insert-only 形态不对:\n got %s\nwant %s", got, want)
	}
	if strings.Contains(got, "ON DUPLICATE") || strings.Contains(got, "IGNORE") {
		t.Fatalf("insert-only 不能带 ON DUPLICATE/IGNORE: %s", got)
	}
}

func TestInsertOnlyDecision(t *testing.T) {
	// 身份只认源端（R1）：无稳定主键 → insert-only；有主键或拿不到 Relation → 不猜。
	if !insertOnly(&source.Table{HasStablePK: false}) {
		t.Fatal("源端无稳定主键的表必须是 insert-only")
	}
	if insertOnly(&source.Table{HasStablePK: true, PrimaryKey: []string{"id"}}) {
		t.Fatal("源端有主键的表不能走 insert-only")
	}
	if insertOnly(nil) {
		t.Fatal("拿不到 Relation 时不能猜 insert-only（没有身份信息就按老规矩要求唯一键）")
	}
}

func TestDeleteSQLRequiresPrimaryKey(t *testing.T) {
	if _, err := deleteSQL(source.TableRef{Table: "t"}, nil); err == nil {
		t.Fatal("没有主键时必须拒绝删除（否则可能变成全表删除）")
	}
	got, err := deleteSQL(source.TableRef{Schema: "s", Table: "t"}, []string{"id", "k"})
	if err != nil {
		t.Fatal(err)
	}
	want := "DELETE FROM `s`.`t` WHERE `id`=? AND `k`=?"
	if got != want {
		t.Fatalf("删除语句 = %q，期望 %q", got, want)
	}
}

func TestCheckRowWidth(t *testing.T) {
	cat := source.Catalog{Tables: []source.Table{{
		TableRef: source.TableRef{Schema: "shop", Table: "orders"},
		Columns: []source.Column{
			{Name: "big", Type: "varchar(1000)"}, // 4002 > 3072
			{Name: "ok", Type: "varchar(700)"},   // 2802 ≤ 3072
			{Name: "tiny", Type: "varchar(64)"},  // 258
			{Name: "body", Type: "longtext"},     // 行内只有指针，不该报
			{Name: "amt", Type: "decimal(12,2)"}, // 非字符，不该报
			{Name: "ch", Type: "char(800)"},      // 3200 > 3072
		},
	}}}

	issues := CheckRowWidth(cat, DefaultWidthOptions())
	got := map[string]WidthIssue{}
	for _, i := range issues {
		got[i.Column] = i
	}
	if _, ok := got["big"]; !ok {
		t.Fatal("varchar(1000) 在 utf8mb4 下必须报警")
	}
	if _, ok := got["ch"]; !ok {
		t.Fatal("char(800) 必须报警")
	}
	for _, skip := range []string{"ok", "tiny", "body", "amt"} {
		if _, ok := got[skip]; ok {
			t.Fatalf("%s 不该报警（会造成假警告）", skip)
		}
	}
	w := got["big"]
	if w.Bytes <= w.Limit {
		t.Fatalf("估算字节必须超过上限: %#v", w)
	}
	for _, want := range []string{"shop.orders", "big", "varchar(1000)", "text", "INDEX"} {
		if !strings.Contains(w.String(), want) {
			t.Fatalf("警告里应包含 %q（哪张表哪一列/估算/上限/怎么改）: %s", want, w.String())
		}
	}
}

func TestWidthLegacyRowFormatUses767(t *testing.T) {
	cat := source.Catalog{Tables: []source.Table{{
		TableRef: source.TableRef{Schema: "s", Table: "t"},
		Columns:  []source.Column{{Name: "c", Type: "varchar(200)"}}, // 802 > 767
	}}}
	if issues := CheckRowWidth(cat, DefaultWidthOptions()); len(issues) != 0 {
		t.Fatalf("DYNAMIC 行格式下 200 字符不该报: %#v", issues)
	}
	legacy := WidthOptions{LargePrefix: false, CharsetBytes: 4}
	issues := CheckRowWidth(cat, legacy)
	if len(issues) != 1 || issues[0].Limit != 767 {
		t.Fatalf("COMPACT/REDUNDANT 行格式上限应是 767: %#v", issues)
	}
}

func TestWidthLatin1NotFlagged(t *testing.T) {
	// latin1 列按 1 字节/字符算：varchar(1000) = 1000 字节，不该报警。
	cat := source.Catalog{Tables: []source.Table{{
		TableRef: source.TableRef{Schema: "s", Table: "t"},
		Columns:  []source.Column{{Name: "c", Type: "varchar(1000)"}},
	}}}
	opt := WidthOptions{LargePrefix: true, CharsetBytes: 1}
	if issues := CheckRowWidth(cat, opt); len(issues) != 0 {
		t.Fatalf("latin1 不该按 utf8mb4 算: %#v", issues)
	}
}

func TestEndsWithCommit(t *testing.T) {
	cases := []struct {
		evs  []source.EventKind
		want bool
	}{
		{[]source.EventKind{source.EvBegin, source.EvInsert, source.EvCommit}, true},
		{[]source.EventKind{source.EvBegin, source.EvInsert, source.EvCommit, source.EvHeartbeat}, true},
		{[]source.EventKind{source.EvBegin, source.EvInsert}, false},
		{nil, false},
	}
	for _, c := range cases {
		var evs []source.Event
		for _, k := range c.evs {
			evs = append(evs, source.Event{Kind: k})
		}
		if got := endsWithCommit(evs); got != c.want {
			t.Fatalf("%v → %v，期望 %v", c.evs, got, c.want)
		}
	}
}

func TestDriverValueJSONUsesText(t *testing.T) {
	// 契约要求 JSON 以 UTF-8 文本交付；sink 不该把它当二进制写进 json 列。
	v := source.Value{Kind: source.KindJSON, S: `{"a":1}`}
	if got := driverValue(v); got != `{"a":1}` {
		t.Fatalf("JSON 应按文本写入，拿到 %#v", got)
	}
}

func TestExcludeKeepsOrder(t *testing.T) {
	got := exclude([]string{"id", "b", "c"}, []string{"ID"})
	if strings.Join(got, ",") != "b,c" {
		t.Fatalf("排除主键后应保持原顺序: %v", got)
	}
}

// 租约的接管判定是纯函数：把「同机重启会不会自己挡自己」这类分支一次说清。
func TestLeaseTakeover(t *testing.T) {
	now := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	future := now.Add(20 * time.Second)
	past := now.Add(-time.Second)
	alive := func(pid int) bool { return pid == 4242 }
	dead := func(pid int) bool { return false }

	cases := []struct {
		name    string
		cur     leaseRow
		exists  bool
		inst    string
		alive   func(int) bool
		wantOK  bool
		mustNot string // 拒绝时不该出现的方向
	}{
		{"没有租约", leaseRow{}, false, "me", alive, true, ""},
		{"已过期", leaseRow{InstanceID: "other", Label: "job#1", Expires: past}, true, "me", alive, true, ""},
		{"别的实例而且在有效期内", leaseRow{InstanceID: "other", Label: "job#1", Expires: future}, true, "me", alive, false, ""},
		{"自己续租", leaseRow{InstanceID: "me", Label: "job#4242#tok", Expires: future}, true, "me", alive, true, ""},
		{"同机同 pid 不带 token", leaseRow{InstanceID: "me", Label: "job#4242", Expires: future}, true, "me", alive, true, ""},
		{"同机另一个活着的进程", leaseRow{InstanceID: "me", Label: "job#4242#a", Expires: future}, true, "me", alive, false, ""},
		{"同机崩溃进程留下的", leaseRow{InstanceID: "me", Label: "job#4242#a", Expires: future}, true, "me", dead, true, ""},
		{"老记录没有 pid", leaseRow{InstanceID: "me", Label: "job", Expires: future}, true, "me", alive, true, ""},
	}
	for _, c := range cases {
		ok, why := leaseTakeover(c.cur, c.exists, now, c.inst, 4242, "tok", c.alive)
		if ok != c.wantOK {
			t.Fatalf("%s: 判定 %v，期望 %v（why=%q）", c.name, ok, c.wantOK, why)
		}
		if !ok && why == "" {
			t.Fatalf("%s: 拒绝时必须说清谁在跑", c.name)
		}
	}
}

func TestLeaseLabelRoundTrip(t *testing.T) {
	for _, c := range []struct {
		label string
		pid   int
		token string
	}{
		{"job#123", 123, ""},
		{"my#job#123", 123, ""},
		{"job#123#abc", 123, "abc"},
		{"my#job#123#abc", 123, "abc"},
	} {
		pid, token, ok := parseLeaseLabel(c.label)
		if !ok || pid != c.pid || token != c.token {
			t.Fatalf("%q 解析成 (%d,%q,%v)，期望 (%d,%q)", c.label, pid, token, ok, c.pid, c.token)
		}
	}
	if _, _, ok := parseLeaseLabel("job"); ok {
		t.Fatal("没有 pid 的 label 应返回 ok=false")
	}
}
