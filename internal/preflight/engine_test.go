package preflight

import (
	"strings"
	"testing"

	"github.com/spencerswagger/jinliang/internal/source"
)

// realEngines 是 MySQL 8.0.28 `information_schema.ENGINES` 的实测值（TRANSACTIONS='YES' → true）。
// 判据读的就是这张表（source.Catalog.Engines），测试用它当「服务器自报」的桩。
//
// 刻意**不带任何引擎名黑名单**：判据改成查表之后，这里只是桩数据，
// 新增引擎（MyRocks…）不需要改生产代码。
func realEngines() map[string]bool {
	return map[string]bool{
		"INNODB":             true,
		"MYISAM":             false,
		"MRG_MYISAM":         false,
		"MEMORY":             false,
		"CSV":                false,
		"ARCHIVE":            false,
		"BLACKHOLE":          false,
		"PERFORMANCE_SCHEMA": false,
		// FEDERATED 的 TRANSACTIONS 是 NULL → 与 NO 同侧；本机未安装（SUPPORT=NO），
		// 所以真库上这个键可能压根不出现，同样落降级（第 5 格）。
		"FEDERATED": false,
	}
}

// 三态与取值是文档契约（docs/SPEC.md MY-08 按这三个值逐条对齐，Ark 定稿）：
// 改字符串 = 改文档口径，必须两处同改。
func TestEngineVerdictValuesAreContract(t *testing.T) {
	for _, want := range [][2]string{
		{"transactional", string(EngineTransactional)},
		{"non_transactional", string(EngineNonTransactional)},
		{"not_judged", string(EngineNotJudged)},
	} {
		if want[0] != want[1] {
			t.Fatalf("EngineVerdict 取值是文档契约：期望 %q，实际 %q", want[0], want[1])
		}
	}
}

// 这一格是 Ark 对齐表（notes/my08-engine-verdict-spec.md §2）的 1–9 行代码侧：
// 每一行一条断言，谁改了口径这里先红。
func TestJudgeEnginesAlignsWithMY08Table(t *testing.T) {
	eng := func(name, engine string) source.Table {
		return source.Table{TableRef: source.TableRef{Schema: "shop", Table: name}, Engine: engine}
	}

	cases := []struct {
		row      string
		kind     string // 空 = mysql
		cat      *source.Catalog
		selected []string
		want     EngineVerdict
		tables   []string
		reason   string // 期望出现在 Reason 里的一句话（空=不查）
	}{
		{
			row:      "2 InnoDB → transactional",
			cat:      catalogWith(realEngines(), eng("orders", "InnoDB")),
			selected: []string{"orders"}, want: EngineTransactional,
		},
		{
			row:      "3 MyISAM → 降级",
			cat:      catalogWith(realEngines(), eng("legacy", "MyISAM")),
			selected: []string{"legacy"}, want: EngineNonTransactional,
			tables: []string{"legacy(MyISAM)"}, reason: "MyISAM 没有事务",
		},
		{
			row:      "3 MRG_MyISAM（大写回报）→ 降级",
			cat:      catalogWith(realEngines(), eng("mrg", "MRG_MyISAM")),
			selected: []string{"mrg"}, want: EngineNonTransactional,
			tables: []string{"mrg(MRG_MyISAM)"}, reason: "MRG_MyISAM 没有事务",
		},
		{
			row:      "3 MRG_MyIsam（历史写法）大小写不敏感 → 降级",
			cat:      catalogWith(realEngines(), eng("mrg", "MRG_MyIsam")),
			selected: []string{"mrg"}, want: EngineNonTransactional,
			tables: []string{"mrg(MRG_MyIsam)"},
		},
		{
			row: "4 MEMORY/CSV/ARCHIVE/BLACKHOLE 同档降级",
			cat: catalogWith(realEngines(),
				eng("m", "MEMORY"), eng("c", "CSV"), eng("a", "ARCHIVE"), eng("b", "BLACKHOLE")),
			selected: []string{"m", "c", "a", "b"}, want: EngineNonTransactional,
			tables: []string{"a(ARCHIVE)", "b(BLACKHOLE)", "c(CSV)", "m(MEMORY)"},
			reason: "ARCHIVE、BLACKHOLE、CSV、MEMORY 没有事务",
		},
		{
			row:      "5 引擎名查不到（ROCKSDB）→ 降级，不许当 ok",
			cat:      catalogWith(realEngines(), eng("r", "ROCKSDB")),
			selected: []string{"r"}, want: EngineNonTransactional,
			tables: []string{"r(ROCKSDB)"}, reason: "服务器没告诉我们 ROCKSDB 有没有事务",
		},
		{
			row:      "6 Engines 为 nil 但表是 InnoDB → 降级（读不到能力 ≠ 有事务）",
			cat:      catalogWith(nil, eng("orders", "InnoDB")),
			selected: []string{"orders"}, want: EngineNonTransactional,
			tables: []string{"orders(InnoDB)"}, reason: "读不到能力 ≠ 有事务",
		},
		{
			row:      "7 PG 源 → not_judged（不冒充 ok）",
			kind:     "postgres",
			cat:      catalogWith(realEngines(), eng("t", "table")),
			selected: []string{"t"}, want: EngineNotJudged,
		},
		{
			row: "8 没读到表清单 → not_judged",
			cat: nil, selected: []string{"t"}, want: EngineNotJudged,
		},
		{
			row:      "9 只判选中的表：库里有 MyISAM 但没选 → transactional",
			cat:      catalogWith(realEngines(), eng("orders", "InnoDB"), eng("legacy", "MyISAM")),
			selected: []string{"orders"}, want: EngineTransactional,
		},
	}
	for _, c := range cases {
		t.Run(c.row, func(t *testing.T) {
			kind := c.kind
			if kind == "" {
				kind = "mysql"
			}
			j := JudgeEngines(kind, c.cat, c.selected)
			if j.Verdict != c.want {
				t.Fatalf("判定 = %s，期望 %s（reason=%s）", j.Verdict, c.want, j.Reason)
			}
			if j.Reason == "" {
				t.Fatal("每种结论都必须有人话原因")
			}
			if len(j.Tables) != len(c.tables) {
				t.Fatalf("点名表 = %v，期望 %v", j.Tables, c.tables)
			}
			for i := range c.tables {
				if j.Tables[i] != c.tables[i] {
					t.Fatalf("点名表 = %v，期望 %v（要带真实引擎名）", j.Tables, c.tables)
				}
			}
			if c.reason != "" && !strings.Contains(j.Reason, c.reason) {
				t.Fatalf("Reason 应含 %q，实际 %q", c.reason, j.Reason)
			}
			if c.want == EngineNonTransactional {
				if len(j.Names) != len(c.tables) {
					t.Fatalf("Affected 要裸表名，得到 %v", j.Names)
				}
				if !strings.Contains(j.SQL, "ENGINE=InnoDB") {
					t.Fatalf("降级必须给出「能做什么」的 SQL，得到 %q", j.SQL)
				}
			} else if len(j.Tables) != 0 || j.SQL != "" {
				t.Fatalf("%s 不该点名表或给 SQL：%+v", c.want, j)
			}
			if c.want == EngineNotJudged && j.Verdict == EngineTransactional {
				t.Fatal("not_judged 不许退化成 ok")
			}
		})
	}
}

// 13 格里的第 10/13 行：文案四段齐 + 每个引擎的特有代价各一句（命中才追加）。
func TestEngineDegradeNoteWording(t *testing.T) {
	note := EngineDegradeNote(JudgeEngines("mysql", catalogWith(realEngines(),
		source.Table{TableRef: source.TableRef{Table: "legacy"}, Engine: "MyISAM"}), []string{"legacy"}))
	for _, want := range []string{
		"降级", // 发生了什么
		"不拒绝",
		"不是一致性切面",
		"读取没有 read view", // 为什么
		"verify 全绿只说明「校验那一刻两边一样」", // 风险
		"ENGINE=InnoDB",  // 能做什么
		"legacy(MyISAM)", // 点名真实引擎
		"要重建表、要磁盘余量、要主键",
	} {
		if !strings.Contains(note, want) {
			t.Errorf("降级文案缺 %q：%s", want, note)
		}
	}
	if strings.Contains(note, "拒绝：") {
		t.Errorf("降级文案不许写成拒绝：%s", note)
	}
	if strings.Contains(note, "\n") {
		t.Errorf("降级文案必须单行（CLI/容器日志按行读）：%s", note)
	}

	// 多表时不把整串 SQL 塞进「说明」，而是指向卡片的 SQL（单行）。
	multi := EngineDegradeNote(JudgeEngines("mysql", catalogWith(realEngines(),
		source.Table{TableRef: source.TableRef{Table: "a"}, Engine: "MEMORY"},
		source.Table{TableRef: source.TableRef{Table: "b"}, Engine: "CSV"}), []string{"a", "b"}))
	if strings.Contains(multi, "\n") {
		t.Errorf("多表降级文案也必须单行：%s", multi)
	}
	if !strings.Contains(multi, "共 2 张") {
		t.Errorf("多表时要说清有几张并指向卡片 SQL：%s", multi)
	}

	// 引擎特有代价：各一句，且 InnoDB 一个字不加。
	costs := []struct {
		engine string
		want   []string
	}{
		{"MEMORY", []string{"重启/崩溃即空", "重搬才收敛"}},
		{"CSV", []string{"不支持任何索引", "SCOPE-02 / TARGET-02", "interval 下以 SCOPE-02 的 blocked 为准"}},
		{"ARCHIVE", []string{"不支持任何索引", "只能 INSERT/SELECT", "不支持可空列"}},
		{"BLACKHOLE", []string{"读永远 0 行"}},
		{"FEDERATED", []string{"表在远端"}},
	}
	for _, c := range costs {
		j := JudgeEngines("mysql", catalogWith(realEngines(),
			source.Table{TableRef: source.TableRef{Table: "t"}, Engine: c.engine}), []string{"t"})
		got := EngineDegradeNote(j)
		for _, want := range c.want {
			if !strings.Contains(got, want) {
				t.Errorf("%s 的特有代价缺 %q：%s", c.engine, want, got)
			}
		}
		if !strings.Contains(got, "t("+c.engine+")") {
			t.Errorf("%s：要点名 t(%s)，不能写成 MyISAM：%s", c.engine, c.engine, got)
		}
	}

	// InnoDB 有事务 → 压根不该有降级文案（第 2 格）。
	if j := JudgeEngines("mysql", catalogWith(realEngines(),
		source.Table{TableRef: source.TableRef{Table: "orders"}, Engine: "InnoDB"}), []string{"orders"}); j.Verdict != EngineTransactional {
		t.Fatalf("InnoDB 不该降级：%+v", j)
	}
}

// 13 格里的第 11 行：只有 MEMORY 表时，卡片标题/正文不许出现 MyISAM（否则标题就是假话）。
func TestMY08CardIsEngineNeutral(t *testing.T) {
	rep := Run(Input{
		SourceKind: "mysql", TargetKind: "postgres",
		SourceCaps: source.CapSet{source.CapCDC: {Supported: true}},
		SourceCat: catalogWith(realEngines(),
			source.Table{TableRef: source.TableRef{Table: "hot", Schema: "shop"}, Engine: "MEMORY"}),
		Tables: []string{"hot"}, Mode: "once", BatchRows: 500,
	})
	c, ok := statusOf(rep, "MY-08")
	if !ok {
		t.Fatalf("MEMORY 表必须有 MY-08：%+v", rep.Checks)
	}
	if c.Status != source.StatusWarn || !rep.CanStart {
		t.Fatalf("降级口径：warn + CanStart，得到 %s / %v", c.Status, rep.CanStart)
	}
	if strings.Contains(c.Title, "MyISAM") {
		t.Fatalf("标题不许写死 MyISAM（MEMORY 也降级之后就是假话）：%s", c.Title)
	}
	if !strings.Contains(c.Detail, "hot(MEMORY)") {
		t.Fatalf("正文要点名真实引擎：%s", c.Detail)
	}
	if !strings.Contains(c.SQL, "ALTER TABLE `shop`.`hot` ENGINE=InnoDB;") {
		t.Fatalf("要给出可直接跑的 ALTER：%q", c.SQL)
	}
	if len(c.Affected) != 1 || c.Affected[0] != "hot" {
		t.Fatalf("Affected 要裸表名：%v", c.Affected)
	}
}

// catalogWith 是「服务器自报 + 表清单」的测试桩（catalog() 默认用 realEngines()）。
func catalogWith(engines map[string]bool, tables ...source.Table) *source.Catalog {
	c := catalog(tables...)
	c.Engines = engines
	return c
}
