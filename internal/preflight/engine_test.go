package preflight

import (
	"strings"
	"testing"

	"github.com/spencerswagger/jinliang/internal/source"
)

// SPEC MY-08 的判据只有 JudgeEngines 一处。这一格把三态和三态之间的边界钉死：
// 谁改了「哪些表算无事务」，这里先红，不用等到文档/收尾/预检三处互相打脸。
func TestJudgeEnginesThreeVerdicts(t *testing.T) {
	innodb := source.Table{TableRef: source.TableRef{Table: "orders"}, Engine: "InnoDB"}
	myisam := source.Table{TableRef: source.TableRef{Table: "legacy"}, Engine: "MyISAM"}
	merge := source.Table{TableRef: source.TableRef{Table: "mrg"}, Engine: "MRG_MyISAM"}

	// 判据的字符串值本身是契约（docs/SPEC.md MY-08 要按这三个值逐条对齐）：
	// 改字符串 = 改文档口径，必须两处同改。
	for _, want := range [][2]string{
		{"transactional", string(EngineTransactional)},
		{"non_transactional", string(EngineNonTransactional)},
		{"not_judged", string(EngineNotJudged)},
	} {
		if want[0] != want[1] {
			t.Fatalf("EngineVerdict 的取值是文档契约，期望 %q，实际 %q", want[0], want[1])
		}
	}

	cases := []struct {
		name     string
		kind     string
		cat      *source.Catalog
		selected []string
		want     EngineVerdict
		tables   []string
	}{
		{
			name: "全 InnoDB：有事务，不发 MY-08",
			kind: "mysql", cat: catalog(innodb, source.Table{TableRef: source.TableRef{Table: "c"}, Engine: "InnoDB"}),
			selected: []string{"orders", "c"}, want: EngineTransactional,
		},
		{
			name: "MyISAM：降级（不是拒绝）",
			kind: "mysql", cat: catalog(innodb, myisam), selected: []string{"orders", "legacy"},
			want: EngineNonTransactional, tables: []string{"legacy(MyISAM)"},
		},
		{
			name: "MRG_MyISAM 一起点名为无事务",
			kind: "mysql", cat: catalog(myisam, merge), selected: []string{"legacy", "mrg"},
			want: EngineNonTransactional, tables: []string{"legacy(MyISAM)", "mrg(MRG_MyISAM)"},
		},
		{
			name: "引擎名大小写不敏感（8.x 真库给的是 MyISAM）",
			kind: "mysql", cat: catalog(source.Table{TableRef: source.TableRef{Table: "odd"}, Engine: "myisam"}),
			selected: []string{"odd"}, want: EngineNonTransactional, tables: []string{"odd(myisam)"},
		},
		{
			name: "没选中 MyISAM 表：不误伤",
			kind: "mysql", cat: catalog(innodb, myisam), selected: []string{"orders"},
			want: EngineTransactional,
		},
		{
			name: "PG 源：Engine 是 relkind，不判定",
			kind: "postgres", cat: catalog(source.Table{TableRef: source.TableRef{Table: "t"}, Engine: "table"}),
			selected: []string{"t"}, want: EngineNotJudged,
		},
		{
			name: "没读到表清单：不判定（不阻断，但也不冒充 ok）",
			kind: "mysql", cat: nil, selected: []string{"t"}, want: EngineNotJudged,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			j := JudgeEngines(c.kind, c.cat, c.selected)
			if j.Verdict != c.want {
				t.Fatalf("判定 = %s，期望 %s（reason=%s）", j.Verdict, c.want, j.Reason)
			}
			if j.Reason == "" {
				t.Fatal("每种结论都必须有人话原因（否则卡片上只剩一个枚举值）")
			}
			if len(c.tables) != len(j.Tables) {
				t.Fatalf("点名表 = %v，期望 %v", j.Tables, c.tables)
			}
			for i := range c.tables {
				if j.Tables[i] != c.tables[i] {
					t.Fatalf("点名表 = %v，期望 %v（要带引擎名，用户才知道从哪改）", j.Tables, c.tables)
				}
			}
			switch c.want {
			case EngineNonTransactional:
				if len(j.Names) != len(c.tables) {
					t.Fatalf("Affected 要裸表名，得到 %v", j.Names)
				}
				if !strings.Contains(j.SQL, "ENGINE=InnoDB") {
					t.Fatalf("降级必须给出「能做什么」的 SQL，得到 %q", j.SQL)
				}
			default:
				if len(j.Tables) != 0 || j.SQL != "" {
					t.Fatalf("%s 不该点名表或给 SQL：%+v", c.want, j)
				}
			}
		})
	}
}

// 降级文案是「唯一来源」：预检卡片与引擎收尾都从这里取。它必须四段齐，且明说
// 不是拒绝 —— 否则用户会以为被拦下了（DESIGN.md：不拒绝，只提示）。
func TestEngineDegradeNoteIsExplicitAndFourPart(t *testing.T) {
	note := EngineDegradeNote(JudgeEngines("mysql", catalog(
		source.Table{TableRef: source.TableRef{Table: "legacy"}, Engine: "MyISAM"}), []string{"legacy"}))
	for _, want := range []string{
		"降级",      // 发生了什么（降级）
		"不拒绝",     // 不阻断（DESIGN：不拒绝，只提示）
		"无事务",     // 为什么
		"不是一致性切面", // 代价说清楚（关键：不许暗示是全绿）
		"verify 全绿也不代表崩溃后仍然一致", // 风险（连 verify 都不作数）
		"改成 InnoDB",      // 你能做什么
		"legacy(MyISAM)", // 点名，不是泛指
	} {
		if !strings.Contains(note, want) {
			t.Errorf("降级文案缺 %q：%s", want, note)
		}
	}
	if strings.Contains(note, "拒绝：") {
		t.Errorf("降级文案不许写成拒绝：%s", note)
	}
}

// MY-08 卡片本身：warn（不是 blocked）+ 明确写「降级」+ 带可执行的 ALTER。
func TestMY08IsDegradeNotRefusal(t *testing.T) {
	rep := Run(Input{
		SourceKind: "mysql", TargetKind: "postgres",
		SourceCaps: source.CapSet{source.CapCDC: {Supported: true}},
		SourceCat: catalog(
			source.Table{TableRef: source.TableRef{Table: "legacy", Schema: "shop"}, Engine: "MyISAM"},
		),
		Tables: []string{"legacy"}, Mode: "once", BatchRows: 500,
	})
	c, ok := statusOf(rep, "MY-08")
	if !ok {
		t.Fatalf("MyISAM 表必须有 MY-08：%+v", rep.Checks)
	}
	if c.Status != source.StatusWarn {
		t.Fatalf("MyISAM 是降级（warn 照跑），不是 %s", c.Status)
	}
	if !rep.CanStart {
		t.Fatal("降级不许把 CanStart 压成 false")
	}
	if !strings.Contains(c.Detail, "降级") || !strings.Contains(c.Detail, "不拒绝") {
		t.Fatalf("卡片必须写清是「降级、不拒绝」：%s", c.Detail)
	}
	if !strings.Contains(c.SQL, "ALTER TABLE `shop`.`legacy` ENGINE=InnoDB;") {
		t.Fatalf("要给出用户能直接跑的那条 ALTER（带 schema），得到 %q", c.SQL)
	}
	if len(c.Affected) != 1 || c.Affected[0] != "legacy" {
		t.Fatalf("Affected 要裸表名：%v", c.Affected)
	}
}
