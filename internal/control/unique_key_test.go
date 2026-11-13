package control

import (
	"strings"
	"testing"
)

// unique_key 的校验规则（docs/DESIGN-NO-PRIMARY-KEY.md §4.3）：这里全是**不连库**能判的，
// 目标端索引对不对得上归 preflight TARGET-02。
func validDocWithUniqueKey(t *testing.T, td TableDoc) JobDoc {
	t.Helper()
	return JobDoc{
		Version: 1,
		Job:     "u",
		Source:  SourceDoc{Kind: "postgres", DSN: "env:SRC", Scope: &ScopeDoc{Schema: "public"}},
		Target:  TargetDoc{Kind: "postgres", DSN: "env:TGT"},
		Mode:    ModeDoc{Frequency: "continuous+initial"},
		Content: &ContentDoc{Tables: map[string]TableDoc{"events": td}},
	}
}

func TestUniqueKeyValidPasses(t *testing.T) {
	doc := validDocWithUniqueKey(t, TableDoc{
		UniqueKey: []string{"tenant_id", "event_no"},
		Columns:   &ColumnsDoc{Include: []string{"tenant_id", "event_no", "payload"}},
	})
	if err := doc.Validate(); err != nil {
		t.Fatalf("合法声明不该报错: %v", err)
	}
}

func TestUniqueKeyRules(t *testing.T) {
	cases := []struct {
		name string
		td   TableDoc
		want string
	}{
		{"空数组", TableDoc{UniqueKey: []string{}}, "不能是空数组"},
		{"空列名", TableDoc{UniqueKey: []string{"tenant_id", " "}}, "空列名"},
		{"重复列", TableDoc{UniqueKey: []string{"a", "a"}}, "重复"},
		{"不在 include 里", TableDoc{UniqueKey: []string{"a"},
			Columns: &ColumnsDoc{Include: []string{"b", "c"}}}, "不在搬运列里"},
		{"被 exclude 排掉", TableDoc{UniqueKey: []string{"a"},
			Columns: &ColumnsDoc{Exclude: []string{"a"}}}, "不在搬运列里"},
		{"在 map 里换类型", TableDoc{UniqueKey: []string{"a"},
			Columns: &ColumnsDoc{Map: map[string]string{"a": "text"}}}, "不能同时在 columns.map"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			doc := validDocWithUniqueKey(t, c.td)
			err := doc.Validate()
			if err == nil {
				t.Fatalf("%s 必须报参数错误", c.name)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Fatalf("错误里应有 %q，得到: %v", c.want, err)
			}
		})
	}
}

func TestUniqueKeyNotRequired(t *testing.T) {
	// 不写 unique_key（nil）是常态：无主键表默认 insert-only。
	doc := validDocWithUniqueKey(t, TableDoc{})
	if err := doc.Validate(); err != nil {
		t.Fatalf("不写 unique_key 不该报错: %v", err)
	}
}

func TestDeclaredUniqueKeys(t *testing.T) {
	doc := validDocWithUniqueKey(t, TableDoc{UniqueKey: []string{"a", "b"}})
	doc.Content.Tables["plain"] = TableDoc{}
	got := doc.DeclaredUniqueKeys()
	if len(got) != 1 || len(got["events"]) != 2 || got["events"][0] != "a" {
		t.Fatalf("DeclaredUniqueKeys 应只含声明过的表: %+v", got)
	}
	empty := JobDoc{}
	if got := empty.DeclaredUniqueKeys(); got != nil {
		t.Fatalf("没有 content 时应返回 nil: %+v", got)
	}
}
