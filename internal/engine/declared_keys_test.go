package engine

import (
	"testing"

	"github.com/spencerswagger/jinliang/internal/control"
)

// 声明的两种口径（Owl PR #120 第 3 轮 P2-1）：
//   - 预检吃 raw：用户写错了要能提示，哪怕当前模式用不上它；
//   - sink 吃过滤后的：once / full_interval 每轮清空重写，行身份用不上。
//
// 混用会让同一条任务在 `jinliang plan` 与 run 的预检里给出两份不同报告。
func TestDeclaredUniqueKeysSplitByMode(t *testing.T) {
	doc := control.JobDoc{
		Content: &control.ContentDoc{Tables: map[string]control.TableDoc{
			"events": {UniqueKey: []string{"tenant_id", "event_no"}},
			"plain":  {},
		}},
	}
	cases := []struct {
		freq     string
		wantSink int
	}{
		{"continuous+initial", 1},
		{"continuous", 1},
		{"interval", 1},
		{"once", 0},
		{"full_interval", 0},
	}
	for _, c := range cases {
		t.Run(c.freq, func(t *testing.T) {
			doc.Mode.Frequency = c.freq
			r := &runner{doc: doc}
			// 预检永远看到 raw 声明（写错了才提示得出来）。
			if raw := r.declaredUniqueKeys(); len(raw) != 1 || len(raw["events"]) != 2 {
				t.Fatalf("预检口径应永远是 raw 声明，得到 %+v", raw)
			}
			got := r.sinkDeclaredUniqueKeys()
			if len(got) != c.wantSink {
				t.Fatalf("%s 下 sink 应拿到 %d 张表的声明，得到 %+v", c.freq, c.wantSink, got)
			}
		})
	}
}
