package mysqlsource

import (
	"os"
	"strings"
	"testing"
)

// SPEC MY-08 的判据读 `Catalog.Engines`，而 **Catalog() 有两个入口**：
//   - internal/connect/mysql.go   —— 预检 / 控制面；
//   - internal/mysqlsource/snapshot.go —— 引擎（快照、收尾标注）。
//
// 只填一处就会出现「预检说降级、收尾不标注」（或反过来）——这是 Ark 在
// notes/my08-engine-verdict-spec.md §6.2 明确点出的最容易漏的一格，而且这种漏法
// 单测看不出来（单测各有各的桩），只有真库集成用例才撞得到。
//
// 所以在这里钉一条**结构断言**：两个文件都必须真的去读 information_schema.ENGINES
// 并写进 cat.Engines。删除任何一处，这条用例立刻红。
func TestBothCatalogImplementationsFillEngines(t *testing.T) {
	for _, path := range []string{"snapshot.go", "../connect/mysql.go"} {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("读 %s 失败: %v", path, err)
		}
		src := string(raw)
		if !strings.Contains(src, "information_schema.ENGINES") {
			t.Errorf("%s 没有读 information_schema.ENGINES：MY-08 的判据输入会缺（该文件的 Catalog() 必须填 Engines）", path)
		}
		if !strings.Contains(src, "cat.Engines = ") {
			t.Errorf("%s 没有把引擎能力写进 cat.Engines：预检与会有一侧拿不到能力，出现「预检降级、收尾不标注」", path)
		}
	}
}

// 反过来：判据本身不许再按引擎名猜（回到 #113 的 strings.Contains(...,"MYISAM")）。
// MY-08 的口径是「服务器自报的能力」，引擎名黑名单在 MEMORY/CSV/ARCHIVE 上就是假绿。
func TestJudgeDoesNotGuessByEngineName(t *testing.T) {
	raw, err := os.ReadFile("../preflight/engine.go")
	if err != nil {
		t.Fatalf("读判据失败: %v", err)
	}
	src := string(raw)
	for _, banned := range []string{`Contains(strings.ToUpper(t.Engine), "MYISAM")`, `"MYISAM" ==`} {
		if strings.Contains(src, banned) {
			t.Errorf("preflight/engine.go 又在按引擎名判事务（%s）：MY-08 必须查 Catalog.Engines", banned)
		}
	}
}
