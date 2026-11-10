package engine

// 契约 docs/CONTRACT-PAUSE-AND-POSITION.md §4.4 的**守卫用例**：
// 「`classifyError` 里不许再出现 `pgsource.` / `mysqlsource.` 前缀」。
//
// 为什么用「读自己的源码」这种写法：这条约束是**依赖方向**，不是运行时行为——
// 靠普通断言抓不到（把 pgsource.IsNoPrivilege 加回去，行为测试照样绿，因为 PG 源
// 恰好还命中）。而异构（MySQL 源）下它会退化成「未识别」，只有 MySQL 真库才暴露。
// 这条用例秒级、不需要库，改回去立刻红。

import (
	"os"
	"strings"
	"testing"

	"github.com/spencerswagger/jinliang/internal/source"
)

func classifyErrorBody(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile("engine.go")
	if err != nil {
		t.Fatalf("读 engine.go 失败: %v", err)
	}
	src := string(raw)
	start := strings.Index(src, "func classifyError(")
	if start < 0 {
		t.Fatal("engine.go 里找不到 classifyError")
	}
	// 到下一个顶层函数声明为止（classifyError 后面紧跟 heartbeatLoop 的注释）。
	rest := src[start:]
	if end := strings.Index(rest, "\n// heartbeatLoop"); end > 0 {
		return rest[:end]
	}
	return rest
}

func TestClassifyErrorUsesConnectorSemanticsNotConnectorPackages(t *testing.T) {
	body := classifyErrorBody(t)
	for _, banned := range []string{"pgsource.", "mysqlsource."} {
		if strings.Contains(body, banned) {
			t.Fatalf("classifyError 里出现了 %q：引擎不许按具体连接器判类（契约 §4.4）——"+
				"异构（MySQL 源）下那个判断不命中，用户拿到的是「未识别的数据库错误」；"+
				"改用 source.ClassOf(err)，让连接器自己标记", banned)
		}
	}
	if !strings.Contains(body, "source.ClassOf") {
		t.Fatal("classifyError 必须经 source.ClassOf 判类（连接器语义 + SQLSTATE 兜底）")
	}
}

// 端到端的那半：一条**只**带语义标记的错误（不经过 pgsource）也要挂对码。
// 这正是 MySQL 源今晚之前的处境：错误本身没问题，是引擎不认。
func TestClassifyErrorHonorsConnectorMarkWithoutPGSource(t *testing.T) {
	cases := []struct {
		class source.Class
		want  string
	}{
		{source.ClassNoPrivilege, "E1012"},
		{source.ClassObjectMissing, "E1013"},
		{source.ClassResourceLimit, "E1016"},
		{source.ClassWalLevel, "E1001"},
		{source.ClassPositionGap, "E3001"},
		{source.ClassPositionForeign, "E1010"},
	}
	for _, c := range cases {
		err := source.Mark(errPlain("源端报错"), c.class)
		code, ok := classifyError(err)
		if !ok || string(code) != c.want {
			t.Errorf("%s 应挂 %s，得到 %q（classified=%v）", c.class, c.want, code, ok)
		}
	}
}

type plainErr string

func (e plainErr) Error() string { return string(e) }

func errPlain(s string) error { return plainErr(s) }
