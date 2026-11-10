package engine

// 契约 docs/CONTRACT-PAUSE-AND-POSITION.md §4.4 的**守卫用例**：
// 「`classifyError` 里不许再出现 `pgsource.` / `mysqlsource.` / `pgwire.` 前缀」。
//
// 为什么用「读自己的源码」这种写法：这条约束是**依赖方向**，不是运行时行为——
// 靠普通断言抓不到（把 `pgsource.IsNoPrivilege` 加回去，行为测试照样绿，因为 PG 源
// 恰好还命中）。而异构（MySQL 源）下它会退化成「未识别」，只有 MySQL 真库才暴露。
//
// 为什么用 `go/ast` 而不是字符串切片（Owl II P2）：字符串版对注释敏感
// （在 classifyError 里写一句带 `pgsource.` 的注释会假红；改掉注释锚点又会退化成扫全文件）。
// AST 只看**真的标识符引用**，注释与字符串一律不算。

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"

	"github.com/spencerswagger/jinliang/internal/source"
)

func TestClassifyErrorUsesConnectorSemanticsNotConnectorPackages(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "engine.go", nil, 0)
	if err != nil {
		t.Fatalf("解析 engine.go 失败: %v", err)
	}
	var fn *ast.FuncDecl
	for _, d := range file.Decls {
		fd, ok := d.(*ast.FuncDecl)
		if ok && fd.Name != nil && fd.Name.Name == "classifyError" {
			fn = fd
			break
		}
	}
	if fn == nil {
		t.Fatal("engine.go 里找不到 classifyError")
	}

	banned := map[string]bool{"pgsource": true, "mysqlsource": true, "pgwire": true}
	usesClassOf := false
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		pkg, ok := sel.X.(*ast.Ident)
		if !ok {
			return true
		}
		if banned[pkg.Name] {
			pos := fset.Position(sel.Pos())
			t.Errorf("%s：classifyError 里引用了 %s.%s —— 引擎不许按具体连接器/协议实现判类（契约 §4.4）；"+
				"改用 source.ClassOf(err)，让连接器自己标记",
				pos, pkg.Name, sel.Sel.Name)
		}
		if pkg.Name == "source" && sel.Sel.Name == "ClassOf" {
			usesClassOf = true
		}
		return true
	})
	if !usesClassOf {
		t.Error("classifyError 必须经 source.ClassOf 判类（连接器语义 + SQLSTATE 兜底）")
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
		err := source.Mark(plainErr("源端报错"), c.class)
		code, ok := classifyError(err)
		if !ok || string(code) != c.want {
			t.Errorf("%s 应挂 %s，得到 %q（classified=%v）", c.class, c.want, code, ok)
		}
	}
	// 可重试类**刻意不挂码**：它交给重试/重连策略，不是「用户要做点什么」的错误。
	// 挂一个码反而会让用户以为要动手。这条断言把「刻意」变成可执行的约定。
	if code, ok := classifyError(source.Mark(plainErr("connection reset"), source.ClassRetryable)); ok {
		t.Errorf("ClassRetryable 不该挂码（交给重试策略），却挂上了 %s", code)
	}
}

type plainErr string

func (e plainErr) Error() string { return string(e) }
