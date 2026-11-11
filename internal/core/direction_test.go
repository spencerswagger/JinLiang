package core

import (
	"os"
	"strings"
	"testing"
)

// TestSupportedDirections 是方向矩阵的表驱动断言：这张表就是「现在能跑什么」的答案。
func TestSupportedDirections(t *testing.T) {
	cases := []struct {
		src, tgt string
		want     bool
	}{
		{"postgres", "postgres", true},
		{"postgres", "mysql", true},
		// MySQL 源已接进引擎主路径（binlog 读流）；MySQL → MySQL 还没接。
		{"mysql", "postgres", true},
		{"mysql", "mysql", false},
		// 大小写与空白不该影响判断（连接类型是用户填的，也可能来自 yaml）
		{"POSTGRES", "MYSQL", true},
		{" postgres ", " mysql ", true},
		{"", "", false},
	}
	for _, c := range cases {
		if got := SupportsDirection(c.src, c.tgt); got != c.want {
			t.Errorf("SupportsDirection(%q,%q) = %v，期望 %v", c.src, c.tgt, got, c.want)
		}
	}
}

// TestE1014WhyCoversEveryDirection：`Why` 必须提到**每一个**已支持方向，
// 也必须提到每一个未接通方向——否则用户看文案会漏掉自己的组合。
func TestE1014WhyCoversEveryDirection(t *testing.T) {
	spec, ok := Lookup(CodeUnsupportedDirection)
	if !ok {
		t.Fatal("E1014 必须在码表里")
	}
	for _, d := range SupportedDirections() {
		if !strings.Contains(spec.Why, d.Label()) {
			t.Errorf("Why 里没提到已支持方向 %s：%s", d.Label(), spec.Why)
		}
		if d.Modes != "" && !strings.Contains(spec.Why, d.Modes) {
			t.Errorf("Why 里没提到 %s 的模式 %q", d.Label(), d.Modes)
		}
	}
	for _, d := range PendingDirections() {
		if !strings.Contains(spec.Why, d.Label()) {
			t.Errorf("Why 里没提到未接通方向 %s：%s", d.Label(), spec.Why)
		}
	}
}

// TestE1014WhyTracksData：改矩阵 → `Why` 自动跟着变。
// 这条用例是「矩阵当数据」的证明：临时加一个方向，文案必须立刻带上它。
func TestE1014WhyTracksData(t *testing.T) {
	before := DirectionsText()
	saved := supportedDirections
	t.Cleanup(func() { supportedDirections = saved })

	supportedDirections = append(append([]Direction{}, saved...),
		Direction{Source: "mysql", Target: "postgres", Modes: "只全量（未上线示例）"})
	after := DirectionsText()
	if before == after {
		t.Fatal("加了方向，生成的文案却没变——说明 Why 不是数据生成的")
	}
	if !strings.Contains(after, "MySQL → PostgreSQL") {
		t.Fatalf("新方向没进文案：%s", after)
	}
}

// TestCodesGoHasNoHardcodedDirections：防「文案与代码分叉」的结构性闸门。
//
// `codes.go` 里不许出现具体方向名（PostgreSQL / MySQL / →）——它们只能来自
// direction.go 的数据。这条用例会在有人把方向名写回文案时立刻红，
// 比口头约定可靠（#46 自己就是「写死的说明文字必然分叉」的例子）。
func TestCodesGoHasNoHardcodedDirections(t *testing.T) {
	raw, err := os.ReadFile("codes.go")
	if err != nil {
		t.Fatalf("读 codes.go 失败: %v", err)
	}
	src := string(raw)
	// 只看 E1014 那一段：其它码（E1010 位点类型、E1011 环境变量……）里出现 MySQL/PG
	// 是正常叙述，不受这条约束。
	start := strings.Index(src, "Code: CodeUnsupportedDirection")
	if start < 0 {
		t.Fatal("codes.go 里找不到 E1014 的 spec")
	}
	end := start + len(src[start:])
	if next := strings.Index(src[start+1:], "Code: Code"); next >= 0 {
		end = start + 1 + next
	}
	block := src[start:end]
	for _, bad := range []string{"→", "PostgreSQL", "MySQL"} {
		if strings.Contains(block, bad) {
			t.Errorf("E1014 的文案里出现了 %q：方向名必须来自 direction.go 的矩阵（DirectionsText/PendingDirectionsText），不要手写", bad)
		}
	}
}

// TestPendingDirectionsNotSupported：矩阵内部自洽——未接通的方向不能同时被判为支持。
func TestPendingDirectionsNotSupported(t *testing.T) {
	for _, d := range PendingDirections() {
		if SupportsDirection(d.Source, d.Target) {
			t.Errorf("%s 同时出现在 supported 与 pending 里", d.Label())
		}
	}
}

// TestErrorsDocE1014MatchesDirectionMatrix：`docs/ERRORS.md` 的 E1014 行是**手抄**的
// 码表副本，最容易变成「与代码不一致的说明书」——2026-11-12 就是它把「MySQL → PostgreSQL」
// 写成还没接通，而 `#75` 早就接上了，用户照文档判断会误以为自己的组合跑不了。
//
// 所以：文档里那一行必须逐字包含矩阵生成的两句话（已支持列表 + 未接通列表）。
// 改方向 → 两处一起改；只改代码不改文档，这条会红。
// （真源仍是 codes.go；这里的断言只保证「文档没说反话」。）
func TestErrorsDocE1014MatchesDirectionMatrix(t *testing.T) {
	raw, err := os.ReadFile("../../docs/ERRORS.md")
	if err != nil {
		t.Fatalf("读 docs/ERRORS.md 失败: %v", err)
	}
	var row string
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(line, "| E1014 ") {
			row = line
			break
		}
	}
	if row == "" {
		t.Fatal("docs/ERRORS.md 里找不到 E1014 那一行")
	}
	for _, want := range []string{DirectionsText(), PendingDirectionsText() + " 还没接通"} {
		if !strings.Contains(row, want) {
			t.Errorf("docs/ERRORS.md 的 E1014 行与方向矩阵不一致，缺：%q\n现行：%s", want, row)
		}
	}
}
