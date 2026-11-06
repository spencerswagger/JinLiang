package jobrun

import (
	"os"
	"strings"
	"testing"
)

// 矩阵与码表必须对得上：矩阵里每一对支持的方向，E1014 的 why 里都要能找到；
// 不支持的两种方向也必须被 why 点名。这条用例是「判断只有一处」的守门人——
// 谁改了支持范围却忘了改码表（或反过来），这里立刻红。
func TestDirectionMatrixMatchesCatalogue(t *testing.T) {
	for _, d := range SupportedDirections() {
		if !DirectionSupported(d[0], d[1]) {
			t.Fatalf("矩阵自相矛盾：%v 不在支持列表里", d)
		}
	}
	if DirectionSupported("mysql", "mysql") || DirectionSupported("mysql", "postgres") {
		t.Fatal("mysql 源的两条方向当前不支持，矩阵写错了")
	}

	// 码表在 internal/core（Ark 的地盘），从同一份源码里读 why，避免复制一份文案
	raw, err := os.ReadFile("../core/codes.go")
	if err != nil {
		t.Skipf("读不到码表源码（包布局变了？）：%v", err)
	}
	src := string(raw)
	if !strings.Contains(src, `"E1014"`) {
		t.Skip("E1014 还没落地（#46 未合），矩阵暂时没有可对照的码表条目")
	}
	for _, d := range SupportedDirections() {
		want := kindWord(d[0]) + " → " + kindWord(d[1])
		if !strings.Contains(src, want) {
			t.Fatalf("E1014 的 why 里没有 %q——支持范围变了但码表没跟上", want)
		}
	}
	for _, d := range UnsupportedDirections() {
		want := kindWord(d[0]) + " → " + kindWord(d[1])
		if !strings.Contains(src, want) {
			t.Fatalf("E1014 的 why 里没有点名不支持的方向 %q", want)
		}
	}
}

func kindWord(kind string) string {
	switch kind {
	case "postgres":
		return "PostgreSQL"
	case "mysql":
		return "MySQL"
	}
	return kind
}
