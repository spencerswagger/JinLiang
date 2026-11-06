package jobrun

import (
	"strings"
	"testing"

	"github.com/spencerswagger/jinliang/internal/core"
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

	// 码表条目从 `core.Lookup` 取**运行时**的 why，而不是 grep 源码：
	// #46 之后 E1014 的 why 由 `core.DirectionsText()` 生成（矩阵是数据、文案是产物），
	// 源码里已经没有「PostgreSQL → MySQL」这种字面量了——grep 源码会永远失败。
	// 校验生成出来的那句话，比校验源码更贴近用户真正看到的东西。
	spec, ok := core.Lookup(core.CodeUnsupportedDirection)
	if !ok {
		t.Skip("E1014 还没落地，矩阵暂时没有可对照的码表条目")
	}
	for _, d := range SupportedDirections() {
		want := kindWord(d[0]) + " → " + kindWord(d[1])
		if !strings.Contains(spec.Why, want) {
			t.Fatalf("E1014 的 why 里没有 %q——支持范围变了但码表没跟上：%s", want, spec.Why)
		}
	}
	for _, d := range UnsupportedDirections() {
		want := kindWord(d[0]) + " → " + kindWord(d[1])
		if !strings.Contains(spec.Why, want) {
			t.Fatalf("E1014 的 why 里没有点名不支持的方向 %q：%s", want, spec.Why)
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
