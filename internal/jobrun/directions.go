package jobrun

import "github.com/spencerswagger/jinliang/internal/core"

// 支持矩阵：**唯一真源在 `internal/core/direction.go`**，这里只是按本包原有的
// `[2]string` 形状转发一下，供 HTTP 入口与引擎共用。
//
// 为什么挪到 core：方向是**产品能力**，而 `E1014` 的 `why` 要由它生成；如果这里
// 再存一份，两处迟早分叉——今晚已经因为「同一个判断散在多处」出过好几次
// （IsLoopbackListen vs IsLoopbackRequest、文档阈值 vs 代码常量、E1013 的分类器 vs 码表）。
// core 是叶子包（不 import 任何内部包），所以 HTTP/引擎/码表都能引用它，没有循环依赖。
//
// 绝对不许在文案里描述方向：每个方向各写一份自然语言，就会出现「mysql→pg 写成
// pg→mysql」这种反过来的错，而用户照着反的提示去做只会更糟。界面只显示客观事实
// 「源类型 → 目标类型」（两个 kind 字段），解释交给码表 E1014。
//
// 与码表的一致性由 TestDirectionMatrixMatchesCatalogue 钉住。

// SupportedDirections 返回「这一对能跑」的方向，形如 {source, target}。
func SupportedDirections() [][2]string {
	src := core.SupportedDirections()
	out := make([][2]string, 0, len(src))
	for _, d := range src {
		out = append(out, [2]string{d.Source, d.Target})
	}
	return out
}

// DirectionSupported 回答「这一对能不能跑」。只认 kind，不看 DSN。
func DirectionSupported(src, tgt string) bool { return core.SupportsDirection(src, tgt) }

// UnsupportedDirections 是已知「不能跑」的方向（用于与码表对齐的断言，以及文案核对）。
func UnsupportedDirections() [][2]string {
	pend := core.PendingDirections()
	out := make([][2]string, 0, len(pend))
	for _, d := range pend {
		out = append(out, [2]string{d.Source, d.Target})
	}
	return out
}
