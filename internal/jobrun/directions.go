package jobrun

// 支持矩阵：**HTTP 层与引擎共用一处判断**。
//
// 为什么放在这里而不是 each 入口各写一份：今晚已经因为「同一个判断散在多处」出过
// 好几次（IsLoopbackListen vs IsLoopbackRequest、文档阈值 vs 代码常量、E1013 的
// 分类器 vs 码表）。方向支持是**产品能力**，只有一处可以说「能跑 / 不能跑」。
//
// 绝对不许在文案里描述方向：每个方向各写一份自然语言，就会出现「mysql→pg 写成
// pg→mysql」这种反过来的错，而用户照着反的提示去做只会更糟。界面只显示客观事实
// 「源类型 → 目标类型」（两个 kind 字段），解释交给码表 E1014。
//
// 与码表的一致性由 TestDirectionMatrixMatchesCatalogue 钉住：矩阵里每一对支持的
// 方向，必须能在 E1014 的 why 里找到对应写法；不支持的两种方向也必须被 why 点名。
func SupportedDirections() [][2]string {
	return [][2]string{
		{"postgres", "postgres"}, // 全量 + 增量
		{"postgres", "mysql"},    // 只全量（mysqlsink 暂无状态表能力，增量会明确拒绝）
	}
}

// DirectionSupported 回答「这一对能不能跑」。只认 kind，不看 DSN。
func DirectionSupported(src, tgt string) bool {
	for _, d := range SupportedDirections() {
		if d[0] == src && d[1] == tgt {
			return true
		}
	}
	return false
}

// UnsupportedDirections 是已知「不能跑」的方向（用于与码表对齐的断言，以及文案核对）。
func UnsupportedDirections() [][2]string {
	return [][2]string{
		{"mysql", "postgres"},
		{"mysql", "mysql"},
	}
}
