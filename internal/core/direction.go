package core

import "strings"

// Direction 是一个「源 → 目标」的同步组合。
//
// 为什么要把它做成数据而不是文案：`E1014` 的 `Why` 要告诉用户「现在支持什么」，
// 而这句话如果手写在文案里，**每次引擎扩一个方向就得记得改文案**——忘了改，
// 用户看到的就是「与代码不一致的说明书」。所以方向矩阵是数据，`Why` 由它生成，
// 引擎侧也用它做判断（`SupportsDirection`），只有一份。
type Direction struct {
	Source string // postgres / mysql
	Target string // postgres / mysql
	// Modes 是这条方向当前支持的模式，直接进用户可见文案（例如「全量 + 增量」）。
	Modes string
}

// name 把引擎名换成用户看得懂的名字。
func name(kind string) string {
	switch kind {
	case "postgres":
		return "PostgreSQL"
	case "mysql":
		return "MySQL"
	}
	return kind
}

// Label 渲染成「PostgreSQL → MySQL」。
func (d Direction) Label() string { return name(d.Source) + " → " + name(d.Target) }

// supportedDirections 是**当前版本真正能跑的**同步方向，唯一真源。
//
// 改这里的规矩：引擎/目标端新增一个方向时改这个切片，`E1014` 的 `Why` 自动跟着变；
// 反过来，**没接上线的方向不许写进来**——码表说支持、引擎跑不了，比不支持更糟。
var supportedDirections = []Direction{
	{Source: "postgres", Target: "postgres", Modes: "全量 + 增量"},
	{Source: "postgres", Target: "mysql", Modes: "只全量"},
}

// pendingDirections 是**路线图上但还没接通**的方向。写进来只是为了在同一条 `Why` 里
// 明确告诉用户「你的组合属于哪一类」，不是承诺。
var pendingDirections = []Direction{
	{Source: "mysql", Target: "postgres"},
	{Source: "mysql", Target: "mysql"},
}

// SupportedDirections 返回当前能跑的方向（副本，调用方随便改）。
func SupportedDirections() []Direction {
	out := make([]Direction, len(supportedDirections))
	copy(out, supportedDirections)
	return out
}

// PendingDirections 返回还没接通的方向。
func PendingDirections() []Direction {
	out := make([]Direction, len(pendingDirections))
	copy(out, pendingDirections)
	return out
}

// SupportsDirection 判断某组合现在能不能跑。引擎与预检都用它，避免各写一套判断。
func SupportsDirection(source, target string) bool {
	source = strings.ToLower(strings.TrimSpace(source))
	target = strings.ToLower(strings.TrimSpace(target))
	for _, d := range supportedDirections {
		if d.Source == source && d.Target == target {
			return true
		}
	}
	return false
}

// DirectionsText 渲染「现在能跑的方向」，供 `E1014` 的 `Why` 使用。
// 例：「PostgreSQL → PostgreSQL（全量 + 增量）、PostgreSQL → MySQL（只全量）」
func DirectionsText() string {
	parts := make([]string, 0, len(supportedDirections))
	for _, d := range supportedDirections {
		label := d.Label()
		if d.Modes != "" {
			label += "（" + d.Modes + "）"
		}
		parts = append(parts, label)
	}
	return strings.Join(parts, "、")
}

// PendingDirectionsText 渲染「还没接通的方向」，例：「MySQL → PostgreSQL、MySQL → MySQL」。
func PendingDirectionsText() string {
	parts := make([]string, 0, len(pendingDirections))
	for _, d := range pendingDirections {
		parts = append(parts, d.Label())
	}
	return strings.Join(parts, "、")
}
