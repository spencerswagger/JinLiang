package verify

import "fmt"

// FingerprintScoper marks a side whose row fingerprint has a specific
// canonicalization scope. Two sides are only fingerprint-comparable when both
// scopes are non-empty and equal.
//
// 为什么必须显式声明：MySQL 侧是客户端 sha256（列名排序 + 主键定序），PG 侧是
// 服务端 sum(hashtextextended)；两个数永远不可能相等。跨端直接比指纹会让
// PG→MySQL 的 verify 永久假红（今晚第三次同类假红），所以宁可退化到逐行比对。
type FingerprintScoper interface {
	FingerprintScope() string
}

// FingerprintInfo is what the report exposes about fingerprint comparability.
type FingerprintInfo struct {
	Comparable  bool   `json:"comparable"`
	SourceScope string `json:"source_scope,omitempty"`
	TargetScope string `json:"target_scope,omitempty"`
	Reason      string `json:"reason,omitempty"`
}

// FingerprintComparable decides whether the two sides' fingerprints may be
// compared at all. Empty scope means "this side did not declare one"; it is
// only safe when the other side is also undeclared (same-connector path where
// verify falls back to row-by-row anyway).
func FingerprintComparable(src, tgt Side) FingerprintInfo {
	a, b := scopeOf(src), scopeOf(tgt)
	info := FingerprintInfo{SourceScope: a, TargetScope: b}
	switch {
	case a == "" && b == "":
		info.Comparable = true
		return info
	case a == "" || b == "":
		info.Reason = "一端没有声明指纹作用域（跨端实现不同），不进行指纹比对，已改用逐行全表比对"
		return info
	case a != b:
		info.Reason = fmt.Sprintf("指纹作用域不同（%s vs %s），两个指纹不可比，已改用逐行全表比对", a, b)
		return info
	}
	info.Comparable = true
	return info
}

func scopeOf(side Side) string {
	if s, ok := side.(FingerprintScoper); ok {
		return s.FingerprintScope()
	}
	return ""
}
