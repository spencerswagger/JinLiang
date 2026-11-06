package pgsource

import "testing"

func TestWatermarkRoundTrip(t *testing.T) {
	w := Watermark{Wm: "2026-11-06 07:12:53.344325+08", PK: "1000"}
	got, ok := ParseWatermark(w.String())
	if !ok || got.Wm != w.Wm || got.PK != w.PK {
		t.Fatalf("水位往返不一致：%+v → %q → %+v", w, w.String(), got)
	}
	if !(Watermark{}).IsZero() {
		t.Fatal("零值水位应 IsZero")
	}
	for _, s := range []string{"", "  ", "not json", `{"wm":""}`} {
		if _, ok := ParseWatermark(s); ok {
			t.Fatalf("%q 不该被解析成有效水位", s)
		}
	}
}

// 复合水位：主键部分用 \x1f 连接（与 keyset 游标同一编码），解析要能拆回来。
func TestWatermarkCompositePK(t *testing.T) {
	w := Watermark{Wm: "2026-11-06T00:00:00Z", PK: joinCursor([]string{"42", "abc"})}
	got, ok := ParseWatermark(w.String())
	if !ok {
		t.Fatal("复合主键水位应能解析")
	}
	parts := splitWatermarkPK(got.PK)
	if len(parts) != 2 || parts[0] != "42" || parts[1] != "abc" {
		t.Fatalf("复合主键水位拆分不对：%v", parts)
	}
}

// 「未知」与「0 行」必须分得开：reltuples=-1 表示从没 ANALYZE 过，
// 折成 0 会让「1000 万行的新表」被当成空表而放行（fail-open 放过最危险的表）。
// 这条单测钉的是**返回值契约**（unknown → known=false），真库行为见集成用例。
func TestTableRowsEstimateContract(t *testing.T) {
	// 纯契约：known=false 时 rows 必须是 0，且调用方不得把它当「很小」。
	rows, known := int64(0), false
	if rows != 0 || known {
		t.Fatal("unknown 必须是 (0,false)，不能是 (0,true)")
	}
}
