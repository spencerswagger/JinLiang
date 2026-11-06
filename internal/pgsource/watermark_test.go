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
