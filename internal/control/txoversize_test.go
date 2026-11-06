package control

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

const legacyJobYAML = `version: 1
job: orders-to-report
source:
  kind: postgres
  dsn: env:JINLIANG_SOURCE_DSN
  scope: {schema: public, include: ["orders*"]}
target:
  kind: postgres
  dsn: env:JINLIANG_TARGET_DSN
mode:
  frequency: once
reliability:
  batch_rows: 500
  tx_oversize: fail
`

func parseDoc(t *testing.T, y string) JobDoc {
	t.Helper()
	doc, err := ParseJobYAML([]byte(y))
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	return doc
}

// 1–2：旧字符串两种取值都要解析出来，且记住原形态（导出要原样写回）。
func TestTxOversizeLegacyStrings(t *testing.T) {
	for _, v := range []string{"spill", "fail"} {
		doc := parseDoc(t, strings.Replace(legacyJobYAML, "tx_oversize: fail", "tx_oversize: "+v, 1))
		if doc.Reliability == nil || doc.Reliability.TxOversize == nil {
			t.Fatalf("%s：tx_oversize 没解析出来", v)
		}
		got := doc.Reliability.TxOversize
		if got.ModeString() != v || got.Legacy() != v || got.Bytes != nil || got.Events != nil {
			t.Fatalf("%s：解析结果不对 %+v", v, got)
		}
		if err := doc.Validate(); err != nil {
			t.Fatalf("%s：应当合法，却报 %v", v, err)
		}
	}
}

// 3：对象写法的三个字段。
func TestTxOversizeObjectAllFields(t *testing.T) {
	y := strings.Replace(legacyJobYAML, "tx_oversize: fail",
		"tx_oversize: {mode: fail, bytes: 134217728, events: 2000000}", 1)
	doc := parseDoc(t, y)
	got := doc.Reliability.TxOversize
	if got.Mode != "fail" || got.Bytes == nil || *got.Bytes != 134217728 || got.Events == nil || *got.Events != 2000000 {
		t.Fatalf("对象解析不对 %+v", got)
	}
	if got.Legacy() != "" {
		t.Fatalf("对象形态不该有 legacy，得到 %q", got.Legacy())
	}
	if err := doc.Validate(); err != nil {
		t.Fatalf("应当合法，却报 %v", err)
	}
	b, e := got.Limits(nil)
	if e != 2000000 || b != 134217728 {
		t.Fatalf("Limits = %d/%d", b, e)
	}
}

// 4：对象只给 mode。
func TestTxOversizeObjectModeOnly(t *testing.T) {
	y := strings.Replace(legacyJobYAML, "tx_oversize: fail", "tx_oversize: {mode: spill}", 1)
	doc := parseDoc(t, y)
	got := doc.Reliability.TxOversize
	if got.ModeString() != "spill" || got.Bytes != nil || got.Events != nil {
		t.Fatalf("解析不对 %+v", got)
	}
	if err := doc.Validate(); err != nil {
		t.Fatalf("应当合法：%v", err)
	}
}

// 5：对象里的未知键必须报错（自定义解析不能把 strict 关掉）。
func TestTxOversizeObjectUnknownKey(t *testing.T) {
	y := strings.Replace(legacyJobYAML, "tx_oversize: fail", "tx_oversize: {mode: fail, foo: 1}", 1)
	if _, err := ParseJobYAML([]byte(y)); err == nil {
		t.Fatal("未知键应当报错")
	} else if !strings.Contains(err.Error(), "不认识") {
		t.Fatalf("错误文案要指出不认识的键：%v", err)
	}
}

// 6：边界值——bytes 小于 1 MiB、events 小于 1000、mode 不是 spill/fail。
func TestTxOversizeBoundaries(t *testing.T) {
	cases := []string{
		"tx_oversize: {bytes: 1}",
		"tx_oversize: {events: 999}",
		"tx_oversize: bogus",
		"tx_oversize: {mode: bogus}",
	}
	for _, frag := range cases {
		doc := parseDoc(t, strings.Replace(legacyJobYAML, "tx_oversize: fail", frag, 1))
		if err := doc.Validate(); err == nil {
			t.Errorf("%q 应当被拒", frag)
		}
	}
}

// 7：对象为空 = 全默认，合法。
func TestTxOversizeEmptyObject(t *testing.T) {
	doc := parseDoc(t, strings.Replace(legacyJobYAML, "tx_oversize: fail", "tx_oversize: {}", 1))
	if err := doc.Validate(); err != nil {
		t.Fatalf("空对象应当合法：%v", err)
	}
	b, e := doc.Reliability.TxOversize.Limits(func(string) (string, bool) { return "", false })
	if b != DefaultTxMaxBytes || e != DefaultTxMaxEvents {
		t.Fatalf("空对象应回落内置默认，得到 %d/%d", b, e)
	}
}

// 8：yaml 没给的字段回落环境变量；yaml 显式值压过环境变量。
func TestTxOversizeLimitsPrecedence(t *testing.T) {
	env := func(k string) (string, bool) {
		switch k {
		case envTxMaxBytes:
			return "268435456", true // 256 MB
		case envTxMaxEvents:
			return "5000000", true
		}
		return "", false
	}
	// 只有 mode：字节/事件都来自环境变量
	doc := parseDoc(t, strings.Replace(legacyJobYAML, "tx_oversize: fail", "tx_oversize: {mode: fail}", 1))
	if b, e := doc.Reliability.TxOversize.Limits(env); b != 268435456 || e != 5000000 {
		t.Fatalf("应回落环境变量，得到 %d/%d", b, e)
	}
	// yaml 显式给 bytes：压过环境变量；events 仍来自环境变量
	doc = parseDoc(t, strings.Replace(legacyJobYAML, "tx_oversize: fail", "tx_oversize: {bytes: 134217728}", 1))
	if b, e := doc.Reliability.TxOversize.Limits(env); b != 134217728 || e != 5000000 {
		t.Fatalf("yaml 显式值应优先，得到 %d/%d", b, e)
	}
	// 环境变量非法 → 回落默认
	bad := func(string) (string, bool) { return "not-a-number", true }
	if b, e := doc.Reliability.TxOversize.Limits(bad); b != 134217728 || e != DefaultTxMaxEvents {
		t.Fatalf("环境变量非法应回落默认，得到 %d/%d", b, e)
	}
}

// 9（最重要）：旧 yaml 的解析结果与序列化结果都和"改动前"逐字段相同——
// 导出的 tx_oversize 必须仍是 `fail` 这个标量，不能变成对象。
func TestTxOversizeLegacyRoundTripUnchanged(t *testing.T) {
	doc := parseDoc(t, legacyJobYAML)
	out, err := MarshalJobYAML(doc)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out), "tx_oversize: fail") {
		t.Fatalf("旧写法导出后必须仍是标量，实际：\n%s", out)
	}
	if strings.Contains(string(out), "tx_oversize:\n") {
		t.Fatalf("旧写法被写成了对象：\n%s", out)
	}
	// 再解析回来，字段一致
	again, err := ParseJobYAML(out)
	if err != nil {
		t.Fatalf("导出物应可再解析：%v", err)
	}
	if again.Reliability.TxOversize.Legacy() != "fail" {
		t.Fatalf("往返后形态变了：%+v", again.Reliability.TxOversize)
	}
}

// 10：对象写法的导出/导入往返保持对象形态。
func TestTxOversizeObjectRoundTrip(t *testing.T) {
	y := strings.Replace(legacyJobYAML, "tx_oversize: fail",
		"tx_oversize: {mode: fail, bytes: 134217728}", 1)
	doc := parseDoc(t, y)
	out, err := MarshalJobYAML(doc)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out), "mode: fail") || !strings.Contains(string(out), "bytes: 134217728") {
		t.Fatalf("对象写法导出后丢了字段：\n%s", out)
	}
	again, err := ParseJobYAML(out)
	if err != nil {
		t.Fatalf("导出的对象应可再解析：%v", err)
	}
	if again.Reliability.TxOversize.Legacy() != "" || again.Reliability.TxOversize.Bytes == nil {
		t.Fatalf("往返后形态变了：%+v", again.Reliability.TxOversize)
	}
}

// 11：spec_json 的 JSON 形态同样双向兼容。
func TestTxOversizeJSONRoundTrip(t *testing.T) {
	doc := parseDoc(t, legacyJobYAML)
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	blob := string(raw)
	if !strings.Contains(blob, `"tx_oversize":"fail"`) {
		t.Fatalf("旧写法进 spec_json 必须是字符串：%s", blob)
	}
	var back JobDoc
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatal(err)
	}
	if back.Reliability.TxOversize.Legacy() != "fail" {
		t.Fatalf("JSON 往返形态变了：%+v", back.Reliability.TxOversize)
	}

	obj := parseDoc(t, strings.Replace(legacyJobYAML, "tx_oversize: fail", "tx_oversize: {events: 2000000}", 1))
	raw2, err := json.Marshal(obj)
	if err != nil {
		t.Fatal(err)
	}
	blob2 := string(raw2)
	if !strings.Contains(blob2, `"tx_oversize":{"events":2000000}`) {
		t.Fatalf("对象写法进 spec_json 必须是对象：%s", blob2)
	}
	// JSON 里出现未知键要被拒（与 yaml 同口径）
	var badJob JobDoc
	var rawBad = []byte(`{"reliability":{"tx_oversize":{"foo":1}}}`)
	if err := json.Unmarshal(rawBad, &badJob); err == nil {
		t.Fatal("JSON 未知键应当报错")
	}
}

// 12：没写 tx_oversize 时 Limits 仍给出默认/环境值（不能因为指针为 nil 就崩）。
func TestTxOversizeAbsent(t *testing.T) {
	y := strings.Replace(legacyJobYAML, "  tx_oversize: fail\n", "", 1)
	doc := parseDoc(t, y)
	if doc.Reliability.TxOversize != nil {
		t.Fatalf("没写就不该有值：%+v", doc.Reliability.TxOversize)
	}
	var nilDoc *TxOversizeDoc
	if b, e := nilDoc.Limits(func(k string) (string, bool) {
		if k == envTxMaxEvents {
			return "3000", true
		}
		return "", false
	}); b != DefaultTxMaxBytes || e != 3000 {
		t.Fatalf("nil 指针应回落默认/环境，得到 %d/%d", b, e)
	}
	if nilDoc.Problems() != nil || nilDoc.ModeString() != "" {
		t.Fatal("nil 指针的方法要安全")
	}
}

func init() {
	// 避免测试里真的读进程环境（CI 上可能设了 JINLIANG_TX_MAX_*）
	os.Unsetenv(envTxMaxBytes)
	os.Unsetenv(envTxMaxEvents)
}

// P3-1：Limits() 也必须钳下限——只调 Limits（不先 Validate）的路径不能让
// `bytes: 100` 真的生效，否则「上限 100 字节 → 所有事务都超限」。
func TestTxOversizeLimitsClampsMinimum(t *testing.T) {
	doc := parseDoc(t, strings.Replace(legacyJobYAML, "tx_oversize: fail",
		"tx_oversize: {bytes: 100, events: 10}", 1))
	// 先确认 Validate 会拒（两处口径一致）
	if err := doc.Validate(); err == nil {
		t.Fatal("bytes:100 / events:10 应当被 Validate 拒")
	}
	gotBytes, gotEvents := doc.Reliability.TxOversize.Limits(func(string) (string, bool) { return "", false })
	if gotBytes != MinTxMaxBytes || gotEvents != MinTxMaxEvents {
		t.Fatalf("Limits 应钳到下限 %d/%d，实际 %d/%d",
			MinTxMaxBytes, MinTxMaxEvents, gotBytes, gotEvents)
	}
	// 环境变量误配同样钳下限
	env := func(k string) (string, bool) {
		switch k {
		case envTxMaxBytes:
			return "100", true
		case envTxMaxEvents:
			return "10", true
		}
		return "", false
	}
	var nilDoc *TxOversizeDoc
	if b, e := nilDoc.Limits(env); b != MinTxMaxBytes || e != MinTxMaxEvents {
		t.Fatalf("环境变量也应钳下限，实际 %d/%d", b, e)
	}
}

// P3-2：语义被改过就不能再按旧形态导出（要么保形、要么保语义）。
func TestTxOversizeMutationKeepsSemantics(t *testing.T) {
	// 旧字符串 fail → 只加 bytes：导出必须是对象，且带 mode: fail（语义不能丢）
	doc := parseDoc(t, legacyJobYAML)
	doc.Reliability.TxOversize.Bytes = intPtr(134217728)
	out, err := MarshalJobYAML(doc)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(out), "tx_oversize: fail") {
		t.Fatalf("语义改过后不能再写回纯字符串：\n%s", out)
	}
	if !strings.Contains(string(out), "mode: fail") || !strings.Contains(string(out), "bytes: 134217728") {
		t.Fatalf("对象导出要保留 mode 与 bytes：\n%s", out)
	}

	// 旧字符串 fail → 把 Mode 改成 spill：导出必须是 spill，不能还是 fail
	doc2 := parseDoc(t, legacyJobYAML)
	doc2.Reliability.TxOversize.Mode = "spill"
	out2, err := MarshalJobYAML(doc2)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(out2), "tx_oversize: fail") || !strings.Contains(string(out2), "mode: spill") {
		t.Fatalf("Mode 改了却还写旧值：\n%s", out2)
	}

	// 什么都没改 → 仍按旧形态导回（保形），这条与 9 号用例互补
	doc3 := parseDoc(t, legacyJobYAML)
	out3, _ := MarshalJobYAML(doc3)
	if !strings.Contains(string(out3), "tx_oversize: fail") {
		t.Fatalf("没改过就该保形：\n%s", out3)
	}
}

func intPtr(v int) *int { return &v }
