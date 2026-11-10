package mysqlsource

import (
	"bytes"
	"encoding/hex"
	"testing"
	"time"

	"github.com/spencerswagger/jinliang/internal/source"
)

// 块边界必须**保类型**：同一段文本还原成比较参数时，二进制还是二进制、时间还是时间。
//
// 为什么这不是「编码风格」问题：块边界是续跑的唯一依据（`_jl_chunk.range_to`）。
// 用 `%v` 打印 Go 值会把 BINARY(16) 主键编成 `"[0 0 …]"`、把 DATETIME(6) 编成
// `"2026-11-01 10:00:00.000001 +0000 UTC"`；拿这种文本去 `WHERE (pk) > (?)` 比，
// 二进制列按字节比（`'['`=0x5B 让绝大多数 UUID 落在它之前），续跑会**直接跳到表尾**，
// 已提交块之后的行不再读，却照常 MarkTableDone、退出码 0 —— 静默少行。
func TestCursorRoundTripKeepsTypes(t *testing.T) {
	ts := time.Date(2026, 11, 1, 10, 0, 0, 123456000, time.UTC)
	bin := []byte{0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff}

	in := []source.Value{
		{Kind: source.KindBytes, B: bin},
		{Kind: source.KindInt, I: -42},
		{Kind: source.KindTime, T: ts},
	}
	cur := EncodeCursor(in)
	if cur == "" {
		t.Fatal("非空主键必须编出非空边界")
	}
	// 边界文本里不许出现 Go 的打印形态（那是丢类型的信号）。
	for _, bad := range []string{"[0 ", "+0000 UTC", "UTC"} {
		if bytes.Contains([]byte(cur), []byte(bad)) {
			t.Fatalf("块边界里出现了 Go 的打印形态 %q（会丢类型）：%s", bad, cur)
		}
	}

	args, err := decodeCursor(cur)
	if err != nil {
		t.Fatalf("解码失败: %v", err)
	}
	if len(args) != 3 {
		t.Fatalf("应还原 3 个键，实际 %d", len(args))
	}
	gotBin, ok := args[0].([]byte)
	if !ok {
		t.Fatalf("二进制主键必须还原成 []byte（否则按文本比会错位），实际 %T", args[0])
	}
	if !bytes.Equal(gotBin, bin) {
		t.Fatalf("二进制主键还原不一致：%x != %x", gotBin, bin)
	}
	if got, ok := args[1].(int64); !ok || got != -42 {
		t.Fatalf("整数主键必须还原成 int64，实际 %T %v", args[1], args[1])
	}
	// 时间列交给 MySQL 按列类型解析：必须是 MySQL 认的字面量，且保留微秒。
	lit, ok := args[2].(string)
	if !ok {
		t.Fatalf("时间主键应还原成 MySQL 字面量字符串，实际 %T", args[2])
	}
	if lit != "2026-11-01 10:00:00.123456" {
		t.Fatalf("时间字面量不对（要 UTC + 微秒）：%q", lit)
	}
}

// 老边界（纯字符串数组）仍要能读：升级过程中目标库里可能还留着上一版写的 range_to。
func TestDecodeCursorAcceptsLegacyStringArray(t *testing.T) {
	args, err := decodeCursor(`["1000","abc"]`)
	if err != nil {
		t.Fatalf("老格式应兼容: %v", err)
	}
	if len(args) != 2 || args[0] != "1000" || args[1] != "abc" {
		t.Fatalf("老格式解码结果不对: %#v", args)
	}
	if _, err := decodeCursor(`{"not":"an array"}`); err == nil {
		t.Fatal("读不懂的边界必须报错（不能静默从表头重读）")
	}
}

// 复合主键：类型逐列保留，顺序不变。
func TestCursorRoundTripCompositeKey(t *testing.T) {
	in := []source.Value{
		{Kind: source.KindBytes, B: []byte{0xde, 0xad}},
		{Kind: source.KindString, S: "2026-11-01"},
		{Kind: source.KindUint, U: 18446744073709551615},
	}
	args, err := decodeCursor(EncodeCursor(in))
	if err != nil {
		t.Fatal(err)
	}
	if b, ok := args[0].([]byte); !ok || hex.EncodeToString(b) != "dead" {
		t.Fatalf("第一列应是二进制 dead，实际 %#v", args[0])
	}
	if s, ok := args[1].(string); !ok || s != "2026-11-01" {
		t.Fatalf("第二列应是字符串，实际 %#v", args[1])
	}
	if u, ok := args[2].(uint64); !ok || u != 18446744073709551615 {
		t.Fatalf("第三列应是 uint64（bigint unsigned 的上界不能丢），实际 %#v", args[2])
	}
}
