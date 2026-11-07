package mysqlsource

import (
	"strings"
	"testing"

	"github.com/spencerswagger/jinliang/internal/source"
)

func TestSafeAckPointEmptyUntilCommitted(t *testing.T) {
	// MySQL 没有服务端 ack，但「已交付到哪」必须读得出来：没交付过 → ok=false。
	s := &Source{}
	if _, ok := s.SafeAckPoint(); ok {
		t.Fatal("还没交付任何事务时不该给出 ack 点")
	}
	if s.LastCommit() != nil {
		t.Fatal("LastCommit 此时应为 nil")
	}
	p, err := ParsePosition("mysql:v1:file=mysql-bin.000003;pos=197;coord=mysql_file_pos")
	if err != nil {
		t.Fatal(err)
	}
	s.markCommitted(p)
	got, ok := s.SafeAckPoint()
	if !ok {
		t.Fatal("交付过之后应给出 ack 点")
	}
	if got.String() != p.String() {
		t.Fatalf("ack 点 = %q，期望 %q", got.String(), p.String())
	}
	// Ack 是 no-op：不许 panic、也不许改状态。
	s.Ack(got)
	if again, _ := s.SafeAckPoint(); again.String() != p.String() {
		t.Fatal("Ack 不该改变 ack 点")
	}
}

func TestCapabilityErrorsAreFourPart(t *testing.T) {
	// 缺能力必须是四段式错误，不能 panic、不能返回空值糊过去。
	s := &Source{}
	if _, err := s.MaxWatermark(nil, source.TableRef{Table: "t"}, ""); err == nil {
		t.Fatal("没有水位列时必须报错")
	} else if !strings.Contains(err.Error(), "你能做的") || !strings.Contains(err.Error(), "风险") {
		t.Fatalf("错误要是四段式（发生了什么/为什么/能做什么/风险），拿到: %v", err)
	}

	// ReadWatermarkPage 同样。
	if _, _, err := s.ReadWatermarkPage(nil, source.TableRef{Table: "t"}, "", "", 10); err == nil {
		t.Fatal("没有水位列时必须报错")
	} else if !strings.Contains(err.Error(), "风险") {
		t.Fatalf("错误要是四段式: %v", err)
	}
}

func TestEarlierThanOrdersBinlogPositions(t *testing.T) {
	// 二进制文件名必须按数字段比较：mysql-bin.000009 早于 mysql-bin.000010。
	// 直接比字符串在这两位上是巧合正确的，换成 .0000099 就会错——所以要有这条用例。
	a, err := ParsePosition("mysql:v1:file=mysql-bin.000009;pos=900;coord=mysql_file_pos")
	if err != nil {
		t.Fatal(err)
	}
	b, err := ParsePosition("mysql:v1:file=mysql-bin.000010;pos=4;coord=mysql_file_pos")
	if err != nil {
		t.Fatal(err)
	}
	if !a.EarlierThan(b) {
		t.Fatal("a 应早于 b")
	}
	if b.EarlierThan(a) {
		t.Fatal("b 不该早于 a（位点会回退）")
	}
	if a.EarlierThan(a) {
		t.Fatal("同一位置不该算「更早」")
	}
	// 同一个文件里比偏移。
	c, _ := ParsePosition("mysql:v1:file=mysql-bin.000010;pos=100;coord=mysql_file_pos")
	if !b.EarlierThan(c) {
		t.Fatal("同文件应比偏移")
	}

	// 不认识的对端串：保守返回 false（宁可不动位点）。
	foreign, _ := ParsePosition("postgres:v1:0/16B6C50")
	if a.EarlierThan(foreign) {
		t.Fatal("解析不了的对端不该被判成「更早」")
	}
}
