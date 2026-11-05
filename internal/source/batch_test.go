package source

import (
	"errors"
	"strings"
	"testing"
)

func TestValidateBatch(t *testing.T) {
	begin := Event{Kind: EvBegin, TxID: "t1"}
	ins := Event{Kind: EvInsert, TxID: "t1"}
	commit := Event{Kind: EvCommit, TxID: "t1"}
	beat := Event{Kind: EvHeartbeat}
	ddl := Event{Kind: EvDDL, DDL: "alter table t add c int"}

	cases := []struct {
		name string
		evs  []Event
		ok   bool
	}{
		{"空批次（只推进位点）", nil, true},
		{"纯心跳", []Event{beat, beat}, true},
		{"完整事务", []Event{begin, ins, commit}, true},
		{"两个完整事务", []Event{begin, ins, commit, begin, ins, commit}, true},
		{"续段 + 收尾（前半段在上一批）", []Event{ins, commit}, true},
		{"事务外 DDL", []Event{ddl}, true},
		{"心跳夹在事务里", []Event{begin, beat, ins, commit}, true},
		{"未收尾的事务", []Event{begin, ins}, false},
		{"以 EvBegin 结尾", []Event{ins, commit, begin}, false},
		{"事务嵌套", []Event{begin, begin, ins, commit}, false},
		// 规则 4：提交完之后又凭空冒出一条数据事件（Owl 在 #7 复核时发现的洞）
		{"提交后裸数据事件", []Event{ins, commit, ins}, false},
		{"只有提交再裸数据事件", []Event{commit, ins}, false},
		{"提交后开新事务再写", []Event{ins, commit, begin, ins, commit}, true},
		{"两个事务之间夹心跳再开新事务", []Event{begin, ins, commit, beat, begin, ins, commit}, true},
		{"提交后只有 DDL（事务外合法）", []Event{begin, ins, commit, ddl}, true},
	}
	for _, c := range cases {
		err := ValidateBatch(c.evs)
		if c.ok && err != nil {
			t.Errorf("%s：期望通过，却报 %v", c.name, err)
		}
		if !c.ok && err == nil {
			t.Errorf("%s：期望报错，却通过了", c.name)
		}
	}
}

func TestFormatAndSplitPosition(t *testing.T) {
	got := FormatPosition("postgres", "0/16B6C50")
	if got != "postgres:v1:0/16B6C50" {
		t.Fatalf("FormatPosition = %q", got)
	}
	kind, payload, err := SplitPosition(got)
	if err != nil || kind != "postgres" || payload != "0/16B6C50" {
		t.Fatalf("SplitPosition = %q %q %v", kind, payload, err)
	}
	// payload 里可以有 ':'（MySQL 的 mysql-bin.000003:1234）
	kind, payload, err = SplitPosition(FormatPosition("mysql", "mysql-bin.000003:1234"))
	if err != nil || kind != "mysql" || payload != "mysql-bin.000003:1234" {
		t.Fatalf("带冒号的载荷解析失败：%q %q %v", kind, payload, err)
	}

	bad := []string{"", "0/16B6C50", "postgres:v1", "postgres:v2:0/16B6C50", ":v1:x", "postgres:v1:"}
	for _, s := range bad {
		if _, _, err := SplitPosition(s); err == nil {
			t.Errorf("SplitPosition(%q) 应该报错", s)
		} else if !errors.Is(err, ErrPositionMalformed) {
			t.Errorf("SplitPosition(%q) 的错误要能被 errors.Is(ErrPositionMalformed) 认出来，得到 %v", s, err)
		}
	}
	// 截断显示，避免把一整串位点灌进日志
	_, _, err = SplitPosition(strings.Repeat("x", 200))
	if err == nil || len(err.Error()) > 200 {
		t.Fatalf("长位点的报错应该被截断：%v", err)
	}
}
