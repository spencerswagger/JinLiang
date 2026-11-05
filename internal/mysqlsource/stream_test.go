package mysqlsource

import (
	"context"
	"net"
	"strings"
	"testing"

	"github.com/spencerswagger/jinliang/internal/dsn"
	"github.com/spencerswagger/jinliang/internal/source"
)

func TestReplicationStreamRefusesTLSFailClosed(t *testing.T) {
	// 复制连接还不支持 TLS：用户按 tls=verify_identity 建 job 时，必须**拒绝开流**，
	// 不能「SQL 侧加密、binlog 与口令交换明文」——那比不支持 TLS 更危险，因为它看起来是安全的。
	s := &Source{ep: dsn.Endpoint{
		Host: "192.0.2.1", Port: 3306, Database: "shop", Username: "u",
		Params: map[string]string{"tls": "verify_identity"},
	}}
	asm := NewAssembler(func(ctx context.Context, tx Tx) error { return nil })
	start := Position{File: "mysql-bin.000001", Pos: 4}

	err := s.RunStream(context.Background(), start, asm)
	if err == nil {
		t.Fatal("要求 TLS 时必须拒绝开流")
	}
	for _, want := range []string{"TLS", "明文", "风险"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("错误里应说清 %q（四段式），拿到: %v", want, err)
		}
	}
	// 必须是「拒绝」而不是「连不上」：不该有任何网络动作（192.0.2.1 不可达，会超时）。
	var opErr *net.OpError
	if ok := asNetOp(err, &opErr); ok {
		t.Fatalf("应该在拨号之前就拒绝，实际发生了网络错误: %v", err)
	}
}

func TestReplicationStreamAllowsExplicitlyNoTLS(t *testing.T) {
	// 明确关掉 TLS（或不写）时不该被 TLS 闸门拦住；这里只验证闸门放行，
	// 不去真连（用一个立刻关闭的本地端口，拿到的是连接错误而不是 TLS 拒绝）。
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("本机不能监听: %v", err)
	}
	addr := ln.Addr().(*net.TCPAddr)
	go func() {
		c, err := ln.Accept()
		if err == nil {
			c.Close()
		}
	}()
	defer ln.Close()

	for _, tlsVal := range []string{"", "false", "off"} {
		s := &Source{ep: dsn.Endpoint{
			Host: "127.0.0.1", Port: addr.Port, Database: "shop", Username: "u",
			Params: map[string]string{"tls": tlsVal},
		}}
		if err := s.checkReplicationTLS(); err != nil {
			t.Fatalf("tls=%q 不该被 TLS 闸门拦住: %v", tlsVal, err)
		}
	}
}

func asNetOp(err error, target **net.OpError) bool {
	for err != nil {
		if e, ok := err.(*net.OpError); ok {
			*target = e
			return true
		}
		type unwrapper interface{ Unwrap() error }
		u, ok := err.(unwrapper)
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

func TestStreamPublicEntryRefusesTLS(t *testing.T) {
	// 公开入口 Stream() 也必须拦：用户/引擎走的是这条，不是 RunStream。
	s := &Source{ep: dsn.Endpoint{
		Host: "192.0.2.1", Port: 3306, Database: "shop", Username: "u",
		Params: map[string]string{"tls": "verify_identity"},
	}}
	from, err := ParsePosition("mysql:v1:file=mysql-bin.000001;pos=4;coord=mysql_file_pos")
	if err != nil {
		t.Fatalf("位点: %v", err)
	}
	err = s.Stream(context.Background(), from, func(ctx context.Context, ev source.Event) error { return nil })
	if err == nil {
		t.Fatal("配了 tls 时 Stream() 必须拒绝开流")
	}
	if !strings.Contains(err.Error(), "TLS") || !strings.Contains(err.Error(), "风险") {
		t.Fatalf("错误要是四段式且说清 TLS: %v", err)
	}
}

func TestReplicationStreamRejectsUpperCaseTLSKey(t *testing.T) {
	// 用户写 ?TLS=verify_identity 时不能因为大小写不同就漏掉闸门——
	// 「配了加密却静默走明文」正是这条闸门要防的事。
	for _, key := range []string{"TLS", "Tls", "tls"} {
		for _, val := range []string{"verify_identity", "preferred", "skip-verify", "REQUIRED"} {
			s := &Source{ep: dsn.Endpoint{
				Host: "192.0.2.1", Port: 3306, Database: "shop", Username: "u",
				Params: map[string]string{key: val},
			}}
			err := s.checkReplicationTLS()
			if err == nil {
				t.Fatalf("%s=%s 必须被拒绝（复制连接不支持任何 TLS 形态）", key, val)
			}
			if !strings.Contains(err.Error(), "preferred") || !strings.Contains(err.Error(), "skip-verify") {
				t.Fatalf("文案要说清 preferred/skip-verify 同样被拒，拿到: %v", err)
			}
			if !strings.Contains(err.Error(), "tls=off") {
				t.Fatalf("文案要给「显式承认明文」的出路，拿到: %v", err)
			}
		}
	}
}

func TestReplicationStreamAcceptsUppercaseOffValues(t *testing.T) {
	// 键与值都要大小写归一：`TLS=OFF` / `Tls=False` / `TLS=NONE` 是「明确承认明文」，
	// 必须放行，否则用户按大写写了反而被拒——那会把人推向「干脆不写 tls」。
	for _, key := range []string{"tls", "TLS", "Tls"} {
		for _, val := range []string{"off", "OFF", "False", "FALSE", "none", "NONE", "disabled", "Disabled", ""} {
			s := &Source{ep: dsn.Endpoint{
				Host: "127.0.0.1", Port: 3306, Database: "shop", Username: "u",
				Params: map[string]string{key: val},
			}}
			if err := s.checkReplicationTLS(); err != nil {
				t.Fatalf("%s=%q 属于「明确不要 TLS」，应放行: %v", key, val, err)
			}
		}
	}
}
