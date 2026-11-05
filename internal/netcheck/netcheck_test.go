package netcheck

import "testing"

// P1 回归（原 cmd/jinliang 的用例迁到这里，并扩到客户端来源判定）：
// ":8080" / "0.0.0.0:8080" / "[::]:8080" 都是绑全网卡，必须判成 non-loopback。
func TestIsLoopbackListen(t *testing.T) {
	cases := []struct {
		listen string
		want   bool
	}{
		{"127.0.0.1:8080", true},
		{"127.0.0.1:0", true},
		{"localhost:8080", true},
		{"[::1]:8080", true},
		{"::1:8080", false}, // 不带方括号无法按 host:port 解析，按不安全处理
		{"0.0.0.0:8080", false},
		{":8080", false}, // 空 host = 绑全网卡（P1 就错在这里）
		{"", false},
		{"[::]:8080", false},
		{"192.168.1.10:8080", false},
		{"0.0.0.0", false},
		{"127.0.0.1", true},
	}
	for _, c := range cases {
		if got := IsLoopbackListen(c.listen); got != c.want {
			t.Errorf("IsLoopbackListen(%q) = %v, want %v", c.listen, got, c.want)
		}
	}
}

func TestHost(t *testing.T) {
	cases := map[string]string{
		"127.0.0.1:8080": "127.0.0.1",
		":8080":          "",
		"[::1]:8080":     "::1",
		"[::]:8080":      "::",
		"localhost:8080": "localhost",
		"127.0.0.1":      "127.0.0.1",
	}
	for in, want := range cases {
		if got := Host(in); got != want {
			t.Errorf("Host(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestIsLoopbackClient(t *testing.T) {
	cases := []struct {
		remote string
		want   bool
	}{
		{"127.0.0.1:54321", true},
		{"[::1]:54321", true},
		{"localhost:1234", true},
		{"192.168.1.50:5555", false},
		{"10.0.0.9:80", false},
		{"203.0.113.7:443", false},
		{"", false},
		{"not-an-address", false},
	}
	for _, c := range cases {
		if got := IsLoopbackClient(c.remote); got != c.want {
			t.Errorf("IsLoopbackClient(%q) = %v, want %v", c.remote, got, c.want)
		}
	}
}
