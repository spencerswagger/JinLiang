package netcheck

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

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

// A-1 的绕过（Owl 复现过）：控制台只听 127.0.0.1 + 同机反代时，远端请求的
// RemoteAddr 恒为 127.0.0.1。所以「本机」必须再要求没有转发头。
func TestIsLoopbackRequestRejectsForwardedTraffic(t *testing.T) {
	mk := func(remote string, headers map[string]string) *http.Request {
		r := httptest.NewRequest(http.MethodGet, "/api/session", nil)
		r.RemoteAddr = remote
		for k, v := range headers {
			r.Header.Set(k, v)
		}
		return r
	}
	cases := []struct {
		name    string
		remote  string
		headers map[string]string
		want    bool
	}{
		{"裸本机，无转发头", "127.0.0.1:54321", nil, true},
		{"本机 + X-Forwarded-For（Owl 的复现用例）", "127.0.0.1:54321",
			map[string]string{"X-Forwarded-For": "203.0.113.9"}, false},
		{"本机 + X-Real-IP", "127.0.0.1:54321", map[string]string{"X-Real-IP": "203.0.113.9"}, false},
		{"本机 + Forwarded", "127.0.0.1:54321",
			map[string]string{"Forwarded": "for=203.0.113.9"}, false},
		// Owl 的 fail-open 复现：空白值过去被 TrimSpace 成空串 → 判本机 → 放行。
		// 现在按「头存在即拒绝」，四种写法都必须 false。
		{"本机 + 空白值", "127.0.0.1:54321", map[string]string{"X-Forwarded-For": "  "}, false},
		{"本机 + 单空格", "127.0.0.1:54321", map[string]string{"X-Real-IP": " "}, false},
		{"本机 + 纯 tab", "127.0.0.1:54321", map[string]string{"Forwarded": "\t"}, false},
		{"本机 + 空值逗号", "127.0.0.1:54321", map[string]string{"X-Forwarded-For": ",,"}, false},
		{"本机 + 空串", "127.0.0.1:54321", map[string]string{"X-Forwarded-For": ""}, false},
		{"非本机，无转发头", "10.0.0.9:5555", nil, false},
		{"非本机 + 转发头", "10.0.0.9:5555", map[string]string{"X-Forwarded-For": "203.0.113.9"}, false},
		{"[::1] 无转发头", "[::1]:5555", nil, true},
	}
	for _, c := range cases {
		if got := IsLoopbackRequest(mk(c.remote, c.headers)); got != c.want {
			t.Errorf("%s：IsLoopbackRequest = %v，期望 %v", c.name, got, c.want)
		}
	}
	if IsLoopbackRequest(nil) {
		t.Error("nil request 必须判 false")
	}
}
