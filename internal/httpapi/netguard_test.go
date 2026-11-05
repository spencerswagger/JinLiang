package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/spencerswagger/jinliang/internal/control"
)

// newServerWithListen builds a Server with an explicit --listen value, without
// starting a real listener (doctor reads the configured value, not the socket).
func newServerWithListen(t *testing.T, listen string) (*Server, *control.Store) {
	t.Helper()
	store, err := control.Open(control.Options{DataDir: t.TempDir(), Autogen: true})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	srv, err := New(Options{Store: store, Version: "test", Listen: listen})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	return srv, store
}

// P1 回归：`--listen :8080` 是绑所有网卡，doctor 必须报问题。
// 旧实现里 httpapi 自己有一份 isLoopback，把空 host 判成 loopback，
// 于是 doctor 报 healthy:true，与 serve 刚打印的暴露警告自相矛盾。
func TestDoctorFlagsWildcardListen(t *testing.T) {
	for _, listen := range []string{":8080", "0.0.0.0:8080", "[::]:8080"} {
		srv, store := newServerWithListen(t, listen)
		ts := httptest.NewServer(srv.Handler())
		token := setupAdmin(t, ts)
		code, body := doJSON(t, http.MethodGet, ts.URL+"/api/doctor", token, nil)
		ts.Close()
		if code != http.StatusOK {
			t.Fatalf("%s: doctor = %d", listen, code)
		}
		problems, _ := body["problems"].([]any)
		found := false
		for _, p := range problems {
			if s, _ := p.(string); strings.Contains(s, "监听地址不是本机") {
				found = true
			}
		}
		if !found {
			t.Fatalf("%s: doctor 必须把「监听在所有网卡」报成问题，实际 problems=%v", listen, problems)
		}
		_ = store
	}
}

// 对照：只听 127.0.0.1 时 doctor 不该报这条（否则用户会学会忽略警告）。
func TestDoctorAcceptsLoopbackListen(t *testing.T) {
	srv, _ := newServerWithListen(t, "127.0.0.1:8080")
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	token := setupAdmin(t, ts)
	_, body := doJSON(t, http.MethodGet, ts.URL+"/api/doctor", token, nil)
	problems, _ := body["problems"].([]any) // 没有问题时是 null，不能直接断言
	for _, p := range problems {
		if s, _ := p.(string); strings.Contains(s, "监听地址不是本机") {
			t.Fatalf("只听本机不该报暴露风险：%v", body["problems"])
		}
	}
}

// A-1 回归：还没设口令时，非本机的请求不能成为管理员。
// CSRF 挡不住直连（攻击者自己 GET /api/session 就能拿到 token），所以守卫按来源判。
func TestSetupFromRemoteIsRefused(t *testing.T) {
	srv, store := newServerWithListen(t, "0.0.0.0:8080")
	h := srv.Handler()

	// 先像正常客户端一样拿 CSRF cookie（这步远端也做得到）。
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/session", nil))
	csrf := ""
	for _, c := range rec.Result().Cookies() {
		if c.Name == csrfCookie {
			csrf = c.Value
		}
	}
	if csrf == "" {
		t.Fatal("没有种下 csrf cookie")
	}

	req := httptest.NewRequest(http.MethodPost, "/api/setup",
		strings.NewReader(`{"password":"remote-attacker-password"}`))
	req.RemoteAddr = "203.0.113.9:51234"
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(csrfHeader, csrf)
	req.AddCookie(&http.Cookie{Name: csrfCookie, Value: csrf})
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("远端首启设口令必须 403，实际 %d %s", rec.Code, rec.Body.String())
	}
	var env errorEnvelope
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("响应不是 JSON: %v", err)
	}
	if !strings.Contains(env.Error.Message, "只能在") || !strings.Contains(env.Error.Message, "机器") {
		t.Fatalf("四段式文案要说清「只能在本机设」: %+v", env.Error)
	}
	configured, err := store.AdminConfigured(req.Context())
	if err != nil {
		t.Fatal(err)
	}
	if configured {
		t.Fatal("远端请求竟然把管理员口令设上了")
	}
}

// A-1 补漏（#6 同批）：只听 127.0.0.1 + 同机反代时，每个请求的 RemoteAddr 都是
// 本机；只要带了转发头，就必须按非本机处理（fail-closed）。
func TestSetupBehindProxyIsRefused(t *testing.T) {
	srv, _ := newServerWithListen(t, "127.0.0.1:8080")
	h := srv.Handler()

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/session", nil))
	csrf := ""
	for _, c := range rec.Result().Cookies() {
		if c.Name == csrfCookie {
			csrf = c.Value
		}
	}

	req := httptest.NewRequest(http.MethodPost, "/api/setup",
		strings.NewReader(`{"password":"proxied-attacker-password"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(csrfHeader, csrf)
	req.Header.Set("X-Forwarded-For", "203.0.113.9")
	req.AddCookie(&http.Cookie{Name: csrfCookie, Value: csrf})
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("带转发头的首启请求必须 403（fail-closed），实际 %d", rec.Code)
	}
}

// Owl 的 fail-open 复现：转发头是**空白值**时，过去被 TrimSpace 成空串 →
// 判成本机 → 首启守卫放行。反代若原样透传客户端的头，远端就能抢先当管理员。
// 现在规则是「头存在即拒绝」，四种写法都必须 403，且控制库仍然没有管理员口令。
func TestSetupRejectsBlankForwardingHeaders(t *testing.T) {
	cases := []struct {
		name   string
		header string
		value  string
	}{
		{"空白值", "X-Forwarded-For", "  "},
		{"单空格", "X-Real-IP", " "},
		{"纯 tab", "Forwarded", "\t"},
		{"空值逗号", "X-Forwarded-For", ",,"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			srv, store := newServerWithListen(t, "127.0.0.1:8080")
			h := srv.Handler()

			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/session", nil))
			csrf := ""
			for _, ck := range rec.Result().Cookies() {
				if ck.Name == csrfCookie {
					csrf = ck.Value
				}
			}
			if csrf == "" {
				t.Fatal("没有种下 csrf cookie")
			}

			req := httptest.NewRequest(http.MethodPost, "/api/setup",
				strings.NewReader(`{"password":"blank-header-attacker"}`))
			// 必须是本机来源：否则守卫会因为 RemoteAddr 直接拒绝，测不到「空白转发头」这一条。
			req.RemoteAddr = "127.0.0.1:54321"
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set(csrfHeader, csrf)
			// 必须用 Set（会做 canonical key 规范化）：直接写 map 时 "X-Real-IP"
			// 与规范键 "X-Real-Ip" 对不上，等于头根本没发出去，测不到守卫。
			req.Header.Set(c.header, c.value) // 头存在、值是空白
			req.AddCookie(&http.Cookie{Name: csrfCookie, Value: csrf})
			rec = httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			if rec.Code != http.StatusForbidden {
				t.Fatalf("%s=%q 必须 403（头存在即拒绝），实际 %d %s",
					c.header, c.value, rec.Code, rec.Body.String())
			}
			configured, err := store.AdminConfigured(req.Context())
			if err != nil {
				t.Fatal(err)
			}
			if configured {
				t.Fatalf("%s=%q 竟然把管理员口令设上了", c.header, c.value)
			}
		})
	}
}
