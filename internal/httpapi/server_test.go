package httpapi

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/spencerswagger/jinliang/internal/control"
	"github.com/spencerswagger/jinliang/internal/core"
	"github.com/spencerswagger/jinliang/internal/engine"
	"github.com/spencerswagger/jinliang/internal/jobrun"
	"github.com/spencerswagger/jinliang/internal/state"
)

const testPassword = "test-admin-password"

func newTestServer(t *testing.T) (*httptest.Server, *control.Store) {
	t.Helper()
	store, err := control.Open(control.Options{DataDir: t.TempDir(), Autogen: true})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	srv, err := New(Options{Store: store, Version: "test", Listen: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(func() { ts.Close(); store.Close() })
	return ts, store
}

func doJSON(t *testing.T, method, url, token string, body any) (int, map[string]any) {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, url, rdr)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	out := map[string]any{}
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &out)
	}
	return res.StatusCode, out
}

// setupRequest 走完整首启流程（先拿 CSRF cookie，再带头发 POST），返回状态码与响应体。
func setupRequest(t *testing.T, ts *httptest.Server, password string) (int, map[string]any) {
	t.Helper()
	jar, _ := cookiejar.New(nil)
	client := &http.Client{Jar: jar}
	res, err := client.Get(ts.URL + "/api/session")
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	csrf := cookieValue(jar, mustURL(t, ts.URL), csrfCookie)
	if csrf == "" {
		t.Fatal("GET /api/session 没有种下 jl_csrf cookie")
	}
	body, _ := json.Marshal(map[string]string{"password": password})
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/setup", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(csrfHeader, csrf)
	res, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	out := map[string]any{}
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &out)
	}
	return res.StatusCode, out
}

func setupAdmin(t *testing.T, ts *httptest.Server) string {
	t.Helper()
	code, body := setupRequest(t, ts, testPassword)
	if code != http.StatusCreated {
		t.Fatalf("setup status = %d body=%v", code, body)
	}
	token, _ := body["token"].(string)
	if token == "" {
		t.Fatalf("setup did not return a token: %v", body)
	}
	return token
}

func cookieValue(jar http.CookieJar, u *url.URL, name string) string {
	for _, c := range jar.Cookies(u) {
		if c.Name == name {
			return c.Value
		}
	}
	return ""
}

func createPGConn(t *testing.T, ts *httptest.Server, token, name string) string {
	t.Helper()
	// 127.0.0.1:1 立刻 refused：测连会失败但很快，正好覆盖「保存不因测连失败而失败」
	code, body := doJSON(t, http.MethodPost, ts.URL+"/api/connections", token, map[string]any{
		"name": name, "kind": "postgres", "host": "127.0.0.1", "port": 1,
		"database": "nope", "username": "u", "password": "s3cret-pw",
		"params": map[string]string{"sslmode": "disable"},
	})
	if code != http.StatusCreated {
		t.Fatalf("create connection status = %d body=%v", code, body)
	}
	conn, _ := body["connection"].(map[string]any)
	id, _ := conn["id"].(string)
	if id == "" {
		t.Fatalf("no connection id: %v", body)
	}
	return id
}

func TestUnauthenticatedRequestsAreRejected(t *testing.T) {
	ts, _ := newTestServer(t)
	setupAdmin(t, ts)

	cases := []struct {
		method, path string
		want         int
	}{
		{http.MethodGet, "/api/jobs", 401},
		{http.MethodGet, "/api/connections", 401},
		{http.MethodPost, "/api/jobs", 401},
		{http.MethodGet, "/api/doctor", 401},
	}
	for _, c := range cases {
		code, _ := doJSON(t, c.method, ts.URL+c.path, "", nil)
		if code != c.want {
			t.Errorf("%s %s = %d, want %d", c.method, c.path, code, c.want)
		}
	}
}

func TestHealthAndSessionArePublic(t *testing.T) {
	ts, _ := newTestServer(t)
	code, body := doJSON(t, http.MethodGet, ts.URL+"/api/health", "", nil)
	if code != 200 || body["ok"] != true {
		t.Fatalf("health = %d %v", code, body)
	}
	code, body = doJSON(t, http.MethodGet, ts.URL+"/api/session", "", nil)
	if code != 200 {
		t.Fatalf("session = %d", code)
	}
	if body["admin_configured"] != false {
		t.Fatalf("fresh install must report admin_configured=false: %v", body)
	}
}

func TestSetupTwiceIsRefused(t *testing.T) {
	ts, _ := newTestServer(t)
	setupAdmin(t, ts)
	code, body := setupRequest(t, ts, "another-password")
	if code != http.StatusConflict {
		t.Fatalf("second setup = %d body=%v", code, body)
	}
}

func TestSetupRejectsShortPassword(t *testing.T) {
	ts, _ := newTestServer(t)
	code, _ := setupRequest(t, ts, "short")
	if code != http.StatusBadRequest {
		t.Fatalf("short password = %d, want 400", code)
	}
}

func TestRequestsBeforeSetupExplainThemselves(t *testing.T) {
	ts, _ := newTestServer(t)
	code, body := doJSON(t, http.MethodGet, ts.URL+"/api/connections", "", nil)
	if code != http.StatusPreconditionRequired {
		t.Fatalf("before setup = %d, want 428", code)
	}
	errObj, _ := body["error"].(map[string]any)
	if errObj == nil || !strings.Contains(errObj["message"].(string), "管理员口令") {
		t.Fatalf("428 must explain the missing password: %v", body)
	}
}

func TestLoginWrongPassword(t *testing.T) {
	ts, _ := newTestServer(t)
	setupAdmin(t, ts)
	code, body := doJSON(t, http.MethodPost, ts.URL+"/api/login", "", map[string]string{"password": "wrong-password"})
	if code != http.StatusUnauthorized {
		t.Fatalf("wrong password = %d body=%v", code, body)
	}
	code, _ = doJSON(t, http.MethodPost, ts.URL+"/api/login", "", map[string]string{"password": testPassword})
	if code != http.StatusOK {
		t.Fatalf("right password = %d", code)
	}
}

func TestCookieWriteWithoutCSRFIsForbidden(t *testing.T) {
	ts, _ := newTestServer(t)
	setupAdmin(t, ts)

	jar, _ := cookiejar.New(nil)
	client := &http.Client{Jar: jar}

	// 用表单登录拿 cookie（不带 X-CSRF-Token 头）
	res, err := client.PostForm(ts.URL+"/login", map[string][]string{"password": {testPassword}})
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/connections",
		strings.NewReader(`{"name":"x","kind":"postgres","host":"h","port":1,"database":"d","username":"u"}`))
	req.Header.Set("Content-Type", "application/json")
	res, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusForbidden {
		t.Fatalf("cookie write without CSRF = %d, want 403", res.StatusCode)
	}
}

func TestConnectionNeverLeaksPassword(t *testing.T) {
	ts, _ := newTestServer(t)
	token := setupAdmin(t, ts)
	id := createPGConn(t, ts, token, "prod")

	cases := []string{"/api/connections", "/api/connections/" + id}
	for _, path := range cases {
		raw := getRaw(t, ts.URL+path, token)
		if strings.Contains(raw, "s3cret-pw") || strings.Contains(raw, "secret_enc") {
			t.Fatalf("%s leaked the secret: %s", path, raw)
		}
		if !strings.Contains(raw, `"has_secret":true`) {
			t.Fatalf("%s should expose has_secret=true: %s", path, raw)
		}
	}
}

func TestJobLifecycleAndSSE(t *testing.T) {
	ts, _ := newTestServer(t)
	token := setupAdmin(t, ts)
	src := createPGConn(t, ts, token, "src")
	tgt := createPGConn(t, ts, token, "tgt")

	body := map[string]any{
		"name": "orders", "source_id": src, "target_id": tgt,
		"spec": map[string]any{
			"source":  map[string]any{"scope": map[string]any{"schema": "public", "include": []string{"orders"}}},
			"target":  map[string]any{"schema": "report"},
			"mode":    map[string]any{"frequency": "once"},
			"content": map[string]any{"tables": map[string]any{"orders": map[string]any{}}},
		},
	}
	code, created := doJSON(t, http.MethodPost, ts.URL+"/api/jobs", token, body)
	if code != http.StatusCreated {
		t.Fatalf("create job = %d %v", code, created)
	}
	jobID, _ := created["id"].(string)
	if jobID == "" {
		t.Fatalf("no job id: %v", created)
	}

	// 同指纹再建一次 → 409 且说明「这其实是同一个任务」
	code, dup := doJSON(t, http.MethodPost, ts.URL+"/api/jobs", token, body)
	if code != http.StatusConflict {
		t.Fatalf("duplicate job = %d %v", code, dup)
	}

	// 导出 yaml 里不能有密码
	raw := getRaw(t, ts.URL+"/api/jobs/"+jobID+"/yaml", token)
	if strings.Contains(raw, "s3cret-pw") {
		t.Fatal("exported yaml leaked a password")
	}
	if !strings.Contains(raw, "env:JINLIANG_CONN_") {
		t.Fatalf("exported yaml should use a placeholder dsn:\n%s", raw)
	}

	// run 现在是异步接受：202 + 事件流地址（不再是桩，也不再回 stub:true）
	code, runBody := doJSON(t, http.MethodPost, ts.URL+"/api/jobs/"+jobID+"/run", token, nil)
	if code != http.StatusAccepted {
		t.Fatalf("run = %d %v", code, runBody)
	}
	if runBody["accepted"] != true {
		t.Fatalf("run 应该回 accepted=true: %v", runBody)
	}
	if runBody["events"] == "" {
		t.Fatalf("run 应该告诉客户端去哪看事件: %v", runBody)
	}
	if _, ok := runBody["stub"]; ok {
		t.Fatalf("run 不该再回 stub 字段（B0 桩已下线）: %v", runBody)
	}

	// 连接是假的（127.0.0.1:1），引擎会立刻失败并发出带四段式的 error 事件——
	// 这正是要验的：失败不是一句「运行失败」，而是带 code/next 的事件。
	code, runs := doJSON(t, http.MethodGet, ts.URL+"/api/runs?job_id="+jobID, token, nil)
	if code != 200 {
		t.Fatalf("runs = %d %v", code, runs)
	}
}

// collectSSEPhases reads the event stream until a done event or the deadline.
// 凭证走 Authorization 头（query 里的 token 已被移除，见 TestSSEAuthChannels）。
func collectSSEPhases(t *testing.T, url, token string, timeout time.Duration) map[string]bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("sse: %v", err)
	}
	defer res.Body.Close()

	out := map[string]bool{}
	sc := bufio.NewScanner(res.Body)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var ev struct {
			Type  string `json:"type"`
			Phase string `json:"phase"`
		}
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &ev); err != nil {
			continue
		}
		if ev.Phase != "" {
			out[ev.Phase] = true
		}
		if ev.Type == "done" {
			return out
		}
	}
	return out
}

func TestPagesRender(t *testing.T) {
	ts, _ := newTestServer(t)
	token := setupAdmin(t, ts)
	connID := createPGConn(t, ts, token, "src")
	jobBody := map[string]any{
		"name": "ui-job", "source_id": connID, "target_id": connID,
		"spec": map[string]any{
			"source": map[string]any{"scope": map[string]any{"schema": "public"}},
			"target": map[string]any{"schema": "report"},
			"mode":   map[string]any{"frequency": "once"},
		},
	}
	code, created := doJSON(t, http.MethodPost, ts.URL+"/api/jobs", token, jobBody)
	if code != http.StatusCreated {
		t.Fatalf("create job = %d %v", code, created)
	}
	jobID, _ := created["id"].(string)

	jar, _ := cookiejar.New(nil)
	client := &http.Client{Jar: jar}
	res, err := client.PostForm(ts.URL+"/login", map[string][]string{"password": {testPassword}})
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()

	pages := []struct {
		path string
		want string
	}{
		{"/", "总览"},
		{"/connections", "连接"},
		{"/connections/new", "新建连接"},
		{"/connections/" + connID, "测连"},
		{"/jobs/new", "新建任务"},
		{"/jobs/" + jobID, "实时状态"},
		{"/history", "运行历史"},
		{"/diagnostics", "诊断"},
		{"/settings", "设置"},
		{"/wizard/catalog?connection_id=" + connID + "&side=source", ""},
	}
	for _, p := range pages {
		res, err := client.Get(ts.URL + p.path)
		if err != nil {
			t.Fatalf("GET %s: %v", p.path, err)
		}
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != 200 {
			t.Errorf("GET %s = %d: %s", p.path, res.StatusCode, clip(string(body), 300))
			continue
		}
		if p.want != "" && !strings.Contains(string(body), p.want) {
			t.Errorf("GET %s 缺少关键内容 %q", p.path, p.want)
		}
		if strings.Contains(string(body), "template not found") {
			t.Errorf("GET %s 模板缺失", p.path)
		}
	}
}

func TestUIRedirectsToSetupThenLogin(t *testing.T) {
	ts, _ := newTestServer(t)
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

	res, err := client.Get(ts.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusSeeOther || res.Header.Get("Location") != "/setup" {
		t.Fatalf("fresh / = %d %q, want 303 /setup", res.StatusCode, res.Header.Get("Location"))
	}

	setupAdmin(t, ts)
	res, err = client.Get(ts.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if loc := res.Header.Get("Location"); !strings.HasPrefix(loc, "/login") {
		t.Fatalf("after setup / should go to /login, got %q", loc)
	}
}

func TestUnknownJSONFieldIsRejected(t *testing.T) {
	ts, _ := newTestServer(t)
	token := setupAdmin(t, ts)
	code, body := doJSON(t, http.MethodPost, ts.URL+"/api/connections", token, map[string]any{
		"name": "x", "kind": "postgres", "host": "h", "port": 1,
		"database": "d", "username": "u", "tpyo": true,
	})
	if code != http.StatusBadRequest {
		t.Fatalf("unknown field = %d %v", code, body)
	}
}

func TestDoctorReportsHealthyControlPlane(t *testing.T) {
	ts, _ := newTestServer(t)
	token := setupAdmin(t, ts)
	code, body := doJSON(t, http.MethodGet, ts.URL+"/api/doctor", token, nil)
	if code != 200 {
		t.Fatalf("doctor = %d", code)
	}
	if body["integrity"] != "ok" {
		t.Fatalf("integrity = %v", body["integrity"])
	}
	if body["healthy"] != true {
		t.Fatalf("expected healthy control plane, got problems %v", body["problems"])
	}
}

func getRaw(t *testing.T, url, token string) string {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	return string(raw)
}

// clip 按 rune 截断：按字节切会把中文切出半个字，测试输出就不是合法 UTF-8。
func clip(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}

// TestUIFormsCreateConnectionAndJob 覆盖向导的表单路径：它和 JSON API 是两条
// 独立代码路径（CSRF 走 form 字段而不是请求头），很容易单独坏掉。
func TestUIFormsCreateConnectionAndJob(t *testing.T) {
	ts, _ := newTestServer(t)
	setupAdmin(t, ts)

	jar, _ := cookiejar.New(nil)
	client := &http.Client{Jar: jar, CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	res, err := client.PostForm(ts.URL+"/login", map[string][]string{"password": {testPassword}})
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()

	csrf := ""
	for _, c := range jar.Cookies(mustURL(t, ts.URL)) {
		if c.Name == csrfCookie {
			csrf = c.Value
		}
	}
	if csrf == "" {
		t.Fatal("登录后没有拿到 CSRF cookie")
	}

	// 1) 新建连接（表单）
	res, err = client.PostForm(ts.URL+"/connections", map[string][]string{
		"csrf_token": {csrf},
		"name":       {"表单连接"},
		"kind":       {"postgres"},
		"host":       {"127.0.0.1"},
		"port":       {"1"},
		"database":   {"nope"},
		"username":   {"u"},
		"password":   {"form-secret"},
		"sslmode":    {"disable"},
	})
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusSeeOther {
		t.Fatalf("表单建连接 = %d，期望 303", res.StatusCode)
	}
	loc := res.Header.Get("Location")
	if !strings.HasPrefix(loc, "/connections/") {
		t.Fatalf("建完应跳到连接详情，实际 %q", loc)
	}
	connID := strings.TrimPrefix(loc, "/connections/")

	// 2) 用向导建任务（表单：多值 tables + 频次）
	res, err = client.PostForm(ts.URL+"/jobs", map[string][]string{
		"csrf_token":    {csrf},
		"name":          {"表单任务"},
		"source_id":     {connID},
		"target_id":     {connID},
		"source_schema": {"public"},
		"target_schema": {"report"},
		"frequency":     {"once"},
		"on_exists":     {"append"},
		"batch_rows":    {"500"},
		"conns":         {"4"},
		"tables":        {"orders", "customers"},
	})
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusSeeOther {
		t.Fatalf("表单建任务 = %d，期望 303", res.StatusCode)
	}
	jobLoc := res.Header.Get("Location")
	if !strings.HasPrefix(jobLoc, "/jobs/") {
		t.Fatalf("建完应跳到任务详情，实际 %q", jobLoc)
	}

	// 3) 详情页能看到两张表
	res, err = client.Get(ts.URL + jobLoc)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(res.Body)
	res.Body.Close()
	for _, want := range []string{"orders", "customers", "表单任务"} {
		if !strings.Contains(string(body), want) {
			t.Fatalf("任务详情缺少 %q", want)
		}
	}
}

func mustURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

// P3 回归：首启端点也不能裸奔——没有 CSRF 的 setup 必须 403。
func TestSetupRequiresCSRF(t *testing.T) {
	ts, _ := newTestServer(t)
	code, body := doJSON(t, http.MethodPost, ts.URL+"/api/setup", "", map[string]string{"password": testPassword})
	if code != http.StatusForbidden {
		t.Fatalf("无 CSRF 的 setup = %d body=%v，期望 403", code, body)
	}
	errObj, _ := body["error"].(map[string]any)
	if errObj == nil || !strings.Contains(errObj["message"].(string), "CSRF") {
		t.Fatalf("403 应该说明缺 CSRF：%v", body)
	}
}

// P2 回归：SSE 不接受 query 里的 token（会进日志/历史/Referer）；
// 但 curl 的 Authorization 头与浏览器 cookie 都必须能过。
func TestSSEAuthChannels(t *testing.T) {
	ts, _ := newTestServer(t)
	token := setupAdmin(t, ts)
	conn := createPGConn(t, ts, token, "src")
	code, created := doJSON(t, http.MethodPost, ts.URL+"/api/jobs", token, map[string]any{
		"name": "sse-job", "source_id": conn, "target_id": conn,
		"spec": map[string]any{
			"source": map[string]any{"scope": map[string]any{"schema": "public"}},
			"target": map[string]any{"schema": "report"},
			"mode":   map[string]any{"frequency": "once"},
		},
	})
	if code != http.StatusCreated {
		t.Fatalf("create job = %d %v", code, created)
	}
	jobID, _ := created["id"].(string)
	eventsURL := ts.URL + "/api/jobs/" + jobID + "/events"

	// 先跑起来，否则流里没有事件可读
	if code, body := doJSON(t, http.MethodPost, ts.URL+"/api/jobs/"+jobID+"/run", token, nil); code != http.StatusAccepted {
		t.Fatalf("run = %d %v", code, body)
	}

	// 1) ?token= 必须被拒
	req, _ := http.NewRequest(http.MethodGet, eventsURL+"?token="+token, nil)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("?token= = %d，期望 401", res.StatusCode)
	}

	// 2) Authorization 头能过（curl 路径）：读一条事件即可
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, _ = http.NewRequestWithContext(ctx, http.MethodGet, eventsURL, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	res, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	sc := bufio.NewScanner(res.Body)
	got := false
	for sc.Scan() {
		if strings.HasPrefix(sc.Text(), "data: ") {
			got = true
			break
		}
	}
	res.Body.Close()
	if !got {
		t.Fatal("带 Authorization 头的 SSE 应该能读到事件")
	}

	// 3) 浏览器 cookie 通道
	jar, _ := cookiejar.New(nil)
	client := &http.Client{Jar: jar}
	lres, err := client.PostForm(ts.URL+"/login", map[string][]string{"password": {testPassword}})
	if err != nil {
		t.Fatal(err)
	}
	lres.Body.Close()
	ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()
	req, _ = http.NewRequestWithContext(ctx2, http.MethodGet, eventsURL, nil)
	cres, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer cres.Body.Close()
	if cres.StatusCode != http.StatusOK {
		t.Fatalf("cookie 通道 SSE = %d，期望 200", cres.StatusCode)
	}
}

// P0-1 回归：未注册的 /api/* 必须回 404 + JSON 信封，不能被 WebUI 的
// "GET /" 兜底吞成 200 HTML——否则「这个接口有没有实现」就无法自证。
func TestUnknownAPIPathReturnsJSON404(t *testing.T) {
	ts, _ := newTestServer(t)
	token := setupAdmin(t, ts)

	cases := []struct{ method, path string }{
		{http.MethodGet, "/api/nope"},
		{http.MethodGet, "/api/gc"},
		{http.MethodPost, "/api/gc"},
		{http.MethodGet, "/api/jobs/x/verify"},
		{http.MethodDelete, "/api/nope"},
	}
	for _, c := range cases {
		req, err := http.NewRequest(c.method, ts.URL+c.path, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+token)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()

		if res.StatusCode != http.StatusNotFound {
			t.Errorf("%s %s = %d，期望 404（body 前 80 字节：%q）", c.method, c.path, res.StatusCode, clip(string(body), 80))
		}
		if ct := res.Header.Get("Content-Type"); !strings.Contains(ct, "application/json") {
			t.Errorf("%s %s 的 Content-Type = %q，期望 application/json", c.method, c.path, ct)
		}
		var env struct {
			Error struct {
				Code    string   `json:"code"`
				Message string   `json:"message"`
				Next    []string `json:"next"`
				Risk    string   `json:"risk"`
			} `json:"error"`
		}
		if err := json.Unmarshal(body, &env); err != nil {
			t.Fatalf("%s %s 返回的不是 JSON 信封: %v（%q）", c.method, c.path, err, clip(string(body), 120))
		}
		if env.Error.Code != string(core.CodeAPINotFound) {
			t.Errorf("%s %s 的 code = %q，期望 %s", c.method, c.path, env.Error.Code, core.CodeAPINotFound)
		}
		if env.Error.Message == "" || len(env.Error.Next) == 0 {
			t.Errorf("%s %s 的信封缺 message/next（四段式不完整）", c.method, c.path)
		}
	}

	// 真接口不受兜底影响
	if code, _ := doJSON(t, http.MethodGet, ts.URL+"/api/connections", token, nil); code != http.StatusOK {
		t.Fatalf("已注册的接口被兜底吞掉了：/api/connections = %d", code)
	}
}

// P0-2 回归（硬要求）：被任务引用的连接不许改坐标——「连接就是身份」，拒绝而不是
// 静默重算 job_key。五个坐标字段逐个钉住（Owl 指出只钉 host 时，把 coordsChanged
// 收窄到只比 host 仍会全绿，等于没测）。
func TestReferencedConnectionCoordinatesAreLocked(t *testing.T) {
	ts, _ := newTestServer(t)
	token := setupAdmin(t, ts)
	src := createPGConn(t, ts, token, "src")

	body := map[string]any{
		"name": "coords-job", "source_id": src, "target_id": src,
		"spec": map[string]any{
			"source": map[string]any{"scope": map[string]any{"schema": "public"}},
			"target": map[string]any{"schema": "report"},
			"mode":   map[string]any{"frequency": "once"},
		},
	}
	code, created := doJSON(t, http.MethodPost, ts.URL+"/api/jobs", token, body)
	if code != http.StatusCreated {
		t.Fatalf("create job = %d %v", code, created)
	}
	jobID, _ := created["id"].(string)
	keyBefore, _ := created["job_key"].(string)
	if jobID == "" || keyBefore == "" {
		t.Fatalf("建任务没拿到 id/job_key: %v", created)
	}

	// createPGConn 建出来的基准值是这些；下面每次只改一个字段。
	base := func() map[string]any {
		return map[string]any{
			"name": "src", "kind": "postgres", "host": "127.0.0.1", "port": 1,
			"database": "nope", "username": "u",
			"params": map[string]string{"sslmode": "disable"},
		}
	}
	// 五个坐标字段，逐个改成不同的值
	coords := []struct {
		field string
		value any
		why   string
	}{
		{"kind", "mysql", "换引擎类型"},
		{"host", "10.0.0.9", "换主机"},
		{"port", 5432, "换端口"},
		{"database", "other_db", "换库"},
		{"username", "other_user", "换账号"},
	}
	for _, c := range coords {
		payload := base()
		payload[c.field] = c.value
		code, resp := doJSON(t, http.MethodPut, ts.URL+"/api/connections/"+src, token, payload)
		if code != http.StatusConflict {
			t.Errorf("改坐标 %s（%s）= %d %v，期望 409", c.field, c.why, code, resp)
			continue
		}
		errObj, _ := resp["error"].(map[string]any)
		if errObj == nil {
			t.Errorf("%s：409 必须带四段式信封，实际 %v", c.field, resp)
			continue
		}
		if msg, _ := errObj["message"].(string); !strings.Contains(msg, "身份") {
			t.Errorf("%s：409 文案必须说明「连接就是身份」，实际 %q", c.field, msg)
		}
		if nxt, _ := errObj["next"].([]any); len(nxt) == 0 {
			t.Errorf("%s：409 必须给下一步（新建连接 / 只改名字口令）", c.field)
		}
	}

	// 原任务的身份必须一个字都没变（拒绝，而不是静默重算）
	code, after := doJSON(t, http.MethodGet, ts.URL+"/api/jobs/"+jobID, token, nil)
	if code != http.StatusOK {
		t.Fatalf("读回任务 = %d", code)
	}
	jobObj, _ := after["job"].(map[string]any)
	if got, _ := jobObj["job_key"].(string); got != keyBefore {
		t.Errorf("被拒之后 job_key 变了：%q → %q（必须是拒绝，不能静默重算）", keyBefore, got)
	}
	if code, jobs := doJSON(t, http.MethodGet, ts.URL+"/api/jobs", token, nil); code != http.StatusOK ||
		jobs["count"].(float64) != 1 {
		t.Errorf("任务数应仍为 1，实际 %v", jobs)
	}

	// 非坐标字段：改名字、改口令都必须放行（不能误锁）
	for _, c := range []struct {
		field string
		value any
	}{
		{"name", "src-renamed"},
		{"password", "new-secret"},
	} {
		payload := base()
		payload[c.field] = c.value
		if code, resp := doJSON(t, http.MethodPut, ts.URL+"/api/connections/"+src, token, payload); code != http.StatusOK {
			t.Errorf("改非坐标字段 %s 应允许，实际 %d %v", c.field, code, resp)
		}
	}

	// 连接确实没被改脏
	code, conn := doJSON(t, http.MethodGet, ts.URL+"/api/connections/"+src, token, nil)
	if code != http.StatusOK {
		t.Fatalf("读回连接 = %d", code)
	}
	for _, f := range []struct {
		field string
		want  any
	}{
		{"kind", "postgres"}, {"host", "127.0.0.1"}, {"database", "nope"}, {"username", "u"},
	} {
		if got := conn[f.field]; got != f.want {
			t.Errorf("坐标 %s 被改动了：%v（应仍为 %v）", f.field, got, f.want)
		}
	}
}

// P0-2 回归（硬要求）：没被任何任务引用的连接，改坐标仍然允许。
func TestUnreferencedConnectionCoordinatesCanChange(t *testing.T) {
	ts, _ := newTestServer(t)
	token := setupAdmin(t, ts)
	conn := createPGConn(t, ts, token, "free")

	code, resp := doJSON(t, http.MethodPut, ts.URL+"/api/connections/"+conn, token, map[string]any{
		"name": "free", "kind": "postgres", "host": "10.0.0.9", "port": 5432,
		"database": "nope", "username": "u", "params": map[string]string{"sslmode": "disable"},
	})
	if code != http.StatusOK {
		t.Fatalf("改未被引用连接的坐标应允许，实际 %d %v", code, resp)
	}
	if host, _ := resp["host"].(string); host != "10.0.0.9" {
		t.Errorf("host 没改成功：%v", resp["host"])
	}
	if code, jobs := doJSON(t, http.MethodGet, ts.URL+"/api/jobs", token, nil); code != http.StatusOK ||
		jobs["count"].(float64) != 0 {
		t.Errorf("这个测试不该产生任务，实际 %v", jobs)
	}
}

// A-1 回归（本轮最值钱的一条）：--listen 0.0.0.0 且尚未设口令时，局域网里第一个
// 来访者能不能抢到管理员？不能——首启设口令只允许本机来源。
//
// 注意这里必须自己构造 RemoteAddr：httptest.NewServer 的连接永远是 127.0.0.1，
// 测不到「远端来访」，所以直接调 Handler。
func TestSetupOnlyFromLoopbackBeforeConfigured(t *testing.T) {
	store, err := control.Open(control.Options{DataDir: t.TempDir(), Autogen: true})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	srv, err := New(Options{Store: store, Version: "test", Listen: "0.0.0.0:8080"})
	if err != nil {
		t.Fatal(err)
	}
	h := srv.Handler()

	// 远端来访者先 GET /api/session 拿到 jl_csrf（攻击者也能做到），
	// 再把同一个值放进 X-CSRF-Token —— 这正是 CSRF 在这里不构成防线的原因。
	remote := "192.168.1.50:5555"
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/session", nil)
	req.RemoteAddr = remote
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("远端 GET /api/session = %d（这个端点本来就是公开的）", rec.Code)
	}
	csrf := ""
	for _, c := range rec.Result().Cookies() {
		if c.Name == csrfCookie {
			csrf = c.Value
		}
	}
	if csrf == "" {
		t.Fatal("GET /api/session 没种下 jl_csrf")
	}

	// ① 远端 + 未配置 → 403，且文案说清「只在本机做」
	body, _ := json.Marshal(map[string]string{"password": testPassword})
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/setup", bytes.NewReader(body))
	req.RemoteAddr = remote
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(csrfHeader, csrf)
	req.AddCookie(&http.Cookie{Name: csrfCookie, Value: csrf})
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("远端首启设口令 = %d，期望 403（body=%s）", rec.Code, clip(rec.Body.String(), 200))
	}
	var env struct {
		Error struct {
			Message string   `json:"message"`
			Next    []string `json:"next"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("403 不是 JSON 信封：%v（%s）", err, clip(rec.Body.String(), 200))
	}
	if !strings.Contains(env.Error.Message, "跑控制台的那台机器") {
		t.Errorf("403 文案必须说明「只能在跑控制台的那台机器上做」，实际 %q", env.Error.Message)
	}
	if len(env.Error.Next) == 0 {
		t.Error("403 必须给下一步（本机打开 /setup，或 --listen 127.0.0.1 起一次）")
	}

	// ①b Owl 复现的绕过：控制台只听 127.0.0.1 + 同机反代时，远端请求的
	// RemoteAddr 恒为 127.0.0.1。带任一转发头就必须按非本机 403。
	for _, hdr := range []string{"X-Forwarded-For", "X-Real-IP", "Forwarded"} {
		rec = httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodPost, "/api/setup", bytes.NewReader(body))
		req.RemoteAddr = "127.0.0.1:54321"
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(csrfHeader, csrf)
		req.AddCookie(&http.Cookie{Name: csrfCookie, Value: csrf})
		req.Header.Set(hdr, "203.0.113.9")
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusForbidden {
			t.Fatalf("本机 RemoteAddr + %s 头 = %d，期望 403（反代绕过）", hdr, rec.Code)
		}
	}

	// ② 本机 + 未配置 → 正常建立管理员
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/session", nil)
	req.RemoteAddr = "127.0.0.1:43210"
	h.ServeHTTP(rec, req)
	localCSRF := ""
	for _, c := range rec.Result().Cookies() {
		if c.Name == csrfCookie {
			localCSRF = c.Value
		}
	}
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/setup", bytes.NewReader(body))
	req.RemoteAddr = "127.0.0.1:43210"
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(csrfHeader, localCSRF)
	req.AddCookie(&http.Cookie{Name: csrfCookie, Value: localCSRF})
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("本机首启设口令 = %d，期望 201（body=%s）", rec.Code, clip(rec.Body.String(), 200))
	}

	// ③ 已配置之后行为不变：远端再打 setup 是 409（已设过），不是 403
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/setup", bytes.NewReader(body))
	req.RemoteAddr = remote
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(csrfHeader, csrf)
	req.AddCookie(&http.Cookie{Name: csrfCookie, Value: csrf})
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusConflict {
		t.Fatalf("已配置后远端 setup = %d，期望 409（守卫只在前置条件下生效）", rec.Code)
	}
}

// A-1 的表单入口同样守住：远端 POST /setup 必须被挡下并给出说明页。
func TestSetupFormOnlyFromLoopback(t *testing.T) {
	store, err := control.Open(control.Options{DataDir: t.TempDir(), Autogen: true})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	srv, err := New(Options{Store: store, Version: "test", Listen: "0.0.0.0:8080"})
	if err != nil {
		t.Fatal(err)
	}
	h := srv.Handler()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/setup", nil)
	req.RemoteAddr = "10.0.0.9:5555"
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("远端 GET /setup = %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "本机") {
		t.Error("远端 GET /setup 应该先说明「只在本机做」")
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/setup",
		strings.NewReader("password="+testPassword+"&password2="+testPassword))
	req.RemoteAddr = "10.0.0.9:5555"
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	h.ServeHTTP(rec, req)
	if rec.Code == http.StatusSeeOther {
		t.Fatal("远端 POST /setup 不该成功建立管理员")
	}
	if configured, _ := store.AdminConfigured(context.Background()); configured {
		t.Fatal("远端 POST /setup 之后管理员口令竟然被设上了")
	}
}

// 导出路径对齐（Ark 的 §4.3）：/export 为规范，/yaml 是保留一个周期的别名。
// 两条 body 必须逐字节相同、别名不带额外 warning 字段；/exports（多个 s）仍 404。
func TestJobExportCanonicalAndAlias(t *testing.T) {
	ts, _ := newTestServer(t)
	token := setupAdmin(t, ts)
	conn := createPGConn(t, ts, token, "src")
	code, created := doJSON(t, http.MethodPost, ts.URL+"/api/jobs", token, map[string]any{
		"name": "export-job", "source_id": conn, "target_id": conn,
		"spec": map[string]any{
			"source":  map[string]any{"scope": map[string]any{"schema": "public", "include": []string{"orders"}}},
			"target":  map[string]any{"schema": "report"},
			"mode":    map[string]any{"frequency": "once"},
			"content": map[string]any{"tables": map[string]any{"orders": map[string]any{}}},
		},
	})
	if code != http.StatusCreated {
		t.Fatalf("create job = %d %v", code, created)
	}
	jobID, _ := created["id"].(string)

	canonical := getRaw(t, ts.URL+"/api/jobs/"+jobID+"/export", token)
	alias := getRaw(t, ts.URL+"/api/jobs/"+jobID+"/yaml", token)
	if canonical == "" || alias == "" {
		t.Fatalf("导出为空：/export=%q /yaml=%q", clip(canonical, 40), clip(alias, 40))
	}
	if canonical != alias {
		t.Fatalf("/export 与 /yaml 的 body 不一致：\n/export:\n%s\n/yaml:\n%s", canonical, alias)
	}
	if !strings.Contains(canonical, "version: 1") {
		t.Fatalf("/export 返回的不是 job.yaml：%s", clip(canonical, 80))
	}

	// 拼错的 /exports 不能被兜到别名上
	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/api/jobs/"+jobID+"/exports", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusNotFound {
		t.Fatalf("/exports = %d，期望 404", res.StatusCode)
	}
	if ct := res.Header.Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Fatalf("/exports 的 Content-Type = %q，期望 application/json", ct)
	}
}

// A-4：错误码表要能被前端读到（SSE 的 error 事件只有 message+code+next，
// 四段式的 why/risk 只能按 code 查表）。
func TestCodesEndpointExposesCatalogue(t *testing.T) {
	ts, _ := newTestServer(t)
	token := setupAdmin(t, ts)
	code, body := doJSON(t, http.MethodGet, ts.URL+"/api/codes", token, nil)
	if code != http.StatusOK {
		t.Fatalf("/api/codes = %d", code)
	}
	codes, _ := body["codes"].(map[string]any)
	if len(codes) < 10 {
		t.Fatalf("码表太小：%d 条", len(codes))
	}
	// 抽三条已知的：字段必须齐（前端靠 why/next/risk 渲染四段式）
	for _, want := range []string{"E1001", "E1011", "E3001"} {
		raw, ok := codes[want].(map[string]any)
		if !ok {
			t.Fatalf("码表缺 %s", want)
		}
		for _, field := range []string{"code", "class", "message", "why", "next", "risk"} {
			if _, ok := raw[field]; !ok {
				t.Errorf("%s 缺字段 %s（前端四段式渲染要用）", want, field)
			}
		}
	}

	// 未登录不能读（它只是码表，但一致性比方便重要）
	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/api/codes", nil)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("未登录读 /api/codes = %d，期望 401", res.StatusCode)
	}
}

// P0 回归：一条 run 结束后必须释放占位，否则同一任务第二次 run 会永远 409。
//
// 之前 startJob 用两个 goroutine（一个 `<-done` 后 release、一个跑引擎）——
// release 排在 `<-done` 之后，而 done 只有 release 才关：死锁，任务永久占位。
// 这里注入一个立刻返回的 runFn，断言「跑完能再起一次」。
func TestStartJobReleasesAfterFinish(t *testing.T) {
	store, err := control.Open(control.Options{DataDir: t.TempDir(), Autogen: true})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	srv, err := New(Options{Store: store, Version: "test", Listen: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	starts := make(chan struct{}, 4)
	srv.runFn = func(ctx context.Context, _ *control.Store, _ control.Job, _ jobrun.Options) (engine.Result, error) {
		starts <- struct{}{}
		<-ctx.Done() // 模拟「跑到被取消/结束」
		return engine.Result{}, ctx.Err()
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	token := setupAdmin(t, ts)
	conn := createPGConn(t, ts, token, "src")
	code, created := doJSON(t, http.MethodPost, ts.URL+"/api/jobs", token, map[string]any{
		"name": "release-job", "source_id": conn, "target_id": conn,
		"spec": map[string]any{
			"source":  map[string]any{"scope": map[string]any{"schema": "public", "include": []string{"orders"}}},
			"target":  map[string]any{"schema": "report"},
			"mode":    map[string]any{"frequency": "once"},
			"content": map[string]any{"tables": map[string]any{"orders": map[string]any{}}},
		},
	})
	if code != http.StatusCreated {
		t.Fatalf("create job = %d %v", code, created)
	}
	jobID, _ := created["id"].(string)

	// 第一次：202
	if code, body := doJSON(t, http.MethodPost, ts.URL+"/api/jobs/"+jobID+"/run", token, nil); code != http.StatusAccepted {
		t.Fatalf("第一次 run = %d %v", code, body)
	}
	<-starts // 引擎已开跑

	// 正在跑时再起一次：409（同一个任务不允许两条活跃）
	if code, _ := doJSON(t, http.MethodPost, ts.URL+"/api/jobs/"+jobID+"/run", token, nil); code != http.StatusConflict {
		t.Fatalf("并发第二次 run = %d，期望 409", code)
	}

	// 停止 → 引擎返回 → 占位必须释放
	if code, _ := doJSON(t, http.MethodPost, ts.URL+"/api/jobs/"+jobID+"/stop", token, nil); code != http.StatusAccepted {
		t.Fatalf("stop = %d", code)
	}
	deadline := time.Now().Add(5 * time.Second)
	for srv.runs.active(jobID) {
		if time.Now().After(deadline) {
			t.Fatal("stop 之后占位没有释放：这个任务以后再也起不来了（死锁）")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// 释放之后必须能再起一次（控制库里那条 run 由引擎的 FinishRun 收尾；
	// 这里 runFn 是桩、不写库，所以直接看内存占位，不查 activeSyncRun）
	if code, body := doJSON(t, http.MethodPost, ts.URL+"/api/jobs/"+jobID+"/run", token, nil); code != http.StatusAccepted {
		t.Fatalf("释放后再 run = %d %v，期望 202", code, body)
	}
}

// 引擎在「建出 run 行之前」就失败（典型：另一个进程持有活跃 run）时，它自己不会发事件；
// 而 HTTP 已经回了 202 —— 客户端会以为「跑了但什么都没发生」。这里断言补发的
// error 事件一定出现在事件流上，且挂 E1005（另一个实例在跑）。
func TestStartJobEmitsErrorWhenEngineCannotStart(t *testing.T) {
	store, err := control.Open(control.Options{DataDir: t.TempDir(), Autogen: true})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	srv, err := New(Options{Store: store, Version: "test", Listen: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	srv.runFn = func(context.Context, *control.Store, control.Job, jobrun.Options) (engine.Result, error) {
		return engine.Result{}, fmt.Errorf("%w：这个任务已经有一条未结束的运行", control.ErrConflict)
	}
	h := srv.Handler()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/session", nil)
	req.RemoteAddr = "127.0.0.1:1111"
	h.ServeHTTP(rec, req)
	csrf := ""
	for _, c := range rec.Result().Cookies() {
		if c.Name == csrfCookie {
			csrf = c.Value
		}
	}
	body, _ := json.Marshal(map[string]string{"password": testPassword})
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/setup", bytes.NewReader(body))
	req.RemoteAddr = "127.0.0.1:1111"
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(csrfHeader, csrf)
	req.AddCookie(&http.Cookie{Name: csrfCookie, Value: csrf})
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("setup = %d %s", rec.Code, clip(rec.Body.String(), 200))
	}

	ctx := context.Background()
	// 直接用控制库造一个任务（这条用例只关心事件，不关心 HTTP 建任务）
	mk := func(name string) control.Connection {
		c, err := store.CreateConnection(ctx, control.ConnectionInput{
			Name: name, Kind: "postgres", Host: "127.0.0.1", Port: 1,
			Database: "nope", Username: "u", Params: map[string]string{"sslmode": "disable"},
		})
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	src, tgt := mk("src"), mk("tgt")
	job, err := store.CreateJob(ctx, control.JobInput{
		Name: "emit-error-job", SourceID: src.ID, TargetID: tgt.ID,
		Spec: control.JobDoc{
			Source: control.SourceDoc{Scope: &control.ScopeDoc{Schema: "public", Include: []string{"orders"}}},
			Target: control.TargetDoc{Schema: "report"},
			Mode:   control.ModeDoc{Frequency: "once"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ch, cancel := store.Bus().Subscribe(job.ID)
	defer cancel()

	if err := srv.startJob(ctx, job, control.ActorAPI); err != nil {
		t.Fatalf("startJob = %v（202 之后才失败，这里应该接受）", err)
	}
	select {
	case ev := <-ch:
		if ev.Type != "error" {
			t.Fatalf("应当是 error 事件，收到 %+v", ev)
		}
		if ev.Code != string(core.CodeJobRunningElsewhere) {
			t.Fatalf("并发冲突应挂 E1005，实际 %q（message=%s）", ev.Code, ev.Message)
		}
		if len(ev.Next) == 0 {
			t.Fatal("error 事件必须带 next（否则用户不知道下一步做什么）")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("引擎没能开始，但事件流上什么也没有——用户会看到「点了没反应」")
	}
}

// P3：码表允许浏览器私有缓存 5 分钟（随二进制版本变，不是用户数据）。
func TestCodesEndpointIsCacheable(t *testing.T) {
	ts, _ := newTestServer(t)
	token := setupAdmin(t, ts)
	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/api/codes", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if cc := res.Header.Get("Cache-Control"); cc != "private, max-age=300" {
		t.Fatalf("Cache-Control = %q，期望 private, max-age=300", cc)
	}
}

// 两种 409 必须分开说：本进程在跑（不要去控制库找 run 行），vs 控制库里确有
// 一条上次没结束的 run（这才是能去查 run 的情形）。
func TestRunConflictCopyDistinguishesCause(t *testing.T) {
	store, err := control.Open(control.Options{DataDir: t.TempDir(), Autogen: true})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	srv, err := New(Options{Store: store, Version: "test", Listen: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	block := make(chan struct{})
	srv.runFn = func(ctx context.Context, _ *control.Store, _ control.Job, _ jobrun.Options) (engine.Result, error) {
		<-block
		return engine.Result{}, nil
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	token := setupAdmin(t, ts)
	conn := createPGConn(t, ts, token, "src")
	code, created := doJSON(t, http.MethodPost, ts.URL+"/api/jobs", token, map[string]any{
		"name": "conflict-copy", "source_id": conn, "target_id": conn,
		"spec": map[string]any{
			"source":  map[string]any{"scope": map[string]any{"schema": "public", "include": []string{"orders"}}},
			"target":  map[string]any{"schema": "report"},
			"mode":    map[string]any{"frequency": "once"},
			"content": map[string]any{"tables": map[string]any{"orders": map[string]any{}}},
		},
	})
	if code != http.StatusCreated {
		t.Fatalf("create job = %d %v", code, created)
	}
	jobID, _ := created["id"].(string)

	if code, _ := doJSON(t, http.MethodPost, ts.URL+"/api/jobs/"+jobID+"/run", token, nil); code != http.StatusAccepted {
		t.Fatalf("第一次 run 应为 202")
	}
	// 本进程在跑：文案要说「已经开始了」，且**不许**指向控制库里的 run
	code, body := doJSON(t, http.MethodPost, ts.URL+"/api/jobs/"+jobID+"/run", token, nil)
	if code != http.StatusConflict {
		t.Fatalf("并发第二次 run = %d，期望 409", code)
	}
	errObj, _ := body["error"].(map[string]any)
	msg, _ := errObj["message"].(string)
	if !strings.Contains(msg, "已经开始在跑了") {
		t.Fatalf("本进程在跑时文案应说「已经开始在跑了」，实际 %q", msg)
	}
	if strings.Contains(msg, "控制库里") {
		t.Fatalf("本进程在跑时不该把人指向控制库的 run 行（那里可能是空的），实际 %q", msg)
	}
	close(block)
}

// 409 的两个「库里那条 run」方向都必须有用例钉住（Owl：#29 的哨兵只钉了一个方向，
// 接反了照样绿）。这里直接造一条 ended_at IS NULL 的 run（不注入 runFn、不启动任何东西），
// 再看 /run 报的是哪一种。
//
// 关键区别在**心跳**：新鲜 = 另一个实例正在写（不能建议重启 serve）；
// 过期 = 上个进程被强杀留下的尸体（这时才谈「等收尾 / 重启」）。
func TestRunConflictFromControlDBSplitsByHeartbeat(t *testing.T) {
	cases := []struct {
		name        string
		heartbeat   time.Duration // 多久以前的心跳
		wantMsg     string
		mustNotHave string
		wantNext    string
		wantOwner   bool
	}{
		{
			name:      "心跳新鲜：另一个实例正在跑",
			heartbeat: 1 * time.Second, // 1 秒前还有心跳
			wantMsg:   "另一个实例正在跑",
			// 这条最重要：不能**建议**重启（文案里可以提醒「别在本机重启」，
			// 但不能把「重启一次 serve」当解药），否则用户会把别人的同步打断
			mustNotHave: "重启一次",
			wantNext:    "POST /api/jobs/{id}/stop",
			wantOwner:   true,
		},
		{
			name:        "心跳过期：上次没结束的运行",
			heartbeat:   10 * time.Minute, // 10 分钟前的心跳（> StaleAfter 90s）
			wantMsg:     "控制库里还有一条上次没结束的运行",
			mustNotHave: "已经在跑了",
			wantNext:    "重启",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			store, err := control.Open(control.Options{DataDir: t.TempDir(), Autogen: true})
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			srv, err := New(Options{Store: store, Version: "test"})
			if err != nil {
				t.Fatal(err)
			}
			srv.runFn = func(context.Context, *control.Store, control.Job, jobrun.Options) (engine.Result, error) {
				t.Fatal("这条路径不该真的开跑：库里已经有未结束的 run")
				return engine.Result{}, nil
			}
			ts := httptest.NewServer(srv.Handler())
			defer ts.Close()
			token := setupAdmin(t, ts)
			conn := createPGConn(t, ts, token, "src")
			code, created := doJSON(t, http.MethodPost, ts.URL+"/api/jobs", token, map[string]any{
				"name": "stale-job", "source_id": conn, "target_id": conn,
				"spec": map[string]any{
					"source":  map[string]any{"scope": map[string]any{"schema": "public", "include": []string{"orders"}}},
					"target":  map[string]any{"schema": "report"},
					"mode":    map[string]any{"frequency": "once"},
					"content": map[string]any{"tables": map[string]any{"orders": map[string]any{}}},
				},
			})
			if code != http.StatusCreated {
				t.Fatalf("create job = %d %v", code, created)
			}
			jobID, _ := created["id"].(string)

			// 直接造一条未结束的 sync run，并把心跳钉在指定时刻
			run, err := store.StartRun(context.Background(), jobID, state.Preflight, control.RunKindSync)
			if err != nil {
				t.Fatal(err)
			}
			at := time.Now().Add(-c.heartbeat).UTC().Format(time.RFC3339)
			if _, err := store.DB().Exec(`UPDATE runs SET heartbeat_at = ?, owner_instance = ? WHERE id = ?`,
				at, run.OwnerInstance, run.ID); err != nil {
				t.Fatalf("钉心跳失败: %v", err)
			}

			code, body := doJSON(t, http.MethodPost, ts.URL+"/api/jobs/"+jobID+"/run", token, nil)
			if code != http.StatusConflict {
				t.Fatalf("POST /run = %d，期望 409", code)
			}
			errObj, _ := body["error"].(map[string]any)
			msg, _ := errObj["message"].(string)
			why, _ := errObj["why"].(string)
			var nextStr string
			if list, ok := errObj["next"].([]any); ok {
				for _, n := range list {
					if s, ok := n.(string); ok {
						nextStr += s + " "
					}
				}
			}
			if !strings.Contains(msg, c.wantMsg) {
				t.Fatalf("message = %q，期望含 %q", msg, c.wantMsg)
			}
			all := msg + " " + why + " " + nextStr
			if strings.Contains(all, c.mustNotHave) {
				t.Fatalf("文案里不该出现 %q（整段：%s）", c.mustNotHave, all)
			}
			if !strings.Contains(nextStr, c.wantNext) {
				t.Fatalf("next 应含 %q，实际 %q", c.wantNext, nextStr)
			}
			// 「到那台机器上 stop」必须可执行：界面上要有那台机器的标识。
			// 跨实例那条要从结构化错误里取（errors.As），不许靠正则解析错误文本。
			if c.wantOwner {
				owner, _ := store.InstanceID(context.Background())
				if owner == "" {
					t.Fatal("测试环境拿不到 instance id")
				}
				if !strings.Contains(all, owner) {
					t.Fatalf("文案里应出现实例标识 %q（用户要据此去那台机器 stop），实际：%s", owner, all)
				}
			}
		})
	}
}

// P3（必修级）：202 之后失败、且引擎还没建出 run 行时，痕迹必须**落在控制库**里。
// 事件只活在环形缓冲里，进程一重启就查不到——用户回头只能记得「它说已接受，
// 然后什么都没发生」。这里用「换一个 Server 实例」（内存登记与 bus 都是新的）
// 来模拟重启，断言 audit 里仍有这条拒绝，且**每条都有可执行的下一步**。
//
// 两种失败都覆盖：冲突（有码 E1005 + 码表里的 next）与无码（权限/schema/连不上，
// 这是最常见的一种，next 必须来自兜底，否则审计里就是一个 null）。
func TestRejectedRunLeavesDurableAuditTrace(t *testing.T) {
	cases := []struct {
		name       string
		runErr     error
		wantCode   string
		wantNextIn string
	}{
		{
			name:       "有码：另一个进程持有活跃 run",
			runErr:     fmt.Errorf("%w：这个任务已经有一条未结束的运行", control.ErrConflict),
			wantCode:   string(core.CodeJobRunningElsewhere),
			wantNextIn: "",
		},
		{
			name:       "无码：源端表读不到（最常见的失败）",
			runErr:     errors.New("没读到源端的表清单（权限不足或 schema 写错）"),
			wantCode:   "",
			wantNextIn: "摘要", // 无码时兜底第一条要指向「摘要里的原始报错」，不是 /api/runs
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			store, err := control.Open(control.Options{DataDir: t.TempDir(), Autogen: true})
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()

			srv1, err := New(Options{Store: store, Version: "test"})
			if err != nil {
				t.Fatal(err)
			}
			rerErr := c.runErr
			srv1.runFn = func(context.Context, *control.Store, control.Job, jobrun.Options) (engine.Result, error) {
				return engine.Result{}, rerErr
			}
			ts1 := httptest.NewServer(srv1.Handler())
			token := setupAdmin(t, ts1)
			conn := createPGConn(t, ts1, token, "src")
			code, created := doJSON(t, http.MethodPost, ts1.URL+"/api/jobs", token, map[string]any{
				"name": "trace-job", "source_id": conn, "target_id": conn,
				"spec": map[string]any{
					"source":  map[string]any{"scope": map[string]any{"schema": "public", "include": []string{"orders"}}},
					"target":  map[string]any{"schema": "report"},
					"mode":    map[string]any{"frequency": "once"},
					"content": map[string]any{"tables": map[string]any{"orders": map[string]any{}}},
				},
			})
			if code != http.StatusCreated {
				t.Fatalf("create job = %d %v", code, created)
			}
			jobID, _ := created["id"].(string)
			if code, _ := doJSON(t, http.MethodPost, ts1.URL+"/api/jobs/"+jobID+"/run", token, nil); code != http.StatusAccepted {
				t.Fatalf("run 应为 202")
			}
			deadline := time.Now().Add(5 * time.Second)
			for srv1.runs.active(jobID) {
				if time.Now().After(deadline) {
					t.Fatal("run 没有结束")
				}
				time.Sleep(20 * time.Millisecond)
			}
			ts1.Close()

			// 「重启」：同一控制库、全新的 Server（内存登记与事件环形缓冲都是空的）
			srv2, err := New(Options{Store: store, Version: "test"})
			if err != nil {
				t.Fatal(err)
			}
			ts2 := httptest.NewServer(srv2.Handler())
			defer ts2.Close()
			tok2 := setupAdminExisting(t, ts2)

			code, audit := doJSON(t, http.MethodGet, ts2.URL+"/api/audit?limit=50", tok2, nil)
			if code != http.StatusOK {
				t.Fatalf("audit = %d", code)
			}
			entries, _ := audit["audit"].([]any)
			var found map[string]any
			for _, raw := range entries {
				e, _ := raw.(map[string]any)
				if e["action"] == control.ActionRunRejected {
					found = e
					break
				}
			}
			if found == nil {
				t.Fatal("重启后查不到「run 被拒绝」的痕迹：用户会以为它从没被接受过")
			}
			detail, _ := found["detail_json"].(string)
			if !strings.Contains(detail, jobID) {
				t.Fatalf("痕迹里应带 job_id，实际 %s", detail)
			}
			var det struct {
				Code  string   `json:"code"`
				Next  []string `json:"next"`
				Actor string   `json:"actor"`
			}
			if err := json.Unmarshal([]byte(detail), &det); err != nil {
				t.Fatalf("detail 不是合法 JSON: %v", err)
			}
			if det.Code != c.wantCode {
				t.Fatalf("code = %q，期望 %q", det.Code, c.wantCode)
			}
			// 每一条都必须有「现在做什么」：没有码的那些不能留 null。
			if len(det.Next) == 0 {
				t.Fatalf("审计里 next 为空：只写了「发生了什么」，没写「现在做什么」（code=%q）", det.Code)
			}
			if c.wantNextIn != "" && !strings.Contains(strings.Join(det.Next, " "), c.wantNextIn) {
				t.Fatalf("兜底 next 应提到 %q，实际 %v", c.wantNextIn, det.Next)
			}
			if det.Actor != control.ActorAPI {
				t.Fatalf("审计 actor 应为真实发起者 %q，实际 %q", control.ActorAPI, det.Actor)
			}
			// 审计行本身的「谁」也要是真实发起者，否则按 actor 过滤看不到这次被拒绝
			if got, _ := found["actor"].(string); got != control.ActorAPI {
				t.Fatalf("audit 行的 actor 应为 %q，实际 %q", control.ActorAPI, got)
			}
		})
	}
}

// setupAdminExisting 给「已经有管理员口令」的库拿一个 token（不走 /api/setup）。
func setupAdminExisting(t *testing.T, ts *httptest.Server) string {
	t.Helper()
	code, body := doJSON(t, http.MethodPost, ts.URL+"/api/login", "", map[string]string{"password": testPassword})
	if code != http.StatusOK {
		t.Fatalf("login = %d %v", code, body)
	}
	token, _ := body["token"].(string)
	if token == "" {
		t.Fatalf("login 没有返回 token: %v", body)
	}
	return token
}

// 兜底文案必须在这条路径上真的可用（Owl 实测「无码」显示后提的两点）：
// ① 不能让人「去看最近一次运行」——这条 audit 多是还没开跑的场景，没有 run 行；
// ② 不能只说「发给维护者」——自托管场景里用户往往就是维护者。
func TestFallbackNextIsUsableBeforeAnyRunExists(t *testing.T) {
	joined := strings.Join(fallbackNext, " ")
	if strings.Contains(joined, "打开任务页 →「最近一次运行」") {
		t.Fatal("兜底第 1 条把人引向「最近一次运行」，而这条路径常常没有 run 行")
	}
	if !strings.Contains(joined, "摘要") {
		t.Fatal("兜底第 1 条应说明「原始报错就在摘要里」")
	}
	if !strings.Contains(joined, "DBA") || !strings.Contains(joined, "issue") {
		t.Fatal("兜底最后一条应区分「自建部署发给 DBA/运维」与「用上游就提 issue」")
	}
	if len(fallbackNext) < 2 || len(fallbackNext) > 3 {
		t.Fatalf("兜底应是 2–3 条可执行动作，实际 %d 条", len(fallbackNext))
	}
}

// 前端脚本的两条不变量（纯文本断言，避免为几行 JS 引入构建链）：
// ① 事件给过 rows_done 之后，轮询回来的旧值不许覆盖它（进度不能往回跳）；
// ② 未知 finish_reason 直接显示原值，不能静默当成「已完成」。
func TestAppJSProgressNeverGoesBackwards(t *testing.T) {
	raw, err := os.ReadFile("web/static/app.js")
	if err != nil {
		t.Fatal(err)
	}
	js := string(raw)
	if !strings.Contains(js, "rowsFromEventSeen") {
		t.Fatal("缺少「事件已更新」标记：在途轮询会把旧值写回去，进度会往回跳")
	}
	if !strings.Contains(js, "if (rowsFromEventSeen) return;") {
		t.Fatal("轮询回调里必须丢弃已被事件更新的情况")
	}
	if !strings.Contains(js, ": fr;") {
		t.Fatal("未知 finish_reason 必须原样显示（`: fr` 兜底），不能静默成「已完成」")
	}
}

// last_error 现在带错误码与下一步（E1013 那条约 250–300 字符）。历史页与详情页的表格
// 必须「正文截断 + 全文进 title」：全文直出会把行撑高，但只截不留全又会让人查不到原因。
func TestLastErrorIsClippedWithFullTitle(t *testing.T) {
	ts, store := newTestServer(t)
	token := setupAdmin(t, ts)
	conn := createPGConn(t, ts, token, "src")
	code, created := doJSON(t, http.MethodPost, ts.URL+"/api/jobs", token, map[string]any{
		"name": "clip-job", "source_id": conn, "target_id": conn,
		"spec": map[string]any{
			"source":  map[string]any{"scope": map[string]any{"schema": "public", "include": []string{"orders"}}},
			"target":  map[string]any{"schema": "report"},
			"mode":    map[string]any{"frequency": "once"},
			"content": map[string]any{"tables": map[string]any{"orders": map[string]any{}}},
		},
	})
	if code != http.StatusCreated {
		t.Fatalf("create job = %d %v", code, created)
	}
	jobID, _ := created["id"].(string)

	long := "[E1013] 缺这个 schema、表或列：relation \"x\" does not exist 下一步：" +
		strings.Repeat("先在源端确认表名再回任务页重跑预检。", 12) // 远超 120 字，且含引号
	run, err := store.StartRun(context.Background(), jobID, state.Preflight, control.RunKindSync)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.FinishRun(context.Background(), run.ID, state.Failed, long, 1, control.FinishFailed); err != nil {
		t.Fatal(err)
	}

	for _, page := range []string{"/jobs/" + jobID, "/history"} {
		res, err := authorizedGet(t, ts.URL+page, token)
		if err != nil {
			t.Fatal(err)
		}
		body := res
		// ① 表格正文必须是截断后的样子（clip 会加省略号）——判据是「单元格文本」，
		//    不是整页：全文出现在 title 属性里是允许且必要的。
		if !strings.Contains(body, "…</td>") {
			t.Fatalf("%s 的表格正文没有截断（应出现 clip 的省略号）", page)
		}
		// ② 全文必须在 title 里（悬停可看全），且 title 以错误码开头
		if !strings.Contains(body, "先在源端确认表名再回任务页重跑预检。") {
			t.Fatalf("%s 的 title 里没有全文：用户查不到原因", page)
		}
		if !strings.Contains(body, "title=\"[E1013]") {
			t.Fatalf("%s 缺少以错误码开头的 title 属性", page)
		}
	}
}

// authorizedGet 用 Bearer token 取一个页面（页面也接受 Authorization 头）。
func authorizedGet(t *testing.T, url, token string) (string, error) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()
	b, err := io.ReadAll(res.Body)
	if err != nil {
		return "", err
	}
	if res.StatusCode != http.StatusOK {
		return "", fmt.Errorf("%s = %d", url, res.StatusCode)
	}
	return string(b), nil
}

// 能力边界不再由 HTTP 入口判断（判断只该有一处：引擎构造期）。
// 入口要保证的是另一件事：引擎在**构造期**拒绝时，202 之后事件流上必须出现 error，
// 而不是「点了没反应」。这里用一个「只支持 PG→PG」的假引擎错误模拟构造期拒绝。
func TestEngineConstructionErrorSurfacesAsEvent(t *testing.T) {
	store, err := control.Open(control.Options{DataDir: t.TempDir(), Autogen: true})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	srv, err := New(Options{Store: store, Version: "test"})
	if err != nil {
		t.Fatal(err)
	}
	srv.runFn = func(context.Context, *control.Store, control.Job, jobrun.Options) (engine.Result, error) {
		return engine.Result{}, errors.New("目标端类型还不支持：mysql（本版只支持 PostgreSQL 目标端）")
	}
	ctx := context.Background()
	mk := func(name string) control.Connection {
		c, err := store.CreateConnection(ctx, control.ConnectionInput{
			Name: name, Kind: "postgres", Host: "127.0.0.1", Port: 1,
			Database: "nope", Username: "u", Params: map[string]string{"sslmode": "disable"},
		})
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	src, tgt := mk("s"), mk("t")
	job, err := store.CreateJob(ctx, control.JobInput{
		Name: "gate-gone", SourceID: src.ID, TargetID: tgt.ID,
		Spec: control.JobDoc{
			Source: control.SourceDoc{Scope: &control.ScopeDoc{Schema: "public", Include: []string{"orders"}}},
			Target: control.TargetDoc{Schema: "report"},
			Mode:   control.ModeDoc{Frequency: "once"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ch, cancel := store.Bus().Subscribe(job.ID)
	defer cancel()

	// 入口不再自判类型：startJob 接受，202 由 handler 给
	if err := srv.startJob(ctx, job, control.ActorAPI); err != nil {
		t.Fatalf("startJob 不该在入口拒绝（能力边界交给引擎）：%v", err)
	}
	select {
	case ev := <-ch:
		if ev.Type != "error" {
			t.Fatalf("应当是 error 事件，收到 %+v", ev)
		}
		if !strings.Contains(ev.Message, "只支持") {
			t.Fatalf("事件应带上引擎的原话（用户要知道为什么），实际 %q", ev.Message)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("引擎构造期拒绝，但事件流上什么都没有——用户会以为「点了没反应」")
	}
	// 同时必须留下可查的痕迹（不随重启消失）
	entries, err := store.ListAuditByAction(ctx, control.ActionRunRejected, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) == 0 {
		t.Fatal("拒绝没有落 audit：重启后用户查不到这次尝试")
	}
}
