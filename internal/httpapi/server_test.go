package httpapi

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/spencerswagger/jinliang/internal/control"
	"github.com/spencerswagger/jinliang/internal/core"
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

	// run 必须明确说自己是桩
	code, runBody := doJSON(t, http.MethodPost, ts.URL+"/api/jobs/"+jobID+"/run", token, nil)
	if code != http.StatusAccepted {
		t.Fatalf("run = %d %v", code, runBody)
	}
	if runBody["stub"] != true {
		t.Fatalf("run must report stub=true: %v", runBody)
	}

	phases := collectSSEPhases(t, ts.URL+"/api/jobs/"+jobID+"/events", token, 15*time.Second)
	want := []string{"preflight", "prepare", "snapshot", "catchup", "streaming", "draining", "done"}
	for _, w := range want {
		if !phases[w] {
			t.Errorf("SSE 里缺少阶段 %s（收到 %v）", w, phases)
		}
	}

	code, runs := doJSON(t, http.MethodGet, ts.URL+"/api/runs?job_id="+jobID, token, nil)
	if code != 200 || runs["count"].(float64) < 1 {
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

func clip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
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
// 静默重算 job_key。断言三件事：409、原任务 job_key 一个字没变、四段式说明「连接就是身份」。
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

	// 改 host（坐标变了）→ 必须 409
	code, resp := doJSON(t, http.MethodPut, ts.URL+"/api/connections/"+src, token, map[string]any{
		"name": "src", "kind": "postgres", "host": "10.0.0.9", "port": 5432,
		"database": "nope", "username": "u", "params": map[string]string{"sslmode": "disable"},
	})
	if code != http.StatusConflict {
		t.Fatalf("改被引用连接的坐标 = %d %v，期望 409", code, resp)
	}
	errObj, _ := resp["error"].(map[string]any)
	if errObj == nil {
		t.Fatalf("409 必须带四段式信封：%v", resp)
	}
	if msg, _ := errObj["message"].(string); !strings.Contains(msg, "身份") {
		t.Errorf("409 文案必须说明「连接就是身份」，实际 %q", msg)
	}
	if nxt, _ := errObj["next"].([]any); len(nxt) == 0 {
		t.Error("409 必须给下一步（新建连接 / 只改名字口令）")
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

	// 只改名字/口令不属于坐标变化 → 允许
	if code, resp := doJSON(t, http.MethodPut, ts.URL+"/api/connections/"+src, token, map[string]any{
		"name": "src-renamed", "kind": "postgres", "host": "127.0.0.1", "port": 1,
		"database": "nope", "username": "u", "password": "new-secret",
		"params": map[string]string{"sslmode": "disable"},
	}); code != http.StatusOK {
		t.Errorf("只改名字/口令应允许，实际 %d %v", code, resp)
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
