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

func setupAdmin(t *testing.T, ts *httptest.Server) string {
	t.Helper()
	code, body := doJSON(t, http.MethodPost, ts.URL+"/api/setup", "", map[string]string{"password": testPassword})
	if code != http.StatusCreated {
		t.Fatalf("setup status = %d body=%v", code, body)
	}
	token, _ := body["token"].(string)
	if token == "" {
		t.Fatalf("setup did not return a token: %v", body)
	}
	return token
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
	code, body := doJSON(t, http.MethodPost, ts.URL+"/api/setup", "", map[string]string{"password": "another-password"})
	if code != http.StatusConflict {
		t.Fatalf("second setup = %d body=%v", code, body)
	}
}

func TestSetupRejectsShortPassword(t *testing.T) {
	ts, _ := newTestServer(t)
	code, _ := doJSON(t, http.MethodPost, ts.URL+"/api/setup", "", map[string]string{"password": "short"})
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

	phases := collectSSEPhases(t, ts.URL+"/api/jobs/"+jobID+"/events?token="+token, 15*time.Second)
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
func collectSSEPhases(t *testing.T, url string, timeout time.Duration) map[string]bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
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
