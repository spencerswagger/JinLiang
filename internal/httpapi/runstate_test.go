package httpapi

import (
	"context"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/spencerswagger/jinliang/internal/control"
)

// pageClient logs in and returns a cookie-carrying client for HTML pages.
func pageClient(t *testing.T, ts *httptest.Server) *http.Client {
	t.Helper()
	jar, _ := cookiejar.New(nil)
	client := &http.Client{Jar: jar}
	res, err := client.Get(ts.URL + "/api/session")
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	res, err = client.Post(ts.URL+"/api/login", "application/json",
		strings.NewReader(`{"password":"`+testPassword+`"}`))
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("login = %d", res.StatusCode)
	}
	return client
}

func getPage(t *testing.T, client *http.Client, url string) string {
	t.Helper()
	res, err := client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("GET %s = %d", url, res.StatusCode)
	}
	return string(raw)
}

// seedStoppedJob 建一个「手动停止」的任务（phase=done + finish_reason=stopped），
// 这是三处页面最容易各说各话的数据形态。
func seedStoppedJob(t *testing.T, store *control.Store) control.Job {
	t.Helper()
	ctx := context.Background()
	conn, err := store.CreateConnection(ctx, control.ConnectionInput{
		Name: "源库", Kind: "postgres", Host: "127.0.0.1", Port: 1,
		Database: "src", Username: "u", Password: "pw",
	})
	if err != nil {
		t.Fatal(err)
	}
	job, err := store.CreateJob(ctx, control.JobInput{
		Name: "被停掉的任务", SourceID: conn.ID, TargetID: conn.ID,
		Spec: control.JobDoc{
			Source:  control.SourceDoc{Scope: &control.ScopeDoc{Schema: "public", Include: []string{"orders"}}},
			Target:  control.TargetDoc{Schema: "report"},
			Mode:    control.ModeDoc{Frequency: "once"},
			Content: &control.ContentDoc{Tables: map[string]control.TableDoc{"orders": {}}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	run, err := store.StartRun(ctx, job.ID, "preflight")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.FinishRun(ctx, run.ID, "done", "", 0, control.FinishStopped); err != nil {
		t.Fatal(err)
	}
	return job
}

// P2 回归：同一条 finish_reason=stopped 的运行，三处页面必须说同一句话。
// 以前 /history 显示「已停止」，总览与任务页仍按 phase 显示「已完成」。
func TestAllRunViewsShowStoppedConsistently(t *testing.T) {
	ts, store := newTestServer(t)
	setupAdmin(t, ts)
	job := seedStoppedJob(t, store)
	client := pageClient(t, ts)

	cases := map[string]string{
		"/history":        getPage(t, client, ts.URL+"/history"),
		"/（总览）":           getPage(t, client, ts.URL+"/"),
		"/jobs/{id}（任务页）": getPage(t, client, ts.URL+"/jobs/"+job.ID),
	}
	for where, page := range cases {
		if !strings.Contains(page, "已停止") {
			t.Fatalf("%s 应该显示「已停止」（finish_reason=stopped）", where)
		}
		if strings.Contains(page, ">已完成<") {
			t.Fatalf("%s 把手动停止渲染成了「已完成」", where)
		}
	}
}

// P2 回归：params 是自由键值，但口令类参数不能落控制库（明文 + 列表接口回传）。
func TestConnectionParamsRejectSecretKeys(t *testing.T) {
	ts, _ := newTestServer(t)
	token := setupAdmin(t, ts)

	cases := []map[string]any{
		{"name": "泄露1", "kind": "postgres", "host": "127.0.0.1", "port": 5432,
			"database": "d", "username": "u", "password": "pw",
			"params": map[string]string{"sslpassword": "s3cret"}},
		{"name": "泄露2", "kind": "postgres", "host": "127.0.0.1", "port": 5432,
			"database": "d", "username": "u", "password": "pw",
			"params": map[string]string{"ssl-key": "s3cret"}},
		// DSN 里的查询参数也会被解析进 params，同样要拒。
		{"name": "泄露3", "dsn": "postgres://u:p@127.0.0.1:5432/d?sslmode=disable&sslpassword=s3cret"},
	}
	for _, body := range cases {
		code, resp := doJSON(t, http.MethodPost, ts.URL+"/api/connections", token, body)
		if code != http.StatusBadRequest {
			t.Fatalf("%v: 口令类参数必须被拒（400），实际 %d %v", body["name"], code, resp)
		}
	}

	// 正常参数照旧能建（别把 sslmode 这类误伤）。
	code, resp := doJSON(t, http.MethodPost, ts.URL+"/api/connections", token, map[string]any{
		"name": "正常", "kind": "postgres", "host": "127.0.0.1", "port": 5432,
		"database": "d", "username": "u", "password": "pw",
		"params": map[string]string{"sslmode": "disable", "charset": "utf8"},
	})
	if code != http.StatusCreated {
		t.Fatalf("普通参数不该被拦：%d %v", code, resp)
	}
}
