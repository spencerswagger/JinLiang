package httpapi

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/cookiejar"
	"strings"
	"testing"

	"github.com/spencerswagger/jinliang/internal/control"
)

const importDocGood = `version: 1
job: imported-good
source:
  kind: postgres
  dsn: env:IMP_SRC
  scope: {schema: public, include: [orders]}
target:
  kind: mysql
  dsn: env:IMP_TGT
  schema: report
mode: {frequency: once}
`

// 坏文档：yaml 合法但校验不过（job 名字为空），属于「这一份跳过」。
const importDocBad = `version: 1
job: ""
source:
  kind: postgres
  dsn: env:IMP_SRC
  scope: {schema: public, include: [orders]}
target:
  kind: mysql
  dsn: env:IMP_TGT
  schema: report
mode: {frequency: once}
`

func importEnv(t *testing.T) {
	t.Helper()
	t.Setenv("IMP_SRC", "postgres://u:p@127.0.0.1:1/src?sslmode=disable")
	t.Setenv("IMP_TGT", "mysql://u:p@127.0.0.1:1/tgt")
}

// C-1（部分成功）：至少一份写进去了 → 207，body 要能看出 N/M。
func TestImportPartialReturns207(t *testing.T) {
	ts, _ := newTestServer(t)
	token := setupAdmin(t, ts)
	importEnv(t)

	stream := "---\n" + importDocGood + "---\n" + importDocBad
	code, body := doJSON(t, http.MethodPost, ts.URL+"/api/jobs/import", token, map[string]any{"yaml": stream})
	if code != http.StatusMultiStatus {
		t.Fatalf("部分成功应返回 207，实际 %d %v", code, body)
	}
	if got := body["imported"].([]any); len(got) != 1 {
		t.Fatalf("imported 数量 = %d，期望 1", len(got))
	}
	skipped := body["skipped"].([]any)
	if len(skipped) != 1 {
		t.Fatalf("skipped 数量 = %d，期望 1", len(skipped))
	}
	first := skipped[0].(map[string]any)
	if first["reason"] == "" {
		t.Fatalf("跳过原因不能为空: %v", first)
	}
	if first["index"].(float64) != 2 {
		t.Fatalf("跳过项序号应为 2，实际 %v", first["index"])
	}
}

// C-1（全失败）：一份都没进 → 422，而不是 207/201。
// 207 属于 2xx，`curl -f` 会当成功，和 §4B「不能看起来像成功」直接冲突。
func TestImportAllSkippedReturns422(t *testing.T) {
	ts, _ := newTestServer(t)
	token := setupAdmin(t, ts)
	importEnv(t)

	code, body := doJSON(t, http.MethodPost, ts.URL+"/api/jobs/import", token,
		map[string]any{"yaml": "---\n" + importDocBad + "---\n" + importDocBad})
	if code != http.StatusUnprocessableEntity {
		t.Fatalf("全失败应返回 422，实际 %d %v", code, body)
	}
	if imported := body["imported"]; imported != nil {
		if arr, ok := imported.([]any); !ok || len(arr) != 0 {
			t.Fatalf("全失败时 imported 必须为空: %v", imported)
		}
	}
	if len(body["skipped"].([]any)) != 2 {
		t.Fatalf("skipped 数量 = %v，期望 2", body["skipped"])
	}
}

// A3 回归：历史页必须按 finish_reason 显示「已停止」，不能把手动停止渲染成「已完成」。
func TestHistoryShowsStopReasonInsteadOfDone(t *testing.T) {
	ts, store := newTestServer(t)
	setupAdmin(t, ts)

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
	run, err := store.StartRun(ctx, job.ID, "preflight", control.RunKindSync)
	if err != nil {
		t.Fatal(err)
	}
	// 在 preflight 就手动停止：阶段表里只能去 prepare/idle/failed，但用户看到的是「已停止」。
	if _, err := store.FinishRun(ctx, run.ID, "done", "", 0, control.FinishStopped); err != nil {
		t.Fatalf("停止收尾: %v", err)
	}

	jar, _ := cookiejar.New(nil)
	client := &http.Client{Jar: jar}
	if res, err := client.Get(ts.URL + "/api/session"); err != nil {
		t.Fatal(err)
	} else {
		res.Body.Close()
	}
	if res, err := client.Post(ts.URL+"/api/login", "application/json",
		strings.NewReader(`{"password":"`+testPassword+`"}`)); err != nil {
		t.Fatal(err)
	} else {
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("login = %d", res.StatusCode)
		}
	}

	res, err := client.Get(ts.URL + "/history")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	page := string(raw)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("history = %d", res.StatusCode)
	}
	if !strings.Contains(page, "已停止") {
		t.Fatalf("历史页应显示「已停止」（finish_reason=stopped），实际页面里没有：\n%s", truncate(page, 800))
	}
	if strings.Contains(page, ">已完成<") {
		t.Fatal("历史页把手动停止渲染成了「已完成」")
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

var _ = json.Marshal // keep encoding/json if future assertions need it
