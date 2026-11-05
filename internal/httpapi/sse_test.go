package httpapi

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/spencerswagger/jinliang/internal/control"
	"github.com/spencerswagger/jinliang/internal/state"
)

// sseFrame 是流里的一帧（id/event/data）。
type sseFrame struct {
	ID    string
	Event string
	Data  string
}

// readSSEFrames 读到 done（或超时/断言条件满足）为止，返回按顺序的帧。
func readSSEFrames(t *testing.T, url, token, lastEventID string, timeout time.Duration, stopAtDone bool) []sseFrame {
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
	if lastEventID != "" {
		req.Header.Set("Last-Event-ID", lastEventID)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("open sse: %v", err)
	}
	defer res.Body.Close()

	var frames []sseFrame
	var cur sseFrame
	sc := bufio.NewScanner(res.Body)
	for sc.Scan() {
		line := sc.Text()
		switch {
		case strings.HasPrefix(line, "id: "):
			cur.ID = strings.TrimPrefix(line, "id: ")
		case strings.HasPrefix(line, "event: "):
			cur.Event = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			cur.Data = strings.TrimPrefix(line, "data: ")
		case line == "":
			if cur.Data != "" {
				frames = append(frames, cur)
				if stopAtDone && cur.Event == "done" {
					return frames
				}
			}
			cur = sseFrame{}
		}
	}
	return frames
}

// P1-c 回归：SSE 每帧必须有 event: <type>，否则浏览器只能靠猜 payload 里的 type，
// 也没法用 addEventListener('phase')。
func TestSSEFramesCarryEventNames(t *testing.T) {
	ts, store := newTestServer(t)
	token := setupAdmin(t, ts)
	connID := createPGConn(t, ts, token, "src")
	jobID := createOnceJob(t, ts, token, connID)

	publishRunLikeEvents(t, store, jobID, 3)
	frames := readSSEFrames(t, ts.URL+"/api/jobs/"+jobID+"/events", token, "", 15*time.Second, true)
	if len(frames) == 0 {
		t.Fatal("SSE 没收到任何帧")
	}
	seen := map[string]bool{}
	for _, f := range frames {
		if f.Event == "" {
			t.Fatalf("帧缺少 event: 行：%+v", f)
		}
		seen[f.Event] = true
		if !strings.Contains(f.ID, "-") {
			t.Fatalf("id 应该是 <epoch>-<seq>，实际 %q", f.ID)
		}
		epoch, seq, ok := parseIDForTest(f.ID)
		if !ok || epoch == "" || seq <= 0 {
			t.Fatalf("id 解析失败：%q", f.ID)
		}
	}
	for _, want := range []string{"phase", "done"} {
		if !seen[want] {
			t.Fatalf("缺少 event: %s（收到 %v）", want, seen)
		}
	}
}

// P1-b 回归：进程重启后 seq 从 1 重来。客户端带着上一辈子的 Last-Event-ID 回来时，
// 服务端必须发 reset，而不是假装能补齐（那样中间的事件会永久丢失）。
func TestSSEResetsOnForeignEpoch(t *testing.T) {
	ts, store := newTestServer(t)
	token := setupAdmin(t, ts)
	connID := createPGConn(t, ts, token, "src")
	jobID := createOnceJob(t, ts, token, connID)

	if code, _ := doJSON(t, http.MethodPost, ts.URL+"/api/jobs/"+jobID+"/run", token, nil); code != http.StatusAccepted {
		t.Fatalf("run = %d", code)
	}
	// 等这一轮跑完，环形缓冲里有事件。
	readSSEFrames(t, ts.URL+"/api/jobs/"+jobID+"/events", token, "", 15*time.Second, true)

	// 换一个 epoch（等价于「重启过」），带着旧的 seq 回来。
	frames := readSSEFrames(t, ts.URL+"/api/jobs/"+jobID+"/events", token,
		"a-previous-process-"+strconv.FormatInt(3, 10), 10*time.Second, true)
	if len(frames) == 0 {
		t.Fatal("重置后至少应该重发当前环形缓冲")
	}
	if frames[0].Event != "reset" {
		t.Fatalf("第一帧应该是 reset，实际 %q", frames[0].Event)
	}
	var payload map[string]string
	if err := json.Unmarshal([]byte(frames[0].Data), &payload); err != nil {
		t.Fatalf("reset 的 data 不是 JSON: %v", err)
	}
	if payload["epoch"] == "" || payload["reason"] == "" {
		t.Fatalf("reset 必须带上新 epoch 和人话原因: %v", payload)
	}
	if payload["epoch"] != store.Bus().Epoch() {
		t.Fatalf("reset 里的 epoch 应该是当前 epoch")
	}
}

// P1-a 回归：先订阅再回放，两步之间的事件不会丢；重复的部分按 seq 去重。
// 这里用一个当前 epoch 的 Last-Event-ID 只取后一半，断言不重复、不倒退。
func TestSSEReplayDedupesAndKeepsOrder(t *testing.T) {
	ts, store := newTestServer(t)
	token := setupAdmin(t, ts)
	connID := createPGConn(t, ts, token, "src")
	jobID := createOnceJob(t, ts, token, connID)
	publishRunLikeEvents(t, store, jobID, 5)
	all := readSSEFrames(t, ts.URL+"/api/jobs/"+jobID+"/events", token, "", 15*time.Second, true)
	if len(all) < 4 {
		t.Fatalf("事件太少，没法验证回放：%d", len(all))
	}
	cut := all[1].ID // 从第 2 帧之后开始要
	_, cutSeq, _ := parseIDForTest(cut)

	frames := readSSEFrames(t, ts.URL+"/api/jobs/"+jobID+"/events", token, cut, 10*time.Second, true)
	if len(frames) == 0 {
		t.Fatal("回放应该给出后面的事件")
	}
	if frames[0].ID != all[2].ID {
		t.Fatalf("回放起点不对：拿到 %q，期望 %q", frames[0].ID, all[2].ID)
	}
	last := cutSeq
	for _, f := range frames {
		_, seq, ok := parseIDForTest(f.ID)
		if !ok {
			t.Fatalf("id 解析失败: %q", f.ID)
		}
		if seq <= last {
			t.Fatalf("回放出现重复或倒退：%d <= %d", seq, last)
		}
		last = seq
	}
	if store.Bus().Epoch() == "" {
		t.Fatal("bus epoch 不能为空")
	}
}

// createOnceJob 建一个「一次性」任务（run 桩会走完全部阶段）。

// publishRunLikeEvents 往 bus 里发一串「像一次真 run」的事件。
//
// 为什么要这样：B0 的桩 runner 已从生产路径移除，UI 与 CLI 现在都走 engine.Run，
// 而 engine 需要真的数据库。但这些用例测的是 **SSE 的帧格式/回放/去重**，
// 与谁产生事件无关，所以直接往 bus 里发。
func publishRunLikeEvents(t *testing.T, store *control.Store, jobID string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		ev := control.Event{
			JobID: jobID, RunID: "run-test", Type: "phase",
			Phase: state.Snapshot, Message: "第 " + strconv.Itoa(i) + " 块",
		}
		if i == n-1 {
			ev.Type, ev.Phase = "done", state.Done
			ev.Message = "结束"
		}
		store.Bus().Publish(ev)
	}
}

func createOnceJob(t *testing.T, ts *httptest.Server, token, connID string) string {
	t.Helper()
	body := map[string]any{
		"name": "sse-job", "source_id": connID, "target_id": connID,
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
	id, _ := created["id"].(string)
	if id == "" {
		t.Fatalf("no job id: %v", created)
	}
	return id
}

// A1 回归：升级前的老页面只会带裸数字 seq（没有 epoch）。它属于「上一辈子的
// 计数器」，拿去跟当前 seq 比就是静默丢事件；必须当未知 epoch → 发 reset。
func TestSSEBareNumericLastEventIDTriggersReset(t *testing.T) {
	ts, _ := newTestServer(t)
	token := setupAdmin(t, ts)
	connID := createPGConn(t, ts, token, "src")
	jobID := createOnceJob(t, ts, token, connID)
	doJSON(t, http.MethodPost, ts.URL+"/api/jobs/"+jobID+"/run", token, nil)

	// 先等出几个事件，确保环形缓冲里确实有东西（否则「补回 0 条」没有意义）。
	readSSEFrames(t, ts.URL+"/api/jobs/"+jobID+"/events", token, "", 15*time.Second, true)

	frames := readSSEFrames(t, ts.URL+"/api/jobs/"+jobID+"/events", token, "3", 10*time.Second, true)
	if len(frames) == 0 {
		t.Fatal("裸数字 id 重连后什么都没收到")
	}
	if frames[0].Event != "reset" {
		t.Fatalf("裸数字 Last-Event-ID 必须先发 reset，实际第一帧 event=%q", frames[0].Event)
	}
	if !strings.Contains(frames[0].Data, "epoch") {
		t.Fatalf("reset 要说清楚为什么重置: %s", frames[0].Data)
	}
}

// C6 回归：SSE 的 type 是闭集（app.js 逐类型 addEventListener）。
// 未知 type 若带 event: 行，浏览器会静默丢掉；服务端要退回无名事件。
func TestSSEUnknownTypeFallsBackToUnnamedEvent(t *testing.T) {
	ts, store := newTestServer(t)
	token := setupAdmin(t, ts)
	connID := createPGConn(t, ts, token, "src")
	jobID := createOnceJob(t, ts, token, connID)

	store.Bus().Publish(control.Event{JobID: jobID, Type: "brand-new-type", Message: "未来才有的类型"})
	store.Bus().Publish(control.Event{JobID: jobID, Type: "done", Message: "结束"})

	frames := readSSEFrames(t, ts.URL+"/api/jobs/"+jobID+"/events", token, "", 5*time.Second, true)
	var found bool
	for _, f := range frames {
		if strings.Contains(f.Data, "brand-new-type") {
			found = true
			if f.Event != "" {
				t.Fatalf("未知 type 不应带 event: 行（浏览器会丢），实际 %q", f.Event)
			}
		}
	}
	if !found {
		t.Fatal("未知 type 的事件没有出现在流里")
	}
}

func parseIDForTest(id string) (epoch string, seq int64, ok bool) {
	cut := strings.LastIndex(id, "-")
	if cut < 0 {
		return "", 0, false
	}
	n, err := strconv.ParseInt(id[cut+1:], 10, 64)
	if err != nil {
		return "", 0, false
	}
	return id[:cut], n, true
}
