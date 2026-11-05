package httpapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/spencerswagger/jinliang/internal/control"
)

// handleEvents streams job events as Server-Sent Events.
//
// 为什么要 SSE 而不是轮询：用户看的是"现在到哪了"。轮询要么慢要么费；
// SSE 断线会自动重连，并且带 Last-Event-ID 补齐断档（events.go 的环形缓冲）。
//
// 两个容易写反的顺序（Ark 的 P1）：
//  1. 必须**先 Subscribe 再回放**。反过来的话，两步之间发布的事件既不在回放里、
//     也没进订阅通道，永久丢失；先订阅最多多收到重复事件，去重由 seq 兜住。
//  2. id 是 "<boot_epoch>-<seq>"。进程重启后 seq 从 1 重来，客户端带着旧 epoch
//     的 Last-Event-ID 回来时不能假装能补齐——发一条 reset 让它从头开始。
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("id")
	if _, err := s.store.GetJob(r.Context(), jobID); err != nil {
		s.fail(w, r, err)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		s.writeError(w, r, http.StatusInternalServerError, plainError("这个连接不支持流式输出"))
		return
	}

	h := w.Header()
	h.Set("Content-Type", "text/event-stream; charset=utf-8")
	h.Set("Cache-Control", "no-cache, no-transform")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no") // 让反向代理别攒着
	w.WriteHeader(http.StatusOK)

	// 断线重连时客户端会带 Last-Event-ID（或 ?last_event_id=）。
	lastEpoch, lastSeq, haveLast := "", int64(0), false
	if v := r.Header.Get("Last-Event-ID"); v != "" {
		if epoch, seq, ok := control.ParseSSEID(v); ok {
			lastEpoch, lastSeq, haveLast = epoch, seq, true
		}
	}
	if v := r.URL.Query().Get("last_event_id"); v != "" {
		if epoch, seq, ok := control.ParseSSEID(v); ok && (!haveLast || seq > lastSeq) {
			lastEpoch, lastSeq, haveLast = epoch, seq, true
		}
	}

	// 先订阅，再回放（顺序不能反，见函数注释）。
	ch, cancel := s.store.Bus().Subscribe(jobID)
	defer cancel()

	fmt.Fprint(w, "retry: 3000\n\n")

	if haveLast {
		if events, epochOK := s.store.Bus().Replay(jobID, lastEpoch, lastSeq); epochOK {
			lastSeq = writeSSEList(w, events, lastSeq)
		} else {
			// 另一个进程生命周期的事件流：旧 seq 与新的没有可比性。
			// 明确告诉客户端「重置」，而不是让它拿着错位的序号继续。
			writeSSEReset(w, s.store.Bus().Epoch())
			events, _ := s.store.Bus().Replay(jobID, "", 0)
			lastSeq = writeSSEList(w, events, 0)
		}
	} else {
		events, _ := s.store.Bus().Replay(jobID, "", 0)
		lastSeq = writeSSEList(w, events, 0)
	}
	flusher.Flush()

	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case ev, ok := <-ch:
			if !ok {
				return
			}
			// 回放与订阅有重叠窗口：同一个 seq 只发一次。
			if ev.Seq <= lastSeq {
				continue
			}
			lastSeq = ev.Seq
			if !writeSSE(w, ev) {
				return
			}
			flusher.Flush()
		case <-heartbeat.C:
			// 注释行心跳：让中间的代理别把空闲连接掐掉
			fmt.Fprint(w, ": ping\n\n")
			flusher.Flush()
		}
	}
}

// writeSSEList emits a batch and returns the highest seq written.
func writeSSEList(w http.ResponseWriter, events []control.Event, since int64) int64 {
	for _, ev := range events {
		if ev.Seq <= since {
			continue
		}
		if !writeSSE(w, ev) {
			return since
		}
		since = ev.Seq
	}
	return since
}

// writeSSEReset tells a reconnecting client that the process restarted: the ids
// it remembers belong to a stream that no longer exists.
func writeSSEReset(w http.ResponseWriter, epoch string) {
	payload, err := json.Marshal(map[string]string{
		"epoch":  epoch,
		"reason": "控制面进程重启过，之前的事件序号已经失效；下面从这一轮的开头重新发送",
	})
	if err != nil {
		return
	}
	fmt.Fprintf(w, "event: reset\ndata: %s\n\n", payload)
}

// writeSSE emits one event. `event:` carries the type so the browser can use
// addEventListener('phase', …) instead of guessing from the payload.
func writeSSE(w http.ResponseWriter, ev control.Event) bool {
	payload, err := json.Marshal(ev)
	if err != nil {
		return true // 跳过坏事件，别把流打断
	}
	_, err = fmt.Fprintf(w, "id: %s\nevent: %s\ndata: %s\n\n", ev.SSEID(), ev.Type, payload)
	return err == nil
}
