package httpapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/spencerswagger/jinliang/internal/control"
)

// handleEvents streams job events as Server-Sent Events.
//
// 为什么要 SSE 而不是轮询：用户看的是"现在到哪了"。轮询要么慢要么费；
// SSE 断线会自动重连，并且带 Last-Event-ID 补齐断档（events.go 的环形缓冲）。
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

	// 断线重连时客户端会带 Last-Event-ID（或 ?last_event_id=），先补历史。
	lastID := int64(0)
	if v := r.Header.Get("Last-Event-ID"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			lastID = n
		}
	}
	if v := r.URL.Query().Get("last_event_id"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > lastID {
			lastID = n
		}
	}

	fmt.Fprint(w, "retry: 3000\n\n")
	for _, ev := range s.store.Bus().Since(jobID, lastID) {
		if !writeSSE(w, ev) {
			return
		}
	}
	flusher.Flush()

	ch, cancel := s.store.Bus().Subscribe(jobID)
	defer cancel()

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

// writeSSE emits one event with its sequence number as the SSE id, so a
// reconnecting client can ask for exactly what it missed.
func writeSSE(w http.ResponseWriter, ev control.Event) bool {
	payload, err := json.Marshal(ev)
	if err != nil {
		return true // 跳过坏事件，别把流打断
	}
	_, err = fmt.Fprintf(w, "id: %d\ndata: %s\n\n", ev.Seq, payload)
	return err == nil
}
