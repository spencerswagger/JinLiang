package control

import (
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/spencerswagger/jinliang/internal/core"
	"github.com/spencerswagger/jinliang/internal/state"
)

// Event is one thing that happened to a job. The same struct feeds the SSE
// stream, the CLI's --json output and the run page history, so there is exactly
// one vocabulary for "what is going on".
type Event struct {
	// Epoch identifies this process' lifetime. Seq alone is a per-process
	// counter, so after a restart it would start from 1 again and a client
	// reconnecting with Last-Event-ID would silently skip everything in
	// between. The SSE id is "<epoch>-<seq>"; a client from another epoch gets
	// a `reset` event instead of a bogus replay (see handlers_sse.go).
	Epoch      string      `json:"epoch,omitempty"`
	Seq        int64       `json:"seq"`
	At         string      `json:"at"`
	JobID      string      `json:"job_id"`
	RunID      string      `json:"run_id,omitempty"`
	Type       string      `json:"type"` // phase | progress | note | error | done
	Phase      state.Phase `json:"phase,omitempty"`
	PhaseLabel string      `json:"phase_label,omitempty"`
	PhaseHint  string      `json:"phase_hint,omitempty"`
	Message    string      `json:"message,omitempty"`
	Code       string      `json:"code,omitempty"`
	Next       []string    `json:"next,omitempty"`
	// Stub marks events from a B0 skeleton run: nothing was actually moved.
	// The UI shows a badge so nobody mistakes this for a real sync.
	Stub bool `json:"stub,omitempty"`
}

// SSEID renders the value a browser sends back as Last-Event-ID.
func (e Event) SSEID() string { return e.Epoch + "-" + strconv.FormatInt(e.Seq, 10) }

// ParseSSEID splits "<epoch>-<seq>". A bare number is accepted as an epoch-less
// legacy id (old clients / bookmarks) and treated as "same epoch, unknown".
func ParseSSEID(v string) (epoch string, seq int64, ok bool) {
	v = strings.TrimSpace(v)
	if v == "" {
		return "", 0, false
	}
	cut := strings.LastIndex(v, "-")
	if cut < 0 {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 0 {
			return "", 0, false
		}
		return "", n, true
	}
	n, err := strconv.ParseInt(v[cut+1:], 10, 64)
	if err != nil || n < 0 {
		return "", 0, false
	}
	return v[:cut], n, true
}

// Bus is an in-process fan-out with a small replay ring per job. SSE clients
// reconnect with Last-Event-ID and get the gap filled from the ring, so a phone
// that slept through three phases is not left guessing.
type Bus struct {
	mu    sync.Mutex
	seq   int64
	epoch string
	subs  map[string]map[chan Event]struct{}
	ring  map[string][]Event
	ringN int
	now   func() time.Time
}

// NewBus returns a bus that keeps the last ringN events per job.
func NewBus(now func() time.Time) *Bus {
	if now == nil {
		now = time.Now
	}
	return &Bus{
		subs:  map[string]map[chan Event]struct{}{},
		ring:  map[string][]Event{},
		ringN: 200,
		now:   now,
		epoch: strconv.FormatInt(time.Now().UnixNano(), 36),
	}
}

// SetEpoch pins the epoch for this process lifetime (the store loads it from
// settings so the id stays stable across the whole process, not per call).
func (b *Bus) SetEpoch(epoch string) {
	if strings.TrimSpace(epoch) == "" {
		return
	}
	b.mu.Lock()
	b.epoch = epoch
	b.mu.Unlock()
}

// Epoch returns the current process epoch.
func (b *Bus) Epoch() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.epoch
}

// Publish assigns a sequence number and delivers to every subscriber of the job.
// A slow subscriber loses events instead of blocking the run: the ring is the
// source of truth for replay.
//
// 为什么发送必须在 b.mu 持有期内：cancel() 会 close(ch)，如果发送在锁外进行，
// 「取快照 → cancel 关闭 → 发送」这个交错就是 panic: send on closed channel，
// 而调用方是 go r.loop(...) 这个没有 recover 的 goroutine —— 用户关一次任务页
// 就可能把整个进程带走。发送本身是非阻塞的（default 丢弃），持锁不会拖慢发布。
func (b *Bus) Publish(ev Event) Event {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.seq++
	ev.Seq = b.seq
	ev.Epoch = b.epoch
	if ev.At == "" {
		ev.At = b.now().UTC().Format(time.RFC3339)
	}
	ev.PhaseLabel = PhaseLabel(ev.Phase)
	ev.PhaseHint = PhaseHint(ev.Phase)
	buf := append(b.ring[ev.JobID], ev)
	if len(buf) > b.ringN {
		buf = buf[len(buf)-b.ringN:]
	}
	b.ring[ev.JobID] = buf

	for ch := range b.subs[ev.JobID] {
		select {
		case ch <- ev:
		default: // 丢弃；客户端靠 Last-Event-ID 从环形缓冲补齐
		}
	}
	return ev
}

// Subscribe returns a channel of events for one job. cancel must be called.
func (b *Bus) Subscribe(jobID string) (<-chan Event, func()) {
	ch := make(chan Event, 32)
	b.mu.Lock()
	if b.subs[jobID] == nil {
		b.subs[jobID] = map[chan Event]struct{}{}
	}
	b.subs[jobID][ch] = struct{}{}
	b.mu.Unlock()

	var once sync.Once
	cancel := func() {
		once.Do(func() {
			b.mu.Lock()
			delete(b.subs[jobID], ch)
			if len(b.subs[jobID]) == 0 {
				delete(b.subs, jobID)
			}
			// 关闭也在锁内：Publish 的发送同样在锁内，两者不可能交错。
			close(ch)
			b.mu.Unlock()
		})
	}
	return ch, cancel
}

// Since replays buffered events with Seq > afterSeq.
func (b *Bus) Since(jobID string, afterSeq int64) []Event {
	events, _ := b.Replay(jobID, "", afterSeq)
	return events
}

// Replay replays buffered events with Seq > afterSeq and reports whether the
// caller's epoch matches this process. A mismatched epoch (the process
// restarted) means the ring cannot answer the gap: the caller must send a
// `reset` and let the client start over.
func (b *Bus) Replay(jobID, epoch string, afterSeq int64) (events []Event, epochOK bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if epoch != "" && epoch != b.epoch {
		return nil, false
	}
	for _, ev := range b.ring[jobID] {
		if ev.Seq > afterSeq {
			events = append(events, ev)
		}
	}
	return events, true
}

// ErrorEvent builds a four-part error event from the shared catalogue.
func ErrorEvent(jobID, runID string, code core.Code, detail string) Event {
	spec, ok := core.Lookup(code)
	ev := Event{JobID: jobID, RunID: runID, Type: "error", Code: string(code), Message: detail}
	if ok {
		if ev.Message == "" {
			ev.Message = spec.Message
		}
		ev.Next = spec.Next
	}
	return ev
}
