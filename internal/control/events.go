package control

import (
	"sync"
	"time"

	"github.com/spencerswagger/jinliang/internal/core"
	"github.com/spencerswagger/jinliang/internal/state"
)

// Event is one thing that happened to a job. The same struct feeds the SSE
// stream, the CLI's --json output and the run page history, so there is exactly
// one vocabulary for "what is going on".
type Event struct {
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

// Bus is an in-process fan-out with a small replay ring per job. SSE clients
// reconnect with Last-Event-ID and get the gap filled from the ring, so a phone
// that slept through three phases is not left guessing.
type Bus struct {
	mu    sync.Mutex
	seq   int64
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
	return &Bus{subs: map[string]map[chan Event]struct{}{}, ring: map[string][]Event{}, ringN: 200, now: now}
}

// Publish assigns a sequence number and delivers to every subscriber of the job.
// A slow subscriber loses events instead of blocking the run: the ring is the
// source of truth for replay.
func (b *Bus) Publish(ev Event) Event {
	b.mu.Lock()
	b.seq++
	ev.Seq = b.seq
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
	targets := make([]chan Event, 0, len(b.subs[ev.JobID]))
	for ch := range b.subs[ev.JobID] {
		targets = append(targets, ch)
	}
	b.mu.Unlock()

	for _, ch := range targets {
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
			b.mu.Unlock()
			close(ch)
		})
	}
	return ch, cancel
}

// Since replays buffered events with Seq > afterSeq.
func (b *Bus) Since(jobID string, afterSeq int64) []Event {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []Event
	for _, ev := range b.ring[jobID] {
		if ev.Seq > afterSeq {
			out = append(out, ev)
		}
	}
	return out
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
