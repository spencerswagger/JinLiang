// Package core carries the sync job state machine: one-shot initial load,
// continuous incremental apply, pause and teardown. It knows nothing about a
// specific database; it consumes source events, calls transform, and hands
// batches to a sink.
package core
