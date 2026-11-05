// Package gc reaps objects this tool created (orphan slots, publications, state
// tables, temporary snapshots). Four gates: our namespace, not active, no job
// claims it, lag beyond threshold.
package gc
