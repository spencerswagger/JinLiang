// Package state manages job state: leases, positions, conflict log and dead
// letters. Positions commit in the same transaction as data, so replays are
// naturally idempotent.
package state
