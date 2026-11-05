// Package pgwire opens PostgreSQL connections from the password-free endpoint
// shape used across the engine (internal/dsn.Endpoint), so the source and sink
// halves never have to know how a credential got here.
//
// 为什么单独一个包：源端与目标端都要连 PostgreSQL，但连接建立的细节（复制
// 连接要带 replication=database、超时、错误分级）只能有一份实现，否则两边的
// 重试语义会慢慢漂移。
package pgwire

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/spencerswagger/jinliang/internal/dsn"
)

// DefaultConnectTimeout is how long we wait for a TCP+TLS+auth handshake.
const DefaultConnectTimeout = 10 * time.Second

// ConnConfig turns an endpoint into a pgx config. The password lives only in
// this struct; callers must never render it (docs/SECURITY.md §1).
func ConnConfig(ep dsn.Endpoint) (*pgx.ConnConfig, error) {
	if ep.Kind != "" && ep.Kind != dsn.KindPostgres {
		return nil, fmt.Errorf("这里只能连 PostgreSQL（收到 %s）", ep.Kind)
	}
	cfg, err := pgx.ParseConfig(ep.PostgresURL())
	if err != nil {
		return nil, fmt.Errorf("连接串无法解析（地址或参数有误）: %w", err)
	}
	cfg.ConnectTimeout = DefaultConnectTimeout
	// 单条语句别被无限期挂住；批量写入由 ctx 控制。
	cfg.RuntimeParams["statement_timeout"] = "0"
	cfg.RuntimeParams["application_name"] = "jinliang"
	return cfg, nil
}

// Connect dials a normal (non-replication) connection.
func Connect(ctx context.Context, ep dsn.Endpoint) (*pgx.Conn, error) {
	cfg, err := ConnConfig(ep)
	if err != nil {
		return nil, err
	}
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("连不上 %s: %w", ep.Redacted(), err)
	}
	return conn, nil
}

// ConnectReplication dials a connection in replication mode. Only logical
// decoding (CREATE_REPLICATION_SLOT / START_REPLICATION) works on it; ordinary
// SQL does not, which is why the source keeps a second, normal connection.
func ConnectReplication(ctx context.Context, ep dsn.Endpoint) (*pgx.Conn, error) {
	cfg, err := ConnConfig(ep)
	if err != nil {
		return nil, err
	}
	cfg.RuntimeParams["replication"] = "database"
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("建立复制连接失败（账号需要 REPLICATION 权限）: %w", err)
	}
	return conn, nil
}

// Quote renders an identifier the way PostgreSQL expects, doubling embedded
// quotes. Identifiers come from the catalog, but 「用户表名里带引号」真的会发生。
func Quote(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// Qualified renders schema.table with both halves quoted.
func Qualified(schema, table string) string {
	if schema == "" {
		return Quote(table)
	}
	return Quote(schema) + "." + Quote(table)
}

// SQLState returns the SQLSTATE of a server error ("" otherwise).
func SQLState(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

// IsRetryable classifies connection-level failures. 5s/20s/60s 退避由调用方做
// （docs/SPEC.md §4：retryable 才重试，user_action 立刻停）。
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	switch SQLState(err) {
	case "40001", // serialization_failure
		"40P01",                                     // deadlock_detected
		"55P03",                                     // lock_not_available
		"57P01",                                     // admin_shutdown
		"57P02",                                     // crash_shutdown
		"57P03",                                     // cannot_connect_now
		"08000", "08003", "08006", "08001", "08004": // connection exceptions
		return true
	}
	msg := strings.ToLower(err.Error())
	for _, s := range []string{
		"connection reset", "broken pipe", "unexpected eof", "connection refused",
		"i/o timeout", "server closed the connection", "failed to receive message",
	} {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}
