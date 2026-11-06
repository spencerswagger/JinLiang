package engine

import (
	"context"
	"testing"

	"github.com/spencerswagger/jinliang/internal/dsn"
	"github.com/spencerswagger/jinliang/internal/mysqlsink"
	"github.com/spencerswagger/jinliang/internal/sink"
	"github.com/spencerswagger/jinliang/internal/source"
)

// mustMySQLSink 返回一个只用来做**类型断言**的 mysqlsink（不连接、不建表）。
func mustMySQLSink(t *testing.T) *mysqlsink.Sink {
	t.Helper()
	return &mysqlsink.Sink{}
}

// sinkOnly 只实现 sink.Sink（没有状态能力），用来验证「缺能力要显式拒绝」。
type sinkOnly struct{}

func (sinkOnly) Kind() string { return "fake" }
func (sinkOnly) EnsureSchema(context.Context, source.Catalog) ([]sink.Action, error) {
	return nil, nil
}
func (sinkOnly) Apply(context.Context, []source.Event, source.Position) error { return nil }
func (sinkOnly) Verify(context.Context, source.TableRef) (int64, error)       { return 0, nil }
func (sinkOnly) Cleanup(context.Context) error                                { return nil }
func (sinkOnly) Close() error                                                 { return nil }

var _ = dsn.KindMySQL
