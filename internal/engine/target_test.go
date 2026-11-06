package engine

import (
	"context"
	"testing"

	"github.com/spencerswagger/jinliang/internal/control"
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

// schema 取值只能有一处实现：`targetSchema()`（runner 的方法）必须与自由函数
// `schemaOf()` 同值——否则闸门与落点会用不同的值，今晚已经因为「两处同义实现」返工四次。
func TestTargetSchemaMatchesSchemaOf(t *testing.T) {
	cases := []struct {
		name string
		doc  control.JobDoc
		tgt  control.Connection
		want string
	}{
		{"PG 未配置 schema → public", control.JobDoc{}, control.Connection{Kind: dsn.KindPostgres, Database: "jinliang"}, "public"},
		{"PG 已配置 → 用配置值", control.JobDoc{Target: control.TargetDoc{Schema: "jl_target"}},
			control.Connection{Kind: dsn.KindPostgres, Database: "jinliang"}, "jl_target"},
		{"MySQL 未配置 → 库名（库就是 schema）", control.JobDoc{},
			control.Connection{Kind: dsn.KindMySQL, Database: "jinliang_test"}, "jinliang_test"},
		{"MySQL 已配置 → 用配置值", control.JobDoc{Target: control.TargetDoc{Schema: "other"}},
			control.Connection{Kind: dsn.KindMySQL, Database: "jinliang_test"}, "other"},
	}
	for _, c := range cases {
		got := schemaOf(c.doc, c.tgt)
		if got != c.want {
			t.Fatalf("%s：schemaOf 得到 %q，期望 %q", c.name, got, c.want)
		}
		r := &runner{
			doc:   c.doc,
			tgtEP: dsn.Endpoint{Kind: c.tgt.Kind, Database: c.tgt.Database},
		}
		if rt := r.targetSchema(); rt != got {
			t.Fatalf("%s：targetSchema()=%q 与 schemaOf()=%q 不一致（同一个判断两处实现）", c.name, rt, got)
		}
	}
}
