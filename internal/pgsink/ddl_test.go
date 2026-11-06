package pgsink

import (
	"strings"
	"testing"
)

// 破坏性 DDL 必须先窄：白名单宁可窄，窄了能手工做，宽了会静默丢数据。
func TestDDLDestructiveDetection(t *testing.T) {
	destructive := []string{
		"DROP TABLE public.t",
		"drop table if exists t",
		"TRUNCATE TABLE public.t",
		"ALTER TABLE public.t DROP COLUMN b",
		"ALTER TABLE public.t RENAME TO t2",
		"DROP SCHEMA public CASCADE",
	}
	for _, s := range destructive {
		if err := checkDDLAllowed(s, "public", "public"); err == nil {
			t.Fatalf("%q 是破坏性语句，必须拒绝执行", s)
		} else if !strings.Contains(err.Error(), "删数据") {
			t.Fatalf("%q 的拒绝要说清后果：%v", s, err)
		}
	}
	allowed := []string{
		"ALTER TABLE public.t ADD COLUMN b text",
		"CREATE INDEX t_a_idx ON public.t (a)",
		"CREATE INDEX IF NOT EXISTS t_a_idx ON public.t (a)",
		"DROP INDEX public.t_a_idx", // 不删数据，且重放可幂等跳过
		"COMMENT ON TABLE public.t IS 'x'",
	}
	for _, s := range allowed {
		if err := checkDDLAllowed(s, "public", "public"); err != nil {
			t.Fatalf("%q 不该被拦：%v", s, err)
		}
	}
}

// 源 schema ≠ 目标 schema 时拒绝：语句里的表名是源端原文，按文本重写太脆弱。
func TestDDLRejectsCrossSchema(t *testing.T) {
	if err := checkDDLAllowed("ALTER TABLE public.t ADD COLUMN b text", "public", "jl_target"); err == nil {
		t.Fatal("跨 schema 的 DDL 必须拒绝（否则可能改到同名但不相干的表）")
	}
	// 同 schema 放行；schema 未知（空）不拦，交给数据库报错
	if err := checkDDLAllowed("ALTER TABLE public.t ADD COLUMN b text", "public", "public"); err != nil {
		t.Fatalf("同 schema 不该被拦：%v", err)
	}
	if err := checkDDLAllowed("ALTER TABLE t ADD COLUMN b text", "", ""); err != nil {
		t.Fatalf("schema 未知时不该被拦：%v", err)
	}
}
