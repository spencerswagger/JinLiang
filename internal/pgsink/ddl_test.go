package pgsink

import (
	"strings"
	"testing"
)

// 破坏性 DDL 必须先窄：白名单宁可窄，窄了能手工做，宽了会静默丢数据。
//
// **反例里必须带注释前缀**：原来 6 条反例全是干净语句，于是「`/*x*/ DROP TABLE t`
// 判定落空、语句照旧执行」这个洞没被抓到——而 pg_dump / ORM 生成的 DDL 常带注释头。
func TestDDLDestructiveDetection(t *testing.T) {
	destructive := []string{
		"DROP TABLE public.t",
		"drop table if exists t",
		"DROP  TABLE t", // 双空格
		"/*x*/ DROP TABLE t",
		"/* multi\nline */ TRUNCATE t",
		"-- hi\nDROP TABLE t",
		"  \n  /* lead */  -- x\nALTER TABLE public.t DROP COLUMN b",
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
		"/* 带注释的加列 */ ALTER TABLE public.t ADD COLUMN c text",
		"DROP INDEX public.t_a_idx", // 不删数据（但会留痕，见 applyDDLTx）
		"COMMENT ON TABLE public.t IS 'x'",
	}
	for _, s := range allowed {
		if err := checkDDLAllowed(s, "public", "public"); err != nil {
			t.Fatalf("%q 不该被拦：%v", s, err)
		}
	}
}

// schema 判定：不相同拒绝（不做文本重写表名）；**不确定也不做**。
func TestDDLRejectsCrossSchemaAndUnknown(t *testing.T) {
	if err := checkDDLAllowed("ALTER TABLE public.t ADD COLUMN b text", "public", "jl_target"); err == nil {
		t.Fatal("跨 schema 的 DDL 必须拒绝（否则可能改到同名但不相干的表）")
	}
	if err := checkDDLAllowed("ALTER TABLE public.t ADD COLUMN b text", "public", "public"); err != nil {
		t.Fatalf("同 schema 不该被拦：%v", err)
	}
	// 不确定 = 不做（结构变更改错表不可逆）
	if err := checkDDLAllowed("ALTER TABLE t ADD COLUMN b text", "", "public"); err == nil {
		t.Fatal("源 schema 未知时必须拒绝（不知道该改哪张表）")
	}
	if err := checkDDLAllowed("ALTER TABLE t ADD COLUMN b text", "public", ""); err == nil {
		t.Fatal("目标 schema 未知时必须拒绝")
	}
}

func TestStripLeadingComments(t *testing.T) {
	cases := map[string]string{
		"DROP TABLE t":                   "DROP TABLE t",
		"/*x*/ DROP TABLE t":             "DROP TABLE t",
		"-- hi\nDROP TABLE t":            "DROP TABLE t",
		"/* a\nb */ TRUNCATE t":          "TRUNCATE t",
		"  -- one\n /* two */  SELECT 1": "SELECT 1",
		"-- only a comment":              "",
	}
	for in, want := range cases {
		if got := stripLeadingComments(in); got != want {
			t.Fatalf("%q 应剥成 %q，得到 %q", in, want, got)
		}
	}
}

// 第③步的前置：两类「这张表不能跟」的错误必须带**稳定原因码**，
// 让 paused_tables / 恢复入口 / doctor 能判等，而不是去抠一句中文。
func TestPausableErrorsCarryStableReasonCodes(t *testing.T) {
	var errs = []PausableError{
		&TypeChangedError{Table: "t", Detail: "v: bigint → text"},
		&SchemaDriftError{Table: "t", Detail: "原 a 已不存在"},
	}
	want := []string{PauseReasonTypeChanged, PauseReasonSchemaDrift}
	for i, e := range errs {
		if got := e.Reason(); got != want[i] {
			t.Fatalf("%T 的原因码应为 %q，得到 %q", e, want[i], got)
		}
		// 原因码要稳定、可判等：不能是中文长句
		if strings.ContainsAny(e.Reason(), "，。 ") {
			t.Fatalf("%T 的原因码不能是句子：%q", e, e.Reason())
		}
	}
	// 两个原因码不能相同（否则暂停原因分不开）
	if PauseReasonTypeChanged == PauseReasonSchemaDrift {
		t.Fatal("改类型与改名/删列必须是不同原因码")
	}
}
