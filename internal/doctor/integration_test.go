//go:build integration

package doctor_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/spencerswagger/jinliang/internal/doctor"
	"github.com/spencerswagger/jinliang/internal/runtime"
)

func openDSN(t *testing.T, env string) *runtime.DB {
	t.Helper()
	raw := os.Getenv(env)
	if raw == "" {
		t.Fatalf("%s 未设置：集成测试需要真实数据库；缺 DSN 必须失败，不允许跳过", env)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	db, err := runtime.Open(ctx, raw)
	if err != nil {
		t.Fatalf("%s 连不上：%v", env, err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func find(r doctor.Report, id string) doctor.Check {
	for _, c := range r.Checks {
		if c.ID == id {
			return c
		}
	}
	return doctor.Check{}
}

func TestDoctorIntegrationPG(t *testing.T) {
	src := openDSN(t, "JINLIANG_TEST_PG_SRC_DSN")
	tgt := openDSN(t, "JINLIANG_TEST_PG_DSN")

	rep := doctor.Run(context.Background(), doctor.Options{
		Source: src, Target: tgt,
		SourceSchema: "public", TargetSchema: "public",
		JobKey: "doctorintegration",
		// 没有控制库：控制区应明确警告，而不是假装正常。
	})
	if c := find(rep, "SRC-01"); c.Status != doctor.StatusOK {
		t.Fatalf("source connectivity: %+v", c)
	}
	if c := find(rep, "TGT-01"); c.Status != doctor.StatusOK {
		t.Fatalf("target connectivity: %+v", c)
	}
	if c := find(rep, "CTL-00"); c.Status != doctor.StatusWarn {
		t.Fatalf("missing control DB must be a visible warning: %+v", c)
	}
}

func TestDoctorIntegrationMySQL(t *testing.T) {
	src := openDSN(t, "JINLIANG_TEST_MYSQL_SRC_DSN")
	tgt := openDSN(t, "JINLIANG_TEST_MYSQL_DSN")

	rep := doctor.Run(context.Background(), doctor.Options{
		Source: src, Target: tgt,
		SourceSchema: src.Schema(), TargetSchema: tgt.Schema(),
	})
	if c := find(rep, "SRC-01"); c.Status != doctor.StatusOK {
		t.Fatalf("source connectivity: %+v", c)
	}
	if c := find(rep, "TGT-01"); c.Status != doctor.StatusOK {
		t.Fatalf("target connectivity: %+v", c)
	}
	if c := find(rep, "SRC-02"); c.Status == doctor.StatusOK && c.Detail == "" {
		t.Fatalf("binlog check should carry the real value: %+v", c)
	}
}
