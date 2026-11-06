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
		Source: src, Target: tgt.WithSchema("jl_test_doctor"),
		SourceSchema: "public", TargetSchema: "jl_test_doctor",
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

// 真库版（在 doctor 包内跑，能直接调 targetChecks）：目标端有 _jl_conflict / _jl_dead_letter
// 且为空时，这两项必须是 skipped 而不是 ok——「引擎没有写入路径」时不能说「一切正常」。
func TestDoctorIntegrationConflictTablesAreNotGreen(t *testing.T) {
	src := openDSN(t, "JINLIANG_TEST_PG_SRC_DSN")
	tgt := openDSN(t, "JINLIANG_TEST_PG_DSN")

	// 建出两张表（引擎初始化会建，这里显式建一份，保证检查路径真的跑到）
	ctx := context.Background()
	for _, tbl := range []string{runtime.TableConflict, runtime.TableDeadLetter} {
		if _, err := tgt.Exec(ctx, "CREATE TABLE IF NOT EXISTS "+tbl+
			" (id bigserial PRIMARY KEY, job_key text NOT NULL, created_at timestamptz DEFAULT now())"); err != nil {
			t.Fatalf("建表 %s 失败：%v", tbl, err)
		}
	}

	rep := doctor.Run(ctx, doctor.Options{
		Source: src, Target: tgt, SourceSchema: "public", TargetSchema: tgt.Schema(),
		JobKey: "doctor-ghost-job", // 这个任务没有任何冲突/死信
	})
	for _, id := range []string{"TGT-05", "TGT-06"} {
		c := find(rep, id)
		if c.ID == "" {
			continue // 目标端没有这两张表时不产出该检查
		}
		if c.Status == doctor.StatusOK {
			t.Fatalf("%s 在「引擎还没有写入路径」时必须 skipped，得到 ok：%s", id, c.Title)
		}
		if c.Status != doctor.StatusSkipped {
			t.Fatalf("%s 应为 skipped，得到 %q", id, c.Status)
		}
	}
}
