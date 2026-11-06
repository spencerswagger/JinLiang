package preflight

import (
	"strings"
	"testing"

	"github.com/spencerswagger/jinliang/internal/core"
	"github.com/spencerswagger/jinliang/internal/source"
)

func catalog(tables ...source.Table) *source.Catalog {
	for i := range tables {
		if tables[i].Schema == "" {
			tables[i].Schema = "public"
		}
	}
	return &source.Catalog{Tables: tables}
}

func statusOf(rep source.Report, id string) (source.Check, bool) {
	for _, c := range rep.Checks {
		if c.ID == id {
			return c, true
		}
	}
	return source.Check{}, false
}

func TestHealthyConfigurationCanStart(t *testing.T) {
	rep := Run(Input{
		SourceKind: "postgres", TargetKind: "mysql",
		SourceVer: "16.2", TargetVer: "8.0.36",
		SourceCaps: source.CapSet{source.CapCDC: {Supported: true}},
		SourceCat:  catalog(source.Table{TableRef: source.TableRef{Table: "orders"}, HasStablePK: true}),
		TargetCat:  catalog(),
		Tables:     []string{"orders"},
		Mode:       "continuous+initial",
		OnExists:   "error",
		BatchRows:  500,
	})
	if !rep.CanStart {
		t.Fatalf("健康配置应该能开跑：%+v", rep.Checks)
	}
	for _, id := range []string{"NET-01", "NET-02", "CAP-01", "SCOPE-01", "SCOPE-02", "BATCH-01"} {
		c, ok := statusOf(rep, id)
		if !ok {
			t.Fatalf("缺少检查项 %s", id)
		}
		if c.Status != source.StatusOK {
			t.Errorf("%s = %s，期望 ok（%s）", id, c.Status, c.Detail)
		}
	}
}

func TestMissingCDCBlocksButOffersAlternative(t *testing.T) {
	rep := Run(Input{
		SourceKind: "postgres",
		SourceCaps: source.CapSet{source.CapCDC: {
			Supported: false,
			Compensation: source.Compensation{
				Level: source.LevelBlocked,
				Note:  "源库没开逻辑变更流（wal_level=replica）",
			},
		}},
		SourceCat: catalog(source.Table{TableRef: source.TableRef{Table: "orders"}, HasStablePK: true}),
		Tables:    []string{"orders"},
		Mode:      "continuous+initial",
		BatchRows: 500,
	})
	c, ok := statusOf(rep, "CAP-01")
	if !ok {
		t.Fatal("缺少 CAP-01")
	}
	if c.Status != source.StatusBlocked {
		t.Fatalf("CAP-01 = %s，缺变更流应该是 blocked", c.Status)
	}
	if c.Code != string(core.CodeWalLevel) {
		t.Fatalf("CAP-01 应该挂 E1001，实际 %q", c.Code)
	}
	if !strings.Contains(c.Detail, "定时增量") {
		t.Fatalf("blocked 必须同时给替代方案，实际：%s", c.Detail)
	}
	if c.SQL == "" {
		t.Fatal("blocked 应该给可复制的 SQL")
	}
	if rep.CanStart {
		t.Fatal("缺变更流时 cannot start（当前模式跑不了）")
	}
}

func TestMissingCDCIsFineForIntervalMode(t *testing.T) {
	rep := Run(Input{
		SourceCaps: source.CapSet{source.CapCDC: {Supported: false}},
		SourceCat: catalog(source.Table{
			TableRef:    source.TableRef{Table: "orders"},
			HasStablePK: true,
			Columns:     []source.Column{{Name: "updated_at", Type: "timestamp"}},
		}),
		Tables:      []string{"orders"},
		Mode:        "interval",
		Incremental: "updated_at",
		BatchRows:   500,
	})
	if !rep.CanStart {
		t.Fatalf("定时增量不需要变更流：%+v", rep.Checks)
	}
	if c, _ := statusOf(rep, "CAP-01"); c.Status != source.StatusOK {
		t.Fatalf("CAP-01 = %s，定时增量时应该说不需要变更流", c.Status)
	}
}

func TestIntervalWithoutWatermarkIsBlocked(t *testing.T) {
	rep := Run(Input{
		SourceCaps: source.CapSet{},
		SourceCat:  catalog(source.Table{TableRef: source.TableRef{Table: "orders"}, HasStablePK: true}),
		Tables:     []string{"orders"},
		Mode:       "interval",
		BatchRows:  500,
	})
	c, _ := statusOf(rep, "MODE-01")
	if c.Status != source.StatusBlocked {
		t.Fatalf("MODE-01 = %s，缺水位列必须 blocked", c.Status)
	}
	if rep.CanStart {
		t.Fatal("缺水位列时不能开跑")
	}
}

func TestExistingTargetTablesBlockOnlyWithOnExistsError(t *testing.T) {
	tgt := catalog(source.Table{TableRef: source.TableRef{Table: "orders"}})
	base := Input{
		SourceCaps: source.CapSet{source.CapCDC: {Supported: true}},
		SourceCat:  catalog(source.Table{TableRef: source.TableRef{Table: "orders"}, HasStablePK: true}),
		TargetCat:  tgt,
		Tables:     []string{"orders"},
		Mode:       "once",
		BatchRows:  500,
	}

	base.OnExists = "error"
	rep := Run(base)
	if c, _ := statusOf(rep, "TARGET-01"); c.Status != source.StatusBlocked {
		t.Fatalf("on_exists=error + 已有同名表 = %s，应 blocked", c.Status)
	}
	if rep.CanStart {
		t.Fatal("blocked 时不能开跑")
	}

	base.OnExists = "append"
	rep = Run(base)
	if _, ok := statusOf(rep, "TARGET-01"); ok {
		t.Fatal("append 时不该再报 TARGET-01（用户已经选了）")
	}
	if !rep.CanStart {
		t.Fatalf("append 应该能开跑：%+v", rep.Checks)
	}
}

func TestNoPKIsWarnNotBlock(t *testing.T) {
	rep := Run(Input{
		SourceCaps: source.CapSet{source.CapCDC: {Supported: true}},
		SourceCat:  catalog(source.Table{TableRef: source.TableRef{Table: "logs"}, HasStablePK: false}),
		Tables:     []string{"logs"},
		Mode:       "continuous+initial",
		BatchRows:  500,
	})
	c, _ := statusOf(rep, "SCOPE-02")
	if c.Status != source.StatusWarn {
		t.Fatalf("无主键 = %s，应该是 warn（提示但不阻断）", c.Status)
	}
	if !rep.CanStart {
		t.Fatal("无主键不该阻断：能跑，只是会插重复行")
	}
}

func TestUnreachableSourceBlocksWithCode(t *testing.T) {
	rep := Run(Input{SourceErr: "端口没有在监听", Tables: []string{"x"}, Mode: "once", BatchRows: 500})
	c, _ := statusOf(rep, "NET-01")
	if c.Status != source.StatusBlocked || c.Code != string(core.CodeConnUnreachable) {
		t.Fatalf("NET-01 = %+v", c)
	}
	if rep.CanStart {
		t.Fatal("源库连不上时不能开跑")
	}
}

func TestWeakTLSIsWarned(t *testing.T) {
	rep := Run(Input{
		TLSWarnings: []string{"源库：sslmode=disable"},
		SourceCaps:  source.CapSet{source.CapCDC: {Supported: true}},
		SourceCat:   catalog(source.Table{TableRef: source.TableRef{Table: "t"}, HasStablePK: true}),
		Tables:      []string{"t"},
		Mode:        "once",
		BatchRows:   500,
	})
	c, _ := statusOf(rep, "TLS-01")
	if c.Status != source.StatusWarn {
		t.Fatalf("TLS-01 = %s，降级应该 warn", c.Status)
	}
	if !rep.CanStart {
		t.Fatal("传输降级只提示，不阻断（docs/SECURITY.md §3）")
	}
}

func TestBatchBoundsAreChecked(t *testing.T) {
	rep := Run(Input{BatchRows: 0, Mode: "once"})
	if c, _ := statusOf(rep, "BATCH-01"); c.Status != source.StatusBlocked || rep.CanStart {
		t.Fatalf("batch_rows=0 必须 blocked")
	}
	rep = Run(Input{BatchRows: 500, SnapshotConn: 64, Mode: "once"})
	if c, _ := statusOf(rep, "CONN-01"); c.Status != source.StatusBlocked {
		t.Fatalf("conns=64 必须 blocked")
	}
}

// 类型映射必须在**开跑前**出现在预检里：有损但可接受的是 warn，数组是 blocked。
// （这条之前预检链里一条类型映射检查都没有，用户的第一次知情是跑完 verify 报差异。）
func TestTypeMappingChecksAppearInPreflight(t *testing.T) {
	cat := &source.Catalog{Tables: []source.Table{
		{
			TableRef: source.TableRef{Schema: "public", Table: "t"},
			Columns: []source.Column{
				{Name: "id", Type: "bigint"},
				{Name: "created_at", Type: "timestamp with time zone"},
				{Name: "tags", Type: "integer[]"},
				{Name: "amount", Type: "money"},
			},
		},
	}}
	rep := Run(Input{
		SourceKind: "postgres", TargetKind: "mysql", Mode: "once",
		SourceCat: cat, Tables: []string{"t"},
	})
	var blocked, warned bool
	for _, c := range rep.Checks {
		switch c.ID {
		case "TYPE-01":
			blocked = true
			if c.Status != source.StatusBlocked || !strings.Contains(c.Detail, "tags") {
				t.Fatalf("数组应 blocked 且点名到列：%+v", c)
			}
			if strings.Contains(c.Detail, "created_at") {
				t.Fatalf("数组项不该混进有损列：%+v", c)
			}
		case "TYPE-02":
			warned = true
			if c.Status != source.StatusWarn {
				t.Fatalf("有损转换应是 warn：%+v", c)
			}
			if !strings.Contains(c.Detail, "created_at") {
				t.Fatalf("timestamptz 应出现在 warn 里（用户开跑前就该知道）：%+v", c)
			}
		}
	}
	if !blocked || !warned {
		t.Fatalf("TYPE-01/TYPE-02 都应出现，得到 blocked=%v warned=%v", blocked, warned)
	}

	// 同构（PG→PG）不产出这两项
	rep = Run(Input{SourceKind: "postgres", TargetKind: "postgres", Mode: "once", SourceCat: cat, Tables: []string{"t"}})
	for _, c := range rep.Checks {
		if c.ID == "TYPE-01" || c.ID == "TYPE-02" {
			t.Fatalf("同构不该有类型映射项：%+v", c)
		}
	}
}
