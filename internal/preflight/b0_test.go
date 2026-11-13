package preflight

import (
	"strings"
	"testing"

	"github.com/spencerswagger/jinliang/internal/core"
	"github.com/spencerswagger/jinliang/internal/source"
)

// catalog 建一个「服务器已经自报了引擎事务能力」的目录桩（8.0.28 实测值）。
// 判据读的是 Catalog.Engines（SPEC MY-08），没有它的目录等于「没读到能力」——
// 那种情况由 engine_test.go 的第 6 格单独钉死，不让每个用例都被它影响。
func catalog(tables ...source.Table) *source.Catalog {
	for i := range tables {
		if tables[i].Schema == "" {
			tables[i].Schema = "public"
		}
	}
	return &source.Catalog{Tables: tables, Engines: realEngines()}
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

// PAUSE-01 的两句话必须与实现同源（契约 docs/CONTRACT-PAUSE-AND-POSITION.md §1.5/§1.6 + DECISIONS #19）。
//
// 这里曾经写反过两次：
//   - `#93` 把「按表暂停」改成「只停该表 + 其它表照常 + 位点照常前进」，codes.go 的 E1015 三句跟着改了，
//     `PAUSE-01`(OK) 却还写着「位点不推进」；
//   - `#102` 把 MySQL 目标端也接上之后，MySQL 那格还写着「引擎侧还没接 / 整条同步会停下来」——
//     同一份报告里两句话互相矛盾。
//
// 现在两个目标端（批式模式）都是新口径；**`interval` 是唯一例外**：它没有跳过通道（§1.6 例外段），
// 所以 interval 下必须 warn，且**不许**出现「只停它自己」。
func TestPause01TextMatchesPerTablePauseSemantics(t *testing.T) {
	mkMode := func(targetKind, mode string) source.Report {
		return Run(Input{
			SourceKind: "postgres", TargetKind: targetKind,
			SourceVer: "16.2", TargetVer: "8.0.36",
			SourceCaps: source.CapSet{source.CapCDC: {Supported: true}},
			SourceCat:  catalog(source.Table{TableRef: source.TableRef{Table: "orders"}, HasStablePK: true}),
			TargetCat:  catalog(),
			Tables:     []string{"orders"},
			Mode:       mode,
			OnExists:   "error",
			BatchRows:  500,
		})
	}
	mk := func(targetKind string) source.Report { return mkMode(targetKind, "continuous+initial") }

	for _, tgt := range []string{"postgres", "mysql"} {
		// 批式模式（continuous* / once / full_interval）：只停该表、位点前进、整表重搬补齐。
		rep, ok := statusOf(mk(tgt), "PAUSE-01")
		if !ok {
			t.Fatalf("%s 目标端必须有 PAUSE-01", tgt)
		}
		if rep.Status != source.StatusOK {
			t.Errorf("%s 目标端的批式模式应是 ok（按表暂停已接），得到 %v", tgt, rep.Status)
		}
		for _, want := range []string{"只停它自己", "位点照常前进", "整表重搬"} {
			if !strings.Contains(rep.Detail, want) {
				t.Errorf("%s 的 PAUSE-01 应写明 %q（只停该表 / 位点前进 / 重搬补齐），得到：%s",
					tgt, want, rep.Detail)
			}
		}
		for _, bad := range []string{"位点不推进", "还没接", "整条同步会停下来"} {
			if strings.Contains(rep.Detail, bad) {
				t.Errorf("%s 的 PAUSE-01 里出现了 %q：那是 #102 之前的假话（那时 MySQL 没绑 pauseReg）",
					tgt, bad)
			}
		}
	}

	// interval：没有跳过通道 → warn，且不许承诺「只停它自己」。
	for _, tgt := range []string{"postgres", "mysql"} {
		rep, ok := statusOf(mkMode(tgt, "interval"), "PAUSE-01")
		if !ok {
			t.Fatalf("%s 目标端 + interval 也要有 PAUSE-01", tgt)
		}
		if rep.Status != source.StatusWarn {
			t.Errorf("%s + interval 应是 warn（interval 不做按表暂停），得到 %v", tgt, rep.Status)
		}
		if strings.Contains(rep.Detail, "只停它自己") {
			t.Errorf("%s + interval 的 PAUSE-01 不许说「只停它自己」（interval 没有这条通道）：%s",
				tgt, rep.Detail)
		}
		if !strings.Contains(rep.Detail, "手工") && !strings.Contains(rep.Detail, "没接") {
			t.Errorf("%s + interval 的 PAUSE-01 必须给一条真能走的路（手工对齐 / 换成批式模式）：%s",
				tgt, rep.Detail)
		}
	}
}

// SPEC MY-08：MySQL 源端的 MyISAM 表 warn 照跑并标注，不阻断；
// InnoDB / 未选中的 MyISAM / PG 源都不触发。
func TestMyISAMSourceTablesWarnNotBlock(t *testing.T) {
	rep := Run(Input{
		SourceKind: "mysql", TargetKind: "postgres",
		SourceVer: "8.0.36", TargetVer: "16.2",
		SourceCaps: source.CapSet{source.CapCDC: {Supported: true}},
		SourceCat: catalog(
			source.Table{TableRef: source.TableRef{Table: "orders"}, Engine: "InnoDB", HasStablePK: true},
			source.Table{TableRef: source.TableRef{Table: "legacy"}, Engine: "MyISAM", HasStablePK: true},
			source.Table{TableRef: source.TableRef{Table: "mrg"}, Engine: "MRG_MyISAM", HasStablePK: true},
		),
		Tables:    []string{"orders", "legacy", "mrg"},
		Mode:      "continuous+initial",
		BatchRows: 500,
	})
	c, ok := statusOf(rep, "MY-08")
	if !ok {
		t.Fatalf("选中表里有 MyISAM，应有 MY-08：%+v", rep.Checks)
	}
	if c.Status != source.StatusWarn {
		t.Fatalf("MY-08 = %s，SPEC 口径是 warn 照跑并标注（无事务，不物理阻断）", c.Status)
	}
	if !rep.CanStart {
		t.Fatal("MyISAM 照跑，不该把 CanStart 压成 false")
	}
	for _, want := range []string{"legacy", "mrg"} {
		if !strings.Contains(c.Detail, want) {
			t.Errorf("MY-08 明细应点名 %s：%s", want, c.Detail)
		}
	}
	if strings.Contains(c.Detail, "orders") {
		t.Errorf("InnoDB 表不该被点名：%s", c.Detail)
	}

	// 没选 MyISAM 表（含 PG 源）不触发。
	rep = Run(Input{
		SourceKind: "mysql", SourceCaps: source.CapSet{source.CapCDC: {Supported: true}},
		SourceCat: catalog(
			source.Table{TableRef: source.TableRef{Table: "orders"}, Engine: "InnoDB", HasStablePK: true},
			source.Table{TableRef: source.TableRef{Table: "legacy"}, Engine: "MyISAM", HasStablePK: true},
		),
		Tables: []string{"orders"}, Mode: "once", BatchRows: 500,
	})
	if _, ok := statusOf(rep, "MY-08"); ok {
		t.Fatal("没选 MyISAM 表不该发 MY-08")
	}
	rep = Run(Input{
		SourceKind: "postgres", SourceCaps: source.CapSet{source.CapCDC: {Supported: true}},
		SourceCat: catalog(source.Table{TableRef: source.TableRef{Table: "t"}, Engine: "table", HasStablePK: true}),
		Tables:    []string{"t"}, Mode: "once", BatchRows: 500,
	})
	if _, ok := statusOf(rep, "MY-08"); ok {
		t.Fatal("PG 源不该发 MY-08（Engine 是 relkind 名，不是存储引擎）")
	}
}

// SCOPE-02 四档（docs/DESIGN-NO-PRIMARY-KEY.md §3.1）：同一个「没有主键」，
// 不同 mode 下结论完全不同，Detail 不许共用一句话（PAUSE-01 踩过两处口径互相打脸）。
func TestSCOPE02FourStatesByMode(t *testing.T) {
	mk := func(mode string) source.Report {
		return Run(Input{
			SourceKind: "postgres", TargetKind: "postgres",
			SourceCaps: source.CapSet{source.CapCDC: {Supported: true}},
			SourceCat: catalog(source.Table{
				TableRef: source.TableRef{Table: "logs"}, HasStablePK: false,
				ReplicaIdentity: "full", // 隔离 PG-06：这格只验 SCOPE-02
			}),
			Tables: []string{"logs"}, Mode: mode, BatchRows: 500,
		})
	}

	// ① once / full_interval：整轮重搬，不需要行身份 → ok，且不阻断。
	for _, mode := range []string{"once", "full_interval"} {
		rep := mk(mode)
		c, ok := statusOf(rep, "SCOPE-02")
		if !ok {
			t.Fatalf("%s：必须有 SCOPE-02", mode)
		}
		if c.Status != source.StatusOK {
			t.Fatalf("%s：无主键不影响这个模式，应 ok，得到 %s（%s）", mode, c.Status, c.Detail)
		}
		if !strings.Contains(c.Detail, "不需要行身份") || !strings.Contains(c.Detail, "部分数据") {
			t.Fatalf("%s：Detail 要写清代价（整轮重搬 / 搬完前部分数据），得到 %q", mode, c.Detail)
		}
		if !rep.CanStart {
			t.Fatalf("%s：不该阻断开跑", mode)
		}
	}

	// ② continuous / continuous+initial / 空：insert-only 能跑，改/删只停这张表 → warn。
	for _, mode := range []string{"continuous", "continuous+initial", ""} {
		rep := mk(mode)
		c, _ := statusOf(rep, "SCOPE-02")
		if c.Status != source.StatusWarn {
			t.Fatalf("%q：应 warn（照跑、代价说清），得到 %s", mode, c.Status)
		}
		if !strings.Contains(c.Detail, "只停它自己") || !strings.Contains(c.Detail, "整表重搬") {
			t.Fatalf("%q：Detail 要写「改/删只停这张表 + 整表重搬补齐」，得到 %q", mode, c.Detail)
		}
		if !rep.CanStart {
			t.Fatalf("%q：warn 不许阻断", mode)
		}
	}

	// ③ interval：每轮按水位重读，没有身份 = 必然重复 → blocked（三条替代必须都在）。
	rep := mk("interval")
	c, _ := statusOf(rep, "SCOPE-02")
	if c.Status != source.StatusBlocked {
		t.Fatalf("interval + 无主键应 blocked（每轮必然重复，不是风险），得到 %s", c.Status)
	}
	if rep.CanStart {
		t.Fatal("interval 无主键必须挡在开跑前")
	}
	for _, want := range []string{"加主键", "unique_key", "continuous+initial"} {
		if !strings.Contains(c.Detail, want) {
			t.Fatalf("blocked 必须给替代方案（缺 %q）：%s", want, c.Detail)
		}
	}
	// 码：E1017（「无身份导致做不到」族码，Ark 裁决）——挂空码会回落 E1001（wal_level 那套），
	// 把用户引到完全无关的方向。
	if c.Code != "E1017" {
		t.Fatalf("interval 无主键 blocked 应挂 E1017，得到 %q（%s）", c.Code, c.Title)
	}
}

// PG-06（**源端**前置，docs/DESIGN-NO-PRIMARY-KEY.md §3.5）：
// REPLICA IDENTITY 不是 full/index 时，源端在第一次改/删就断开整条流——
// 「只停这张表」是一句有前置的承诺，卡片必须把它说清，并给可执行的 ALTER。
func TestPG06SourceReplicaIdentityPreflight(t *testing.T) {
	mk := func(kind, mode string, tbl source.Table) source.Report {
		return Run(Input{
			SourceKind: kind, TargetKind: "postgres",
			SourceCaps: source.CapSet{source.CapCDC: {Supported: true}},
			SourceCat:  catalog(tbl),
			Tables:     []string{tbl.Table}, Mode: mode, BatchRows: 500,
		})
	}
	noPK := source.Table{TableRef: source.TableRef{Table: "events"}, HasStablePK: false}

	// ① default(no-pk)：不通过 → warn + 可复制的 ALTER（带 schema 限定与引号），不阻断。
	rep := mk("postgres", "continuous+initial", source.Table{
		TableRef: noPK.TableRef, HasStablePK: false, ReplicaIdentity: "default(no-pk)",
	})
	c, ok := statusOf(rep, "PG-06")
	if !ok {
		t.Fatalf("PG 源 + 无主键 + 增量：必须有 PG-06：%+v", rep.Checks)
	}
	if c.Status != source.StatusWarn {
		t.Fatalf("PG-06 应是 warn（照跑，但改/删会断流），得到 %s", c.Status)
	}
	if !strings.Contains(c.SQL, `ALTER TABLE "public"."events" REPLICA IDENTITY FULL;`) {
		t.Fatalf("PG-06 要给出可直接执行的 ALTER，得到 %q", c.SQL)
	}
	for _, want := range []string{"整条流停", "WAL"} {
		if !strings.Contains(c.Detail, want) {
			t.Fatalf("Detail 要写清「断的是整条流」与 WAL 代价（缺 %q）：%s", want, c.Detail)
		}
	}
	if !rep.CanStart {
		t.Fatal("PG-06 是 warn，不许挡开跑（用户可以选择只跑全量）")
	}

	// ② full / index：通过，不发卡片。
	for _, ri := range []string{"full", "index"} {
		rep := mk("postgres", "continuous+initial", source.Table{
			TableRef: noPK.TableRef, HasStablePK: false, ReplicaIdentity: ri,
		})
		if _, ok := statusOf(rep, "PG-06"); ok {
			t.Fatalf("REPLICA IDENTITY=%s 已能解出旧行，不该发 PG-06", ri)
		}
	}

	// ③ 有主键的表：PG-06 不适用。
	rep = mk("postgres", "continuous+initial", source.Table{
		TableRef: source.TableRef{Table: "orders"}, HasStablePK: true, ReplicaIdentity: "default(pk)",
	})
	if _, ok := statusOf(rep, "PG-06"); ok {
		t.Fatal("有主键的表不需要 REPLICA IDENTITY FULL，不该发 PG-06")
	}

	// ④ 非变更流模式（once）：不适用。
	rep = mk("postgres", "once", source.Table{
		TableRef: noPK.TableRef, HasStablePK: false, ReplicaIdentity: "default(no-pk)",
	})
	if _, ok := statusOf(rep, "PG-06"); ok {
		t.Fatal("once 模式没有变更流，不该发 PG-06")
	}

	// ⑤ MySQL 源：不要抄一份 PG-06（行镜像由 MY-02/MY-03 管）。
	rep = mk("mysql", "continuous+initial", source.Table{
		TableRef: noPK.TableRef, HasStablePK: false, Engine: "InnoDB",
	})
	if _, ok := statusOf(rep, "PG-06"); ok {
		t.Fatal("MySQL 源不该有 PG-06（那是 PG 源专有的前置）")
	}

	// ⑥ 分区表：父表 ALTER 不覆盖叶子（挂账 D10）→ 明说不代做、不给可执行 ALTER。
	rep = mk("postgres", "continuous+initial", source.Table{
		TableRef: noPK.TableRef, HasStablePK: false, ReplicaIdentity: "default(no-pk)", Partitioned: true,
	})
	c, ok = statusOf(rep, "PG-06")
	if !ok {
		t.Fatal("分区表也需要告警（叶子要逐张设置）")
	}
	if c.SQL != "" {
		t.Fatalf("分区表本版不代做：不许给一条「看起来能一次改完」的 ALTER，得到 %q", c.SQL)
	}
	for _, want := range []string{"分区表", "叶子", "不代做"} {
		if !strings.Contains(c.Detail, want) {
			t.Fatalf("分区表的 Detail 缺 %q：%s", want, c.Detail)
		}
	}
}

// SCOPE-02（continuous）在 PG 源上必须同时说出「REPLICA IDENTITY 不到位 → 整条流停」
// （§3.1：卡片必须同时出现，否则用户以为只是「停一张表」）。
func TestSCOPE02PGSourceMentionsReplicaIdentity(t *testing.T) {
	rep := Run(Input{
		SourceKind: "postgres", TargetKind: "postgres",
		SourceCaps: source.CapSet{source.CapCDC: {Supported: true}},
		SourceCat: catalog(source.Table{
			TableRef: source.TableRef{Table: "events"}, HasStablePK: false, ReplicaIdentity: "nothing",
		}),
		Tables: []string{"events"}, Mode: "continuous", BatchRows: 500,
	})
	c, _ := statusOf(rep, "SCOPE-02")
	if !strings.Contains(c.Detail, "REPLICA IDENTITY") || !strings.Contains(c.Detail, "PG-06") {
		t.Fatalf("PG 源 + 无主键 + continuous：SCOPE-02 必须同时点出 RI 这个前置，得到 %q", c.Detail)
	}
}
