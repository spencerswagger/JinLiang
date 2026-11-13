package preflight

import (
	"strings"
	"testing"

	"github.com/spencerswagger/jinliang/internal/source"
)

// TARGET-02 的三/四档（docs/DESIGN-NO-PRIMARY-KEY.md §3.4）与五条核对判据（§4.3）。
//
// 判据里「非部分/非表达式/无前缀」由 connect 的目录层过滤（UniqueKeys 只收全列唯一索引），
// 这里测的是 preflight 能补的两条：列集合完全一致、列都 NOT NULL，以及四种档位聚合。

func tgtTable(name string, pk []string, uniq [][]string, nullable ...string) source.Table {
	t := source.Table{TableRef: source.TableRef{Schema: "public", Table: name}, PrimaryKey: pk}
	t.HasStablePK = len(pk) > 0
	t.Columns = []source.Column{
		{Name: "id", Type: "bigint"},
		{Name: "tenant_id", Type: "bigint", Nullable: false},
		{Name: "event_no", Type: "text", Nullable: false},
		{Name: "note", Type: "text", Nullable: true},
	}
	t.UniqueKeys = uniq
	for _, n := range nullable {
		for i := range t.Columns {
			if t.Columns[i].Name == n {
				t.Columns[i].Nullable = true
			}
		}
	}
	return t
}

func srcTable(name string, hasPK bool) source.Table {
	return source.Table{TableRef: source.TableRef{Schema: "public", Table: name}, HasStablePK: hasPK}
}

func TestTarget02DeclaredKeyMatchesUniqueIndex(t *testing.T) {
	rep := Run(Input{
		SourceKind: "postgres", TargetKind: "postgres",
		SourceVer: "16.2", TargetVer: "16.2",
		SourceCaps: source.CapSet{source.CapCDC: {Supported: true}},
		SourceCat:  catalog(srcTable("events", false)),
		TargetCat:  catalog(tgtTable("events", nil, [][]string{{"tenant_id", "event_no"}})),
		Tables:     []string{"events"},
		Mode:       "continuous+initial",
		OnExists:   "append",
		BatchRows:  500,
		UniqueKeys: map[string][]string{"events": {"tenant_id", "event_no"}},
	})
	c, ok := statusOf(rep, "TARGET-02")
	if !ok {
		t.Fatal("声明核对通过时 TARGET-02 也必须在报告里（说明「核对过了」）")
	}
	if c.Status != source.StatusOK {
		t.Fatalf("声明与目标端索引逐列一致时应 ok，得到 %s：%s", c.Status, c.Detail)
	}
}

func TestTarget02DeclaredKeyColumnSetMismatchBlocks(t *testing.T) {
	rep := Run(Input{
		SourceKind: "postgres", TargetKind: "postgres",
		SourceVer: "16.2", TargetVer: "16.2",
		SourceCaps: source.CapSet{source.CapCDC: {Supported: true}},
		SourceCat:  catalog(srcTable("events", false)),
		TargetCat:  catalog(tgtTable("events", nil, [][]string{{"tenant_id", "event_no", "note"}})),
		Tables:     []string{"events"},
		Mode:       "continuous+initial",
		OnExists:   "append",
		BatchRows:  500,
		UniqueKeys: map[string][]string{"events": {"tenant_id", "event_no"}},
	})
	c, ok := statusOf(rep, "TARGET-02")
	if !ok || c.Status != source.StatusBlocked {
		t.Fatalf("列集合不一致必须 blocked，得到 %+v", c)
	}
	if !strings.Contains(c.Detail, "列集合不一致") {
		t.Fatalf("Detail 要点名哪条判据不过（列不一致），得到：%s", c.Detail)
	}
	if rep.CanStart {
		t.Fatal("TARGET-02 blocked 时不许开跑")
	}
}

func TestTarget02DeclaredKeyNullableColumnBlocks(t *testing.T) {
	rep := Run(Input{
		SourceKind: "postgres", TargetKind: "postgres",
		SourceVer: "16.2", TargetVer: "16.2",
		SourceCaps: source.CapSet{source.CapCDC: {Supported: true}},
		SourceCat:  catalog(srcTable("events", false)),
		TargetCat:  catalog(tgtTable("events", nil, [][]string{{"tenant_id", "event_no"}}, "event_no")),
		Tables:     []string{"events"},
		Mode:       "continuous+initial",
		OnExists:   "append",
		BatchRows:  500,
		UniqueKeys: map[string][]string{"events": {"tenant_id", "event_no"}},
	})
	c, ok := statusOf(rep, "TARGET-02")
	if !ok || c.Status != source.StatusBlocked {
		t.Fatalf("声明列可空必须 blocked（NULL 互不相等），得到 %+v", c)
	}
	if !strings.Contains(c.Detail, "允许为空") {
		t.Fatalf("Detail 要点名「含 NULL」，得到：%s", c.Detail)
	}
}

func TestTarget02DeclaredKeyMissingIndexBlocks(t *testing.T) {
	rep := Run(Input{
		SourceKind: "postgres", TargetKind: "postgres",
		SourceVer: "16.2", TargetVer: "16.2",
		SourceCaps: source.CapSet{source.CapCDC: {Supported: true}},
		SourceCat:  catalog(srcTable("events", false)),
		TargetCat:  catalog(tgtTable("events", nil, nil)),
		Tables:     []string{"events"},
		Mode:       "continuous+initial",
		OnExists:   "append",
		BatchRows:  500,
		UniqueKeys: map[string][]string{"events": {"tenant_id", "event_no"}},
	})
	c, ok := statusOf(rep, "TARGET-02")
	if !ok || c.Status != source.StatusBlocked {
		t.Fatalf("目标端没有任何可用唯一索引时必须 blocked，得到 %+v", c)
	}
	if !strings.Contains(c.Detail, "没有任何可用唯一索引") {
		t.Fatalf("Detail 要点明「不存在」，得到：%s", c.Detail)
	}
}

func TestTarget02NoPKNoDeclarationWarns(t *testing.T) {
	rep := Run(Input{
		SourceKind: "postgres", TargetKind: "mysql",
		SourceVer: "16.2", TargetVer: "8.0.36",
		SourceCaps: source.CapSet{source.CapCDC: {Supported: true}},
		SourceCat:  catalog(srcTable("events", false)),
		TargetCat:  catalog(tgtTable("events", nil, [][]string{{"tenant_id", "event_no"}})),
		Tables:     []string{"events"},
		Mode:       "continuous+initial",
		OnExists:   "append",
		BatchRows:  500,
	})
	c, ok := statusOf(rep, "TARGET-02")
	if !ok || c.Status != source.StatusWarn {
		t.Fatalf("无主键且未声明应 warn（只追加），得到 %+v", c)
	}
	if !strings.Contains(c.Detail, "unique_key") || !strings.Contains(c.Detail, "once / full_interval") {
		t.Fatalf("Detail 要给两条出路（unique_key / once·full_interval），得到：%s", c.Detail)
	}
}

func TestTarget02DeclaredIgnoredWhenSourceHasPK(t *testing.T) {
	rep := Run(Input{
		SourceKind: "postgres", TargetKind: "postgres",
		SourceVer: "16.2", TargetVer: "16.2",
		SourceCaps: source.CapSet{source.CapCDC: {Supported: true}},
		SourceCat:  catalog(srcTable("orders", true)),
		TargetCat:  catalog(tgtTable("orders", []string{"id"}, nil)),
		Tables:     []string{"orders"},
		Mode:       "continuous+initial",
		OnExists:   "append",
		BatchRows:  500,
		UniqueKeys: map[string][]string{"orders": {"tenant_id", "event_no"}},
	})
	c, ok := statusOf(rep, "TARGET-02")
	if !ok || c.Status != source.StatusWarn {
		t.Fatalf("源端有主键时声明应「忽略 + warn」（§2.2 格 7），得到 %+v", c)
	}
	if !strings.Contains(c.Detail, "忽略") {
		t.Fatalf("Detail 要说清声明被忽略，得到：%s", c.Detail)
	}
	if rep.CanStart != true {
		t.Fatal("声明被忽略只是 warn，不该阻断")
	}
}

func TestTarget02SourcePKTargetMissingKeyWarns(t *testing.T) {
	rep := Run(Input{
		SourceKind: "postgres", TargetKind: "postgres",
		SourceVer: "16.2", TargetVer: "16.2",
		SourceCaps: source.CapSet{source.CapCDC: {Supported: true}},
		SourceCat:  catalog(srcTable("orders", true)),
		TargetCat:  catalog(tgtTable("orders", nil, nil)),
		Tables:     []string{"orders"},
		Mode:       "continuous+initial",
		OnExists:   "append",
		BatchRows:  500,
	})
	c, ok := statusOf(rep, "TARGET-02")
	if !ok || c.Status != source.StatusWarn {
		t.Fatalf("源端有主键、目标端缺对应唯一键应 warn（格 6），得到 %+v", c)
	}
	if rep.CanStart != true {
		t.Fatal("格 6 只是 warn，不该阻断")
	}
}

func TestTarget02SkippedWhenTargetCatalogUnavailable(t *testing.T) {
	rep := Run(Input{
		SourceKind: "postgres", TargetKind: "postgres",
		SourceVer: "16.2", TargetVer: "16.2",
		SourceCaps: source.CapSet{source.CapCDC: {Supported: true}},
		SourceCat:  catalog(srcTable("orders", true)),
		TargetCat:  nil, // 目标表清单没读到：TARGET-01 已经说过，这里不假装知道
		Tables:     []string{"orders"},
		Mode:       "continuous+initial",
		OnExists:   "append",
		BatchRows:  500,
	})
	if _, ok := statusOf(rep, "TARGET-02"); ok {
		t.Fatal("目标端目录读不到时不该出 TARGET-02（不能假装核对过）")
	}
}

func TestTarget02SourcePKTargetHasOtherUniqueKeyWarns(t *testing.T) {
	rep := Run(Input{
		SourceKind: "postgres", TargetKind: "postgres",
		SourceVer: "16.2", TargetVer: "16.2",
		SourceCaps: source.CapSet{source.CapCDC: {Supported: true}},
		SourceCat:  catalog(srcTable("orders", true)),
		// 目标表没有主键，只有另一组唯一索引：upsert 会照它更新，语义≠按源端主键。
		TargetCat: catalog(tgtTable("orders", nil, [][]string{{"tenant_id", "event_no"}})),
		Tables:    []string{"orders"},
		Mode:      "continuous+initial",
		OnExists:  "append",
		BatchRows: 500,
	})
	c, ok := statusOf(rep, "TARGET-02")
	if !ok || c.Status != source.StatusWarn {
		t.Fatalf("源端有主键、目标端只有别的唯一索引应 warn，得到 %+v", c)
	}
	if !strings.Contains(c.Detail, "另一组唯一索引") {
		t.Fatalf("Detail 要把「按别的键更新」说清，得到：%s", c.Detail)
	}
	if !rep.CanStart {
		t.Fatal("这只是 warn，不该阻断")
	}
}
