//go:build integration

// 矩阵 smoke 格：MY-08 引擎事务能力走真库（SPEC MY-08：按服务器自报的
// information_schema.ENGINES.TRANSACTIONS 判，无事务 → 降级照跑并标注）。
//
// 单测（internal/preflight/engine_test.go）钉的是判据本身；这一格钉的是「生产同款
// 装配」的两条链路，任一条断了 MY-08 就哑火：
//   - 引擎名从 information_schema.TABLES 带进 Catalog.Table.Engine；
//   - 事务能力从 information_schema.ENGINES 带进 Catalog.Engines（两处 Catalog() 都要填，
//     connect/mysql.go 是预检那条、mysqlsource/snapshot.go 是引擎那条）。
//
// 覆盖 MyISAM / MRG_MyISAM / MEMORY / CSV / ARCHIVE（都是 TRANSACTIONS=NO），
// 对同批 InnoDB 断言不误伤、不阻断。
//
// CI：mysql-matrix.yml 已把 ./internal/mysqlsource/ 的 integration 用例在
// 5.7/8.0/8.4/9 四档全扫进去，无需改 workflow。五个引擎四档都可建
// （ARCHIVE 要求所有列 NOT NULL，CSV/ARCHIVE 不能建索引）。
package mysqlsource

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/spencerswagger/jinliang/internal/connect"
	"github.com/spencerswagger/jinliang/internal/dsn"
	"github.com/spencerswagger/jinliang/internal/preflight"
	"github.com/spencerswagger/jinliang/internal/source"
)

// TestIntegrationMyISAMPreflightWarnCreatesJobableReport：
// 真建 InnoDB / MyISAM / MRG_MyISAM 三张表，走 connect.Open → Catalog 填
// SourceCat（生产 PreflightJob 的同款装配），断言：
//   - MY-08 存在且 Status = warn（照跑）；
//   - CanStart 不被压成 false（不阻断）；
//   - Affected/Detail 点名两张 MyISAM 系表（标注正确），InnoDB 不被点名；
//   - 只选 InnoDB 时凑不成 MY-08（warn 路径不误伤）。
func TestIntegrationMyISAMPreflightWarnCreatesJobableReport(t *testing.T) {
	ep, err := dsn.Parse(requireDSN(t))
	if err != nil {
		t.Fatalf("解析 DSN 失败: %v", err)
	}
	db := ep.Database
	if db == "" {
		t.Fatal("DSN 必须带库名：MySQL 的 schema 就是库名，Catalog 靠它取表清单")
	}

	s := openSource(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// 建表：MRG_MyISAM 的 UNION 子表本身也是 MyISAM，语义一并覆盖。
	// 引擎名用大写建表，断言 Catalog 回读的大小写必须是镜像规范形式。
	exec(t, s, "DROP TABLE IF EXISTS "+quoteIdent(db)+".jl_smoke_mrg")
	exec(t, s, "DROP TABLE IF EXISTS "+quoteIdent(db)+".jl_smoke_myisam")
	exec(t, s, "DROP TABLE IF EXISTS "+quoteIdent(db)+".jl_smoke_innodb")
	exec(t, s, "DROP TABLE IF EXISTS "+quoteIdent(db)+".jl_smoke_memory")
	exec(t, s, "DROP TABLE IF EXISTS "+quoteIdent(db)+".jl_smoke_csv")
	exec(t, s, "DROP TABLE IF EXISTS "+quoteIdent(db)+".jl_smoke_archive")
	exec(t, s, "CREATE TABLE "+quoteIdent(db)+".jl_smoke_myisam ("+
		"id INT PRIMARY KEY, note VARCHAR(32) NULL) ENGINE=MyISAM")
	exec(t, s, "CREATE TABLE "+quoteIdent(db)+".jl_smoke_mrg ("+
		"id INT PRIMARY KEY, note VARCHAR(32) NULL) ENGINE=MRG_MyISAM "+
		"UNION=("+quoteIdent(db)+".jl_smoke_myisam) INSERT_METHOD=LAST")
	exec(t, s, "CREATE TABLE "+quoteIdent(db)+".jl_smoke_innodb ("+
		"id INT PRIMARY KEY, note VARCHAR(32) NULL) ENGINE=InnoDB")
	// MEMORY 有事务外的另一层代价（重启即空），但能建主键；
	// CSV / ARCHIVE 不能建索引（⇒ 恒无主键，归 SCOPE-02），ARCHIVE 还不接受可空列。
	exec(t, s, "CREATE TABLE "+quoteIdent(db)+".jl_smoke_memory ("+
		"id INT PRIMARY KEY, note VARCHAR(32) NULL) ENGINE=MEMORY")
	exec(t, s, "CREATE TABLE "+quoteIdent(db)+".jl_smoke_csv (id INT NOT NULL) ENGINE=CSV")
	exec(t, s, "CREATE TABLE "+quoteIdent(db)+".jl_smoke_archive (id INT NOT NULL) ENGINE=ARCHIVE")
	defer func() {
		exec(t, s, "DROP TABLE IF EXISTS "+quoteIdent(db)+".jl_smoke_mrg")
		exec(t, s, "DROP TABLE IF EXISTS "+quoteIdent(db)+".jl_smoke_myisam")
		exec(t, s, "DROP TABLE IF EXISTS "+quoteIdent(db)+".jl_smoke_innodb")
		exec(t, s, "DROP TABLE IF EXISTS "+quoteIdent(db)+".jl_smoke_memory")
		exec(t, s, "DROP TABLE IF EXISTS "+quoteIdent(db)+".jl_smoke_csv")
		exec(t, s, "DROP TABLE IF EXISTS "+quoteIdent(db)+".jl_smoke_archive")
	}()

	// 生产 PreflightJob 的同款装配：connect.Open → Version/Probe/Catalog。
	conn, err := connect.Open(ep)
	if err != nil {
		t.Fatalf("生产同款连接失败: %v", err)
	}
	defer conn.Close()

	sctx, sc := context.WithTimeout(ctx, 12*time.Second)
	defer sc()
	ver, err := conn.Version(sctx)
	if err != nil {
		t.Fatalf("取版本失败: %v", err)
	}
	caps, err := conn.Probe(sctx)
	if err != nil {
		t.Fatalf("探测失败: %v", err)
	}
	cat, err := conn.Catalog(sctx, source.Scope{Schema: db})
	if err != nil {
		t.Fatalf("取 Catalog 失败: %v", err)
	}

	// 前置自证：Catalog 必须把引擎名带出来，否则下面断言的是空转。
	for _, want := range []struct{ table, engine string }{
		{"jl_smoke_myisam", "MyISAM"},
		{"jl_smoke_mrg", "MRG_MyISAM"},
		{"jl_smoke_innodb", "InnoDB"},
		{"jl_smoke_memory", "MEMORY"},
		{"jl_smoke_csv", "CSV"},
		{"jl_smoke_archive", "ARCHIVE"},
	} {
		got := engineOf(cat, want.table)
		if got == "" {
			t.Fatalf("Catalog 没带回 %s 的 ENGINE（connect/mysql.go 的装配在这档 MySQL 上断了）", want.table)
		}
		if !strings.EqualFold(got, want.engine) {
			t.Fatalf("%s ENGINE = %q，期望 %s", want.table, got, want.engine)
		}
	}

	// 判据的输入必须是服务器自报：接线断了（Engines 为空）这里先红，
	// 而不是等到「预检降级、收尾不标注」那种假绿。
	if tx, ok := cat.Engines["INNODB"]; !ok || !tx {
		t.Fatalf("Catalog.Engines[INNODB] 应为 true（实际 %v，ok=%v）：两处 Catalog() 的 ENGINE 探测断了", tx, ok)
	}
	for _, e := range []string{"MYISAM", "MRG_MYISAM", "MEMORY", "CSV", "ARCHIVE"} {
		if tx, ok := cat.Engines[e]; !ok || tx {
			t.Fatalf("Catalog.Engines[%s] 应为 false（实际 %v，ok=%v）", e, tx, ok)
		}
	}

	rep := preflight.Run(preflight.Input{
		JobName:    "smoke-myisam",
		SourceKind: "mysql", TargetKind: "postgres",
		SourceVer: ver, TargetVer: "16.2",
		SourceCaps: caps,
		SourceCat:  &cat,
		Tables: []string{"jl_smoke_myisam", "jl_smoke_mrg", "jl_smoke_memory",
			"jl_smoke_csv", "jl_smoke_archive", "jl_smoke_innodb"},
		Mode:      "continuous+initial",
		OnExists:  "error",
		BatchRows: 500,
	})
	c, ok := statusOfCheck(rep, "MY-08")
	if !ok {
		t.Fatalf("选中表里有五个无事务引擎，应有 MY-08：%+v", rep.Checks)
	}
	if c.Status != source.StatusWarn {
		t.Fatalf("MY-08 = %s，SPEC 口径是 warn 降级（无事务，不物理阻断）：%s", c.Status, c.Detail)
	}
	if !rep.CanStart {
		t.Fatalf("降级照跑，CanStart 不该被压成 false：%+v", rep.Checks)
	}
	// information_schema 回报的引擎名大小写不统一（MRG_MYISAM 全大写，历史文档写 MRG_MyISAM），
	// 所以按小写比：要的是「点名真实引擎名」，不是「大小写跟文档一致」。
	detailLower := strings.ToLower(c.Detail)
	for _, want := range []string{"jl_smoke_myisam(myisam)", "jl_smoke_mrg(mrg_myisam)",
		"jl_smoke_memory(memory)", "jl_smoke_csv(csv)", "jl_smoke_archive(archive)"} {
		if !strings.Contains(detailLower, strings.ToLower(want)) {
			t.Errorf("MY-08 明细应点名 %s（带真实引擎名）：%s", want, c.Detail)
		}
	}
	// 引擎特有代价：命中才追加（SPEC MY-08 的四段文案之后）。
	for _, want := range []string{"重启/崩溃即空", "不支持任何索引"} {
		if !strings.Contains(c.Detail, want) {
			t.Errorf("MY-08 应带上引擎特有代价 %q：%s", want, c.Detail)
		}
	}
	if strings.Contains(c.Detail, "jl_smoke_innodb") {
		t.Errorf("InnoDB 表不该被点名：%s", c.Detail)
	}

	// 收尾反例：只选 InnoDB 时凑不成 MY-08，warn 路径不误伤。
	rep = preflight.Run(preflight.Input{
		JobName:    "smoke-myisam-clean",
		SourceKind: "mysql", TargetKind: "postgres",
		SourceVer: ver, TargetVer: "16.2",
		SourceCaps: caps,
		SourceCat:  &cat,
		Tables:     []string{"jl_smoke_innodb"},
		Mode:       "once", BatchRows: 500,
	})
	if _, ok := statusOfCheck(rep, "MY-08"); ok {
		t.Fatal("只选 InnoDB 不该发 MY-08")
	}
	if !rep.CanStart {
		t.Fatalf("InnoDB 配置应 CanStart：%+v", rep.Checks)
	}
}

func engineOf(cat source.Catalog, table string) string {
	for i := range cat.Tables {
		if cat.Tables[i].Table == table {
			return cat.Tables[i].Engine
		}
	}
	return ""
}

func statusOfCheck(rep source.Report, id string) (source.Check, bool) {
	for _, c := range rep.Checks {
		if c.ID == id {
			return c, true
		}
	}
	return source.Check{}, false
}
