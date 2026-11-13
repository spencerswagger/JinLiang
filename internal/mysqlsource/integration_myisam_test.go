//go:build integration

// 矩阵 smoke 格：MY-08 存储引擎走真库（SPEC:76 的拍板口径——MyISAM 无事务，
// warn 照跑并标注，不阻断）。
//
// 单测（internal/preflight/b0_test.go）钉的是 engineChecks 的判断逻辑；这一格钉
// 的是「生产同款装配」：引擎名必须从 connect/mysql.go 的 information_schema.TABLES
// 一路带进 preflight.Input.SourceCat——中间任何一环把 Engine 丢了，MY-08 就哑火。
// MyISAM 与 MRG_MyISAM 一并覆盖；对同批 InnoDB 表断言不误伤、不阻断。
//
// CI：mysql-matrix.yml 已把 ./internal/mysqlsource/ 的 integration 用例在
// 5.7/8.0/8.4/9 四档全扫进去，无需改 workflow。MyISAM/MRG_MyISAM 四档都可建。
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
	exec(t, s, "CREATE TABLE "+quoteIdent(db)+".jl_smoke_myisam ("+
		"id INT PRIMARY KEY, note VARCHAR(32) NULL) ENGINE=MyISAM")
	exec(t, s, "CREATE TABLE "+quoteIdent(db)+".jl_smoke_mrg ("+
		"id INT PRIMARY KEY, note VARCHAR(32) NULL) ENGINE=MRG_MyISAM "+
		"UNION=("+quoteIdent(db)+".jl_smoke_myisam) INSERT_METHOD=LAST")
	exec(t, s, "CREATE TABLE "+quoteIdent(db)+".jl_smoke_innodb ("+
		"id INT PRIMARY KEY, note VARCHAR(32) NULL) ENGINE=InnoDB")
	defer func() {
		exec(t, s, "DROP TABLE IF EXISTS "+quoteIdent(db)+".jl_smoke_mrg")
		exec(t, s, "DROP TABLE IF EXISTS "+quoteIdent(db)+".jl_smoke_myisam")
		exec(t, s, "DROP TABLE IF EXISTS "+quoteIdent(db)+".jl_smoke_innodb")
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
	} {
		got := engineOf(cat, want.table)
		if got == "" {
			t.Fatalf("Catalog 没带回 %s 的 ENGINE（connect/mysql.go 的装配在这档 MySQL 上断了）", want.table)
		}
		if !strings.EqualFold(got, want.engine) {
			t.Fatalf("%s ENGINE = %q，期望 %s", want.table, got, want.engine)
		}
	}

	rep := preflight.Run(preflight.Input{
		JobName:    "smoke-myisam",
		SourceKind: "mysql", TargetKind: "postgres",
		SourceVer: ver, TargetVer: "16.2",
		SourceCaps: caps,
		SourceCat:  &cat,
		Tables:     []string{"jl_smoke_myisam", "jl_smoke_mrg", "jl_smoke_innodb"},
		Mode:       "continuous+initial",
		OnExists:   "error",
		BatchRows:  500,
	})
	c, ok := statusOfCheck(rep, "MY-08")
	if !ok {
		t.Fatalf("选中表里有 MyISAM/MRG_MyISAM，应有 MY-08：%+v", rep.Checks)
	}
	if c.Status != source.StatusWarn {
		t.Fatalf("MY-08 = %s，SPEC 口径是 warn 照跑并标注（无事务，不物理阻断）：%s", c.Status, c.Detail)
	}
	if !rep.CanStart {
		t.Fatalf("MyISAM 照跑，CanStart 不该被压成 false：%+v", rep.Checks)
	}
	for _, want := range []string{"jl_smoke_myisam", "jl_smoke_mrg"} {
		if !strings.Contains(c.Detail, want) {
			t.Errorf("MY-08 明细应点名 %s：%s", want, c.Detail)
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
