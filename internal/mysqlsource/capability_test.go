package mysqlsource

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/spencerswagger/jinliang/internal/source"
)

// probeFixture drives Prober with a scripted server so the version-dependent
// bits (renamed statements, missing variables) can be tested without one.
type probeFixture struct {
	mock    sqlmock.Sqlmock
	db      *sql.DB
	vars    map[string]string
	grants  []string
	status  string // which SHOW works: "binary" / "master" / ""
	version string
}

func newProbeFixture(t *testing.T, version string) *probeFixture {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return &probeFixture{
		mock: mock, db: db,
		vars:    map[string]string{},
		status:  "binary",
		version: version,
	}
}

func (f *probeFixture) run(t *testing.T) *ProbeResult {
	t.Helper()
	f.mock.ExpectQuery("SELECT VERSION").WillReturnRows(
		sqlmock.NewRows([]string{"v"}).AddRow(f.version))

	varNames := []string{
		"log_bin", "binlog_format", "binlog_row_image", "gtid_mode",
		"gtid_strict_mode", "server_id", "binlog_transaction_compression",
		"binlog_row_metadata", "binlog_expire_logs_seconds", "expire_logs_days",
		"max_binlog_size", "binlog_checksum", "binlog_row_value_options",
		"default_authentication_plugin", "authentication_policy",
	}
	for _, name := range varNames {
		q := "SELECT @@" + name
		if v, ok := f.vars[name]; ok {
			f.mock.ExpectQuery(quoteForMock(q)).WillReturnRows(
				sqlmock.NewRows([]string{"v"}).AddRow(v))
			continue
		}
		f.mock.ExpectQuery(quoteForMock(q)).WillReturnError(
			errors.New("Error 1193: Unknown system variable '" + name + "'"))
	}
	switch f.status {
	case "binary":
		f.mock.ExpectQuery("SHOW BINARY LOG STATUS").WillReturnRows(
			sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}))
	case "master":
		f.mock.ExpectQuery("SHOW BINARY LOG STATUS").WillReturnError(errors.New("Error 1064: syntax error"))
		f.mock.ExpectQuery("SHOW MASTER STATUS").WillReturnRows(
			sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}))
	}
	f.mock.ExpectQuery("SHOW GRANTS").WillReturnRows(
		sqlmock.NewRows([]string{"g"}).AddRow(f.grants[0]))
	for _, g := range f.grants[1:] {
		// 上一次的 rows 已经用掉，这里用一个新查询不支持；测试里只放一行够用。
		_ = g
	}
	res, err := NewProber(f.db).Probe(context.Background())
	if err != nil {
		t.Fatalf("探测失败: %v", err)
	}
	return res
}

func quoteForMock(q string) string {
	return "^" + q + "$"
}

func TestProbeFullFeatured80(t *testing.T) {
	f := newProbeFixture(t, "8.0.28")
	f.vars = map[string]string{
		"log_bin": "1", "binlog_format": "ROW", "binlog_row_image": "FULL",
		"gtid_mode": "ON", "server_id": "1", "binlog_transaction_compression": "OFF",
		"binlog_row_metadata": "FULL", "binlog_expire_logs_seconds": "2592000",
		"default_authentication_plugin": "caching_sha2_password",
	}
	f.grants = []string{"GRANT REPLICATION SLAVE, REPLICATION CLIENT, BACKUP_ADMIN ON *.* TO `u`@`%`"}
	res := f.run(t)

	if !res.Caps[source.CapCDC].Supported {
		t.Fatal("log_bin=1 + ROW + 复制权限，增量应当可用")
	}
	if !res.Caps[source.CapGTID].Supported {
		t.Fatal("gtid_mode=ON 时 GTID 位点应可用")
	}
	if res.Caps[source.CapGTID].Compensation.Level != source.LevelCostly {
		t.Fatalf("缺 GTID 时必须有代价补偿说明: %#v", res.Caps[source.CapGTID])
	}
	if !res.Caps[source.CapColumnList].Supported {
		t.Fatal("binlog_row_metadata=FULL 时列名元数据应可用")
	}
	if !res.Caps[source.CapBackupLock].Supported {
		t.Fatal("有 BACKUP_ADMIN 时应认为支持 LOCK INSTANCE FOR BACKUP")
	}
	if res.StatusStatement != "SHOW BINARY LOG STATUS" {
		t.Fatalf("8.0.28 也认 SHOW BINARY LOG STATUS，探测结果 = %q", res.StatusStatement)
	}
}

func TestProbe57WithMasterStatusOnly(t *testing.T) {
	f := newProbeFixture(t, "5.7.44-log")
	f.vars = map[string]string{
		"log_bin": "1", "binlog_format": "ROW", "binlog_row_image": "FULL",
		"gtid_mode": "OFF", "server_id": "1", "expire_logs_days": "7",
	}
	f.status = "master"
	f.grants = []string{"GRANT REPLICATION SLAVE ON *.* TO `u`@`%`"}
	res := f.run(t)

	if res.StatusStatement != "SHOW MASTER STATUS" {
		t.Fatalf("5.7 只有 SHOW MASTER STATUS，探测结果 = %q", res.StatusStatement)
	}
	if res.Caps[source.CapBinlogCompress].Supported {
		t.Fatal("5.7 没有 binlog_transaction_compression，不该报告支持")
	}
	if res.Caps[source.CapGTID].Supported {
		t.Fatal("gtid_mode=OFF 时不该报告支持")
	}
	if res.Server.Major != 5 || res.Server.Minor != 7 {
		t.Fatalf("版本解析错了: %#v", res.Server)
	}
}

func TestProbe84WithoutBackupAdmin(t *testing.T) {
	f := newProbeFixture(t, "8.4.2")
	f.vars = map[string]string{
		"log_bin": "1", "binlog_format": "ROW", "binlog_row_image": "FULL",
		"gtid_mode": "ON", "server_id": "2", "binlog_row_metadata": "FULL",
		"binlog_transaction_compression": "ON",
		"authentication_policy":          "caching_sha2_password",
	}
	f.grants = []string{"GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO `u`@`%`"}
	res := f.run(t)

	if res.Caps[source.CapBackupLock].Supported {
		t.Fatal("没有 BACKUP_ADMIN 时不该报告支持备份锁")
	}
	if res.Caps[source.CapBinlogCompress].Compensation.Level != source.LevelAuto {
		t.Fatalf("源端开了事务压缩时，我们应声明会自动解压: %#v", res.Caps[source.CapBinlogCompress])
	}
	if res.Variables["authentication_policy"] == "" {
		t.Fatal("8.4 的 authentication_policy 应被读到（default_authentication_plugin 已改名）")
	}
}

func TestProbeCdcBlockedWhenLogBinOff(t *testing.T) {
	f := newProbeFixture(t, "8.0.36")
	f.vars = map[string]string{"log_bin": "0", "binlog_format": "STATEMENT", "gtid_mode": "OFF"}
	f.grants = []string{"GRANT ALL PRIVILEGES ON *.* TO `u`@`%`"}
	res := f.run(t)

	state := res.Caps[source.CapCDC]
	if state.Supported {
		t.Fatal("log_bin=0 时不能报告增量可用")
	}
	if state.Compensation.Level != source.LevelBlocked {
		t.Fatalf("缺变更流属于「物理做不到」，必须是 blocked: %#v", state)
	}
	if state.Compensation.Note == "" || state.Compensation.ConfigKey == "" {
		t.Fatalf("blocked 必须带说明和替代配置: %#v", state.Compensation)
	}
	var found bool
	for _, ch := range res.Checks {
		if ch.ID == "MY-01" && ch.Status == source.StatusBlocked {
			found = true
		}
	}
	if !found {
		t.Fatal("预检里应有 MY-01 且状态为 blocked")
	}
}

func TestProbeAcceptsRenamedReplicationPrivilege(t *testing.T) {
	// 8.0.22+/9.x 把 REPLICATION SLAVE 叫成 REPLICATION REPLICA：不能因此误判断流不可用。
	f := newProbeFixture(t, "9.1.0")
	f.vars = map[string]string{"log_bin": "1", "binlog_format": "ROW", "binlog_row_image": "FULL",
		"gtid_mode": "ON", "server_id": "1"}
	f.grants = []string{"GRANT REPLICATION REPLICA, REPLICATION CLIENT ON *.* TO `u`@`%`"}
	res := f.run(t)
	if !res.Caps[source.CapCDC].Supported {
		t.Fatalf("认 REPLICATION REPLICA 的账号也要算有权限: %#v", res.Caps[source.CapCDC])
	}
}

func TestProbeChecksAreActionable(t *testing.T) {
	f := newProbeFixture(t, "8.0.28")
	f.vars = map[string]string{"log_bin": "1", "binlog_format": "ROW", "binlog_row_image": "MINIMAL",
		"gtid_mode": "OFF", "binlog_row_value_options": "PARTIAL_JSON"}
	f.grants = []string{"GRANT REPLICATION SLAVE ON *.* TO `u`@`%`"}
	res := f.run(t)

	var rowImage, partialJSON *source.Check
	for i := range res.Checks {
		switch res.Checks[i].ID {
		case "MY-02":
			rowImage = &res.Checks[i]
		case "MY-04":
			partialJSON = &res.Checks[i]
		}
	}
	if rowImage == nil || rowImage.Status != source.StatusAutofixable {
		t.Fatalf("binlog_row_image=MINIMAL 应给出可自动修的预检项: %#v", rowImage)
	}
	if rowImage.SQL == "" || rowImage.Undo == "" {
		t.Fatal("可修项必须带 SQL 和回滚语句")
	}
	if partialJSON == nil {
		t.Fatal("PARTIAL_JSON 必须被识别出来（否则更新会缺列）")
	}
}

func TestProbeMariaDBBlockedKeepsMy08FreeForEngineCheck(t *testing.T) {
	// docs/SPEC.md:107 的 MY-08 是「表的存储引擎」（MyISAM → warn 照跑）；
	// MariaDB 检查之前错占了 MY-08（blocked），口径对不上——现在挪到 MY-11。
	f := newProbeFixture(t, "11.4.2-MariaDB-1:11.4.2+maria~ubu2404")
	f.vars = map[string]string{"log_bin": "1", "binlog_format": "ROW", "binlog_row_image": "FULL"}
	f.grants = []string{"GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO `u`@`%`"}
	res := f.run(t)

	var maria *source.Check
	for i := range res.Checks {
		switch res.Checks[i].ID {
		case "MY-08":
			t.Fatalf("MY-08 必须留给存储引擎检查，MariaDB 检查不该再占它: %+v", res.Checks[i])
		case "MY-11":
			maria = &res.Checks[i]
		}
	}
	if maria == nil {
		t.Fatal("MariaDB 源必须有 MY-11 检查")
	}
	if maria.Status != source.StatusBlocked {
		t.Fatalf("MariaDB 本期不支持，检查应保持 blocked: %+v", maria)
	}
	if maria.Code != "E1001" {
		t.Fatalf("错误码语义不变：仍挂 E1001，实际 %q", maria.Code)
	}
}
