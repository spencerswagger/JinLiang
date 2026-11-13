package mysqlsource

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/spencerswagger/jinliang/internal/source"
)

// querier is the slice of *sql.DB that probing needs, so the capability tests
// can run without a server and the real code can pass a *sql.DB.
type querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// ServerInfo is what the server says about itself. Version numbers are
// recorded for the support matrix only — decisions are made on probed
// capabilities, never on these numbers (docs/DESIGN.md §4A).
type ServerInfo struct {
	Version string // 8.0.28 / 8.4.2 / 9.1.0
	Flavor  string // mysql / percona / mariadb（MariaDB 本期不支持）
	Major   int
	Minor   int
	Patch   int
}

// ProbeResult is everything probing learned, in the shape preflight consumes.
type ProbeResult struct {
	Server ServerInfo
	Caps   source.CapSet
	Checks []source.Check

	// StatusStatement is the one that worked on this server:
	// "SHOW BINARY LOG STATUS" (8.4+) or "SHOW MASTER STATUS" (≤8.0).
	StatusStatement string
	// Variables holds the probed values we care about later (binlog file size
	// limits, retention, row image…).
	Variables map[string]string
	// Missing lists capability names that are absent, for the summary line.
	Missing []source.Capability
}

// Prober reads capabilities out of a live server.
type Prober struct {
	q querier
}

// NewProber wraps a *sql.DB.
func NewProber(db *sql.DB) *Prober { return &Prober{q: db} }

// newProberWith is used by tests to inject a fake.
func newProberWith(q querier) *Prober { return &Prober{q: q} }

// Probe implements source.Endpoint's capability part.
//
// 探测而不是判版本号：8.4 把 SHOW MASTER STATUS 换了名字、9.x 删了
// mysql_native_password，靠版本号写 if/else 会在下一个小版本上崩掉。
func (p *Prober) Probe(ctx context.Context) (*ProbeResult, error) {
	res := &ProbeResult{
		Caps:      source.CapSet{},
		Variables: map[string]string{},
	}
	ver, err := p.version(ctx)
	if err != nil {
		return nil, err
	}
	res.Server = ver

	vars := map[string]string{}
	for _, name := range []string{
		"log_bin", "binlog_format", "binlog_row_image", "gtid_mode",
		"gtid_strict_mode", "server_id", "binlog_transaction_compression",
		"binlog_row_metadata", "binlog_expire_logs_seconds", "expire_logs_days",
		"max_binlog_size", "binlog_checksum", "binlog_row_value_options",
	} {
		if v, ok := p.readVar(ctx, name); ok {
			vars[name] = v
		}
	}
	// 8.4/9.x 把 default_authentication_plugin 换成 authentication_policy，
	// 两个都试，记下哪个在（这就是「改名」类差异的处理方式）。
	for _, name := range []string{"default_authentication_plugin", "authentication_policy"} {
		if v, ok := p.readVar(ctx, name); ok {
			vars[name] = v
		}
	}
	res.Variables = vars

	statusStmt, statusErr := p.probeStatusStatement(ctx)
	if statusErr == nil {
		res.StatusStatement = statusStmt
	}

	grants, grantsErr := p.grants(ctx)
	canReplicate := grantsErr != nil || grantsAllowReplication(grants)

	cdcReady := vars["log_bin"] == "1" && strings.EqualFold(vars["binlog_format"], "ROW") && canReplicate
	rowImageFull := strings.EqualFold(vars["binlog_row_image"], "FULL")
	gtidOn := strings.EqualFold(vars["gtid_mode"], "ON")

	comp := func(level source.Level, note, key string) source.Compensation {
		return source.Compensation{Level: level, Note: note, ConfigKey: key}
	}

	res.Caps[source.CapSnapshot] = source.CapabilityState{
		Supported:    true,
		Compensation: comp(source.LevelAuto, "全量读取总是可用", ""),
	}
	res.Caps[source.CapCDC] = source.CapabilityState{
		Supported: cdcReady,
		Compensation: comp(source.LevelBlocked,
			"源库的变更流水没开好（log_bin="+orNA(vars["log_bin"])+"，binlog_format="+orNA(vars["binlog_format"])+
				"，复制权限="+yesNo(canReplicate)+"）：实时增量做不到，可先改用定时增量", "mode.frequency"),
	}
	res.Caps[source.CapGTID] = source.CapabilityState{
		Supported:    gtidOn,
		SinceVersion: "5.6",
		Compensation: comp(source.LevelCostly,
			"没开 GTID：位点只能靠文件名+偏移，主从切换后可能对不上；我们仍会写 GTID 字段但为空", ""),
	}
	res.Caps[source.CapBinlogCompress] = source.CapabilityState{
		Supported:    vars["binlog_transaction_compression"] != "",
		SinceVersion: "8.0.20",
		Compensation: comp(source.LevelAuto,
			"没有事务压缩变量：源端不会压缩事务，缺少的只是带宽优化", ""),
	}
	if v, ok := vars["binlog_transaction_compression"]; ok && isOn(v) {
		res.Caps[source.CapBinlogCompress] = source.CapabilityState{
			Supported:    true,
			SinceVersion: "8.0.20",
			Compensation: comp(source.LevelAuto, "源端开了事务压缩，我们会按 zstd 解压后再解析", ""),
		}
	}
	res.Caps[source.CapColumnList] = source.CapabilityState{
		Supported:    strings.EqualFold(vars["binlog_row_metadata"], "FULL"),
		SinceVersion: "8.0.1",
		Compensation: comp(source.LevelCostly,
			"变更流不带列名/主键元数据：加列时会重发整行，我们靠读表结构补全", ""),
	}
	res.Caps[source.CapBackupLock] = source.CapabilityState{
		Supported:    hasPrivilege(grants, "BACKUP_ADMIN"),
		SinceVersion: "8.0",
		Compensation: comp(source.LevelCostly,
			"不能用 LOCK INSTANCE FOR BACKUP：初始化会加一次毫秒级全局读锁（FTWRL）", ""),
	}
	res.Caps[source.CapTruncate] = source.CapabilityState{
		Supported: cdcReady,
		Compensation: comp(source.LevelCostly,
			"TRUNCATE 在行格式下只记语句：目标端按「清空」处理，过滤条件可能不生效", ""),
	}
	res.Caps[source.CapSnapshotExport] = source.CapabilityState{
		Supported: false,
		Compensation: comp(source.LevelCostly,
			"MySQL 没有一致性快照导出：初始化和增量之间有极小窗口的重复，靠 upsert 去重", ""),
	}
	res.Caps[source.CapRowFilter] = source.CapabilityState{
		Supported:    false,
		Compensation: comp(source.LevelCostly, "MySQL 变更流不支持行过滤，只能整表跟随", ""),
	}
	res.Caps[source.CapStreamingTx] = source.CapabilityState{
		Supported:    false,
		Compensation: comp(source.LevelCostly, "超大事务要整段读进内存（可 spill 到磁盘）", "reliability.tx_oversize"),
	}
	res.Caps[source.CapTwoPhase] = source.CapabilityState{
		Supported:    false,
		Compensation: comp(source.LevelCostly, "MySQL 的 XA 事务按单阶段处理，极小概率出现半提交", ""),
	}
	res.Caps[source.CapSlotDiskLimit] = source.CapabilityState{Supported: false, Compensation: comp(source.LevelAuto, "", "")}
	res.Caps[source.CapSlotAdvance] = source.CapabilityState{Supported: false, Compensation: comp(source.LevelAuto, "", "")}
	res.Caps[source.CapBinaryCopy] = source.CapabilityState{Supported: false, Compensation: comp(source.LevelAuto, "", "")}

	for cap := range res.Caps {
		if !res.Caps[cap].Supported && res.Caps[cap].Compensation.Level != source.LevelAuto {
			res.Missing = append(res.Missing, cap)
		}
	}

	res.Checks = p.checks(res, vars, grants, canReplicate, rowImageFull, cdcReady)
	return res, nil
}

// checks builds the preflight rows a non-DBA can act on.
func (p *Prober) checks(res *ProbeResult, vars map[string]string, grants string,
	canReplicate, rowImageFull, cdcReady bool) []source.Check {
	var out []source.Check

	out = append(out, source.Check{
		ID: "MY-01", Title: "源库在记录「变更流水」（binlog）",
		Status: pick(cdcReady, source.StatusOK, source.StatusBlocked),
		Detail: "log_bin=" + orNA(vars["log_bin"]) + "，binlog_format=" + orNA(vars["binlog_format"]),
		SQL:    "SET GLOBAL log_bin = ON; -- 需要写进 my.cnf 并重启 mysqld",
		Who:    "dba", Code: "E1001",
	})
	out = append(out, source.Check{
		ID: "MY-02", Title: "每一行变更都带完整的前后镜像",
		Status: pick(rowImageFull, source.StatusOK, source.StatusAutofixable),
		Detail: "binlog_row_image=" + orNA(vars["binlog_row_image"]),
		SQL:    "SET GLOBAL binlog_row_image = 'FULL';",
		Undo:   "SET GLOBAL binlog_row_image = '" + orNA(vars["binlog_row_image"]) + "';",
		Who:    "dba", Code: "E1001",
	})
	out = append(out, source.Check{
		ID: "MY-03", Title: "账号能读变更流水",
		Status: pick(canReplicate, source.StatusOK, source.StatusAutofixable),
		Detail: grantsSummary(grants),
		SQL:    "GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '<账号>'@'<来源>';",
		Undo:   "REVOKE REPLICATION SLAVE, REPLICATION CLIENT ON *.* FROM '<账号>'@'<来源>';",
		Who:    "dba", Code: "E1003",
	})
	if strings.EqualFold(vars["binlog_row_value_options"], "PARTIAL_JSON") {
		out = append(out, source.Check{
			ID: "MY-04", Title: "JSON 列改成了只发差异",
			Status: source.StatusAutofixable,
			Detail: "binlog_row_value_options=PARTIAL_JSON，更新事件里只有 JSON 差异，我们读不出整行",
			SQL:    "SET GLOBAL binlog_row_value_options = '';",
			Undo:   "SET GLOBAL binlog_row_value_options = 'PARTIAL_JSON';",
			Who:    "dba", Code: "E4001",
		})
	}
	if res.StatusStatement == "" {
		out = append(out, source.Check{
			ID: "MY-05", Title: "读不到当前 binlog 位点",
			Status: source.StatusWarn,
			Detail: "SHOW BINARY LOG STATUS 和 SHOW MASTER STATUS 都没成功（多半是权限）",
			SQL:    "SHOW GRANTS; -- 需要的权限见 MY-03",
			Who:    "dba", Code: "E1003",
		})
	}
	retention := vars["binlog_expire_logs_seconds"]
	if retention == "" {
		if d := vars["expire_logs_days"]; d != "" {
			retention = d + " 天（旧变量 expire_logs_days）"
		}
	}
	if retention != "" {
		out = append(out, source.Check{
			ID: "MY-06", Title: "变更流水保留得够久（断线后还能接上）",
			Status: source.StatusWarn,
			Detail: "保留 " + retention + "；断线超过这个时长就得重新搬全量",
			SQL:    "SET GLOBAL binlog_expire_logs_seconds = 604800;",
			Who:    "dba", Code: "E3001",
		})
	}
	if v, ok := vars["gtid_mode"]; ok && !isOn(v) {
		out = append(out, source.Check{
			ID: "MY-07", Title: "主从切换后位点仍能对上（GTID）",
			Status: source.StatusWarn,
			Detail: "gtid_mode=" + v + "，位点只能靠文件名+偏移",
			SQL: "SET GLOBAL enforce_gtid_consistency = ON; SET GLOBAL gtid_mode = OFF_PERMISSIVE; " +
				"SET GLOBAL gtid_mode = ON_PERMISSIVE; SET GLOBAL gtid_mode = ON;",
			Undo: "SET GLOBAL gtid_mode = OFF_PERMISSIVE; SET GLOBAL gtid_mode = OFF;",
			Who:  "dba", Code: "E3001",
		})
	}
	if strings.EqualFold(res.Server.Flavor, "mariadb") {
		out = append(out, source.Check{
			// MY-08 在 docs/SPEC.md §3「MySQL」是「表的存储引擎**有没有事务**」
			//（判据 = 服务器自报的 information_schema.ENGINES.TRANSACTIONS，
			// 降级照跑；唯一入口 preflight.JudgeEngines，本文件不参与——Prober 只有
			// 服务器级变量与 grant，拿不到 Catalog.Table.Engine）。
			// 这条 MariaDB 检查之前错占了 MY-08；MY-09/MY-10 也已留给 SPEC 的
			// 认证插件 / 主从角色，所以用下一个空号 MY-11。
			ID: "MY-11", Title: "这是 MariaDB，本期不支持",
			Status: source.StatusBlocked,
			Detail: "MariaDB 的 binlog 事件编号与 GTID 格式和 MySQL 不同",
			Who:    "you", Code: "E1001",
		})
	}
	return out
}

func (p *Prober) version(ctx context.Context) (ServerInfo, error) {
	var v string
	if err := p.q.QueryRowContext(ctx, "SELECT VERSION()").Scan(&v); err != nil {
		return ServerInfo{}, fmt.Errorf("读服务器版本失败: %w", err)
	}
	info := ServerInfo{Version: v, Flavor: "mysql"}
	lower := strings.ToLower(v)
	switch {
	case strings.Contains(lower, "mariadb"):
		info.Flavor = "mariadb"
	case strings.Contains(lower, "percona"):
		info.Flavor = "percona"
	}
	parts := strings.FieldsFunc(v, func(r rune) bool { return r == '.' || r == '-' || r == '+' })
	if len(parts) >= 3 {
		info.Major, _ = strconv.Atoi(parts[0])
		info.Minor, _ = strconv.Atoi(parts[1])
		info.Patch, _ = strconv.Atoi(strings.TrimFunc(parts[2], func(r rune) bool { return r < '0' || r > '9' }))
	}
	return info, nil
}

// readVar reads one global variable; ok=false when the server has no such
// variable (renamed or removed in a newer family).
func (p *Prober) readVar(ctx context.Context, name string) (string, bool) {
	var v sql.NullString
	// 变量名来自代码里的常量列表，不接受外部输入。
	row := p.q.QueryRowContext(ctx, "SELECT @@"+name)
	if err := row.Scan(&v); err != nil {
		return "", false
	}
	if !v.Valid {
		return "", true
	}
	return v.String, true
}

// probeStatusStatement finds out which of the two names this server accepts.
// 8.4 removed SHOW MASTER STATUS; 5.7/8.0 do not know SHOW BINARY LOG STATUS.
func (p *Prober) probeStatusStatement(ctx context.Context) (string, error) {
	for _, stmt := range []string{"SHOW BINARY LOG STATUS", "SHOW MASTER STATUS"} {
		rows, err := p.q.QueryContext(ctx, stmt)
		if err != nil {
			continue
		}
		for rows.Next() {
			// 只需要「这条语句能不能跑」，内容由 Status() 再读一次。
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			continue
		}
		rows.Close()
		return stmt, nil
	}
	return "", fmt.Errorf("源库既不认 SHOW BINARY LOG STATUS 也不认 SHOW MASTER STATUS（多半是权限不够）")
}

func (p *Prober) grants(ctx context.Context) (string, error) {
	rows, err := p.q.QueryContext(ctx, "SHOW GRANTS")
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var b strings.Builder
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return "", err
		}
		b.WriteString(line)
		b.WriteByte('\n')
	}
	return b.String(), rows.Err()
}

func grantsAllowReplication(grants string) bool {
	up := strings.ToUpper(grants)
	// 授权名一直是 REPLICATION SLAVE（改名的是 SHOW REPLICA STATUS 这类语句，
	// 实测 9.x 上 GRANT REPLICATION REPLICA 仍是语法错误）。这里额外认一下
	// REPLICATION REPLICA，纯粹是防御：某些托管服务/代理会把 SHOW GRANTS 改写成
	// 新术语，不能因此误判「没权限」。
	return strings.Contains(up, "REPLICATION SLAVE") ||
		strings.Contains(up, "REPLICATION REPLICA") ||
		strings.Contains(up, "REPLICATION CLIENT") ||
		strings.Contains(up, "ALL PRIVILEGES")
}

func hasPrivilege(grants, name string) bool {
	up := strings.ToUpper(grants)
	return strings.Contains(up, name) || strings.Contains(up, "ALL PRIVILEGES")
}

func pick(cond bool, yes, no source.CheckStatus) source.CheckStatus {
	if cond {
		return yes
	}
	return no
}

func orNA(v string) string {
	if v == "" {
		return "(读不到)"
	}
	return v
}

func yesNo(b bool) string {
	if b {
		return "有"
	}
	return "没有"
}

func isOn(v string) bool {
	switch strings.ToUpper(strings.TrimSpace(v)) {
	case "1", "ON", "TRUE", "YES":
		return true
	}
	return false
}

func grantsSummary(grants string) string {
	if strings.TrimSpace(grants) == "" {
		return "读不到授权信息"
	}
	lines := strings.Split(strings.TrimSpace(grants), "\n")
	if len(lines) > 3 {
		lines = lines[:3]
	}
	return strings.Join(lines, " / ")
}
