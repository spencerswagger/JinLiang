package mysqlsink

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/spencerswagger/jinliang/internal/catalog"
	"github.com/spencerswagger/jinliang/internal/sink"
	"github.com/spencerswagger/jinliang/internal/source"
)

// metaDDL 是运行时库（目标端 `_jl_*`）里本 package 需要的那几张表。
// 与 docs/STATE-SCHEMA.md §1「MySQL」一致；这里是幂等建表，绝不改用户表。
var metaDDL = []string{
	"CREATE TABLE IF NOT EXISTS `_jl_meta` (" +
		"job_key char(10) NOT NULL PRIMARY KEY," +
		"name varchar(128) NOT NULL," +
		"instance_id varchar(32) NOT NULL," +
		"schema_version int NOT NULL DEFAULT 1," +
		"job_hash char(64) NOT NULL DEFAULT ''," +
		"schema_hash char(64) NULL," +
		"phase varchar(16) NOT NULL," +
		"position text NULL," +
		"watermark text NULL," +
		"snapshot_id varchar(128) NULL," +
		// 按表暂停（契约 §1.4）：一列 JSON 文本，key = 目标端裸表名。
		// 老库用 ensurePausedColumn 补列——MySQL 没有 ADD COLUMN IF NOT EXISTS，只能先查再 ALTER。
		"paused_tables text NULL," +
		"started_at datetime(6) NOT NULL," +
		"updated_at datetime(6) NOT NULL" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
	"CREATE TABLE IF NOT EXISTS `_jl_lease` (" +
		"job_key char(10) NOT NULL PRIMARY KEY," +
		"instance_id varchar(32) NOT NULL," +
		"label varchar(96) NULL," +
		"expires_at datetime(6) NOT NULL" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
	"CREATE TABLE IF NOT EXISTS `_jl_object` (" +
		"object_name varchar(191) NOT NULL PRIMARY KEY," +
		"object_kind varchar(32) NOT NULL," +
		"job_key char(10) NOT NULL," +
		"instance_id varchar(32) NOT NULL," +
		"created_at datetime(6) NOT NULL," +
		"dropped_at datetime(6) NULL" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
	"CREATE TABLE IF NOT EXISTS `_jl_chunk` (" +
		"job_key char(10) NOT NULL," +
		"table_name varchar(191) NOT NULL," +
		"chunk_no int NOT NULL," +
		"range_from text NULL," +
		"range_to text NULL," +
		"done tinyint(1) NOT NULL DEFAULT 0," +
		"`rows` bigint NOT NULL DEFAULT 0," +
		"PRIMARY KEY (job_key, table_name, chunk_no)" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
	"CREATE TABLE IF NOT EXISTS `_jl_instance` (" +
		"instance_id varchar(32) NOT NULL PRIMARY KEY," +
		"label varchar(64) NULL," +
		"host varchar(255) NULL," +
		"first_seen datetime(6) NOT NULL," +
		"last_seen datetime(6) NOT NULL" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
}

// EnsureSchema 实现 sink.Sink：按 on_exists 三策略把表准备好，并保证 `_jl_*` 存在。
//
// 三策略语义（与 PG 侧同名同义）：
//   - error：撞表就停（E1002），防误删；
//   - recreate：先 DROP 再建——**会清空目标表**，动作里带 undo 提示；
//   - append：用已存在的表，只补缺失的列。**源端删过的行会在目标端残留**——
//     这是 append 的语义选择（我们不去 TRUNCATE 用户的表来"对齐"），写进动作说明。
func (s *Sink) EnsureSchema(ctx context.Context, cat source.Catalog) ([]sink.Action, error) {
	var actions []sink.Action

	if err := s.ensureStateTables(ctx); err != nil {
		return actions, err
	}

	exists, err := s.tableExists(ctx, catScope(cat))
	if err != nil {
		return actions, err
	}
	_ = exists

	for _, t := range cat.Tables {
		if t.Table == "" {
			continue
		}
		ok, err := s.tableExists(ctx, t.TableRef)
		if err != nil {
			return actions, err
		}
		switch {
		case !ok:
			if err := s.createTable(ctx, t); err != nil {
				return actions, err
			}
			// 登记所有权：只有「本任务建的表」才允许在重搬前清空（与 pgsink 同构）。
			// 不记的话引擎分不清「我们的表」和「用户复用的表」，重搬时会一律停下——
			// 或者更糟：一律清掉用户的表。
			if err := s.RecordObject(ctx, s.tableObjectName(t.TableRef), "table"); err != nil {
				return actions, fmt.Errorf("登记表 %s 的所有权失败: %w", qualified(t.TableRef), err)
			}
			actions = append(actions, sink.Action{
				ID: "create:" + t.Table, What: "建表 " + qualified(t.TableRef),
				Reason: "目标端原本没有这张表", Applied: true,
				Undo: "DROP TABLE " + qualified(t.TableRef),
			})
		case s.opt.OnExists == OnExistsError:
			return actions, fmt.Errorf("%w：%s 已存在。你能做的：① 改 target.on_exists: append（保留已有数据，源端删过的行会残留）；"+
				"② 改 recreate（先清空再建，⚠ 会丢目标端已有数据）；③ 换一个目标 schema。"+
				"风险：recreate 会清空目标表，append 会让源端已删的行留在目标端",
				ErrTargetTableExists, qualified(t.TableRef))
		case s.opt.OnExists == OnExistsRecreate:
			if _, err := s.db.ExecContext(ctx, "DROP TABLE "+qualified(t.TableRef)); err != nil {
				return actions, fmt.Errorf("按 on_exists=recreate 清空 %s 失败: %w", qualified(t.TableRef), err)
			}
			if err := s.createTable(ctx, t); err != nil {
				return actions, err
			}
			actions = append(actions, sink.Action{
				ID: "recreate:" + t.Table, What: "清空并重建 " + qualified(t.TableRef),
				Reason: "on_exists=recreate：先 DROP 再建", Applied: true,
				Undo: "无法撤销：目标端原有数据已被清空（请从备份恢复）",
			})
		default: // append
			added, err := s.addMissingColumns(ctx, t)
			if err != nil {
				return actions, err
			}
			actions = append(actions, sink.Action{
				ID: "append:" + t.Table, What: fmt.Sprintf("沿用已有表 %s（补了 %d 列）", qualified(t.TableRef), added),
				Reason:  "on_exists=append：不清表；源端删过的行会在目标端残留（语义选择，不是缺陷）",
				Applied: true, Undo: "无（未改动已有数据）",
			})
		}
	}
	return actions, nil
}

// ensureStateTables 建运行时库（幂等）。放在 Open 里而不是只放在 EnsureSchema：
// 引擎拿租约/读位点可能早于任何 EnsureSchema 调用（崩溃接管就是这种），
// 那时表还不存在会让「有没有人在跑」这个问题直接报错。
func (s *Sink) ensureStateTables(ctx context.Context) error {
	for _, ddl := range metaDDL {
		if _, err := s.db.ExecContext(ctx, ddl); err != nil {
			return fmt.Errorf("建运行时表失败: %w", err)
		}
	}
	return nil
}

func catScope(cat source.Catalog) source.TableRef {
	if len(cat.Tables) > 0 {
		return source.TableRef{Schema: cat.Tables[0].Schema}
	}
	return source.TableRef{}
}

func (s *Sink) tableExists(ctx context.Context, t source.TableRef) (bool, error) {
	if t.Table == "" {
		return false, nil
	}
	var n int
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`,
		s.schemaOf(t), t.Table).Scan(&n)
	if err != nil {
		return false, fmt.Errorf("查 %s 是否存在失败: %w", qualified(t), err)
	}
	return n > 0, nil
}

// createTable 用 catalog 建一张 MySQL 表。
func (s *Sink) createTable(ctx context.Context, t source.Table) error {
	if len(t.Columns) == 0 {
		return fmt.Errorf("%s 没有任何列定义，拒绝建一张空表", qualified(t.TableRef))
	}
	var defs []string
	for _, c := range t.Columns {
		typ := s.targetType(c)
		def := "  " + quoteIdent(c.Name) + " " + typ
		if !c.Nullable {
			def += " NOT NULL"
		} else {
			def += " NULL"
		}
		defs = append(defs, def)
	}
	if len(t.PrimaryKey) > 0 {
		var pk []string
		for _, c := range t.PrimaryKey {
			pk = append(pk, quoteIdent(c))
		}
		defs = append(defs, "  PRIMARY KEY ("+strings.Join(pk, ",")+")")
	}
	stmt := fmt.Sprintf("CREATE TABLE %s (\n%s\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
		qualified(t.TableRef), strings.Join(defs, ",\n"))
	if _, err := s.db.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("建表 %s 失败: %w", qualified(t.TableRef), err)
	}
	return nil
}

// addMissingColumns 给已存在的表补列（append 策略）。
func (s *Sink) addMissingColumns(ctx context.Context, t source.Table) (int, error) {
	have := map[string]bool{}
	rows, err := s.db.QueryContext(ctx, `
		SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`,
		s.schemaOf(t.TableRef), t.Table)
	if err != nil {
		return 0, err
	}
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			rows.Close()
			return 0, err
		}
		have[strings.ToLower(c)] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	added := 0
	for _, c := range t.Columns {
		if have[strings.ToLower(c.Name)] {
			continue
		}
		stmt := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", qualified(t.TableRef), quoteIdent(c.Name), s.targetType(c))
		if c.Nullable {
			stmt += " NULL"
		} else {
			// 已有数据的表加 NOT NULL 列必须给默认值，否则 MySQL 会拿隐式零值填旧行。
			stmt += " NOT NULL DEFAULT " + zeroLiteral(c)
		}
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return added, fmt.Errorf("给 %s 补列 %s 失败: %w", qualified(t.TableRef), c.Name, err)
		}
		added++
	}
	return added, nil
}

// targetType 把源类型映射成 MySQL 类型（字典归 Ark，这里只调用不修改）。
func (s *Sink) targetType(c source.Column) string {
	from := s.opt.SourceKind
	if from == "" {
		from = catalog.MySQL
	}
	if m, ok := catalog.Map(from, catalog.MySQL, c.Type); ok && m.Target != "" {
		return m.Target
	}
	// 字典没覆盖：按文本兜底（契约：不拒绝，提示）。调用方会在 Preflight 里看到提示。
	return "longtext"
}

func zeroLiteral(c source.Column) string {
	base, _ := splitType(c.Type)
	switch base {
	case "int", "integer", "bigint", "smallint", "tinyint", "mediumint", "decimal", "numeric", "float", "double":
		return "0"
	case "bool", "boolean":
		return "0"
	case "date":
		return "'1970-01-01'"
	case "timestamp", "datetime":
		return "'1970-01-01 00:00:00'"
	}
	return "''"
}

// Preflight 给出「只提示不阻断」的预览：utf8mb4 行宽/索引长度警告。
func (s *Sink) Preflight(ctx context.Context, cat source.Catalog) (source.Report, error) {
	checks := s.widthChecks(ctx, cat)
	return source.Report{Checks: checks, CanStart: true}, nil
}

// MetaPosition 读回已落库的位点（空串表示还没有）。
func (s *Sink) MetaPosition(ctx context.Context) (string, error) {
	var p sql.NullString
	err := s.db.QueryRowContext(ctx, "SELECT position FROM _jl_meta WHERE job_key = ?", s.opt.JobKey).Scan(&p)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("读位点失败: %w", err)
	}
	return p.String, nil
}
