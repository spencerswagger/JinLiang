package mysqlsink

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/spencerswagger/jinliang/internal/sink"
	"github.com/spencerswagger/jinliang/internal/source"
)

// Apply 实现 sink.Sink。
//
// 一个批次通常是「一个目标端事务 = 数据 + 位点」。但 **MySQL 的 DDL 与 TRUNCATE 会隐式
// 提交**：把它们塞进同一个 tx，已累积的 INSERT 会被顺手提交掉、`tx.Commit()` 退化成空操作，
// 「数据 + 位点同事务」在有 DDL 的批次里就不成立了；更糟的是重放会再执行一次 CREATE INDEX /
// ADD COLUMN，直接 `Error 1061/1060` 永久卡死在该批次。
//
// 所以分两种走法：
//   - 批次里没有 DDL/TRUNCATE：单事务，数据 + 位点一起提交（最强保证）；
//   - 有：先提交数据（带**上一个已提交位点**）、再单独执行 DDL（执行前先判是否已存在，
//     幂等跳过）、最后单独写新位点。顺序是「数据先落、位点后落」——中间崩最多重放数据
//     （upsert 幂等）或重跑一次幂等的 DDL，绝不重复建索引。
func (s *Sink) Apply(ctx context.Context, evs []source.Event, pos source.Position) error {
	// 契约不变量先机械校验一遍：事务不配对、以半截事务结尾的批次一律不收。
	if err := source.ValidateBatch(evs); err != nil {
		return fmt.Errorf("拒绝写入：批次不满足事务边界不变量: %w", err)
	}

	advance := pos != nil && (len(evs) == 0 || endsWithCommit(evs))

	// **最后一道防线**（契约 §1.5 第 1 条）：判定权在引擎，写路径不做暂停判定。
	// 但万一引擎那边漏了（表名口径变了、新写路径没包 pauseSink），这里要立刻炸——
	// 而不是带着新关系往一张结构没对齐的表里写。只查内存集合，不产生 IO。
	// pausedCache 为空（没调过 LoadPausedTables）时这段是空转，不影响别的路径。
	for _, ev := range evs {
		if _, paused := s.pausedCache[ev.Table.Table]; !paused {
			continue
		}
		// 整表重搬窗口内**故意**写这张表（契约 §1.6 步 3）：记录要留到搬成才解除，
		// 所以这里按上下文放行，而不是要求引擎先删记录（那会打开崩溃窗口）。
		if sink.InRecopy(ctx, ev.Table.Table) {
			continue
		}
		return fmt.Errorf("引擎没有过滤掉暂停表 %s 的事件（内部错误：按表暂停的判定权在引擎，"+
			"不该把这张表的行交到这里）", ev.Table.Table)
	}

	if !batchHasImplicitCommit(evs) {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("开目标端事务失败: %w", err)
		}
		fail := true
		defer func() {
			if fail {
				_ = tx.Rollback()
			}
		}()
		if err := s.applyData(ctx, tx, evs); err != nil {
			return err
		}
		if advance {
			if err := s.writePosition(ctx, tx, pos.String()); err != nil {
				return err
			}
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("提交目标端事务失败: %w", err)
		}
		fail = false
		return nil
	}

	// —— 含隐式提交语句：拆成三个独立事务 ——
	if err := s.commitDataWithPreviousPosition(ctx, evs); err != nil {
		return err
	}
	if err := s.applyImplicitCommitStatements(ctx, evs); err != nil {
		return err
	}
	if advance {
		return s.commitPosition(ctx, pos.String())
	}
	return nil
}

// batchHasImplicitCommit 报告批次里是否含 DDL / TRUNCATE（它们会隐式提交）。
func batchHasImplicitCommit(evs []source.Event) bool {
	for _, ev := range evs {
		if ev.Kind == source.EvDDL || ev.Kind == source.EvTruncate {
			return true
		}
	}
	return false
}

// commitDataWithPreviousPosition 把数据落库，位点写**上一个已提交位点**（不变）。
func (s *Sink) commitDataWithPreviousPosition(ctx context.Context, evs []source.Event) error {
	prev, err := s.MetaPosition(ctx)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("开目标端事务失败: %w", err)
	}
	fail := true
	defer func() {
		if fail {
			_ = tx.Rollback()
		}
	}()
	if err := s.applyData(ctx, tx, evs); err != nil {
		return err
	}
	if prev != "" {
		if err := s.writePosition(ctx, tx, prev); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("提交数据失败: %w", err)
	}
	fail = false
	return nil
}

// applyImplicitCommitStatements 单独执行 DDL/TRUNCATE；已生效的直接跳过（幂等）。
func (s *Sink) applyImplicitCommitStatements(ctx context.Context, evs []source.Event) error {
	for _, ev := range evs {
		switch ev.Kind {
		case source.EvTruncate:
			if _, err := s.db.ExecContext(ctx, "TRUNCATE TABLE "+qualified(ev.Table)); err != nil {
				return fmt.Errorf("清空 %s 失败: %w", qualified(ev.Table), err)
			}
		case source.EvDDL:
			if err := s.applyDDL(ctx, ev.DDL); err != nil {
				return err
			}
		}
	}
	return nil
}

// commitPosition 把位点单独落库（DDL 之后的最后一步）。
func (s *Sink) commitPosition(ctx context.Context, position string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("开位点事务失败: %w", err)
	}
	if err := s.writePosition(ctx, tx, position); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("提交位点失败: %w", err)
	}
	return nil
}

// applyData 写行（插入/更新/删除），不含任何会隐式提交的语句。
func (s *Sink) applyData(ctx context.Context, tx *sql.Tx, evs []source.Event) error {
	i := 0
	for i < len(evs) {
		ev := evs[i]
		switch ev.Kind {
		case source.EvInsert, source.EvUpdate:
			j := i
			for j < len(evs) && (evs[j].Kind == source.EvInsert || evs[j].Kind == source.EvUpdate) &&
				evs[j].Table == ev.Table {
				j++
			}
			if err := s.applyUpserts(ctx, tx, evs[i:j]); err != nil {
				return err
			}
			i = j
			continue
		case source.EvDelete:
			if err := s.applyDelete(ctx, tx, ev); err != nil {
				return err
			}
		}
		i++
	}
	return nil
}

func endsWithCommit(evs []source.Event) bool {
	for i := len(evs) - 1; i >= 0; i-- {
		if evs[i].Kind == source.EvHeartbeat {
			continue
		}
		return evs[i].Kind == source.EvCommit
	}
	return false
}

// applyUpserts 把连续的同表插入/更新合并成一条多行 upsert。
func (s *Sink) applyUpserts(ctx context.Context, tx *sql.Tx, evs []source.Event) error {
	table := evs[0].Table
	cols, updateCols, err := s.columnsFor(ctx, table, evs[0].Relation, evs[0].New)
	if err != nil {
		return err
	}
	ok, err := s.hasUniqueKey(ctx, table)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%w：%s。三种做法任选：① 给目标表加主键/唯一索引（推荐，与源端一致）；"+
			"② 用 content.tables.<表>.columns 显式指定业务唯一键；③ 把该表设成 insert-only 并接受重放可能重复（不推荐）。"+
			"风险：加唯一索引需要目标端已有数据无重复，建索引期间会短暂阻塞写入",
			ErrNoUniqueKey, qualified(table))
	}

	stmt := s.upsertSQL(table, cols, len(evs), updateCols)
	args := make([]any, 0, len(evs)*len(cols))
	for _, ev := range evs {
		if len(ev.New.Values) != len(cols) {
			return fmt.Errorf("%s 的行有 %d 列，表定义是 %d 列（源端结构变了？）", qualified(table), len(ev.New.Values), len(cols))
		}
		for _, v := range ev.New.Values {
			args = append(args, driverValue(v))
		}
	}
	if _, err := tx.ExecContext(ctx, stmt, args...); err != nil {
		return fmt.Errorf("写入 %s 失败（%d 行）: %w", qualified(table), len(evs), err)
	}
	return nil
}

func (s *Sink) applyDelete(ctx context.Context, tx *sql.Tx, ev source.Event) error {
	pk := primaryKeyOf(ev)
	stmt, err := deleteSQL(ev.Table, pk)
	if err != nil {
		return err
	}
	args := make([]any, 0, len(pk))
	for _, name := range pk {
		idx, ok := columnIndex(ev, name)
		if !ok {
			return fmt.Errorf("删除 %s 时找不到主键列 %s 的值", qualified(ev.Table), name)
		}
		args = append(args, driverValue(ev.Old.Values[idx]))
	}
	if _, err := tx.ExecContext(ctx, stmt, args...); err != nil {
		return fmt.Errorf("删除 %s 失败: %w", qualified(ev.Table), err)
	}
	return nil
}

// classify 是「带目标端现状」的白名单判定：MODIFY/CHANGE 要读目标端现有类型，
// 才能分清「放宽」（可跟随）与「改窄」（会截断）。
func (s *Sink) classify(ctx context.Context, stmt string) DDLDecision {
	d := ClassifyDDL(stmt)
	if d.Class != DDLConfirm || d.Column == "" || d.NewType == "" {
		return d
	}
	var oldType string
	err := s.db.QueryRowContext(ctx, `
		SELECT COLUMN_TYPE FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?`,
		s.schemaOf(d.Table), d.Table.Table, d.Column).Scan(&oldType)
	if err != nil {
		return DDLDecision{Class: DDLConfirm, Table: d.Table, Column: d.Column, NewType: d.NewType, SQL: d.SQL,
			Reason: fmt.Sprintf("读不到 %s.%s 现有列类型，无法判断是放宽还是改窄", d.Table.Table, d.Column)}
	}
	decided := DecideColumnChange(oldType, d.NewType)
	decided.Table, decided.Column, decided.NewType, decided.SQL = d.Table, d.Column, d.NewType, d.SQL
	return decided
}

// applyDDL 走白名单并**独立执行**（DDL 隐式提交，不能塞进数据事务）。
//
// 幂等：执行前先查目标端是否已经生效（列/索引/类型），已生效就跳过——
// 这样「DDL 之后、位点写入之前崩溃 → 重放」不会再撞 1061/1060。
func (s *Sink) applyDDL(ctx context.Context, stmt string) error {
	d := s.classify(ctx, stmt)
	switch d.Class {
	case DDLAllow:
		done, err := s.ddlAlreadyApplied(ctx, d)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		if _, err := s.db.ExecContext(ctx, d.SQL); err != nil {
			return fmt.Errorf("跟随 DDL 失败（%s）：%w", d.Reason, err)
		}
		// 结构变了：唯一键缓存必须失效，否则加完唯一索引仍按「没有唯一键」报错。
		if d.Table.Table != "" {
			delete(s.uniqueCache, s.schemaOf(d.Table)+"."+d.Table.Table)
		}
		return nil
	case DDLSkip:
		return nil
	case DDLConfirm:
		if !s.opt.AutoConfirmDDL {
			return fmt.Errorf("DDL 需要确认后才跟随：%s。原语句：%s。"+
				"你能做的：① 在源端暂缓这类变更；② 确认后手工在目标端执行同样的语句，再重跑本任务；"+
				"③ 确实要我们自动跟随，把 content.schema_follow.auto_confirm 打开（会失去人工把关）。"+
				"风险：删列/加唯一索引会改变目标端数据或 upsert 判据，自动执行后无法回滚",
				d.Reason, stmt)
		}
		if _, err := s.db.ExecContext(ctx, d.SQL); err != nil {
			return fmt.Errorf("执行需确认的 DDL 失败（%s）：%w", d.Reason, err)
		}
		if d.Table.Table != "" {
			delete(s.uniqueCache, s.schemaOf(d.Table)+"."+d.Table.Table)
		}
		return nil
	default:
		return fmt.Errorf("DDL 不在白名单，已停在目标端写入之前：%s。原语句：%s。"+
			"你能做的：① 把这条变更拆成白名单内的形态（加列/放宽长度/加索引）；"+
			"② 或者先在源端与目标端各自手工完成结构变更，再重跑；"+
			"③ 该表不再同步就把 content.tables.<表> 关掉。"+
			"风险：我们不会照搬这类 DDL，也不会静默跳过——继续跑会让后续行按错误结构写入",
			d.Reason, stmt)
	}
}

// ddlAlreadyApplied 判断一条 allow 类 DDL 是否已经在目标端生效。
func (s *Sink) ddlAlreadyApplied(ctx context.Context, d DDLDecision) (bool, error) {
	if d.Table.Table == "" {
		return false, nil
	}
	schema := s.schemaOf(d.Table)
	switch {
	case d.Column != "" && d.NewType != "":
		// MODIFY/CHANGE：目标端类型已经等于目标类型 → 已生效。
		var cur string
		err := s.db.QueryRowContext(ctx, `
			SELECT COLUMN_TYPE FROM information_schema.COLUMNS
			WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?`,
			schema, d.Table.Table, d.Column).Scan(&cur)
		if err == sql.ErrNoRows {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		return strings.EqualFold(normalizeTypeText(cur), normalizeTypeText(d.NewType)), nil
	case d.Column != "":
		// ADD COLUMN：列已存在 → 跳过。
		var n int
		if err := s.db.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM information_schema.COLUMNS
			WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?`,
			schema, d.Table.Table, d.Column).Scan(&n); err != nil {
			return false, err
		}
		return n > 0, nil
	}
	// 索引：按索引名判存在。
	if name := indexNameOf(d.SQL); name != "" {
		var n int
		if err := s.db.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM information_schema.STATISTICS
			WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_NAME = ?`,
			schema, d.Table.Table, name).Scan(&n); err != nil {
			return false, err
		}
		return n > 0, nil
	}
	return false, nil
}

// indexNameOf 从 ADD INDEX / CREATE INDEX 语句里取索引名。
func indexNameOf(stmt string) string {
	fields := strings.Fields(stmt)
	for i, f := range fields {
		switch strings.ToUpper(f) {
		case "INDEX", "KEY":
			// ALTER TABLE … ADD INDEX name (…)  /  CREATE INDEX name ON …
			if i+1 < len(fields) {
				n := trimBackticks(strings.TrimSuffix(fields[i+1], "("))
				if strings.EqualFold(n, "ON") || n == "" {
					return ""
				}
				return n
			}
		}
	}
	return ""
}

func normalizeTypeText(t string) string {
	return strings.Join(strings.Fields(strings.ToLower(t)), " ")
}

// columnsFor 取目标表的列清单与「要更新的列」（排除主键）。
// 优先用事件里的 Relation（源端给的行镜像列定义），没有就查目标端。
func (s *Sink) columnsFor(ctx context.Context, table source.TableRef, rel *source.Table, row source.Row) (cols, updateCols []string, err error) {
	if rel != nil && len(rel.Columns) == len(row.Values) {
		for _, c := range rel.Columns {
			cols = append(cols, c.Name)
		}
		updateCols = exclude(cols, rel.PrimaryKey)
		return cols, updateCols, nil
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT COLUMN_NAME FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION`,
		s.schemaOf(table), table.Table)
	if err != nil {
		return nil, nil, fmt.Errorf("读 %s 的列定义失败: %w", qualified(table), err)
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, nil, err
		}
		cols = append(cols, name)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	if len(cols) != len(row.Values) {
		return nil, nil, fmt.Errorf("%s 目标端有 %d 列，事件里有 %d 个值：结构不一致，先跑 ensure-schema",
			qualified(table), len(cols), len(row.Values))
	}
	pk, err := s.primaryKey(ctx, table)
	if err != nil {
		return nil, nil, err
	}
	return cols, exclude(cols, pk), nil
}

func (s *Sink) primaryKey(ctx context.Context, table source.TableRef) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY'
		ORDER BY ORDINAL_POSITION`, s.schemaOf(table), table.Table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func exclude(all, minus []string) []string {
	if len(minus) == 0 {
		return all
	}
	skip := map[string]bool{}
	for _, m := range minus {
		skip[strings.ToLower(m)] = true
	}
	out := make([]string, 0, len(all))
	for _, c := range all {
		if !skip[strings.ToLower(c)] {
			out = append(out, c)
		}
	}
	return out
}

// primaryKeyOf 取事件表的主键列名（优先 Relation，其次读目标端）。
func primaryKeyOf(ev source.Event) []string {
	if ev.Relation != nil && len(ev.Relation.PrimaryKey) > 0 {
		return ev.Relation.PrimaryKey
	}
	return nil
}

func columnIndex(ev source.Event, name string) (int, bool) {
	if ev.Relation == nil {
		return 0, false
	}
	for i, c := range ev.Relation.Columns {
		if strings.EqualFold(c.Name, name) {
			return i, true
		}
	}
	return 0, false
}

// writePosition 把位点写进 `_jl_meta`（与数据同一个事务）。
func (s *Sink) writePosition(ctx context.Context, tx *sql.Tx, position string) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO _jl_meta (job_key, name, instance_id, schema_version, job_hash, phase, position, started_at, updated_at)
		VALUES (?, ?, ?, 1, ?, 'continuous', ?, NOW(6), NOW(6))
		ON DUPLICATE KEY UPDATE position = VALUES(position), updated_at = NOW(6)`,
		s.opt.JobKey, s.opt.Name, s.opt.InstanceID, s.opt.JobKey, position)
	if err != nil {
		return fmt.Errorf("写位点失败（本次数据不提交）: %w", err)
	}
	return nil
}

func driverValue(v source.Value) any {
	switch v.Kind {
	case source.KindNull:
		return nil
	case source.KindInt:
		return v.I
	case source.KindUint:
		return v.U
	case source.KindFloat:
		return v.F
	case source.KindString:
		return v.S
	case source.KindBytes:
		return v.B
	case source.KindTime:
		return v.T
	case source.KindBool:
		return v.I != 0
	case source.KindJSON:
		// 契约要求 JSON 以文本交付；MySQL 的 json 列接受文本，会自动校验并转成二进制。
		return v.S
	default:
		return v.S
	}
}

var _ = time.Now
