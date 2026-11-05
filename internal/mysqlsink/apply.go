package mysqlsink

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/spencerswagger/jinliang/internal/source"
)

// Apply 实现 sink.Sink：一个批次 = 一个目标端事务 = 数据 + 位点，原子提交。
//
// 批次里的行按 `(表, 插入/更新)` 分组做多行 upsert；删除按主键逐行（多行 DELETE 的
// 参数顺序更难保证正确，收益也小）；DDL 过白名单后立即执行。
func (s *Sink) Apply(ctx context.Context, evs []source.Event, pos source.Position) error {
	// 契约不变量先机械校验一遍：事务不配对、以半截事务结尾的批次一律不收。
	// 引擎也会校验；这里再挡一次是因为「位点与数据同事务」的代价太大，宁可多一道。
	if err := source.ValidateBatch(evs); err != nil {
		return fmt.Errorf("拒绝写入：批次不满足事务边界不变量: %w", err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("开目标端事务失败: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	// 同一张表连续的插入/更新合并成一条多行 upsert；顺序按源顺序保留。
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
			if err = s.applyUpserts(ctx, tx, evs[i:j]); err != nil {
				return err
			}
			i = j
			continue
		case source.EvDelete:
			if err = s.applyDelete(ctx, tx, ev); err != nil {
				return err
			}
		case source.EvTruncate:
			if _, err = tx.ExecContext(ctx, "TRUNCATE TABLE "+qualified(ev.Table)); err != nil {
				return fmt.Errorf("清空 %s 失败: %w", qualified(ev.Table), err)
			}
		case source.EvDDL:
			if err = s.applyDDL(ctx, tx, ev); err != nil {
				return err
			}
		}
		i++
	}

	// 位点与数据同事务（docs/STATE-SCHEMA.md §1）。半个事务的批次由引擎给「上一个
	// 已提交位点」；这里只在批次以 EvCommit 收尾时推进，否则原样保留。
	if pos != nil && (len(evs) == 0 || endsWithCommit(evs)) {
		if err = s.writePosition(ctx, tx, pos.String()); err != nil {
			return err
		}
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("提交目标端事务失败: %w", err)
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

// applyDDL 走白名单：allow 执行、skip 跳过、confirm/reject 停下。
func (s *Sink) applyDDL(ctx context.Context, tx *sql.Tx, ev source.Event) error {
	d := s.classify(ctx, ev.DDL)
	switch d.Class {
	case DDLAllow:
		if _, err := tx.ExecContext(ctx, d.SQL); err != nil {
			return fmt.Errorf("跟随 DDL 失败（%s）：%w", d.Reason, err)
		}
		return nil
	case DDLSkip:
		return nil
	case DDLConfirm:
		if s.opt.AutoConfirmDDL {
			if _, err := tx.ExecContext(ctx, d.SQL); err != nil {
				return fmt.Errorf("执行需确认的 DDL 失败（%s）：%w", d.Reason, err)
			}
			return nil
		}
		return fmt.Errorf("DDL 需要确认后才跟随：%s。原语句：%s。"+
			"你能做的：① 在源端暂缓这类变更；② 确认后手工在目标端执行同样的语句，再重跑本任务；"+
			"③ 确实要我们自动跟随，把 content.schema_follow.auto_confirm 打开（会失去人工把关）。"+
			"风险：删列/加唯一索引会改变目标端数据或 upsert 判据，自动执行后无法回滚",
			d.Reason, ev.DDL)
	default:
		return fmt.Errorf("DDL 不在白名单，已停在目标端写入之前：%s。原语句：%s。"+
			"你能做的：① 把这条变更拆成白名单内的形态（加列/放宽长度/加索引）；"+
			"② 或者先在源端与目标端各自手工完成结构变更，再重跑；"+
			"③ 该表不再同步就把 content.tables.<表> 关掉。"+
			"风险：我们不会照搬这类 DDL，也不会静默跳过——继续跑会让后续行按错误结构写入",
			d.Reason, ev.DDL)
	}
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
