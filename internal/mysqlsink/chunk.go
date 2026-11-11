package mysqlsink

import (
	"context"
	"fmt"

	"github.com/spencerswagger/jinliang/internal/source"
)

// MySQL 目标端的块进度记账（与 pgsink 的 `_jl_chunk` 同构，docs/STATE-SCHEMA.md §1）。
//
// 两个编号空间（与 PG 侧逐字相同，消费者是同一段 planCopy 逻辑）：
//   - 块号从 1 开始，一表一套，右边界是 keyset 游标（`range_to`）；
//   - 块号 0 保留给「这张表的存量搬完了」的完成标记。
//
// 为什么现在才补：MySQL 目标端的通用全量路径（engine.copyGeneric）原来**一块账都不记**——
// 搬完就搬完，进程崩在中途只能整轮重来，「这张表欠一次重搬」也无从表达
// （契约 §1.6 步 2 的 `MarkTableRecopyPending` 要清的就是这张表所有块标记）。
//
// 与 PG 侧的差别（写清楚，免得被当成同一个东西）：
//   - PG 路径把「数据 + 块标记 + 位点」放在**同一个事务**里（BulkApply）；
//     MySQL 目标端这一版是「数据先落、标记后落」两步——崩在中间只会「数据在、标记没写」，
//     下一轮重搬一遍靠 upsert 收敛（不丢数据，多搬一次）。这个代价换的是不用把
//     整个 copyGeneric 改成事务化的批量写。

// TableDoneChunkNo 是「存量已搬完」的保留块号（与 pgsink.TableDoneChunkNo 同值）。
const TableDoneChunkNo = 0

// ChunkProgress 是一张表的存量进度。
type ChunkProgress struct {
	Done   bool
	LastNo int
	Cursor string
	Rows   int64
}

// MarkChunkDone 记下「第 no 块已提交」，右边界是 to（keyset 游标）。
//
// 幂等：同一块重复记（重放/重试）只更新右边界与行数，不新增块号。
func (s *Sink) MarkChunkDone(ctx context.Context, ref source.TableRef, no int, from, to string,
	rows int64) error {
	if no <= 0 {
		return fmt.Errorf("内部错误：块号必须从 1 开始（0 是完成标记），拿到 %d", no)
	}
	q := "INSERT INTO `_jl_chunk` (job_key, table_name, chunk_no, range_from, range_to, done, `rows`) " +
		"VALUES (?,?,?,?,?,1,?) ON DUPLICATE KEY UPDATE range_from=VALUES(range_from), " +
		"range_to=VALUES(range_to), done=1, `rows`=VALUES(`rows`)"
	if _, err := s.db.ExecContext(ctx, q, s.opt.JobKey, ref.Table, no, from, to, rows); err != nil {
		return fmt.Errorf("记 %s 第 %d 块的进度失败: %w", ref.Table, no, err)
	}
	return nil
}

// MarkTableDone 盖「这张表的存量搬完了」（块号 0）。
//
// 单独一步、不跟最后一块绑在一起：崩在两者之间时下一轮从头重搬（幂等），不丢数据。
func (s *Sink) MarkTableDone(ctx context.Context, ref source.TableRef) error {
	var sum int64
	if err := s.db.QueryRowContext(ctx,
		"SELECT COALESCE(SUM(`rows`),0) FROM `_jl_chunk` WHERE job_key = ? AND table_name = ? AND chunk_no > 0",
		s.opt.JobKey, ref.Table).Scan(&sum); err != nil {
		return fmt.Errorf("统计 %s 已搬行数失败: %w", ref.Table, err)
	}
	q := "INSERT INTO `_jl_chunk` (job_key, table_name, chunk_no, range_from, range_to, done, `rows`) " +
		// rows 必须跟着刷新：块重放（续跑重读 / 重搬后再盖一次）会改各块之和，
		// 完成标记若停在旧值，它和 `ChunkProgress.Rows` 就永久脱节——
		// 任何「按已搬行数判断搬没搬完」的逻辑都会被这个陈旧值带偏（该重搬判成已搬完）。
		"VALUES (?,?,?,NULL,NULL,1,?) ON DUPLICATE KEY UPDATE done=1, `rows`=VALUES(`rows`)"
	if _, err := s.db.ExecContext(ctx, q, s.opt.JobKey, ref.Table, TableDoneChunkNo, sum); err != nil {
		return fmt.Errorf("记 %s 的搬完标记失败: %w", ref.Table, err)
	}
	return nil
}

// ChunkProgress 读回本任务每张表的块进度（table → 进度）。
func (s *Sink) ChunkProgress(ctx context.Context) (map[string]ChunkProgress, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT table_name, chunk_no, COALESCE(range_to,''), `rows`, done FROM `_jl_chunk` WHERE job_key = ?",
		s.opt.JobKey)
	if err != nil {
		return nil, fmt.Errorf("读块进度失败: %w", err)
	}
	defer rows.Close()

	out := map[string]ChunkProgress{}
	for rows.Next() {
		var (
			table  string
			no     int
			cursor string
			n      int64
			done   bool
		)
		if err := rows.Scan(&table, &no, &cursor, &n, &done); err != nil {
			return nil, err
		}
		p := out[table]
		if no == TableDoneChunkNo {
			p.Done = true
		} else {
			if no > p.LastNo {
				p.LastNo, p.Cursor = no, cursor
			}
			p.Rows += n
		}
		out[table] = p
	}
	return out, rows.Err()
}

// MarkChunkDoneWithPos 记一块的进度，并在**同一个事务**里把起点位点写进 `_jl_meta.position`。
//
// 为什么必须带上位点（Owl II P0，CI `TestMySQLTargetCopyResumesFromChunkBoundary` 就是这么红的）：
// 通用路径的全量块是用 `Apply(evs, nil)` 写的（不带位点），位点只在**全量结束**时写一次。
// 于是进程被 kill -9 之后目标端「有块进度、没有位点」——`planCopy` 会判「位点缺失 → 从头搬」，
// 块边界根本没被看到，续跑退化成整轮重搬。
//
// PG 目标端没这个问题：`BulkApply` 每一块的事务里就把**同一个起点位点**写进去了。
// 这里对齐那个口径：位点写的是 `pos`（本轮全量的起点，不是「当前进度」），所以
// 「已搬块在停机窗口里的变更」仍然由增量从**原起点**重放补上。
//
// 数据行本身是分小批 upsert 的（一块十万行不能一个事务），重放时从块起点重读、靠 upsert 收敛；
// 这条与 PG 的差别只在粒度，不在正确性方向。
func (s *Sink) MarkChunkDoneWithPos(ctx context.Context, ref source.TableRef, no int,
	from, to string, rows int64, pos string) error {
	if no <= 0 {
		return fmt.Errorf("内部错误：块号必须从 1 开始（0 是完成标记），拿到 %d", no)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("开事务失败: %w", err)
	}
	fail := true
	defer func() {
		if fail {
			_ = tx.Rollback()
		}
	}()
	q := "INSERT INTO `_jl_chunk` (job_key, table_name, chunk_no, range_from, range_to, done, `rows`) " +
		"VALUES (?,?,?,?,?,1,?) ON DUPLICATE KEY UPDATE range_from=VALUES(range_from), " +
		"range_to=VALUES(range_to), done=1, `rows`=VALUES(`rows`)"
	if _, err := tx.ExecContext(ctx, q, s.opt.JobKey, ref.Table, no, from, to, rows); err != nil {
		return fmt.Errorf("记 %s 第 %d 块的进度失败: %w", ref.Table, no, err)
	}
	if pos != "" {
		// 位点只在「还没写过」时落：续跑轮不该把它往前挪（契约 §1.6：续跑不动位点）。
		if _, err := tx.ExecContext(ctx,
			"UPDATE `_jl_meta` SET position = COALESCE(NULLIF(position,''), ?), updated_at = NOW(6) WHERE job_key = ?",
			pos, s.opt.JobKey); err != nil {
			return fmt.Errorf("写全量起点位点失败: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("提交块进度失败: %w", err)
	}
	fail = false
	return nil
}
