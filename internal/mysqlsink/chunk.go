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
		"VALUES (?,?,?,NULL,NULL,1,?) ON DUPLICATE KEY UPDATE done=1"
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
