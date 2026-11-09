package pgsink

import (
	"context"
	"fmt"

	"github.com/spencerswagger/jinliang/internal/pgwire"
	"github.com/spencerswagger/jinliang/internal/source"
)

// 块级续跑的真源（docs/SNAPSHOT.md §4、docs/STATE-SCHEMA.md §1）。
//
// 一条规矩：**块的边界与位点都写在目标库**，和租约、权威位点同一张 `_jl_*` 状态库，
// 并且和数据在**同一个事务**里提交（BulkApply）。所以「已提交的块」永远是完整的一块：
// 进程被 kill -9 时正在写的那一块整个回滚，不会留下半块，也不会出现「数据在、进度不在」。
//
// 两个编号空间：
//   - 块号从 1 开始，一表一套（`chunk_no`），边界是 keyset 游标（`range_to`）；
//   - 块号 0 保留给「这张表存量已搬完」的完成标记（TableDoneChunkNo）。
//     完成标记不能拿「最后一块」代替：一页数据读空时根本不会产生新块。
//
// 为什么不用「有块标记就算搬完」：一次全量可能只搬了几块就被中断，
// 「有标记」只说明搬过，不说明搬完了——这两件事以前被混成一件，重启只能整轮重搬。

// TableDoneChunkNo is the reserved chunk number that marks "this table's
// initial copy finished". Real chunks start at 1, so 0 is unambiguous.
const TableDoneChunkNo = 0

// ChunkProgress is one table's durable copy progress: where the last committed
// chunk ended and whether the table ever finished.
type ChunkProgress struct {
	// Done 表示这张表的存量搬完了（有完成标记）。
	Done bool
	// LastNo 是最后一个已提交块的编号（0 = 一块都没落地）。
	LastNo int
	// Cursor 是最后一个已提交块的右边界（range_to，keyset 游标）：
	// 续跑就从这里之后读。空 = 从表头读。
	Cursor string
	// Rows 是已提交块的行数合计（不含完成标记）。
	Rows int64
}

// ChunkProgress reads every table's durable progress for this job_key.
//
// 标记行数 = 块数 + 表数，量级是「万」，一次读完再在内存里折起来足够；
// 反过来一表一条 SQL 会把连接往返变成表的数量级。
func (s *Sink) ChunkProgress(ctx context.Context) (map[string]ChunkProgress, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	q := fmt.Sprintf(`SELECT table_name, chunk_no, COALESCE(range_to,''), rows, done
		FROM %s WHERE job_key=$1`, s.stateTable("_jl_chunk"))
	rows, err := s.conn.Query(ctx, q, s.opt.JobKey)
	if err != nil {
		return nil, err
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
			// 完成标记：只认 done，不参与块边界。
			if done {
				p.Done = true
			}
			out[table] = p
			continue
		}
		if !done {
			// 未提交的块本来不该存在（块和数据同事务），保守地当它没搬。
			out[table] = p
			continue
		}
		if no > p.LastNo {
			p.LastNo = no
			p.Cursor = cursor
		}
		p.Rows += n
		out[table] = p
	}
	return out, rows.Err()
}

// MarkTableDone records "this table's initial copy finished".
//
// 单独一个事务，不跟最后一块绑在一起：崩在两者之间时，续跑会从上次的游标往后读，
// 得到 0 行，然后重新盖章——不丢数据，只是多一次空读。
func (s *Sink) MarkTableDone(ctx context.Context, ref source.TableRef) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	ck := s.stateTable("_jl_chunk")
	q := fmt.Sprintf(`INSERT INTO %s (job_key, table_name, chunk_no, range_from, range_to, done, rows)
		SELECT $1, $2, %d, NULL, NULL, true,
		       COALESCE((SELECT sum(rows) FROM %s WHERE job_key=$1 AND table_name=$2 AND chunk_no > 0), 0)
		ON CONFLICT (job_key, table_name, chunk_no) DO UPDATE SET done=true`,
		ck, TableDoneChunkNo, ck)
	_, err := s.conn.Exec(ctx, q, s.opt.JobKey, ref.Table)
	return err
}

// StartFreshCopy opens a new initial copy: it drops this job's chunk markers and
// moves the authoritative position to the new view's start **in one transaction**.
//
// 为什么必须同一个事务（这是 Owl 必改 1）：这两件事分开写、还先推位点的话，中间崩一次
// 就留下「新位点 P2 + 上一轮的块标记 1..50」。下次 planCopy 会读到 anyMarker=true /
// not-done / StoredCP=P2 / 书签可用 → 判成**续跑**：从旧游标接着搬，增量却从 P2 重放，
// 于是 (P1,P2] 之间的增/改/删永远不会被重放，而且退出码是 0、日志还写着「续跑」。
//
// 同事务之后，库里只有两种状态：要么「旧位点 + 旧标记」（下次照旧判重搬），
// 要么「新位点 + 没有标记」（新的一次拷贝，从头开始）。
func (s *Sink) StartFreshCopy(ctx context.Context, pos string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// 按 job_key 清**全部**标记，不只本次选中的表：上次在范围内、这次不在的表留下的
	// 旧标记同样是「另一次拷贝」的东西，留着只会让下一次判定读错代号。
	q := fmt.Sprintf(`DELETE FROM %s WHERE job_key=$1`, s.stateTable("_jl_chunk"))
	if _, err := tx.Exec(ctx, q, s.opt.JobKey); err != nil {
		return fmt.Errorf("清掉本任务的块进度失败: %w", err)
	}
	if pos != "" {
		if err := s.setPositionTx(ctx, tx, pos); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// ResetTableChunks puts one table back to the start: empty the table and drop
// its chunk markers **in one transaction**.
//
// 顺序很重要，所以只能在一个事务里做：先清数据再清标记，中间崩了就是
// 「标记说搬完了、数据没了」——静默丢行；反过来中间崩了就是「标记说没搬、数据还在」——
// 重搬一遍靠 upsert 收敛。一个事务里做，两种中间态都不存在。
//
// 只用于**本任务建的表**（调用方先用 DidCreateTable 问过）：append 复用的用户表里
// 有不属于我们的行，清它就是删用户数据。
func (s *Sink) ResetTableChunks(ctx context.Context, ref source.TableRef) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if _, err := tx.Exec(ctx, "TRUNCATE TABLE "+pgwire.Qualified(ref.Schema, ref.Table)); err != nil {
		return fmt.Errorf("清空目标表 %s 失败: %w", ref.Table, err)
	}
	q := fmt.Sprintf(`DELETE FROM %s WHERE job_key=$1 AND table_name=$2`, s.stateTable("_jl_chunk"))
	if _, err := tx.Exec(ctx, q, s.opt.JobKey, ref.Table); err != nil {
		return fmt.Errorf("清掉 %s 的块进度失败: %w", ref.Table, err)
	}
	return tx.Commit(ctx)
}
