// Package remedy is the manual-intervention surface from docs/CONFLICT.md:
// list conflicts, retry dead letters, and (explicitly not implemented yet)
// undo recorded actions.
//
// 设计取舍：重放一条死信 = 把 `raw_row` 按主键 upsert 回目标表。它不推进权威
// 位点（位点只由引擎在数据同事务里推进），因此重放是幂等的、可重复执行。
package remedy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/spencerswagger/jinliang/internal/runtime"
)

// ErrUndoUnimplemented is returned by Undo. It is a deliberate, explicit
// "not done yet" — not a half-working undo that might drop the wrong object.
var ErrUndoUnimplemented = errors.New(
	"undo 今晚未实现：动作回滚需要按 _jl_action 逐类校验，半截实现比不做更危险；" +
		"已做的连接级动作仍可用各自的回滚语句手工执行（见 jinliang doctor 输出与 _jl_action.undo）")

// Undo exists so callers get a sentence instead of a missing feature.
func Undo(ctx context.Context, db *runtime.DB, jobKey string, dryRun bool) error {
	if db == nil {
		return ErrUndoUnimplemented
	}
	return ErrUndoUnimplemented
}

// Conflicts lists recorded conflicts. Table must exist (runtime tables are
// created by the engine's EnsureSchema).
func Conflicts(ctx context.Context, db *runtime.DB, jobKey string, limit int) ([]runtime.Conflict, error) {
	if db == nil {
		return nil, errors.New("conflicts 需要一个目标库连接")
	}
	tables, err := db.Tables(ctx)
	if err != nil {
		return nil, err
	}
	if !tables[runtime.TableConflict] {
		return nil, fmt.Errorf("目标库没有 %s（任务还没跑过，或初始化没建表）", runtime.TableConflict)
	}
	return db.Conflicts(ctx, jobKey, limit)
}

// RetryResult is the outcome of one dead-letter retry.
type RetryResult struct {
	ID       int64  `json:"id"`
	Table    string `json:"table"`
	PK       string `json:"pk"`
	Applied  bool   `json:"applied"`
	DryRun   bool   `json:"dry_run"`
	ActionID string `json:"action_id,omitempty"`
	Reason   string `json:"reason,omitempty"`
}

// RetryDeadLetter re-applies one dead letter row by primary key.
//
// 拒绝而不是猜测的情况：raw_row 不是 JSON 对象、缺主键、目标列已不存在。
// 这些都会原样写进错误，不半写。
func RetryDeadLetter(ctx context.Context, db *runtime.DB, schema string, id int64, dryRun bool) (RetryResult, error) {
	if db == nil {
		return RetryResult{}, errors.New("dead-letter retry 需要一个目标库连接")
	}
	tables, err := db.Tables(ctx)
	if err != nil {
		return RetryResult{}, err
	}
	if !tables[runtime.TableDeadLetter] {
		return RetryResult{}, fmt.Errorf("目标库没有 %s", runtime.TableDeadLetter)
	}
	dl, found, err := db.GetDeadLetter(ctx, id)
	if err != nil {
		return RetryResult{}, err
	}
	if !found {
		return RetryResult{}, fmt.Errorf("死信 #%d 不存在（可能已经重放过）", id)
	}
	res := RetryResult{ID: dl.ID, Table: dl.Table, DryRun: dryRun}

	row, err := decodeRawRow(dl.RawRow)
	if err != nil {
		return res, fmt.Errorf("死信 #%d 的 raw_row 不能重放：%w", id, err)
	}
	t, exists, err := db.Table(ctx, schema, dl.Table)
	if err != nil {
		return res, err
	}
	if !exists {
		return res, fmt.Errorf("目标表 %s 不存在（结构被改过？）", dl.Table)
	}
	if len(t.PrimaryKey) == 0 {
		return res, fmt.Errorf("目标表 %s 没有主键：死信不能按行重放", dl.Table)
	}
	pkParts := make([]string, 0, len(t.PrimaryKey))
	for _, c := range t.PrimaryKey {
		v, ok := row[c]
		if !ok {
			return res, fmt.Errorf("死信 #%d 缺少主键列 %q", id, c)
		}
		pkParts = append(pkParts, fmt.Sprintf("%s=%v", c, v))
	}
	res.PK = strings.Join(pkParts, ",")
	if dryRun {
		res.Reason = "dry-run：校验通过，未写入"
		return res, nil
	}

	if err := db.ApplyJSONRow(ctx, schema, dl.Table, t.PrimaryKey, row); err != nil {
		return res, fmt.Errorf("重放死信 #%d 失败（未删除死信）: %w", id, err)
	}
	res.Applied = true
	res.ActionID = fmt.Sprintf("dead-letter-retry/%d", id)
	// 留痕失败不影响数据已写入的事实；把错误带回去但不谎报未写。
	if err := db.InsertAction(ctx, dl.JobKey, res.ActionID,
		"重放死信 #"+fmt.Sprint(id)+"（"+dl.Table+"）", "cli"); err != nil {
		return res, fmt.Errorf("已写入目标表，但审计记录失败: %w", err)
	}
	if err := db.DeleteDeadLetter(ctx, id); err != nil {
		return res, fmt.Errorf("已写入目标表，但删除死信失败（下次重放是幂等的）: %w", err)
	}
	return res, nil
}

func decodeRawRow(raw string) (map[string]any, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "null" {
		return nil, errors.New("raw_row 是空的（可能配置成只存哈希）")
	}
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.UseNumber()
	var obj map[string]any
	if err := dec.Decode(&obj); err != nil {
		return nil, fmt.Errorf("不是 JSON 对象: %w", err)
	}
	if len(obj) == 0 {
		return nil, errors.New("raw_row 是空对象")
	}
	return obj, nil
}
