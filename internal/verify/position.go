package verify

import (
	"context"
	"fmt"
	"strings"

	"github.com/spencerswagger/jinliang/internal/runtime"
)

// DBSourcePosition reads the source-side checkpoint for the consistency tier.
//
// PostgreSQL：slot 的 confirmed_flush_lsn（护栏位点）。
// MySQL：binlog 的 file:pos（SHOW MASTER STATUS / SHOW BINARY LOG STATUS）。
//
// slotPrefix 建议传 "jl_<job_key>"，避免同库多个任务时选错 slot；没给时若
// 只有一个 jl_ slot 就用它，多于一个就明确报错（不猜）。
func DBSourcePosition(ctx context.Context, db *runtime.DB, slotPrefix string) (string, error) {
	switch db.Kind() {
	case runtime.KindMySQL:
		return db.BinlogPosition(ctx)
	case runtime.KindPostgres:
		slots, err := db.Slots(ctx, "jl_")
		if err != nil {
			return "", err
		}
		if slotPrefix != "" {
			var filtered []runtime.Slot
			for _, s := range slots {
				if strings.HasPrefix(s.Name, slotPrefix) {
					filtered = append(filtered, s)
				}
			}
			slots = filtered
		}
		switch len(slots) {
		case 0:
			return "", nil
		case 1:
			if slots[0].ConfirmedFlush != "" {
				return slots[0].ConfirmedFlush, nil
			}
			return slots[0].RestartLSN, nil
		default:
			names := make([]string, len(slots))
			for i, s := range slots {
				names[i] = s.Name
			}
			return "", fmt.Errorf("有 %d 个同步书签匹配：%s；用 --job-key 或 slot 前缀指定一个",
				len(slots), strings.Join(names, ", "))
		}
	default:
		return "", fmt.Errorf("不支持的数据库类型 %q", db.Kind())
	}
}

// DBTargetPosition reads the authoritative position from `_jl_meta`.
func DBTargetPosition(ctx context.Context, db *runtime.DB, jobKey string) (string, error) {
	if jobKey == "" {
		return "", fmt.Errorf("读目标端位点需要 --job-key（_jl_meta 按 job_key 分区）")
	}
	m, found, err := db.Meta(ctx, jobKey)
	if err != nil {
		return "", err
	}
	if !found {
		return "", nil
	}
	return m.Position, nil
}
