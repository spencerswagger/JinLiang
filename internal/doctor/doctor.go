// Package doctor answers 「现在这套系统健康吗」 for three places at once:
// the local control database, the source database, and the target database.
//
// 它是只读体检：不修、不删、不写。控制库里 `runs.ended_at IS NULL` 的行
// （上一次进程被 kill 后留下的「幽灵运行」）只做识别与报告 —— 启动时的收尾
// 由控制面负责（CONTROL-PLANE），本包不越界去改。
package doctor

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/spencerswagger/jinliang/internal/runtime"
)

// Status is the severity of one check.
type Status string

// Check statuses. problem makes the report unhealthy (exit code 1).
const (
	StatusOK      Status = "ok"
	StatusWarn    Status = "warn"
	StatusProblem Status = "problem"
)

// Check is one structured conclusion.
type Check struct {
	ID     string `json:"id"`
	Area   string `json:"area"` // control / source / target
	Title  string `json:"title"`
	Status Status `json:"status"`
	Detail string `json:"detail,omitempty"`
	Hint   string `json:"hint,omitempty"`
}

// RunInfo is one open run row (ended_at IS NULL), as far as doctor needs it.
type RunInfo struct {
	ID        string
	JobID     string
	Phase     string
	StartedAt string
	Stub      bool
}

// ControlProbe is the slice of the control store doctor reads. Keeping it an
// interface avoids a compile-time dependency on internal/control (another
// track owns that package).
type ControlProbe interface {
	InstanceID(ctx context.Context) (string, error)
	IntegrityCheck(ctx context.Context) (string, error)
	SchemaVersion(ctx context.Context) (int, error)
	AdminConfigured(ctx context.Context) (bool, error)
	OpenRuns(ctx context.Context) ([]RunInfo, error)
}

// Options is everything doctor is allowed to look at.
type Options struct {
	Control ControlProbe // nil: skip control checks

	LatestSchemaVersion int
	GhostAfter          time.Duration // 默认 5 分钟；更老的未收尾运行算幽灵
	Now                 func() time.Time

	Source *runtime.DB
	Target *runtime.DB

	SourceSchema string
	TargetSchema string
	JobKey       string
	SlotPrefix   string

	// InstanceID is filled by Run from the control probe; tests may set it.
	InstanceID string

	// SlotLagWarnBytes is when a replication slot's retained WAL is "too
	// much". Default 5 GiB (DESIGN.md §7A).
	SlotLagWarnBytes int64
}

// Report is the whole health report.
type Report struct {
	GeneratedAt string  `json:"generated_at"`
	InstanceID  string  `json:"instance_id,omitempty"`
	Checks      []Check `json:"checks"`
	Problems    int     `json:"problems"`
	Warnings    int     `json:"warnings"`
	Healthy     bool    `json:"healthy"`
}

// Run performs the checks.
func Run(ctx context.Context, opts Options) Report {
	now := time.Now()
	if opts.Now != nil {
		now = opts.Now()
	}
	if opts.GhostAfter <= 0 {
		opts.GhostAfter = 5 * time.Minute
	}
	if opts.SlotLagWarnBytes <= 0 {
		opts.SlotLagWarnBytes = 5 << 30
	}
	if opts.SourceSchema == "" {
		opts.SourceSchema = "public"
	}
	if opts.TargetSchema == "" {
		opts.TargetSchema = "public"
	}
	r := Report{GeneratedAt: now.UTC().Format(time.RFC3339), Healthy: true}
	add := func(c Check) { r.Checks = append(r.Checks, c) }

	r.InstanceID = controlChecks(ctx, opts, now, add)
	opts.InstanceID = r.InstanceID
	sourceChecks(ctx, opts, add)
	targetChecks(ctx, opts, now, add)

	for _, c := range r.Checks {
		switch c.Status {
		case StatusProblem:
			r.Problems++
		case StatusWarn:
			r.Warnings++
		}
	}
	r.Healthy = r.Problems == 0
	return r
}

func controlChecks(ctx context.Context, opts Options, now time.Time, add func(Check)) string {
	if opts.Control == nil {
		add(Check{ID: "CTL-00", Area: "control", Title: "跳过控制库检查", Status: StatusWarn,
			Detail: "没有打开控制库（--data-dir 不存在，或只做源/目标体检）"})
		return ""
	}
	instance, err := opts.Control.InstanceID(ctx)
	if err != nil || strings.TrimSpace(instance) == "" {
		add(Check{ID: "CTL-01", Area: "control", Title: "实例 ID 读不到", Status: StatusWarn,
			Detail: errText(err), Hint: "实例身份用于租约与对象所有权；跑一次 jinliang serve 会补上"})
	} else {
		add(Check{ID: "CTL-01", Area: "control", Title: "实例 ID 正常", Status: StatusOK, Detail: instance})
	}

	if integrity, err := opts.Control.IntegrityCheck(ctx); err != nil || integrity != "ok" {
		add(Check{ID: "CTL-02", Area: "control", Title: "控制库完整性检查没通过", Status: StatusProblem,
			Detail: firstNonEmpty(integrity, errText(err)), Hint: "先备份 data-dir，再考虑从备份恢复（控制库丢了不影响目标端位点）"})
	} else {
		add(Check{ID: "CTL-02", Area: "control", Title: "控制库完整", Status: StatusOK, Detail: "PRAGMA integrity_check = ok"})
	}

	if v, err := opts.Control.SchemaVersion(ctx); err != nil {
		add(Check{ID: "CTL-03", Area: "control", Title: "控制库版本读不到", Status: StatusProblem, Detail: errText(err)})
	} else if opts.LatestSchemaVersion > 0 && v > opts.LatestSchemaVersion {
		add(Check{ID: "CTL-03", Area: "control", Title: "控制库比当前二进制新", Status: StatusProblem,
			Detail: fmt.Sprintf("控制库 schema %d，二进制只认识 %d", v, opts.LatestSchemaVersion),
			Hint:   "换回更新的二进制；旧版本写新库可能丢字段"})
	} else if opts.LatestSchemaVersion > 0 && v < opts.LatestSchemaVersion {
		add(Check{ID: "CTL-03", Area: "control", Title: "有未应用的控制库迁移", Status: StatusWarn,
			Detail: fmt.Sprintf("当前 %d，最新 %d", v, opts.LatestSchemaVersion),
			Hint:   "跑一次 jinliang serve（启动时会自动迁移）"})
	} else {
		add(Check{ID: "CTL-03", Area: "control", Title: "控制库版本匹配", Status: StatusOK,
			Detail: fmt.Sprintf("schema %d", v)})
	}

	configured, err := opts.Control.AdminConfigured(ctx)
	if err != nil {
		add(Check{ID: "CTL-04", Area: "control", Title: "管理员状态读不到", Status: StatusWarn, Detail: errText(err)})
	} else if !configured {
		add(Check{ID: "CTL-04", Area: "control", Title: "还没有管理员口令", Status: StatusWarn,
			Detail: "控制台任何人在本机打开都能改连接与任务",
			Hint:   "跑一次 jinliang serve，打开页面设置口令；或用 --api-token 限制自动化访问"})
	} else {
		add(Check{ID: "CTL-04", Area: "control", Title: "管理员口令已设置", Status: StatusOK})
	}

	runs, err := opts.Control.OpenRuns(ctx)
	if err != nil {
		add(Check{ID: "CTL-05", Area: "control", Title: "运行记录读不到", Status: StatusWarn, Detail: errText(err)})
		return instance
	}
	ghosts, open := 0, 0
	var ghostIDs []string
	for _, run := range runs {
		started, perr := time.Parse(time.RFC3339, strings.TrimSpace(run.StartedAt))
		if perr != nil {
			// 时间解析不了也按"未收尾"报告，别静默放过。
			ghosts++
			ghostIDs = append(ghostIDs, run.ID)
			continue
		}
		if now.Sub(started) >= opts.GhostAfter {
			ghosts++
			ghostIDs = append(ghostIDs, fmt.Sprintf("%s(%s, %s)", run.ID, run.Phase, run.JobID))
		} else {
			open++
		}
	}
	if ghosts > 0 {
		sort.Strings(ghostIDs)
		add(Check{ID: "CTL-05", Area: "control", Title: "发现幽灵运行（ended_at 为空）", Status: StatusProblem,
			Detail: fmt.Sprintf("%d 条超过 %s 仍未收尾：%s", ghosts, opts.GhostAfter, strings.Join(ghostIDs, ", ")),
			Hint:   "控制面会在下次启动时收尾这些行；若反复出现，说明上次进程被 kill -9 或容器没优雅退出。目标端位点是权威，重启后可续跑"})
	} else if open > 0 {
		add(Check{ID: "CTL-05", Area: "control", Title: "有正在进行的运行", Status: StatusOK,
			Detail: fmt.Sprintf("%d 条未收尾，但都在 %s 内（视为正常在跑）", open, opts.GhostAfter)})
	} else {
		add(Check{ID: "CTL-05", Area: "control", Title: "没有未收尾的运行", Status: StatusOK})
	}
	return instance
}

func sourceChecks(ctx context.Context, opts Options, add func(Check)) {
	if opts.Source == nil {
		add(Check{ID: "SRC-00", Area: "source", Title: "跳过源端检查", Status: StatusWarn,
			Detail: "没有提供源端 DSN"})
		return
	}
	ver, err := opts.Source.Version(ctx)
	if err != nil {
		add(Check{ID: "SRC-01", Area: "source", Title: "源库连不上", Status: StatusProblem,
			Detail: errText(err), Hint: "确认地址/账号/防火墙；源端只读探测不会改任何东西"})
		return
	}
	add(Check{ID: "SRC-01", Area: "source", Title: "源库可以连上", Status: StatusOK, Detail: ver})

	switch opts.Source.Kind() {
	case runtime.KindPostgres:
		wal, err := opts.Source.WalLevel(ctx)
		if err != nil {
			add(Check{ID: "SRC-02", Area: "source", Title: "读不到 wal_level", Status: StatusWarn, Detail: errText(err)})
		} else if wal != "logical" {
			add(Check{ID: "SRC-02", Area: "source", Title: "源库没有开启变更流水", Status: StatusWarn,
				Detail: "wal_level=" + wal + "：实时增量读不到变更",
				Hint:   "ALTER SYSTEM SET wal_level='logical' 后重启；或把任务改成「定时增量」（只要 SELECT）"})
		} else {
			add(Check{ID: "SRC-02", Area: "source", Title: "变更流水已开启", Status: StatusOK, Detail: "wal_level=logical"})
		}
		if cap := opts.Source.MaxSlotWALKeepSize(ctx); cap == "" || cap == "-1" || cap == "0" {
			add(Check{ID: "SRC-03", Area: "source", Title: "这个版本没有书签磁盘上限", Status: StatusWarn,
				Detail: "max_slot_wal_keep_size=" + firstNonEmpty(cap, "（不存在）") + "：书签落后会一直占磁盘",
				Hint:   "用 guard.slot_warn_lag 告警并盯住磁盘；PG 13+ 可以设 max_slot_wal_keep_size"})
		} else {
			add(Check{ID: "SRC-03", Area: "source", Title: "书签有磁盘上限", Status: StatusOK, Detail: "max_slot_wal_keep_size=" + cap})
		}
		slots, err := opts.Source.Slots(ctx, opts.SlotPrefix)
		if err != nil {
			add(Check{ID: "SRC-04", Area: "source", Title: "读不到同步书签", Status: StatusWarn, Detail: errText(err)})
			return
		}
		if len(slots) == 0 {
			add(Check{ID: "SRC-04", Area: "source", Title: "没有找到本任务的同步书签", Status: StatusWarn,
				Detail: "前缀 " + firstNonEmpty(opts.SlotPrefix, "jl_") + " 下没有 slot",
				Hint:   "任务还没初始化，或书签已被清理；后者需要 --resync"})
			return
		}
		for _, s := range slots {
			if s.LagBytes > opts.SlotLagWarnBytes {
				add(Check{ID: "SRC-04", Area: "source", Title: "同步书签占用磁盘过多", Status: StatusWarn,
					Detail: fmt.Sprintf("%s 落后 %d 字节（active=%v）", s.Name, s.LagBytes, s.Active),
					Hint:   "确认目标端是否卡住；必要时先追平再观察"})
			}
		}
		add(Check{ID: "SRC-04", Area: "source", Title: fmt.Sprintf("同步书签 %d 个", len(slots)), Status: StatusOK})
	case runtime.KindMySQL:
		vars, err := opts.Source.BinlogSettings(ctx)
		if err != nil {
			add(Check{ID: "SRC-02", Area: "source", Title: "读不到 binlog 设置", Status: StatusWarn, Detail: errText(err)})
			return
		}
		if !strings.EqualFold(vars["log_bin"], "ON") && vars["log_bin"] != "1" {
			add(Check{ID: "SRC-02", Area: "source", Title: "源库没有开启变更流水", Status: StatusWarn,
				Detail: "log_bin=" + firstNonEmpty(vars["log_bin"], "?"),
				Hint:   "开 log_bin + binlog_format=ROW 后重启；或改用定时增量"})
		} else if !strings.EqualFold(vars["binlog_format"], "ROW") {
			add(Check{ID: "SRC-02", Area: "source", Title: "binlog 格式不是 ROW", Status: StatusWarn,
				Detail: "binlog_format=" + firstNonEmpty(vars["binlog_format"], "?"),
				Hint:   "SET GLOBAL binlog_format=ROW（会话级不够，要全局）"})
		} else {
			add(Check{ID: "SRC-02", Area: "source", Title: "变更流水可用", Status: StatusOK, Detail: "log_bin=ON, binlog_format=ROW"})
		}
		if pos, err := opts.Source.BinlogPosition(ctx); err == nil {
			if pos == "" {
				add(Check{ID: "SRC-03", Area: "source", Title: "读不到 binlog 位点", Status: StatusWarn,
					Detail: "SHOW MASTER STATUS / SHOW BINARY LOG STATUS 都没有返回"})
			} else {
				add(Check{ID: "SRC-03", Area: "source", Title: "binlog 位点可读", Status: StatusOK, Detail: pos})
			}
		}
	}
}

func targetChecks(ctx context.Context, opts Options, now time.Time, add func(Check)) {
	if opts.Target == nil {
		add(Check{ID: "TGT-00", Area: "target", Title: "跳过目标端检查", Status: StatusWarn,
			Detail: "没有提供目标端 DSN"})
		return
	}
	ver, err := opts.Target.Version(ctx)
	if err != nil {
		add(Check{ID: "TGT-01", Area: "target", Title: "目标库连不上", Status: StatusProblem,
			Detail: errText(err), Hint: "位点与状态表都在目标库；连不上就不能续跑"})
		return
	}
	add(Check{ID: "TGT-01", Area: "target", Title: "目标库可以连上", Status: StatusOK, Detail: ver})

	tables, err := opts.Target.Tables(ctx)
	if err != nil {
		add(Check{ID: "TGT-02", Area: "target", Title: "读不到运行时表", Status: StatusWarn, Detail: errText(err)})
		return
	}
	required := []string{runtime.TableMeta, runtime.TableLease, runtime.TableObject, runtime.TableVersion}
	var missing []string
	for _, t := range required {
		if !tables[t] {
			missing = append(missing, t)
		}
	}
	switch {
	case len(missing) == 0:
		add(Check{ID: "TGT-02", Area: "target", Title: "运行时表齐全", Status: StatusOK, Detail: "4/4"})
	case len(missing) == len(required):
		add(Check{ID: "TGT-02", Area: "target", Title: "目标库还没有运行时表", Status: StatusWarn,
			Detail: "任务还没初始化过（这不是故障）", Hint: "跑一次初始化后这些表会自动建好"})
	default:
		add(Check{ID: "TGT-02", Area: "target", Title: "运行时表不完整", Status: StatusWarn,
			Detail: "缺：" + strings.Join(missing, ", "),
			Hint:   "让 engine 重跑 EnsureSchema；不要手工建，结构必须与二进制一致"})
	}

	if tables[runtime.TableLease] {
		leases, err := opts.Target.Leases(ctx)
		if err == nil {
			expired, heldOther := 0, 0
			for _, l := range leases {
				if !l.ExpiresAt.After(now) {
					expired++
				} else if l.InstanceID != opts.InstanceID {
					heldOther++
				}
			}
			if expired > 0 {
				add(Check{ID: "TGT-03", Area: "target", Title: "有过期租约", Status: StatusWarn,
					Detail: fmt.Sprintf("%d 条租约已过期，可被接管", expired),
					Hint:   "过期租约本身不危险；启动时会记一条「接管了超时租约」"})
			} else if heldOther > 0 {
				add(Check{ID: "TGT-03", Area: "target", Title: "有别的实例在跑", Status: StatusOK,
					Detail: fmt.Sprintf("%d 条有效租约属于其他实例（不会双写）", heldOther)})
			} else {
				add(Check{ID: "TGT-03", Area: "target", Title: "租约正常", Status: StatusOK})
			}
		}
	}

	if tables[runtime.TableMeta] && opts.JobKey != "" {
		m, found, err := opts.Target.Meta(ctx, opts.JobKey)
		switch {
		case err != nil:
			add(Check{ID: "TGT-04", Area: "target", Title: "读不到任务状态", Status: StatusWarn, Detail: errText(err)})
		case !found:
			add(Check{ID: "TGT-04", Area: "target", Title: "目标端没有这个任务的记录", Status: StatusWarn,
				Detail: "job_key=" + opts.JobKey, Hint: "确认 job_key 是否正确；新任务第一次跑会自己建"})
		default:
			st := StatusOK
			hint := ""
			if m.Position == "" {
				st = StatusWarn
				hint = "任务还没写入过任何数据"
			}
			add(Check{ID: "TGT-04", Area: "target", Title: "任务状态可读", Status: st,
				Detail: fmt.Sprintf("phase=%s position=%s", m.Phase, firstNonEmpty(m.Position, "（空）")), Hint: hint})
		}
	}
	if tables[runtime.TableDeadLetter] && opts.JobKey != "" {
		if dls, err := opts.Target.DeadLetters(ctx, opts.JobKey, 10000); err == nil {
			if len(dls) > 0 {
				add(Check{ID: "TGT-05", Area: "target", Title: "有死信没处理", Status: StatusWarn,
					Detail: fmt.Sprintf("%d 条变更没写进目标库", len(dls)),
					Hint:   "jinliang dead-letter list --job-key " + opts.JobKey + "；修好根因后重放"})
			} else {
				add(Check{ID: "TGT-05", Area: "target", Title: "没有死信", Status: StatusOK})
			}
		}
	}
	if tables[runtime.TableConflict] && opts.JobKey != "" {
		if cs, err := opts.Target.Conflicts(ctx, opts.JobKey, 1000); err == nil {
			open := 0
			for _, c := range cs {
				if !c.Resolved {
					open++
				}
			}
			if open > 0 {
				add(Check{ID: "TGT-06", Area: "target", Title: "有未处理的冲突", Status: StatusWarn,
					Detail: fmt.Sprintf("%d 条冲突等人决定（default policy=manual）", open),
					Hint:   "jinliang conflicts list --job-key " + opts.JobKey})
			} else {
				add(Check{ID: "TGT-06", Area: "target", Title: "没有未处理的冲突", Status: StatusOK})
			}
		}
	}
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
