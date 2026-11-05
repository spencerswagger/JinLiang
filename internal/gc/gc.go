// Package gc decides which objects this tool created may be removed.
//
// 所有权四判定（docs/IDENTITY.md §5、docs/STATE-SCHEMA.md §4）：
//
//	owned_by_this_instance     本实例创建，且没人用 → 默认可清
//	in_use_by_this_instance    本实例创建，但本实例仍在用（active / 租约）→ 不清
//	in_use_by_other_instance   归属或正被其他实例使用 → 默认不清
//	unknown                    登记缺失/读不到/名字不在命名空间 → 永不清理，只报告
//
// 默认 dry-run；默认只清第一类。跨实例清理必须显式 Force，且只能碰「非活动且
// 无有效租约」的对象；unknown 即使 Force 也不动。
package gc

import (
	"context"
	"strings"
	"time"

	"github.com/spencerswagger/jinliang/internal/runtime"
)

// Verdict is one of the four ownership outcomes.
type Verdict string

// The four verdicts.
const (
	OwnedByThisInstance  Verdict = "owned_by_this_instance"
	InUseByThisInstance  Verdict = "in_use_by_this_instance"
	InUseByOtherInstance Verdict = "in_use_by_other_instance"
	Unknown              Verdict = "unknown"
)

// LiveObject is what actually exists on the source side right now.
type LiveObject struct {
	Name   string
	Kind   string // slot / publication / temp_snapshot
	Active bool   // PG: slot 正在被读
	Exists bool
	Detail string
}

// Input is everything the planner is allowed to look at. It is deliberately
// plain data so the four verdicts can be tested without a database.
type Input struct {
	InstanceID string
	Now        time.Time

	// RegistryKnown=false means _jl_object is missing/unreadable. Every live
	// object then becomes Unknown (never deleted) — that is the safe default.
	RegistryKnown bool
	Registered    []runtime.Object
	Leases        []runtime.Lease
	Live          []LiveObject
}

// Options tunes the plan.
type Options struct {
	DryRun              bool
	AllowOtherInstances bool // --force：只放宽第三类里「空闲」的对象
}

// Decision is one object's verdict plus the action we would take.
type Decision struct {
	Name       string  `json:"name"`
	Kind       string  `json:"kind"`
	JobKey     string  `json:"job_key,omitempty"`
	Owner      string  `json:"owner_instance,omitempty"`
	Verdict    Verdict `json:"verdict"`
	Reason     string  `json:"reason"`
	Active     bool    `json:"active"`
	LeaseOwner string  `json:"lease_owner,omitempty"`
	Delete     bool    `json:"delete"` // true = eligible；dry-run 时只表示「会删」
}

// Plan is the full gc output.
type Plan struct {
	InstanceID  string     `json:"instance_id"`
	DryRun      bool       `json:"dry_run"`
	GeneratedAt string     `json:"generated_at"`
	Decisions   []Decision `json:"decisions"`
	Deletable   int        `json:"deletable"`
	// Counts per verdict, handy for tests and the UI.
	Counts map[Verdict]int `json:"counts"`
}

// Build applies the four verdicts. It never performs I/O.
func Build(in Input, opts Options) Plan {
	now := in.Now
	if now.IsZero() {
		now = time.Now()
	}
	plan := Plan{
		InstanceID:  in.InstanceID,
		DryRun:      opts.DryRun,
		GeneratedAt: now.UTC().Format(time.RFC3339),
		Counts:      map[Verdict]int{},
	}

	reg := map[string]runtime.Object{}
	for _, o := range in.Registered {
		reg[o.Name] = o
	}
	liveLease := map[string]string{} // job_key -> instance holding a valid lease
	for _, l := range in.Leases {
		if l.ExpiresAt.After(now) {
			liveLease[l.JobKey] = l.InstanceID
		}
	}

	for _, live := range in.Live {
		if !live.Exists {
			continue // 已经不在了：没有可删的东西
		}
		d := Decision{Name: live.Name, Kind: live.Kind, Active: live.Active}
		o, registered := reg[live.Name]
		switch {
		case !in.RegistryKnown || !registered || strings.TrimSpace(o.InstanceID) == "" || !strings.HasPrefix(live.Name, "jl_"):
			d.Verdict = Unknown
			d.Reason = "没有 _jl_object 登记或登记表读不到：无法判定归属"
			if registered && o.InstanceID != "" {
				d.Owner, d.JobKey = o.InstanceID, o.JobKey
			}
		default:
			d.Owner, d.JobKey = o.InstanceID, o.JobKey
			holder := liveLease[o.JobKey]
			d.LeaseOwner = holder
			switch {
			case live.Active && (holder == in.InstanceID || (holder == "" && o.InstanceID == in.InstanceID)):
				d.Verdict = InUseByThisInstance
				d.Reason = "本实例正在使用（对象 active）"
			case live.Active:
				d.Verdict = InUseByOtherInstance
				d.Reason = "对象中有读连接（active=true）：任何实例都不该删"
			case holder == in.InstanceID:
				d.Verdict = InUseByThisInstance
				d.Reason = "本实例持有有效租约"
			case holder != "" && holder != in.InstanceID:
				d.Verdict = InUseByOtherInstance
				d.Reason = "其他实例持有有效租约：" + holder
			case d.Owner != in.InstanceID:
				d.Verdict = InUseByOtherInstance
				d.Reason = "登记归属其他实例：" + d.Owner + "（默认保留；--force 只放宽空闲对象）"
			default:
				d.Verdict = OwnedByThisInstance
				d.Reason = "本实例创建，且没有活动读、没有有效租约"
				d.Delete = true
			}
		}
		if d.Verdict == InUseByOtherInstance && opts.AllowOtherInstances && !d.Active && d.LeaseOwner == "" {
			d.Delete = true
			d.Reason += "；--force 允许清理空闲的跨实例对象"
		}
		if d.Verdict == Unknown {
			d.Delete = false // 纪律：无法判定就只报告
		}
		plan.Counts[d.Verdict]++
		if d.Delete {
			plan.Deletable++
		}
		plan.Decisions = append(plan.Decisions, d)
	}

	// 第二遍：publication 没有 active 语义，只能靠「同 job_key 的 slot 是否仍在读」
	// 判断。否则租约缺失/过期时，正在跑的 publication 会落进「本实例创建且空闲」，
	// 默认档就被删掉 —— 结果是 slot 活着、publication 没了，下次重连才炸。
	activeJobs := map[string]bool{}
	for _, d := range plan.Decisions {
		if d.Kind == "slot" && d.Active && d.JobKey != "" {
			activeJobs[d.JobKey] = true
		}
	}
	for i := range plan.Decisions {
		d := &plan.Decisions[i]
		if d.Kind != "publication" || !d.Delete || !activeJobs[d.JobKey] {
			continue
		}
		plan.Counts[d.Verdict]--
		if d.Owner == in.InstanceID {
			d.Verdict = InUseByThisInstance
		} else {
			d.Verdict = InUseByOtherInstance
		}
		d.Delete = false
		plan.Deletable--
		plan.Counts[d.Verdict]++
		d.Reason += "；同任务的同步书签仍在读（active=true），发布清单可能仍在使用"
	}
	return plan
}

// Dropper performs the actual removal on the source side. runtime.DB
// implements it for PostgreSQL slots/publications.
type Dropper interface {
	DropObject(ctx context.Context, name, kind string) error
}

// RegistryMarker soft-deletes the `_jl_object` row after a successful drop.
// It lives on the target database, so the CLI passes a composite dropper.
type RegistryMarker interface {
	MarkObjectDropped(ctx context.Context, name string) error
}

// Result is what happened.
type Result struct {
	Deleted []string `json:"deleted"`
	Failed  []string `json:"failed"`
}

// Execute removes every eligible object. A dry-run plan deletes nothing.
func Execute(ctx context.Context, plan Plan, d Dropper) (Result, error) {
	res := Result{}
	if plan.DryRun {
		return res, nil
	}
	var firstErr error
	for _, dec := range plan.Decisions {
		if !dec.Delete {
			continue
		}
		if err := d.DropObject(ctx, dec.Name, dec.Kind); err != nil {
			res.Failed = append(res.Failed, dec.Name+": "+err.Error())
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		res.Deleted = append(res.Deleted, dec.Name)
		if m, ok := d.(RegistryMarker); ok {
			_ = m.MarkObjectDropped(ctx, dec.Name)
		}
	}
	return res, firstErr
}
