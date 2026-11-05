package gc

import (
	"context"

	"github.com/spencerswagger/jinliang/internal/runtime"
)

// Load reads the registry, leases and live source objects, then builds a plan.
//
// 只读：target 用来读 `_jl_object` / `_jl_lease`，source 用来列 slot /
// publication。target 可以为 nil（只做源端盘点），此时 RegistryKnown=false，
// 所有对象都会落到 unknown —— 只报告、不删。
func Load(ctx context.Context, source, target *runtime.DB, instanceID string, opts Options) (Plan, error) {
	in := Input{InstanceID: instanceID}
	if target != nil {
		tables, err := target.Tables(ctx)
		if err != nil {
			return Plan{}, err
		}
		in.RegistryKnown = tables[runtime.TableObject]
		if in.RegistryKnown {
			objs, err := target.Objects(ctx)
			if err != nil {
				return Plan{}, err
			}
			in.Registered = objs
		}
		if tables[runtime.TableLease] {
			leases, err := target.Leases(ctx)
			if err != nil {
				return Plan{}, err
			}
			in.Leases = leases
		}
	}
	if source != nil {
		switch source.Kind() {
		case runtime.KindPostgres:
			slots, err := source.Slots(ctx, "jl_")
			if err != nil {
				return Plan{}, err
			}
			for _, s := range slots {
				in.Live = append(in.Live, LiveObject{Name: s.Name, Kind: "slot", Active: s.Active, Exists: true})
			}
			pubs, err := source.Publications(ctx, "jl_")
			if err != nil {
				return Plan{}, err
			}
			for _, p := range pubs {
				in.Live = append(in.Live, LiveObject{Name: p.Name, Kind: "publication", Exists: true})
			}
		case runtime.KindMySQL:
			// MySQL 没有持久化的 slot/publication；binlog 位点由 server 自己轮转。
			// 这里不伪造对象，plan 为空就是事实。
		}
	}
	return Build(in, opts), nil
}
