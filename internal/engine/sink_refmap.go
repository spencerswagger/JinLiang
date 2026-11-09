package engine

import (
	"context"

	"github.com/spencerswagger/jinliang/internal/source"
)

// refMappedSink 把事件里的表引用换成**目标端**的，再交给真正的目标端。
//
// 为什么需要它：变更事件里的表名是**源端**的（MySQL 源是 `jinliang_src.orders`，
// PG 目标端只认 `public.orders`）。目标端拿事件里的 ref 直接查自己的表定义，
// 不换就会报「收到未知表 orders 的变更（发布里多了表？）」——文案看起来像源端配置
// 问题，实际是引擎没做映射。同引擎（PG→PG）时两个 schema 都是 public，映射是恒等。
//
// Relation 也要换，而且**同一版定义必须复用同一个指针**：契约规定「指针相等即同一版」，
// 每批都新建一个的话，目标端会以为源端每批都在改表结构。
type refMappedSink struct {
	inner  applier
	mapRef func(source.TableRef) source.TableRef
	// rels 缓存「源端 Relation 指针 → 换过表名的副本指针」，保证同一版复用同一个指针。
	// batcher 串行调用 Apply，所以这里不需要锁。
	rels map[*source.Table]*source.Table
}

func newRefMappedSink(inner applier, mapRef func(source.TableRef) source.TableRef) *refMappedSink {
	return &refMappedSink{inner: inner, mapRef: mapRef, rels: map[*source.Table]*source.Table{}}
}

func (s *refMappedSink) Apply(ctx context.Context, evs []source.Event, pos source.Position) error {
	if len(evs) == 0 {
		return s.inner.Apply(ctx, evs, pos)
	}
	out := make([]source.Event, len(evs))
	copy(out, evs)
	for i := range out {
		out[i].Table = s.mapTable(out[i].Table)
		out[i].New.Table = s.mapTable(out[i].New.Table)
		out[i].Old.Table = s.mapTable(out[i].Old.Table)
		if rel := out[i].Relation; rel != nil {
			out[i].Relation = s.mapRelation(rel)
		}
	}
	return s.inner.Apply(ctx, out, pos)
}

// mapTable 只换非空引用：零值 ref 表示「这条事件没有行」，换出一个只有 schema 的
// 空表名会被目标端当成真表名去查。
func (s *refMappedSink) mapTable(ref source.TableRef) source.TableRef {
	if ref.Table == "" {
		return ref
	}
	return s.mapRef(ref)
}

func (s *refMappedSink) mapRelation(rel *source.Table) *source.Table {
	if cached, ok := s.rels[rel]; ok {
		return cached
	}
	cp := *rel
	cp.TableRef = s.mapTable(rel.TableRef)
	s.rels[rel] = &cp
	return &cp
}
