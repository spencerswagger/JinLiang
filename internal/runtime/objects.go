package runtime

import (
	"context"
	"fmt"
	"strings"
)

// Publication is one PostgreSQL publication (「发布清单」).
type Publication struct {
	Name string
}

// Publications lists publications, optionally filtered by name prefix.
func (d *DB) Publications(ctx context.Context, prefix string) ([]Publication, error) {
	if d.kind != KindPostgres {
		return nil, nil
	}
	rows, err := d.db.QueryContext(ctx, `SELECT pubname FROM pg_publication ORDER BY pubname`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Publication
	for rows.Next() {
		var p Publication
		if err := rows.Scan(&p.Name); err != nil {
			return nil, err
		}
		if prefix != "" && !strings.HasPrefix(p.Name, prefix) {
			continue
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// DropObject removes one object we own. The registry row is soft-deleted by
// MarkObjectDropped, which lives on the target database (the registry is in
// the target, the slot/publication is on the source).
//
// 只允许本包认识的两种对象；名字必须带 jl_ 前缀（命名空间纪律）。活动 slot
// 由数据库自己拒绝（pg_drop_replication_slot 会报错），所以误删风险还有一层
// 数据库侧保护。
func (d *DB) DropObject(ctx context.Context, name, kind string) error {
	if len(name) < 3 || name[:3] != "jl_" {
		return fmt.Errorf("拒绝删除命名空间外的对象 %q（只清 jl_ 前缀）", name)
	}
	switch kind {
	case "slot":
		if d.kind != KindPostgres {
			return fmt.Errorf("MySQL 源端没有 slot 可删")
		}
		if _, err := d.db.ExecContext(ctx, `SELECT pg_drop_replication_slot($1)`, name); err != nil {
			return fmt.Errorf("删除书签 %s 失败（可能仍在使用）: %w", name, err)
		}
	case "publication":
		if d.kind != KindPostgres {
			return fmt.Errorf("MySQL 源端没有 publication 可删")
		}
		if _, err := d.db.ExecContext(ctx, `DROP PUBLICATION `+d.quoteIdent(name)); err != nil {
			return fmt.Errorf("删除发布清单 %s 失败: %w", name, err)
		}
	default:
		return fmt.Errorf("不认识的对象类型 %q（只清 slot / publication）", kind)
	}
	return nil
}

// MarkObjectDropped soft-deletes the registry row after a successful drop.
func (d *DB) MarkObjectDropped(ctx context.Context, name string) error {
	exists, err := d.HasTable(ctx, d.schema, TableObject)
	if err != nil || !exists {
		return err
	}
	_, err = d.db.ExecContext(ctx,
		`UPDATE `+d.qualify(d.schema, TableObject)+
			` SET dropped_at = CURRENT_TIMESTAMP WHERE object_name = `+d.placeholder(1), name)
	return err
}
