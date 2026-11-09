//go:build integration

package mysqlsource

import (
	"context"
	"testing"
	"time"

	"github.com/spencerswagger/jinliang/internal/source"
)

// DELETE 事件必须带「删除前的值」。
//
// 为什么单独钉这一条：行事件只有一份镜像，解析器原来把它一律放进 New
// （INSERT 是对的），DELETE 就成了「没有值的删除」——目标端按空值拼 WHERE，
// 静默删 0 行：数据永远删不掉，而日志里一切正常。这是「看起来成功」里最难查的一类。
func TestIntegrationDeleteCarriesBeforeImage(t *testing.T) {
	s := openSource(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	exec(t, s, "DROP TABLE IF EXISTS del_img")
	exec(t, s, "CREATE TABLE del_img (id INT PRIMARY KEY, v VARCHAR(16), n DECIMAL(10,2))")
	t.Cleanup(func() { exec(t, s, "DROP TABLE IF EXISTS del_img") })

	start, err := s.Status(ctx)
	if err != nil {
		t.Fatalf("读位点失败: %v", err)
	}
	exec(t, s, "INSERT INTO del_img VALUES (1,'a',1.10),(2,'b',2.20)")
	exec(t, s, "DELETE FROM del_img WHERE id=2")

	streamCtx, stop := context.WithTimeout(ctx, 15*time.Second)
	defer stop()
	var del *source.Event
	err = s.Stream(streamCtx, start, func(c context.Context, ev source.Event) error {
		if ev.Kind == source.EvDelete {
			e := ev
			del = &e
			stop()
		}
		return nil
	})
	_ = err
	if del == nil {
		t.Fatal("没有收到 DELETE 事件")
	}
	if len(del.Old.Values) != 3 {
		t.Fatalf("删除事件应带 3 列删除前的值，实际 %d（New 有 %d）", len(del.Old.Values), len(del.New.Values))
	}
	if got := del.Old.Values[0]; got.Kind != source.KindInt || got.I != 2 {
		t.Fatalf("删除前的 id 应是 2，实际 %+v", got)
	}
	if del.Table.Table != "del_img" || del.Table.Schema == "" {
		t.Fatalf("删除事件要带表名（含 schema），实际 %+v", del.Table)
	}
}
