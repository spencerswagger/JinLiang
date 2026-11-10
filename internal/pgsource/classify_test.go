package pgsource

// PG 侧的语义标记（契约 docs/CONTRACT-PAUSE-AND-POSITION.md §4.4）。
//
// 引擎不再 import 本包：它只问 `source.ClassOf(err)`。所以「哪条错误算权限/缺对象/位点失效」
// 必须在这里翻成标记——尤其是**措辞型**的位点失效（SQLSTATE 认不出来，只有连接器知道）。

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/spencerswagger/jinliang/internal/source"
)

func TestMarkClassMapsPGSemantics(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want source.Class
	}{
		{"42501 权限不足", &pgconn.PgError{Code: "42501", Message: "permission denied for table orders"}, source.ClassNoPrivilege},
		{"3F000 缺 schema", &pgconn.PgError{Code: "3F000", Message: `schema "report" does not exist`}, source.ClassObjectMissing},
		{"42P01 缺表", &pgconn.PgError{Code: "42P01", Message: `relation "public.orders" does not exist`}, source.ClassObjectMissing},
		// 措辞型：SQLSTATE 55000 本身不说明什么，只有连接器知道这是「书签被作废」。
		{"槽被作废（措辞）", &pgconn.PgError{Code: "55000",
			Message: `replication slot "jl_x_1" was invalidated`}, source.ClassPositionGap},
		{"55000 + wal_level", &pgconn.PgError{Code: "55000",
			Message: "logical decoding requires wal_level >= logical"}, source.ClassWalLevel},
		{"53400 资源满", &pgconn.PgError{Code: "53400", Message: "all replication slots are in use"}, source.ClassResourceLimit},
	}
	for _, c := range cases {
		marked := markClass(c.err)
		if got := source.ClassOf(marked); got != c.want {
			t.Errorf("%s：应标记为 %s，得到 %s", c.name, c.want, got)
		}
		// 标记不能改错误文本，也不能断 errors.Is/As 链（用户看到的仍是原始报错）。
		if marked.Error() != c.err.Error() {
			t.Errorf("%s：标记后错误文本变了", c.name)
		}
		if !errors.Is(marked, c.err) {
			t.Errorf("%s：标记后 errors.Is 断了", c.name)
		}
	}
}

func TestMarkClassLeavesUnknownAlone(t *testing.T) {
	err := errors.New("some odd failure")
	if got := source.ClassOf(markClass(err)); got != source.ClassUnknown {
		t.Fatalf("认不出来的错误不该被标类（引擎会如实说「未识别」），得到 %s", got)
	}
	// 包一层也要认（prepare/stream 的出口会把内部错误 %w 包起来）。
	wrapped := fmt.Errorf("准备源端对象失败: %w", &pgconn.PgError{Code: "42501", Message: "denied"})
	if got := source.ClassOf(markClass(wrapped)); got != source.ClassNoPrivilege {
		t.Fatalf("包一层的 42501 也应标出来，得到 %s", got)
	}
}
