package source

// 契约 docs/CONTRACT-PAUSE-AND-POSITION.md §4.4：引擎按**接口**判类，不 import 具体连接器。
// 这一层是那个接口的唯一实现点，用例钉住三件事：标记优先、SQLSTATE 兜底、没有标记时不瞎猜。

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestClassOfPrefersConnectorMark(t *testing.T) {
	// 连接器说「这是位点追不回来」，哪怕底层是个 SQLSTATE 55000，也按标记走：
	// 连接器掌握上下文（它知道自己在读 binlog 还是 WAL），措辞/码只是它的证据。
	inner := &pgconn.PgError{Code: "55000", Message: "logical decoding requires wal_level >= logical"}
	got := ClassOf(Mark(inner, ClassPositionGap))
	if got != ClassPositionGap {
		t.Fatalf("连接器的标记必须优先于 SQLSTATE 兜底，得到 %s", got)
	}
	// 底层错误不能被包丢：用户看到的报错文本与 errors.Is 链都要原样。
	if !errors.Is(Mark(inner, ClassPositionGap), inner) {
		t.Fatal("Mark 必须能 Unwrap 到底层错误（否则 last_error 与 Is/As 全断）")
	}
	if Mark(nil, ClassPositionGap) != nil {
		t.Fatal("Mark(nil) 必须返回 nil，否则调用方 return Mark(err,…) 会造假错误")
	}
}

func TestClassOfFallsBackToSQLState(t *testing.T) {
	cases := []struct {
		state string
		want  Class
	}{
		{"42501", ClassNoPrivilege},
		{"3F000", ClassObjectMissing},
		{"42P01", ClassObjectMissing},
		{"42703", ClassObjectMissing},
		{"53400", ClassResourceLimit},
		{"58P01", ClassPositionGap},
	}
	for _, c := range cases {
		err := &pgconn.PgError{Code: c.state, Message: "x"}
		if got := ClassOf(err); got != c.want {
			t.Errorf("SQLSTATE %s 应归 %s，得到 %s", c.state, c.want, got)
		}
	}
	// 包了几层也要认得出（classifyError 收到的是被 %w 包过的错误）。
	wrapped := fmt.Errorf("准备源端对象失败: %w", &pgconn.PgError{Code: "42501", Message: "permission denied"})
	if got := ClassOf(wrapped); got != ClassNoPrivilege {
		t.Fatalf("包了一层也应认出 42501，得到 %s", got)
	}
}

func TestClassOfDoesNotGuess(t *testing.T) {
	if got := ClassOf(errors.New("some database error we have not seen")); got != ClassUnknown {
		t.Fatalf("没标记 + 没 SQLSTATE → 必须是 unknown（不许瞎猜），得到 %s", got)
	}
	if got := ClassOf(nil); got != ClassUnknown {
		t.Fatalf("nil 应归 unknown，得到 %s", got)
	}
	// 55000 只有配上 wal_level 才算「没开变更流水」：别的 55000 不许被误判成 E1001。
	if got := ClassOf(&pgconn.PgError{Code: "55000", Message: "object not in prerequisite state"}); got != ClassUnknown {
		t.Fatalf("55000 但没有 wal_level 字样，不该判成 ClassWalLevel，得到 %s", got)
	}
	if got := ClassOf(&pgconn.PgError{Code: "55000", Message: "requires wal_level >= logical"}); got != ClassWalLevel {
		t.Fatalf("55000 + wal_level 才是 ClassWalLevel，得到 %s", got)
	}
}
