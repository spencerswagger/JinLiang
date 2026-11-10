package engine

// 「停止请求之后才到的真实流错误」不能被记成「已停止」。
//
// 背景（评审风险）：`target.go` 的 streamGeneric 收尾分支里有一格
//
//	case stopHit.Load() || idleHit.Load():
//	        code = ExitStopped
//
// 它的本意是：**我们**要求收尾（按停 / --stop-after / 追平）之后，ctx 被 cancel，
// 有的连接器会把这个中断冒成一个网络错误（例如 MySQL 源的 `use of closed network
// connection`），那种错误不该算失败。
//
// 但这一格的判据只看「我们有没有要停」，不看「这条错误是不是取消造成的」。于是
// 一个**与取消无关的真实错误**只要晚到一步（用户刚好按了停、或刚好追平），就会被
// 吞成「已停止」：退出码 130、finish_reason=stopped、last_error 为空——
// 用户看到的是「我自己停的」，实际是源端/网络报错，下一条排查线索全丢了。
//
// 触发条件（本用例构造的就是它）：
//   1. 流已经跑起来，`--stop-after`（或追平）先把 stopHit 置 true 并 cancel ctx；
//   2. 连接器的 Stream 在 ctx 取消之后，因为**自己的原因**返回一个非取消错误
//      （binlog 被清理 / 复制权限被收回 / 目标端网络断开……）；
//   3. 引擎在 switch 里命中 `stopHit.Load()` 那一格，把真实错误丢掉。
//
// 判据：这种错误必须**返回给调用方**（run 记 failed + last_error 带原话），
// 不能走 drain(ExitStopped)。用例本身不改实现，只钉行为。

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/spencerswagger/jinliang/internal/control"
	"github.com/spencerswagger/jinliang/internal/dsn"
	"github.com/spencerswagger/jinliang/internal/mysqlsink"
	"github.com/spencerswagger/jinliang/internal/sink"
	"github.com/spencerswagger/jinliang/internal/source"
	"github.com/spencerswagger/jinliang/internal/state"
)

// lateErrSource：等 ctx 被取消（= 引擎自己要停），再返回一个与取消无关的真实错误。
//
// 嵌一个 runSource 接口只为了「不实现的方法留白」：本用例只走 Stream / Ack /
// LastCommit / IsZero / Advance / Backlog 这几条，其余方法不会被调用（调了会 panic，
// 正好把「用例依赖了契约外的路径」暴露出来）。
type lateErrSource struct {
	runSource
	err   error
	delay time.Duration
}

func (f *lateErrSource) Stream(ctx context.Context, from source.Position, fn source.EventHandler) error {
	<-ctx.Done()        // 引擎自己要求收尾：stopHit 已经置位、ctx 已 cancel
	time.Sleep(f.delay) // 让开关先走完（真实连接器也是「稍后才冒出来」）
	return f.err        // 但这条错误与取消无关：必须被当成失败
}
func (f *lateErrSource) Ack(pos source.Position)     {}
func (f *lateErrSource) LastCommit() source.Position { return nil }
func (f *lateErrSource) IsZero(p source.Position) bool {
	return p == nil
}
func (f *lateErrSource) Advance(ctx context.Context, pos source.Position) error { return nil }
func (f *lateErrSource) Before(a, b source.Position) bool                       { return false }
func (f *lateErrSource) Now(ctx context.Context) (source.Position, error)       { return nil, nil }
func (f *lateErrSource) ServerEnd() source.Position                             { return nil }
func (f *lateErrSource) SafeAckPoint() (source.Position, bool)                  { return nil, false }
func (f *lateErrSource) Kind() string                                           { return "postgres" }
func (f *lateErrSource) Backlog(ctx context.Context, from source.Position) (int64, error) {
	return -1, nil // -1 = 不知道（契约：不许用 0 冒充「已追平」）
}

// lateEndpoint 只给错误文案用（tgtEP.Kind），不真连库。
func lateEndpoint(t *testing.T, raw string) dsn.Endpoint {
	t.Helper()
	ep, err := dsn.Parse(raw)
	if err != nil {
		t.Fatalf("解析 DSN: %v", err)
	}
	return ep
}

// quietSink 是最小的目标端：不写任何东西，只声明位点语义（ack 守卫要求）。
type quietSink struct{}

func (q *quietSink) Kind() string { return "postgres" }
func (q *quietSink) EnsureSchema(ctx context.Context, cat source.Catalog) ([]sink.Action, error) {
	return nil, nil
}
func (q *quietSink) Apply(ctx context.Context, evs []source.Event, pos source.Position) error {
	return nil
}
func (q *quietSink) Verify(ctx context.Context, table source.TableRef) (int64, error) { return 0, nil }
func (q *quietSink) Cleanup(ctx context.Context) error                                { return nil }
func (q *quietSink) Close() error                                                     { return nil }
func (q *quietSink) PositionDurability() string                                       { return "same_tx" }
func (q *quietSink) PositionDurable(ctx context.Context, want string) (mysqlsink.PositionState, error) {
	return mysqlsink.PositionDurable, nil
}

// TestStopRequestDoesNotSwallowLateRealError 钉住「停止请求之后的错误**不能一律吞**」。
//
// 两条口径必须同时成立（Core #99 把 `isLocalClose` 收窄到「本地关闭」四种形态，
// 裸 `io.EOF` 刻意不认——对端发 FIN ≠ 我们自己关的连接）：
//
//	· 真实业务/协议错误（如 binlog 被清理）→ 必须失败并把根因写进 last_error；
//	· 裸 `io.EOF`（对端走了）→ **也必须是失败**，不许被当成「我们按了停」。
//
// 第二条就是本用例的 mmol：把 `io.EOF` 加进 `isLocalClose` 的识别集合 ⇒ 这一格必红。
func TestStopRequestDoesNotSwallowLateRealError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string // last_error 里必须出现的根因片段
	}{
		{"real-error", errors.New("源端报错：binlog 已被清理（Error 1236），增量无法继续"), "binlog 已被清理"},
		{"bare-eof", io.EOF, "EOF"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store, jobID := newControlStore(t)
			run, err := store.StartRun(context.Background(), jobID, state.Streaming, control.RunKindSync)
			if err != nil {
				t.Fatalf("建 run 失败: %v", err)
			}

			r := &runner{
				opt: Options{
					Store:     store,
					StopAfter: 40 * time.Millisecond, // 先触发「我们自己要停」
					IdleAfter: 0,
				},
				job:   control.Job{ID: jobID},
				run:   run,
				phase: state.Streaming,
				src:   &lateErrSource{err: tc.err, delay: 30 * time.Millisecond},
				gsink: &quietSink{},
				tgtEP: lateEndpoint(t, "postgres://u:p@127.0.0.1:5432/db"),
				srcEP: lateEndpoint(t, "postgres://u:p@127.0.0.1:5432/db"),
			}

			err = r.streamGeneric(context.Background(), nil, nil)
			if err == nil {
				t.Fatalf("错误被吞了：streamGeneric 返回 nil（run 会被记成 stopped，last_error 为空）")
			}
			var done *reachedDone
			if errors.As(err, &done) {
				t.Fatalf("错误被记成「正常收尾」（reachedDone）：退出码会变成 %d（已停止）", ExitStopped)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("错误内容被替换了（根因丢了）：%v", err)
			}

			// 引擎的真实调用方是 `run()`：它拿到 streamGeneric 的错误后交给 fail() 落库。
			_ = r.fail(context.Background(), err)

			got, err := store.GetRun(context.Background(), run.ID)
			if err != nil {
				t.Fatal(err)
			}
			if got.FinishReason == control.FinishStopped {
				t.Fatalf("run 被记成 stopped（finish_reason=%q，exit=%v）：错误在停止之后才到，不能算「我们停的」",
					got.FinishReason, got.ExitCode)
			}
			if !strings.Contains(got.LastError, tc.want) {
				t.Fatalf("last_error 没留下根因（%q）：%q", tc.want, got.LastError)
			}
		})
	}
}
