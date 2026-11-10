package engine

// 「这条流错误是不是**我们自己**把读打断造成的？」——收尾判定的最后一道歧义。
//
// 背景有两个，都是真跑撞出来的：
//  1. 我们要求收尾（按停 / `--stop-after` / `--stop-when-idle`）时会 `cancelStream()`，
//     打断阻塞读；★有些连接器不返回 `context.Canceled`，而是把关闭冒成网络错误
//     （MySQL 源的 `use of closed network connection`）。所以不能只判 `ctx.Err()`。
//  2. 但反过来也不能「只要 `stopHit/idleHit` 置着就当正常停」——停止请求之后**才到**的
//     真实流错误（目标库断连、权限被回收、解码失败）会被记成「已停止」，退出码 0，
//     用户以为一切正常（维护者 2026-11-11 止损指令点名的就是这条）。
//
// 判据因此收窄成：**错误本身长得像「本地主动关闭」**。这类错误的特点是「本进程把 fd 关了」，
// 不是对端拒绝或数据出错。`b.Err()`（后台提交失败）在调用点更早就已经优先返回，
// 所以这里只需要管「读路径」这一种。

import (
	"context"
	"errors"
	"net"
	"strings"
)

// isLocalClose reports whether err looks like "we closed this connection ourselves".
//
// 认的形状（都来自「本进程 close 之后继续读写」）：
//   - `context.Canceled` —— 标准库路径；
//   - `net.ErrClosed`（含 `use of closed network connection` 这一族）；
//   - 少数驱动把关闭包成文本：`connection is closed`。
//
// 刻意**不认**：`connection reset by peer`、`broken pipe`、超时、认证/权限/解码错误——
// 那些要么是对端/网络的问题，要么是数据的问题，都不该被记成「用户按了停」。
func isLocalClose(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, net.ErrClosed) {
		return true
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "use of closed network connection"):
		return true
	case strings.Contains(msg, "connection is closed"):
		return true
	}
	// 裸 `EOF` **刻意不认**：Go 里「我们自己关了 conn」之后读返回的是 `net.ErrClosed`
	//（`use of closed network connection`，上面已认）；而 `io.EOF` 的含义是**对端发了 FIN**，
	// 也就是「对端走了」——把它写成「我们按了停」正是这条止损要禁的事。
	// 两个连接器在 ctx 结束时都会先返回 `ctx.Err()`（pgsource 循环顶 + ReceiveMessage 失败后；
	// mysqlsource 侧是 #91 修的），所以正常收尾拿到的是 `context.Canceled`，不是裸 EOF。
	// 真有连接器冒裸 EOF，那是它的 bug，让它以「假失败」暴露比悄悄吞掉好。
	return false
}
