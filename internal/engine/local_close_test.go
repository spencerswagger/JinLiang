package engine

// 收尾判定的歧义 проходит здесь（维护者 2026-08-11 止损指令 + Gate 正在写的回归用例）：
// 「停止请求之后才到的真实错误」不许被记成「已停止」。
//
// 这条边界只在真库上暴露（MySQL 源把关闭冒成 `use of closed network connection`，
// 而目标库断连是同一条 read 路径上的网络错误），所以判据必须**看错误形状**，
// 不是只看「我们先要停」。这里把两边都钉住。

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
)

func TestIsLocalCloseAcceptsOurOwnCancel(t *testing.T) {
	cases := []error{
		context.Canceled,
		fmt.Errorf("读变更流失败: %w", context.Canceled), // 包一层也认得出
		net.ErrClosed,
		errors.New("read tcp 127.0.0.1:3306: use of closed network connection"),
		errors.New("connection is closed"),
	}
	for _, err := range cases {
		if !isLocalClose(err) {
			t.Errorf("%v 应判成「本地主动关闭」（这是我们自己 cancel 打断读的常见形状）", err)
		}
	}
}

func TestIsLocalCloseRejectsRealFailures(t *testing.T) {
	cases := []error{
		nil,
		errors.New("unexpected EOF"), // 对端在报文中间断了
		errors.New("read tcp: connection reset by peer"), // 对端/网络的问题
		errors.New("write tcp: broken pipe"),
		errors.New("目标表没有唯一键，无法做 upsert"), // 写入错误（另有 b.Err() 通道）
		errors.New("i/o timeout"),
		errors.New("Error 1146: Table 'x' doesn't exist"),
		errors.New("解码失败：事务边界丢失"),
	}
	for _, err := range cases {
		if isLocalClose(err) {
			t.Errorf("%v 是**真实故障**，不许判成「我们自己要停的」——那会把退出码记成 0、"+
				"用户以为一切正常而数据停在半路", err)
		}
	}
}
