// Package netcheck answers one question in exactly one place: 「这个地址/这个请求
// 是本机的吗」。
//
// 为什么单独成包：控制台默认只监听 127.0.0.1，但 `--listen` 可以放开；放开之后
// 「谁在访问」就必须能判定——首启设口令（A-1）和 doctor 的风险提示都靠它。
// 判定逻辑写两份迟早会分叉，所以 cmd/jinliang 与 internal/httpapi 共用这一份。
package netcheck

import (
	"net"
	"strings"
)

// Host splits "host:port" / "[::1]:8080" / ":8080" into the host part.
//
// 注意 ":8080" 的 host 是空串，它等价于「绑所有网卡」，**不是** loopback。
func Host(addr string) string {
	host, _, err := net.SplitHostPort(strings.TrimSpace(addr))
	if err != nil {
		// 没有端口（或格式不规范）：把整串当 host，交给 ParseIP 判断。
		return strings.Trim(strings.TrimSpace(addr), "[]")
	}
	return host
}

// IsLoopbackListen reports whether this listen address only accepts local
// connections. 空 host 返回 false：那是 0.0.0.0 / ::（全部网卡）。
func IsLoopbackListen(listen string) bool {
	host := strings.TrimSpace(Host(listen))
	if host == "" {
		return false // ":8080" = 绑全网卡
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return false // 具体网卡名/IP 都可能被外面访问，按「不 loopback」处理
	}
	return ip.IsLoopback()
}

// IsLoopbackClient reports whether a request's RemoteAddr comes from this
// machine. 用于「首启设口令只允许本机」这类 fail-closed 判定。
//
// 只看 RemoteAddr，**不看** X-Forwarded-For / Host：那些头由客户端填写，
// 拿来当安全判定等于把门交给对方。反代场景下 RemoteAddr 是反代自己（本机），
// 仍需管理员显式把控制台放到可信网络里，这一点写在 serve 的风险提示里。
func IsLoopbackClient(remoteAddr string) bool {
	host := strings.TrimSpace(Host(remoteAddr))
	if host == "" {
		return false
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}
	return ip.IsLoopback()
}
