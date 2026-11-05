package main

import (
	"net"
	"strings"
)

// listenHost splits "host:port" / "[::1]:8080" / ":8080" into the host part.
//
// 注意 ":8080" 的 host 是空串，它等价于「绑所有网卡」，**不是** loopback。
// 这一点写错会让「你把控制台暴露到网络上了」的警告在真正需要它的场景下不打印，
// 所以这里有单测钉住（isloopback_test.go）。
func listenHost(listen string) string {
	host, _, err := net.SplitHostPort(listen)
	if err != nil {
		// 没有端口（或格式不规范）：把整串当 host，交给 net.ParseIP 判断。
		return strings.Trim(strings.TrimSpace(listen), "[]")
	}
	return host
}

// isLoopbackListen reports whether this listen address only accepts local
// connections. 空 host 返回 false：那是 0.0.0.0 / ::（全部网卡）。
func isLoopbackListen(listen string) bool {
	host := strings.TrimSpace(listenHost(listen))
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
