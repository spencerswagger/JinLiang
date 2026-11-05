// Package netcheck answers one question in exactly one place: 「这个地址/这个请求
// 是本机的吗」。
//
// 为什么单独成包：控制台默认只监听 127.0.0.1，但 `--listen` 可以放开；放开之后
// 「谁在访问」就必须能判定——首启设口令（A-1）和 doctor 的风险提示都靠它。
// 判定逻辑写两份迟早会分叉，所以 cmd/jinliang 与 internal/httpapi 共用这一份。
package netcheck

import (
	"net"
	"net/http"
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

// forwardingHeaders 是反向代理会加的「原始客户端」头。
//
// 为什么它决定安全判定：标准部署里「控制台只听 127.0.0.1 + 同机 nginx 反代」时，
// 每个远端请求的 RemoteAddr **都是** 127.0.0.1——只看 IP 等于把门敞开。
// 所以 A-1 的守卫用 IsLoopbackRequest：只要带了任何一个转发头，就按非本机处理
// （fail-closed），宁可让反代场景的人回本机首启一次，也不让远端抢先当管理员。
var forwardingHeaders = []string{"Forwarded", "X-Forwarded-For", "X-Real-IP"}

// IsLoopbackRequest reports whether this request provably came from this
// machine: RemoteAddr 是本机，**且** 没有出现任何转发头。
//
// 注意：不解析转发头里的内容（那是不可信输入），只看「这个头在不在」。
//
// 为什么用 len(Values) 而不是 TrimSpace(Get()) != ""：`X-Forwarded-For: "  "`
// （空白值）在 TrimSpace 之后是空串，会被判成「没有转发头」→ 本机 → 放行。
// 反代如果原样透传客户端的头，远端就能这样绕过首启守卫（fail-open）。
// 头的存在本身就是「请求至少经过一跳」的证据，空白值同样是证据。
func IsLoopbackRequest(r *http.Request) bool {
	if r == nil {
		return false
	}
	for _, h := range forwardingHeaders {
		if len(r.Header.Values(h)) > 0 {
			return false // 头存在（哪怕是空白）= 请求至少经过一跳，不能算本机
		}
	}
	return IsLoopbackClient(r.RemoteAddr)
}
