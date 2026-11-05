package main

import (
	"github.com/spencerswagger/jinliang/internal/netcheck"
)

// 判定逻辑只有一份：internal/netcheck（doctor、serve 警告、首启守卫共用）。
// 这里保留薄封装是为了让 cmd 层的调用点读起来仍是「这个监听地址安全吗」。
//
// 注意 ":8080" 的 host 是空串，它等价于「绑所有网卡」，**不是** loopback。
// 这一点写错会让「你把控制台暴露到网络上了」的警告在真正需要它的场景下不打印，
// 所以 netcheck 里有单测钉住（internal/netcheck/netcheck_test.go）。
func listenHost(listen string) string { return netcheck.Host(listen) }

// isLoopbackListen reports whether this listen address only accepts local
// connections. 空 host 返回 false：那是 0.0.0.0 / ::（全部网卡）。
func isLoopbackListen(listen string) bool { return netcheck.IsLoopbackListen(listen) }
