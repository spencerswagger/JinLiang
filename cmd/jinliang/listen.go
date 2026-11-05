package main

import "github.com/spencerswagger/jinliang/internal/netcheck"

// 判定逻辑在 internal/netcheck（httpapi 的「首启只允许本机」也用同一份），
// 这里只留薄封装，免得两处各写一份然后分叉。

func listenHost(listen string) string     { return netcheck.Host(listen) }
func isLoopbackListen(listen string) bool { return netcheck.IsLoopbackListen(listen) }
