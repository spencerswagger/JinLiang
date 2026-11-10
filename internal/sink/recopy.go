package sink

// 「整表重搬窗口」的上下文标记（契约 §1.6 步 3 的实现细节）。
//
// 背景：目标端的写路径有一道**最后一道防线**——收到暂停表的事件就报错，防的是
// 「引擎漏过滤 → 静默把行写进一张结构没对齐的表」。但「整表重搬」是**故意**写这张表：
// 那时暂停记录还在库里（按契约 §1.6，记录要留到搬成才解除），所以防线会误伤重搬。
//
// 解法不是「重搬前先删记录」——那会打开一个崩溃窗口：清空了、记录也没了、搬一半进程死了，
// 下一轮既不会重搬也不会暂停，表停在一半而退出码 0（静默缺数据）。
// 正确做法是**记录留到搬成**，重搬期间用这个上下文标记告诉写路径「这几张表的写入是重搬，放行」。
//
// 为什么放在 contract 包：两个目标端（pgsink / mysqlsink）都要读这个标记，
// 而它们只共享 `internal/sink`；放引擎里它们读不到，放各自包里就要抄两份。

import "context"

type recopyKey struct{}

// recopyScope 记录「这一轮重搬涉及哪些表」（按目标端裸表名）。
type recopyScope map[string]bool

// WithRecopy 把一个重搬窗口挂到 ctx 上（可多次调用，累加）。
func WithRecopy(ctx context.Context, tables ...string) context.Context {
	cur, _ := ctx.Value(recopyKey{}).(recopyScope)
	next := make(recopyScope, len(cur)+len(tables))
	for k := range cur {
		next[k] = true
	}
	for _, t := range tables {
		if t != "" {
			next[t] = true
		}
	}
	return context.WithValue(ctx, recopyKey{}, next)
}

// InRecopy 报告这张表是不是在「整表重搬窗口」里（写路径据此放行）。
func InRecopy(ctx context.Context, table string) bool {
	scope, _ := ctx.Value(recopyKey{}).(recopyScope)
	return scope[table]
}
