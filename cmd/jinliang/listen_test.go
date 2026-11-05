package main

import "testing"

// P1 回归：":8080" / "0.0.0.0:8080" / "[::]:8080" 都是「绑所有网卡」，
// 必须被判成 non-loopback，否则「暴露到网络」的风险提示不会打印。
func TestIsLoopbackListen(t *testing.T) {
	cases := []struct {
		listen string
		want   bool
		why    string
	}{
		{"127.0.0.1:8080", true, "显式本机"},
		{"127.0.0.1:0", true, "本机随机端口"},
		{"localhost:8080", true, "主机名 localhost"},
		{"[::1]:8080", true, "IPv6 本机"},
		{"::1:8080", false, "IPv6 不带方括号时无法解析，按不安全处理"},
		{"0.0.0.0:8080", false, "显式全网卡"},
		{":8080", false, "空 host = 绑全网卡（P1 就是这里判错了）"},
		{"", false, "空地址等于全网卡"},
		{"[::]:8080", false, "IPv6 全网卡"},
		{"192.168.1.10:8080", false, "具体网卡 IP"},
		{"0.0.0.0", false, "只有 host 没有端口"},
		{"127.0.0.1", true, "只有本机 host"},
	}
	for _, c := range cases {
		if got := isLoopbackListen(c.listen); got != c.want {
			t.Errorf("isLoopbackListen(%q) = %v, want %v（%s）", c.listen, got, c.want, c.why)
		}
	}
}

func TestListenHost(t *testing.T) {
	cases := map[string]string{
		"127.0.0.1:8080": "127.0.0.1",
		":8080":          "",
		"[::1]:8080":     "::1",
		"[::]:8080":      "::",
		"localhost:8080": "localhost",
		"127.0.0.1":      "127.0.0.1",
	}
	for in, want := range cases {
		if got := listenHost(in); got != want {
			t.Errorf("listenHost(%q) = %q, want %q", in, got, want)
		}
	}
}
