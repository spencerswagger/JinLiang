package engine

// MySQL 目标端没有「按表暂停」那条状态列（B2 才做同构列）。
// 这里钉住三件事：**明确失败**（不是静默跳过）、**挂 E1014 家族**、**不假装登记成功**。
//
// 为什么必须能跑出失败再上 B2：这条路径一旦「UPDATE 影响 0 行当成功」，用户会以为
// 暂停登记生效了，而下一轮又从头踩一遍——那是最难查的一种。

import (
	"errors"
	"strings"
	"testing"

	"github.com/spencerswagger/jinliang/internal/core"
	"github.com/spencerswagger/jinliang/internal/dsn"
	"github.com/spencerswagger/jinliang/internal/pgsink"
)

func TestPauseRegistrationOnMySQLTargetFailsLoudly(t *testing.T) {
	r := &runner{tgtEP: dsn.Endpoint{Kind: dsn.KindMySQL, Database: "jinliang_test"}}
	inner := &pgsink.TypeChangedError{TableName: "orders", Detail: "列 v：目标端当前是 text，源端是 bigint"}
	err := r.registerPause(t.Context(), inner)
	if err == nil {
		t.Fatal("MySQL 目标端无法登记暂停：必须返回错误，不能静默跳过")
	}
	var pe *pauseUnsupportedError
	if !errors.As(err, &pe) {
		t.Fatalf("应是 pauseUnsupportedError（目标端能力问题），得到 %T：%v", err, err)
	}
	// 挂码：E1014 家族（方向/能力不支持），不是「这条同步本身错了」。
	code, ok := classifyError(err)
	if !ok || code != core.CodeUnsupportedDirection {
		t.Fatalf("应挂 %s，得到 %q（classified=%v）", core.CodeUnsupportedDirection, code, ok)
	}
	// 四段齐全：发生了什么 / 你能做什么 / 风险，且带上原始原因。
	for _, want := range []string{"需要按表暂停", "你现在能做什么", "风险", "text", "bigint"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("文案缺 %q：\n%s", want, err.Error())
		}
	}
	// 结构上就不可能写进去：没有 PG sink 时 registerPause 不会走到 PauseTable。
	if r.sink != nil {
		t.Fatal("这条用例的前提是没有 PG sink（MySQL 目标端）")
	}
}

// fail() 的前缀在表名为空时不许拼出「（表 ，原因码 x）」这种残句。
func TestFailPrefixTolerantOfEmptyTable(t *testing.T) {
	var pe pgsink.PausableError = &pgsink.SchemaDriftError{TableName: "", Detail: "改名了"}
	if got := pauseHead("表结构不一致", pe); strings.Contains(got, "表 ，") || strings.Contains(got, "表 )") {
		t.Fatalf("表名为空时前缀出现了残句：%q", got)
	} else if !strings.Contains(got, "原因码") {
		t.Fatalf("表名缺失时仍要写原因码：%q", got)
	}
}
