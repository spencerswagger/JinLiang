package control

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/spencerswagger/jinliang/internal/state"
)

// 守卫：有未结束的 run 时不许删任务——否则就是「记录没了、引擎还在写、租约还在续期」，
// 同目标新建任务一直撞 E1005，而用户没有办法让那个进程停下来。
// 这个判断放在数据入口（DeleteJob）而不是每个调用方，两个入口各写一遍迟早漏一个。
func TestDeleteJobRefusesWhenRunActive(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	src, tgt := seedConnections(t, store)
	job, err := store.CreateJob(ctx, sampleJobInput(src, tgt))
	if err != nil {
		t.Fatal(err)
	}
	run, err := store.StartRun(ctx, job.ID, state.Preflight, RunKindSync)
	if err != nil {
		t.Fatal(err)
	}
	err = store.DeleteJob(ctx, job.ID)
	if err == nil {
		t.Fatal("有未结束的 run 时必须拒删")
	}
	if !errors.Is(err, ErrRunActive) {
		t.Fatalf("应是 ErrRunActive，得到 %v", err)
	}
	// 文案要给出可执行的路（先停再删），而不是只说「不行」
	for _, want := range []string{"jinliang delete", run.ID, "先停"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("拒绝理由里应包含 %q：%s", want, err.Error())
		}
	}
	// 收尾之后才允许删
	// 正常收尾路径是 preflight → failed/interrupted（preflight 不能直接到 done）
	if _, err := store.FinishRun(ctx, run.ID, state.Failed, "测试收尾", 1, FinishFailed); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteJob(ctx, job.ID); err != nil {
		t.Fatalf("运行结束后应能删：%v", err)
	}
}
