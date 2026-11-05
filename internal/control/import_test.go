package control

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// P0-2 回归：`export --all` 写出的是 `---` 多文档，而导入过去只 Decode 一次，
// 恢复备份时静默丢掉除第一份以外的全部任务、退出码 0。这个测试就是那个场景：
// 3 个任务 → 导出成一份多文档流 → 全新控制库导入 → 必须回来 3 个。
func TestImportAllDocumentsFromExportAll(t *testing.T) {
	src := newTestStore(t)
	ctx := context.Background()
	s1, t1 := seedConnections(t, src)

	var stream strings.Builder
	// 同一个「源+目标+表范围」只能有一个任务（job_key 指纹），所以三份任务
	// 各选不同的表，模拟的是真实的多任务备份，而不是三个重复任务。
	tables := []string{"orders", "customers", "invoices"}
	for i, name := range []string{"job-a", "job-b", "job-c"} {
		in := sampleJobInput(s1, t1)
		in.Name = name
		in.Spec.Source.Scope.Include = []string{tables[i]}
		in.Spec.Content = &ContentDoc{Tables: map[string]TableDoc{tables[i]: {}}}
		job, err := src.CreateJob(ctx, in)
		if err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
		doc, err := src.ExportYAML(ctx, job)
		if err != nil {
			t.Fatalf("export %s: %v", name, err)
		}
		stream.WriteString("---\n")
		stream.Write(doc)
	}

	docs, err := ParseJobYAMLDocs([]byte(stream.String()))
	if err != nil {
		t.Fatalf("parse multi-doc: %v", err)
	}
	if len(docs) != 3 {
		t.Fatalf("解析出 %d 份，期望 3 份（这就是静默丢任务的老 bug）", len(docs))
	}

	// 换一个控制库导入：模拟「换机器恢复备份」。
	dst := newTestStore(t)
	// 导出用的是 env:JINLIANG_CONN_<id> 占位符；换机器恢复时由环境变量提供真实 DSN。
	// 这里给两端各一个「环境变量」，模拟运维把备份搬到了另一台机器。
	lookup := func(name string) (string, bool) {
		norm := strings.ToLower(strings.ReplaceAll(name, "_", "-"))
		switch {
		case strings.HasSuffix(norm, strings.ToLower(s1.ID)):
			return "postgres://jinliang:pw@127.0.0.1:5432/jinliang_src?sslmode=disable", true
		case strings.HasSuffix(norm, strings.ToLower(t1.ID)):
			return "mysql://jinliang:pw@127.0.0.1:3306/jinliang_test", true
		}
		return "", false
	}
	rep, err := dst.ImportYAMLDocs(ctx, []byte(stream.String()), ImportOptions{Actor: ActorCLI, EnvLookup: lookup})
	if err != nil {
		t.Fatalf("import multi-doc: %v", err)
	}
	if rep.ImportedCount() != 3 || len(rep.Skipped) != 0 {
		t.Fatalf("报告不对：%s（skipped=%v）", rep.Summary(), rep.Skipped)
	}
	jobs, err := dst.ListJobs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 3 {
		t.Fatalf("控制库里有 %d 个任务，期望 3 个", len(jobs))
	}
	names := map[string]bool{}
	for _, j := range jobs {
		names[j.Name] = true
	}
	for _, want := range []string{"job-a", "job-b", "job-c"} {
		if !names[want] {
			t.Fatalf("恢复后缺少任务 %s（现有 %v）", want, names)
		}
	}
}

// 一份坏文档不能让其余文档一起被吞掉，而且必须如实报 N/M。
func TestImportReportsSkippedDocument(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	src, tgt := seedConnections(t, s)

	good := sampleJobInput(src, tgt)
	doc, err := s.marshalForTest(ctx, good)
	if err != nil {
		t.Fatal(err)
	}
	// 第二份故意缺 target.dsn 之外的必填项：job 名字是空的 → 校验失败。
	bad := strings.Replace(string(doc), "job: orders-to-report", "job: \"\"", 1)
	stream := "---\n" + string(doc) + "---\n" + bad

	rep, err := s.ImportYAMLDocs(ctx, []byte(stream), ImportOptions{Actor: ActorCLI})
	if err != nil && rep.ImportedCount() == 0 {
		t.Fatalf("至少第一份应该成功: %v", err)
	}
	if rep.ImportedCount() != 1 || len(rep.Skipped) != 1 {
		t.Fatalf("期望 1 成功 / 1 跳过，实际 %s", rep.Summary())
	}
	if rep.Skipped[0].Index != 2 {
		t.Fatalf("跳过项的序号应是 2（第几份），实际 %d", rep.Skipped[0].Index)
	}
	if !strings.Contains(rep.Skipped[0].Reason, "job") && !strings.Contains(rep.Skipped[0].Reason, "任务") {
		t.Fatalf("跳过原因要能看懂: %q", rep.Skipped[0].Reason)
	}
}

// 单文档入口收到多份时必须明确报错，不能悄悄只取第一份。
func TestParseJobYAMLRefusesMultipleDocuments(t *testing.T) {
	one := "job: a\nsource: {kind: postgres, dsn: env:X}\ntarget: {kind: mysql, dsn: env:Y, schema: r}\nmode: {frequency: once}\n"
	_, err := ParseJobYAML([]byte("---\n" + one + "---\n" + one))
	if err == nil || !strings.Contains(err.Error(), "2") {
		t.Fatalf("多份输入应报错并说明份数，实际 %v", err)
	}
}

// 空文档（只有注释）不该被当成任务，也不该让整条导入失败。
func TestParseJobYAMLDocsSkipsEmptyDocuments(t *testing.T) {
	one := "job: a\nsource: {kind: postgres, dsn: env:X}\ntarget: {kind: mysql, dsn: env:Y, schema: r}\nmode: {frequency: once}\n"
	docs, err := ParseJobYAMLDocs([]byte("# 备份文件\n---\n\n---\n" + one + "---\n# 结尾注释\n"))
	if err != nil {
		t.Fatal(err)
	}
	if len(docs) != 1 {
		t.Fatalf("空文档被算进任务了：%d 份", len(docs))
	}
	if _, err := ParseJobYAMLDocs([]byte("# 只有注释\n")); err == nil {
		t.Fatal("没有任何任务时要报错")
	}
}

// marshalForTest renders one job as yaml (same path the CLI export uses).
func (s *Store) marshalForTest(ctx context.Context, in JobInput) ([]byte, error) {
	job, err := s.CreateJob(ctx, in)
	if err != nil {
		return nil, fmt.Errorf("create: %w", err)
	}
	return s.ExportYAML(ctx, job)
}
