package control

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(Options{DataDir: t.TempDir(), Autogen: true})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func seedConnections(t *testing.T, s *Store) (src, tgt Connection) {
	t.Helper()
	ctx := context.Background()
	src, err := s.CreateConnection(ctx, ConnectionInput{
		Name: "源库", Kind: "postgres", Host: "127.0.0.1", Port: 5432,
		Database: "jinliang_src", Username: "jinliang", Password: "src-secret",
		Params: map[string]string{"sslmode": "disable"},
	})
	if err != nil {
		t.Fatalf("create source: %v", err)
	}
	tgt, err = s.CreateConnection(ctx, ConnectionInput{
		Name: "目标库", Kind: "mysql", Host: "127.0.0.1", Port: 3306,
		Database: "jinliang_test", Username: "jinliang", Password: "tgt-secret",
	})
	if err != nil {
		t.Fatalf("create target: %v", err)
	}
	return src, tgt
}

func sampleJobInput(src, tgt Connection) JobInput {
	return JobInput{
		Name: "orders-to-report", SourceID: src.ID, TargetID: tgt.ID,
		Spec: JobDoc{
			Source: SourceDoc{Scope: &ScopeDoc{Schema: "public", Include: []string{"orders", "customers"}}},
			Target: TargetDoc{Schema: "report"},
			Mode:   ModeDoc{Frequency: "once"},
			Content: &ContentDoc{Tables: map[string]TableDoc{
				"orders":    {},
				"customers": {},
			}},
		},
	}
}

func TestOpenCreatesSchema(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	for _, table := range []string{"connections", "jobs", "runs", "settings", "audit"} {
		var name string
		err := s.DB().QueryRowContext(ctx,
			`SELECT name FROM sqlite_master WHERE type='table' AND name = ?`, table).Scan(&name)
		if err != nil {
			t.Fatalf("table %s missing: %v", table, err)
		}
	}
	if v, err := s.SchemaVersion(ctx); err != nil || v != LatestSchemaVersion() {
		t.Fatalf("schema version = %d err=%v", v, err)
	}
	if id, err := s.InstanceID(ctx); err != nil || len(id) != 26 {
		t.Fatalf("instance id = %q err=%v", id, err)
	}
	if res, err := s.IntegrityCheck(ctx); err != nil || res != "ok" {
		t.Fatalf("integrity = %q err=%v", res, err)
	}
}

func TestDataSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	s1, err := Open(Options{DataDir: dir, Autogen: true})
	if err != nil {
		t.Fatal(err)
	}
	src, tgt := seedConnections(t, s1)
	job, err := s1.CreateJob(ctx, sampleJobInput(src, tgt))
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}

	// 验收 ②：重启进程后连接与任务仍在（落盘，不是内存）
	s2, err := Open(Options{DataDir: dir, Autogen: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	conns, err := s2.ListConnections(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(conns) != 2 {
		t.Fatalf("connections after reopen = %d, want 2", len(conns))
	}
	got, err := s2.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("job after reopen: %v", err)
	}
	if got.JobKey != job.JobKey || got.Name != job.Name {
		t.Fatalf("job changed across restart: %+v vs %+v", got, job)
	}
	// 密钥复用：密文必须还能解开
	secret, err := s2.Secret(ctx, src.ID)
	if err != nil || secret != "src-secret" {
		t.Fatalf("secret after reopen = %q err=%v", secret, err)
	}
}

func TestConnectionSecretIsEncryptedAndNeverSerialised(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	c, err := s.CreateConnection(ctx, ConnectionInput{
		Name: "prod", Kind: "postgres", Host: "db", Port: 5432,
		Database: "app", Username: "u", Password: "hunter2",
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(c.SecretEnc), "hunter2") {
		t.Fatal("secret_enc must not contain the plaintext")
	}
	if !c.HasSecret() {
		t.Fatal("has_secret must be true")
	}
	b, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "hunter2") || strings.Contains(string(b), "secret_enc") {
		t.Fatalf("connection JSON must not carry the secret: %s", b)
	}
	list, err := s.ListConnections(ctx)
	if err != nil {
		t.Fatal(err)
	}
	lb, _ := json.Marshal(list)
	if strings.Contains(string(lb), "hunter2") {
		t.Fatalf("list JSON leaked the password: %s", lb)
	}
}

func TestUpdateKeepsPasswordUnlessCleared(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	c, err := s.CreateConnection(ctx, ConnectionInput{
		Name: "prod", Kind: "postgres", Host: "db", Port: 5432,
		Database: "app", Username: "u", Password: "keep-me",
	})
	if err != nil {
		t.Fatal(err)
	}
	// 改名字不带口令 → 口令必须还在（表单留空=不修改）
	if _, err := s.UpdateConnection(ctx, c.ID, ConnectionInput{
		Name: "prod-renamed", Kind: "postgres", Host: "db", Port: 5432,
		Database: "app", Username: "u",
	}); err != nil {
		t.Fatal(err)
	}
	if secret, err := s.Secret(ctx, c.ID); err != nil || secret != "keep-me" {
		t.Fatalf("password lost on update: %q err=%v", secret, err)
	}
	// 显式清空才清
	u, err := s.UpdateConnection(ctx, c.ID, ConnectionInput{
		Name: "prod", Kind: "postgres", Host: "db", Port: 5432,
		Database: "app", Username: "u", ClearPassword: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if u.HasSecret() {
		t.Fatal("clear_password should wipe the secret")
	}
}

func TestDeleteConnectionRefusedWhileReferenced(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	src, tgt := seedConnections(t, s)
	if _, err := s.CreateJob(ctx, sampleJobInput(src, tgt)); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteConnection(ctx, src.ID); err == nil {
		t.Fatal("must refuse to delete a connection used by a job")
	}
	if err := s.DeleteConnection(ctx, tgt.ID); err == nil {
		t.Fatal("must refuse to delete a connection used by a job")
	}
}

func TestJobKeyStableAcrossPasswordChangeAndRangeChange(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	src, tgt := seedConnections(t, s)
	job, err := s.CreateJob(ctx, sampleJobInput(src, tgt))
	if err != nil {
		t.Fatal(err)
	}
	// 换口令不换身份（docs/IDENTITY.md §2）
	if _, err := s.UpdateConnection(ctx, src.ID, ConnectionInput{
		Name: "源库", Kind: "postgres", Host: "127.0.0.1", Port: 5432,
		Database: "jinliang_src", Username: "jinliang", Password: "rotated",
		Params: map[string]string{"sslmode": "disable"},
	}); err != nil {
		t.Fatal(err)
	}
	updated, err := s.UpdateJob(ctx, job.ID, sampleJobInput(src, tgt))
	if err != nil {
		t.Fatal(err)
	}
	if updated.JobKey != job.JobKey {
		t.Fatalf("job_key changed after password rotation: %s → %s", job.JobKey, updated.JobKey)
	}

	// 换表范围就是另一个任务：能建，且指纹不同
	in := sampleJobInput(src, tgt)
	in.Name = "only-orders-diff-range"
	in.Spec.Content = &ContentDoc{Tables: map[string]TableDoc{"orders": {}}}
	newJob, err := s.CreateJob(ctx, in)
	if err != nil {
		t.Fatalf("different table range should be a new job: %v", err)
	}
	if newJob.JobKey == job.JobKey {
		t.Fatal("different table range must change job_key")
	}
	other := sampleJobInput(src, tgt)
	other.Name = "only-orders-same-range"
	if _, err := s.CreateJob(ctx, other); err == nil {
		t.Fatal("same range must conflict on job_key")
	}
}

func TestDuplicateJobRejectedWithSameFingerprint(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	src, tgt := seedConnections(t, s)
	if _, err := s.CreateJob(ctx, sampleJobInput(src, tgt)); err != nil {
		t.Fatal(err)
	}
	dup := sampleJobInput(src, tgt)
	dup.Name = "别的名字"
	_, err := s.CreateJob(ctx, dup)
	if err == nil {
		t.Fatal("duplicate fingerprint must be refused")
	}
	if !strings.Contains(err.Error(), "同一个任务") {
		t.Fatalf("message should explain it is the same job: %v", err)
	}
}

func TestExportOmitsPasswordAndImportsBack(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	src, tgt := seedConnections(t, s)
	job, err := s.CreateJob(ctx, sampleJobInput(src, tgt))
	if err != nil {
		t.Fatal(err)
	}
	yamlBytes, err := s.ExportYAML(ctx, job)
	if err != nil {
		t.Fatal(err)
	}
	out := string(yamlBytes)
	for _, secret := range []string{"src-secret", "tgt-secret", "password"} {
		if strings.Contains(out, secret) {
			t.Fatalf("export leaked %q:\n%s", secret, out)
		}
	}
	if !strings.Contains(out, "env:JINLIANG_CONN_") {
		t.Fatalf("export must reference connections by placeholder:\n%s", out)
	}
	doc, err := ParseJobYAML(yamlBytes)
	if err != nil {
		t.Fatalf("exported yaml does not parse back: %v", err)
	}
	if err := doc.Validate(); err != nil {
		t.Fatalf("exported yaml does not validate: %v", err)
	}

	// 同一台机器上再导入一次：应该更新原任务而不是报冲突（幂等）
	again, err := s.ImportYAML(ctx, yamlBytes, ImportOptions{Actor: ActorCLI})
	if err != nil {
		t.Fatalf("re-import: %v", err)
	}
	if again.ID != job.ID {
		t.Fatalf("re-import should update the same job: %s vs %s", again.ID, job.ID)
	}
}

func TestImportCreatesConnectionsFromEnv(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	const yamlDoc = `
version: 1
job: from-file
source:
  kind: postgres
  dsn: env:MY_SRC
  scope: {schema: public, include: [orders]}
target:
  kind: mysql
  dsn: env:MY_TGT
  schema: report
mode:
  frequency: once
content:
  tables:
    orders: {}
`
	env := map[string]string{
		"MY_SRC": "postgres://jinliang:pw@127.0.0.1:5432/jinliang_src?sslmode=disable",
		"MY_TGT": "mysql://jinliang:pw@127.0.0.1:3306/jinliang_test",
	}
	job, err := s.ImportYAML(ctx, []byte(yamlDoc), ImportOptions{
		EnvLookup: func(k string) (string, bool) { v, ok := env[k]; return v, ok },
	})
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	if job.Name != "from-file" || job.JobKey == "" {
		t.Fatalf("job = %+v", job)
	}
	conns, err := s.ListConnections(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(conns) != 2 {
		t.Fatalf("import should create 2 connections, got %d", len(conns))
	}
	for _, c := range conns {
		if !c.HasSecret() {
			t.Fatalf("connection %s lost its password during import", c.Name)
		}
	}
	// 第二次导入：不重复建连接
	if _, err := s.ImportYAML(ctx, []byte(yamlDoc), ImportOptions{
		EnvLookup: func(k string) (string, bool) { v, ok := env[k]; return v, ok },
	}); err != nil {
		t.Fatalf("second import: %v", err)
	}
	conns, _ = s.ListConnections(ctx)
	if len(conns) != 2 {
		t.Fatalf("second import duplicated connections: %d", len(conns))
	}
}

func TestImportRejectsPlaintextDSN(t *testing.T) {
	s := newTestStore(t)
	const bad = `
version: 1
job: bad
source: {kind: postgres, dsn: "postgres://u:p@h:5432/db"}
target: {kind: mysql, dsn: env:X}
mode: {frequency: once}
`
	if _, err := s.ImportYAML(context.Background(), []byte(bad), ImportOptions{}); err == nil {
		t.Fatal("plaintext dsn must be refused (docs/SECURITY.md §1)")
	}
}

func TestRunsPersistAndPhaseTransitionsAreChecked(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	src, tgt := seedConnections(t, s)
	job, err := s.CreateJob(ctx, sampleJobInput(src, tgt))
	if err != nil {
		t.Fatal(err)
	}
	run, err := s.StartRun(ctx, job.ID, "preflight")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.UpdateRunPhase(ctx, run.ID, "snapshot"); err == nil {
		t.Fatal("preflight → snapshot is not a legal transition")
	}
	if _, err := s.UpdateRunPhase(ctx, run.ID, "prepare"); err != nil {
		t.Fatalf("preflight → prepare should be legal: %v", err)
	}
	if _, err := s.UpdateRunPhase(ctx, run.ID, "snapshot"); err != nil {
		t.Fatalf("prepare → snapshot should be legal: %v", err)
	}
	if _, err := s.FinishRun(ctx, run.ID, "done", "", 0, FinishFailed); err == nil {
		t.Fatal("snapshot → done is illegal and must be refused")
	}
	latest, err := s.LatestRun(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if latest.ID != run.ID || latest.Phase != "snapshot" {
		t.Fatalf("latest run = %+v", latest)
	}
}

func TestAuditRecordsWrites(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	if err := s.Audit(ctx, ActorCLI, ActionConnCreate, map[string]any{"name": "x"}); err != nil {
		t.Fatal(err)
	}
	entries, err := s.ListAudit(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Action != ActionConnCreate {
		t.Fatalf("audit = %+v", entries)
	}
}

func TestDataDirPermissionsTightened(t *testing.T) {
	dir := t.TempDir()
	loose := filepath.Join(dir, "cfg")
	if err := os.MkdirAll(loose, 0o755); err != nil {
		t.Fatal(err)
	}
	s, err := Open(Options{DataDir: loose, Autogen: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	info, err := os.Stat(loose)
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm&0o077 != 0 {
		t.Fatalf("data dir stayed world-readable: %04o", perm)
	}
	dbInfo, err := os.Stat(filepath.Join(loose, DBFileName))
	if err != nil {
		t.Fatal(err)
	}
	if perm := dbInfo.Mode().Perm(); perm != 0o600 {
		t.Fatalf("db perm = %04o, want 0600", perm)
	}
}

// P2 回归：能写控制库文件的人把 A 行的密文搬进 B 行，必须解不开。
// 这条是 AAD 绑定的存在理由（crypto.go 的 Seal/Open 都带 aad）。
func TestSwappedCiphertextDoesNotDecrypt(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	a, err := s.CreateConnection(ctx, ConnectionInput{
		Name: "alice", Kind: "postgres", Host: "h", Port: 5432,
		Database: "d", Username: "alice", Password: "alice-pw",
	})
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.CreateConnection(ctx, ConnectionInput{
		Name: "bob", Kind: "postgres", Host: "h", Port: 5432,
		Database: "d", Username: "bob", Password: "bob-pw",
	})
	if err != nil {
		t.Fatal(err)
	}
	// 模拟拿到库写权限的攻击者：把 alice 的密文搬到 bob 这一行
	if _, err := s.DB().ExecContext(ctx,
		`UPDATE connections SET secret_enc = (SELECT secret_enc FROM connections WHERE id = ?) WHERE id = ?`,
		a.ID, b.ID); err != nil {
		t.Fatal(err)
	}
	if pw, err := s.Secret(ctx, b.ID); err == nil {
		t.Fatalf("换行后的密文不该解开，却拿到了 %q", pw)
	} else if !errors.Is(err, ErrAADMismatch) {
		t.Fatalf("应该是 ErrAADMismatch，实际 %v", err)
	}
	// alice 自己仍然正常
	if pw, err := s.Secret(ctx, a.ID); err != nil || pw != "alice-pw" {
		t.Fatalf("alice 的密文应仍然可解：%q %v", pw, err)
	}
}
