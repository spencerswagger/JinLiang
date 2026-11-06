package mysqlsink

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/spencerswagger/jinliang/internal/dsn"
	"github.com/spencerswagger/jinliang/internal/source"
)

// MySQL 目标端的运行时状态表（与 PG 侧 `_jl_*` 同一套语义，docs/STATE-SCHEMA.md）：
// 租约、位点、对象登记、块断点。没有这套表，PG→MySQL 的**增量就跑不了**——
// 引擎拿不到「谁在跑」「从哪儿续」，只能明确拒绝增量。
//
// 语义与 pgsink 对齐（不另发明）：
//   - 同一个 job_key 同时只允许一个实例写；TTL 30s；过期可抢占；
//   - 同机同 job：租约 label 里带 pid，进程已死可立刻接管（kill -9 后不用等 30s），
//     进程还活着则拒绝并告诉用户是哪个 pid；
//   - 位点写 `_jl_meta.position`。**注意限定**：只有**不含 DDL/TRUNCATE** 的批次才是
//     「数据 + 位点同一个事务」；含 DDL/TRUNCATE 的批次必须拆成三段——数据（带**旧**位点）
//     → DDL 单独执行 → 新位点单独提交，因为 MySQL 的 DDL/TRUNCATE 会隐式提交，塞进同一
//     事务时 `Commit` 退化成空操作（`#25` 的必改）。方向仍安全：崩在中间位点停在旧值，
//     重放时数据靠 upsert 幂等、DDL 靠 information_schema 判存在跳过。

// defaultLeaseTTL 与控制面心跳同阶（docs/STATE-SCHEMA.md §3）。
const defaultLeaseTTL = 30 * time.Second

// leaseRow 是租约表的一行。
type leaseRow struct {
	InstanceID string
	Label      string // "<job>#<pid>[#<token>]"
	Expires    time.Time
}

// LeaseHolder 是租约的只读视图（崩溃重启时判断「还有没有活着的进程」）。
type LeaseHolder struct {
	Exists     bool
	InstanceID string
	Label      string
	Expires    time.Time
}

// leaseOwnerLabel 渲染持有者：任务名#pid#token。
func leaseOwnerLabel(job string, pid int, token string) string {
	job = strings.TrimSpace(job)
	if job == "" {
		job = "job"
	}
	if token == "" {
		return fmt.Sprintf("%s#%d", job, pid)
	}
	return fmt.Sprintf("%s#%d#%s", job, pid, token)
}

// parseLeaseLabel 拆 "<job>#<pid>[#<token>]"：从右往左切，任务名里带 # 也不怕。
func parseLeaseLabel(label string) (pid int, token string, ok bool) {
	i := strings.LastIndex(label, "#")
	if i < 0 || i == len(label)-1 {
		return 0, "", false
	}
	last := label[i+1:]
	rest := label[:i]
	if p, err := strconv.Atoi(last); err == nil && p > 0 {
		return p, "", true
	}
	token = last
	j := strings.LastIndex(rest, "#")
	if j < 0 || j == len(rest)-1 {
		return 0, "", false
	}
	p, err := strconv.Atoi(rest[j+1:])
	if err != nil || p <= 0 {
		return 0, "", false
	}
	return p, token, true
}

// leaseTakeover 判断能不能接管（纯函数：好测、评审一眼看完所有分支）。
func leaseTakeover(cur leaseRow, exists bool, now time.Time, instanceID string,
	myPID int, myToken string, pidAlive func(int) bool) (ok bool, why string) {

	if !exists {
		return true, ""
	}
	if cur.Expires.Before(now) {
		return true, "上一个租约已经过期"
	}
	if cur.InstanceID != instanceID {
		return false, fmt.Sprintf("另一个实例 %s 正在跑（租约到 %s）",
			cur.InstanceID, cur.Expires.UTC().Format(time.RFC3339))
	}
	pid, token, hasPID := parseLeaseLabel(cur.Label)
	if !hasPID {
		return true, "接管了本机上一次运行留下的租约（那条记录没有 pid）"
	}
	if pid == myPID && (token == "" || token == myToken) {
		return true, "" // 自己：续租
	}
	if pidAlive(pid) {
		return false, fmt.Sprintf("同一台机器上还有一个进程在跑这个任务（pid %d）：等它结束，或在那边先停掉", pid)
	}
	return true, fmt.Sprintf("接管了本机崩溃进程（pid %d）留下的租约", pid)
}

// AcquireLease 拿租约；拿到返回 (true, 空/接管原因)；拿不到返回 (false, 谁在跑)。
func (s *Sink) AcquireLease(ctx context.Context, ttl time.Duration) (bool, string, error) {
	if ttl <= 0 {
		ttl = defaultLeaseTTL
	}
	label := leaseOwnerLabel(s.opt.Name, os.Getpid(), s.ownerToken)
	now := time.Now()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, "", err
	}
	defer tx.Rollback() //nolint:errcheck

	cur, exists, err := s.readLeaseForUpdate(ctx, tx)
	if err != nil {
		return false, "", err
	}
	ok, why := leaseTakeover(cur, exists, now, s.opt.InstanceID, os.Getpid(), s.ownerToken, processAlive)
	if !ok {
		return false, why, nil
	}
	expires := now.Add(ttl)
	if !exists {
		q := "INSERT INTO `_jl_lease` (job_key, instance_id, label, expires_at) VALUES (?,?,?,?)"
		if _, err := tx.ExecContext(ctx, q, s.opt.JobKey, s.opt.InstanceID, label, expires); err != nil {
			return false, "", fmt.Errorf("写租约失败: %w", err)
		}
	} else {
		q := "UPDATE `_jl_lease` SET instance_id=?, label=?, expires_at=? WHERE job_key=?"
		if _, err := tx.ExecContext(ctx, q, s.opt.InstanceID, label, expires, s.opt.JobKey); err != nil {
			return false, "", fmt.Errorf("续租失败: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return false, "", err
	}
	return true, why, nil
}

func (s *Sink) readLeaseForUpdate(ctx context.Context, tx *sql.Tx) (leaseRow, bool, error) {
	var r leaseRow
	err := tx.QueryRowContext(ctx,
		"SELECT instance_id, COALESCE(label,''), expires_at FROM `_jl_lease` WHERE job_key = ? FOR UPDATE",
		s.opt.JobKey).Scan(&r.InstanceID, &r.Label, &r.Expires)
	if err == sql.ErrNoRows {
		return leaseRow{}, false, nil
	}
	if err != nil {
		return leaseRow{}, false, err
	}
	return r, true, nil
}

// RenewLease 续租；返回 false 表示租约已经不是自己的（被接管/被删）。
func (s *Sink) RenewLease(ctx context.Context, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		ttl = defaultLeaseTTL
	}
	label := leaseOwnerLabel(s.opt.Name, os.Getpid(), s.ownerToken)
	res, err := s.db.ExecContext(ctx,
		"UPDATE `_jl_lease` SET expires_at = ? WHERE job_key = ? AND instance_id = ? AND label = ?",
		time.Now().Add(ttl), s.opt.JobKey, s.opt.InstanceID, label)
	if err != nil {
		return false, fmt.Errorf("续租失败: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// ReleaseLease 释放租约（只删自己的那条）。
func (s *Sink) ReleaseLease(ctx context.Context) error {
	label := leaseOwnerLabel(s.opt.Name, os.Getpid(), s.ownerToken)
	_, err := s.db.ExecContext(ctx,
		"DELETE FROM `_jl_lease` WHERE job_key = ? AND instance_id = ? AND label = ?",
		s.opt.JobKey, s.opt.InstanceID, label)
	if err != nil {
		return fmt.Errorf("释放租约失败: %w", err)
	}
	return nil
}

// ReadLease 只读租约（崩溃重启时判断「还有没有活着的进程」）。
func ReadLease(ctx context.Context, ep dsn.Endpoint, jobKey string) (LeaseHolder, error) {
	db, err := sql.Open("mysql", driverDSN(ep))
	if err != nil {
		return LeaseHolder{}, err
	}
	defer db.Close()
	var h LeaseHolder
	err = db.QueryRowContext(ctx,
		"SELECT instance_id, COALESCE(label,''), expires_at FROM `_jl_lease` WHERE job_key = ?",
		jobKey).Scan(&h.InstanceID, &h.Label, &h.Expires)
	if err == sql.ErrNoRows {
		return LeaseHolder{}, nil
	}
	if err != nil {
		return LeaseHolder{}, fmt.Errorf("读租约失败: %w", err)
	}
	h.Exists = true
	return h, nil
}

// EnsureMeta 建立/更新本 job 的元数据行（幂等）。
func (s *Sink) EnsureMeta(ctx context.Context, jobHash, phase, position, snapshotID string) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO _jl_meta (job_key, name, instance_id, schema_version, job_hash, phase, position, snapshot_id, started_at, updated_at)
		VALUES (?, ?, ?, 1, ?, ?, ?, ?, NOW(6), NOW(6))
		ON DUPLICATE KEY UPDATE
		  name=VALUES(name), instance_id=VALUES(instance_id), job_hash=VALUES(job_hash),
		  phase=VALUES(phase), snapshot_id=VALUES(snapshot_id), updated_at=NOW(6)`,
		s.opt.JobKey, s.opt.Name, s.opt.InstanceID, jobHash, phase, position, snapshotID)
	if err != nil {
		return fmt.Errorf("写 _jl_meta 失败: %w", err)
	}
	return nil
}

// SetPhase 只改阶段。
func (s *Sink) SetPhase(ctx context.Context, phase string) error {
	_, err := s.db.ExecContext(ctx,
		"UPDATE `_jl_meta` SET phase = ?, updated_at = NOW(6) WHERE job_key = ?", phase, s.opt.JobKey)
	if err != nil {
		return fmt.Errorf("写阶段失败: %w", err)
	}
	return nil
}

// State 读回阶段与位点。
func (s *Sink) State(ctx context.Context) (phase, position string, err error) {
	var ph, pos sql.NullString
	err = s.db.QueryRowContext(ctx,
		"SELECT phase, position FROM `_jl_meta` WHERE job_key = ?", s.opt.JobKey).Scan(&ph, &pos)
	if err == sql.ErrNoRows {
		return "", "", nil
	}
	if err != nil {
		return "", "", fmt.Errorf("读状态失败: %w", err)
	}
	return ph.String, pos.String, nil
}

// Position 只读位点（没有返回空串）。
func (s *Sink) Position(ctx context.Context) (string, error) { return s.MetaPosition(ctx) }

// DoneChunks 读回已完成的块（table → chunk_no → done）。
func (s *Sink) DoneChunks(ctx context.Context) (map[string]map[int]bool, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT table_name, chunk_no, done FROM `_jl_chunk` WHERE job_key = ?", s.opt.JobKey)
	if err != nil {
		return nil, fmt.Errorf("读块断点失败: %w", err)
	}
	defer rows.Close()
	out := map[string]map[int]bool{}
	for rows.Next() {
		var table string
		var chunk int
		var done bool
		if err := rows.Scan(&table, &chunk, &done); err != nil {
			return nil, err
		}
		if out[table] == nil {
			out[table] = map[int]bool{}
		}
		out[table][chunk] = done
	}
	return out, rows.Err()
}

// ResetChunks 清掉本 job 的块断点（重搬时用）。
func (s *Sink) ResetChunks(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, "DELETE FROM `_jl_chunk` WHERE job_key = ?", s.opt.JobKey); err != nil {
		return fmt.Errorf("清块断点失败: %w", err)
	}
	return nil
}

// TruncateTable 清空目标表（recreate/重搬路径用）。
func (s *Sink) TruncateTable(ctx context.Context, ref source.TableRef) error {
	if _, err := s.db.ExecContext(ctx, "TRUNCATE TABLE "+s.qualifiedOn(ref)); err != nil {
		return fmt.Errorf("清空 %s 失败: %w", qualified(ref), err)
	}
	return nil
}

// RecordObject 登记「这个对象是本任务建的」（孤儿判定与 recopy fail-closed 的依据）。
func (s *Sink) RecordObject(ctx context.Context, name, kind string) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO _jl_object (object_name, object_kind, job_key, instance_id, created_at)
		VALUES (?, ?, ?, ?, NOW(6))
		ON DUPLICATE KEY UPDATE object_kind=VALUES(object_kind), dropped_at=NULL`,
		name, kind, s.opt.JobKey, s.opt.InstanceID)
	if err != nil {
		return fmt.Errorf("登记对象 %s 失败: %w", name, err)
	}
	return nil
}

// MarkObjectDropped 软删对象登记（保留审计）。
func (s *Sink) MarkObjectDropped(ctx context.Context, name string) error {
	_, err := s.db.ExecContext(ctx,
		"UPDATE `_jl_object` SET dropped_at = NOW(6) WHERE object_name = ?", name)
	return err
}

// DidCreateTable 报告「目标表是不是本任务建的」——recopy 的 fail-closed 判据。
func (s *Sink) DidCreateTable(ctx context.Context, ref source.TableRef) (bool, error) {
	var n int
	err := s.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM `_jl_object` WHERE object_name = ? AND job_key = ? AND dropped_at IS NULL",
		s.tableObjectName(ref), s.opt.JobKey).Scan(&n)
	if err != nil {
		return false, fmt.Errorf("查对象登记失败: %w", err)
	}
	return n > 0, nil
}

// tableObjectName 是表在 `_jl_object` 里的登记名（与 PG 侧同构：schema.table）。
func (s *Sink) tableObjectName(ref source.TableRef) string {
	return s.schemaOf(ref) + "." + ref.Table + "#table"
}

// Count 目标表行数（全量/增量对账用）。
func (s *Sink) Count(ctx context.Context, ref source.TableRef) (int64, error) {
	var n int64
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+s.qualifiedOn(ref)).Scan(&n); err != nil {
		return 0, fmt.Errorf("数 %s 行数失败: %w", qualified(ref), err)
	}
	return n, nil
}
