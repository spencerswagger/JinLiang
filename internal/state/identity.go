package state

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base32"
	"fmt"
	"strings"
)

// 多个津梁实例可能连同一个源库/同一个目标库，所以「名字」不能当身份用。
//
// 三层身份，各管一件事：
//   1. job_key    —— 任务身份，由「源 + 目标 + 范围」的指纹决定，不含用户起的名字。
//                    两个实例算出同一个 job_key 才是同一个任务（此时靠租约排他）。
//   2. instance   —— 实例身份，只用于审计、租约持有者、对象所有权与 server_id 派生，
//                    **不参与**「是否同一任务」的判断。
//   3. object name —— 落在源库/目标库的对象名：jl_<job_key>_<n>，库内唯一。

const (
	keyLen      = 10 // base32 后 10 位 = 50 bit，碰撞概率在可接受范围
	instanceLen = 26 // base32(16 字节)=26
	// NameMax is the tightest identifier limit we target (PostgreSQL: 63 bytes).
	NameMax = 63
	// serverIDMin/Max keep derived ids away from the small values admins hand out.
	serverIDMin = 1 << 20
	serverIDMax = 1<<31 - 1
)

var b32 = base32.NewEncoding("abcdefghijklmnopqrstuvwxyz234567").WithPadding(base32.NoPadding)

// JobKey derives the stable identity of a sync job from its endpoints and scope.
// The user-visible `job:` name is display-only and may repeat across clusters.
func JobKey(sourceDSN, targetDSN, scope string) string {
	h := sha256.New()
	for _, part := range []string{
		normalizeSource(sourceDSN), // 去掉密码但保留主机/库/用户
		normalizeTarget(targetDSN),
		strings.ToLower(strings.Join(strings.Fields(scope), " ")),
	} {
		h.Write([]byte(part))
		h.Write([]byte{0})
	}
	return strings.ToLower(b32.EncodeToString(h.Sum(nil))[:keyLen])
}

// normalizeSource strips credentials so a rotated password does not change identity.
func normalizeSource(dsn string) string { return stripPassword(strings.TrimSpace(dsn)) }
func normalizeTarget(dsn string) string { return stripPassword(strings.TrimSpace(dsn)) }

// stripPassword keeps scheme://user@host (user and host matter, password does not).
func stripPassword(dsn string) string {
	i := strings.Index(dsn, "://")
	if i < 0 {
		return dsn
	}
	scheme, rest := dsn[:i], dsn[i+3:]
	at := strings.LastIndex(rest, "@")
	if at < 0 {
		return dsn
	}
	cred, host := rest[:at], rest[at:]
	if c := strings.Index(cred, ":"); c >= 0 {
		cred = cred[:c]
	}
	return scheme + "://" + cred + host
}

// ObjectName builds a database object name for this job, e.g. slot jl_abc1234567_1.
// Returns an error only when the result would exceed the tightest identifier limit.
func ObjectName(kind, jobKey string, n int) (string, error) {
	var base string
	switch kind {
	case "slot":
		base = fmt.Sprintf("jl_%s_%d", jobKey, n)
	case "publication":
		base = fmt.Sprintf("jl_%s", jobKey)
	case "state_prefix":
		base = fmt.Sprintf("jl_%s", jobKey)
	default:
		base = fmt.Sprintf("jl_%s_%d", jobKey, n)
	}
	if len(base) > NameMax {
		return "", fmt.Errorf("object name %q exceeds %d bytes", base, NameMax)
	}
	return base, nil
}

// NewInstanceID returns a fresh random instance id (base32 lowercase, 26 chars).
func NewInstanceID() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return b32.EncodeToString(buf), nil
}

// ServerID derives a MySQL replication server_id from the instance and job keys,
// so two instances never collide by accident (this is why we no longer need to
// stop and ask the user to pick one).
func ServerID(instanceID, jobKey string) uint32 {
	sum := sha256.Sum256([]byte(instanceID + "\x00" + jobKey))
	v := uint32(sum[0])<<24 | uint32(sum[1])<<16 | uint32(sum[2])<<8 | uint32(sum[3])
	span := uint32(serverIDMax - serverIDMin)
	return serverIDMin + v%span
}
