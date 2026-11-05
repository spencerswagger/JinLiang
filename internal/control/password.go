package control

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"golang.org/x/crypto/argon2"
)

// 管理员口令用 argon2id 存 PHC 串（docs/CONTROL-PLANE.md §8）。
// 参数取 OWASP 2024 建议的下限之上：64 MiB / 3 轮 / 4 并行。
// 单管理员、登录频次极低，这点内存换来的抗爆破很划算。

const (
	argonTime    uint32 = 3
	argonMemory  uint32 = 64 * 1024 // KiB
	argonThreads uint8  = 4
	argonKeyLen  uint32 = 32
	argonSaltLen        = 16
	// MinPasswordLen 是口令下限。短于这个长度直接拒绝，不给"弱口令能用"的选项。
	MinPasswordLen = 10
)

var b64 = base64.RawStdEncoding

// HashPassword returns a PHC-formatted argon2id hash, e.g.
// $argon2id$v=19$m=65536,t=3,p=4$<salt>$<hash>
func HashPassword(password string) (string, error) {
	if len(password) < MinPasswordLen {
		return "", fmt.Errorf("口令至少 %d 个字符", MinPasswordLen)
	}
	salt := make([]byte, argonSaltLen)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}
	key := argon2.IDKey([]byte(password), salt, argonTime, argonMemory, argonThreads, argonKeyLen)
	return fmt.Sprintf("$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s",
		argon2.Version, argonMemory, argonTime, argonThreads,
		b64.EncodeToString(salt), b64.EncodeToString(key)), nil
}

// VerifyPassword reports whether password matches the stored PHC hash.
// Comparison is constant-time; a malformed hash is an error, not a mismatch,
// so the caller can tell "库坏了" from "口令错了".
func VerifyPassword(encoded, password string) (bool, error) {
	params, salt, want, err := decodePHC(encoded)
	if err != nil {
		return false, err
	}
	got := argon2.IDKey([]byte(password), salt, params.time, params.memory, params.threads, uint32(len(want)))
	return subtle.ConstantTimeCompare(got, want) == 1, nil
}

type argonParams struct {
	time    uint32
	memory  uint32
	threads uint8
}

func decodePHC(encoded string) (argonParams, []byte, []byte, error) {
	var p argonParams
	parts := strings.Split(encoded, "$")
	// ["", "argon2id", "v=19", "m=65536,t=3,p=4", "<salt>", "<hash>"]
	if len(parts) != 6 || parts[1] != "argon2id" {
		return p, nil, nil, errors.New("口令哈希格式不对（不是 argon2id PHC 串）")
	}
	var version int
	if _, err := fmt.Sscanf(parts[2], "v=%d", &version); err != nil {
		return p, nil, nil, fmt.Errorf("口令哈希版本无法解析: %w", err)
	}
	if version != argon2.Version {
		return p, nil, nil, fmt.Errorf("口令哈希版本 %d 与本二进制(%d)不一致", version, argon2.Version)
	}
	if _, err := fmt.Sscanf(parts[3], "m=%d,t=%d,p=%d", &p.memory, &p.time, &p.threads); err != nil {
		return p, nil, nil, fmt.Errorf("口令哈希参数无法解析: %w", err)
	}
	salt, err := b64.DecodeString(parts[4])
	if err != nil {
		return p, nil, nil, err
	}
	hash, err := b64.DecodeString(parts[5])
	if err != nil {
		return p, nil, nil, err
	}
	return p, salt, hash, nil
}
