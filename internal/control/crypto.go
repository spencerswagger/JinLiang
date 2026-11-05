package control

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spencerswagger/jinliang/internal/core"
)

// KeySource says where the encryption key came from. It is shown in the UI and
// in `doctor` so the user always knows which key protects the control database
// (and therefore which file to back up).
type KeySource string

const (
	KeyFromFlag KeySource = "key-file" // --key-file 显式指定
	KeyFromEnv  KeySource = "env"      // JINLIANG_KEY
	KeyFromData KeySource = "data-dir" // 首启生成 <data-dir>/key
	KeyLen                = 32         // AES-256
	keyFileName           = "key"      // <data-dir>/key
	nonceLen              = 12         // GCM 标准 nonce 长度
)

// ErrKeyPermTooOpen is returned when a key file is readable by anyone but its
// owner. This is the one place in the product that hard-stops (docs/SECURITY.md
// §1): a key file that other users can read is a compromised key file.
var ErrKeyPermTooOpen = errors.New("key file 权限过宽")

// Cipher encrypts connection secrets with AES-256-GCM.
//
// 每条密文独立 nonce，格式为 nonce || ciphertext+tag。同一明文加密两次结果不同，
// 所以密文不能用来做相等比较——需要比较时请先解密。
type Cipher struct {
	aead   cipher.AEAD
	source KeySource
	path   string // 密钥文件路径（env 来源时为空）
}

// KeySource reports where the key was loaded from.
func (c *Cipher) KeySource() KeySource { return c.source }

// KeyPath returns the on-disk key file, or "" when the key came from the
// environment. Backups: 备份控制库必须同时备份这个文件，否则密文解不开（这是特性）。
func (c *Cipher) KeyPath() string { return c.path }

// Seal encrypts plaintext into nonce||ciphertext||tag.
func (c *Cipher) Seal(plaintext []byte) ([]byte, error) {
	nonce := make([]byte, nonceLen)
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("生成 nonce 失败: %w", err)
	}
	out := make([]byte, nonceLen, nonceLen+len(plaintext)+c.aead.Overhead())
	copy(out, nonce)
	return c.aead.Seal(out, nonce, plaintext, nil), nil
}

// Open decrypts a blob produced by Seal. A wrong key or a tampered blob fails
// here rather than returning garbage, which is the whole point of GCM.
func (c *Cipher) Open(blob []byte) ([]byte, error) {
	if len(blob) < nonceLen+c.aead.Overhead() {
		return nil, errors.New("密文长度不足，控制库可能损坏")
	}
	nonce, ct := blob[:nonceLen], blob[nonceLen:]
	out, err := c.aead.Open(nil, nonce, ct, nil)
	if err != nil {
		return nil, fmt.Errorf("解密失败（key 文件与密文不匹配，或控制库损坏）: %w", err)
	}
	return out, nil
}

// newCipher wraps a 32-byte key.
func newCipher(key []byte, source KeySource, path string) (*Cipher, error) {
	if len(key) != KeyLen {
		return nil, fmt.Errorf("密钥必须是 %d 字节，实际 %d", KeyLen, len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &Cipher{aead: aead, source: source, path: path}, nil
}

// KeyOptions selects where the master key comes from.
//
// 优先级（docs/CONTROL-PLANE.md §5）：--key-file → JINLIANG_KEY → <data-dir>/key。
// 什么都没给就在 data-dir 里生成一个 0600 的 key，这样用户不用管密钥也能开工。
type KeyOptions struct {
	KeyFile string // --key-file
	DataDir string // 首启生成 key 的落点
	EnvKey  string // JINLIANG_KEY 的值（调用方从环境读，便于测试注入）
	// Autogen permits creating <data-dir>/key. Read-only commands (export)
	// leave it false so they never invent a key that would decrypt nothing.
	Autogen bool
}

// LoadCipher resolves the master key. Errors carry a four-part prompt so the UI
// can render them directly (docs/ERRORS.md).
func LoadCipher(opts KeyOptions) (*Cipher, error) {
	if opts.KeyFile != "" {
		key, err := readKeyFile(opts.KeyFile)
		if err != nil {
			return nil, err
		}
		return newCipher(key, KeyFromFlag, opts.KeyFile)
	}
	if strings.TrimSpace(opts.EnvKey) != "" {
		key, err := parseKeyMaterial([]byte(strings.TrimSpace(opts.EnvKey)))
		if err != nil {
			return nil, fmt.Errorf("JINLIANG_KEY 无法解析: %w", err)
		}
		return newCipher(key, KeyFromEnv, "")
	}
	if opts.DataDir == "" {
		return nil, errors.New("没有可用的密钥来源：请给 --key-file、JINLIANG_KEY 或 --data-dir")
	}
	path := filepath.Join(opts.DataDir, keyFileName)
	if _, err := os.Stat(path); err == nil {
		key, kerr := readKeyFile(path)
		if kerr != nil {
			return nil, kerr
		}
		return newCipher(key, KeyFromData, path)
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if !opts.Autogen {
		return nil, fmt.Errorf("找不到密钥文件 %s（先跑一次 jinliang serve 生成）", path)
	}
	key := make([]byte, KeyLen)
	if _, err := rand.Read(key); err != nil {
		return nil, err
	}
	if err := writeFile0600(path, key); err != nil {
		return nil, err
	}
	return newCipher(key, KeyFromData, path)
}

// readKeyFile loads a key and enforces 0600.
func readKeyFile(path string) ([]byte, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("读不到密钥文件 %s: %w", path, err)
	}
	if info.Mode().Perm()&0o077 != 0 {
		spec, _ := core.Lookup(core.CodeSecretPerm)
		return nil, fmt.Errorf("%w: %s 权限是 %04o，必须是 0600。%s",
			ErrKeyPermTooOpen, path, info.Mode().Perm(), strings.Join(spec.Next, "；"))
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return parseKeyMaterial(raw)
}

// parseKeyMaterial accepts raw 32 bytes, 64 hex chars, or base64. Accepting the
// encoded forms matters for JINLIANG_KEY, where users copy a printable string
// out of a secret manager.
func parseKeyMaterial(raw []byte) ([]byte, error) {
	if len(raw) == KeyLen {
		return raw, nil
	}
	s := strings.TrimSpace(string(raw))
	if b, err := hex.DecodeString(s); err == nil && len(b) == KeyLen {
		return b, nil
	}
	if b, err := base64.StdEncoding.DecodeString(s); err == nil && len(b) == KeyLen {
		return b, nil
	}
	if b, err := base64.RawStdEncoding.DecodeString(s); err == nil && len(b) == KeyLen {
		return b, nil
	}
	return nil, fmt.Errorf("密钥格式不对：需要 32 字节原始值、64 位 hex 或 base64（当前 %d 字节）", len(raw))
}

// writeFile0600 creates path with 0600, replacing an existing file atomically
// enough for our purposes (temp file + rename in the same directory).
func writeFile0600(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}
