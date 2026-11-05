package control

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestSealOpenRoundTrip(t *testing.T) {
	c, err := newCipher(bytes.Repeat([]byte{7}, KeyLen), KeyFromEnv, "")
	if err != nil {
		t.Fatal(err)
	}
	plain := []byte("s3cr3t-密码-🔐")
	aad := []byte("conn-1")
	blob, err := c.Seal(aad, plain)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(blob, plain) {
		t.Fatal("ciphertext must not contain the plaintext")
	}
	got, err := c.Open(aad, blob)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, plain) {
		t.Fatalf("round trip mismatch: %q", got)
	}
}

func TestSealUsesFreshNonce(t *testing.T) {
	c, _ := newCipher(bytes.Repeat([]byte{1}, KeyLen), KeyFromEnv, "")
	aad := []byte("conn-1")
	a, err := c.Seal(aad, []byte("same"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := c.Seal(aad, []byte("same"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(a, b) {
		t.Fatal("same plaintext must not produce identical ciphertext (nonce reuse)")
	}
	if bytes.Equal(a[:nonceLen], b[:nonceLen]) {
		t.Fatal("nonces must differ per encryption")
	}
}

func TestOpenRejectsWrongKeyAndTamper(t *testing.T) {
	a, _ := newCipher(bytes.Repeat([]byte{1}, KeyLen), KeyFromEnv, "")
	b, _ := newCipher(bytes.Repeat([]byte{2}, KeyLen), KeyFromEnv, "")
	aad := []byte("conn-1")
	blob, err := a.Seal(aad, []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Open(aad, blob); err == nil {
		t.Fatal("wrong key must fail")
	}
	blob[len(blob)-1] ^= 0xff
	if _, err := a.Open(aad, blob); err == nil {
		t.Fatal("tampered ciphertext must fail")
	}
	if _, err := a.Open(aad, []byte{1, 2, 3}); err == nil {
		t.Fatal("short blob must fail")
	}
}

func TestLoadCipherKeyFilePermissions(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "key")
	if err := os.WriteFile(path, bytes.Repeat([]byte{9}, KeyLen), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadCipher(KeyOptions{KeyFile: path}); err != nil {
		t.Fatalf("0600 key should load: %v", err)
	}
	// 0644 是"别的用户也能读"，必须硬停（E1007）
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadCipher(KeyOptions{KeyFile: path}); err == nil {
		t.Fatal("0644 key file must be refused")
	}
}

func TestLoadCipherAcceptsEncodedKeys(t *testing.T) {
	hexKey := "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
	c, err := LoadCipher(KeyOptions{EnvKey: hexKey})
	if err != nil {
		t.Fatalf("hex key: %v", err)
	}
	if c.KeySource() != KeyFromEnv {
		t.Fatalf("source = %s", c.KeySource())
	}
	if _, err := LoadCipher(KeyOptions{EnvKey: "too-short"}); err == nil {
		t.Fatal("short key must be refused")
	}
}

func TestLoadCipherGeneratesDataDirKey(t *testing.T) {
	dir := t.TempDir()
	c, err := LoadCipher(KeyOptions{DataDir: dir, Autogen: true})
	if err != nil {
		t.Fatal(err)
	}
	if c.KeySource() != KeyFromData {
		t.Fatalf("source = %s", c.KeySource())
	}
	info, err := os.Stat(filepath.Join(dir, "key"))
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("generated key perm = %04o, want 0600", perm)
	}
	// 第二次必须复用同一把 key，否则之前加密的密文全废
	again, err := LoadCipher(KeyOptions{DataDir: dir, Autogen: true})
	if err != nil {
		t.Fatal(err)
	}
	blob, _ := c.Seal([]byte("conn-1"), []byte("x"))
	if _, err := again.Open([]byte("conn-1"), blob); err != nil {
		t.Fatalf("second load must reuse the same key: %v", err)
	}
}

func TestLoadCipherReadOnlyWithoutKey(t *testing.T) {
	dir := t.TempDir()
	if _, err := LoadCipher(KeyOptions{DataDir: dir, Autogen: false}); err == nil {
		t.Fatal("without Autogen a missing key must be an error, not a silent new key")
	}
}

func TestHashVerifyPassword(t *testing.T) {
	hash, err := HashPassword("correct-horse-battery")
	if err != nil {
		t.Fatal(err)
	}
	ok, err := VerifyPassword(hash, "correct-horse-battery")
	if err != nil || !ok {
		t.Fatalf("verify ok=%v err=%v", ok, err)
	}
	ok, err = VerifyPassword(hash, "wrong")
	if err != nil || ok {
		t.Fatalf("wrong password must not verify (ok=%v err=%v)", ok, err)
	}
	if _, err := HashPassword("short"); err == nil {
		t.Fatal("short password must be refused")
	}
	if _, err := VerifyPassword("not-a-phc-string", "x"); err == nil {
		t.Fatal("malformed hash must be an error, not a mismatch")
	}
}

// P2 回归：把 A 连接的密文搬进 B 行，必须解不开（AAD 绑定生效）。
func TestCipherBindsCiphertextToRow(t *testing.T) {
	c, err := newCipher(bytes.Repeat([]byte{3}, KeyLen), KeyFromEnv, "")
	if err != nil {
		t.Fatal(err)
	}
	blob, err := c.Seal([]byte("conn-A"), []byte("alice-password"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Open([]byte("conn-B"), blob); err == nil {
		t.Fatal("another row's AAD must not open this ciphertext")
	} else if !errors.Is(err, ErrAADMismatch) {
		t.Fatalf("error should be ErrAADMismatch, got %v", err)
	}
	// 同一行仍然正常
	if got, err := c.Open([]byte("conn-A"), blob); err != nil || string(got) != "alice-password" {
		t.Fatalf("same row must still decrypt: %q %v", got, err)
	}
}
