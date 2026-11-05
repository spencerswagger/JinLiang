package control

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
)

// UUIDv7 (RFC 9562): 48-bit millisecond timestamp, then randomness. We use it
// for every primary key in the control plane because it sorts by creation time,
// which makes "recent runs" queries cheap without an extra index.
//
// We generate it ourselves instead of pulling a dependency: it is 20 lines and
// keeps the dependency list honest (docs/SECURITY.md §6).

var (
	uuidMu     sync.Mutex
	uuidLastMS int64
	uuidSeq    [10]byte // monotonic counter inside the same millisecond
)

// NewID returns a fresh UUIDv7 in canonical 8-4-4-4-12 form.
func NewID() (string, error) {
	return newIDAt(time.Now())
}

// MustID is NewID for callers that treat a crypto/rand failure as fatal.
func MustID() string {
	id, err := NewID()
	if err != nil {
		panic("jinliang: 无法生成 ID（crypto/rand 失败）: " + err.Error())
	}
	return id
}

func newIDAt(now time.Time) (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	ms := now.UnixMilli()

	uuidMu.Lock()
	if ms <= uuidLastMS {
		// Same millisecond (or the clock stepped back): keep ordering by
		// incrementing the counter instead of emitting a smaller timestamp.
		ms = uuidLastMS
		for i := len(uuidSeq) - 1; i >= 0; i-- {
			uuidSeq[i]++
			if uuidSeq[i] != 0 {
				break
			}
		}
	} else {
		uuidLastMS = ms
		uuidSeq = [10]byte{}
	}
	seq := uuidSeq
	uuidMu.Unlock()

	b[0] = byte(ms >> 40)
	b[1] = byte(ms >> 32)
	b[2] = byte(ms >> 24)
	b[3] = byte(ms >> 16)
	b[4] = byte(ms >> 8)
	b[5] = byte(ms)
	b[6] = 0x70 | (b[6] & 0x0f) // version 7
	b[7] = b[7] & 0x3f
	// Fill the 62 random bits that follow rand_a with the monotonic counter so
	// IDs generated inside one millisecond still sort deterministically.
	b[6] ^= seq[0] & 0x0f
	b[7] ^= seq[1] & 0x3f
	b[8] = b[8]&0xc0 | seq[2]&0x3f
	b[9] = 0x80 | (b[9] & 0x3f) // variant 10
	b[10] ^= seq[3]
	b[11] ^= seq[4]
	b[12] ^= seq[5]
	b[13] ^= seq[6]
	b[14] ^= seq[7]
	b[15] ^= seq[8] ^ seq[9]

	h := hex.EncodeToString(b[:])
	return fmt.Sprintf("%s-%s-%s-%s-%s", h[0:8], h[8:12], h[12:16], h[16:20], h[20:32]), nil
}
