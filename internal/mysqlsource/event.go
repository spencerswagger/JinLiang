package mysqlsource

import (
	"encoding/binary"
	"fmt"
	"strings"
	"time"
)

// Binlog event types we consume. Numbers come from MySQL sql/log_event.h and
// are stable across 5.7 / 8.0 / 8.4 / 9.x — the *names* of the server
// variables changed, not the wire format.
const (
	evQuery             = 2
	evStop              = 3
	evRotate            = 4
	evFormatDescription = 15
	evXID               = 16
	evTableMap          = 19
	evWriteRowsV1       = 23
	evUpdateRowsV1      = 24
	evDeleteRowsV1      = 25
	evHeartbeat         = 27
	evIgnorable         = 28
	evWriteRowsV2       = 30
	evUpdateRowsV2      = 31
	evDeleteRowsV2      = 32
	evGTID              = 33
	evAnonymousGTID     = 34
	evPreviousGTIDs     = 35
	evTransactionCtx    = 36
	evViewChange        = 37
	evXAPrepare         = 38
	evPartialUpdateRows = 39
	evTransactionPaylod = 40
	evHeartbeatV2       = 41
	evGTIDTagged        = 42
)

// headerSize is the fixed part of every binlog event.
const headerSize = 19

// checksumLen is what CRC32 adds to every event when binlog_checksum=CRC32.
const checksumLen = 4

// Header is the 19-byte common prefix.
type Header struct {
	Timestamp uint32
	Type      uint8
	ServerID  uint32
	Size      uint32
	LogPos    uint32
	Flags     uint16
}

func parseHeader(b []byte) (Header, error) {
	if len(b) < headerSize {
		return Header{}, fmt.Errorf("事件头只有 %d 字节，至少要 %d", len(b), headerSize)
	}
	h := Header{
		Timestamp: binary.LittleEndian.Uint32(b[0:4]),
		Type:      b[4],
		ServerID:  binary.LittleEndian.Uint32(b[5:9]),
		Size:      binary.LittleEndian.Uint32(b[9:13]),
		LogPos:    binary.LittleEndian.Uint32(b[13:17]),
		Flags:     binary.LittleEndian.Uint16(b[17:19]),
	}
	if h.Size < headerSize {
		return Header{}, fmt.Errorf("事件长度 %d 小于事件头", h.Size)
	}
	return h, nil
}

// FormatDescription mirrors the parts of the FDE we need: which event types
// this server writes, and whether events carry a CRC32 trailer.
type FormatDescription struct {
	BinlogVersion     uint16
	ServerVersion     string
	CreateTimestamp   uint32
	HeaderLength      uint8
	PostHeaderLens    []byte // indexed by event type
	ChecksumAlgorithm uint8  // 0=off, 1=CRC32, 255=undeclared
}

// Checksum algorithms.
const (
	checksumOff     = 0
	checksumCRC32   = 1
	checksumUndecla = 255
)

// postHeaderLen returns the declared post-header length for an event type.
func (f *FormatDescription) postHeaderLen(evType uint8) (int, bool) {
	if f == nil || int(evType) >= len(f.PostHeaderLens) {
		return 0, false
	}
	return int(f.PostHeaderLens[evType]), true
}

func parseFormatDescription(body []byte) (*FormatDescription, error) {
	if len(body) < 2+50+4+1 {
		return nil, fmt.Errorf("格式描述事件只有 %d 字节，读不出服务器版本", len(body))
	}
	f := &FormatDescription{
		BinlogVersion:   binary.LittleEndian.Uint16(body[0:2]),
		ServerVersion:   strings.TrimRight(string(body[2:52]), "\x00"),
		CreateTimestamp: binary.LittleEndian.Uint32(body[52:56]),
		HeaderLength:    body[56],
	}
	rest := body[57:]
	// 5.6.1+ 在末尾多一个校验算法字节 + 4 字节 CRC。判据：长度 ≥5 且倒数第 5 字节
	// 是已知算法值 —— 这是 MySQL 自己的判断方式，不猜版本号。
	if len(rest) >= 5 {
		alg := rest[len(rest)-5]
		if alg == checksumOff || alg == checksumCRC32 || alg == checksumUndecla {
			f.ChecksumAlgorithm = alg
			rest = rest[:len(rest)-5]
			f.PostHeaderLens = rest
			return f, nil
		}
	}
	f.ChecksumAlgorithm = checksumUndecla
	f.PostHeaderLens = rest
	return f, nil
}

// QueryEvent is a statement event: BEGIN / COMMIT / ROLLBACK / DDL live here.
type QueryEvent struct {
	Schema        string
	Query         string
	ExecutionTime uint32
	ErrorCode     uint16
	StatusVars    []byte
}

func parseQuery(body []byte) (*QueryEvent, error) {
	const fixed = 4 + 4 + 1 + 2 + 2
	if len(body) < fixed {
		return nil, fmt.Errorf("语句事件只有 %d 字节", len(body))
	}
	q := &QueryEvent{
		ExecutionTime: binary.LittleEndian.Uint32(body[4:8]),
		ErrorCode:     binary.LittleEndian.Uint16(body[9:11]),
	}
	schemaLen := int(body[8])
	statusLen := int(binary.LittleEndian.Uint16(body[11:13]))
	pos := fixed
	if pos+statusLen+schemaLen+1 > len(body) {
		return nil, fmt.Errorf("语句事件的状态变量/库名越界（状态 %d 库名 %d，总长 %d）", statusLen, schemaLen, len(body))
	}
	q.StatusVars = body[pos : pos+statusLen]
	pos += statusLen
	q.Schema = string(body[pos : pos+schemaLen])
	pos += schemaLen + 1 // 末尾的 0x00
	q.Query = string(body[pos:])
	return q, nil
}

// queryKeywords we branch on. Comparison is on a normalised copy so tabs,
// newlines and trailing spaces do not break the match.
func queryVerb(q string) string {
	s := strings.TrimSpace(q)
	s = strings.TrimRight(s, "; \t\r\n")
	if i := strings.IndexAny(s, " \t\r\n("); i > 0 {
		return strings.ToUpper(s[:i])
	}
	return strings.ToUpper(s)
}

// GTIDEvent carries the transaction identifier that follows it.
type GTIDEvent struct {
	SID         [16]byte
	GNO         int64
	CommitFlag  uint8
	Tag         string
	ImmediateTS uint64 // microseconds since epoch, 8.0.1+
	OriginalTS  uint64
	LastCommit  int64
	SequenceNo  int64
}

// GTID renders "uuid:gno" (or "uuid:tag:gno" on 8.4 with tags).
func (g *GTIDEvent) GTID() string {
	uuid := formatUUID(g.SID[:])
	if g.Tag != "" {
		return uuid + ":" + g.Tag + ":" + fmt.Sprint(g.GNO)
	}
	return uuid + ":" + fmt.Sprint(g.GNO)
}

// CommitTime returns the commit timestamp with microsecond input (zero when the
// server is older than 8.0.1).
func (g *GTIDEvent) CommitTime() (t time.Time) {
	return microsToTime(g.ImmediateTS)
}

func parseGTID(body []byte) (*GTIDEvent, error) {
	if len(body) < 1+16+8 {
		return nil, fmt.Errorf("事务号事件只有 %d 字节", len(body))
	}
	g := &GTIDEvent{CommitFlag: body[0]}
	copy(g.SID[:], body[1:17])
	g.GNO = int64(binary.LittleEndian.Uint64(body[17:25]))
	if len(body) < 42 {
		return g, nil
	}
	pos := 25
	if body[pos] != 2 { // LOGICAL_TIMESTAMP_TYPECODE
		return g, nil
	}
	pos++
	if pos+8+8 > len(body) {
		return g, nil
	}
	g.LastCommit = int64(binary.LittleEndian.Uint64(body[pos:]))
	pos += 8
	g.SequenceNo = int64(binary.LittleEndian.Uint64(body[pos:]))
	pos += 8
	if pos+7 > len(body) {
		return g, nil
	}
	g.ImmediateTS = fixedInt(body[pos:])
	pos += 7
	if g.ImmediateTS&(1<<55) != 0 {
		g.ImmediateTS &^= 1 << 55
		if pos+7 > len(body) {
			return g, nil
		}
		g.OriginalTS = fixedInt(body[pos:])
	} else {
		g.OriginalTS = g.ImmediateTS
	}
	return g, nil
}

// RotateEvent switches binlog file.
type RotateEvent struct {
	NextFile string
	NextPos  uint64
}

func parseRotate(body []byte) (*RotateEvent, error) {
	if len(body) < 8 {
		return nil, fmt.Errorf("切换文件事件只有 %d 字节", len(body))
	}
	return &RotateEvent{
		NextPos:  binary.LittleEndian.Uint64(body[0:8]),
		NextFile: sanitizeBinlogName(body[8:]),
	}, nil
}

// Heartbeat carries the source's current file+offset when the stream is idle.
type Heartbeat struct {
	File string
	Pos  uint64
}

func parseHeartbeat(body []byte, version2 bool) *Heartbeat {
	if !version2 {
		return &Heartbeat{File: sanitizeBinlogName(body)}
	}
	hb := &Heartbeat{}
	pos := 0
	for pos < len(body) {
		switch body[pos] {
		case 0: // header end mark
			return hb
		case 1: // filename
			pos++
			if pos >= len(body) {
				return hb
			}
			n := int(body[pos])
			pos++
			if pos+n > len(body) {
				return hb
			}
			hb.File = string(body[pos : pos+n])
			pos += n
		case 2: // offset
			pos++
			if pos >= len(body) {
				return hb
			}
			n := int(body[pos])
			pos++
			if pos+n > len(body) {
				return hb
			}
			hb.Pos = uint64(fixedInt(body[pos : pos+n]))
			pos += n
		default:
			return hb
		}
	}
	return hb
}

// XIDEvent ends a transaction; the number is InnoDB's transaction id.
type XIDEvent struct{ XID uint64 }

func parseXID(body []byte) (*XIDEvent, error) {
	if len(body) < 8 {
		return nil, fmt.Errorf("提交事件只有 %d 字节", len(body))
	}
	return &XIDEvent{XID: binary.LittleEndian.Uint64(body)}, nil
}

// sanitizeBinlogName keeps the printable part of a binlog file name.
//
// 切换文件事件在开启校验和时末尾多 4 字节 CRC，而流开头的「伪造切换」事件
// 常常没有 FDE 可参照；文件名本身永远是可见 ASCII，所以在第一个不可见字节
// 处截断即可，不去猜校验算法的状态。
func sanitizeBinlogName(b []byte) string {
	for i, c := range b {
		if c < 0x21 || c > 0x7e {
			return string(b[:i])
		}
	}
	return string(b)
}

// formatUUID renders 16 raw bytes as the canonical UUID text.
func formatUUID(b []byte) string {
	const hexDigits = "0123456789abcdef"
	var out [36]byte
	j := 0
	for i, v := range b {
		if i == 4 || i == 6 || i == 8 || i == 10 {
			out[j] = '-'
			j++
		}
		out[j] = hexDigits[v>>4]
		out[j+1] = hexDigits[v&0x0f]
		j += 2
	}
	return string(out[:])
}

// fixedInt reads a little-endian integer of 1..8 bytes.
func fixedInt(b []byte) uint64 {
	var v uint64
	for i := len(b) - 1; i >= 0; i-- {
		v = v<<8 | uint64(b[i])
	}
	return v
}

// lenEnc reads a MySQL length-encoded integer, returning the value and how
// many bytes it used.
func lenEnc(b []byte) (uint64, int, error) {
	if len(b) == 0 {
		return 0, 0, fmt.Errorf("长度前缀缺失")
	}
	switch first := b[0]; {
	case first < 0xfb:
		return uint64(first), 1, nil
	case first == 0xfb:
		return 0, 1, nil // NULL，调用方自己判断语义
	case first == 0xfc:
		if len(b) < 3 {
			return 0, 0, fmt.Errorf("两字节长度前缀被截断")
		}
		return uint64(binary.LittleEndian.Uint16(b[1:3])), 3, nil
	case first == 0xfd:
		if len(b) < 4 {
			return 0, 0, fmt.Errorf("三字节长度前缀被截断")
		}
		return uint64(b[1]) | uint64(b[2])<<8 | uint64(b[3])<<16, 4, nil
	default: // 0xfe
		if len(b) < 9 {
			return 0, 0, fmt.Errorf("八字节长度前缀被截断")
		}
		return binary.LittleEndian.Uint64(b[1:9]), 9, nil
	}
}

func bitmapByteSize(bits int) int { return (bits + 7) / 8 }

// bitSet reports whether bit i is set in a little-endian bitmap.
func bitSet(bitmap []byte, i int) bool {
	if i/8 >= len(bitmap) {
		return false
	}
	return bitmap[i/8]&(1<<uint(i%8)) != 0
}
