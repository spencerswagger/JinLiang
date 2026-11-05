package mysqlsource

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/spencerswagger/jinliang/internal/source"
)

// Event is one decoded binlog event. Only the field matching Header.Type is
// filled; everything else is nil.
type Event struct {
	Header Header
	File   string // 这条事件所在的 binlog 文件名
	Offset uint32 // 事件在文件里的起始偏移

	Format    *FormatDescription
	TableMap  *TableMap
	Rows      *RowsEvent
	Query     *QueryEvent
	XID       *XIDEvent
	GTID      *GTIDEvent
	Rotate    *RotateEvent
	Heartbeat *Heartbeat
	Payload   *Payload
	Previous  GTIDSet
}

// Parser turns raw event frames into decoded events. It keeps the table map
// cache (rows events only carry a table id) and the format description (which
// tells us whether events end with a CRC32 trailer).
//
// 一个 Parser 只服务一条流：表映射缓存是流内状态，跨流复用会把不同源库的
// 表号混在一起。
type Parser struct {
	fde        *FormatDescription
	tables     map[uint64]*TableMap
	file       string
	headerErr  error
	lastOffset uint32
}

// NewParser returns an empty parser.
func NewParser() *Parser {
	return &Parser{tables: map[uint64]*TableMap{}}
}

// SetFile tells the parser which binlog file the following events come from.
func (p *Parser) SetFile(name string) { p.file = name }

// File returns the file the parser is currently reading.
func (p *Parser) File() string { return p.file }

// Format returns the format description seen so far (nil before the first FDE).
func (p *Parser) Format() *FormatDescription { return p.fde }

// TableMap returns a cached table map by id (nil when unknown).
func (p *Parser) TableMap(id uint64) *TableMap { return p.tables[id] }

// Parse decodes one event frame (header + body + optional CRC). A
// TRANSACTION_PAYLOAD event is expanded into the events it contains, so callers
// never see compression.
func (p *Parser) Parse(raw []byte) ([]*Event, error) {
	return p.parseFrame(raw, 0)
}

func (p *Parser) parseFrame(raw []byte, depth int) ([]*Event, error) {
	if len(raw) < headerSize {
		return nil, fmt.Errorf("事件帧只有 %d 字节，不足一个事件头", len(raw))
	}
	h, err := parseHeader(raw)
	if err != nil {
		return nil, err
	}
	if int(h.Size) != len(raw) {
		return nil, fmt.Errorf("事件头说 %d 字节，实际给了 %d 字节（流被截断）", h.Size, len(raw))
	}
	// 格式描述事件自己带着「校验算法」这一信息，因此不做通用剥离。
	if h.Type == evFormatDescription && depth == 0 {
		fde, err := parseFormatDescription(raw[headerSize:])
		if err != nil {
			return nil, err
		}
		if fde.HeaderLength != headerSize {
			return nil, fmt.Errorf("格式描述说事件头 %d 字节，只认 %d 字节", fde.HeaderLength, headerSize)
		}
		p.fde = fde
		p.checkPostHeaders(fde)
		return []*Event{{Header: h, File: p.file, Offset: p.lastOffset, Format: fde}}, nil
	}

	// 压缩载荷里的事件不带 CRC（校验和覆盖的是外层载荷事件）。
	if depth > 0 {
		return p.decode(h, raw[headerSize:], depth)
	}
	body, err := p.stripChecksum(raw, h)
	if err != nil {
		return nil, err
	}
	return p.decode(h, body, depth)
}

// stripChecksum removes and verifies the CRC32 trailer when the server
// declared one. A bad CRC means the stream is corrupt: replaying from here
// would silently skip transactions, so we stop.
func (p *Parser) stripChecksum(raw []byte, h Header) ([]byte, error) {
	body := raw[headerSize:]
	if p.fde == nil || p.fde.ChecksumAlgorithm != checksumCRC32 {
		// 还没见到格式描述（流开头的伪造切换事件就是这种）：用「尾部 4 字节
		// 是否正好是前面内容的 CRC32」来判断有没有校验和，不猜。
		if len(body) >= checksumLen {
			want := binary.LittleEndian.Uint32(body[len(body)-checksumLen:])
			if crc32.ChecksumIEEE(raw[:len(raw)-checksumLen]) == want {
				return body[:len(body)-checksumLen], nil
			}
		}
		return body, nil
	}
	// 伪造的 ROTATE / 心跳事件不参与校验（MySQL 自己也不校验）。
	if h.Flags&0x20 != 0 || len(body) < checksumLen {
		return body, nil
	}
	want := binary.LittleEndian.Uint32(body[len(body)-checksumLen:])
	got := crc32.ChecksumIEEE(raw[:len(raw)-checksumLen])
	if want != got {
		return nil, fmt.Errorf("事件校验和不符（类型 %d，位置 %d）：流已损坏，不能从这里继续", h.Type, h.LogPos)
	}
	return body[:len(body)-checksumLen], nil
}

// decode dispatches on the event type.
func (p *Parser) decode(h Header, body []byte, depth int) ([]*Event, error) {
	ev := &Event{Header: h, File: p.file, Offset: p.lastOffset}
	switch h.Type {
	case evRotate:
		rot, err := parseRotate(body)
		if err != nil {
			return nil, err
		}
		p.file = rot.NextFile
		ev.File = rot.NextFile
		ev.Rotate = rot
	case evTableMap:
		tm, err := parseTableMap(body)
		if err != nil {
			return nil, err
		}
		p.tables[tm.TableID] = tm
		ev.TableMap = tm
	case evWriteRowsV1, evUpdateRowsV1, evDeleteRowsV1:
		rows, err := parseRows(body, rowsKind(h.Type), 1, p.tables)
		if err != nil {
			return nil, err
		}
		ev.Rows = rows
	case evWriteRowsV2, evUpdateRowsV2, evDeleteRowsV2:
		rows, err := parseRows(body, rowsKind(h.Type), 2, p.tables)
		if err != nil {
			return nil, err
		}
		ev.Rows = rows
	case evPartialUpdateRows:
		return nil, fmt.Errorf("源库开了 binlog_row_value_options=PARTIAL_JSON，更新只发了 JSON 差异；" +
			"请执行 SET GLOBAL binlog_row_value_options='' 后重新取位点（宁可停下也不猜）")
	case evXID:
		xid, err := parseXID(body)
		if err != nil {
			return nil, err
		}
		ev.XID = xid
	case evQuery:
		q, err := parseQuery(body)
		if err != nil {
			return nil, err
		}
		ev.Query = q
	case evGTID, evGTIDTagged:
		g, err := parseGTID(body)
		if err != nil {
			return nil, err
		}
		ev.GTID = g
	case evAnonymousGTID:
		// 没开 GTID 的源库用它占位：事务从这里开始，但没有事务号。
		ev.GTID = &GTIDEvent{}
	case evPreviousGTIDs:
		set, err := parsePreviousGTIDs(body)
		if err != nil {
			return nil, err
		}
		ev.Previous = set
	case evHeartbeat:
		ev.Heartbeat = parseHeartbeat(body, false)
	case evHeartbeatV2:
		ev.Heartbeat = parseHeartbeat(body, true)
	case evTransactionPaylod:
		pl, err := parsePayload(body)
		if err != nil {
			return nil, err
		}
		ev.Payload = pl
		expanded, err := pl.Decompress()
		if err != nil {
			return nil, err
		}
		frames, err := splitPayloadEvents(expanded)
		if err != nil {
			return nil, err
		}
		out := make([]*Event, 0, len(frames))
		for _, frame := range frames {
			inner, err := p.parseFrame(frame, depth+1)
			if err != nil {
				return nil, fmt.Errorf("压缩事务解析失败: %w", err)
			}
			out = append(out, inner...)
		}
		if len(out) == 0 {
			return nil, fmt.Errorf("压缩事务解出来 0 个事件，按丢数据处理")
		}
		return out, nil
	default:
		// 不关心的事件（INTVAR / RAND / USER_VAR / STOP / 未知新类型）：
		// 原样放过，交由上层忽略。
	}
	return []*Event{ev}, nil
}

// checkPostHeaders cross-checks the FDE's declared post-header lengths for the
// events we parse. A mismatch means we would misread bodies, so we stop rather
// than produce garbage rows.
func (p *Parser) checkPostHeaders(fde *FormatDescription) {
	want := map[uint8]int{
		evTableMap:     8,
		evWriteRowsV1:  8,
		evUpdateRowsV1: 8,
		evDeleteRowsV1: 8,
		evWriteRowsV2:  10,
		evUpdateRowsV2: 10,
		evDeleteRowsV2: 10,
		evXID:          0,
		evQuery:        13,
	}
	for typ, expect := range want {
		got, ok := fde.postHeaderLen(typ)
		if !ok || typ == evXID {
			continue
		}
		if got != expect {
			p.headerErr = fmt.Errorf("服务器声明事件类型 %d 的后置头 %d 字节，我们按 %d 字节解析", typ, got, expect)
			return
		}
	}
}

// HeaderError exposes a format mismatch discovered from the FDE, if any.
func (p *Parser) HeaderError() error { return p.headerErr }

func rowsKind(t uint8) source.EventKind {
	switch t {
	case evWriteRowsV1, evWriteRowsV2:
		return source.EvInsert
	case evUpdateRowsV1, evUpdateRowsV2:
		return source.EvUpdate
	case evDeleteRowsV1, evDeleteRowsV2:
		return source.EvDelete
	}
	return source.EvHeartbeat
}

// parsePreviousGTIDs reads the executed set a binlog file starts with. Used to
// seed the GTID checkpoint before the first transaction arrives.
func parsePreviousGTIDs(body []byte) (GTIDSet, error) {
	set := GTIDSet{sets: map[string][]interval{}}
	if len(body) < 8 {
		return set, fmt.Errorf("文件开头的事务号清单只有 %d 字节", len(body))
	}
	nSIDs := binary.LittleEndian.Uint64(body[0:8])
	pos := 8
	if nSIDs > 1<<20 {
		return set, fmt.Errorf("文件开头的事务号清单说 %d 个服务器，超出合理范围", nSIDs)
	}
	for i := uint64(0); i < nSIDs; i++ {
		if pos+24 > len(body) {
			return set, fmt.Errorf("文件开头的事务号清单第 %d 个服务器越界", i+1)
		}
		uuid := formatUUID(body[pos : pos+16])
		pos += 16
		nIntervals := binary.LittleEndian.Uint64(body[pos : pos+8])
		pos += 8
		if nIntervals > 1<<20 {
			return set, fmt.Errorf("文件开头的事务号清单第 %d 个服务器说有 %d 个区间，超出合理范围", i+1, nIntervals)
		}
		for j := uint64(0); j < nIntervals; j++ {
			if pos+16 > len(body) {
				return set, fmt.Errorf("文件开头的事务号清单第 %d 个服务器的第 %d 个区间越界", i+1, j+1)
			}
			start := int64(binary.LittleEndian.Uint64(body[pos : pos+8]))
			stop := int64(binary.LittleEndian.Uint64(body[pos+8 : pos+16]))
			pos += 16
			set.sets[uuid] = append(set.sets[uuid], interval{start: start, stop: stop + 1})
		}
	}
	set.normalize()
	return set, nil
}
