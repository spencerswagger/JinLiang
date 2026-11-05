package mysqlsource

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/spencerswagger/jinliang/internal/source"
)

// ErrPositionKindMismatch 表示这串位点是别的类型的源写的（E1010：换回对应的源，
// 或重新开始同步）。和数据损坏（E3001：建议重搬全量）必须分开报。
var ErrPositionKindMismatch = errors.New("位点的类型与当前源不匹配")

// KindMySQL is what Position.Kind() returns.
//
// 契约（internal/source/contract.go 2026-11-05 裁决）：Kind() 与 Endpoint.Kind()
// 同值，引擎拿它做对账——位点串和连接类型不匹配时给 E1010（换回对应的源），
// 而不是猜。所以这里只能返回 "mysql"，不能是 "mysql_gtid" / "mysql_file_pos"：
// 那会把「连接类型对账」和「用哪套坐标续跑」两件事混成一个字符串，引擎还得知道
// 每种源的内部位点变体，契约就白抽了。
// 「两套位点各有一个明确名字」这件事由 Coordinate() 承担（见下）。
const KindMySQL = "mysql"

// Coordinate names which checkpoint axis a position resumes on. 两套位点不共用
// 同一个名字：file+pos 在主从切换后会对不上，GTID 在源端没开时根本不存在，
// 混用会让恢复走到错误的轴上。
type Coordinate string

// Coordinate kinds.
const (
	CoordFilePos Coordinate = "mysql_file_pos" // 文件名 + 偏移
	CoordGTID    Coordinate = "mysql_gtid"     // 执行过的事务号集合
)

// Position is a MySQL checkpoint carried in two coordinate systems at once.
//
//	File+Pos  5.7 也能用：重启时直接按文件名+偏移重新拉流。
//	GTID      主从切换后仍对得上：换了主机、换了端口，事务号还是那些。
//	TakenAt   取位点的那一刻，精确到毫秒。秒级不够：高并发下一个 1 秒窗口里
//	          可能有成百上千个事务，恢复时按秒回退就会重复重放一大片。
//
// 两套都写：String() 一定带上能拿到的全部坐标；恢复时优先用 GTID（若源端开了），
// 源端关了 GTID 或位点里没有 GTID 时才退回 File+Pos。判断逻辑见 RestartFrom。
type Position struct {
	File     string    // "mysql-bin.000003"
	Pos      uint32    // 下一个待读事件的偏移（不是最后一个已读事件的偏移）
	GTID     GTIDSet   // 已提交事务集合；gtid_mode=OFF 时为空
	TakenAt  time.Time // 取位点时刻，序列化时截到毫秒
	ServerID uint32    // 取位点的源库 server_id，用来发现「换了源库」

	// Coord 固定住这份位点用哪套坐标续跑。零值表示按 GTID 是否为空推导
	// （老位点没有这个字段），推导结果写进 String()，之后就不再变。
	Coord Coordinate
}

// Kind implements source.Position: 连接类型，与 Endpoint.Kind() 同值。
func (p Position) Kind() string { return KindMySQL }

// Coordinate reports which axis this checkpoint resumes on.
func (p Position) Coordinate() Coordinate {
	if p.Coord != "" {
		return p.Coord
	}
	if !p.GTID.IsEmpty() {
		return CoordGTID
	}
	return CoordFilePos
}

// String serializes a position into the single opaque text the sink stores in
// the same transaction as the data (docs/STATE-SCHEMA.md).
//
// 格式由契约定死：`<kind>:v1:<载荷>`。kind 是连接类型（mysql），v1 是位点格式版本，
// 载荷是本连接器自己的事——这里放 file/pos/coord/gtid/t（毫秒）/srv。
// 版本位现在零成本，等位点已经写进用户目标库之后再加就是一次迁移。
func (p Position) String() string {
	return source.FormatPosition(KindMySQL, p.payload())
}

func (p Position) payload() string {
	var b strings.Builder
	b.WriteString("file=")
	b.WriteString(p.File)
	b.WriteString(";pos=")
	b.WriteString(strconv.FormatUint(uint64(p.Pos), 10))
	b.WriteString(";coord=")
	b.WriteString(string(p.Coordinate()))
	if gtid := p.GTID.String(); gtid != "" {
		b.WriteString(";gtid=")
		b.WriteString(gtid)
	}
	if !p.TakenAt.IsZero() {
		b.WriteString(";t=")
		b.WriteString(strconv.FormatInt(p.TakenAt.UnixMilli(), 10))
	}
	if p.ServerID != 0 {
		b.WriteString(";srv=")
		b.WriteString(strconv.FormatUint(uint64(p.ServerID), 10))
	}
	return b.String()
}

// ParsePosition reads back what String wrote.
//
// 解析严格按 `<kind>:v1:<载荷>`：缺段、版本未知、kind 不是 mysql 都直接报错，
// 不猜、不回落（位点错一位就是重放或漏数据）。
func ParsePosition(raw string) (Position, error) {
	kind, payload, err := source.SplitPosition(strings.TrimSpace(raw))
	if err != nil {
		return Position{}, err
	}
	if kind != KindMySQL {
		return Position{}, fmt.Errorf("%w：位点属于 %q，当前源是 %q；格式为 <kind>:v1:<载荷>",
			ErrPositionKindMismatch, kind, KindMySQL)
	}
	return parsePositionPayload(payload)
}

func parsePositionPayload(payload string) (Position, error) {
	if strings.TrimSpace(payload) == "" {
		return Position{}, fmt.Errorf("位点载荷是空的")
	}
	var p Position
	for _, kv := range strings.Split(payload, ";") {
		if kv == "" {
			continue
		}
		k, v, ok := strings.Cut(kv, "=")
		if !ok {
			return Position{}, fmt.Errorf("位点片段 %q 不是 键=值", kv)
		}
		switch k {
		case "file":
			p.File = v
		case "pos":
			n, err := strconv.ParseUint(v, 10, 32)
			if err != nil {
				return Position{}, fmt.Errorf("位点里的偏移 %q 不是数字", v)
			}
			p.Pos = uint32(n)
		case "coord":
			switch Coordinate(v) {
			case CoordFilePos, CoordGTID:
				p.Coord = Coordinate(v)
			default:
				return Position{}, fmt.Errorf("位点里的坐标类型 %q 不认识（只认 %s / %s）", v, CoordFilePos, CoordGTID)
			}
		case "gtid":
			g, err := ParseGTIDSet(v)
			if err != nil {
				return Position{}, err
			}
			p.GTID = g
		case "t":
			ms, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return Position{}, fmt.Errorf("位点里的时间 %q 不是毫秒数", v)
			}
			p.TakenAt = time.UnixMilli(ms).UTC()
		case "srv":
			n, err := strconv.ParseUint(v, 10, 32)
			if err != nil {
				return Position{}, fmt.Errorf("位点里的 server_id %q 不是数字", v)
			}
			p.ServerID = uint32(n)
		default:
			// 前向兼容：不认识的键跳过，不要因为多了个字段就读不出老位点。
		}
	}
	// 坐标类型与内容必须自洽：说按事务号续，就得真有事务号。
	if p.Coord == CoordGTID && p.GTID.IsEmpty() {
		return Position{}, fmt.Errorf("位点说按事务号续（coord=%s），但里面没有事务号", CoordGTID)
	}
	if p.File == "" && p.GTID.IsEmpty() {
		return Position{}, fmt.Errorf("位点里既没有文件名也没有事务号，没法恢复")
	}
	if p.Pos == 0 {
		// 新文件的起点在 binlog 里是 4（魔数之后），0 只可能来自没写全的位点。
		p.Pos = 4
	}
	return p, nil
}

// IsZero reports whether the position carries nothing usable.
func (p Position) IsZero() bool { return p.File == "" && p.GTID.IsEmpty() }

// Compare orders two positions on the file+pos axis.
func (p Position) Compare(o Position) int {
	if c := compareBinlogName(p.File, o.File); c != 0 {
		return c
	}
	switch {
	case p.Pos < o.Pos:
		return -1
	case p.Pos > o.Pos:
		return 1
	}
	return 0
}

// compareBinlogName orders "mysql-bin.000009" before "mysql-bin.000010":
// compare the shared prefix, then the numeric tail.
func compareBinlogName(a, b string) int {
	if a == b {
		return 0
	}
	ai, an := splitBinlogName(a)
	bi, bn := splitBinlogName(b)
	if ai == bi {
		switch {
		case an < bn:
			return -1
		case an > bn:
			return 1
		}
		return strings.Compare(a, b)
	}
	return strings.Compare(ai, bi)
}

func splitBinlogName(s string) (string, int64) {
	i := len(s)
	for i > 0 && s[i-1] >= '0' && s[i-1] <= '9' {
		i--
	}
	if i == len(s) {
		return s, -1
	}
	n, err := strconv.ParseInt(s[i:], 10, 64)
	if err != nil {
		return s, -1
	}
	return s[:i], n
}

// RestartFrom answers "从哪儿开始重拉". GTID 优先（换主也能接上），
// 没有 GTID 才退回文件名+偏移。返回的 Position 已把位置挪到「下一件事务之前」。
func RestartFrom(saved Position, sourceGTID GTIDSet, sourceServerID uint32) Position {
	if !saved.GTID.IsEmpty() && !sourceGTID.IsEmpty() {
		// 事务号仍被源端认可：按事务号续，文件名/偏移只是给日志看的。
		out := saved
		out.ServerID = sourceServerID
		return out
	}
	out := saved
	out.GTID = GTIDSet{}
	out.ServerID = sourceServerID
	return out
}

// Advance moves the checkpoint to the position right after a committed
// transaction.
func (p *Position) Advance(file string, pos uint32, gtid GTIDSet, at time.Time) {
	if file != "" {
		p.File = file
	}
	p.Pos = pos
	if !gtid.IsEmpty() {
		p.GTID = gtid.Clone()
	}
	p.TakenAt = at
}

// sortPositions is used by tests and by the "which checkpoint is newer"
// display; kept here so ordering rules live in one place.
func sortPositions(ps []Position) {
	sort.Slice(ps, func(i, j int) bool { return ps[i].Compare(ps[j]) < 0 })
}
