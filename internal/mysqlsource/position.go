package mysqlsource

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// positionVersion tags the serialized form so an old checkpoint can be
// recognised instead of misparsed.
const positionVersion = "v1"

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
}

// String serializes a position into the single opaque text the sink stores in
// the same transaction as the data (docs/STATE-SCHEMA.md).
func (p Position) String() string {
	var b strings.Builder
	b.WriteString(positionVersion)
	b.WriteString(";file=")
	b.WriteString(p.File)
	b.WriteString(";pos=")
	b.WriteString(strconv.FormatUint(uint64(p.Pos), 10))
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

// ParsePosition reads back what String wrote. It is deliberately strict about
// the shape (version tag, known keys) but tolerant about missing optional
// coordinates, because 5.7 sources never produce a GTID.
func ParsePosition(s string) (Position, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return Position{}, fmt.Errorf("位点是空的")
	}
	parts := strings.Split(s, ";")
	if parts[0] != positionVersion {
		return Position{}, fmt.Errorf("位点格式不认识（版本 %q，只认 %q）", parts[0], positionVersion)
	}
	var p Position
	for _, kv := range parts[1:] {
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
