package mysqlsource

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// GTIDSet is a MySQL GTID set: server UUID (optionally with an 8.4 GTID tag)
// mapped to sorted, disjoint half-open intervals [start, stop) of transaction
// numbers.
//
// 为什么不用字符串直接比：位点恢复要回答「这个集合包不包含我上次提交的那笔」，
// 字符串比较答不了（uuid:1-5 与 uuid:1-5:7 的先后看字面会错）。
type GTIDSet struct {
	sets map[string][]interval
}

// interval is [start, stop); a single transaction is [n, n+1).
type interval struct {
	start int64
	stop  int64
}

// ParseGTIDSet parses the canonical text form, e.g.
//
//	3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5:7,uuid2:1-3
//	3e11fa47-71ca-11e1-9e33-c80aa9429562:tag_a:1-5     (MySQL 8.4 GTID tags)
//
// 空串是合法的（gtid_mode=OFF 的源库没有 GTID），返回空集合。
func ParseGTIDSet(s string) (GTIDSet, error) {
	g := GTIDSet{sets: map[string][]interval{}}
	s = strings.TrimSpace(s)
	if s == "" {
		return g, nil
	}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if err := g.addText(part); err != nil {
			return GTIDSet{}, err
		}
	}
	g.normalize()
	return g, nil
}

// addText adds one "uuid[:tag]:range[:range]" group.
func (g *GTIDSet) addText(part string) error {
	fields := strings.Split(part, ":")
	if len(fields) < 2 {
		return fmt.Errorf("GTID 片段 %q 缺少事务号（形如 uuid:1-5）", part)
	}
	uuid := strings.ToLower(strings.TrimSpace(fields[0]))
	if !looksLikeUUID(uuid) {
		return fmt.Errorf("GTID 片段 %q 的服务器标识不是 UUID", part)
	}
	key := uuid
	rest := fields[1:]
	// 8.4 的 GTID tag：第二段不是区间时视为标签。
	if !looksLikeRange(rest[0]) {
		key = uuid + ":" + rest[0]
		rest = rest[1:]
	}
	if len(rest) == 0 {
		return fmt.Errorf("GTID 片段 %q 缺少事务号（形如 uuid:1-5）", part)
	}
	if g.sets == nil {
		g.sets = map[string][]interval{}
	}
	for _, r := range rest {
		iv, err := parseRange(r)
		if err != nil {
			return fmt.Errorf("GTID 片段 %q：%w", part, err)
		}
		g.sets[key] = append(g.sets[key], iv)
	}
	return nil
}

func parseRange(s string) (interval, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return interval{}, fmt.Errorf("事务号是空的")
	}
	if lo, hi, ok := strings.Cut(s, "-"); ok {
		start, err := strconv.ParseInt(lo, 10, 64)
		if err != nil || start <= 0 {
			return interval{}, fmt.Errorf("事务号 %q 不是正整数", lo)
		}
		stop, err := strconv.ParseInt(hi, 10, 64)
		if err != nil || stop <= 0 {
			return interval{}, fmt.Errorf("事务号 %q 不是正整数", hi)
		}
		if stop < start {
			return interval{}, fmt.Errorf("事务号区间 %q 的结尾小于开头", s)
		}
		return interval{start: start, stop: stop + 1}, nil
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n <= 0 {
		return interval{}, fmt.Errorf("事务号 %q 不是正整数", s)
	}
	return interval{start: n, stop: n + 1}, nil
}

func looksLikeRange(s string) bool {
	first, _, _ := strings.Cut(s, "-")
	if first == "" {
		return false
	}
	for _, c := range first {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

func looksLikeUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i, c := range s {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if c != '-' {
				return false
			}
			continue
		}
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

// Add merges one transaction ("uuid:42" or "uuid:tag:42") into the set.
func (g *GTIDSet) Add(gtid string) error {
	if g.sets == nil {
		g.sets = map[string][]interval{}
	}
	if err := g.addText(strings.TrimSpace(gtid)); err != nil {
		return err
	}
	g.normalize()
	return nil
}

// Merge unions other into g.
func (g *GTIDSet) Merge(other GTIDSet) {
	if g.sets == nil {
		g.sets = map[string][]interval{}
	}
	for k, ivs := range other.sets {
		g.sets[k] = append(g.sets[k], ivs...)
	}
	g.normalize()
}

// Clone returns an independent copy.
func (g GTIDSet) Clone() GTIDSet {
	out := GTIDSet{sets: make(map[string][]interval, len(g.sets))}
	for k, ivs := range g.sets {
		out.sets[k] = append([]interval(nil), ivs...)
	}
	return out
}

// IsEmpty reports whether the set has no transactions.
func (g GTIDSet) IsEmpty() bool { return len(g.sets) == 0 }

// String renders the canonical form, keys sorted so two equal sets serialize
// identically (the checkpoint column is compared as text by operators).
func (g GTIDSet) String() string {
	if len(g.sets) == 0 {
		return ""
	}
	keys := make([]string, 0, len(g.sets))
	for k := range g.sets {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(k)
		for _, iv := range g.sets[k] {
			b.WriteByte(':')
			if iv.stop == iv.start+1 {
				b.WriteString(strconv.FormatInt(iv.start, 10))
				continue
			}
			b.WriteString(strconv.FormatInt(iv.start, 10))
			b.WriteByte('-')
			b.WriteString(strconv.FormatInt(iv.stop-1, 10))
		}
	}
	return b.String()
}

// Contains reports whether every transaction in other is also in g. An empty
// other is always contained (nothing to replay).
func (g GTIDSet) Contains(other GTIDSet) bool {
	for k, ivs := range other.sets {
		mine, ok := g.sets[k]
		if !ok {
			return false
		}
		for _, want := range ivs {
			covered := false
			for _, have := range mine {
				if have.start <= want.start && want.stop <= have.stop {
					covered = true
					break
				}
			}
			if !covered {
				return false
			}
		}
	}
	return true
}

// Equal compares two sets by content, not by text.
func (g GTIDSet) Equal(other GTIDSet) bool {
	a, b := g.Clone(), other.Clone()
	a.normalize()
	b.normalize()
	if len(a.sets) != len(b.sets) {
		return false
	}
	for k, ivs := range a.sets {
		theirs, ok := b.sets[k]
		if !ok || len(ivs) != len(theirs) {
			return false
		}
		for i := range ivs {
			if ivs[i] != theirs[i] {
				return false
			}
		}
	}
	return true
}

// Count returns how many transactions the set names (used by tests and by the
// progress display).
func (g GTIDSet) Count() int64 {
	var n int64
	for _, ivs := range g.sets {
		for _, iv := range ivs {
			n += iv.stop - iv.start
		}
	}
	return n
}

// normalize sorts and merges overlapping or adjacent intervals per key.
func (g *GTIDSet) normalize() {
	for k, ivs := range g.sets {
		if len(ivs) == 0 {
			delete(g.sets, k)
			continue
		}
		sort.Slice(ivs, func(i, j int) bool { return ivs[i].start < ivs[j].start })
		out := ivs[:1]
		for _, iv := range ivs[1:] {
			last := &out[len(out)-1]
			if iv.start <= last.stop {
				if iv.stop > last.stop {
					last.stop = iv.stop
				}
				continue
			}
			out = append(out, iv)
		}
		g.sets[k] = out
	}
}
