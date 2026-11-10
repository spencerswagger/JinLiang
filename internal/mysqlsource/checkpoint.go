package mysqlsource

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/spencerswagger/jinliang/internal/source"
)

// 位点「还能不能重放」与「还差多少」——引擎在全量续跑与追平判断上要的两个事实。
//
// MySQL 没有 PG 那种「槽位确认位点」（服务端替我们记住消费者到哪儿），
// 只有源端自己的 binlog 保留策略：文件还在 → 那段变更还能重放；被 PURGE 掉 → 追不回来。
// 所以这里的判据是**客观的**（SHOW BINARY LOGS 里有没有那个文件），不是猜。

// binlogFile 是 SHOW BINARY LOGS 的一行。
type binlogFile struct {
	Name string
	Size int64
}

// binlogFiles 读源端当前保留的 binlog 清单（按文件名升序，服务端本来就这么返回）。
func (s *Source) binlogFiles(ctx context.Context) ([]binlogFile, error) {
	rows, err := s.db.QueryContext(ctx, "SHOW BINARY LOGS")
	if err != nil {
		return nil, fmt.Errorf("读源端 binlog 清单失败（需要 REPLICATION CLIENT）: %w", err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	// 5.7 是 Log_name/File_size；8.0+ 多了 Encrypted 列，按列名取，不按下标。
	nameIdx, sizeIdx := -1, -1
	for i, c := range cols {
		switch strings.ToLower(c) {
		case "log_name":
			nameIdx = i
		case "file_size":
			sizeIdx = i
		}
	}
	if nameIdx < 0 || sizeIdx < 0 {
		return nil, fmt.Errorf("SHOW BINARY LOGS 的列不认识（%s）", strings.Join(cols, ", "))
	}
	var out []binlogFile
	for rows.Next() {
		raw := make([]any, len(cols))
		dest := make([]any, len(cols))
		for i := range raw {
			dest[i] = &raw[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}
		f := binlogFile{Name: asText(raw[nameIdx])}
		n, err := strconv.ParseInt(asText(raw[sizeIdx]), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("binlog %s 的大小 %q 不是数字", f.Name, asText(raw[sizeIdx]))
		}
		f.Size = n
		out = append(out, f)
	}
	return out, rows.Err()
}

// asText 把驱动返回的 []byte/string 统一成文本（database/sql 两种都可能给）。
func asText(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case []byte:
		return string(x)
	default:
		return fmt.Sprintf("%v", x)
	}
}

// BinlogContains 判断位点所在的 binlog 文件是否还在源端保留着。
//
// 返回 (true, "") 表示还能从它重放；(false, 原因) 表示这段变更已经被轮转/清理掉了，
// 引擎据此决定「整轮重搬」而不是静默丢掉一段。
func (s *Source) BinlogContains(ctx context.Context, pos source.Position) (bool, string, error) {
	if pos == nil {
		return true, "", nil
	}
	p, ok := pos.(Position)
	if !ok {
		parsed, err := s.ParsePosition(pos.String())
		if err != nil {
			return false, "", err
		}
		p, ok = parsed.(Position)
		if !ok {
			return false, "", fmt.Errorf("位点类型 %T 不是 MySQL 位点", parsed)
		}
	}
	if p.IsZero() {
		return true, "", nil
	}
	files, err := s.binlogFiles(ctx)
	if err != nil {
		return false, "", err
	}
	if len(files) == 0 {
		return false, "源端现在一个 binlog 文件都没有（log_bin 关掉了，或刚清理过）", nil
	}
	for _, f := range files {
		if f.Name == p.File {
			if int64(p.Pos) > f.Size {
				// 文件在、但位点超出了文件大小：文件被截断过（或位点是别的源写的）。
				return false, fmt.Sprintf("位点 %s:%d 超出了该文件当前大小 %d", p.File, p.Pos, f.Size), nil
			}
			return true, "", nil
		}
	}
	oldest := files[0].Name
	return false, fmt.Sprintf("位点所在的 %s 已被源端清理（现在最老的是 %s）", p.File, oldest), nil
}

// BacklogBytes 估算「从 pos 到源端此刻」还有多少 binlog 没重放（字节）。
//
// 引擎用它做进度里的 lag_bytes 与 --stop-when-idle 的追平判据：拿不到就返回 -1
// （= 不知道），**不返回 0**——0 会被当成「已经追平」，那是把落后藏起来。
func (s *Source) BacklogBytes(ctx context.Context, pos source.Position) (int64, error) {
	if pos == nil {
		return -1, nil
	}
	p, ok := pos.(Position)
	if !ok {
		parsed, err := s.ParsePosition(pos.String())
		if err != nil {
			return -1, nil // 位点读不懂时不给数（调用方按「未知」处理）
		}
		p, ok = parsed.(Position)
		if !ok {
			return -1, nil
		}
	}
	if p.IsZero() {
		return -1, nil
	}
	files, err := s.binlogFiles(ctx)
	if err != nil {
		return -1, nil // 没有权限读清单：lag 未知，不阻断（追平判据另有位点比较）
	}
	var lag int64
	seen := false
	for _, f := range files {
		if f.Name == p.File {
			seen = true
			lag += f.Size - int64(p.Pos)
			if lag < 0 {
				lag = 0
			}
			continue
		}
		if seen {
			lag += f.Size
		}
	}
	if !seen {
		// 位点所在文件已被清理：落后量无从计算，交给 BinlogContains 报「追不回来」。
		return -1, nil
	}
	return lag, nil
}
