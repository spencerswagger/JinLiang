package mysqlsource

import (
	"fmt"

	"github.com/spencerswagger/jinliang/internal/source"
)

// Image is one row image aligned to the table map's column order.
//
// Present[i] 说明第 i 列这次到底有没有被写进 binlog：
// binlog_row_image=FULL 时全为 true；MINIMAL 时 UPDATE 只带主键和改动列，
// 缺席的列既不是 NULL 也不是 0 —— 必须能和「真的是 NULL」区分开，
// 否则会把没变的列写成 NULL。这个区分是 upsert 正确性的前提。
type Image struct {
	Row     source.Row
	Present []bool
}

// Change is one row-level change decoded from a rows event.
type Change struct {
	Kind source.EventKind // EvInsert / EvUpdate / EvDelete
	New  Image            // EvDelete 时全 false
	Old  Image            // EvInsert 时全 false
}

// RowsEvent is one rows event: N changes against one table.
type RowsEvent struct {
	Table   *TableMap
	Version int
	Changes []Change
}

// parseRows decodes WRITE/UPDATE/DELETE_ROWS_EVENT v1 or v2.
func parseRows(body []byte, kind source.EventKind, version int, tables map[uint64]*TableMap) (*RowsEvent, error) {
	if len(body) < tableIDSize+2 {
		return nil, fmt.Errorf("行事件只有 %d 字节", len(body))
	}
	tableID := fixedInt(body[0:tableIDSize])
	pos := tableIDSize + 2 // flags

	if version == 2 {
		if pos+2 > len(body) {
			return nil, fmt.Errorf("行事件 v2 缺少额外数据长度")
		}
		extraLen := int(fixedInt(body[pos : pos+2]))
		pos += 2
		if extraLen > 2 {
			pos += extraLen - 2
		}
	}
	tm, ok := tables[tableID]
	if !ok {
		return nil, fmt.Errorf("行事件引用了表号 %d，但没收到它的表映射事件（连接可能从半个事务中间开始了）", tableID)
	}

	colCount, n, err := lenEnc(body[pos:])
	if err != nil {
		return nil, fmt.Errorf("行事件的列数读不出来: %w", err)
	}
	pos += n
	if int(colCount) != tm.Columns {
		return nil, fmt.Errorf("行事件说 %d 列，表映射说是 %d 列（表结构在拉流期间被改过）", colCount, tm.Columns)
	}
	bitmapLen := bitmapByteSize(int(colCount))
	if pos+bitmapLen > len(body) {
		return nil, fmt.Errorf("行事件的列位图越界")
	}
	bitmap1 := body[pos : pos+bitmapLen]
	pos += bitmapLen

	var bitmap2 []byte
	if kind == source.EvUpdate {
		if pos+bitmapLen > len(body) {
			return nil, fmt.Errorf("行事件的第二个列位图越界")
		}
		bitmap2 = body[pos : pos+bitmapLen]
		pos += bitmapLen
	}

	re := &RowsEvent{Table: tm, Version: version}
	for pos < len(body) {
		oldImg, n, err := decodeImage(tm, body[pos:], bitmap1)
		if err != nil {
			return nil, fmt.Errorf("%s.%s 的行镜像解不开: %w", tm.Schema, tm.Table, err)
		}
		pos += n
		if kind != source.EvUpdate {
			// 行事件只有一份镜像：INSERT 的是「新增后的值」，DELETE 的是「删除前的值」。
			// 归位必须按语义放对侧——删除的定位依据是删除前的值（Change.Old），
			// 放进 New 会让上层拿到一条「没有值」的删除事件（目标端 WHERE 全 NULL，
			// 静默删 0 行：数据永远删不掉，而日志里一切正常）。
			if kind == source.EvDelete {
				re.Changes = append(re.Changes, Change{Kind: kind, Old: oldImg})
			} else {
				re.Changes = append(re.Changes, Change{Kind: kind, New: oldImg})
			}
			continue
		}
		newImg, n, err := decodeImage(tm, body[pos:], bitmap2)
		if err != nil {
			return nil, fmt.Errorf("%s.%s 的更新后镜像解不开: %w", tm.Schema, tm.Table, err)
		}
		pos += n
		re.Changes = append(re.Changes, Change{Kind: kind, Old: oldImg, New: newImg})
	}
	return re, nil
}

// decodeImage reads one row image using the column bitmap.
func decodeImage(tm *TableMap, data []byte, bitmap []byte) (Image, int, error) {
	want := 0
	for i := 0; i < tm.Columns; i++ {
		if bitSet(bitmap, i) {
			want++
		}
	}
	nullBytes := bitmapByteSize(want)
	if len(data) < nullBytes {
		return Image{}, 0, errShort(0, nullBytes, len(data))
	}
	nullBitmap := data[:nullBytes]
	pos := nullBytes

	img := Image{
		Row:     source.Row{Table: source.TableRef{Schema: tm.Schema, Table: tm.Table}, Values: make([]source.Value, tm.Columns)},
		Present: make([]bool, tm.Columns),
	}
	nullIdx := 0
	for i := 0; i < tm.Columns; i++ {
		if !bitSet(bitmap, i) {
			img.Row.Values[i] = nullValue()
			continue
		}
		isNull := bitSet(nullBitmap, nullIdx)
		nullIdx++
		img.Present[i] = true
		if isNull {
			img.Row.Values[i] = nullValue()
			continue
		}
		v, n, err := decodeCell(tm, i, data[pos:])
		if err != nil {
			return Image{}, 0, err
		}
		pos += n
		img.Row.Values[i] = v
	}
	return img, pos, nil
}
