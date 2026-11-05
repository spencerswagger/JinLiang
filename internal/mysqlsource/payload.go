package mysqlsource

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/klauspost/compress/zstd"
)

// Compression types inside a TRANSACTION_PAYLOAD event
// (MySQL binary_log::codecs::binary::Transaction_payload::compression_type).
const (
	compressionZSTD = 0
	compressionNone = 255
)

// Field type codes of the payload header, on the wire.
const (
	payloadHeaderEnd   = 0
	payloadSizeField   = 1
	payloadCompression = 2
	payloadUncompSize  = 3
)

// ErrCompressionUnsupported is returned when a compressed transaction uses an
// algorithm we cannot decode. We say which one instead of failing silently.
var ErrCompressionUnsupported = errors.New("事务压缩算法不支持")

// Payload is a TRANSACTION_PAYLOAD event (MySQL 8.0.20+ with
// binlog_transaction_compression=ON): one whole transaction, compressed.
type Payload struct {
	Size             uint64
	UncompressedSize uint64
	Compression      uint64
	Data             []byte // still compressed
}

// parsePayload reads the TLV header and keeps the compressed bytes.
//
// 线上格式（MySQL binary_log/codecs/binary/transaction_payload.cpp）：
//
//	type(1) len(1) value(len) … 直到 type=0 的结束标记，之后是压缩后的载荷。
func parsePayload(body []byte) (*Payload, error) {
	p := &Payload{}
	pos := 0
	for {
		if pos >= len(body) {
			return nil, fmt.Errorf("压缩事务的头没有结束标记（已读 %d 字节）", pos)
		}
		fieldType := body[pos]
		pos++
		if fieldType == payloadHeaderEnd {
			p.Data = body[pos:]
			break
		}
		if pos >= len(body) {
			return nil, fmt.Errorf("压缩事务的字段 0x%02x 缺少长度", fieldType)
		}
		fieldLen := int(body[pos])
		pos++
		if pos+fieldLen > len(body) {
			return nil, fmt.Errorf("压缩事务的字段 0x%02x 越界（长度 %d，剩余 %d）", fieldType, fieldLen, len(body)-pos)
		}
		value := fixedInt(body[pos : pos+fieldLen])
		pos += fieldLen
		switch fieldType {
		case payloadSizeField:
			p.Size = value
		case payloadCompression:
			p.Compression = value
		case payloadUncompSize:
			p.UncompressedSize = value
		default:
			// 未知字段：跳过，保持前向兼容。
		}
	}
	if p.Size != 0 && uint64(len(p.Data)) != p.Size {
		return nil, fmt.Errorf("压缩事务说载荷 %d 字节，实际 %d 字节", p.Size, len(p.Data))
	}
	return p, nil
}

// Decompress expands the payload into the raw event stream it encodes.
func (p *Payload) Decompress() ([]byte, error) {
	switch p.Compression {
	case compressionNone:
		return p.Data, nil
	case compressionZSTD:
		dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
		if err != nil {
			return nil, fmt.Errorf("%w：初始化 zstd 解码器失败: %v", ErrCompressionUnsupported, err)
		}
		defer dec.Close()
		out, err := dec.DecodeAll(p.Data, nil)
		if err != nil {
			return nil, fmt.Errorf("压缩事务解压失败（zstd）：%w；源端可关掉 binlog_transaction_compression", err)
		}
		if p.UncompressedSize != 0 && uint64(len(out)) != p.UncompressedSize {
			return nil, fmt.Errorf("压缩事务解压后 %d 字节，头里写的是 %d 字节", len(out), p.UncompressedSize)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("%w：类型 %d（只认 0=zstd、255=不压缩）；"+
			"请在源端执行 SET GLOBAL binlog_transaction_compression=OFF 后重新取位点", ErrCompressionUnsupported, p.Compression)
	}
}

// splitPayloadEvents cuts an uncompressed payload into individual events. Each
// inner event carries its own 19-byte header and no CRC (the CRC covers the
// outer payload event only).
func splitPayloadEvents(data []byte) ([][]byte, error) {
	var out [][]byte
	for pos := 0; pos < len(data); {
		if len(data)-pos < headerSize {
			// 末尾可能剩几个填充/校验字节；不足一个事件头就结束。
			break
		}
		size := int(binary.LittleEndian.Uint32(data[pos+9 : pos+13]))
		if size < headerSize || pos+size > len(data) {
			return nil, fmt.Errorf("压缩事务里的第 %d 个事件长度 %d 越界（载荷 %d 字节）", len(out)+1, size, len(data))
		}
		out = append(out, data[pos:pos+size])
		pos += size
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("压缩事务里没有任何事件")
	}
	return out, nil
}
