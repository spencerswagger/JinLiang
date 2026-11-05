package mysqlsource

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/x509"
	"encoding/binary"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

// COM_* commands we need.
const (
	comQuery       = 0x03
	comRegisterRep = 0x15
	comBinlogDump  = 0x12
	comBinlogDumpG = 0x1e
)

// Capability flags of the client/server protocol.
const (
	capLongPassword     = 0x00000001
	capConnectWithDB    = 0x00000008
	capProtocol41       = 0x00000200
	capSSL              = 0x00000800 // 我们不实现 TLS 复制连接，必须显式不宣告
	capTransactions     = 0x00002000
	capSecureConnection = 0x00008000 // 名字里的 secure 指「口令用 scramble 不发明文」，不是 TLS
	capPluginAuth       = 0x00080000
	capPluginAuthLen    = 0x00200000
	capSessionTrack     = 0x00800000
)

// ClientConfig is everything needed to open a replication connection.
type ClientConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string

	// ServerID must be unique among replicas of this source; the source uses it
	// to know which stream to kill. 0 means "pick one from the host name".
	ServerID uint32
	// Heartbeat asks the source to send an idle heartbeat this often
	// (@master_heartbeat_period). 0 disables it.
	Heartbeat time.Duration
	// Timeout bounds the handshake and each read while idle.
	Timeout time.Duration
}

// ClientConn is a minimal MySQL client that can do one thing the driver cannot:
// stream binlog events off a replication connection.
type ClientConn struct {
	netConn  net.Conn
	buf      *bufio.Reader
	seq      uint8
	cfg      ClientConfig
	server   string
	threadID uint32
}

// DialReplication performs the handshake and authenticates.
func DialReplication(ctx context.Context, cfg ClientConfig) (*ClientConn, error) {
	if cfg.Port == 0 {
		cfg.Port = 3306
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 10 * time.Second
	}
	addr := net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))
	d := net.Dialer{Timeout: cfg.Timeout}
	nc, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("连不上源库 %s: %w", addr, err)
	}
	c := &ClientConn{netConn: nc, buf: bufio.NewReaderSize(nc, 64<<10), cfg: cfg}
	if err := c.handshake(); err != nil {
		nc.Close()
		return nil, err
	}
	return c, nil
}

// Close closes the connection.
func (c *ClientConn) Close() error { return c.netConn.Close() }

// ServerVersion is what the handshake reported (e.g. "8.0.28").
func (c *ClientConn) ServerVersion() string { return c.server }

// ThreadID is the connection id (for KILL on shutdown).
func (c *ClientConn) ThreadID() uint32 { return c.threadID }

func (c *ClientConn) handshake() error {
	_ = c.netConn.SetDeadline(time.Now().Add(c.cfg.Timeout))
	greeting, err := c.readPacket()
	if err != nil {
		return fmt.Errorf("读服务器问候失败: %w", err)
	}
	if len(greeting) > 0 && greeting[0] == 0xff {
		return parseErrPacket(greeting)
	}
	if len(greeting) < 1 || greeting[0] < 10 {
		return fmt.Errorf("服务器问候的协议版本 %d 不认识", greeting[0])
	}
	pos := 1
	end := indexByte(greeting[pos:], 0)
	if end < 0 {
		return fmt.Errorf("服务器问候里的版本串没有结尾")
	}
	c.server = string(greeting[pos : pos+end])
	pos += end + 1
	if pos+4+8+1+2 > len(greeting) {
		return fmt.Errorf("服务器问候被截断")
	}
	c.threadID = binary.LittleEndian.Uint32(greeting[pos:])
	pos += 4
	seed := make([]byte, 0, 20)
	seed = append(seed, greeting[pos:pos+8]...)
	pos += 8 + 1 // filler
	capLow := uint32(binary.LittleEndian.Uint16(greeting[pos:]))
	pos += 2
	pos += 1 + 2 // charset + status flags
	capHigh := uint32(0)
	if pos+2 <= len(greeting) {
		capHigh = uint32(binary.LittleEndian.Uint16(greeting[pos:]))
		pos += 2
	}
	serverCaps := capLow | capHigh<<16
	authLen := 0
	if pos < len(greeting) {
		authLen = int(greeting[pos])
		pos++
	}
	pos += 10 // reserved
	if pos < len(greeting) {
		rest := greeting[pos:]
		if authLen > 8 && len(rest) >= authLen-8 {
			seed = append(seed, rest[:authLen-8]...)
			pos += authLen - 8
		} else if len(rest) > 13 {
			seed = append(seed, rest[:13]...)
			pos += 13
		}
	}
	plugin := "mysql_native_password"
	if pos < len(greeting) {
		if end := indexByte(greeting[pos:], 0); end >= 0 {
			plugin = string(greeting[pos : pos+end])
		} else {
			plugin = string(greeting[pos:])
		}
	}
	// 交给服务器的 seed 是 20 字节，末尾 0x00 要去掉。
	seed = trimNul(seed)

	caps := uint32(capLongPassword | capProtocol41 | capTransactions | capSecureConnection |
		capPluginAuth | capPluginAuthLen | capSessionTrack)
	if c.cfg.Database != "" {
		caps |= capConnectWithDB
	}
	// 只保留「我也支持」的位，并且**永远不宣告 CLIENT_SSL**：宣告了却不发 SSLRequest，
	// 服务器会按 TLS 握手来回等，最后超时——比不支持更难查。
	caps &= serverCaps | capProtocol41
	caps &^= capSSL

	auth, err := scramble(plugin, c.cfg.Password, seed)
	if err != nil {
		return err
	}

	payload := make([]byte, 0, 128)
	payload = append(payload, u32(caps)...)
	payload = append(payload, u32(0xffffff)...) // max packet
	payload = append(payload, 45)               // utf8mb4_general_ci
	payload = append(payload, make([]byte, 23)...)
	payload = append(payload, []byte(c.cfg.User)...)
	payload = append(payload, 0)
	payload = append(payload, u8len(len(auth))...)
	payload = append(payload, auth...)
	if c.cfg.Database != "" {
		payload = append(payload, []byte(c.cfg.Database)...)
		payload = append(payload, 0)
	}
	if caps&capPluginAuth != 0 {
		payload = append(payload, []byte(plugin)...)
		payload = append(payload, 0)
	}
	if err := c.writePacket(payload); err != nil {
		return fmt.Errorf("发认证包失败: %w", err)
	}
	return c.finishAuth(plugin, seed)
}

func (c *ClientConn) finishAuth(plugin string, seed []byte) error {
	for {
		resp, err := c.readPacket()
		if err != nil {
			return fmt.Errorf("读认证结果失败: %w", err)
		}
		if len(resp) == 0 {
			return fmt.Errorf("服务器返回了空包")
		}
		switch resp[0] {
		case 0x00:
			return nil
		case 0xff:
			return parseErrPacket(resp)
		case 0xfe: // auth switch request
			if len(resp) == 1 {
				return fmt.Errorf("服务器要求切换认证方式但没说换成什么")
			}
			name, rest := splitNul(resp[1:])
			plugin = name
			seed = trimNul(rest)
			auth, err := scramble(plugin, c.cfg.Password, seed)
			if err != nil {
				return err
			}
			if err := c.writePacket(auth); err != nil {
				return err
			}
		case 0x01: // more data (caching_sha2)
			if len(resp) < 2 {
				return fmt.Errorf("caching_sha2 的中间包内容不全")
			}
			switch resp[1] {
			case 0x03: // fast auth ok，后面还有一个 OK 包
				continue
			case 0x04: // full auth：要公钥再加密
				if err := c.writePacket([]byte{0x02}); err != nil {
					return err
				}
				key, err := c.readPacket()
				if err != nil {
					return err
				}
				if len(key) == 0 || key[0] != 0x01 {
					return fmt.Errorf("服务器没有返回公钥（caching_sha2 完整认证失败）")
				}
				if err := c.sendEncryptedPassword(seed, key[1:]); err != nil {
					return err
				}
			default:
				return fmt.Errorf("caching_sha2 的中间状态 %d 不认识", resp[1])
			}
		default:
			return fmt.Errorf("认证结果里出现意料之外的包类型 0x%02x", resp[0])
		}
	}
}

// sendEncryptedPassword does caching_sha2's full-auth over a plain connection:
// XOR the password with the nonce, then RSA-OAEP(SHA1) it with the server key.
func (c *ClientConn) sendEncryptedPassword(seed, pemBytes []byte) error {
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return fmt.Errorf("服务器公钥不是 PEM 格式")
	}
	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return fmt.Errorf("解析服务器公钥失败: %w", err)
	}
	rsaPub, ok := pub.(*rsa.PublicKey)
	if !ok {
		return fmt.Errorf("服务器公钥不是 RSA 公钥")
	}
	plain := append([]byte(c.cfg.Password), 0)
	for i := range plain {
		plain[i] ^= seed[i%len(seed)]
	}
	enc, err := rsa.EncryptOAEP(sha1.New(), rand.Reader, rsaPub, plain, nil)
	if err != nil {
		return fmt.Errorf("加密口令失败: %w", err)
	}
	return c.writePacket(enc)
}

// execute runs one statement and drains its reply (we only use SETs).
func (c *ClientConn) execute(query string) error {
	c.seq = 0
	payload := append([]byte{comQuery}, []byte(query)...)
	if err := c.writePacket(payload); err != nil {
		return err
	}
	for {
		resp, err := c.readPacket()
		if err != nil {
			return fmt.Errorf("执行 %q 失败: %w", query, err)
		}
		if len(resp) == 0 {
			return nil
		}
		switch resp[0] {
		case 0x00:
			return nil
		case 0xff:
			return parseErrPacket(resp)
		case 0xfe:
			if len(resp) < 9 {
				return nil
			}
		default:
			// 结果集：这里不该出现，但读干净再返回，别把流搞乱。
			if !c.drainResultSet() {
				return nil
			}
		}
	}
}

func (c *ClientConn) drainResultSet() bool {
	for {
		resp, err := c.readPacket()
		if err != nil {
			return false
		}
		if len(resp) > 0 && (resp[0] == 0xfe || resp[0] == 0x00) && len(resp) < 9 {
			return true
		}
	}
}

// registerReplica tells the source who we are (optional but it makes
// SHOW REPLICAS / SHOW PROCESSLIST readable).
func (c *ClientConn) registerReplica(ctx context.Context) error {
	host, _ := net.LookupAddr(c.cfg.Host)
	name := "jinliang"
	if len(host) > 0 {
		name = strings.TrimSuffix(host[0], ".")
	}
	payload := []byte{comRegisterRep}
	payload = append(payload, u32(c.cfg.ServerID)...)
	payload = append(payload, byte(len(name)))
	payload = append(payload, name...)
	payload = append(payload, byte(len(c.cfg.User)))
	payload = append(payload, c.cfg.User...)
	payload = append(payload, 0)
	payload = append(payload, byte(c.cfg.Port>>8), byte(c.cfg.Port))
	payload = append(payload, u32(0)...)
	payload = append(payload, u32(0)...)
	c.seq = 0
	if err := c.writePacket(payload); err != nil {
		return err
	}
	resp, err := c.readPacket()
	if err != nil {
		return err
	}
	if len(resp) > 0 && resp[0] == 0xff {
		// 注册失败不影响取流（很多云上账号没有这个权限），只记一笔。
		return nil
	}
	return nil
}

// startBinlogDump asks the source to start streaming at pos.
func (c *ClientConn) startBinlogDump(pos Position) error {
	useGTID := !pos.GTID.IsEmpty()
	var payload []byte
	if useGTID {
		gtid := encodeGTIDSet(pos.GTID)
		payload = append(payload, comBinlogDumpG)
		payload = append(payload, 0, 0) // flags
		payload = append(payload, u32(c.cfg.ServerID)...)
		payload = append(payload, u32(0)...) // 文件名长度 0：按 GTID 找位点
		payload = append(payload, u64(4)...)
		payload = append(payload, u32(uint32(len(gtid)))...)
		payload = append(payload, gtid...)
	} else {
		payload = append(payload, comBinlogDump)
		payload = append(payload, u32(pos.Pos)...)
		payload = append(payload, 0, 0) // flags：阻塞式
		payload = append(payload, u32(c.cfg.ServerID)...)
		payload = append(payload, []byte(pos.File)...)
	}
	c.seq = 0
	return c.writePacket(payload)
}

// encodeGTIDSet is the wire form MySQL expects in COM_BINLOG_DUMP_GTID.
func encodeGTIDSet(g GTIDSet) []byte {
	out := u64(uint64(len(g.sets)))
	for uuid, ivs := range g.sets {
		var sid [16]byte
		if raw, ok := parseUUIDBytes(uuid); ok {
			sid = raw
		}
		out = append(out, sid[:]...)
		out = append(out, u64(uint64(len(ivs)))...)
		for _, iv := range ivs {
			// MySQL 的线上格式是半开区间 [start, stop)：end 写的是「最后一个 +1」。
			// 写成 stop-1 会少一个事务，服务器就把那一笔重放一遍（9.x 矩阵实测到过）。
			out = append(out, u64(uint64(iv.start))...)
			out = append(out, u64(uint64(iv.stop))...)
		}
	}
	return out
}

func parseUUIDBytes(s string) ([16]byte, bool) {
	var out [16]byte
	if i := strings.IndexByte(s, ':'); i >= 0 {
		s = s[:i] // 带 tag 的 GTID：线上仍按 uuid 段
	}
	clean := strings.ReplaceAll(s, "-", "")
	if len(clean) != 32 {
		return out, false
	}
	for i := 0; i < 16; i++ {
		hi := hexVal(clean[i*2])
		lo := hexVal(clean[i*2+1])
		if hi < 0 || lo < 0 {
			return out, false
		}
		out[i] = byte(hi<<4 | lo)
	}
	return out, true
}

func hexVal(c byte) int {
	switch {
	case c >= '0' && c <= '9':
		return int(c - '0')
	case c >= 'a' && c <= 'f':
		return int(c-'a') + 10
	case c >= 'A' && c <= 'F':
		return int(c-'A') + 10
	}
	return -1
}

// ReadEventPacket returns the next binlog event frame from the dump stream.
//
// dump 流里每个事件包都带一个 0x00 前缀（MySQL 的 OK 包头），去掉它剩下的
// 才是真正的 binlog 事件；0xFF 是错误包，0xFE 且包体不足 9 字节是 EOF。
func (c *ClientConn) ReadEventPacket(ctx context.Context) ([]byte, error) {
	if deadline, ok := ctx.Deadline(); ok {
		_ = c.netConn.SetReadDeadline(deadline)
	} else {
		_ = c.netConn.SetReadDeadline(time.Now().Add(60 * time.Second))
	}
	payload, err := c.readPacket()
	if err != nil {
		return nil, err
	}
	if len(payload) == 0 {
		return nil, nil
	}
	switch payload[0] {
	case 0xff:
		return nil, parseErrPacket(payload)
	case 0xfe:
		if len(payload) < 9 {
			return nil, io.EOF
		}
	case 0x00:
		return payload[1:], nil
	}
	return payload, nil
}

func (c *ClientConn) readPacket() ([]byte, error) {
	var out []byte
	for {
		head := make([]byte, 4)
		if _, err := io.ReadFull(c.buf, head); err != nil {
			return nil, err
		}
		n := int(head[0]) | int(head[1])<<8 | int(head[2])<<16
		c.seq = head[3] + 1
		buf := make([]byte, n)
		if _, err := io.ReadFull(c.buf, buf); err != nil {
			return nil, err
		}
		out = append(out, buf...)
		if n < 0xffffff {
			return out, nil
		}
	}
}

func (c *ClientConn) writePacket(payload []byte) error {
	for {
		n := len(payload)
		if n > 0xffffff {
			n = 0xffffff
		}
		frame := make([]byte, 4+n)
		frame[0] = byte(n)
		frame[1] = byte(n >> 8)
		frame[2] = byte(n >> 16)
		frame[3] = c.seq
		copy(frame[4:], payload[:n])
		if _, err := c.netConn.Write(frame); err != nil {
			return err
		}
		c.seq++
		payload = payload[n:]
		if n < 0xffffff {
			return nil
		}
	}
}

func scramble(plugin, password string, seed []byte) ([]byte, error) {
	if password == "" {
		return nil, nil
	}
	switch plugin {
	case "mysql_native_password", "":
		h1 := sha1.Sum([]byte(password))
		h2 := sha1.Sum(h1[:])
		h3 := sha1.New()
		h3.Write(seed)
		h3.Write(h2[:])
		sum := h3.Sum(nil)
		out := make([]byte, len(h1))
		for i := range h1 {
			out[i] = h1[i] ^ sum[i]
		}
		return out, nil
	case "caching_sha2_password":
		h1 := sha256.Sum256([]byte(password))
		h2 := sha256.Sum256(h1[:])
		h3 := sha256.New()
		h3.Write(h2[:])
		h3.Write(seed)
		sum := h3.Sum(nil)
		out := make([]byte, len(h1))
		for i := range h1 {
			out[i] = h1[i] ^ sum[i]
		}
		return out, nil
	default:
		return nil, fmt.Errorf("认证插件 %q 不认识（只支持 mysql_native_password / caching_sha2_password）；"+
			"请把账号改成这两种之一", plugin)
	}
}

func parseErrPacket(b []byte) error {
	if len(b) < 3 {
		return fmt.Errorf("服务器返回了错误包，但内容不全")
	}
	code := binary.LittleEndian.Uint16(b[1:3])
	msg := string(b[3:])
	if len(b) > 9 && b[3] == '#' {
		msg = string(b[9:])
	}
	return fmt.Errorf("源库返回错误 %d：%s", code, strings.TrimSpace(msg))
}

func indexByte(b []byte, c byte) int {
	for i := range b {
		if b[i] == c {
			return i
		}
	}
	return -1
}

func splitNul(b []byte) (string, []byte) {
	if i := indexByte(b, 0); i >= 0 {
		return string(b[:i]), b[i+1:]
	}
	return string(b), nil
}

func trimNul(b []byte) []byte {
	for len(b) > 0 && b[len(b)-1] == 0 {
		b = b[:len(b)-1]
	}
	return b
}

func u32(v uint32) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, v)
	return b
}

func u64(v uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, v)
	return b
}

// u8len encodes a length-prefixed string the way the handshake wants it.
func u8len(n int) []byte {
	switch {
	case n < 251:
		return []byte{byte(n)}
	case n < 1<<16:
		return []byte{0xfc, byte(n), byte(n >> 8)}
	default:
		return []byte{0xfd, byte(n), byte(n >> 8), byte(n >> 16)}
	}
}
