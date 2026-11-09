package mysqlsource

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/spencerswagger/jinliang/internal/core"
	"github.com/spencerswagger/jinliang/internal/dsn"
	"github.com/spencerswagger/jinliang/internal/source"
)

// debugEvents 打开后把每个事件类型打到 stderr（只在排查线上差异时开）。
var debugEvents = os.Getenv("JINLIANG_DEBUG_EVENTS") != ""

// Source 必须始终满足共享契约；契约一改，这里先炸，而不是等到引擎启动。
var _ source.Source = (*Source)(nil)

// Source is the MySQL read side: capability probing, catalog, consistent
// snapshot, and the binlog change stream. It implements source.Source from
// internal/source/contract.go without changing it.
type Source struct {
	db *sql.DB
	ep dsn.Endpoint

	// Stream tuning.
	ServerID        uint32
	HeartbeatPeriod time.Duration
	// UseGTID prefers the GTID checkpoint when the source has GTID on.
	UseGTID bool

	// JobID 是这个任务的稳定标识（job_key / 任务名）。给了就按它派生 server_id，
	// 重启后仍是同一个号——服务端会把上次没断干净的连接踢掉，这正是我们要的。
	// 不给则退化成「主机 + pid + 随机」，保证同机多任务也不会撞号。
	JobID string

	// MaxTxBytes / MaxTxEvents 是单事务缓冲上限；0 表示默认值（128 MB / 100 万事件）。
	// 超限明确报错（E1011），绝不 OOM。
	//
	// 注意对外曝光的路径：`schema/job.schema.json` 里 `reliability.tx_oversize` 目前只有
	// `spill | fail` 两个值，**没有数值上限**；所以给用户的话术只能是「拆小事务」或
	// 「用环境变量 JINLIANG_TX_MAX_BYTES / JINLIANG_TX_MAX_EVENTS 调高」，
	// 不能让他去改一个 schema 会拒绝的键（B2 待定 yaml 数值形状）。
	MaxTxBytes  int
	MaxTxEvents int

	lastNote SnapshotNote

	// snapMu 护住 snap：分块全量的读会话（BeginSnapshot → SnapshotChunksFrom → EndSnapshot）。
	// 读会话必须挂在 Source 上，因为「位点」和「一致性快照」必须在同一个连接里取，
	// 而引擎要在读第一块之前就把位点写进目标库。
	snapMu sync.Mutex
	snap   *snapshotSession

	// known / enriched：binlog_row_metadata=MINIMAL 时按 catalog 补列名（见 enrichRelation）。
	knownMu  sync.Mutex
	known    map[string]source.Table
	enriched map[*source.Table]*source.Table

	// lastCommit 是「下游已经成功处理完的最后一笔事务位点」——引擎的
	// SafeAckPoint 语义（MySQL 没有服务端 ack，但这个值仍要能读到）。
	lastCommit atomic.Value // Position
}

// Open builds a Source from a DSN (mysql://user:pass@host:3306/db or the
// driver's own form). The password never leaves this function's stack beyond
// the driver.
func Open(ctx context.Context, rawDSN string) (*Source, error) {
	ep, err := dsn.Parse(rawDSN)
	if err != nil {
		return nil, err
	}
	return OpenEndpoint(ctx, ep)
}

// OpenEndpoint 从**已解析的端点**构造（引擎手里就是 dsn.Endpoint，口令只在内存里）。
// 与 Open 走同一条构造路径，两处不会分叉。
func OpenEndpoint(ctx context.Context, ep dsn.Endpoint) (*Source, error) {
	if ep.Kind != dsn.KindMySQL {
		return nil, fmt.Errorf("这不是 MySQL 的连接串（%s）", ep.Kind)
	}
	db, err := sql.Open("mysql", buildDriverDSN(ep))
	if err != nil {
		return nil, fmt.Errorf("初始化 MySQL 连接失败: %w", err)
	}
	db.SetMaxOpenConns(16)
	db.SetConnMaxIdleTime(5 * time.Minute)
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("连不上源库 %s:%d: %w", ep.Host, ep.Port, err)
	}
	s := &Source{db: db, ep: ep, HeartbeatPeriod: 10 * time.Second}
	s.ServerID = serverIDFor("", ep)
	// 单事务缓冲上限：job 配置之外的兜底开关（运维改不了 job 文件时用环境变量）。
	// 默认 128 MB / 100 万事件，见 docs/STREAMING.md §1。
	if v := os.Getenv("JINLIANG_TX_MAX_BYTES"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			s.MaxTxBytes = int(n)
		}
	}
	if v := os.Getenv("JINLIANG_TX_MAX_EVENTS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			s.MaxTxEvents = n
		}
	}
	return s, nil
}

// Close releases the pool.
func (s *Source) Close() error { return s.db.Close() }

func buildDriverDSN(ep dsn.Endpoint) string {
	params := []string{"parseTime=true", "loc=UTC", "charset=utf8mb4", "interpolateParams=false"}
	if v, ok := paramFold(ep.Params, "tls"); ok {
		params = append(params, "tls="+v)
	}
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?%s",
		ep.Username, ep.Password, ep.Host, ep.Port, ep.Database, strings.Join(params, "&"))
}

// serverIDFor derives this stream's replication identity.
//
// MySQL 用 server_id 区分副本：**两条流撞号时服务端会主动踢掉先来的那条**
// （客户端看到的是「时不时断流」，极难与网络问题区分）。所以：
//   - 给了 jobKey：按「主机 + 源库坐标 + jobKey」派生，稳定且同机多任务互不相同；
//     重启后还是同一个号，服务端会把上次残留的连接踢掉；
//   - 没给 jobKey：主机 + pid + 随机，保证不撞号（不保证跨重启稳定）。
//
// 要对账 SHOW REPLICA STATUS 时用 Source.ServerID 显式指定。
func serverIDFor(jobKey string, ep dsn.Endpoint) uint32 {
	h := fnv.New32a()
	if host, err := os.Hostname(); err == nil {
		h.Write([]byte(host))
	}
	fmt.Fprintf(h, "|%s|%d|%s|%s", ep.Host, ep.Port, ep.Database, ep.Username)
	if jobKey != "" {
		h.Write([]byte("|" + jobKey))
	} else {
		if pid := os.Getpid(); pid > 0 {
			h.Write([]byte("|" + strconv.Itoa(pid)))
		}
		seed := make([]byte, 2)
		if _, err := rand.Read(seed); err == nil {
			h.Write(seed)
		}
	}
	id := h.Sum32() & 0x3fffffff
	if id < 1000 {
		id += 1000
	}
	return id
}

// Kind implements source.Endpoint.
func (s *Source) Kind() string { return dsn.KindMySQL }

// Version implements source.Endpoint.
func (s *Source) Version(ctx context.Context) (string, error) {
	var v string
	if err := s.db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&v); err != nil {
		return "", err
	}
	return v, nil
}

// Probe implements source.Endpoint.
func (s *Source) Probe(ctx context.Context) (source.CapSet, error) {
	res, err := NewProber(s.db).Probe(ctx)
	if err != nil {
		return nil, err
	}
	return res.Caps, nil
}

// ProbeDetail is Probe plus the preflight rows and the discovered statement
// names, for the UI and for run().
func (s *Source) ProbeDetail(ctx context.Context) (*ProbeResult, error) {
	return NewProber(s.db).Probe(ctx)
}

// Preflight implements source.Source.
func (s *Source) Preflight(ctx context.Context, req source.Requirements) (source.Report, error) {
	res, err := s.ProbeDetail(ctx)
	if err != nil {
		return source.Report{}, err
	}
	checks := res.Checks
	canStart := true
	for _, ch := range checks {
		if ch.Status == source.StatusBlocked {
			canStart = false
		}
	}
	for _, need := range req.Needs {
		state, ok := res.Caps[need]
		if ok && !state.Supported && state.Compensation.Level == source.LevelBlocked {
			canStart = false
			checks = append(checks, source.Check{
				ID:     "MY-REQ",
				Title:  "这次同步需要「" + string(need) + "」，源库没有",
				Status: source.StatusBlocked,
				Detail: state.Compensation.Note,
			})
		}
	}
	return source.Report{Checks: checks, CanStart: canStart}, nil
}

// Prepare implements source.Source. MySQL needs no server-side objects for a
// stream, so there is nothing to apply — and nothing to undo.
func (s *Source) Prepare(ctx context.Context, actions []source.Action) ([]source.Action, error) {
	out := make([]source.Action, 0, len(actions))
	for _, a := range actions {
		a.Applied = false
		a.Reason = "MySQL 的增量不依赖服务端对象（没有槽位/发布），无需准备"
		out = append(out, a)
	}
	return out, nil
}

// Cleanup implements source.Source: the only thing we create is a replication
// connection, which closes with the stream.
func (s *Source) Cleanup(ctx context.Context) ([]source.Action, error) {
	return nil, nil
}

// Advance implements source.Source. MySQL has no server-side ack: the
// checkpoint lives in the target database, so there is nothing to advance.
func (s *Source) Advance(ctx context.Context, to source.Position) error { return nil }

// Status reads the source's current checkpoint with millisecond precision.
//
// 取位点必须“同一瞬间”拿全：文件名+偏移、GTID 集合、以及取的那一刻（毫秒）。
// 高并发下按秒记会漏掉同一秒内已提交的事务，恢复时就会重放一大片。
func (s *Source) Status(ctx context.Context) (Position, error) {
	probe, err := s.ProbeDetail(ctx)
	if err != nil {
		return Position{}, err
	}
	stmt := probe.StatusStatement
	if stmt == "" {
		return Position{}, fmt.Errorf("读不到源库当前位点：既没有 SHOW BINARY LOG STATUS 也没有 SHOW MASTER STATUS")
	}
	return s.readStatus(ctx, stmt)
}

func (s *Source) readStatus(ctx context.Context, stmt string) (Position, error) {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return Position{}, err
	}
	defer conn.Close()

	takenAt := time.Now().UTC().Truncate(time.Millisecond)
	rows, err := conn.QueryContext(ctx, stmt)
	if err != nil {
		return Position{}, fmt.Errorf("执行 %s 失败: %w", stmt, err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return Position{}, err
	}
	if !rows.Next() {
		return Position{}, fmt.Errorf("源库没有返回位点（可能还没写过 binlog）")
	}
	raw := make([]sql.RawBytes, len(cols))
	dest := make([]any, len(cols))
	for i := range raw {
		dest[i] = &raw[i]
	}
	if err := rows.Scan(dest...); err != nil {
		return Position{}, err
	}
	pos := Position{TakenAt: takenAt}
	var gtidText string
	for i, name := range cols {
		val := string(raw[i])
		switch strings.ToLower(name) {
		case "file":
			pos.File = val
		case "position":
			n, err := strconv.ParseUint(val, 10, 32)
			if err != nil {
				return Position{}, fmt.Errorf("源库返回的位点 %q 不是数字", val)
			}
			pos.Pos = uint32(n)
		case "executed_gtid_set":
			gtidText = val
		}
	}
	if pos.File == "" {
		return Position{}, fmt.Errorf("%s 的返回里没有文件名，读不到位点", stmt)
	}
	if gtidText != "" {
		set, err := ParseGTIDSet(gtidText)
		if err != nil {
			return Position{}, err
		}
		pos.GTID = set
	}
	if v, ok := s.readServerID(ctx); ok {
		pos.ServerID = v
	}
	return pos, nil
}

func (s *Source) readServerID(ctx context.Context) (uint32, bool) {
	var id uint64
	if err := s.db.QueryRowContext(ctx, "SELECT @@server_id").Scan(&id); err != nil {
		return 0, false
	}
	return uint32(id), true
}

// Stream implements source.Source: it reads the binlog from `from` and hands
// whole transactions to handler.
func (s *Source) Stream(ctx context.Context, from source.Position, handler source.EventHandler) error {
	// TLS 闸门放在最前面：连库、读排序规则都不该发生在「这条链路注定不能开流」之前。
	if err := s.checkReplicationTLS(); err != nil {
		return err
	}
	start, err := s.resolveStart(ctx, from)
	if err != nil {
		return err
	}
	asm := NewAssembler(func(ctx context.Context, tx Tx) error {
		for _, ev := range tx.Events {
			// EvCommit 交付**之前**就把这笔事务的提交位点记下来：引擎的 batcher 在
			// EvCommit 上取 LastCommit 当作「这一批要写进目标库的位点」，晚一步就会
			// 永远落后一个事务（下次重启多重放一笔，幂等但会白做）。
			// 提前记不会丢数据：此刻数据还在引擎的批缓冲里，位点只有在
			// sink.Apply 成功之后才会随数据一起落库；而 batcher 在事务未提交前
			// 拒绝 AdvanceTo（inTx=true），所以不存在「位点先走、数据没写」。
			if ev.Kind == source.EvCommit {
				s.markCommitted(tx.Commit)
			}
			if ev.Relation != nil {
				ev.Relation = s.enrichRelation(ev.Relation)
			}
			if err := handler(ctx, ev); err != nil {
				return err
			}
		}
		// 整笔事务都交付成功后才算「已提交」：这正是 ack 的安全点。
		s.markCommitted(tx.Commit)
		return nil
	})
	asm.Collations = s.loadCollations(ctx)
	s.applyLimits(asm)
	return s.RunStream(ctx, start, asm)
}

// ParsePosition implements source.Source: 把目标库里存着的位点串还原成本连接器的
// 类型。两种文本形式（file+pos 与 GTID）都认；解析失败一律返回错误，不 panic、
// 也不静默回落成全量重搬。
func (s *Source) ParsePosition(raw string) (source.Position, error) {
	p, err := ParsePosition(raw)
	if err == nil {
		return p, nil
	}
	trimmed := strings.TrimSpace(raw)
	if errors.Is(err, ErrPositionKindMismatch) {
		// E1010 是 user_action：位点属于别的类型的源，换回那个源就好，不要重搬全量。
		return nil, fmt.Errorf("%s %s（位点 %q）：%w",
			core.CodePositionKindMismatch, mustMessage(core.CodePositionKindMismatch), clip(trimmed, 48), err)
	}
	return nil, fmt.Errorf("%s 位点读不出来（%q）：%w", core.CodePositionGap, clip(trimmed, 48), err)
}

// mustMessage 取错误码的人话标题（文案由 internal/core 统一维护）。
func mustMessage(c core.Code) string {
	if spec, ok := core.Lookup(c); ok {
		return spec.Message
	}
	return string(c)
}

func clip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// resolveStart turns the opaque checkpoint into coordinates the server accepts.
func (s *Source) resolveStart(ctx context.Context, from source.Position) (Position, error) {
	if from == nil {
		return s.Status(ctx)
	}
	parsed, err := s.ParsePosition(from.String())
	if err != nil {
		return Position{}, err
	}
	saved, ok := parsed.(Position)
	if !ok {
		return Position{}, fmt.Errorf("位点类型 %q 不认识（内部错误）", parsed.Kind())
	}
	if !s.UseGTID {
		saved.GTID = GTIDSet{}
	}
	return saved, nil
}

// applyLimits 把 job 配置里的单事务上限套到装配器上。
func (s *Source) applyLimits(asm *Assembler) {
	if s.MaxTxBytes > 0 {
		asm.MaxBytes = s.MaxTxBytes
	}
	if s.MaxTxEvents > 0 {
		asm.MaxEvents = s.MaxTxEvents
	}
}

// effectiveServerID 决定这条流用哪个 server_id（显式指定 > 按 job 派生 > 随机）。
func (s *Source) effectiveServerID() uint32 {
	if s.ServerID != 0 {
		return s.ServerID
	}
	return serverIDFor(s.JobID, s.ep)
}

// checkReplicationTLS 是复制连接的 TLS 闸门（fail-closed）。
//
// 现状：SQL 连接可以走 driver 的 tls=…，但**复制连接（COM_BINLOG_DUMP）还不支持 TLS**。
// 如果用户按 tls=verify_identity 建了 job 而我们照旧开流，结果会是：SQL 侧加密、
// binlog 行数据与 caching_sha2 口令交换全走明文——这比「不支持 TLS」危险得多，
// 因为它看起来是安全的。所以 tls 参数一旦要求加密，就拒绝开流并说清怎么办。
// paramFold 按不区分大小写找参数：用户写 `?TLS=verify_identity` 或 `?Tls=…` 时，
// 精确取键会漏掉，于是「配了加密却静默走明文」——正是这条闸门要防的事。
func paramFold(params map[string]string, key string) (string, bool) {
	if v, ok := params[key]; ok {
		return v, true
	}
	for k, v := range params {
		if strings.EqualFold(k, key) {
			return v, true
		}
	}
	return "", false
}

func (s *Source) checkReplicationTLS() error {
	raw := ""
	if v, ok := paramFold(s.ep.Params, "tls"); ok {
		raw = strings.ToLower(strings.TrimSpace(v))
	}
	switch raw {
	case "", "false", "off", "disabled", "none":
		return nil
	}
	return fmt.Errorf("连到 %s:%d 的复制连接不支持 TLS（tls=%s），不能开流。"+
		"原因：查询/全量可以走 TLS，但读变更流水的那条连接目前是明文；"+
		"如果就这样开流，binlog 里的行数据与 caching_sha2 口令交换都会以明文经过网络，"+
		"而你看到的配置是「已加密」——这比不支持 TLS 更危险，所以这里直接停。"+
		"你能做的：① 只在内网/回环或已有 VPN 的链路上同步（建议把源库绑内网地址并限制来源）；"+
		"② 用 ssh 隧道/跳板机把 3306 转到本机再连 localhost；③ 等我们支持 TLS 复制连接。"+
		"注意：tls=preferred / skip-verify 同样会被拒绝——复制连接目前不支持**任何** TLS 形态。"+
		"「能加密就加密」「加密但不校验」这两种语义我们都没实现，放行只会让你以为拿到了加密，"+
		"实际仍是明文。确实需要加密：走 VPN / ssh 隧道；确定链路可信：显式设 tls=off 承认明文并限制网络访问。"+
		"风险：明文链路上同网段的人能看到全部行数据与查询语句；口令本身用 challenge-response，"+
		"不会明文发送，但整条链路没有保密性、也挡不住中间人",
		s.ep.Host, s.ep.Port, raw)
}

// RunStream opens the replication connection and feeds the assembler until ctx
// is cancelled or the source fails.
func (s *Source) RunStream(ctx context.Context, start Position, asm *Assembler) error {
	if err := s.checkReplicationTLS(); err != nil {
		return err
	}
	cfg := ClientConfig{
		Host:      s.ep.Host,
		Port:      s.ep.Port,
		User:      s.ep.Username,
		Password:  s.ep.Password,
		Database:  s.ep.Database,
		ServerID:  s.effectiveServerID(),
		Heartbeat: s.HeartbeatPeriod,
	}
	s.applyLimits(asm)
	conn, err := DialReplication(ctx, cfg)
	if err != nil {
		return err
	}
	defer conn.Close()

	// 取消要**立刻**生效：ReadEventPacket 会一直阻塞到下一帧或心跳（默认 10 秒），
	// 只在循环开头查 ctx.Done() 意味着「用户按停后还要等 10 秒」，收尾路径里的
	// drain/flush 全被拖住（引擎那边看到的就是「--stop-after 3s 实际跑了 10s」）。
	// 关掉连接让读立刻返回，这才是取消该有的形状。
	stopClose := make(chan struct{})
	defer close(stopClose)
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-stopClose:
		}
	}()

	if err := conn.execute(`SET @master_binlog_checksum='CRC32', @source_binlog_checksum='CRC32'`); err != nil {
		return err
	}
	if s.HeartbeatPeriod > 0 {
		// @master_heartbeat_period 的单位是**纳秒**（time.Duration 本身就是），
		// 换成微秒会变成 10ms 一次的心跳洪水。
		period := uint64(s.HeartbeatPeriod / time.Nanosecond)
		if err := conn.execute(fmt.Sprintf(
			"SET @master_heartbeat_period=%d, @source_heartbeat_period=%d", period, period)); err != nil {
			return err
		}
	}
	if err := conn.registerReplica(ctx); err != nil {
		return err
	}
	if err := conn.startBinlogDump(start); err != nil {
		return err
	}

	parser := NewParser()
	parser.SetFile(start.File)
	if !start.GTID.IsEmpty() {
		asm.SetExecutedGTID(start.GTID)
	}
	asm.SetPosition(start)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		frame, err := conn.ReadEventPacket(ctx)
		if err != nil {
			return fmt.Errorf("读取变更流中断: %w", err)
		}
		if len(frame) == 0 {
			continue
		}
		events, err := parser.Parse(frame)
		if err != nil {
			return err
		}
		if debugEvents {
			for _, ev := range events {
				switch {
				case ev.GTID != nil:
					fmt.Fprintf(os.Stderr, "[binlog] gtid gno=%d pos=%d\n", ev.GTID.GNO, ev.Header.LogPos)
				case ev.Query != nil:
					fmt.Fprintf(os.Stderr, "[binlog] query %q pos=%d\n", clip(ev.Query.Query, 40), ev.Header.LogPos)
				case ev.TableMap != nil:
					fmt.Fprintf(os.Stderr, "[binlog] table_map %s.%s cols=%d pos=%d\n",
						ev.TableMap.Schema, ev.TableMap.Table, ev.TableMap.Columns, ev.Header.LogPos)
				case ev.Rows != nil:
					fmt.Fprintf(os.Stderr, "[binlog] rows %s.%s n=%d pos=%d\n",
						ev.Rows.Table.Schema, ev.Rows.Table.Table, len(ev.Rows.Changes), ev.Header.LogPos)
				case ev.XID != nil:
					fmt.Fprintf(os.Stderr, "[binlog] commit xid=%d pos=%d\n", ev.XID.XID, ev.Header.LogPos)
				default:
					fmt.Fprintf(os.Stderr, "[binlog] type=%d size=%d pos=%d\n", ev.Header.Type, ev.Header.Size, ev.Header.LogPos)
				}
			}
		}
		if err != nil {
			return err
		}
		for _, ev := range events {
			if ev.Header.Size >= headerSize {
				ev.Offset = ev.Header.LogPos - ev.Header.Size
			} else {
				ev.Offset = ev.Header.LogPos
			}
			if ev.File == "" {
				ev.File = parser.File()
			}
			if err := asm.Feed(ctx, ev); err != nil {
				return err
			}
		}
	}
}
