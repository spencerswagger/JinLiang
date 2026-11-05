package mysqlsource

import (
	"context"
	"database/sql"
	"fmt"
	"hash/fnv"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/spencerswagger/jinliang/internal/dsn"
	"github.com/spencerswagger/jinliang/internal/source"
)

// Source is the MySQL read side: capability probing, catalog, consistent
// snapshot, and the binlog change stream. It implements source.Source from
// internal/source/contract.go without changing it.
type Source struct {
	db  *sql.DB
	ep  dsn.Endpoint
	raw string

	// Stream tuning.
	ServerID        uint32
	HeartbeatPeriod time.Duration
	// UseGTID prefers the GTID checkpoint when the source has GTID on.
	UseGTID bool

	lastNote SnapshotNote
}

// Open builds a Source from a DSN (mysql://user:pass@host:3306/db or the
// driver's own form). The password never leaves this function's stack beyond
// the driver.
func Open(ctx context.Context, rawDSN string) (*Source, error) {
	ep, err := dsn.Parse(rawDSN)
	if err != nil {
		return nil, err
	}
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
	s := &Source{db: db, ep: ep, raw: rawDSN, HeartbeatPeriod: 10 * time.Second}
	s.ServerID = defaultServerID()
	return s, nil
}

// Close releases the pool.
func (s *Source) Close() error { return s.db.Close() }

func buildDriverDSN(ep dsn.Endpoint) string {
	params := []string{"parseTime=true", "loc=UTC", "charset=utf8mb4", "interpolateParams=false"}
	if v, ok := ep.Params["tls"]; ok {
		params = append(params, "tls="+v)
	}
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?%s",
		ep.Username, ep.Password, ep.Host, ep.Port, ep.Database, strings.Join(params, "&"))
}

// defaultServerID keeps the stream's identity stable across restarts of the
// same job, and different between jobs on the same source.
func defaultServerID() uint32 {
	host, _ := os.Hostname()
	h := fnv.New32a()
	h.Write([]byte(host))
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
	start, err := s.resolveStart(ctx, from)
	if err != nil {
		return err
	}
	asm := NewAssembler(func(ctx context.Context, tx Tx) error {
		for _, ev := range tx.Events {
			if err := handler(ctx, ev); err != nil {
				return err
			}
		}
		return nil
	})
	return s.RunStream(ctx, start, asm)
}

// resolveStart turns the opaque checkpoint into coordinates the server accepts.
func (s *Source) resolveStart(ctx context.Context, from source.Position) (Position, error) {
	if from == nil {
		return s.Status(ctx)
	}
	saved, err := ParsePosition(from.String())
	if err != nil {
		return Position{}, err
	}
	if !s.UseGTID {
		saved.GTID = GTIDSet{}
	}
	return saved, nil
}

// RunStream opens the replication connection and feeds the assembler until ctx
// is cancelled or the source fails.
func (s *Source) RunStream(ctx context.Context, start Position, asm *Assembler) error {
	cfg := ClientConfig{
		Host:      s.ep.Host,
		Port:      s.ep.Port,
		User:      s.ep.Username,
		Password:  s.ep.Password,
		Database:  s.ep.Database,
		ServerID:  s.ServerID,
		Heartbeat: s.HeartbeatPeriod,
	}
	conn, err := DialReplication(ctx, cfg)
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := conn.execute(`SET @master_binlog_checksum='CRC32', @source_binlog_checksum='CRC32'`); err != nil {
		return err
	}
	if s.HeartbeatPeriod > 0 {
		period := uint64(s.HeartbeatPeriod / time.Microsecond)
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
