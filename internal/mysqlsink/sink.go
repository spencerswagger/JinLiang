package mysqlsink

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/spencerswagger/jinliang/internal/catalog"
	"github.com/spencerswagger/jinliang/internal/dsn"
	"github.com/spencerswagger/jinliang/internal/sink"
	"github.com/spencerswagger/jinliang/internal/source"
)

// OnExists 三策略（与 PG 侧同名同义，docs/SPEC.md `target.on_exists`）。
type OnExists string

// on_exists 取值。
const (
	OnExistsError    OnExists = "error"    // 默认：撞表就停（E1002）
	OnExistsRecreate OnExists = "recreate" // 先 DROP 再建（会清空目标表）
	OnExistsAppend   OnExists = "append"   // 用已有表；源端删过的行会残留（语义选择）
)

// Options 是打开目标端需要的全部配置。
type Options struct {
	DSN        dsn.Endpoint
	JobKey     string // 状态表按 job_key 分区
	Name       string // 用户起的名字，仅展示
	InstanceID string // 本次运行的实例 id（审计/孤儿判定）
	SourceKind catalog.Engine
	OnExists   OnExists

	// AutoConfirmDDL 允许自动执行「需确认」类 DDL（删列、加唯一索引）。
	// 默认 false：这类改动要人点头。
	AutoConfirmDDL bool

	// Width 决定 utf8mb4 行宽预检用哪套上限；零值用 DefaultWidthOptions()。
	Width WidthOptions
}

// ErrTargetTableExists 是 `on_exists=error` 撞到已有表。
var ErrTargetTableExists = errors.New("目标端已有同名表")

// ErrVerifyUnsupported 说明 sink 单独做不了行级校验（需要源端配合）。
var ErrVerifyUnsupported = errors.New("sink 单独无法做行级校验：需要与源端逐块对拍（verify 命令负责）")

// Sink 是 MySQL 目标端的写入实现。
type Sink struct {
	db  *sql.DB
	ep  dsn.Endpoint
	opt Options

	width       WidthOptions
	aliasUpsert bool // 8.0.19+ 支持 INSERT ... AS new ON DUPLICATE KEY UPDATE new.c=…
	uniqueCache map[string]bool
}

var _ sink.Sink = (*Sink)(nil)

// Open 连接目标库并探测方言能力（版本、行格式、索引长度上限）。
func Open(ctx context.Context, opt Options) (*Sink, error) {
	if opt.DSN.Kind != "" && opt.DSN.Kind != dsn.KindMySQL {
		return nil, fmt.Errorf("目标端类型是 %q，这个 package 只负责 MySQL", opt.DSN.Kind)
	}
	if opt.OnExists == "" {
		opt.OnExists = OnExistsError
	}
	db, err := sql.Open("mysql", driverDSN(opt.DSN))
	if err != nil {
		return nil, fmt.Errorf("初始化目标库连接失败: %w", err)
	}
	db.SetMaxOpenConns(8)
	db.SetConnMaxIdleTime(5 * time.Minute)
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("连不上目标库 %s:%d: %w", opt.DSN.Host, opt.DSN.Port, err)
	}
	s := &Sink{db: db, ep: opt.DSN, opt: opt, uniqueCache: map[string]bool{}}
	if opt.Width == (WidthOptions{}) {
		s.width = s.probeWidthOptions(ctx)
	} else {
		s.width = opt.Width
	}
	s.aliasUpsert = s.serverAtLeast(ctx, 8, 0, 19)
	return s, nil
}

// Close 释放连接池。
func (s *Sink) Close() error { return s.db.Close() }

// Kind 实现 sink.Sink。
func (s *Sink) Kind() string { return dsn.KindMySQL }

func driverDSN(ep dsn.Endpoint) string {
	params := []string{"parseTime=true", "loc=UTC", "charset=utf8mb4", "interpolateParams=false", "multiStatements=false"}
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?%s",
		ep.Username, ep.Password, ep.Host, ep.Port, ep.Database, strings.Join(params, "&"))
}

func (s *Sink) serverAtLeast(ctx context.Context, maj, min, patch int) bool {
	var v string
	if err := s.db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&v); err != nil {
		return false
	}
	parts := strings.FieldsFunc(v, func(r rune) bool { return r == '.' || r == '-' || r == '+' })
	if len(parts) < 3 {
		return false
	}
	a, _ := strconv.Atoi(parts[0])
	b, _ := strconv.Atoi(parts[1])
	c, _ := strconv.Atoi(strings.TrimFunc(parts[2], func(r rune) bool { return r < '0' || r > '9' }))
	if a != maj {
		return a > maj
	}
	if b != min {
		return b > min
	}
	return c >= patch
}

// schemaOf 把「空 schema」归一到连接的默认库：catalog 里的表常常只带表名。
func (s *Sink) schemaOf(t source.TableRef) string {
	if t.Schema != "" {
		return t.Schema
	}
	return s.ep.Database
}

// Cleanup 实现 sink.Sink：MySQL 目标端没有需要回收的服务端对象（临时表用完即删）。
func (s *Sink) Cleanup(ctx context.Context) error { return nil }

// Verify 实现 sink.Sink。
//
// 目标端单独能给出的只有「这张表有多少行」，给不出「和源端差几行」——
// 那需要源端一起对拍（verify 命令做）。这里明确返回不支持，而不是返回一个
// 看起来通过的 0（静默假通过比报错危险得多）。
func (s *Sink) Verify(ctx context.Context, table source.TableRef) (int64, error) {
	var n int64
	err := s.db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", qualified(table))).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("读目标表 %s 行数失败: %w", qualified(table), err)
	}
	return 0, fmt.Errorf("%w（%s 现有 %d 行）", ErrVerifyUnsupported, qualified(table), n)
}
