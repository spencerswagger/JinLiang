package control

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/spencerswagger/jinliang/internal/connect"
	"github.com/spencerswagger/jinliang/internal/dsn"
	"github.com/spencerswagger/jinliang/internal/preflight"
	"github.com/spencerswagger/jinliang/internal/source"
)

// 控制面替用户做的只读探测：测连、枚举库表、预检。
// 这三件事都只发 SELECT/SHOW，不改对端任何东西 —— 所以它们可以在用户"还没
// 决定要不要同步"的时候随时跑。

// Endpoint decrypts a connection into coordinates ready for a driver. The
// returned struct holds a plaintext password in memory: don't log it, don't
// serialise it (use Endpoint.Redacted for messages).
func (s *Store) Endpoint(ctx context.Context, connID string) (dsn.Endpoint, error) {
	c, err := s.GetConnection(ctx, connID)
	if err != nil {
		return dsn.Endpoint{}, err
	}
	secret, err := s.Secret(ctx, connID)
	if err != nil {
		return dsn.Endpoint{}, err
	}
	return dsn.Endpoint{
		Kind:     c.Kind,
		Host:     c.Host,
		Port:     c.Port,
		Database: c.Database,
		Username: c.Username,
		Password: secret,
		Params:   c.Params(),
	}, nil
}

// TestConnectionResult is what the 「测连」 button returns and what we persist
// as last_test_*.
type TestConnectionResult struct {
	ConnectionID  string          `json:"connection_id"`
	OK            bool            `json:"ok"`
	Step          string          `json:"step,omitempty"`
	Message       string          `json:"message,omitempty"`
	ServerVersion string          `json:"server_version,omitempty"`
	LatencyMS     int64           `json:"latency_ms"`
	TLS           bool            `json:"tls"`
	TLSWarning    string          `json:"tls_warning,omitempty"`
	Code          string          `json:"code,omitempty"`
	Capabilities  map[string]bool `json:"capabilities,omitempty"`
	Notes         []string        `json:"notes,omitempty"`
	TestedAt      string          `json:"tested_at"`
}

// TestConnection really connects and reads the server version. No simulation:
// if the red dot shows, the database really is unreachable.
func (s *Store) TestConnection(ctx context.Context, connID string) (TestConnectionResult, error) {
	ep, err := s.Endpoint(ctx, connID)
	if err != nil {
		return TestConnectionResult{}, err
	}
	probe := connect.Test(ctx, ep, 8*time.Second)
	res := TestConnectionResult{
		ConnectionID:  connID,
		OK:            probe.OK,
		Step:          probe.Step,
		Message:       probe.Message,
		ServerVersion: probe.ServerVersion,
		LatencyMS:     probe.LatencyMS,
		TLS:           probe.TLS,
		TLSWarning:    probe.TLSWarning,
		Capabilities:  probe.Capabilities,
		Notes:         probe.CapabilityNotes,
		TestedAt:      s.nowText(),
	}
	if !res.OK {
		res.Code = "E1008"
	}
	msg := fmt.Sprintf("ok=%v v=%s %dms", res.OK, res.ServerVersion, res.LatencyMS)
	if !res.OK {
		msg = res.Message
	}
	if err := s.RecordTest(ctx, connID, res.OK, msg); err != nil {
		return res, err
	}
	_ = s.Audit(ctx, ActorAPI, ActionConnTest, map[string]any{
		"connection_id": connID, "ok": res.OK, "version": res.ServerVersion,
	})
	return res, nil
}

// CatalogResult is the picker payload: schemas to choose from + one schema's tables.
type CatalogResult struct {
	ConnectionID string            `json:"connection_id"`
	Kind         string            `json:"kind"`
	Schemas      []string          `json:"schemas"`
	Schema       string            `json:"schema"`
	Tables       []source.Table    `json:"tables"`
	Views        []source.TableRef `json:"views"`
	Warning      string            `json:"warning,omitempty"`
	ElapsedMS    int64             `json:"elapsed_ms"`
}

// Catalog enumerates tables for the wizard. schema "" means "default" (public
// for postgres, the connection's database for mysql).
func (s *Store) Catalog(ctx context.Context, connID, schema string) (CatalogResult, error) {
	ep, err := s.Endpoint(ctx, connID)
	if err != nil {
		return CatalogResult{}, err
	}
	start := time.Now()
	conn, err := connect.Open(ep)
	if err != nil {
		return CatalogResult{}, err
	}
	defer conn.Close()

	out := CatalogResult{ConnectionID: connID, Kind: ep.Kind, Schema: schema}
	if lister, ok := conn.(interface {
		Schemas(context.Context) ([]string, error)
	}); ok {
		if schemas, err := lister.Schemas(ctx); err == nil {
			out.Schemas = schemas
		} else {
			out.Warning = "列不出 schema 列表：" + connect.HumanError(err)
		}
	}
	if out.Schema == "" {
		out.Schema = defaultCatalogSchema(ep, out.Schemas)
	}
	cat, err := conn.Catalog(ctx, source.Scope{Schema: out.Schema})
	if err != nil {
		return out, fmt.Errorf("枚举库表失败: %s", connect.HumanError(err))
	}
	out.Tables, out.Views = cat.Tables, cat.Views
	out.ElapsedMS = time.Since(start).Milliseconds()
	return out, nil
}

// defaultCatalogSchema 决定「没显式选 schema 时列哪个 schema 的表」。
//
// MySQL 的 schema 就是库名：必须用 DSN 里那个库，不能拿账号能看见的第一个库——
// 那个常常是别的库（比如 jinliang_src），界面就会列出错的表。
// PostgreSQL 里 database 与 schema 是两回事，仍取搜索路径的第一个 schema。
func defaultCatalogSchema(ep dsn.Endpoint, schemas []string) string {
	if ep.Kind == dsn.KindMySQL && ep.Database != "" {
		return ep.Database
	}
	if len(schemas) > 0 {
		return schemas[0]
	}
	return "public"
}

// PreflightJob assembles the checks for one job and returns the four-state
// report. It is read-only: it never creates tables or objects.
func (s *Store) PreflightJob(ctx context.Context, jobID string) (source.Report, error) {
	job, err := s.GetJob(ctx, jobID)
	if err != nil {
		return source.Report{}, err
	}
	doc, err := s.buildDoc(ctx, job)
	if err != nil {
		return source.Report{}, err
	}
	srcC, err := s.GetConnection(ctx, job.SourceID)
	if err != nil {
		return source.Report{}, err
	}
	tgtC, err := s.GetConnection(ctx, job.TargetID)
	if err != nil {
		return source.Report{}, err
	}
	in := preflight.Input{
		JobName:    job.Name,
		SourceKind: srcC.Kind, TargetKind: tgtC.Kind,
		Mode:        doc.Mode.Frequency,
		Incremental: doc.Mode.IncrementalColumn,
		OnExists:    doc.Target.OnExists,
		BatchRows:   batchRowsOf(doc),
		Tables:      selectedTables(doc),
		Lookback:    doc.Mode.Lookback,
	}
	if doc.Source.Snapshot != nil && doc.Source.Snapshot.Conns != nil {
		in.SnapshotConn = *doc.Source.Snapshot.Conns
	}

	// 源端：独立超时，失败也继续把报告出完（一项 blocked 比一片空白有用）
	srcEP, err := s.Endpoint(ctx, job.SourceID)
	if err != nil {
		return source.Report{}, err
	}
	tgtEP, err := s.Endpoint(ctx, job.TargetID)
	if err != nil {
		return source.Report{}, err
	}
	if weak, why := srcEP.WeakTLS(); weak {
		in.TLSWarnings = append(in.TLSWarnings, "源库："+why)
	}
	if weak, why := tgtEP.WeakTLS(); weak {
		in.TLSWarnings = append(in.TLSWarnings, "目标库："+why)
	}

	sctx, cancel := context.WithTimeout(ctx, 12*time.Second)
	defer cancel()
	if conn, err := connect.Open(srcEP); err != nil {
		in.SourceErr = connect.HumanError(err)
	} else {
		defer conn.Close()
		if v, err := conn.Version(sctx); err != nil {
			in.SourceErr = connect.HumanError(err)
		} else {
			in.SourceVer = v
			if caps, err := conn.Probe(sctx); err == nil {
				in.SourceCaps = caps
			}
			if cat, err := conn.Catalog(sctx, source.Scope{Schema: doc.SchemaOrDefault()}); err == nil {
				in.SourceCat = &cat
			}
		}
	}
	if conn, err := connect.Open(tgtEP); err != nil {
		in.TargetErr = connect.HumanError(err)
	} else {
		defer conn.Close()
		if v, err := conn.Version(sctx); err != nil {
			in.TargetErr = connect.HumanError(err)
		} else {
			in.TargetVer = v
			if cat, err := conn.Catalog(sctx, source.Scope{Schema: doc.Target.Schema}); err == nil {
				in.TargetCat = &cat
			}
		}
	}
	rep := preflight.Run(in)
	_ = s.Audit(ctx, ActorAPI, "job.preflight", map[string]any{
		"job_id": jobID, "can_start": rep.CanStart, "checks": len(rep.Checks),
	})
	return rep, nil
}

// SchemaOrDefault is the source schema to enumerate.
func (d *JobDoc) SchemaOrDefault() string {
	if d.Source.Scope != nil && strings.TrimSpace(d.Source.Scope.Schema) != "" {
		return d.Source.Scope.Schema
	}
	if d.Source.Kind == "mysql" {
		return "" // MySQL 用连接上的库名
	}
	return "public"
}

func selectedTables(doc JobDoc) []string {
	if doc.Content != nil && len(doc.Content.Tables) > 0 {
		out := make([]string, 0, len(doc.Content.Tables))
		for name := range doc.Content.Tables {
			out = append(out, name)
		}
		sortStrings(out)
		return out
	}
	if doc.Source.Scope != nil {
		return append([]string{}, doc.Source.Scope.Include...)
	}
	return nil
}

func batchRowsOf(doc JobDoc) int {
	if doc.Reliability != nil && doc.Reliability.BatchRows != nil {
		return *doc.Reliability.BatchRows
	}
	return DefaultBatchRows
}
