package dsn

import (
	"strings"
	"testing"
)

func TestParsePostgresURL(t *testing.T) {
	ep, err := Parse("postgres://alice:s3cret@db.internal:6432/orders?sslmode=require&application_name=jl")
	if err != nil {
		t.Fatal(err)
	}
	if ep.Kind != KindPostgres || ep.Host != "db.internal" || ep.Port != 6432 {
		t.Fatalf("endpoint = %+v", ep)
	}
	if ep.Database != "orders" || ep.Username != "alice" || ep.Password != "s3cret" {
		t.Fatalf("endpoint = %+v", ep)
	}
	if ep.Params["application_name"] != "jl" {
		t.Fatalf("params = %v", ep.Params)
	}
	if ep.SSLMode() != "require" {
		t.Fatalf("sslmode = %q", ep.SSLMode())
	}
}

func TestParseDefaults(t *testing.T) {
	ep, err := Parse("postgres://u:p@h/db")
	if err != nil {
		t.Fatal(err)
	}
	if ep.Port != 5432 {
		t.Fatalf("default pg port = %d", ep.Port)
	}
	// SECURITY §3：默认 verify-full，不是 prefer
	if ep.SSLMode() != DefaultPGSSLMode {
		t.Fatalf("default sslmode = %q, want %q", ep.SSLMode(), DefaultPGSSLMode)
	}
	if weak, _ := ep.WeakTLS(); weak {
		t.Fatal("verify-full must not be flagged as weak TLS")
	}

	my, err := Parse("mysql://u:p@h/db")
	if err != nil {
		t.Fatal(err)
	}
	if my.Port != 3306 {
		t.Fatalf("default mysql port = %d", my.Port)
	}
}

func TestWeakTLSDetection(t *testing.T) {
	for _, mode := range []string{"disable", "prefer", "require", "allow"} {
		ep := Endpoint{Kind: KindPostgres, Params: map[string]string{"sslmode": mode}}
		weak, why := ep.WeakTLS()
		if !weak || why == "" {
			t.Errorf("%s should be flagged as weaker than verify-full", mode)
		}
	}
	my := Endpoint{Kind: KindMySQL, Params: map[string]string{}}
	if weak, why := my.WeakTLS(); !weak || why == "" {
		t.Error("mysql without tls must be flagged")
	}
	myOK := Endpoint{Kind: KindMySQL, Params: map[string]string{"tls": "true"}}
	if weak, _ := myOK.WeakTLS(); weak {
		t.Error("mysql with tls=true should not be flagged")
	}
}

func TestMySQLNativeForm(t *testing.T) {
	ep, err := Parse("bob:pw@tcp(127.0.0.1:3307)/report?charset=utf8mb4&parseTime=true")
	if err != nil {
		t.Fatal(err)
	}
	if ep.Kind != KindMySQL || ep.Host != "127.0.0.1" || ep.Port != 3307 || ep.Database != "report" {
		t.Fatalf("endpoint = %+v", ep)
	}
	if ep.Username != "bob" || ep.Password != "pw" {
		t.Fatalf("creds = %+v", ep)
	}
}

func TestRedactedNeverShowsPassword(t *testing.T) {
	ep, err := Parse("postgres://alice:s3cret@db.internal:5432/orders")
	if err != nil {
		t.Fatal(err)
	}
	if got := ep.Redacted(); strings.Contains(got, "s3cret") {
		t.Fatalf("Redacted leaked the password: %s", got)
	}
	if !strings.Contains(ep.Redacted(), "alice@db.internal:5432/orders") {
		t.Fatalf("Redacted lost useful context: %s", ep.Redacted())
	}
}

func TestParseRejectsGarbage(t *testing.T) {
	for _, raw := range []string{"", "   ", "http://example.com", "just-a-string"} {
		if _, err := Parse(raw); err == nil {
			t.Errorf("%q should not parse", raw)
		}
	}
	// 错误信息里也不能回显密码
	_, err := Parse("postgres://u:s3cret@h:notaport/db")
	if err == nil {
		t.Fatal("bad port should fail")
	}
	if strings.Contains(err.Error(), "s3cret") {
		t.Fatalf("error leaked the password: %v", err)
	}
}

func TestBuildURLsRoundTrip(t *testing.T) {
	ep := Endpoint{
		Kind: KindPostgres, Host: "db", Port: 5432, Database: "app",
		Username: "u", Password: "p@ss:word", Params: map[string]string{"sslmode": "verify-full"},
	}
	back, err := Parse(ep.PostgresURL())
	if err != nil {
		t.Fatalf("re-parse %q: %v", ep.PostgresURL(), err)
	}
	if back.Password != ep.Password {
		t.Fatalf("password with special chars did not round-trip: %q", back.Password)
	}
	my := Endpoint{Kind: KindMySQL, Host: "db", Port: 3306, Database: "app", Username: "u", Password: "p"}
	backMy, err := Parse(my.MySQLConfig())
	if err != nil {
		t.Fatalf("re-parse mysql %q: %v", my.MySQLConfig(), err)
	}
	if backMy.Database != "app" || backMy.Password != "p" {
		t.Fatalf("mysql round-trip = %+v", backMy)
	}
}
