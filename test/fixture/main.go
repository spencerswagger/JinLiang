// Command fixture creates the tiny tables the end-to-end script needs.
//
// 为什么单独一个程序而不是写在测试里：e2e 脚本要能对着一个从没被碰过的空库跑。
// 直接 `go run ./test/fixture` 比假设 CI 里装了 psql/mysql 客户端更可靠。
//
// 只建测试用的表，而且只在显式给了 DSN 时才建；不碰任何业务数据。
package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/spencerswagger/jinliang/internal/dsn"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if raw := os.Getenv("JINLIANG_TEST_PG_DSN"); raw != "" {
		if err := fixturePG(ctx, raw); err != nil {
			fmt.Fprintf(os.Stderr, "fixture(postgres): %v\n", err)
			os.Exit(1)
		}
		fmt.Println("fixture: postgres ok")
	}
	if raw := os.Getenv("JINLIANG_TEST_MYSQL_DSN"); raw != "" {
		if err := fixtureMySQL(ctx, raw); err != nil {
			fmt.Fprintf(os.Stderr, "fixture(mysql): %v\n", err)
			os.Exit(1)
		}
		fmt.Println("fixture: mysql ok")
	}
}

func fixturePG(ctx context.Context, raw string) error {
	ep, err := dsn.Parse(raw)
	if err != nil {
		return err
	}
	db, err := sql.Open("pgx", ep.PostgresURL())
	if err != nil {
		return err
	}
	defer db.Close()
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS e2e_customers (
			id          bigserial PRIMARY KEY,
			name        text NOT NULL,
			city        text,
			updated_at  timestamptz NOT NULL DEFAULT now()
		)`,
		`CREATE TABLE IF NOT EXISTS e2e_logs (
			message text
		)`,
		// B1 引擎是 PG→PG：e2e 用这条 schema 当目标端，避免与源端 public 撞表。
		`CREATE SCHEMA IF NOT EXISTS jl_e2e_out`,
		`INSERT INTO e2e_customers (name, city)
		 SELECT 'demo-'||g, 'city-'||(g%5)
		 FROM generate_series(1,3) g
		 WHERE NOT EXISTS (SELECT 1 FROM e2e_customers)`,
	}
	for _, s := range stmts {
		if _, err := db.ExecContext(ctx, s); err != nil {
			return fmt.Errorf("%s: %w", firstLine(s), err)
		}
	}
	return nil
}

func fixtureMySQL(ctx context.Context, raw string) error {
	ep, err := dsn.Parse(raw)
	if err != nil {
		return err
	}
	db, err := sql.Open("mysql", ep.MySQLConfig())
	if err != nil {
		return err
	}
	defer db.Close()
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS e2e_customers (
			id          BIGINT AUTO_INCREMENT PRIMARY KEY,
			name        VARCHAR(100) NOT NULL,
			city        VARCHAR(100),
			updated_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
		`CREATE TABLE IF NOT EXISTS e2e_logs (
			message TEXT
		)`,
	}
	for _, s := range stmts {
		if _, err := db.ExecContext(ctx, s); err != nil {
			return fmt.Errorf("%s: %w", firstLine(s), err)
		}
	}
	return nil
}

func firstLine(s string) string {
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			return s[:i]
		}
	}
	if len(s) > 60 {
		return s[:60]
	}
	return s
}
