package bootstrap

import (
	"context"
	"errors"
	"net/url"
	"testing"
	"time"

	"github.com/lychee-technology/forma"
)

func TestNewPostgresPoolFromConfigContext_Canceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := NewPostgresPoolFromConfigContext(ctx, testDatabaseConfig())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}
}

func TestNewPostgresPoolFromConfigContext_DeadlineExceeded(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	_, err := NewPostgresPoolFromConfigContext(ctx, testDatabaseConfig())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
}

func testDatabaseConfig() forma.DatabaseConfig {
	return forma.DatabaseConfig{
		Host:           "localhost",
		Port:           5432,
		Database:       "forma",
		Username:       "postgres",
		Password:       "postgres",
		SSLMode:        "disable",
		MaxConnections: 4,
		Timeout:        3 * time.Second,
	}
}

func TestBuildDSN_SpecialCharsInPassword(t *testing.T) {
	cfg := forma.DatabaseConfig{
		Host:     "db.example.com",
		Port:     5432,
		Database: "mydb",
		Username: "user@domain",
		Password: "p@ss:w/ord",
		SSLMode:  "require",
	}
	dsn := buildDSN(cfg)

	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("buildDSN produced unparseable URL: %v\ndsn=%s", err, dsn)
	}
	if u.Hostname() != "db.example.com" {
		t.Errorf("hostname: want db.example.com, got %s", u.Hostname())
	}
	if u.Port() != "5432" {
		t.Errorf("port: want 5432, got %s", u.Port())
	}
	pass, _ := u.User.Password()
	if pass != "p@ss:w/ord" {
		t.Errorf("password not round-tripped: got %s", pass)
	}
	if u.User.Username() != "user@domain" {
		t.Errorf("username not round-tripped: got %s", u.User.Username())
	}
	if u.Path != "/mydb" {
		t.Errorf("database path: want /mydb, got %s", u.Path)
	}
}
