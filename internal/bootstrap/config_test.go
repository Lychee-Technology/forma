package bootstrap

import (
	"testing"
	"time"

	"github.com/lychee-technology/forma"
)

func TestEnvIntFallbackOnInvalidValue(t *testing.T) {
	t.Setenv("BOOTSTRAP_INT_TEST", "invalid")
	got := EnvInt("BOOTSTRAP_INT_TEST", 42)
	if got != 42 {
		t.Fatalf("expected fallback 42, got %d", got)
	}
}

func TestDatabaseConfigFromEnv(t *testing.T) {
	t.Setenv("DB_HOST", "db-host")
	t.Setenv("DB_PORT", "15432")
	t.Setenv("DB_NAME", "db-name")
	t.Setenv("DB_USER", "db-user")
	t.Setenv("DB_PASSWORD", "db-password")
	t.Setenv("DB_SSL_MODE", "require")
	t.Setenv("DB_SCHEMA", "tenant")
	t.Setenv("DB_MAX_CONNECTIONS", "20")
	t.Setenv("DB_MAX_IDLE_CONNS", "4")
	t.Setenv("DB_CONN_MAX_LIFETIME_SECONDS", "600")
	t.Setenv("DB_CONN_MAX_IDLE_TIME_SECONDS", "120")
	t.Setenv("DB_TIMEOUT_SECONDS", "15")

	cfg := DatabaseConfigFromEnv(DBDefaults{
		Host:                   "localhost",
		Port:                   5432,
		Database:               "forma",
		Username:               "postgres",
		SSLMode:                "disable",
		Schema:                 "public",
		MaxConnections:         25,
		MaxIdleConns:           5,
		ConnMaxLifetimeSeconds: 3600,
		ConnMaxIdleTimeSeconds: 300,
		TimeoutSeconds:         30,
	})

	if cfg.Host != "db-host" {
		t.Fatalf("expected host db-host, got %s", cfg.Host)
	}
	if cfg.Port != 15432 {
		t.Fatalf("expected port 15432, got %d", cfg.Port)
	}
	if cfg.Database != "db-name" {
		t.Fatalf("expected db-name, got %s", cfg.Database)
	}
	if cfg.Username != "db-user" {
		t.Fatalf("expected db-user, got %s", cfg.Username)
	}
	if cfg.Password != "db-password" {
		t.Fatalf("expected db-password, got %s", cfg.Password)
	}
	if cfg.SSLMode != "require" {
		t.Fatalf("expected sslmode require, got %s", cfg.SSLMode)
	}
	if cfg.Schema != "tenant" {
		t.Fatalf("expected schema tenant, got %s", cfg.Schema)
	}
	if cfg.MaxConnections != 20 {
		t.Fatalf("expected max connections 20, got %d", cfg.MaxConnections)
	}
	if cfg.MaxIdleConns != 4 {
		t.Fatalf("expected max idle conns 4, got %d", cfg.MaxIdleConns)
	}
	if cfg.ConnMaxLifetime != 600*time.Second {
		t.Fatalf("expected conn max lifetime 600s, got %s", cfg.ConnMaxLifetime)
	}
	if cfg.ConnMaxIdleTime != 120*time.Second {
		t.Fatalf("expected conn max idle 120s, got %s", cfg.ConnMaxIdleTime)
	}
	if cfg.Timeout != 15*time.Second {
		t.Fatalf("expected timeout 15s, got %s", cfg.Timeout)
	}
}

func TestTableNamesFromEnv(t *testing.T) {
	t.Setenv("SCHEMA_TABLE", "schema_tbl")
	t.Setenv("EAV_TABLE", "eav_tbl")
	t.Setenv("ENTITY_MAIN_TABLE", "main_tbl")
	t.Setenv("CHANGE_LOG_TABLE", "change_tbl")

	tables := TableNamesFromEnv(forma.TableNames{
		SchemaRegistry: "schema_default",
		EAVData:        "eav_default",
		EntityMain:     "main_default",
		ChangeLog:      "change_default",
	})

	if tables.SchemaRegistry != "schema_tbl" {
		t.Fatalf("expected schema table schema_tbl, got %s", tables.SchemaRegistry)
	}
	if tables.EAVData != "eav_tbl" {
		t.Fatalf("expected eav table eav_tbl, got %s", tables.EAVData)
	}
	if tables.EntityMain != "main_tbl" {
		t.Fatalf("expected entity table main_tbl, got %s", tables.EntityMain)
	}
	if tables.ChangeLog != "change_tbl" {
		t.Fatalf("expected change table change_tbl, got %s", tables.ChangeLog)
	}
}
