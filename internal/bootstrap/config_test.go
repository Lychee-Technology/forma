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

// TestEntityConfigFromEnvValidateUpdatesStrict pins that the #314 update-strictness
// flag is operator-settable. The staged rollout depends on an operator being able
// to flip it without rebuilding, so an unread env var would make the flag
// decorative.
func TestEntityConfigFromEnvValidateUpdatesStrict(t *testing.T) {
	t.Setenv("VALIDATE_UPDATES_STRICT", "true")

	cfg := EntityConfigFromEnv(forma.EntityConfig{})
	if !cfg.ValidateUpdatesStrict {
		t.Fatalf("expected VALIDATE_UPDATES_STRICT=true to enable strict updates")
	}
}

// TestEntityConfigFromEnvPreservesUnrelatedFields guards the overlay shape: it
// must return the defaults it was given with only the env-backed field replaced,
// never a zero-valued struct that silently drops caller settings.
func TestEntityConfigFromEnvPreservesUnrelatedFields(t *testing.T) {
	defaults := forma.EntityConfig{
		BatchSize:       500,
		SchemaDirectory: "/schemas",
		MaxEntitySize:   1024,
	}

	cfg := EntityConfigFromEnv(defaults)

	if cfg.ValidateUpdatesStrict {
		t.Fatalf("expected strict updates to default to false when the env var is unset")
	}
	if cfg.BatchSize != 500 {
		t.Fatalf("expected batch size 500 to be preserved, got %d", cfg.BatchSize)
	}
	if cfg.SchemaDirectory != "/schemas" {
		t.Fatalf("expected schema directory to be preserved, got %s", cfg.SchemaDirectory)
	}
	if cfg.MaxEntitySize != 1024 {
		t.Fatalf("expected max entity size 1024 to be preserved, got %d", cfg.MaxEntitySize)
	}
}
