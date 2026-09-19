package bootstrap

import (
	"net/http"
	"testing"
	"time"

	"github.com/lychee-technology/forma"
)

func TestHTTPServerConfigDefaultsBoundEveryPhase(t *testing.T) {
	cfg := DefaultHTTPServerConfig()
	if cfg.ReadHeaderTimeout <= 0 || cfg.ReadTimeout <= 0 || cfg.WriteTimeout <= 0 || cfg.IdleTimeout <= 0 || cfg.MaxHeaderBytes <= 0 {
		t.Fatalf("a default phase is unbounded: %+v", cfg)
	}
	// The write timeout must outlast the manager's default budgets, or a
	// request that legitimately spends its 30s budget loses its response.
	def := forma.DefaultConfig(nil)
	if cfg.WriteTimeout <= def.Query.DefaultTimeout || cfg.WriteTimeout <= def.Transaction.DefaultTimeout {
		t.Fatalf("WriteTimeout %s must exceed the 30s query/transaction budgets", cfg.WriteTimeout)
	}

	srv := NewHTTPServer(":0", http.NotFoundHandler(), cfg)
	if srv.ReadHeaderTimeout != cfg.ReadHeaderTimeout || srv.ReadTimeout != cfg.ReadTimeout ||
		srv.WriteTimeout != cfg.WriteTimeout || srv.IdleTimeout != cfg.IdleTimeout ||
		srv.MaxHeaderBytes != cfg.MaxHeaderBytes || srv.Addr != ":0" {
		t.Fatalf("NewHTTPServer dropped a bound: %+v", srv)
	}
}

func TestHTTPServerConfigFromEnv(t *testing.T) {
	t.Setenv("HTTP_READ_HEADER_TIMEOUT_SECONDS", "3")
	t.Setenv("HTTP_READ_TIMEOUT_SECONDS", "7")
	t.Setenv("HTTP_WRITE_TIMEOUT_SECONDS", "90")
	t.Setenv("HTTP_IDLE_TIMEOUT_SECONDS", "0")
	t.Setenv("HTTP_MAX_HEADER_BYTES", "4096")

	cfg := HTTPServerConfigFromEnv(DefaultHTTPServerConfig())
	if cfg.ReadHeaderTimeout != 3*time.Second || cfg.ReadTimeout != 7*time.Second ||
		cfg.WriteTimeout != 90*time.Second || cfg.IdleTimeout != 0 || cfg.MaxHeaderBytes != 4096 {
		t.Fatalf("env overlay not applied: %+v", cfg)
	}

	t.Setenv("HTTP_READ_TIMEOUT_SECONDS", "not-a-number")
	cfg = HTTPServerConfigFromEnv(DefaultHTTPServerConfig())
	if cfg.ReadTimeout != DefaultHTTPServerConfig().ReadTimeout {
		t.Fatalf("an unparsable value must keep the default, got %s", cfg.ReadTimeout)
	}
}

func TestApplyLimitsFromEnv(t *testing.T) {
	t.Setenv("MAX_ENTITY_SIZE_BYTES", "2048")
	t.Setenv("MAX_BATCH_SIZE", "250")
	t.Setenv("QUERY_TIMEOUT_SECONDS", "5")
	t.Setenv("TRANSACTION_TIMEOUT_SECONDS", "0")
	t.Setenv("DUCKDB_QUERY_TIMEOUT_SECONDS", "12")

	cfg := forma.DefaultConfig(nil)
	ApplyLimitsFromEnv(cfg)
	if cfg.Entity.MaxEntitySize != 2048 || cfg.Performance.MaxBatchSize != 250 ||
		cfg.Query.DefaultTimeout != 5*time.Second || cfg.Transaction.DefaultTimeout != 0 ||
		cfg.DuckDB.QueryTimeout != 12*time.Second {
		t.Fatalf("limits overlay not applied: entity=%d batch=%d query=%s tx=%s duckdb=%s",
			cfg.Entity.MaxEntitySize, cfg.Performance.MaxBatchSize,
			cfg.Query.DefaultTimeout, cfg.Transaction.DefaultTimeout, cfg.DuckDB.QueryTimeout)
	}

	// Unset leaves the defaults, and a nil config is a no-op.
	for _, key := range []string{"MAX_ENTITY_SIZE_BYTES", "MAX_BATCH_SIZE", "QUERY_TIMEOUT_SECONDS",
		"TRANSACTION_TIMEOUT_SECONDS", "DUCKDB_QUERY_TIMEOUT_SECONDS"} {
		t.Setenv(key, "")
	}
	cfg = forma.DefaultConfig(nil)
	want := *forma.DefaultConfig(nil)
	ApplyLimitsFromEnv(cfg)
	if cfg.Entity.MaxEntitySize != want.Entity.MaxEntitySize || cfg.Performance.MaxBatchSize != want.Performance.MaxBatchSize ||
		cfg.Query.DefaultTimeout != want.Query.DefaultTimeout || cfg.Transaction.DefaultTimeout != want.Transaction.DefaultTimeout ||
		cfg.DuckDB.QueryTimeout != want.DuckDB.QueryTimeout {
		t.Fatalf("unset env must keep defaults")
	}
	ApplyLimitsFromEnv(nil)
}
