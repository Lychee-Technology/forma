package bootstrap

import (
	"errors"
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

// TestHTTPServerConfigValidate pins the boot-time rules on the HTTP_* overlay
// (#465 review): a negative phase would disable that timeout in net/http, and
// a bounded WriteTimeout shorter than a budget would cut a legitimately slow
// request's connection instead of letting the 504 out.
func TestHTTPServerConfigValidate(t *testing.T) {
	budgets := forma.DefaultConfig(nil)
	if err := DefaultHTTPServerConfig().Validate(budgets); err != nil {
		t.Fatalf("defaults must validate against the default budgets: %v", err)
	}

	negative := map[string]func(*HTTPServerConfig){
		"http.readHeaderTimeout": func(c *HTTPServerConfig) { c.ReadHeaderTimeout = -time.Second },
		"http.readTimeout":       func(c *HTTPServerConfig) { c.ReadTimeout = -time.Second },
		"http.writeTimeout":      func(c *HTTPServerConfig) { c.WriteTimeout = -time.Second },
		"http.idleTimeout":       func(c *HTTPServerConfig) { c.IdleTimeout = -time.Second },
		"http.maxHeaderBytes":    func(c *HTTPServerConfig) { c.MaxHeaderBytes = -1 },
	}
	for field, mutate := range negative {
		cfg := DefaultHTTPServerConfig()
		mutate(&cfg)
		assertConfigError(t, cfg.Validate(budgets), field)
	}

	// Zero is "unbounded"/"default" everywhere, never an error.
	if err := (HTTPServerConfig{}).Validate(budgets); err != nil {
		t.Fatalf("an all-zero config must validate: %v", err)
	}

	// A bounded write timeout must cover both budgets; an unbounded write
	// timeout or an unbounded budget needs no check.
	short := DefaultHTTPServerConfig()
	short.WriteTimeout = budgets.Query.DefaultTimeout - time.Second
	assertConfigError(t, short.Validate(budgets), "http.writeTimeout")

	txOnly := forma.DefaultConfig(nil)
	txOnly.Query.DefaultTimeout = 0
	assertConfigError(t, short.Validate(txOnly), "http.writeTimeout")

	unboundedBudgets := forma.DefaultConfig(nil)
	unboundedBudgets.Query.DefaultTimeout = 0
	unboundedBudgets.Transaction.DefaultTimeout = 0
	if err := short.Validate(unboundedBudgets); err != nil {
		t.Fatalf("unbounded budgets need no write-timeout cover: %v", err)
	}
	short.WriteTimeout = 0
	if err := short.Validate(budgets); err != nil {
		t.Fatalf("an unbounded write timeout covers every budget: %v", err)
	}
	if err := short.Validate(nil); err != nil {
		t.Fatalf("nil budgets skip the cross-check: %v", err)
	}
}

// TestNegativeLimitOverlayFailsValidation is the end-to-end shape of the
// boot-time contract: ApplyLimitsFromEnv only parses, and the value it lets
// through is refused by forma.Config.Validate, which both entry points run
// before opening the database.
func TestNegativeLimitOverlayFailsValidation(t *testing.T) {
	cases := map[string]string{
		"MAX_ENTITY_SIZE_BYTES":        "entity.maxEntitySize",
		"MAX_BATCH_SIZE":               "performance.maxBatchSize",
		"QUERY_TIMEOUT_SECONDS":        "query.defaultTimeout",
		"TRANSACTION_TIMEOUT_SECONDS":  "transaction.defaultTimeout",
		"DUCKDB_QUERY_TIMEOUT_SECONDS": "duckdb.queryTimeout",
	}
	for key, field := range cases {
		t.Run(key, func(t *testing.T) {
			t.Setenv(key, "-1")
			cfg := forma.DefaultConfig(nil)
			ApplyLimitsFromEnv(cfg)
			assertConfigError(t, cfg.Validate(), field)
		})
	}

	// Zero budgets are the documented "disabled" value and must pass.
	for _, key := range []string{"QUERY_TIMEOUT_SECONDS", "TRANSACTION_TIMEOUT_SECONDS", "DUCKDB_QUERY_TIMEOUT_SECONDS"} {
		t.Setenv(key, "0")
	}
	cfg := forma.DefaultConfig(nil)
	ApplyLimitsFromEnv(cfg)
	if err := cfg.Validate(); err != nil {
		t.Fatalf("zero budgets must validate: %v", err)
	}
}

func assertConfigError(t *testing.T, err error, field string) {
	t.Helper()
	var cfgErr *forma.ConfigError
	if !errors.As(err, &cfgErr) {
		t.Fatalf("expected a ConfigError on %s, got %v", field, err)
	}
	if cfgErr.Field != field {
		t.Fatalf("expected field %s, got %s (%v)", field, cfgErr.Field, err)
	}
}
