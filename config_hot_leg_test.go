package forma

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestValidateHotLeg(t *testing.T) {
	cases := []struct {
		name    string
		cfg     DuckDBConfig
		wantErr string
	}{
		{name: "default transport", cfg: DuckDBConfig{}},
		{name: "pgx with a cap", cfg: DuckDBConfig{HotLegTransport: HotLegTransportPgx, HotLegMaxDirtyRows: 10000}},
		{name: "retired attach", cfg: DuckDBConfig{HotLegTransport: "attach"}, wantErr: "duckdb.hotLegTransport"},
		{name: "unknown transport", cfg: DuckDBConfig{HotLegTransport: "PGX"}, wantErr: "duckdb.hotLegTransport"},
		{name: "negative cap", cfg: DuckDBConfig{HotLegMaxDirtyRows: -1}, wantErr: "duckdb.hotLegMaxDirtyRows"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.ValidateHotLeg()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("ValidateHotLeg() = %v, want nil", err)
				}
				return
			}
			var cfgErr *ConfigError
			if !errors.As(err, &cfgErr) || cfgErr.Field != tc.wantErr {
				t.Fatalf("ValidateHotLeg() = %v, want a ConfigError on %s", err, tc.wantErr)
			}
		})
	}
}

// A deployment that rolled back to attach must fail at startup, not move to
// pgx silently, and the error must say the rollback is gone (#858). Config
// validation runs the hot-leg rules, so the value fails startup whether it
// came from a config file, an embedder or DUCKDB_HOT_LEG_TRANSPORT.
func TestConfigValidateRefusesTheRetiredAttachTransport(t *testing.T) {
	cfg := DefaultConfig(nil)
	if err := json.Unmarshal([]byte(`{"hotLegTransport":"attach"}`), &cfg.DuckDB); err != nil {
		t.Fatalf("decode: %v", err)
	}
	var cfgErr *ConfigError
	err := cfg.Validate()
	if !errors.As(err, &cfgErr) || cfgErr.Field != "duckdb.hotLegTransport" {
		t.Fatalf("Validate() = %v, want a ConfigError on duckdb.hotLegTransport", err)
	}
	if !strings.Contains(cfgErr.Message, "retired") {
		t.Fatalf("message %q does not say the attach transport was retired", cfgErr.Message)
	}
}

// DefaultConfig and NewConfig name pgx, and a field built field by field
// (empty) means pgx too: there is no other transport.
func TestHotLegTransportDefaultsToPgx(t *testing.T) {
	for name, cfg := range map[string]*Config{"DefaultConfig": DefaultConfig(nil), "NewConfig": NewConfig()} {
		if cfg.DuckDB.HotLegTransport != HotLegTransportPgx {
			t.Fatalf("%s's transport is %q, want %q", name, cfg.DuckDB.HotLegTransport, HotLegTransportPgx)
		}
	}
	var decoded DuckDBConfig
	if err := json.Unmarshal([]byte(`{"hotLegTransport":"pgx","hotLegMaxDirtyRows":5}`), &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.HotLegTransport != HotLegTransportPgx || decoded.HotLegMaxDirtyRows != 5 {
		t.Fatalf("decoded %+v, want pgx with a cap of 5", decoded)
	}
}
