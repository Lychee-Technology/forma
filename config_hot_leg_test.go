package forma

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestValidateHotLeg(t *testing.T) {
	cases := []struct {
		name    string
		cfg     DuckDBConfig
		wantErr string
	}{
		{name: "default transport", cfg: DuckDBConfig{}},
		{name: "attach", cfg: DuckDBConfig{HotLegTransport: HotLegTransportAttach}},
		{name: "pgx with a cap", cfg: DuckDBConfig{HotLegTransport: HotLegTransportPgx, HotLegMaxDirtyRows: 10000}},
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

// Config validation runs the hot-leg rules, so a bad transport fails startup.
func TestConfigValidateRejectsAnUnknownHotLegTransport(t *testing.T) {
	cfg := DefaultConfig(nil)
	cfg.DuckDB.HotLegTransport = "duckdb"
	var cfgErr *ConfigError
	if err := cfg.Validate(); !errors.As(err, &cfgErr) || cfgErr.Field != "duckdb.hotLegTransport" {
		t.Fatalf("Validate() = %v, want a ConfigError on duckdb.hotLegTransport", err)
	}
}

// The default stays attach until the tier parity suite passes
// (pgx-hot-leg-plan.md step 6).
func TestHotLegTransportDefaultsToAttach(t *testing.T) {
	cfg := DefaultConfig(nil)
	if cfg.DuckDB.HotLegTransport.IsPgx() {
		t.Fatalf("default transport %q selects pgx", cfg.DuckDB.HotLegTransport)
	}
	var decoded DuckDBConfig
	if err := json.Unmarshal([]byte(`{"hotLegTransport":"pgx","hotLegMaxDirtyRows":5}`), &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !decoded.HotLegTransport.IsPgx() || decoded.HotLegMaxDirtyRows != 5 {
		t.Fatalf("decoded %+v, want pgx with a cap of 5", decoded)
	}
}
