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

// The default is pgx since the tier parity suite and the dual-run test
// passed (pgx-hot-leg-plan.md step 6, #857); NewConfig starts from it too.
// The field's zero value still selects attach.
func TestHotLegTransportDefaultsToPgx(t *testing.T) {
	for name, cfg := range map[string]*Config{"DefaultConfig": DefaultConfig(nil), "NewConfig": NewConfig()} {
		if cfg.DuckDB.HotLegTransport != HotLegTransportPgx {
			t.Fatalf("%s's transport is %q, want %q", name, cfg.DuckDB.HotLegTransport, HotLegTransportPgx)
		}
	}
	if (DuckDBConfig{}).HotLegTransport.IsPgx() {
		t.Fatalf("the zero value selects pgx, want attach")
	}
	var decoded DuckDBConfig
	if err := json.Unmarshal([]byte(`{"hotLegTransport":"pgx","hotLegMaxDirtyRows":5}`), &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !decoded.HotLegTransport.IsPgx() || decoded.HotLegMaxDirtyRows != 5 {
		t.Fatalf("decoded %+v, want pgx with a cap of 5", decoded)
	}
}
