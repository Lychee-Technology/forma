package factory

import (
	"strings"
	"testing"

	"github.com/lychee-technology/forma"
)

func TestPgxPoolSizingWarning(t *testing.T) {
	pgx := forma.DuckDBConfig{Enabled: true, HotLegTransport: forma.HotLegTransportPgx, MaxConnections: 4}
	for _, tc := range []struct {
		name     string
		poolMax  int32
		cfg      forma.DuckDBConfig
		wantWarn bool
	}{
		{name: "room beyond the passes", poolMax: 5, cfg: pgx},
		{name: "a pool the passes can fill", poolMax: 4, cfg: pgx, wantWarn: true},
		{name: "an unset DuckDB pool admits one pass", poolMax: 1, cfg: forma.DuckDBConfig{Enabled: true, HotLegTransport: forma.HotLegTransportPgx}, wantWarn: true},
		{name: "the default transport", poolMax: 1, cfg: forma.DuckDBConfig{Enabled: true, HotLegTransport: forma.DefaultConfig(nil).DuckDB.HotLegTransport}, wantWarn: true},
		{name: "attach holds no pooled connection per pass", poolMax: 1, cfg: forma.DuckDBConfig{Enabled: true, HotLegTransport: forma.HotLegTransportAttach}},
		{name: "DuckDB off", poolMax: 1, cfg: forma.DuckDBConfig{HotLegTransport: forma.HotLegTransportPgx}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			msg := pgxPoolSizingWarning(tc.poolMax, tc.cfg)
			if (msg != "") != tc.wantWarn {
				t.Fatalf("pgxPoolSizingWarning(%d, %+v) = %q, want a warning: %v", tc.poolMax, tc.cfg, msg, tc.wantWarn)
			}
			if tc.wantWarn && !strings.Contains(msg, "duckdb.maxConnections") {
				t.Fatalf("the warning %q does not name the setting to change", msg)
			}
		})
	}
}
