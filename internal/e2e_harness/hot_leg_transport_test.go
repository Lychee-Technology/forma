package e2e_harness

import (
	"testing"

	"github.com/lychee-technology/forma"
)

// Unset, the harnesses run the transport a deployment gets by default (pgx
// since #857); set, they run the one it names, and refuse any other.
func TestHotLegTransportFromEnv(t *testing.T) {
	for _, tc := range []struct {
		env     string
		want    forma.HotLegTransport
		wantErr bool
	}{
		{env: "", want: forma.DefaultConfig(nil).DuckDB.HotLegTransport},
		{env: "attach", want: forma.HotLegTransportAttach},
		{env: "pgx", want: forma.HotLegTransportPgx},
		{env: "duckdb", wantErr: true},
	} {
		t.Setenv(HotLegTransportEnv, tc.env)
		got, err := HotLegTransportFromEnv()
		if (err != nil) != tc.wantErr || got != tc.want {
			t.Fatalf("%s=%q: got %q, %v; want %q (error: %v)", HotLegTransportEnv, tc.env, got, err, tc.want, tc.wantErr)
		}
	}
	if forma.DefaultConfig(nil).DuckDB.HotLegTransport != forma.HotLegTransportPgx {
		t.Fatalf("the default transport is not pgx")
	}
}
