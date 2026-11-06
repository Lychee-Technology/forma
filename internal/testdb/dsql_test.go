package testdb

import (
	"context"
	"net"
	"testing"
	"time"
)

// TestMiniStackStrictProxy is the lane's gate (#739): scripts/test_dsql_ministack.sh
// runs it before any suite and stops when it fails. It runs only when the
// environment names the proxy, and then it cannot pass without reaching it.
func TestMiniStackStrictProxy(t *testing.T) {
	dsn, kind := ResolveDSQL(t)
	if kind != DSQLMiniStack {
		t.Skipf("%s does not name MiniStack's proxy", DSQLTargetEnv)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := ProveMiniStackStrictProxy(ctx, dsn); err != nil {
		t.Fatalf("this run did not reach MiniStack's strict SQL proxy: %v", err)
	}
}

// The gate's negative control: the integration PostgreSQL is a working SQL
// endpoint that answers everything the lane's suites send, and the gate must
// still refuse it. CI's Test job runs this.
func TestProveMiniStackStrictProxyRefusesDirectPostgres(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	Connect(t, ctx) // skips locally, and fails in CI, when PostgreSQL is absent
	err := ProveMiniStackStrictProxy(ctx, ResolveDSN())
	if err == nil {
		t.Fatal("the gate accepted a direct PostgreSQL endpoint as the strict proxy")
	}
	t.Logf("refused as it should be: %v", err)
}

// A metadata-only cluster reports ACTIVE with nothing listening on its port.
func TestProveMiniStackStrictProxyRefusesAnEndpointWithoutSQL(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve a port: %v", err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("free the port: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := ProveMiniStackStrictProxy(ctx, "postgres://admin@"+addr+"/postgres?sslmode=disable"); err == nil {
		t.Fatal("the gate accepted an endpoint with nothing listening")
	}
}

func TestResolveDSQL(t *testing.T) {
	const dsn = "postgres://admin@127.0.0.1:25432/postgres?sslmode=disable"
	for _, tc := range []struct {
		name, dsn, target string
		want              DSQLKind
		wantErr           bool
	}{
		{name: "nothing named is the emulation", want: DSQLEmulated},
		{name: "a DSN alone is a genuine cluster", dsn: dsn, want: DSQLCluster},
		{name: "a DSN named as MiniStack is MiniStack", dsn: dsn, target: "ministack", want: DSQLMiniStack},
		{name: "MiniStack without a DSN fails rather than run on the integration PostgreSQL", target: "ministack", wantErr: true},
		{name: "an unknown target fails rather than run as a cluster", dsn: dsn, target: "ministak", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, kind, err := resolveDSQL(tc.dsn, tc.target)
			if (err != nil) != tc.wantErr {
				t.Fatalf("resolveDSQL err = %v, wantErr %t", err, tc.wantErr)
			}
			if kind != tc.want {
				t.Errorf("resolveDSQL kind = %d, want %d", kind, tc.want)
			}
		})
	}
}
