package main

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
)

// A warm execution environment outlives the IAM token's 15-minute default
// expiry and replaces its connections every ConnMaxLifetime, so the DSQL pool
// must sign a token for each connection it opens rather than reuse the one
// from the cold start.
func TestDSQLPoolSignsATokenPerConnection(t *testing.T) {
	t.Setenv("PGPASSWORD", "")
	signed := 0
	newToken := func(context.Context) (string, error) {
		signed++
		return fmt.Sprintf("token-%d", signed), nil
	}
	poolConfig, err := dsqlPoolConfig("example.dsql.us-east-2.on.aws", lambdaPoolSettings(), newToken)
	if err != nil {
		t.Fatalf("dsqlPoolConfig: %v", err)
	}
	if poolConfig.ConnConfig.Password != "" {
		t.Fatalf("the pool config must carry no token of its own, got %q", poolConfig.ConnConfig.Password)
	}
	if poolConfig.BeforeConnect == nil {
		t.Fatal("the pool config has no BeforeConnect, so every connection would reuse one token")
	}

	for want := 1; want <= 3; want++ {
		connConfig := poolConfig.ConnConfig.Copy()
		if err := poolConfig.BeforeConnect(context.Background(), connConfig); err != nil {
			t.Fatalf("BeforeConnect: %v", err)
		}
		if got, wantToken := connConfig.Password, fmt.Sprintf("token-%d", want); got != wantToken {
			t.Fatalf("connection %d authenticates with %q, want the freshly signed %q", want, got, wantToken)
		}
	}
}

// The pool calls the hook for every connection it opens, and a token that
// cannot be signed fails that connection with the signing error attached.
func TestDSQLPoolSigningFailureFailsTheConnection(t *testing.T) {
	errSigning := errors.New("credentials expired")
	signed := 0
	newToken := func(context.Context) (string, error) {
		signed++
		return "", errSigning
	}
	// 127.0.0.1 keeps the endpoint off DNS; the hook fails before any dial.
	poolConfig, err := dsqlPoolConfig("127.0.0.1", lambdaPoolSettings(), newToken)
	if err != nil {
		t.Fatalf("dsqlPoolConfig: %v", err)
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		t.Fatalf("NewWithConfig: %v", err)
	}
	defer pool.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for attempt := 1; attempt <= 2; attempt++ {
		conn, err := pool.Acquire(ctx)
		if err == nil {
			conn.Release()
			t.Fatalf("attempt %d: connected without a signed token", attempt)
		}
		if !errors.Is(err, errSigning) {
			t.Fatalf("attempt %d: expected the signing error, got %v", attempt, err)
		}
	}
	if signed != 2 {
		t.Fatalf("expected one signing per connection attempt (2), got %d", signed)
	}
}

func lambdaPoolSettings() forma.DatabaseConfig {
	return forma.DatabaseConfig{
		MaxConnections:  2,
		ConnMaxLifetime: 5 * time.Minute,
		ConnMaxIdleTime: time.Minute,
		Timeout:         time.Second,
	}
}
