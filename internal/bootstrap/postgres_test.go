package bootstrap

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lychee-technology/forma"
)

func TestNewPostgresPoolFromConfigContext_Canceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := NewPostgresPoolFromConfigContext(ctx, testDatabaseConfig())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}
}

func TestNewPostgresPoolFromConfigContext_DeadlineExceeded(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	_, err := NewPostgresPoolFromConfigContext(ctx, testDatabaseConfig())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
}

func testDatabaseConfig() forma.DatabaseConfig {
	return forma.DatabaseConfig{
		Host:           "localhost",
		Port:           5432,
		Database:       "forma",
		Username:       "postgres",
		Password:       "postgres",
		SSLMode:        "disable",
		MaxConnections: 4,
		Timeout:        3 * time.Second,
	}
}
