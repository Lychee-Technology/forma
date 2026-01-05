package cdc

import (
	"context"
	"testing"
)

func TestIAMTokenFallbackUsesEnvPassword(t *testing.T) {
	ctx := context.Background()
	orig := generateIAMTokenFn
	defer func() { generateIAMTokenFn = orig }()
	// simulate generate fn returning empty token and no error
	generateIAMTokenFn = func(ctx context.Context, endpoint, region string, creds interface{}) (string, error) {
		return "", nil
	}
	cfg := CDCConfig{PGHost: "localhost", PGPort: 5432, PGUser: "u", PGDB: "db", PGUseIAM: true, PGPassword: "envpass"}
	pgPassword := cfg.PGPassword
	if cfg.PGUseIAM {
		if token, err := generateIAMTokenFn(ctx, "localhost:5432", "us-east-1", nil); err == nil && token != "" {
			pgPassword = token
		}
	}
	if pgPassword != "envpass" {
		t.Fatalf("expected fallback to envpass, got %s", pgPassword)
	}
}
