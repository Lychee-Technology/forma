package main

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
)

type fakeSchemaRegistry struct{}

func (fakeSchemaRegistry) GetSchemaAttributeCacheByName(string) (int16, forma.SchemaAttributeCache, error) {
	return 0, nil, nil
}

func (fakeSchemaRegistry) GetSchemaAttributeCacheByID(int16) (string, forma.SchemaAttributeCache, error) {
	return "", nil, nil
}

func (fakeSchemaRegistry) GetSchemaByName(string) (int16, forma.JSONSchema, error) {
	return 0, forma.JSONSchema{}, nil
}

func (fakeSchemaRegistry) GetSchemaByID(int16) (string, forma.JSONSchema, error) {
	return "", forma.JSONSchema{}, nil
}

func (fakeSchemaRegistry) ListSchemas() []string {
	return nil
}

func TestBuildToolPostgresPool_UsesSharedFactory(t *testing.T) {
	old := toolPostgresPoolFn
	defer func() { toolPostgresPoolFn = old }()

	called := false
	toolPostgresPoolFn = func(ctx context.Context, cfg forma.DatabaseConfig) (*pgxpool.Pool, error) {
		called = true
		if cfg.Host != "db.example" {
			t.Fatalf("expected forwarded config, got %+v", cfg)
		}
		return nil, context.Canceled
	}

	_, err := buildToolPostgresPool(context.Background(), forma.DatabaseConfig{Host: "db.example"})
	if !called {
		t.Fatal("expected shared postgres factory to be called")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}
}

func TestBuildToolSchemaRegistry_UsesSharedFactory(t *testing.T) {
	old := toolSchemaRegistryFn
	defer func() { toolSchemaRegistryFn = old }()

	called := false
	toolSchemaRegistryFn = func(ctx context.Context, pool *pgxpool.Pool, table, dir string) (forma.SchemaRegistry, error) {
		called = true
		if table != "schema_registry" || dir != "/tmp/schemas" {
			t.Fatalf("unexpected args: table=%s dir=%s", table, dir)
		}
		return fakeSchemaRegistry{}, nil
	}

	registry, err := buildToolSchemaRegistry(context.Background(), nil, "schema_registry", "/tmp/schemas")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Fatal("expected shared schema registry factory to be called")
	}
	if registry == nil {
		t.Fatal("expected non-nil registry")
	}
}

func TestBuildToolS3Client_UsesSharedLoader(t *testing.T) {
	old := toolLoadAWSConfigFn
	defer func() { toolLoadAWSConfigFn = old }()

	called := false
	toolLoadAWSConfigFn = func(ctx context.Context, optFns ...func(*awsconfig.LoadOptions) error) (aws.Config, error) {
		called = true
		return aws.Config{Region: "us-west-2"}, nil
	}

	client, err := buildToolS3Client(context.Background(), "us-west-2", "http://localhost:9000", true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Fatal("expected AWS config loader to be called")
	}
	if client == nil {
		t.Fatal("expected non-nil s3 client")
	}
}
