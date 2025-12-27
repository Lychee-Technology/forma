package e2e_harness

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// TestHarness holds lightweight runners for dependencies used by E2E tests.
type TestHarness struct {
	PGContainer testcontainers.Container
	PGDSN       string
	PGDB        *sql.DB
	S3Container testcontainers.Container
	S3Endpoint  string
	Duck        *internal.DuckDBClient
}

// StartPostgres starts a postgres container and returns a DSN.
// It waits until Postgres is reachable. Caller is responsible for calling StopPostgres.
func (h *TestHarness) StartPostgres(ctx context.Context) (string, error) {
	req := testcontainers.ContainerRequest{
		Image:        "postgres:16",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_PASSWORD": "password",
			"POSTGRES_USER":     "postgres",
			"POSTGRES_DB":       "postgres",
		},
		WaitingFor: wait.ForListeningPort("5432/tcp").WithStartupTimeout(30 * time.Second),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return "", err
	}
	h.PGContainer = container

	host, err := container.Host(ctx)
	if err != nil {
		return "", err
	}
	mapped, err := container.MappedPort(ctx, "5432")
	if err != nil {
		return "", err
	}
	dsn := fmt.Sprintf("postgres://postgres:password@%s:%s/postgres?sslmode=disable", host, mapped.Port())
	h.PGDSN = dsn

	// Open DB connection
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return "", err
	}
	// Wait until reachable
	deadline := time.Now().Add(20 * time.Second)
	for {
		if err := db.PingContext(ctx); err == nil {
			h.PGDB = db
			return dsn, nil
		}
		if time.Now().After(deadline) {
			db.Close()
			return "", fmt.Errorf("postgres did not become ready: %w", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// StopPostgres stops the Postgres container and closes DB handle.
func (h *TestHarness) StopPostgres(ctx context.Context) error {
	if h.PGDB != nil {
		h.PGDB.Close()
		h.PGDB = nil
	}
	if h.PGContainer != nil {
		if err := h.PGContainer.Terminate(ctx); err != nil {
			return err
		}
		h.PGContainer = nil
	}
	return nil
}

// StartS3 starts a MinIO container (optional) and returns its endpoint.
func (h *TestHarness) StartS3(ctx context.Context) (string, error) {
	req := testcontainers.ContainerRequest{
		Image:        "rustfs/rustfs:latest",
		ExposedPorts: []string{"9000/tcp"},
		Env: map[string]string{
			"RUSTFS_ACCESS_KEY": "minio",
			"RUSTFS_SECRET_KEY": "minio",
		},
		WaitingFor: wait.ForListeningPort("9000/tcp").WithStartupTimeout(30 * time.Second),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return "", err
	}
	h.S3Container = container
	host, err := container.Host(ctx)
	if err != nil {
		return "", err
	}
	mapped, err := container.MappedPort(ctx, "9000")
	if err != nil {
		return "", err
	}
	endpoint := fmt.Sprintf("http://%s:%s", host, mapped.Port())
	h.S3Endpoint = endpoint
	return endpoint, nil
}

// StopS3 stops the MinIO container.
func (h *TestHarness) StopS3(ctx context.Context) error {
	if h.S3Container != nil {
		if err := h.S3Container.Terminate(ctx); err != nil {
			return err
		}
		h.S3Container = nil
	}
	return nil
}

// StartDuckDB creates a DuckDB client configured to optionally use S3/httpfs.
// It reuses NewDuckDBClient defined in internal/duckdb_conn.go.
func (h *TestHarness) StartDuckDB(cfg forma.DuckDBConfig) error {
	c, err := internal.NewDuckDBClient(cfg)
	if err != nil {
		return err
	}
	h.Duck = c
	return nil
}

// StopDuckDB closes the duckdb client.
func (h *TestHarness) StopDuckDB() error {
	if h.Duck != nil {
		if err := h.Duck.Close(); err != nil {
			return err
		}
		h.Duck = nil
	}
	return nil
}
