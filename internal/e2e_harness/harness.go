package e2e_harness

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	fedengine "github.com/lychee-technology/forma/internal/federated"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/lychee-technology/forma"
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
	Duck        *fedengine.DuckDBClient
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
	if err := h.ConnectPostgres(ctx, dsn); err != nil {
		return "", err
	}
	return dsn, nil
}

// ConnectPostgres opens a Postgres connection using an existing DSN.
func (h *TestHarness) ConnectPostgres(ctx context.Context, dsn string) error {
	h.PGDSN = dsn

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return err
	}

	deadline := time.Now().Add(20 * time.Second)
	for {
		if pingErr := db.PingContext(ctx); pingErr == nil {
			h.PGDB = db
			return nil
		} else if time.Now().After(deadline) {
			db.Close()
			return fmt.Errorf("postgres did not become ready: %w", pingErr)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// RestartPostgres stops and restarts the Postgres container in place
// (docker stop/start — the writable layer and therefore all data survive,
// unlike StopPostgres, which terminates and destroys the container). The
// host-mapped port is not guaranteed stable across restarts, so the DSN is
// re-derived before reconnecting.
func (h *TestHarness) RestartPostgres(ctx context.Context) error {
	if err := h.HaltPostgres(ctx); err != nil {
		return err
	}
	return h.ResumePostgres(ctx)
}

// HaltPostgres stops the Postgres container in place (docker stop — the
// container and its data survive) and closes the harness DB handle, leaving
// the server really unreachable: queries through per-test pools and DuckDB's
// postgres attachment fail with genuine network errors, the only fault
// vector that exercises production connection-failure handling (#187).
// Resume with ResumePostgres.
func (h *TestHarness) HaltPostgres(ctx context.Context) error {
	if h.PGContainer == nil {
		return fmt.Errorf("halt postgres: no container (external infrastructure or already terminated)")
	}
	if h.PGDB != nil {
		h.PGDB.Close()
		h.PGDB = nil
	}
	stopTimeout := 30 * time.Second
	if err := h.PGContainer.Stop(ctx, &stopTimeout); err != nil {
		return fmt.Errorf("stop postgres container: %w", err)
	}
	return nil
}

// ResumePostgres starts a previously halted Postgres container and
// reconnects. The host-mapped port is not guaranteed stable across restarts,
// so the DSN is re-derived before reconnecting.
func (h *TestHarness) ResumePostgres(ctx context.Context) error {
	if h.PGContainer == nil {
		return fmt.Errorf("resume postgres: no container (external infrastructure or already terminated)")
	}
	if err := h.PGContainer.Start(ctx); err != nil {
		return fmt.Errorf("start postgres container: %w", err)
	}
	host, err := h.PGContainer.Host(ctx)
	if err != nil {
		return fmt.Errorf("resolve postgres host after restart: %w", err)
	}
	mapped, err := h.PGContainer.MappedPort(ctx, "5432")
	if err != nil {
		return fmt.Errorf("resolve postgres port after restart: %w", err)
	}
	dsn := fmt.Sprintf("postgres://postgres:password@%s:%s/postgres?sslmode=disable", host, mapped.Port())
	if err := h.ConnectPostgres(ctx, dsn); err != nil {
		return fmt.Errorf("reconnect postgres after restart: %w", err)
	}
	return nil
}

// HaltS3 stops the S3 container in place (docker stop — the container and
// its data survive, unlike StopS3, which terminates it). The endpoint
// becomes really unreachable for both the Go S3 client and DuckDB httpfs,
// which is the only fault vector that reaches DuckDB reads: client-level
// decorators cannot, because httpfs bypasses the Go S3 client entirely.
func (h *TestHarness) HaltS3(ctx context.Context) error {
	if h.S3Container == nil {
		return fmt.Errorf("halt s3: no container (external infrastructure or already terminated)")
	}
	stopTimeout := 30 * time.Second
	if err := h.S3Container.Stop(ctx, &stopTimeout); err != nil {
		return fmt.Errorf("stop s3 container: %w", err)
	}
	return nil
}

// RestartS3 starts the S3 container again after HaltS3 (docker start — the
// container and its data survive a halt) and re-derives the endpoint: the
// host-mapped port is not guaranteed stable across restarts.
func (h *TestHarness) RestartS3(ctx context.Context) error {
	if h.S3Container == nil {
		return fmt.Errorf("restart s3: no container (external infrastructure or already terminated)")
	}
	if err := h.S3Container.Start(ctx); err != nil {
		return fmt.Errorf("start s3 container: %w", err)
	}
	host, err := h.S3Container.Host(ctx)
	if err != nil {
		return fmt.Errorf("resolve s3 host after restart: %w", err)
	}
	mapped, err := h.S3Container.MappedPort(ctx, "9000")
	if err != nil {
		return fmt.Errorf("resolve s3 port after restart: %w", err)
	}
	h.S3Endpoint = fmt.Sprintf("http://%s:%s", host, mapped.Port())
	return nil
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
		Image:        "minio/minio:latest",
		ExposedPorts: []string{"9000/tcp"},
		Env: map[string]string{
			"MINIO_ROOT_USER":     "minioadmin",
			"MINIO_ROOT_PASSWORD": "minioadmin",
		},
		Cmd:        []string{"server", "/data", "--address", ":9000"},
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
	c, err := fedengine.NewDuckDBClient(cfg)
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
