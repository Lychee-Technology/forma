package e2e_harness

import (
	"context"
	"fmt"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// RustFS credentials used by StartRustFS. They intentionally match the MinIO
// defaults in StartS3 so callers can switch S3 backends without touching
// client configuration.
const (
	RustFSAccessKey = "minioadmin"
	RustFSSecretKey = "minioadmin"
)

// StartRustFS starts a RustFS container (S3-compatible object store, same
// image as deploy/docker-compose.yml) and returns its endpoint. RustFS is the
// preferred S3 backend for new suites: the official MinIO docker image is
// archived and no longer receives updates. StartS3 (MinIO) remains for the
// existing federated suite. Caller is responsible for calling StopS3.
func (h *TestHarness) StartRustFS(ctx context.Context) (string, error) {
	req := testcontainers.ContainerRequest{
		Image:        "rustfs/rustfs:latest",
		ExposedPorts: []string{"9000/tcp"},
		Env: map[string]string{
			"RUSTFS_ACCESS_KEY": RustFSAccessKey,
			"RUSTFS_SECRET_KEY": RustFSSecretKey,
		},
		WaitingFor: wait.ForListeningPort("9000/tcp").WithStartupTimeout(60 * time.Second),
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
