package e2e_harness

import (
	"context"
	"fmt"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// LocalStack accepts any static credentials; these are its documented
// defaults.
const (
	LocalStackAccessKey = "test"
	LocalStackSecretKey = "test"
)

// localStackImage is LocalStack 4.9.2, the community image, pinned by digest
// for the same reproducibility reason as the RustFS image. Later LocalStack
// releases refuse to start without a LOCALSTACK_AUTH_TOKEN, which neither CI
// nor a contributor's machine has, so a bump must stay on an image that
// starts without one.
const localStackImage = "localstack/localstack@sha256:59373b4a27dba3ec15c4db0b7455f0cb68c8a42300e2cbbe362b440f9a131697"

// StartLocalStackS3 starts a LocalStack container serving only S3 and
// returns its endpoint and a function that terminates it. The harness's
// default object store stays RustFS (StartS3); this second backend exists so
// a test can pin an S3 behaviour, such as the If-None-Match: * conditional
// put, against more than one implementation.
func StartLocalStackS3(ctx context.Context) (string, func(context.Context) error, error) {
	req := testcontainers.ContainerRequest{
		Image:        localStackImage,
		ExposedPorts: []string{"4566/tcp"},
		Env: map[string]string{
			"SERVICES":               "s3",
			"DISABLE_EVENTS":         "1",
			"SKIP_SSL_CERT_DOWNLOAD": "1",
		},
		// The health endpoint answers 200 only once the gateway serves
		// requests; the wide budget matches StartS3's for a loaded host.
		WaitingFor: wait.ForHTTP("/_localstack/health").
			WithPort("4566/tcp").
			WithStartupTimeout(120 * time.Second),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return "", nil, fmt.Errorf("failed to start localstack container: %w", err)
	}
	stop := func(ctx context.Context) error {
		if err := container.Terminate(ctx); err != nil {
			return fmt.Errorf("failed to terminate localstack container: %w", err)
		}
		return nil
	}
	host, err := container.Host(ctx)
	if err != nil {
		_ = stop(ctx)
		return "", nil, fmt.Errorf("failed to get localstack container host: %w", err)
	}
	mapped, err := container.MappedPort(ctx, "4566")
	if err != nil {
		_ = stop(ctx)
		return "", nil, fmt.Errorf("failed to get localstack container mapped port: %w", err)
	}
	return fmt.Sprintf("http://%s:%s", host, mapped.Port()), stop, nil
}
