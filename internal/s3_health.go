package internal

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/lychee-technology/forma"
)

// ValidateS3Config performs basic sanity checks on S3-related DuckDB settings.
func ValidateS3Config(cfg forma.DuckDBConfig) error {
	if !cfg.EnableS3 {
		return nil
	}
	if cfg.S3Endpoint == "" && cfg.S3AccessKey == "" && cfg.S3SecretKey == "" {
		return fmt.Errorf("s3: enableS3=true requires at least s3Endpoint or credentials")
	}
	if cfg.S3AccessKey != "" && cfg.S3SecretKey == "" {
		return fmt.Errorf("s3AccessKey provided without s3SecretKey")
	}
	if cfg.S3SecretKey != "" && cfg.S3AccessKey == "" {
		return fmt.Errorf("s3SecretKey provided without s3AccessKey")
	}
	return nil
}

// S3HealthCheck attempts a best-effort HTTP ping against the configured S3 endpoint.
// This is intentionally lightweight and non-authoritative: it will only succeed for endpoints
// that accept anonymous HEAD/GET requests (e.g., some MinIO setups). For AWS S3 this will often
// return 403 but is still useful to validate DNS/resolution and TLS.
func S3HealthCheck(ctx context.Context, cfg forma.DuckDBConfig, timeout time.Duration) error {
	if !cfg.EnableS3 {
		return nil
	}
	if cfg.S3Endpoint == "" {
		return fmt.Errorf("s3 endpoint not configured")
	}
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	client := &http.Client{
		Timeout: timeout,
	}

	req, err := http.NewRequestWithContext(reqCtx, http.MethodHead, cfg.S3Endpoint, nil)
	if err != nil {
		return fmt.Errorf("s3 health request build failed: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		// Best-effort: network/DNS failure is actionable.
		return fmt.Errorf("s3 health request failed: %w", err)
	}
	defer resp.Body.Close()

	// Treat 200-399 as success; 403/401 as warning but not fatal for presence check.
	if resp.StatusCode >= 200 && resp.StatusCode < 400 {
		return nil
	}
	if resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusUnauthorized {
		return fmt.Errorf("s3 endpoint reachable but returned auth error: %d", resp.StatusCode)
	}
	return fmt.Errorf("s3 endpoint returned unexpected status: %d", resp.StatusCode)
}
