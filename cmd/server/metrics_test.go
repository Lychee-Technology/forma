package main

import (
	"testing"

	"github.com/lychee-technology/forma/internal/telemetry"
	"go.uber.org/zap"
)

// TestInstallServerMetrics_OffByDefault pins that a server started with no
// METRICS_* variables mounts no scrape endpoint (#423).
func TestInstallServerMetrics_OffByDefault(t *testing.T) {
	h, _, err := installServerMetrics(zap.NewNop())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if h != nil {
		t.Fatalf("expected no metrics handler by default")
	}
}

// TestInstallServerMetrics_PrometheusOptIn pins that METRICS_ENABLED=true
// alone yields a Prometheus handler on the default path.
func TestInstallServerMetrics_PrometheusOptIn(t *testing.T) {
	t.Setenv("METRICS_ENABLED", "true")
	t.Cleanup(func() { telemetry.RegisterTelemetryEmitter(nil) })

	h, path, err := installServerMetrics(zap.NewNop())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if h == nil || path != "/metrics" {
		t.Fatalf("expected prometheus handler on /metrics, got handler=%v path=%q", h != nil, path)
	}
}

// TestInstallServerMetrics_RejectsUnknownProvider pins that a misspelt provider
// fails startup instead of running silently inert.
func TestInstallServerMetrics_RejectsUnknownProvider(t *testing.T) {
	t.Setenv("METRICS_ENABLED", "true")
	t.Setenv("METRICS_PROVIDER", "statsd")

	if _, _, err := installServerMetrics(zap.NewNop()); err == nil {
		t.Fatalf("expected an error for provider statsd")
	}
}
