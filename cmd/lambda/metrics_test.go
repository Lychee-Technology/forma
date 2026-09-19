package main

import (
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/telemetry"
	"go.uber.org/zap"
)

// TestInstallLambdaMetrics_OffByDefault pins that a function started with no
// METRICS_* variables registers nothing (#423).
func TestInstallLambdaMetrics_OffByDefault(t *testing.T) {
	if err := installLambdaMetrics(zap.NewNop()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestInstallLambdaMetrics_EMFIsTheDefaultProvider pins that METRICS_ENABLED
// alone selects EMF on Lambda: it is the only provider a function can use, so
// the operator need not name it.
func TestInstallLambdaMetrics_EMFIsTheDefaultProvider(t *testing.T) {
	t.Setenv("METRICS_ENABLED", "true")
	t.Cleanup(func() { telemetry.RegisterTelemetryEmitter(nil) })

	if err := installLambdaMetrics(zap.NewNop()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestInstallLambdaMetrics_RefusesPrometheus pins the ruling that Lambda has
// nothing to scrape: asking for prometheus is a startup error naming emf.
func TestInstallLambdaMetrics_RefusesPrometheus(t *testing.T) {
	t.Setenv("METRICS_ENABLED", "true")
	t.Setenv("METRICS_PROVIDER", "prometheus")

	err := installLambdaMetrics(zap.NewNop())
	if err == nil {
		t.Fatalf("expected an error for provider prometheus on lambda")
	}
	if !strings.Contains(err.Error(), "emf") {
		t.Fatalf("error should point at the emf provider, got: %v", err)
	}
}
