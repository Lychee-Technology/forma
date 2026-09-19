package forma

import "testing"

// TestDefaultMetricsConfigIsOff pins #423's rollout rule at the library layer:
// nothing registers a telemetry emitter unless a deployment opts in, so the
// default MetricsConfig must not claim to be enabled.
func TestDefaultMetricsConfigIsOff(t *testing.T) {
	cfg := DefaultConfig(nil).Metrics
	if cfg.Enabled {
		t.Fatalf("expected Metrics.Enabled=false by default, got %+v", cfg)
	}
	if cfg.Provider != MetricsProviderPrometheus {
		t.Fatalf("expected default provider %q, got %q", MetricsProviderPrometheus, cfg.Provider)
	}
}
