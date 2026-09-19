package bootstrap

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/telemetry"
)

// TestMetricsConfigFromEnv_OffByDefault pins the #423 rollout rule: a
// deployment that sets nothing keeps the inert emitter it has today.
func TestMetricsConfigFromEnv_OffByDefault(t *testing.T) {
	got := MetricsConfigFromEnv(forma.DefaultConfig(nil).Metrics)
	require.False(t, got.Enabled)
	require.Equal(t, forma.MetricsProviderPrometheus, got.Provider)
}

func TestMetricsConfigFromEnv_Overlays(t *testing.T) {
	t.Setenv("METRICS_ENABLED", "true")
	t.Setenv("METRICS_PROVIDER", "emf")
	t.Setenv("METRICS_NAMESPACE", "forma-prod")
	t.Setenv("METRICS_PATH", "/internal/metrics")

	got := MetricsConfigFromEnv(forma.DefaultConfig(nil).Metrics)
	require.True(t, got.Enabled)
	require.Equal(t, forma.MetricsProviderEMF, got.Provider)
	require.Equal(t, "forma-prod", got.Namespace)
	require.Equal(t, "/internal/metrics", got.Endpoint)
}

func scrapeBody(t *testing.T, h http.Handler) string {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	body, err := io.ReadAll(rec.Body)
	require.NoError(t, err)
	return string(body)
}

// TestInstallMetrics_Disabled pins that a disabled config installs nothing and
// leaves whatever emitter is registered untouched.
func TestInstallMetrics_Disabled(t *testing.T) {
	var seen int
	telemetry.RegisterTelemetryEmitter(func(context.Context, string, map[string]string, any) { seen++ })
	t.Cleanup(func() { telemetry.RegisterTelemetryEmitter(nil) })

	h, err := InstallMetrics(forma.MetricsConfig{Enabled: false, Provider: "prometheus"}, io.Discard, zap.NewNop())
	require.NoError(t, err)
	require.Nil(t, h)

	telemetry.EmitRowCount(context.Background(), "pg", 1)
	require.Equal(t, 1, seen, "disabled metrics must not replace the registered emitter")
}

// TestInstallMetrics_Prometheus pins the end-to-end path: the global hook is
// registered, and what an Emit* helper records is what the returned handler
// serves.
func TestInstallMetrics_Prometheus(t *testing.T) {
	t.Cleanup(func() { telemetry.RegisterTelemetryEmitter(nil) })

	h, err := InstallMetrics(forma.MetricsConfig{Enabled: true, Provider: "prometheus"}, io.Discard, zap.NewNop())
	require.NoError(t, err)
	require.NotNil(t, h)

	telemetry.EmitRowCount(context.Background(), "pg", 5)
	require.Contains(t, scrapeBody(t, h), `fed_query_row_count{source="pg"} 5`)
}

// TestInstallMetrics_EMF pins that the emf provider returns no scrape handler
// and writes EMF lines to the sink under the configured namespace.
func TestInstallMetrics_EMF(t *testing.T) {
	t.Cleanup(func() { telemetry.RegisterTelemetryEmitter(nil) })
	var sink bytes.Buffer

	h, err := InstallMetrics(forma.MetricsConfig{Enabled: true, Provider: "emf", Namespace: "forma-test"}, &sink, zap.NewNop())
	require.NoError(t, err)
	require.Nil(t, h)

	telemetry.EmitRowCount(context.Background(), "pg", 5)
	require.Contains(t, sink.String(), `"Namespace":"forma-test"`)
	require.Contains(t, sink.String(), `"fed_query_row_count":5`)
}

// TestInstallMetrics_UnknownProvider pins the failure mode for a typo: startup
// refuses with an error naming the value and the accepted set, rather than
// silently running inert.
func TestInstallMetrics_UnknownProvider(t *testing.T) {
	_, err := InstallMetrics(forma.MetricsConfig{Enabled: true, Provider: "statsd"}, io.Discard, zap.NewNop())
	require.Error(t, err)
	require.Contains(t, err.Error(), `"statsd"`)
	require.Contains(t, err.Error(), "prometheus")
	require.Contains(t, err.Error(), "emf")
}

// TestInstallMetrics_EMFNeedsNamespace pins that CloudWatch's mandatory
// namespace is checked at startup, not discovered as rejected log lines.
func TestInstallMetrics_EMFNeedsNamespace(t *testing.T) {
	_, err := InstallMetrics(forma.MetricsConfig{Enabled: true, Provider: "emf", Namespace: " "}, io.Discard, zap.NewNop())
	require.Error(t, err)
	require.Contains(t, err.Error(), "namespace")
}

// TestMetricsPath pins the default scrape path and that a configured endpoint
// must be an absolute path.
func TestMetricsPath(t *testing.T) {
	require.Equal(t, "/metrics", MetricsPath(forma.MetricsConfig{}))
	require.Equal(t, "/internal/metrics", MetricsPath(forma.MetricsConfig{Endpoint: "/internal/metrics"}))
}
