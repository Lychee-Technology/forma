package bootstrap

import (
	"fmt"
	"io"
	"net/http"
	"strings"

	"go.uber.org/zap"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/telemetry"
	"github.com/lychee-technology/forma/internal/telemetry/emfexport"
	"github.com/lychee-technology/forma/internal/telemetry/promexport"
)

// defaultMetricsPath is the Prometheus scrape path when Endpoint is unset.
const defaultMetricsPath = "/metrics"

// MetricsConfigFromEnv overlays the operator-settable metrics options (#423):
// METRICS_ENABLED (opt-in, default off), METRICS_PROVIDER (prometheus | emf),
// METRICS_NAMESPACE (CloudWatch namespace for emf) and METRICS_PATH (Prometheus
// scrape path). Every other MetricsConfig field passes through untouched.
func MetricsConfigFromEnv(defaults forma.MetricsConfig) forma.MetricsConfig {
	cfg := defaults
	cfg.Enabled = EnvBool("METRICS_ENABLED", defaults.Enabled)
	cfg.Provider = strings.ToLower(strings.TrimSpace(Env("METRICS_PROVIDER", defaults.Provider)))
	cfg.Namespace = Env("METRICS_NAMESPACE", defaults.Namespace)
	cfg.Endpoint = Env("METRICS_PATH", defaults.Endpoint)
	return cfg
}

// MetricsPath is the path a server mounts the Prometheus handler on.
func MetricsPath(cfg forma.MetricsConfig) string {
	if p := strings.TrimSpace(cfg.Endpoint); p != "" {
		return p
	}
	return defaultMetricsPath
}

// InstallMetrics registers the configured provider as the process-wide
// telemetry emitter and returns the scrape handler when the provider has one
// (Prometheus), or nil (EMF, or metrics disabled). A disabled config leaves
// the currently registered emitter alone. sink receives EMF lines; production
// passes os.Stdout. An unknown provider is a startup error: a typo must not run
// silently inert, which is the condition #423 exists to end.
func InstallMetrics(cfg forma.MetricsConfig, sink io.Writer, logger *zap.Logger) (http.Handler, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	switch cfg.Provider {
	case forma.MetricsProviderPrometheus:
		exp, err := promexport.New(logger)
		if err != nil {
			return nil, fmt.Errorf("failed to build prometheus metrics exporter: %w", err)
		}
		telemetry.RegisterTelemetryEmitter(exp.Emit)
		return exp.Handler(), nil
	case forma.MetricsProviderEMF:
		ns := strings.TrimSpace(cfg.Namespace)
		if ns == "" {
			return nil, fmt.Errorf("metrics provider %q requires a non-empty namespace (METRICS_NAMESPACE)", cfg.Provider)
		}
		telemetry.RegisterTelemetryEmitter(emfexport.New(sink, ns, logger).Emit)
		return nil, nil
	default:
		return nil, fmt.Errorf("unknown metrics provider %q: expected %q or %q",
			cfg.Provider, forma.MetricsProviderPrometheus, forma.MetricsProviderEMF)
	}
}
