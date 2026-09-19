package main

import (
	"fmt"
	"os"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/bootstrap"
	"go.uber.org/zap"
)

// installLambdaMetrics resolves the metrics config from the environment and
// registers the telemetry emitter (#423). A function has nothing a Prometheus
// server could scrape, so EMF is the only provider here and also the default:
// METRICS_ENABLED=true alone writes CloudWatch Embedded Metric Format lines
// to stdout, which the function's log group turns into metrics. Off by
// default, like cmd/server.
func installLambdaMetrics(logger *zap.Logger) error {
	defaults := forma.DefaultConfig(nil).Metrics
	defaults.Provider = forma.MetricsProviderEMF
	cfg := bootstrap.MetricsConfigFromEnv(defaults)
	if cfg.Enabled && cfg.Provider != forma.MetricsProviderEMF {
		return fmt.Errorf("metrics provider %q is not supported on lambda: a function cannot be scraped; use %q",
			cfg.Provider, forma.MetricsProviderEMF)
	}
	if _, err := bootstrap.InstallMetrics(cfg, os.Stdout, logger); err != nil {
		return fmt.Errorf("invalid metrics configuration: %w", err)
	}
	if cfg.Enabled {
		logger.Info("telemetry emitter registered", zap.String("provider", cfg.Provider), zap.String("namespace", cfg.Namespace))
	}
	return nil
}
