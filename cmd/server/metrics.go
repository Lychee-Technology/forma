package main

import (
	"fmt"
	"net/http"
	"os"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/bootstrap"
	"go.uber.org/zap"
)

// installServerMetrics resolves the metrics config from the environment and
// registers the chosen provider as the process-wide telemetry emitter (#423).
// It returns the scrape handler to mount (nil when metrics are off or the
// provider is EMF) and the path to mount it on. Off by default: with no
// METRICS_* variable set, the emitter stays the no-op it has always been.
func installServerMetrics(logger *zap.Logger) (http.Handler, string, error) {
	cfg := bootstrap.MetricsConfigFromEnv(forma.DefaultConfig(nil).Metrics)
	handler, err := bootstrap.InstallMetrics(cfg, os.Stdout, logger)
	if err != nil {
		return nil, "", fmt.Errorf("invalid metrics configuration: %w", err)
	}
	if cfg.Enabled {
		logger.Info("telemetry emitter registered", zap.String("provider", cfg.Provider))
	}
	return handler, bootstrap.MetricsPath(cfg), nil
}
