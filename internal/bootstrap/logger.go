package bootstrap

import (
	"fmt"

	"github.com/lychee-technology/forma/internal/errorid"
	"go.uber.org/zap"
)

// NewProductionLogger is zap.NewProduction with one difference: the sampler
// it installs never drops a correlation record. Every cmd/ entrypoint that
// runs at the production Info threshold builds its logger here — directly,
// or through factory.NewProductionLogger, the public name an embedder is told
// to use — so the contract holds in every binary that can mint an error_id
// (#398).
func NewProductionLogger(opts ...zap.Option) (*zap.Logger, error) {
	return BuildLogger(zap.NewProductionConfig(), opts...)
}

// BuildLogger builds cfg as cfg.Build does, except that cfg.Sampling is
// installed through errorid.SamplerOption so a line written by errorid.Logger
// bypasses it. A cfg with Sampling nil builds exactly as cfg.Build would.
//
// The sampler is the reason this exists. zap's production sampler keys on
// level and message alone (zapcore/sampler.go), and every line that carries
// an error_id has a constant message — "BatchCreate operation failed", or
// the HTTP handler's op — so the 101st identical failure inside one second
// would return a caller an id whose line was never written. The join between
// body and log is the whole point of the id, and it has to survive the
// failure storm that produces such rates, so those lines are exempt. Every
// other line keeps the sampler's protection, including the report-only
// validation line that relies on it for its volume bound (#317).
func BuildLogger(cfg zap.Config, opts ...zap.Option) (*zap.Logger, error) {
	sampling := cfg.Sampling
	cfg.Sampling = nil
	if sampling != nil {
		// Ahead of the caller's options, where cfg.Build would have put zap's
		// own sampler, so a caller's WrapCore wraps the sampled core as before.
		opts = append([]zap.Option{errorid.SamplerOption(sampling)}, opts...)
	}
	logger, err := cfg.Build(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to build logger: %w", err)
	}
	return logger, nil
}
