package factory

import (
	"github.com/lychee-technology/forma/internal/bootstrap"
	"github.com/lychee-technology/forma/internal/errorid"
	"go.uber.org/zap"
)

// NewProductionLogger is zap.NewProduction with one difference: its sampler
// never drops a log line that a caller holds an error_id for (#398).
//
// Forma logs through zap's global logger, so the process that embeds it owns
// the sampler. zap's production sampler keys on level and message alone —
// the first 100 identical entries per second, then every 100th — and every
// line Forma writes beside an error_id has a constant message: the
// best-effort batch failure line that keeps the full error a
// forma.OperationError withheld, and the HTTP error lines behind a redacted
// body. Under stock zap.NewProduction, a batch of 101 identical failures
// inside one second returns 101 ids and writes 100 lines; the caller of the
// 101st holds a handle to nothing. Installing the global logger from here
// keeps every issued id joinable:
//
//	logger, err := factory.NewProductionLogger()
//	if err != nil { ... }
//	zap.ReplaceGlobals(logger)
//
// Every other line keeps the sampler's protection. This is the logger
// cmd/server and cmd/lambda run on.
func NewProductionLogger(opts ...zap.Option) (*zap.Logger, error) {
	return bootstrap.NewProductionLogger(opts...)
}

// BuildLogger builds cfg as cfg.Build does, with cfg.Sampling installed the
// way NewProductionLogger installs it, for an embedder that starts from its
// own zap.Config (console encoding, another level, extra output paths). A
// cfg with Sampling nil builds exactly as cfg.Build would.
//
// The level is kept as given, and it decides which ids are issued: a failed
// best-effort operation and a disclosed 4xx that withholds operator detail
// log their error_id at Warn, so above Warn they carry no id at all, and a
// redacted response logs at Error. Forma never returns an id for a line the
// logger will not write.
func BuildLogger(cfg zap.Config, opts ...zap.Option) (*zap.Logger, error) {
	return bootstrap.BuildLogger(cfg, opts...)
}

// SamplerOption wraps a logger's core in the sampler cfg describes, with the
// same exemption NewProductionLogger applies, for an embedder that assembles
// its core by hand (zap.New over a custom encoder or sink) rather than from
// a zap.Config. Pass it in place of zap's own sampler; a sampler installed
// underneath it still drops correlation lines, because the exemption can
// only route around a sampler it sits in front of.
func SamplerOption(cfg *zap.SamplingConfig) zap.Option {
	return errorid.SamplerOption(cfg)
}
