package errorid

import (
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// SamplerOption wraps a logger's core in the sampler cfg describes — the same
// construction zap.Config.Build performs for its Sampling field — with
// correlation lines (those written through Logger) routed to the core
// underneath it. It is the one place the exemption is installed:
// internal/bootstrap.BuildLogger applies it for a zap.Config,
// factory.SamplerOption hands it to an embedder that assembles its own core,
// and a test installs it over an observer to prove a correlation record
// survives production-equivalent sampling.
func SamplerOption(cfg *zap.SamplingConfig) zap.Option {
	return zap.WrapCore(func(core zapcore.Core) zapcore.Core {
		var samplerOpts []zapcore.SamplerOption
		if cfg.Hook != nil {
			samplerOpts = append(samplerOpts, zapcore.SamplerHook(cfg.Hook))
		}
		sampled := zapcore.NewSamplerWithOptions(core, time.Second, cfg.Initial, cfg.Thereafter, samplerOpts...)
		return &samplerExemption{sampled: sampled, core: core}
	})
}

// correlationMarker is the value correlationField carries. It is unexported,
// so no code outside this package — least of all a host application sharing
// the global logger — can build a field the exemption accepts.
type correlationMarker struct{}

// correlationField is what makes a logger a correlation logger. Logger adds
// it with With, which zap hands to every core in the chain when the child
// logger is built; samplerExemption.With recognises it and returns the core
// beneath the sampler. It is a SkipType field, so a core that does not know
// it — the global logger an embedder built without SamplerOption, a tee
// beside the exemption — encodes nothing for it.
//
// The exemption keys on this rather than on the logger name. A name is
// shared with every host logger in the process: a host component logging
// under zap.S().Named("errorid") would otherwise bypass the sampler with
// lines no caller holds an id for, weakening the volume bound every other
// line relies on (PR #606 review).
var correlationField = zapcore.Field{Type: zapcore.SkipType, Interface: correlationMarker{}}

// correlated returns logger as a correlation logger: named LoggerName for
// the operator, and marked with correlationField for the sampler.
func correlated(logger *zap.Logger) *zap.Logger {
	return logger.Named(LoggerName).With(correlationField)
}

// withoutCorrelationField returns fields minus the marker, and whether it was
// there.
func withoutCorrelationField(fields []zapcore.Field) ([]zapcore.Field, bool) {
	for i, field := range fields {
		if _, ok := field.Interface.(correlationMarker); ok && field.Type == zapcore.SkipType {
			rest := make([]zapcore.Field, 0, len(fields)-1)
			rest = append(rest, fields[:i]...)
			return append(rest, fields[i+1:]...), true
		}
	}
	return fields, false
}

// samplerExemption sends every entry through sampled, zap's sampler over
// core, until a child is built with correlationField: that child is core
// itself, beneath the sampler, and so is every child built from it. The
// routing is decided at With rather than per entry because a core sees
// nothing at Check time but the entry's level, message and name — fields
// logged with the line arrive at Write, after the sampler has decided — and
// the name is not Forma's to reserve.
type samplerExemption struct {
	sampled zapcore.Core
	core    zapcore.Core
}

func (c *samplerExemption) Enabled(level zapcore.Level) bool {
	return c.core.Enabled(level)
}

// Level makes zapcore.LevelOf see through the wrapper, as it does through
// zap's own sampler.
func (c *samplerExemption) Level() zapcore.Level {
	return zapcore.LevelOf(c.core)
}

// With keeps the routing for an ordinary child (zap's With) by wrapping both
// sides again, and ends it for a correlation logger, whose marker it strips
// so the sink never sees it.
func (c *samplerExemption) With(fields []zapcore.Field) zapcore.Core {
	if rest, marked := withoutCorrelationField(fields); marked {
		if len(rest) == 0 {
			return c.core
		}
		return c.core.With(rest)
	}
	return &samplerExemption{sampled: c.sampled.With(fields), core: c.core.With(fields)}
}

func (c *samplerExemption) Check(entry zapcore.Entry, checked *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	return c.sampled.Check(entry, checked)
}

// Write is unreachable through Check, which registers the sampler on the
// CheckedEntry rather than this wrapper; it is here to satisfy the interface.
func (c *samplerExemption) Write(entry zapcore.Entry, fields []zapcore.Field) error {
	return c.sampled.Write(entry, fields)
}

// Sync flushes the shared core once. The sampler in front of it buffers
// nothing of its own — its Sync is the core's — so syncing both sides would
// run the sink's flush twice per logger.Sync, which zapcore.Core does not
// promise is harmless.
func (c *samplerExemption) Sync() error {
	return c.core.Sync()
}
