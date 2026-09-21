package errorid

import (
	"strings"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// SamplerOption wraps a logger's core in the sampler cfg describes — the same
// construction zap.Config.Build performs for its Sampling field — with
// correlation lines (see IsCorrelationLogger) routed to the core underneath
// it. It is the one place the exemption is installed: internal/bootstrap.
// BuildLogger applies it for a zap.Config, factory.SamplerOption hands it to
// an embedder that assembles its own core, and a test installs it over an
// observer to prove a correlation record survives production-equivalent
// sampling.
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

// IsCorrelationLogger reports whether an entry under loggerName was written
// by Logger: its last name segment is LoggerName. Logger builds on the
// global logger with Named, and zap joins names with a period, so under a
// global an embedder has already named — zap.ReplaceGlobals(l.Named("svc"))
// — the line arrives as "svc.errorid", not "errorid". Matching the last
// segment alone keeps the exemption independent of how the embedder named
// its root; a child of Logger ("errorid.x") or a coincidental "myerrorid"
// is not a correlation line and stays sampled.
func IsCorrelationLogger(loggerName string) bool {
	return loggerName == LoggerName || strings.HasSuffix(loggerName, "."+LoggerName)
}

// samplerExemption sends correlation lines to core and every other entry to
// sampled, which is zap's sampler over that same core. It keys on the
// entry's logger name because that is the only thing a core can see at Check
// time: fields arrive at Write, after the sampler has already decided, which
// is why adding a field to a line cannot exempt it.
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

func (c *samplerExemption) With(fields []zapcore.Field) zapcore.Core {
	return &samplerExemption{sampled: c.sampled.With(fields), core: c.core.With(fields)}
}

func (c *samplerExemption) Check(entry zapcore.Entry, checked *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if IsCorrelationLogger(entry.LoggerName) {
		return c.core.Check(entry, checked)
	}
	return c.sampled.Check(entry, checked)
}

// Write is unreachable through Check, which registers the chosen inner core
// on the CheckedEntry rather than this one; it is here to satisfy the
// interface and routes the same way for a caller that skips Check.
func (c *samplerExemption) Write(entry zapcore.Entry, fields []zapcore.Field) error {
	if IsCorrelationLogger(entry.LoggerName) {
		return c.core.Write(entry, fields)
	}
	return c.sampled.Write(entry, fields)
}

// Sync flushes the shared core once. The sampler in front of it buffers
// nothing of its own — its Sync is the core's — so syncing both sides would
// run the sink's flush twice per logger.Sync, which zapcore.Core does not
// promise is harmless.
func (c *samplerExemption) Sync() error {
	return c.core.Sync()
}
