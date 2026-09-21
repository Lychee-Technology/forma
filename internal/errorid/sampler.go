package errorid

import (
	"errors"

	"go.uber.org/zap/zapcore"
)

// ExemptFromSampler returns a core that sends entries named LoggerName to
// unsampled and every other entry to sampled. The two are expected to be the
// same underlying core with and without a zapcore sampler in front of it;
// internal/bootstrap builds them that way.
//
// It keys on the entry's logger name because that is the only thing a core
// can see at Check time: fields arrive at Write, after the sampler has
// already decided, which is why adding a field to a line cannot exempt it.
func ExemptFromSampler(sampled, unsampled zapcore.Core) zapcore.Core {
	return &samplerExemption{sampled: sampled, unsampled: unsampled}
}

type samplerExemption struct {
	sampled   zapcore.Core
	unsampled zapcore.Core
}

func (c *samplerExemption) Enabled(level zapcore.Level) bool {
	return c.sampled.Enabled(level) || c.unsampled.Enabled(level)
}

// Level makes zapcore.LevelOf see through the wrapper, as it does through
// zap's own sampler.
func (c *samplerExemption) Level() zapcore.Level {
	sampled, unsampled := zapcore.LevelOf(c.sampled), zapcore.LevelOf(c.unsampled)
	if unsampled < sampled {
		return unsampled
	}
	return sampled
}

func (c *samplerExemption) With(fields []zapcore.Field) zapcore.Core {
	return &samplerExemption{sampled: c.sampled.With(fields), unsampled: c.unsampled.With(fields)}
}

func (c *samplerExemption) Check(entry zapcore.Entry, checked *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if entry.LoggerName == LoggerName {
		return c.unsampled.Check(entry, checked)
	}
	return c.sampled.Check(entry, checked)
}

// Write is unreachable through Check, which registers the chosen inner core
// on the CheckedEntry rather than this one; it is here to satisfy the
// interface and routes the same way for a caller that skips Check.
func (c *samplerExemption) Write(entry zapcore.Entry, fields []zapcore.Field) error {
	if entry.LoggerName == LoggerName {
		return c.unsampled.Write(entry, fields)
	}
	return c.sampled.Write(entry, fields)
}

// Sync flushes both sides. In the intended arrangement they share one
// underlying core and the second flush is a no-op; syncing both keeps the
// wrapper correct for a caller that hands it two distinct cores.
func (c *samplerExemption) Sync() error {
	return errors.Join(c.sampled.Sync(), c.unsampled.Sync())
}
