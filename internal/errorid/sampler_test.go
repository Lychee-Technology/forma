package errorid

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// productionSampled wraps core in the sampler zap.NewProductionConfig
// installs: 100 identical (level, message) entries per second, then every
// 100th.
func productionSampled(core zapcore.Core) zapcore.Core {
	return zapcore.NewSamplerWithOptions(core, time.Second, 100, 100)
}

// logNTimes writes the same Warn message n times.
func logNTimes(logger *zap.SugaredLogger, n int, msg string) {
	for i := 0; i < n; i++ {
		logger.Warnw(msg, "i", i)
	}
}

// frozenClock pins every entry to one instant so all of a test's lines fall
// inside a single sampler tick, whatever the wall clock does meanwhile.
type frozenClock struct{ at time.Time }

func (c frozenClock) Now() time.Time                       { return c.at }
func (c frozenClock) NewTicker(time.Duration) *time.Ticker { return time.NewTicker(time.Hour) }

func frozenClockOption() zap.Option {
	return zap.WithClock(frozenClock{at: time.Date(2026, 9, 21, 0, 0, 0, 0, time.UTC)})
}

// TestExemptFromSamplerKeepsEveryCorrelationLine is the invariant the
// wrapper exists for: 150 identical lines from Logger inside one second all
// reach the core, while the same 150 from the plain global logger are cut to
// the sampler's first 100 — proof the sampler is live and the exemption is
// what saved the correlation lines, not its absence.
func TestExemptFromSamplerKeepsEveryCorrelationLine(t *testing.T) {
	core, logs := observer.New(zap.InfoLevel)
	restore := zap.ReplaceGlobals(zap.New(ExemptFromSampler(productionSampled(core), core), frozenClockOption()))
	t.Cleanup(restore)

	logNTimes(Logger(), 150, "correlated")
	logNTimes(zap.S(), 150, "uncorrelated")

	require.Len(t, logs.FilterMessage("correlated").All(), 150, "a correlation line is never sampled")
	require.Len(t, logs.FilterMessage("uncorrelated").All(), 100, "every other line still is")
	for _, entry := range logs.FilterMessage("correlated").All() {
		require.Equal(t, LoggerName, entry.LoggerName)
	}
}

// TestExemptFromSamplerSurvivesWith pins that a child logger built with
// fields (zap's With) keeps the routing: the wrapper must wrap both sides
// again rather than collapse to one of them.
func TestExemptFromSamplerSurvivesWith(t *testing.T) {
	core, logs := observer.New(zap.InfoLevel)
	logger := zap.New(ExemptFromSampler(productionSampled(core), core), frozenClockOption()).
		With(zap.String("component", "test"))

	logNTimes(logger.Sugar().Named(LoggerName), 150, "correlated")
	logNTimes(logger.Sugar(), 150, "uncorrelated")

	require.Len(t, logs.FilterMessage("correlated").All(), 150)
	require.Len(t, logs.FilterMessage("uncorrelated").All(), 100)
	require.Equal(t, "test", logs.FilterMessage("correlated").All()[0].ContextMap()["component"],
		"the With fields reach the unsampled side too")
}

// TestExemptFromSamplerLevelAndSync: zapcore.LevelOf must see the wrapped
// level, and Sync must flush both sides and report either failure.
func TestExemptFromSamplerLevelAndSync(t *testing.T) {
	core, _ := observer.New(zap.WarnLevel)
	wrapped := ExemptFromSampler(productionSampled(core), core)
	require.Equal(t, zap.WarnLevel, zapcore.LevelOf(wrapped))
	require.False(t, wrapped.Enabled(zap.InfoLevel))
	require.True(t, wrapped.Enabled(zap.WarnLevel))

	syncErr := errors.New("flush failed")
	failing := ExemptFromSampler(core, &syncFailingCore{Core: core, err: syncErr})
	require.ErrorIs(t, failing.Sync(), syncErr)
}

type syncFailingCore struct {
	zapcore.Core
	err error
}

func (c *syncFailingCore) Sync() error { return c.err }
