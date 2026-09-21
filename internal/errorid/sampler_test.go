package errorid_test

import (
	"errors"
	"testing"
	"time"

	"github.com/lychee-technology/forma/internal/errorid"
	"github.com/lychee-technology/forma/internal/errorid/erroridtest"
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

// TestSamplerOptionKeepsEveryCorrelationLine is the invariant the exemption
// exists for: 150 identical lines from Logger inside one second all reach the
// core, while the same 150 from the plain global logger are cut to the
// sampler's first 100 — proof the sampler is live and the exemption is what
// saved the correlation lines, not its absence.
func TestSamplerOptionKeepsEveryCorrelationLine(t *testing.T) {
	logs := erroridtest.ObserveUnderProductionSampler(t, zap.InfoLevel)

	logNTimes(errorid.Logger(), 150, "correlated")
	logNTimes(zap.S(), 150, "uncorrelated")

	require.Len(t, logs.FilterMessage("correlated").All(), 150, "a correlation line is never sampled")
	require.Len(t, logs.FilterMessage("uncorrelated").All(), 100, "every other line still is")
	for _, entry := range logs.FilterMessage("correlated").All() {
		require.Equal(t, errorid.LoggerName, entry.LoggerName)
	}
}

// TestSamplerOptionHonoursTheHook: a sampling hook an embedder configured
// must still fire for the sampled side, since SamplerOption stands in for
// zap.Config.Build's own sampler installation.
func TestSamplerOptionHonoursTheHook(t *testing.T) {
	core, _ := observer.New(zap.InfoLevel)
	dropped := 0
	cfg := &zap.SamplingConfig{Initial: 1, Thereafter: 0, Hook: func(_ zapcore.Entry, d zapcore.SamplingDecision) {
		if d == zapcore.LogDropped {
			dropped++
		}
	}}
	logger := zap.New(core, erroridtest.FrozenClock(), errorid.SamplerOption(cfg))

	logNTimes(logger.Sugar(), 3, "uncorrelated")
	logNTimes(logger.Sugar().Named(errorid.LoggerName), 3, "correlated")

	require.Equal(t, 2, dropped, "the hook sees the sampled side's drops and none from the exempt side")
}

// TestExemptFromSamplerSurvivesWith pins that a child logger built with
// fields (zap's With) keeps the routing: the wrapper must wrap both sides
// again rather than collapse to one of them.
func TestExemptFromSamplerSurvivesWith(t *testing.T) {
	core, logs := observer.New(zap.InfoLevel)
	logger := zap.New(errorid.ExemptFromSampler(productionSampled(core), core), erroridtest.FrozenClock()).
		With(zap.String("component", "test"))

	logNTimes(logger.Sugar().Named(errorid.LoggerName), 150, "correlated")
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
	wrapped := errorid.ExemptFromSampler(productionSampled(core), core)
	require.Equal(t, zap.WarnLevel, zapcore.LevelOf(wrapped))
	require.False(t, wrapped.Enabled(zap.InfoLevel))
	require.True(t, wrapped.Enabled(zap.WarnLevel))

	syncErr := errors.New("flush failed")
	failing := errorid.ExemptFromSampler(core, &syncFailingCore{Core: core, err: syncErr})
	require.ErrorIs(t, failing.Sync(), syncErr)
}

type syncFailingCore struct {
	zapcore.Core
	err error
}

func (c *syncFailingCore) Sync() error { return c.err }
