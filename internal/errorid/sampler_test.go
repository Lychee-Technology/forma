package errorid_test

import (
	"errors"
	"testing"

	"github.com/lychee-technology/forma/internal/errorid"
	"github.com/lychee-technology/forma/internal/errorid/erroridtest"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

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

// TestSamplerOptionRoutesByTheLastNameSegment pins what the exemption keys
// on. Logger builds on the global logger with Named, and zap joins names with
// a period, so under a global the embedder already named the line arrives
// as "svc.errorid": that must still clear the sampler, or a named-global
// embedder gets ids whose lines were dropped. A child of the correlation
// logger or a name that merely ends in the letters is not one and stays
// sampled. The sampler here keeps one line in a hundred, so two writes per
// name tell the two routes apart.
func TestSamplerOptionRoutesByTheLastNameSegment(t *testing.T) {
	cases := []struct {
		name   string
		exempt bool
	}{
		{errorid.LoggerName, true},
		{"svc." + errorid.LoggerName, true},
		{"a.b." + errorid.LoggerName, true},
		{"", false},
		{"svc", false},
		{"my" + errorid.LoggerName, false},
		{errorid.LoggerName + ".child", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.exempt, errorid.IsCorrelationLogger(tc.name))

			core, logs := observer.New(zap.InfoLevel)
			logger := zap.New(core, erroridtest.FrozenClock(),
				errorid.SamplerOption(&zap.SamplingConfig{Initial: 1, Thereafter: 100})).Named(tc.name)
			logNTimes(logger.Sugar(), 2, "line")

			want := 1
			if tc.exempt {
				want = 2
			}
			require.Len(t, logs.All(), want)
		})
	}
}

// TestSamplerOptionSurvivesWith pins that a child logger built with fields
// (zap's With) keeps the routing: the wrapper must wrap both sides again
// rather than collapse to one of them.
func TestSamplerOptionSurvivesWith(t *testing.T) {
	core, logs := observer.New(zap.InfoLevel)
	logger := zap.New(core, erroridtest.FrozenClock(), errorid.SamplerOption(zap.NewProductionConfig().Sampling)).
		With(zap.String("component", "test"))

	logNTimes(logger.Sugar().Named(errorid.LoggerName), 150, "correlated")
	logNTimes(logger.Sugar(), 150, "uncorrelated")

	require.Len(t, logs.FilterMessage("correlated").All(), 150)
	require.Len(t, logs.FilterMessage("uncorrelated").All(), 100)
	require.Equal(t, "test", logs.FilterMessage("correlated").All()[0].ContextMap()["component"],
		"the With fields reach the unsampled side too")
}

// TestSamplerOptionLevelAndSync: zapcore.LevelOf must see the wrapped level,
// and one logger.Sync must reach the sink exactly once — the sampler and
// the exemption both sit over the same core, and zapcore.Core does not
// promise a second Sync is harmless — and report its failure.
func TestSamplerOptionLevelAndSync(t *testing.T) {
	core, _ := observer.New(zap.WarnLevel)
	sink := &countingCore{Core: core, err: errors.New("flush failed")}
	logger := zap.New(sink, errorid.SamplerOption(zap.NewProductionConfig().Sampling))

	require.Equal(t, zap.WarnLevel, zapcore.LevelOf(logger.Core()))
	require.False(t, logger.Core().Enabled(zap.InfoLevel))
	require.True(t, logger.Core().Enabled(zap.WarnLevel))

	require.ErrorIs(t, logger.Sync(), sink.err)
	require.Equal(t, 1, sink.syncs, "one Sync of the logger is one Sync of the sink")
}

type countingCore struct {
	zapcore.Core
	syncs int
	err   error
}

func (c *countingCore) Sync() error {
	c.syncs++
	return c.err
}
