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
	t.Cleanup(zap.ReplaceGlobals(zap.New(core, erroridtest.FrozenClock(), errorid.SamplerOption(cfg))))

	logNTimes(zap.S(), 3, "uncorrelated")
	logNTimes(errorid.Logger(), 3, "correlated")

	require.Equal(t, 2, dropped, "the hook sees the sampled side's drops and none from the exempt side")
}

// TestSamplerOptionExemptsOnlyTheCorrelationLogger pins what the exemption
// keys on. It is Logger's marker, not a name: under any global name the
// embedder chose — none, "svc", "a.b" — Logger's lines clear the sampler,
// while a host logger that happens to use Forma's label, "errorid" on its
// own or as the last segment, is sampled like any other (PR #606 review: a
// name-keyed exemption let such host lines bypass the volume bound). The
// sampler here keeps one line in a hundred, so two writes per logger tell
// the two routes apart.
func TestSamplerOptionExemptsOnlyTheCorrelationLogger(t *testing.T) {
	for _, global := range []string{"", "svc", "a.b"} {
		t.Run("global="+global, func(t *testing.T) {
			core, logs := observer.New(zap.InfoLevel)
			root := zap.New(core, erroridtest.FrozenClock(),
				errorid.SamplerOption(&zap.SamplingConfig{Initial: 1, Thereafter: 100})).Named(global)
			t.Cleanup(zap.ReplaceGlobals(root))

			logNTimes(errorid.Logger(), 2, "correlated")
			logNTimes(zap.S().Named(errorid.LoggerName), 2, "host logger under the label")
			logNTimes(zap.S().Named("svc").Named(errorid.LoggerName), 2, "host logger under the label as last segment")
			logNTimes(errorid.Logger().Named("child").With("k", "v"), 2, "child of the correlation logger")

			require.Len(t, logs.FilterMessage("correlated").All(), 2)
			require.Len(t, logs.FilterMessage("host logger under the label").All(), 1)
			require.Len(t, logs.FilterMessage("host logger under the label as last segment").All(), 1)
			require.Len(t, logs.FilterMessage("child of the correlation logger").All(), 2)
		})
	}
}

// TestSamplerOptionStripsTheMarker: the marker is how the exemption
// recognises Logger, not part of the line, so the core beneath never
// receives it — only the fields the caller logged.
func TestSamplerOptionStripsTheMarker(t *testing.T) {
	logs := erroridtest.ObserveUnderProductionSampler(t, zap.InfoLevel)

	errorid.Logger().Warnw("correlated", "k", "v")

	entries := logs.All()
	require.Len(t, entries, 1)
	require.Equal(t, []zapcore.Field{zap.String("k", "v")}, entries[0].Context)
}

// TestSamplerOptionSurvivesWith pins that a global built with fields (zap's
// With) keeps the routing: the wrapper must wrap both sides again rather
// than collapse to one of them, so the marker still finds it.
func TestSamplerOptionSurvivesWith(t *testing.T) {
	core, logs := observer.New(zap.InfoLevel)
	t.Cleanup(zap.ReplaceGlobals(zap.New(core, erroridtest.FrozenClock(),
		errorid.SamplerOption(zap.NewProductionConfig().Sampling)).With(zap.String("component", "test"))))

	logNTimes(errorid.Logger(), 150, "correlated")
	logNTimes(zap.S(), 150, "uncorrelated")

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
