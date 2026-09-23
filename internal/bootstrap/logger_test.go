package bootstrap

import (
	"bytes"
	"encoding/json"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/lychee-technology/forma/internal/errorid"
	"github.com/lychee-technology/forma/internal/errorid/erroridtest"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// memorySink is a zap output the test can read back. zap resolves OutputPaths
// through a global scheme registry, so it is registered once per process.
type memorySink struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *memorySink) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}
func (s *memorySink) Sync() error  { return nil }
func (s *memorySink) Close() error { return nil }
func (s *memorySink) lines() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return strings.Split(strings.TrimSpace(s.buf.String()), "\n")
}

var (
	sink         = &memorySink{}
	registerSink sync.Once
)

// inMemoryProductionConfig is zap.NewProductionConfig — JSON encoding, Info
// threshold, the 100:100 sampler — with only its destination swapped for the
// sink, which it empties first.
func inMemoryProductionConfig(t *testing.T) zap.Config {
	t.Helper()
	registerSink.Do(func() {
		require.NoError(t, zap.RegisterSink("memory", func(*url.URL) (zap.Sink, error) { return sink, nil }))
	})
	sink.mu.Lock()
	sink.buf.Reset()
	sink.mu.Unlock()

	cfg := zap.NewProductionConfig()
	cfg.OutputPaths = []string{"memory://"}
	return cfg
}

// buildProductionLoggerInMemory is NewProductionLogger over the sink, on a
// frozen clock.
func buildProductionLoggerInMemory(t *testing.T) *zap.Logger {
	t.Helper()
	cfg := inMemoryProductionConfig(t)
	logger, err := BuildLogger(cfg, erroridtest.FrozenClock())
	require.NoError(t, err)
	return logger
}

// countMessages tallies the JSON lines on the sink by their msg field, and
// records the logger name each carried.
func countMessages(t *testing.T) (counts map[string]int, loggerNames map[string]string) {
	t.Helper()
	counts, loggerNames = map[string]int{}, map[string]string{}
	for _, line := range sink.lines() {
		var entry struct {
			Msg    string `json:"msg"`
			Logger string `json:"logger"`
		}
		require.NoError(t, json.Unmarshal([]byte(line), &entry), "not a JSON log line: %s", line)
		counts[entry.Msg]++
		loggerNames[entry.Msg] = entry.Logger
	}
	return counts, loggerNames
}

// TestProductionLoggerNeverSamplesACorrelationLine is the production half of
// the #398 contract: through the real production config, 150 identical
// failure lines written by errorid.Log inside one second all reach the
// output, while 150 identical lines from the plain logger are cut to the
// sampler's first 100 — so the sampler is live for everything else, and it
// is the exemption, not its absence, that keeps every issued id joinable.
func TestProductionLoggerNeverSamplesACorrelationLine(t *testing.T) {
	logger := buildProductionLoggerInMemory(t)
	restore := zap.ReplaceGlobals(logger)
	t.Cleanup(restore)

	for i := 0; i < 150; i++ {
		require.NotEmpty(t, errorid.Log(zap.WarnLevel, "BatchCreate operation failed"))
		zap.S().Warnw("some other warning", "i", i)
	}
	require.NoError(t, logger.Sync())

	counts, loggerNames := countMessages(t)
	require.Equal(t, 150, counts["BatchCreate operation failed"], "a correlation line is never sampled")
	require.Equal(t, 100, counts["some other warning"], "every other line keeps the production sampler")
	require.Equal(t, errorid.LoggerName, loggerNames["BatchCreate operation failed"],
		"the line names its logger so an operator can filter on it")
	require.Equal(t, "", loggerNames["some other warning"])
}

// TestBuildLoggerWithoutSamplingIsPlainBuild: a config with Sampling nil
// must build exactly as cfg.Build would — no wrapper, no sampler.
func TestBuildLoggerWithoutSamplingIsPlainBuild(t *testing.T) {
	cfg := inMemoryProductionConfig(t)
	cfg.Sampling = nil

	logger, err := BuildLogger(cfg)
	require.NoError(t, err)
	for i := 0; i < 150; i++ {
		logger.Warn("unsampled")
	}
	require.NoError(t, logger.Sync())
	counts, _ := countMessages(t)
	require.Equal(t, 150, counts["unsampled"])
}

// TestBuildLoggerKeepsProductionThreshold: the exemption must not widen what
// is logged. Info is the production threshold and Debug stays below it on
// both sides of the wrapper.
func TestBuildLoggerKeepsProductionThreshold(t *testing.T) {
	logger := buildProductionLoggerInMemory(t)
	require.Equal(t, zapcore.InfoLevel, logger.Level())
	require.False(t, logger.Named(errorid.LoggerName).Core().Enabled(zapcore.DebugLevel))
	require.True(t, logger.Named(errorid.LoggerName).Core().Enabled(zapcore.InfoLevel))
}

// TestBuildLoggerReportsAnUnbuildableConfig pins the wrapped error.
func TestBuildLoggerReportsAnUnbuildableConfig(t *testing.T) {
	cfg := zap.NewProductionConfig()
	cfg.Encoding = "no-such-encoding"
	_, err := BuildLogger(cfg)
	require.ErrorContains(t, err, "failed to build logger")
}
