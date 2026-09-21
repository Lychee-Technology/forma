// Package erroridtest installs production-equivalent log sampling in a test
// so it can prove that a line carrying an error_id survives it, and pins the
// clock so the proof does not depend on wall-clock timing.
package erroridtest

import (
	"testing"
	"time"

	"github.com/lychee-technology/forma/internal/errorid"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// frozenClock pins every entry to one instant so all of a test's lines fall
// inside a single sampler tick, whatever the wall clock does meanwhile. A
// test that relied on the real clock could straddle a tick boundary, split
// its lines into groups the sampler never cuts, and pass with the exemption
// gone.
type frozenClock struct{ at time.Time }

func (c frozenClock) Now() time.Time                       { return c.at }
func (c frozenClock) NewTicker(time.Duration) *time.Ticker { return time.NewTicker(time.Hour) }

// FrozenClock is the zap option that installs the pinned clock.
func FrozenClock() zap.Option {
	return zap.WithClock(frozenClock{at: time.Date(2026, 9, 21, 0, 0, 0, 0, time.UTC)})
}

// ObserveUnderProductionSampler replaces the global logger for the rest of the
// test with an observer that sits under the sampler zap.NewProductionConfig
// installs (100 identical level-and-message entries per second, then every
// 100th), wrapped exactly as every production logger wraps it, on a frozen
// clock. The returned logs see every line that survives the sampler.
func ObserveUnderProductionSampler(t testing.TB, level zapcore.LevelEnabler) *observer.ObservedLogs {
	t.Helper()
	core, logs := observer.New(level)
	logger := zap.New(core, FrozenClock(), errorid.SamplerOption(zap.NewProductionConfig().Sampling))
	t.Cleanup(zap.ReplaceGlobals(logger))
	return logs
}
