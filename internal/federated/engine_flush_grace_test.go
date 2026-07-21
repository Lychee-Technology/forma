package federated

import (
	"testing"
	"time"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"github.com/stretchr/testify/require"
)

// Pins the #252 grace-knob semantics: config zero selects the built-in
// default, config negative disables, and the disabled engine emits the
// FlushGraceCutoffDisabled cutoff (the exact pre-#252 barrier) instead of a
// time-derived one.
func TestFlushVisibilityGraceResolution(t *testing.T) {
	require.Equal(t, defaultFlushVisibilityGraceMs, resolveFlushVisibilityGraceMs(0),
		"config zero must select the built-in default")
	require.Equal(t, int64(1234), resolveFlushVisibilityGraceMs(1234))
	require.Equal(t, int64(-1), resolveFlushVisibilityGraceMs(-1),
		"negative config must stay disabled")

	newEngine := func(cfg forma.DuckDBConfig, opts ...EngineOption) *DBFederatedQueryEngine {
		return NewDBFederatedQueryEngine(nil, nil, nil, nil, cfg, nil, "", opts...)
	}

	disabled := newEngine(forma.DuckDBConfig{FlushVisibilityGraceMs: -1})
	require.Equal(t, sqlgen.FlushGraceCutoffDisabled, disabled.flushGraceCutoffMs(),
		"disabled grace must emit the always-false cutoff, not now-minus-anything")

	graced := newEngine(forma.DuckDBConfig{FlushVisibilityGraceMs: 5000})
	cutoff := graced.flushGraceCutoffMs()
	now := time.Now().UnixMilli()
	require.InDelta(t, now-5000, cutoff, 1000,
		"cutoff must be query-start now minus the configured grace")

	// The option overrides the config, and d <= 0 disables (unlike config 0).
	overridden := newEngine(forma.DuckDBConfig{FlushVisibilityGraceMs: 5000},
		WithFlushVisibilityGrace(0))
	require.Equal(t, sqlgen.FlushGraceCutoffDisabled, overridden.flushGraceCutoffMs())
	widened := newEngine(forma.DuckDBConfig{}, WithFlushVisibilityGrace(2*time.Minute))
	require.Equal(t, int64(120_000), widened.flushVisibilityGraceMs)
}
