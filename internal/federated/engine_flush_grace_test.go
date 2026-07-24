package federated

import (
	"regexp"
	"testing"
	"time"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"github.com/stretchr/testify/require"
)

// flushGraceCutoffLiteral matches the #252 per-request cutoff spliced into
// bound SQL. Tests comparing SQL across separate engine requests must
// normalize it: each request stamps its own millisecond cutoff, so two
// otherwise-identical builds legitimately differ in exactly this literal
// (review P0 — the raw comparison was flaky across a millisecond boundary).
var flushGraceCutoffLiteral = regexp.MustCompile(`flushed_at >= \d+`)

func normalizeFlushGraceCutoff(sql string) string {
	return flushGraceCutoffLiteral.ReplaceAllString(sql, "flushed_at >= <cutoff>")
}

// Pins the #252 grace-knob semantics: the cutoff anchors at the query's
// path-resolution timestamp minus the configured clock-skew margin (zero =
// exact anchor, the default), and a negative margin disables the widening
// entirely (the always-false FlushGraceCutoffDisabled cutoff).
func TestFlushVisibilityGraceResolution(t *testing.T) {
	newEngine := func(cfg forma.DuckDBConfig, opts ...EngineOption) *DBFederatedQueryEngine {
		return NewDBFederatedQueryEngine(nil, nil, nil, nil, cfg, nil, "", opts...)
	}
	const resolvedAt = int64(1_752_900_000_000)

	exact := newEngine(forma.DuckDBConfig{})
	require.Equal(t, resolvedAt, exact.flushGraceCutoffMs(resolvedAt),
		"zero margin must anchor the cutoff exactly at path resolution")

	margined := newEngine(forma.DuckDBConfig{FlushVisibilityGraceMs: 5000})
	require.Equal(t, resolvedAt-5000, margined.flushGraceCutoffMs(resolvedAt),
		"a positive margin subtracts from the path-resolution anchor")

	disabled := newEngine(forma.DuckDBConfig{FlushVisibilityGraceMs: -1})
	require.Equal(t, sqlgen.FlushGraceCutoffDisabled, disabled.flushGraceCutoffMs(resolvedAt),
		"disabled widening must emit the always-false cutoff")

	// The option overrides the config; a negative duration disables.
	overridden := newEngine(forma.DuckDBConfig{FlushVisibilityGraceMs: 5000},
		WithFlushVisibilityGrace(-time.Second))
	require.Equal(t, sqlgen.FlushGraceCutoffDisabled, overridden.flushGraceCutoffMs(resolvedAt))
	widened := newEngine(forma.DuckDBConfig{}, WithFlushVisibilityGrace(2*time.Minute))
	require.Equal(t, int64(120_000), widened.flushVisibilityGraceMs)
}
