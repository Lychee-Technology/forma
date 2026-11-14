package federated

import (
	"testing"
	"time"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// Pins the #252 grace-knob semantics: the cutoff anchors at the pass's
// path-resolution timestamp minus the configured clock-skew margin (zero =
// exact anchor, the default). A negative margin reads as 0: the widening is
// never disabled (TestPgxPassCutoffNeverDisablesTheWidening).
func TestFlushVisibilityGraceResolution(t *testing.T) {
	newEngine := func(cfg forma.DuckDBConfig, opts ...EngineOption) *DBFederatedQueryEngine {
		return NewDBFederatedQueryEngine(nil, nil, nil, cfg, nil, opts...)
	}
	const resolvedAt = int64(1_752_900_000_000)

	exact := newEngine(forma.DuckDBConfig{})
	require.Equal(t, resolvedAt, exact.passCutoffMs(resolvedAt),
		"zero margin must anchor the cutoff exactly at path resolution")

	margined := newEngine(forma.DuckDBConfig{FlushVisibilityGraceMs: 5000})
	require.Equal(t, resolvedAt-5000, margined.passCutoffMs(resolvedAt),
		"a positive margin subtracts from the path-resolution anchor")

	// The option overrides the config.
	overridden := newEngine(forma.DuckDBConfig{FlushVisibilityGraceMs: 5000},
		WithFlushVisibilityGrace(2*time.Second))
	require.Equal(t, resolvedAt-2000, overridden.passCutoffMs(resolvedAt))
	widened := newEngine(forma.DuckDBConfig{}, WithFlushVisibilityGrace(2*time.Minute))
	require.Equal(t, int64(120_000), widened.flushVisibilityGraceMs)
}
