package cdc

import (
	"testing"

	"go.uber.org/zap"
)

// A zero-value CDCConfig must not silently disable the init export: without
// defaults, BatchSize=0 sizes every export snapshot at zero rows and RunInit
// would report success after exporting nothing.
func TestNormalizeInitOptions_ZeroValueConfigGetsUsableBatchSize(t *testing.T) {
	opts := normalizeInitOptions(InitOptions{})

	if opts.Config.BatchSize <= 0 {
		t.Fatalf("normalized BatchSize = %d, want > 0", opts.Config.BatchSize)
	}
	if opts.Logger == nil {
		t.Fatal("normalized Logger is nil")
	}

	runCtx := &initRunContext{cfg: opts.Config, logger: opts.Logger}
	if got := resolveInitBatchSize(runCtx, 1, nil); got <= 0 {
		t.Fatalf("resolveInitBatchSize with defaulted config = %d, want > 0", got)
	}
}

func TestResolveInitBatchSize_RawZeroValueConfigIsZero(t *testing.T) {
	// Documents the trap normalizeInitOptions exists to prevent.
	runCtx := &initRunContext{cfg: CDCConfig{}, logger: zap.NewNop()}
	if got := resolveInitBatchSize(runCtx, 1, nil); got != 0 {
		t.Fatalf("resolveInitBatchSize with raw zero-value config = %d, want 0", got)
	}
}
