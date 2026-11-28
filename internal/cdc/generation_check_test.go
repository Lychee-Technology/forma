package cdc

import (
	"context"
	"errors"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/schemaledger"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// trackingRegistry is a gated registry that started on generation 1 of
// schema 7; apply moves the active generation, as apply-schema would.
type trackingRegistry struct {
	forma.SchemaRegistry
	active int64
}

func (r *trackingRegistry) GetSchemaAttributeCacheByID(int16) (string, forma.SchemaAttributeCache, error) {
	return "orders", derivedAncestry(forma.SchemaAttributeCache{
		"qty": {AttributeName: "qty", AttributeID: 1, ValueType: forma.ValueTypeSmallInt},
	}), nil
}

func (r *trackingRegistry) LoadedGeneration(schemaID int16) (int64, bool) { return 1, schemaID == 7 }

func (r *trackingRegistry) ActiveGeneration(context.Context, int16) (int64, bool, error) {
	return r.active, true, nil
}

func (r *trackingRegistry) apply() { r.active++ }

var _ schemaledger.Tracker = (*trackingRegistry)(nil)

// TestRunPreflightRefusesASupersededGeneration: a long-lived process whose
// registry started on generation 1 does not start a run once another
// generation is active; a registry with no ledger is not checked.
func TestRunPreflightRefusesASupersededGeneration(t *testing.T) {
	ctx := context.Background()
	registry := &trackingRegistry{active: 1}
	_, err := resolveRunAttrCache(ctx, registry, 7)
	require.NoError(t, err)

	registry.apply()
	_, err = resolveRunAttrCache(ctx, registry, 7)
	require.ErrorIs(t, err, schemaledger.ErrGenerationChanged)

	runCtx := &initRunContext{schemaRegistry: registry, logger: zap.NewNop()}
	_, err = processInitSchemas(ctx, runCtx, []int64{7})
	require.ErrorIs(t, err, schemaledger.ErrGenerationChanged)
	flushCtx := &schemaFlushContext{schemaRegistry: registry, logger: zap.NewNop(),
		processSchemaFn: func(context.Context, int16) error { return errors.New("must not run") }}
	_, err = flushCtx.processSchemas(ctx, []int64{7})
	require.ErrorIs(t, err, schemaledger.ErrGenerationChanged)

	untracked := struct{ forma.SchemaRegistry }{registry}
	_, err = resolveRunAttrCache(ctx, untracked, 7)
	require.NoError(t, err, "a registry that is not a Tracker has no ledger to check")
}
