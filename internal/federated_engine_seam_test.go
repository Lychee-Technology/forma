package internal

import (
	"context"
	"testing"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/assert"
)

// wrappedEngine is a federated engine wrapper that remembers what it wraps.
type wrappedEngine struct {
	countingEngine
	inner model.FederatedQueryEngine
}

func TestWrapFederatedEngineWithoutAWrapperAnswersTheEngine(t *testing.T) {
	engine := &countingEngine{}
	assert.Same(t, engine, WrapFederatedEngine(context.Background(), engine))
}

func TestWrapFederatedEngineAppliesTheContextsWrapper(t *testing.T) {
	engine := &countingEngine{}
	ctx := WithFederatedEngineWrapper(context.Background(), func(inner model.FederatedQueryEngine) model.FederatedQueryEngine {
		return &wrappedEngine{inner: inner}
	})
	wrapped, ok := WrapFederatedEngine(ctx, engine).(*wrappedEngine)
	if assert.True(t, ok, "the manager gets the wrapper's engine") {
		assert.Same(t, engine, wrapped.inner, "the wrapper receives the factory's engine")
	}
}
