package internal

import (
	"context"

	"github.com/lychee-technology/forma/internal/model"
)

// federatedEngineWrapperKey is the context key of a federated engine
// wrapper.
type federatedEngineWrapperKey struct{}

// FederatedEngineWrapper replaces the federated engine a factory built with
// one that wraps it.
type FederatedEngineWrapper func(model.FederatedQueryEngine) model.FederatedQueryEngine

// WithFederatedEngineWrapper answers ctx carrying wrap. A manager the factory
// builds with that ctx uses wrap's result as its federated engine. It is a
// test seam: the coordination suite (#654) builds its manager through the
// public factory and counts the engine's calls, to show that no Tx path
// reaches it. Being internal, only this module can use it.
func WithFederatedEngineWrapper(ctx context.Context, wrap FederatedEngineWrapper) context.Context {
	return context.WithValue(ctx, federatedEngineWrapperKey{}, wrap)
}

// WrapFederatedEngine answers engine wrapped by the wrapper ctx carries, or
// engine itself when ctx carries none.
func WrapFederatedEngine(ctx context.Context, engine model.FederatedQueryEngine) model.FederatedQueryEngine {
	wrap, ok := ctx.Value(federatedEngineWrapperKey{}).(FederatedEngineWrapper)
	if !ok || wrap == nil {
		return engine
	}
	return wrap(engine)
}
