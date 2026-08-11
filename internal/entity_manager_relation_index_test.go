package internal

import (
	"context"
	"errors"
	"testing"

	"github.com/lychee-technology/forma/internal/schemavalidate"
	"github.com/lychee-technology/forma/internal/transform"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// TestNewEntityManagerUsesTheSuppliedRelationIndex pins the #318 review's
// build-once wiring: an index handed in by the composition root is the index the
// manager strips with, and the manager does not go back to the registry for a
// second, possibly different one.
//
// The registry here serves its child document exactly once, then fails — which
// is what a registry backed by a database or a network can do at any moment, and
// is why two independent loads were a hazard rather than a tidiness question.
// Under the self-load the second read fails, NewEntityManager warns and
// continues with a nil index, and stripping is silently off for the life of the
// process even though the preflight passed.
//
// Observed structurally rather than through the error text. relation_ok_not
// says {"not": {"required": ["contactSnapshot"]}}, so a payload that still
// carries the relation root fails validation and never reaches the transformer:
// the spy sees a write only if the root was stripped first.
func TestNewEntityManagerUsesTheSuppliedRelationIndex(t *testing.T) {
	registry := serveRelationFixture(t, "relation_ok_not")
	idx, err := LoadRelationIndex(registry)
	require.NoError(t, err)
	require.Len(t, idx.Relations("child"), 1)

	registry.docErr["child"] = errors.New("backing store unavailable")

	validator, err := schemavalidate.New(serveRelationFixture(t, "relation_ok_not"),
		resolveRelationFixtureDir("relation_ok_not"))
	require.NoError(t, err)

	config := createTestConfig()
	config.Entity.SchemaDirectory = resolveRelationFixtureDir("relation_ok_not")
	spy := &writeSpy{inner: transform.NewPersistentRecordTransformer(registry)}
	manager := NewEntityManager(spy, newMockPersistentRecordRepository(), nil,
		registry, config, validator, WithRelationIndex(idx))

	_, _ = manager.Create(context.Background(), &forma.EntityOperation{
		Type:             forma.OperationCreate,
		EntityIdentifier: forma.EntityIdentifier{SchemaName: "child"},
		Data:             map[string]any{"id": "x", "parentId": "p", "contactSnapshot": map[string]any{"name": "Ada"}},
	})

	require.Len(t, spy.seen, 1,
		"the payload must have passed validation, which it can only do once the relation root is stripped")
	require.NotContains(t, spy.seen[0].keys, "contactSnapshot")
}

// TestWithRelationIndexIgnoresNil keeps the option from being a way to turn
// stripping off by accident. An option handed nothing does nothing, and the
// manager falls back to its own load — the behaviour every direct constructor
// already relies on.
func TestWithRelationIndexIgnoresNil(t *testing.T) {
	registry := serveRelationFixture(t, "relation_ok_not")
	validator, err := schemavalidate.New(registry, resolveRelationFixtureDir("relation_ok_not"))
	require.NoError(t, err)

	config := createTestConfig()
	config.Entity.SchemaDirectory = resolveRelationFixtureDir("relation_ok_not")
	spy := &writeSpy{inner: transform.NewPersistentRecordTransformer(registry)}
	manager := NewEntityManager(spy, newMockPersistentRecordRepository(), nil,
		registry, config, validator, WithRelationIndex(nil))

	_, _ = manager.Create(context.Background(), &forma.EntityOperation{
		Type:             forma.OperationCreate,
		EntityIdentifier: forma.EntityIdentifier{SchemaName: "child"},
		Data:             map[string]any{"id": "x", "parentId": "p", "contactSnapshot": map[string]any{"name": "Ada"}},
	})

	require.Len(t, spy.seen, 1, "the self-load must still have produced a stripping index")
	require.NotContains(t, spy.seen[0].keys, "contactSnapshot")
}
