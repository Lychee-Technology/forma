package internal

import (
	"errors"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/transform"
)

// recordingCloser counts Close calls and returns a configured error.
type recordingCloser struct {
	closed int
	err    error
}

func (r *recordingCloser) Close() error {
	r.closed++
	return r.err
}

// newTestEntityManagerForClose builds a manager the same way
// TestEntityManager_Create does (entity_manager_test.go): same registry,
// transformer, mock repository, and default test config, with opts appended.
func newTestEntityManagerForClose(t *testing.T, opts ...EntityManagerOption) forma.EntityManager {
	t.Helper()

	config := createTestConfig()
	registry, err := newFileSchemaRegistryFromDir("../cmd/server/schemas")
	if err != nil {
		t.Fatalf("failed to create schema registry: %v", err)
	}
	transformer := transform.NewPersistentRecordTransformer(registry)
	mockRepo := newMockPersistentRecordRepository()

	return NewEntityManager(transformer, mockRepo, nil, registry, config, nil, opts...)
}

// TestEntityManagerClose_ClosesRegisteredResourcesOnce pins the lifecycle
// contract #302 adds: Close releases every registered resource, and a second
// Close is a no-op rather than a double-close.
func TestEntityManagerClose_ClosesRegisteredResourcesOnce(t *testing.T) {
	first := &recordingCloser{}
	second := &recordingCloser{}
	em := newTestEntityManagerForClose(t, WithCloser(first), WithCloser(second))

	if err := em.Close(); err != nil {
		t.Fatalf("Close with healthy resources: %v", err)
	}
	if first.closed != 1 || second.closed != 1 {
		t.Fatalf("expected each closer called once, got first=%d second=%d", first.closed, second.closed)
	}
	if err := em.Close(); err != nil {
		t.Fatalf("second Close must be a no-op, got %v", err)
	}
	if first.closed != 1 || second.closed != 1 {
		t.Fatalf("second Close must not re-close resources, got first=%d second=%d", first.closed, second.closed)
	}
}

// TestEntityManagerClose_JoinsResourceErrors pins that a failing resource
// does not mask its siblings: every closer runs, and the error surfaces.
func TestEntityManagerClose_JoinsResourceErrors(t *testing.T) {
	boom := errors.New("duckdb close failed")
	failing := &recordingCloser{err: boom}
	healthy := &recordingCloser{}
	em := newTestEntityManagerForClose(t, WithCloser(failing), WithCloser(healthy))

	err := em.Close()
	if !errors.Is(err, boom) {
		t.Fatalf("expected Close error to wrap the resource failure, got %v", err)
	}
	if healthy.closed != 1 {
		t.Fatalf("a sibling failure must not skip healthy closers, got %d", healthy.closed)
	}
}

// TestEntityManagerClose_NoResourcesIsNil pins that managers built without
// owned resources (the harness path) Close to nil.
func TestEntityManagerClose_NoResourcesIsNil(t *testing.T) {
	em := newTestEntityManagerForClose(t)
	if err := em.Close(); err != nil {
		t.Fatalf("Close with no registered resources: %v", err)
	}
}
