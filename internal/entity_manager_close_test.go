package internal

import (
	"errors"
	"strings"
	"sync"
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
// TestEntityManager_Create does (entity_crud_service_test.go): same registry,
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

	return mustNewEntityManager(t, transformer, mockRepo, nil, registry, config, nil, opts...)
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
	if !strings.Contains(err.Error(), "close entity manager resource") {
		t.Fatalf("close error must carry manager-level context (coding-standard §3), got %v", err)
	}
	if healthy.closed != 1 {
		t.Fatalf("a sibling failure must not skip healthy closers, got %d", healthy.closed)
	}
}

// TestEntityManagerClose_ConcurrentCallsCloseOnce pins that Close is safe for
// concurrent use (#327 review round 2): the public EntityManager surface
// invites concurrent teardown, teardown must run exactly once, and every
// caller — including ones that lose the race after a failure — receives the
// same cached result. Run under -race to catch unsynchronized teardown.
func TestEntityManagerClose_ConcurrentCallsCloseOnce(t *testing.T) {
	boom := errors.New("duckdb close failed")
	failing := &recordingCloser{err: boom}
	em := newTestEntityManagerForClose(t, WithCloser(failing))

	const callers = 32
	results := make(chan error, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- em.Close()
		}()
	}
	wg.Wait()
	close(results)

	if failing.closed != 1 {
		t.Fatalf("concurrent Close must run teardown exactly once, resource closed %d times", failing.closed)
	}
	for err := range results {
		if !errors.Is(err, boom) {
			t.Fatalf("every caller must receive the same cached close result, got %v", err)
		}
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
