package internal

import (
	"context"
	"errors"
	"testing"

	"github.com/lychee-technology/forma/internal/transform"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

// #554 pins at the service seam for atomic BatchUpdate: the merge base is
// whatever the repository reads under its lock, and the answer is what the
// repository stored — the two properties #553 gave single-row Update.

func atomicUpdateOp(rowID uuid.UUID, updates map[string]any) forma.EntityOperation {
	return forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: "test", RowID: rowID},
		Type:             forma.OperationUpdate,
		Updates:          updates,
	}
}

func TestEntityManager_BatchUpdate_AtomicMergeBaseComesFromTheWriteTransaction(t *testing.T) {
	ctx := context.Background()
	registry := newStubSchemaRegistry()
	transformer := transform.NewPersistentRecordTransformer(registry)
	mockRepo := newMockPersistentRecordRepository()

	schemaID, _, err := registry.GetSchemaAttributeCacheByName("test")
	if err != nil {
		t.Fatalf("failed to get schema metadata: %v", err)
	}

	rowA, rowB := uuid.New(), uuid.New()
	mockRepo.storeRecord(buildPersistentRecord(t, transformer, schemaID, rowA, map[string]any{"name": "Alice", "age": 30}))
	mockRepo.storeRecord(buildPersistentRecord(t, transformer, schemaID, rowB, map[string]any{"name": "Bob", "age": 40}))
	// A concurrent committer lands between the caller's request and the
	// locked read; the batch must merge onto THIS state, not a pre-read one.
	mockRepo.beforeMerge = func() {
		mockRepo.storeRecord(buildPersistentRecord(t, transformer, schemaID, rowA, map[string]any{"name": "Alicia", "age": 30}))
		mockRepo.storeRecord(buildPersistentRecord(t, transformer, schemaID, rowB, map[string]any{"name": "Robert", "age": 40}))
	}

	em := mustNewEntityManager(t, transformer, mockRepo, nil, registry, createTestConfig(), nil)
	result, err := em.BatchUpdate(ctx, &forma.BatchOperation{
		Atomic: true,
		Operations: []forma.EntityOperation{
			atomicUpdateOp(rowA, map[string]any{"age": 31}),
			atomicUpdateOp(rowB, map[string]any{"age": 41}),
		},
	})
	if err != nil {
		t.Fatalf("BatchUpdate failed: %v", err)
	}
	if mockRepo.getCalls != 0 {
		t.Fatalf("expected no pool-side read outside the write transaction, got %d", mockRepo.getCalls)
	}
	if got := result.Successful[0].Attributes["name"]; got != "Alicia" {
		t.Fatalf("operation[0]: expected the merge base read at merge time (name=Alicia), got %v", got)
	}
	if got := result.Successful[1].Attributes["name"]; got != "Robert" {
		t.Fatalf("operation[1]: expected the merge base read at merge time (name=Robert), got %v", got)
	}
}

func TestEntityManager_BatchUpdate_AtomicAnswersStoredVersion(t *testing.T) {
	ctx := context.Background()
	// The stub schema plus a read-only alias of ltbase_updated_at, so the
	// answered document carries the row's version.
	stub := newStubSchemaRegistry().(*stubSchemaRegistry)
	stub.cache["version"] = forma.AttributeMetadata{
		AttributeID:   8,
		ValueType:     forma.ValueTypeNumeric,
		ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnUpdatedAt},
	}
	registry := forma.SchemaRegistry(stub)
	transformer := transform.NewPersistentRecordTransformer(registry)
	mockRepo := newMockPersistentRecordRepository()

	schemaID, _, err := registry.GetSchemaAttributeCacheByName("test")
	if err != nil {
		t.Fatalf("failed to get schema metadata: %v", err)
	}

	rowID := uuid.New()
	seed := buildPersistentRecord(t, transformer, schemaID, rowID, map[string]any{"name": "Alice", "age": 30})
	seed.UpdatedAt = 1_000
	mockRepo.storeRecord(seed)

	em := mustNewEntityManager(t, transformer, mockRepo, nil, registry, createTestConfig(), nil)
	result, err := em.BatchUpdate(ctx, &forma.BatchOperation{
		Atomic:     true,
		Operations: []forma.EntityOperation{atomicUpdateOp(rowID, map[string]any{"age": 31})},
	})
	if err != nil {
		t.Fatalf("BatchUpdate failed: %v", err)
	}

	// The mock repository stamps the stored version from the row's previous
	// one (standing in for GREATEST(now, prev + 1)); the service could not
	// have computed it, so an echo of the pre-write merge cannot pass.
	stored := mockRepo.records[schemaID][rowID]
	if stored.UpdatedAt != 1_001 {
		t.Fatalf("mock stored version = %d, want 1001", stored.UpdatedAt)
	}
	got, ok := result.Successful[0].Attributes["version"].(float64)
	if !ok || got != float64(stored.UpdatedAt) {
		t.Fatalf("answered version = %v, want the stored ltbase_updated_at %d", result.Successful[0].Attributes["version"], stored.UpdatedAt)
	}
	if age, ok := result.Successful[0].Attributes["age"].(float64); !ok || age != 31 {
		t.Fatalf("expected the storage round-trip value float64(31), got %T(%v)", result.Successful[0].Attributes["age"], result.Successful[0].Attributes["age"])
	}
}

func TestEntityManager_BatchUpdate_AtomicMissingRowIsNotFoundNamingTheOperation(t *testing.T) {
	ctx := context.Background()
	registry := newStubSchemaRegistry()
	transformer := transform.NewPersistentRecordTransformer(registry)
	mockRepo := newMockPersistentRecordRepository()

	schemaID, _, err := registry.GetSchemaAttributeCacheByName("test")
	if err != nil {
		t.Fatalf("failed to get schema metadata: %v", err)
	}
	present, missing := uuid.New(), uuid.New()
	seed := buildPersistentRecord(t, transformer, schemaID, present, map[string]any{"name": "Alice", "age": 30})
	mockRepo.storeRecord(seed)

	em := mustNewEntityManager(t, transformer, mockRepo, nil, registry, createTestConfig(), nil)
	_, err = em.BatchUpdate(ctx, &forma.BatchOperation{
		Atomic: true,
		Operations: []forma.EntityOperation{
			atomicUpdateOp(present, map[string]any{"age": 31}),
			atomicUpdateOp(missing, map[string]any{"age": 41}),
		},
	})
	if !errors.Is(err, forma.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	msg, ok := forma.ResolvePublicMessage(err)
	if !ok || msg != "operation[1]: entity not found: test/"+missing.String() {
		t.Fatalf("published message = %q (ok=%v)", msg, ok)
	}
	// All or nothing: the present row must still be the seed, not a merge.
	if got := mockRepo.records[schemaID][present]; got != seed {
		t.Fatalf("the failed batch must leave the present row untouched, got %+v", got)
	}
}
