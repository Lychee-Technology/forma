package internal

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/transform"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

// duplicateRowUpdateOps builds two update operations on the same visit row that
// touch disjoint attributes, so a correct merge keeps both and a lost update is
// observable.
func duplicateRowUpdateOps(rowID uuid.UUID) []forma.EntityOperation {
	return []forma.EntityOperation{
		{
			EntityIdentifier: forma.EntityIdentifier{SchemaName: "visit", RowID: rowID},
			Type:             forma.OperationUpdate,
			Updates:          map[string]any{"status": "visited"},
		},
		{
			EntityIdentifier: forma.EntityIdentifier{SchemaName: "visit", RowID: rowID},
			Type:             forma.OperationUpdate,
			Updates:          map[string]any{"feedback": "great visit"},
		},
	}
}

// seedVisitRow stores one visit row in a fresh mock repository and returns the
// wired entity manager together with the pieces a test needs to inspect it.
func seedVisitRow(t *testing.T, id string) (forma.EntityManager, *mockPersistentRecordRepository, model.PersistentRecordTransformer, int16, uuid.UUID) {
	t.Helper()
	registry, err := newFileSchemaRegistryFromDir("../cmd/server/schemas")
	if err != nil {
		t.Fatalf("failed to create schema registry: %v", err)
	}
	transformer := transform.NewPersistentRecordTransformer(registry)
	mockRepo := newMockPersistentRecordRepository()

	schemaID, _, err := registry.GetSchemaAttributeCacheByName("visit")
	if err != nil {
		t.Fatalf("failed to get schema metadata: %v", err)
	}

	rowID := uuid.New()
	mockRepo.storeRecord(buildPersistentRecord(t, transformer, schemaID, rowID, visitPayload(id)))

	em := mustNewEntityManager(t, transformer, mockRepo, nil, registry, createTestConfig(), nil)
	return em, mockRepo, transformer, schemaID, rowID
}

func TestEntityManager_BatchUpdate_AtomicRejectsDuplicateRowID(t *testing.T) {
	ctx := context.Background()
	em, mockRepo, transformer, schemaID, rowID := seedVisitRow(t, "visit-atomic-dup")

	result, err := em.BatchUpdate(ctx, &forma.BatchOperation{
		Atomic:     true,
		Operations: duplicateRowUpdateOps(rowID),
	})
	if err == nil {
		t.Fatalf("expected atomic batch with a duplicate row id to be rejected, got result %+v", result)
	}
	if !errors.Is(err, forma.ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput, got %v", err)
	}
	msg, ok := forma.ResolvePublicMessage(err)
	if !ok {
		t.Fatalf("expected a published message, got %v", err)
	}
	for _, want := range []string{"operation[1]", "operation[0]", rowID.String(), "visit"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("expected published message to mention %q, got %q", want, msg)
		}
	}
	if mockRepo.getCalls != 0 || mockRepo.batchUpdateCalls != 0 {
		t.Fatalf("expected rejection before any repository read or write, got reads=%d writes=%d", mockRepo.getCalls, mockRepo.batchUpdateCalls)
	}

	attrs, err := transformer.FromPersistentRecord(ctx, mockRepo.records[schemaID][rowID])
	if err != nil {
		t.Fatalf("failed to convert stored record: %v", err)
	}
	if attrs["status"] != "scheduled" {
		t.Fatalf("expected stored status unchanged, got %v", attrs["status"])
	}
	if _, exists := attrs["feedback"]; exists {
		t.Fatalf("expected no feedback stored, got %v", attrs["feedback"])
	}
}

// The atomic pre-pass reports errors in list order: a malformed operation
// outranks a duplicate later in the list, and a duplicate never masks a
// missing field on the same operation.
func TestEntityManager_BatchUpdate_AtomicRequiredFieldsOutrankDuplicate(t *testing.T) {
	ctx := context.Background()
	em, mockRepo, _, _, rowID := seedVisitRow(t, "visit-atomic-precedence")
	visit := func(id uuid.UUID, updates map[string]any) forma.EntityOperation {
		return forma.EntityOperation{
			EntityIdentifier: forma.EntityIdentifier{SchemaName: "visit", RowID: id},
			Type:             forma.OperationUpdate,
			Updates:          updates,
		}
	}

	cases := []struct {
		name string
		ops  []forma.EntityOperation
		want string
	}{
		{
			name: "duplicate operation with nil updates",
			ops:  []forma.EntityOperation{visit(rowID, map[string]any{"status": "visited"}), visit(rowID, nil)},
			want: "operation[1]: updates are required for update operation",
		},
		{
			name: "earlier operation missing schema",
			ops: []forma.EntityOperation{
				{EntityIdentifier: forma.EntityIdentifier{RowID: rowID}, Type: forma.OperationUpdate, Updates: map[string]any{"status": "visited"}},
				visit(rowID, map[string]any{"status": "visited"}),
				visit(rowID, map[string]any{"feedback": "great visit"}),
			},
			want: "operation[0]: schema name is required",
		},
		{
			name: "earlier operation missing row id",
			ops: []forma.EntityOperation{
				visit(uuid.UUID{}, map[string]any{"status": "visited"}),
				visit(rowID, map[string]any{"status": "visited"}),
				visit(rowID, map[string]any{"feedback": "great visit"}),
			},
			want: "operation[0]: row id is required for update operation",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := em.BatchUpdate(ctx, &forma.BatchOperation{Atomic: true, Operations: tc.ops})
			if !errors.Is(err, forma.ErrInvalidInput) {
				t.Fatalf("expected ErrInvalidInput, got %v", err)
			}
			msg, ok := forma.ResolvePublicMessage(err)
			if !ok || msg != tc.want {
				t.Fatalf("expected published message %q, got %q (published=%v)", tc.want, msg, ok)
			}
			if mockRepo.getCalls != 0 || mockRepo.batchUpdateCalls != 0 {
				t.Fatalf("expected rejection before any repository read or write, got reads=%d writes=%d", mockRepo.getCalls, mockRepo.batchUpdateCalls)
			}
		})
	}
}

func TestEntityManager_BatchUpdate_BestEffortAppliesDuplicateRowIDInOrder(t *testing.T) {
	ctx := context.Background()
	em, mockRepo, transformer, schemaID, rowID := seedVisitRow(t, "visit-best-effort-dup")

	result, err := em.BatchUpdate(ctx, &forma.BatchOperation{
		Atomic:     false,
		Operations: duplicateRowUpdateOps(rowID),
	})
	if err != nil {
		t.Fatalf("best-effort batch update failed: %v", err)
	}
	if len(result.Successful) != 2 || len(result.Failed) != 0 {
		t.Fatalf("expected both operations to succeed, got successful=%d failed=%d", len(result.Successful), len(result.Failed))
	}

	attrs, err := transformer.FromPersistentRecord(ctx, mockRepo.records[schemaID][rowID])
	if err != nil {
		t.Fatalf("failed to convert stored record: %v", err)
	}
	if attrs["status"] != "visited" {
		t.Fatalf("expected the first operation's status to survive, got %v", attrs["status"])
	}
	if attrs["feedback"] != "great visit" {
		t.Fatalf("expected the second operation's feedback to be applied, got %v", attrs["feedback"])
	}
}
