package internal

import (
	"testing"

	"github.com/google/uuid"
)

func makeRec(schema int16, id uuid.UUID, updated int64) *PersistentRecord {
	return &PersistentRecord{
		SchemaID:  schema,
		RowID:     id,
		UpdatedAt: updated,
	}
}

func TestMergeLWW_PrefersNewest(t *testing.T) {
	rowID := uuid.New()
	hot := makeRec(1, rowID, 100)
	warm := makeRec(1, rowID, 200)
	cold := makeRec(1, rowID, 150)

	inputs := map[DataTier][]*PersistentRecord{
		DataTierHot:  {hot},
		DataTierWarm: {warm},
		DataTierCold: {cold},
	}

	results, err := MergePersistentRecordsByTier(inputs, false)
	if err != nil {
		t.Fatalf("merge error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0] != warm {
		t.Fatalf("expected warm record to win (newest). got UpdatedAt=%d", results[0].UpdatedAt)
	}
}

func TestMergeLWW_PreferHotTie(t *testing.T) {
	rowID := uuid.New()
	hot := makeRec(1, rowID, 100)
	warm := makeRec(1, rowID, 100)

	inputs := map[DataTier][]*PersistentRecord{
		DataTierHot:  {hot},
		DataTierWarm: {warm},
	}

	// Without preferHot, deterministic tie-breaker may choose lexicographic tier;
	// with preferHot=true Hot must win.
	results, err := MergePersistentRecordsByTier(inputs, true)
	if err != nil {
		t.Fatalf("merge error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0] != hot {
		t.Fatalf("expected hot record to win on tie when preferHot=true; got tier record UpdatedAt=%d", results[0].UpdatedAt)
	}
}
