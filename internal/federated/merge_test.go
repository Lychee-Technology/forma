package federated

import (
	"testing"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/google/uuid"
)

func makeRec(schema int16, id uuid.UUID, updated int64) *model.PersistentRecord {
	return &model.PersistentRecord{
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

	inputs := map[model.DataTier][]*model.PersistentRecord{
		model.DataTierHot:  {hot},
		model.DataTierWarm: {warm},
		model.DataTierCold: {cold},
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

	inputs := map[model.DataTier][]*model.PersistentRecord{
		model.DataTierHot:  {hot},
		model.DataTierWarm: {warm},
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

func TestMergeLWW_DeletedNewestSuppressesOlderActive(t *testing.T) {
	rowID := uuid.New()
	deletedAt := int64(300)
	hot := makeRec(1, rowID, 200)
	hot.DeletedAt = &deletedAt
	cold := makeRec(1, rowID, 100)

	results, err := MergePersistentRecordsByTier(map[model.DataTier][]*model.PersistentRecord{
		model.DataTierHot:  {hot},
		model.DataTierCold: {cold},
	}, false)

	if err != nil {
		t.Fatalf("merge error: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("expected deleted winner to suppress row, got %d results", len(results))
	}
}

func TestMergeLWW_EqualTimestampUsesTierPriority(t *testing.T) {
	rowID := uuid.New()
	hot := makeRec(1, rowID, 100)
	cold := makeRec(1, rowID, 100)

	results, err := MergePersistentRecordsByTier(map[model.DataTier][]*model.PersistentRecord{
		model.DataTierHot:  {hot},
		model.DataTierCold: {cold},
	}, false)

	if err != nil {
		t.Fatalf("merge error: %v", err)
	}
	if len(results) != 1 || results[0] != hot {
		t.Fatalf("expected hot to win equal timestamp tie")
	}
}

// TestMergeLWW_ZeroDeletedAtIsLiveNotTombstone pins the #274 contract at the
// Go merge: DeletedAt = &0 is a LIVE row (the cold-tier parquet encoding on
// both base and, post-#274, delta), not a tombstone. An equal-UpdatedAt tie
// between a live cold copy (&0) and a live hot copy (nil) must fall to tier
// priority (hot wins), mirroring the SQL rank order where
// source_tier_priority DESC is evaluated before deleted_ts DESC.
func TestMergeLWW_ZeroDeletedAtIsLiveNotTombstone(t *testing.T) {
	rowID := uuid.New()
	zero := int64(0)
	hot := makeRec(1, rowID, 100)
	cold := makeRec(1, rowID, 100)
	cold.DeletedAt = &zero

	results, err := MergePersistentRecordsByTier(map[model.DataTier][]*model.PersistentRecord{
		model.DataTierHot:  {hot},
		model.DataTierCold: {cold},
	}, false)

	if err != nil {
		t.Fatalf("merge error: %v", err)
	}
	if len(results) != 1 || results[0] != hot {
		t.Fatalf("expected hot to win the equal-timestamp tie via tier priority; DeletedAt=&0 must not be treated as a tombstone")
	}

	// Both argument orders of the pairwise compare must agree: &0 is live, so
	// neither presence branch may fire and tier priority decides.
	tierPriority := map[model.DataTier]int{model.DataTierHot: 3, model.DataTierCold: 1}
	if got := chooseLWW(hot, model.DataTierHot, cold, model.DataTierCold, false, tierPriority); got != hot {
		t.Fatalf("chooseLWW(hot, cold): expected hot, got cold (&0 treated as tombstone)")
	}
	if got := chooseLWW(cold, model.DataTierCold, hot, model.DataTierHot, false, tierPriority); got != hot {
		t.Fatalf("chooseLWW(cold, hot): expected hot, got cold (&0 treated as tombstone)")
	}
}
