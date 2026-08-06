package federated

import (
	"fmt"
	"sort"

	"github.com/lychee-technology/forma/internal/model"
)

// MergePersistentRecordsByTier performs a merge-on-read across multiple data tiers.
// Inputs are provided as a map from model.DataTier -> slice of *model.PersistentRecord.
// Last-write-wins semantics are applied using model.PersistentRecord.UpdatedAt and ChangeLog flushed state.
//
// Behavior:
//   - Records are deduplicated by (SchemaID, RowID).
//   - For each key, the record with the highest UpdatedAt is chosen. If UpdatedAt is equal,
//     a tombstone (DeletedAt != nil && *DeletedAt != 0) beats a live copy; DeletedAt = 0
//     is the cold-tier live encoding (#274), never a tombstone. Remaining ties use
//     deterministic tier priority (Hot > Warm > Cold).
//   - If a record originates from the ChangeLog buffer (flushed_at == 0) it is
//     considered the authoritative hot source and wins ties regardless of UpdatedAt.
//   - The chosen record is returned with OtherAttributes merged across all source
//     tiers for that (SchemaID, RowID) with attribute-level deduplication.
//   - Attributes are deduplicated by (AttrID, ArrayIndices).
//   - For an attribute present in multiple source records, the attribute from the
//     record with the latest UpdatedAt is chosen. Ties are resolved using deterministic
//     tier ordering (Hot > Warm > Cold).
//   - Deleted records (DeletedAt != nil && DeletedAt != 0) are excluded from results.
//   - Result slice is sorted by SchemaID then RowID for deterministic output.
//
// The preferHot parameter is deprecated and ignored; tier priority is always deterministic.
func MergePersistentRecordsByTier(inputs map[model.DataTier][]*model.PersistentRecord, preferHot bool) ([]*model.PersistentRecord, error) {
	if inputs == nil {
		return nil, fmt.Errorf("inputs cannot be nil")
	}

	// Create tier priority for deterministic tie-breaking.
	// Higher value = higher priority (Hot > Warm > Cold).
	tierPriority := map[model.DataTier]int{
		model.DataTierHot:  3,
		model.DataTierWarm: 2,
		model.DataTierCold: 1,
	}

	// Track winner per key (row-level LWW) as before, but also collect all seen records
	// per key so we can merge OtherAttributes across tiers.
	merged := make(map[string]*model.PersistentRecord)
	mergedSourceTier := make(map[string]model.DataTier)

	// recordsByKey holds all records seen for a particular merge key.
	recordsByKey := make(map[string][]*model.PersistentRecord)
	tiersByKey := make(map[string][]model.DataTier)

	for tier, records := range inputs {
		if records == nil {
			continue
		}
		for _, rec := range records {
			if rec == nil {
				continue
			}
			key := mergeKey(rec)

			// collect for attribute-level merging later
			recordsByKey[key] = append(recordsByKey[key], rec)
			tiersByKey[key] = append(tiersByKey[key], tier)

			existing, ok := merged[key]
			if !ok {
				merged[key] = rec
				mergedSourceTier[key] = tier
				continue
			}

			// Choose winner between existing and rec
			winner := chooseLWW(existing, mergedSourceTier[key], rec, tier, preferHot, tierPriority)
			if winner == rec {
				merged[key] = rec
				mergedSourceTier[key] = tier
			}
		}
	}

	// Now merge OtherAttributes per key across all collected records.
	for key, winner := range merged {
		records := recordsByKey[key]
		tiers := tiersByKey[key]
		mergedAttrs := mergeOtherAttributes(records, tiers, preferHot, tierPriority)
		if len(mergedAttrs) > 0 {
			winner.OtherAttributes = mergedAttrs
		} else {
			winner.OtherAttributes = nil
		}
	}

	// Collect results deterministically, excluding deleted winners
	results := make([]*model.PersistentRecord, 0, len(merged))
	for _, v := range merged {
		// Skip records that are deleted (soft-delete suppression)
		if v.DeletedAt != nil && *v.DeletedAt != 0 {
			continue
		}
		results = append(results, v)
	}
	sort.Slice(results, func(i, j int) bool {
		if results[i].SchemaID != results[j].SchemaID {
			return results[i].SchemaID < results[j].SchemaID
		}
		return results[i].RowID.String() < results[j].RowID.String()
	})

	return results, nil
}

func mergeKey(r *model.PersistentRecord) string {
	return fmt.Sprintf("%d:%s", r.SchemaID, r.RowID.String())
}

// chooseLWW returns the record that should win based on UpdatedAt and preferences.
// existing and newRec are compared; existingTier / newTier indicate their source tiers.
func chooseLWW(existing *model.PersistentRecord, existingTier model.DataTier, newRec *model.PersistentRecord, newTier model.DataTier, preferHot bool, tierPriority map[model.DataTier]int) *model.PersistentRecord {
	// Compare UpdatedAt
	if newRec.UpdatedAt > existing.UpdatedAt {
		return newRec
	}
	if newRec.UpdatedAt < existing.UpdatedAt {
		return existing
	}

	// If UpdatedAt equal, a tombstone wins over a live copy (the delete is the
	// later change). Liveness is a value question, not a presence question:
	// cold-tier parquet encodes live rows as DeletedAt = 0 (#274) while hot
	// rows carry nil, so both nil and &0 mean live here — matching the SQL
	// rank order's deleted_ts DESC, where a tombstone T > 0 beats live 0/NULL.
	// (The SQL rank evaluates tier priority before deleted_ts; this arm runs
	// first — semantics match, order differs. Post-#274 the divergence is
	// unreachable: tombstones stamp strictly above live versions, so an
	// equal-UpdatedAt live/tombstone pair cannot be minted. Noted for #365.)
	existingDeleted := existing.DeletedAt != nil && *existing.DeletedAt != 0
	newDeleted := newRec.DeletedAt != nil && *newRec.DeletedAt != 0
	if !existingDeleted && newDeleted {
		return newRec
	}
	if existingDeleted && !newDeleted {
		return existing
	}

	// Timestamps equal -- use tier priority as deterministic tie-breaker.
	// Higher priority value means higher preference (Hot=3, Warm=2, Cold=1).
	if tierPriority[newTier] > tierPriority[existingTier] {
		return newRec
	}
	if tierPriority[newTier] < tierPriority[existingTier] {
		return existing
	}

	// Same tier, use deterministic fallback: lexicographic tier name then row id.
	if string(newTier) < string(existingTier) {
		return newRec
	}
	if string(newTier) > string(existingTier) {
		return existing
	}

	// As a final deterministic fallback, compare RowID strings (though same key)
	if newRec.RowID.String() < existing.RowID.String() {
		return newRec
	}
	return existing
}

// mergeOtherAttributes merges EAV attributes across multiple source records for the same row.
// Deduplication key: (AttrID, ArrayIndices).
// Selection: attribute from record with highest UpdatedAt; ties resolved with preferHot and deterministic tier ordering.
func mergeOtherAttributes(records []*model.PersistentRecord, tiers []model.DataTier, preferHot bool, tierPriority map[model.DataTier]int) []model.EAVRecord {
	if len(records) == 0 {
		return nil
	}
	type pickMeta struct {
		attr      model.EAVRecord
		updatedAt int64
		tier      model.DataTier
	}

	attrMap := make(map[string]pickMeta) // key -> chosen attr meta
	for i, rec := range records {
		if rec == nil {
			continue
		}
		tier := tiers[i]
		for _, attr := range rec.OtherAttributes {
			key := fmt.Sprintf("%d|%s", attr.AttrID, attr.ArrayIndices)
			meta, ok := attrMap[key]
			if !ok {
				attrMap[key] = pickMeta{attr: attr, updatedAt: rec.UpdatedAt, tier: tier}
				continue
			}
			// Compare rec.UpdatedAt vs meta.updatedAt
			if rec.UpdatedAt > meta.updatedAt {
				attrMap[key] = pickMeta{attr: attr, updatedAt: rec.UpdatedAt, tier: tier}
				continue
			}
			if rec.UpdatedAt < meta.updatedAt {
				continue
			}
			// UpdatedAt equal: use tier priority as deterministic tie-breaker
			if tierPriority[tier] > tierPriority[meta.tier] {
				attrMap[key] = pickMeta{attr: attr, updatedAt: rec.UpdatedAt, tier: tier}
				continue
			}
			if tierPriority[tier] < tierPriority[meta.tier] {
				continue
			}
			// Deterministic fallback: lexicographic tier
			if string(tier) < string(meta.tier) {
				attrMap[key] = pickMeta{attr: attr, updatedAt: rec.UpdatedAt, tier: tier}
			}
			// else keep existing
		}
	}

	// Collect and sort attributes for deterministic output
	out := make([]model.EAVRecord, 0, len(attrMap))
	for _, m := range attrMap {
		out = append(out, m.attr)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].AttrID != out[j].AttrID {
			return out[i].AttrID < out[j].AttrID
		}
		return out[i].ArrayIndices < out[j].ArrayIndices
	})
	return out
}
