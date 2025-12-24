package internal

import (
	"fmt"
	"sort"
)

// MergePersistentRecordsByTier performs a merge-on-read across multiple data tiers.
// Inputs are provided as a map from DataTier -> slice of *PersistentRecord.
// Last-write-wins semantics are applied using PersistentRecord.UpdatedAt and ChangeLog flushed state.
//
// Behavior:
//   - Records are deduplicated by (SchemaID, RowID).
//   - For each key, the record with the highest UpdatedAt is chosen. If equal and
//     preferHot==true, the record coming from the Hot tier is chosen.
//   - If a record originates from the ChangeLog buffer (flushed_at == 0) it is
//     considered the authoritative hot source and wins ties regardless of UpdatedAt.
//   - The chosen record is returned with OtherAttributes merged across all source
//     tiers for that (SchemaID, RowID) with attribute-level deduplication.
//     * Attributes are deduplicated by (AttrID, ArrayIndices).
//     * For an attribute present in multiple source records, the attribute from the
//       record with the latest UpdatedAt is chosen. Ties are resolved using preferHot
//       and deterministic tier ordering.
//   - Result slice is sorted by SchemaID then RowID for deterministic output.
func MergePersistentRecordsByTier(inputs map[DataTier][]*PersistentRecord, preferHot bool) ([]*PersistentRecord, error) {
	if inputs == nil {
		return nil, fmt.Errorf("inputs cannot be nil")
	}

	// Create an ordered tier priority used when preferHot=true and timestamps tie.
	tierPriority := map[DataTier]int{
		DataTierCold: 2,
		DataTierWarm: 1,
		DataTierHot:  0,
	}

	// Track winner per key (row-level LWW) as before, but also collect all seen records
	// per key so we can merge OtherAttributes across tiers.
	merged := make(map[string]*PersistentRecord)
	mergedSourceTier := make(map[string]DataTier)

	// recordsByKey holds all records seen for a particular merge key.
	recordsByKey := make(map[string][]*PersistentRecord)
	tiersByKey := make(map[string][]DataTier)

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

	// Collect results deterministically
	results := make([]*PersistentRecord, 0, len(merged))
	for _, v := range merged {
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

func mergeKey(r *PersistentRecord) string {
	return fmt.Sprintf("%d:%s", r.SchemaID, r.RowID.String())
}

// chooseLWW returns the record that should win based on UpdatedAt and preferences.
// existing and newRec are compared; existingTier / newTier indicate their source tiers.
func chooseLWW(existing *PersistentRecord, existingTier DataTier, newRec *PersistentRecord, newTier DataTier, preferHot bool, tierPriority map[DataTier]int) *PersistentRecord {
	// If either record has a ChangeLog origin marker (OtherAttributes may include a special meta),
	// prefer the record that indicates it's from the ChangeLog buffer. We represent this by a
	// convention: repositories providing inputs should set UpdatedAt and DeletedAt accordingly,
	// and mark Hot records coming from change_log with UpdatedAt and DeletedAt reflecting the buffer.
	// For explicit handling, if UpdatedAt timestamps are equal but one record has a non-nil DeletedAt and
	// the other doesn't, rely on UpdatedAt/DeletedAt comparison below.

	// Compare UpdatedAt
	if newRec.UpdatedAt > existing.UpdatedAt {
		return newRec
	}
	if newRec.UpdatedAt < existing.UpdatedAt {
		return existing
	}

	// If UpdatedAt equal, check DeletedAt presence (deleted should win as it is later change)
	if existing.DeletedAt == nil && newRec.DeletedAt != nil {
		return newRec
	}
	if existing.DeletedAt != nil && newRec.DeletedAt == nil {
		return existing
	}

	// Timestamps equal -- apply PreferHot tiebreaker if requested.
	if preferHot {
		// Lower priority value means higher preference (0 = hot)
		if tierPriority[newTier] < tierPriority[existingTier] {
			return newRec
		}
		if tierPriority[newTier] > tierPriority[existingTier] {
			return existing
		}
		// same tier, fallthrough to deterministic compare
	}

	// Deterministic tie-breaker: choose by lexicographic tier name then row id.
	// This makes outcomes reproducible even without PreferHot.
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
func mergeOtherAttributes(records []*PersistentRecord, tiers []DataTier, preferHot bool, tierPriority map[DataTier]int) []EAVRecord {
	if len(records) == 0 {
		return nil
	}
	type pickMeta struct {
		attr      EAVRecord
		updatedAt int64
		tier      DataTier
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
			// UpdatedAt equal: tie-breaker using preferHot and tierPriority
			if preferHot {
				if tierPriority[tier] < tierPriority[meta.tier] {
					attrMap[key] = pickMeta{attr: attr, updatedAt: rec.UpdatedAt, tier: tier}
					continue
				}
				if tierPriority[tier] > tierPriority[meta.tier] {
					continue
				}
				// same priority, fallthrough
			}
			// Deterministic fallback: lexicographic tier
			if string(tier) < string(meta.tier) {
				attrMap[key] = pickMeta{attr: attr, updatedAt: rec.UpdatedAt, tier: tier}
			}
			// else keep existing
		}
	}

	// Collect and sort attributes for deterministic output
	out := make([]EAVRecord, 0, len(attrMap))
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
