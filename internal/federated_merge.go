package internal

import (
	"fmt"
	"sort"
)

// MergePersistentRecordsByTier performs a merge-on-read across multiple data tiers.
// Inputs are provided as a map from DataTier -> slice of *PersistentRecord.
// Last-write-wins semantics are applied using PersistentRecord.UpdatedAt as the
// timestamp. If timestamps are equal and preferHot is true, Hot wins over Warm/Cold.
//
// Behavior:
// - Records are deduplicated by (SchemaID, RowID).
// - For each key, the record with the highest UpdatedAt is chosen. If equal and
//   preferHot==true, the record coming from the Hot tier is chosen.
// - The chosen record is returned as-is (including OtherAttributes). No attribute-level
//   merge is performed; whole-record LWW semantics are used for simplicity and correctness.
// - Result slice is sorted by SchemaID then RowID for deterministic output.
func MergePersistentRecordsByTier(inputs map[DataTier][]*PersistentRecord, preferHot bool) ([]*PersistentRecord, error) {
	if inputs == nil {
		return nil, fmt.Errorf("inputs cannot be nil")
	}

	// Create an ordered tier priority used when preferHot=true and timestamps tie.
	tierPriority := map[DataTier]int{
		DataTierCold:  2,
		DataTierWarm:  1,
		DataTierHot:   0,
	}

	merged := make(map[string]*PersistentRecord)
	mergedSourceTier := make(map[string]DataTier)

	for tier, records := range inputs {
		if records == nil {
			continue
		}
		for _, rec := range records {
			if rec == nil {
				continue
			}
			key := mergeKey(rec)
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
	// Compare UpdatedAt
	if newRec.UpdatedAt > existing.UpdatedAt {
		return newRec
	}
	if newRec.UpdatedAt < existing.UpdatedAt {
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
