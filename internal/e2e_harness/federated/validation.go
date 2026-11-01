package federated

import (
	"fmt"
	"reflect"
	"sort"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/google/uuid"
)

// CompareResults compares federated and postgres query results. The two
// results match only when their totals agree, neither repeats a row ID, both
// hold the same row IDs, and every row agrees on its system columns and on
// every typed value either side carries: an attribute present on one side
// only is a mismatch, not a skip.
func (h *FederatedTestHarness) CompareResults(federated, postgres *QueryResult) *ComparisonReport {
	report := &ComparisonReport{
		FederatedCount: federated.TotalRecords,
		PostgresCount:  postgres.TotalRecords,
		Match:          federated.TotalRecords == postgres.TotalRecords,
	}

	fedMap, fedDuplicates := indexRecordsByRowID(federated.Records)
	pgMap, pgDuplicates := indexRecordsByRowID(postgres.Records)
	report.DuplicatesInFed = fedDuplicates
	report.DuplicatesInPG = pgDuplicates
	if len(fedDuplicates) > 0 || len(pgDuplicates) > 0 {
		report.Match = false
	}

	report.MissingInFed = missingRowIDs(pgMap, fedMap)
	report.MissingInPG = missingRowIDs(fedMap, pgMap)
	if len(report.MissingInFed) > 0 || len(report.MissingInPG) > 0 {
		report.Match = false
	}

	for _, id := range sortedRowIDs(fedMap) {
		if pgRec, ok := pgMap[id]; ok {
			compareRecordFields(report, id, fedMap[id], pgRec)
		}
	}

	report.FederatedChecksum = h.CalculateChecksum(federated.Records)
	report.PostgresChecksum = h.CalculateChecksum(postgres.Records)

	return report
}

// indexRecordsByRowID maps each row ID to its first record and lists, once
// each in input order, the row IDs that occur more than once.
func indexRecordsByRowID(records []*model.PersistentRecord) (map[uuid.UUID]*model.PersistentRecord, []uuid.UUID) {
	byID := make(map[uuid.UUID]*model.PersistentRecord, len(records))
	var duplicates []uuid.UUID
	reported := make(map[uuid.UUID]bool)
	for _, r := range records {
		if _, seen := byID[r.RowID]; !seen {
			byID[r.RowID] = r
			continue
		}
		if !reported[r.RowID] {
			reported[r.RowID] = true
			duplicates = append(duplicates, r.RowID)
		}
	}
	return byID, duplicates
}

// missingRowIDs lists, sorted, the row IDs of want that got lacks.
func missingRowIDs(want, got map[uuid.UUID]*model.PersistentRecord) []uuid.UUID {
	var missing []uuid.UUID
	for _, id := range sortedRowIDs(want) {
		if _, ok := got[id]; !ok {
			missing = append(missing, id)
		}
	}
	return missing
}

func sortedRowIDs(byID map[uuid.UUID]*model.PersistentRecord) []uuid.UUID {
	ids := make([]uuid.UUID, 0, len(byID))
	for id := range byID {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })
	return ids
}

// absentValue stands in an AttributeMismatch for a value one side does not
// carry, so that a missing attribute is distinguishable from an empty one.
type absentValue struct{}

func (absentValue) String() string { return "<absent>" }

// compareRecordFields records every field on which two records of the same
// row ID differ: the system columns, then each typed attribute map.
func compareRecordFields(report *ComparisonReport, id uuid.UUID, fedRec, pgRec *model.PersistentRecord) {
	mismatch := func(name string, fedVal, pgVal any) {
		report.AttributeMismatches = append(report.AttributeMismatches, AttributeMismatch{
			RowID: id, AttributeName: name, FederatedVal: fedVal, PostgresVal: pgVal,
		})
		report.Match = false
	}
	if fedRec.SchemaID != pgRec.SchemaID {
		mismatch("schema_id", fedRec.SchemaID, pgRec.SchemaID)
	}
	if fedRec.CreatedAt != pgRec.CreatedAt {
		mismatch("created_at", fedRec.CreatedAt, pgRec.CreatedAt)
	}
	if fedRec.UpdatedAt != pgRec.UpdatedAt {
		mismatch("updated_at", fedRec.UpdatedAt, pgRec.UpdatedAt)
	}
	if fedDeleted, pgDeleted := deletedAtValue(fedRec), deletedAtValue(pgRec); fedDeleted != pgDeleted {
		mismatch("deleted_at", fedDeleted, pgDeleted)
	}
	if !reflect.DeepEqual(normalizeEAV(fedRec.OtherAttributes), normalizeEAV(pgRec.OtherAttributes)) {
		mismatch("other_attributes", fedRec.OtherAttributes, pgRec.OtherAttributes)
	}
	compareItems("text", fedRec.TextItems, pgRec.TextItems, mismatch)
	compareItems("int16", fedRec.Int16Items, pgRec.Int16Items, mismatch)
	compareItems("int32", fedRec.Int32Items, pgRec.Int32Items, mismatch)
	compareItems("int64", fedRec.Int64Items, pgRec.Int64Items, mismatch)
	compareItems("float64", fedRec.Float64Items, pgRec.Float64Items, mismatch)
	compareItems("uuid", fedRec.UUIDItems, pgRec.UUIDItems, mismatch)
}

// deletedAtValue renders a record's deletion stamp comparably: nil when the
// record is live.
func deletedAtValue(rec *model.PersistentRecord) any {
	if rec.DeletedAt == nil {
		return nil
	}
	return *rec.DeletedAt
}

// normalizeEAV treats a nil and an empty attribute list alike.
func normalizeEAV(attrs []model.EAVRecord) []model.EAVRecord {
	if len(attrs) == 0 {
		return nil
	}
	return attrs
}

// compareItems reports, in key order, every key whose value differs between
// two typed attribute maps, including a key only one side carries.
func compareItems[V comparable](kind string, fed, pg map[string]V, mismatch func(name string, fedVal, pgVal any)) {
	keys := make(map[string]struct{}, len(fed)+len(pg))
	for k := range fed {
		keys[k] = struct{}{}
	}
	for k := range pg {
		keys[k] = struct{}{}
	}
	sorted := make([]string, 0, len(keys))
	for k := range keys {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)
	for _, k := range sorted {
		fedVal, fedOK := fed[k]
		pgVal, pgOK := pg[k]
		if fedOK && pgOK && fedVal == pgVal {
			continue
		}
		mismatch(kind+":"+k, presentOrAbsent(fedVal, fedOK), presentOrAbsent(pgVal, pgOK))
	}
}

func presentOrAbsent[V any](v V, ok bool) any {
	if !ok {
		return absentValue{}
	}
	return v
}

// ValidateDeduplication verifies that records are properly deduplicated.
func (h *FederatedTestHarness) ValidateDeduplication(records []*model.PersistentRecord) error {
	seen := make(map[uuid.UUID]bool)
	for _, r := range records {
		if seen[r.RowID] {
			return fmt.Errorf("duplicate row_id found: %s", r.RowID)
		}
		seen[r.RowID] = true
	}
	return nil
}

// CalculateChecksum calculates a simple checksum of records.
func (h *FederatedTestHarness) CalculateChecksum(records []*model.PersistentRecord) string {
	ids := make([]string, len(records))
	for i, r := range records {
		ids[i] = r.RowID.String()
	}
	sort.Strings(ids)

	// Simple hash
	hash := uint32(0)
	for _, id := range ids {
		for _, c := range id {
			hash = hash*31 + uint32(c)
		}
	}
	return fmt.Sprintf("%08x", hash)
}
