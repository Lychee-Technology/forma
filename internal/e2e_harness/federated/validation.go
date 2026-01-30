package federated

import (
	"fmt"
	"sort"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal"
)

// CompareResults compares federated and postgres query results.
func (h *FederatedTestHarness) CompareResults(federated, postgres *QueryResult) *ComparisonReport {
	report := &ComparisonReport{
		FederatedCount: federated.TotalRecords,
		PostgresCount:  postgres.TotalRecords,
		Match:          true,
	}

	fedMap := make(map[uuid.UUID]*internal.PersistentRecord)
	for _, r := range federated.Records {
		fedMap[r.RowID] = r
	}

	pgMap := make(map[uuid.UUID]*internal.PersistentRecord)
	for _, r := range postgres.Records {
		pgMap[r.RowID] = r
	}

	// Find missing in federated
	for id := range pgMap {
		if _, ok := fedMap[id]; !ok {
			report.MissingInFed = append(report.MissingInFed, id)
			report.Match = false
		}
	}

	// Find missing in postgres
	for id := range fedMap {
		if _, ok := pgMap[id]; !ok {
			report.MissingInPG = append(report.MissingInPG, id)
			report.Match = false
		}
	}

	// Compare attributes for matching records
	for id, fedRec := range fedMap {
		if pgRec, ok := pgMap[id]; ok {
			h.compareRecordAttributes(report, id, fedRec, pgRec)
		}
	}

	// Calculate checksums
	report.FederatedChecksum = h.CalculateChecksum(federated.Records)
	report.PostgresChecksum = h.CalculateChecksum(postgres.Records)

	return report
}

// compareRecordAttributes compares attributes between federated and postgres records.
func (h *FederatedTestHarness) compareRecordAttributes(report *ComparisonReport, id uuid.UUID, fedRec, pgRec *internal.PersistentRecord) {
	for k, fedVal := range fedRec.TextItems {
		if pgVal, ok := pgRec.TextItems[k]; ok && fedVal != pgVal {
			report.AttributeMismatches = append(report.AttributeMismatches, AttributeMismatch{
				RowID:         id,
				AttributeName: k,
				FederatedVal:  fedVal,
				PostgresVal:   pgVal,
			})
			report.Match = false
		}
	}
}

// ValidateDeduplication verifies that records are properly deduplicated.
func (h *FederatedTestHarness) ValidateDeduplication(records []*internal.PersistentRecord) error {
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
func (h *FederatedTestHarness) CalculateChecksum(records []*internal.PersistentRecord) string {
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
