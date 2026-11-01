package federated

import (
	"testing"

	"github.com/google/uuid"

	"github.com/lychee-technology/forma/internal/model"
)

// comparisonRowIDs are fixed so that each case names the row it alters.
var comparisonRowIDs = []uuid.UUID{
	uuid.MustParse("01900000-0000-7000-8000-000000000001"),
	uuid.MustParse("01900000-0000-7000-8000-000000000002"),
	uuid.MustParse("01900000-0000-7000-8000-000000000003"),
}

func comparisonRecord(id uuid.UUID) *model.PersistentRecord {
	return &model.PersistentRecord{
		SchemaID:     7,
		RowID:        id,
		TextItems:    map[string]string{"name": "row " + id.String()[34:]},
		Float64Items: map[string]float64{"version": 3},
		Int64Items:   map[string]int64{"tradeTime": 1_700_000_000_000},
		CreatedAt:    1_000,
		UpdatedAt:    2_000,
	}
}

func comparisonResult() *QueryResult {
	result := &QueryResult{TotalRecords: int64(len(comparisonRowIDs))}
	for _, id := range comparisonRowIDs {
		result.Records = append(result.Records, comparisonRecord(id))
	}
	return result
}

// TestCompareResultsMatchesIdenticalResults is the positive control for the
// negative cases below: two independently built, equal results match, and a
// nil map on one side equals an empty map on the other.
func TestCompareResultsMatchesIdenticalResults(t *testing.T) {
	fed, pg := comparisonResult(), comparisonResult()
	pg.Records[0].UUIDItems = map[string]uuid.UUID{}
	pg.Records[0].OtherAttributes = []model.EAVRecord{}

	report := (&FederatedTestHarness{}).CompareResults(fed, pg)
	if !report.Match {
		t.Fatalf("identical results reported as different: %+v", report)
	}
}

// TestCompareResultsDetectsDivergence pins each way the comparison used to
// report Match=true for results that differ (#753): an unequal total, a
// duplicated row, a changed numeric or system value, and an attribute only one
// side carries. Every case must fail the comparison and say what differs.
func TestCompareResultsDetectsDivergence(t *testing.T) {
	deletedAt := int64(3_000)
	cases := []struct {
		name     string
		alter    func(fed, pg *QueryResult)
		wantAttr string
		check    func(t *testing.T, r *ComparisonReport)
	}{
		{name: "total differs", alter: func(fed, _ *QueryResult) { fed.TotalRecords++ }},
		{
			name: "duplicate row in federated",
			alter: func(fed, _ *QueryResult) {
				fed.Records = append(fed.Records, comparisonRecord(comparisonRowIDs[1]))
			},
			check: func(t *testing.T, r *ComparisonReport) {
				if len(r.DuplicatesInFed) != 1 || r.DuplicatesInFed[0] != comparisonRowIDs[1] {
					t.Errorf("DuplicatesInFed = %v, want [%s]", r.DuplicatesInFed, comparisonRowIDs[1])
				}
			},
		},
		{
			name: "duplicate row in postgres",
			alter: func(_, pg *QueryResult) {
				pg.Records = append(pg.Records, comparisonRecord(comparisonRowIDs[2]))
			},
			check: func(t *testing.T, r *ComparisonReport) {
				if len(r.DuplicatesInPG) != 1 {
					t.Errorf("DuplicatesInPG = %v, want one row", r.DuplicatesInPG)
				}
			},
		},
		{
			name:     "numeric version differs",
			alter:    func(fed, _ *QueryResult) { fed.Records[0].Float64Items["version"] = 4 },
			wantAttr: "float64:version",
		},
		{
			name:     "int64 value differs",
			alter:    func(_, pg *QueryResult) { pg.Records[2].Int64Items["tradeTime"]++ },
			wantAttr: "int64:tradeTime",
		},
		{
			name:     "text attribute missing in federated",
			alter:    func(fed, _ *QueryResult) { delete(fed.Records[1].TextItems, "name") },
			wantAttr: "text:name",
		},
		{
			name:     "text attribute missing in postgres",
			alter:    func(_, pg *QueryResult) { delete(pg.Records[1].TextItems, "name") },
			wantAttr: "text:name",
		},
		{
			name: "uuid attribute only in federated",
			alter: func(fed, _ *QueryResult) {
				fed.Records[0].UUIDItems = map[string]uuid.UUID{"owner": comparisonRowIDs[2]}
			},
			wantAttr: "uuid:owner",
		},
		{
			name:     "updated_at differs",
			alter:    func(fed, _ *QueryResult) { fed.Records[2].UpdatedAt++ },
			wantAttr: "updated_at",
		},
		{
			name:     "created_at differs",
			alter:    func(_, pg *QueryResult) { pg.Records[0].CreatedAt = 0 },
			wantAttr: "created_at",
		},
		{
			name:     "deleted in one side only",
			alter:    func(fed, _ *QueryResult) { fed.Records[1].DeletedAt = &deletedAt },
			wantAttr: "deleted_at",
		},
		{
			name:     "schema differs",
			alter:    func(fed, _ *QueryResult) { fed.Records[0].SchemaID = 8 },
			wantAttr: "schema_id",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fed, pg := comparisonResult(), comparisonResult()
			tc.alter(fed, pg)
			report := (&FederatedTestHarness{}).CompareResults(fed, pg)
			if report.Match {
				t.Fatalf("divergent results reported as matching: %+v", report)
			}
			if tc.wantAttr != "" {
				requireSingleMismatch(t, report, tc.wantAttr)
			}
			if tc.check != nil {
				tc.check(t, report)
			}
		})
	}
}

// TestCompareResultsReportsMissingRows keeps the row-set check: a row on one
// side only is listed on the side that lacks it.
func TestCompareResultsReportsMissingRows(t *testing.T) {
	fed, pg := comparisonResult(), comparisonResult()
	pg.Records = pg.Records[:2]
	pg.TotalRecords = 2

	report := (&FederatedTestHarness{}).CompareResults(fed, pg)
	if report.Match {
		t.Fatal("a row present only in federated reported as matching")
	}
	if len(report.MissingInPG) != 1 || report.MissingInPG[0] != comparisonRowIDs[2] {
		t.Errorf("MissingInPG = %v, want [%s]", report.MissingInPG, comparisonRowIDs[2])
	}
	if len(report.MissingInFed) != 0 {
		t.Errorf("MissingInFed = %v, want none", report.MissingInFed)
	}
}

func requireSingleMismatch(t *testing.T, report *ComparisonReport, attr string) {
	t.Helper()
	if len(report.AttributeMismatches) != 1 {
		t.Fatalf("AttributeMismatches = %+v, want exactly one on %s", report.AttributeMismatches, attr)
	}
	if got := report.AttributeMismatches[0].AttributeName; got != attr {
		t.Fatalf("mismatch on %s, want %s", got, attr)
	}
}
