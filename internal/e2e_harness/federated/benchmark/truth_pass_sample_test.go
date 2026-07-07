package benchmark

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestTruthPassSampleIndicesNotAppliedWhenCapCoversTotal(t *testing.T) {
	if got := selectTruthPassSampleIndices(42, "eav-selective-page", 100, 0); got != nil {
		t.Fatalf("cap=0 must disable sampling, got %v", got)
	}
	if got := selectTruthPassSampleIndices(42, "eav-selective-page", 100, 100); got != nil {
		t.Fatalf("total<=cap must disable sampling, got %v", got)
	}
}

func TestTruthPassSampleIndicesDeterministicAndBounded(t *testing.T) {
	first := selectTruthPassSampleIndices(42, "eav-selective-page", 1000, 25)
	second := selectTruthPassSampleIndices(42, "eav-selective-page", 1000, 25)
	if !reflect.DeepEqual(first, second) {
		t.Fatal("same seed/workload/total/cap must sample identically")
	}
	if len(first) != 25 {
		t.Fatalf("expected exactly 25 sampled indices, got %d", len(first))
	}
	for idx := range first {
		if idx < 0 || idx >= 1000 {
			t.Fatalf("sampled index %d out of range [0,1000)", idx)
		}
	}
}

func TestTruthPassSampleIndicesVaryBySeedAndWorkload(t *testing.T) {
	base := selectTruthPassSampleIndices(42, "eav-selective-page", 1000, 25)
	otherSeed := selectTruthPassSampleIndices(43, "eav-selective-page", 1000, 25)
	otherWorkload := selectTruthPassSampleIndices(42, "hot-selective-page", 1000, 25)
	if reflect.DeepEqual(base, otherSeed) && reflect.DeepEqual(base, otherWorkload) {
		t.Fatal("expected seed and workload name to influence the sample")
	}
}

func spotCheckCandidates(n int) []GeneratedRecord {
	records := make([]GeneratedRecord, 0, n)
	for i := 0; i < n; i++ {
		records = append(records, GeneratedRecord{
			RowID: uuid.MustParse(fmt.Sprintf("00000000-0000-0000-0000-%012d", i+1)),
		})
	}
	return records
}

func TestBuildTruthPassExpectedUncappedKeepsFilteringSemantics(t *testing.T) {
	candidates := spotCheckCandidates(5)
	invisible := map[string]struct{}{
		candidates[1].RowID.String(): {},
		candidates[3].RowID.String(): {},
	}
	calls := 0
	isVisible := func(_ context.Context, candidate GeneratedRecord) (bool, error) {
		calls++
		_, hidden := invisible[candidate.RowID.String()]
		return !hidden, nil
	}
	workload := WorkloadDefinition{Name: "spot-check-test", PageSize: 20, PageNumber: 1}
	expected, stats, err := buildTruthPassExpected(context.Background(), isVisible, workload, 20, candidates, 0, 42)
	if err != nil {
		t.Fatalf("uncapped truth pass failed: %v", err)
	}
	if calls != 5 {
		t.Fatalf("uncapped pass must query every candidate, queried %d of 5", calls)
	}
	if expected.TotalRecords != 3 {
		t.Fatalf("expected invisible candidates removed (total=3), got %d", expected.TotalRecords)
	}
	if stats.Applied {
		t.Fatal("cap=0 must not report sampling as applied")
	}
}

func TestBuildTruthPassExpectedCappedSpotCheckPasses(t *testing.T) {
	candidates := spotCheckCandidates(50)
	calls := 0
	isVisible := func(_ context.Context, _ GeneratedRecord) (bool, error) {
		calls++
		return true, nil
	}
	workload := WorkloadDefinition{Name: "spot-check-test", PageSize: 20, PageNumber: 1}
	expected, stats, err := buildTruthPassExpected(context.Background(), isVisible, workload, 20, candidates, 10, 42)
	if err != nil {
		t.Fatalf("capped spot check failed: %v", err)
	}
	if calls != 10 {
		t.Fatalf("capped pass must query exactly cap candidates, queried %d", calls)
	}
	if expected.TotalRecords != 50 {
		t.Fatalf("capped pass must keep the full candidate set as expected (total=50), got %d", expected.TotalRecords)
	}
	if !stats.Applied || stats.Cap != 10 || stats.Candidates != 50 || stats.Sampled != 10 {
		t.Fatalf("unexpected sample stats: %+v", stats)
	}
}

func TestBuildTruthPassExpectedCappedSpotCheckFailsHardOnInvisibleCandidate(t *testing.T) {
	candidates := spotCheckCandidates(50)
	isVisible := func(_ context.Context, _ GeneratedRecord) (bool, error) {
		return false, nil
	}
	workload := WorkloadDefinition{Name: "spot-check-test", PageSize: 20, PageNumber: 1}
	_, _, err := buildTruthPassExpected(context.Background(), isVisible, workload, 20, candidates, 10, 42)
	if err == nil {
		t.Fatal("expected hard failure when a sampled candidate is not visible")
	}
	if !strings.Contains(err.Error(), "spot check failed") {
		t.Fatalf("error must identify the spot-check divergence, got: %v", err)
	}
}
