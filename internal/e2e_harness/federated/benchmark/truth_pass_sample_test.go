package benchmark

import (
	"reflect"
	"testing"
)

func TestTruthPassSampleIndicesNotAppliedWhenCapCoversTotal(t *testing.T) {
	if got := truthPassSampleIndices(42, "eav-selective-page", 100, 0); got != nil {
		t.Fatalf("cap=0 must disable sampling, got %v", got)
	}
	if got := truthPassSampleIndices(42, "eav-selective-page", 100, 100); got != nil {
		t.Fatalf("total<=cap must disable sampling, got %v", got)
	}
}

func TestTruthPassSampleIndicesDeterministicAndBounded(t *testing.T) {
	first := truthPassSampleIndices(42, "eav-selective-page", 1000, 25)
	second := truthPassSampleIndices(42, "eav-selective-page", 1000, 25)
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
	base := truthPassSampleIndices(42, "eav-selective-page", 1000, 25)
	otherSeed := truthPassSampleIndices(43, "eav-selective-page", 1000, 25)
	otherWorkload := truthPassSampleIndices(42, "hot-selective-page", 1000, 25)
	if reflect.DeepEqual(base, otherSeed) && reflect.DeepEqual(base, otherWorkload) {
		t.Fatal("expected seed and workload name to influence the sample")
	}
}
