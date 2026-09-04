package cdc

import (
	"strings"
	"testing"
)

const (
	testRowMin  = "018f05c0-0000-7000-8000-000000000001"
	testRowMax  = "018f05c0-0000-7000-8000-000000000009"
	testFileID  = "019bed54-48eb-7cdc-aed3-8d38ec9c1394"
	testFileID2 = "019bed54-48eb-7cdc-aed3-8d38ec9c1395"
)

// Init base keys are write-once (#416): the same row range exported twice
// must land on two different keys, so a re-run never overwrites an object
// the manifest still lists under the previous run's stamp.
func TestBuildBasePath_SameRangeDistinctFileIDsMintDistinctKeys(t *testing.T) {
	a := BuildBasePath("data", 7, testRowMin, testRowMax, testFileID)
	b := BuildBasePath("data", 7, testRowMin, testRowMax, testFileID2)
	if a == b {
		t.Fatalf("two file ids rendered the same key %q; init keys must be write-once", a)
	}
	want := "data/7/" + testRowMin + "_" + testRowMax + "_" + testFileID + ".parquet"
	if a != want {
		t.Fatalf("BuildBasePath = %q, want %q", a, want)
	}
	if strings.HasPrefix(a, "data//") {
		t.Fatalf("prefix trailing slash not normalized: %q", a)
	}
}

func TestParseInitBaseStem(t *testing.T) {
	tests := []struct {
		name     string
		stem     string
		min, max string
		ok       bool
	}{
		{"write-once shape", testRowMin + "_" + testRowMax + "_" + testFileID, testRowMin, testRowMax, true},
		{"legacy deterministic shape", testRowMin + "_" + testRowMax, testRowMin, testRowMax, true},
		{"delta uuid", testFileID, "", "", false},
		{"merged base", "base-" + testFileID, "", "", false},
		{"junk two parts", "a_b", "", "", false},
		{"junk file id", testRowMin + "_" + testRowMax + "_nope", "", "", false},
		{"four parts", testRowMin + "_" + testRowMax + "_" + testFileID + "_" + testFileID2, "", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			min, max, ok := ParseInitBaseStem(tt.stem)
			if ok != tt.ok {
				t.Fatalf("ParseInitBaseStem(%q) ok = %v, want %v", tt.stem, ok, tt.ok)
			}
			if min != tt.min || max != tt.max {
				t.Fatalf("ParseInitBaseStem(%q) = %q,%q want %q,%q", tt.stem, min, max, tt.min, tt.max)
			}
		})
	}
}
