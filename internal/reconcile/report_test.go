package reconcile

import (
	"strings"
	"testing"
)

func TestReport_HasResidualDiscrepancies(t *testing.T) {
	tests := []struct {
		name   string
		report Report
		want   bool
	}{
		{"clean", Report{Schemas: []SchemaReport{{SchemaID: 1}}}, false},
		{"dangling only", Report{Schemas: []SchemaReport{
			{SchemaID: 1, Dangling: []string{"data/1/x.parquet"}},
		}}, true},
		{"skipped locked schema", Report{Schemas: []SchemaReport{
			{SchemaID: 1, Skipped: true},
		}}, true},
		{"unknown shape", Report{Schemas: []SchemaReport{
			{SchemaID: 1, Unknown: []string{"data/1/weird.parquet"}},
		}}, true},
		{"delta orphan left unrepaired", Report{Schemas: []SchemaReport{
			{SchemaID: 1, DeltaOrphans: []string{"data/1/a.parquet"}},
		}}, true},
		{"repaired delta orphan is resolved", Report{Schemas: []SchemaReport{
			{SchemaID: 1, DeltaOrphans: []string{"data/1/a.parquet"}, Repaired: []string{"data/1/a.parquet"}},
		}}, false},
		{"gc deleted base orphan is resolved", Report{Schemas: []SchemaReport{
			{SchemaID: 1, BaseOrphans: []string{"data/1/base-x.parquet"}, Deleted: []string{"data/1/base-x.parquet"}},
		}}, false},
		{"tmp orphan within grace stays residual", Report{Schemas: []SchemaReport{
			{SchemaID: 1, TmpOrphans: []string{"data/1/_tmp/x.parquet"}},
		}}, true},
		{"unverifiable is informational only", Report{Schemas: []SchemaReport{
			{SchemaID: 1, Unverifiable: []string{"s3://other/x.parquet"}},
		}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.report.HasResidualDiscrepancies(); got != tt.want {
				t.Fatalf("HasResidualDiscrepancies() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReport_RenderNamesExactKeys(t *testing.T) {
	r := Report{Schemas: []SchemaReport{{
		SchemaID:     7,
		DeltaOrphans: []string{"data/7/a.parquet"},
		BaseOrphans:  []string{"data/7/base-b.parquet"},
		TmpOrphans:   []string{"data/7/_tmp/c.parquet"},
		Dangling:     []string{"data/7/gone.parquet"},
		Repaired:     []string{"data/7/a.parquet"},
		Deleted:      []string{"data/7/base-b.parquet"},
	}, {
		SchemaID: 9,
		Skipped:  true,
	}}}

	var sb strings.Builder
	r.Render(&sb)
	out := sb.String()

	for _, want := range []string{
		"schema 7",
		"data/7/a.parquet",
		"data/7/base-b.parquet",
		"data/7/_tmp/c.parquet",
		"data/7/gone.parquet",
		"schema 9",
		"skipped",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("Render output missing %q:\n%s", want, out)
		}
	}
}
