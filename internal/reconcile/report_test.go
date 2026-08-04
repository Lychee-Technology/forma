package reconcile

import (
	"bytes"
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
		{"refused init orphan is residual", Report{Schemas: []SchemaReport{
			{SchemaID: 1, BaseOrphans: []string{"data/1/a_b.parquet"}, InitPromotionRefusal: "reason"},
		}}, true},
		{"partial promotion of base orphans is residual", Report{Schemas: []SchemaReport{
			{SchemaID: 1, BaseOrphans: []string{"data/1/k1", "data/1/k2"}, PromotedBase: []string{"data/1/k1"}},
		}}, true},
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

func TestResidual_PromotedBaseCountsResolved(t *testing.T) {
	s := SchemaReport{
		BaseOrphans:  []string{"data/7/a_b.parquet"},
		PromotedBase: []string{"data/7/a_b.parquet"},
	}
	if s.Residual() {
		t.Fatal("promoted base orphan must not be residual")
	}
}

func TestRender_PromotionLines(t *testing.T) {
	var buf bytes.Buffer
	Report{Schemas: []SchemaReport{{
		SchemaID:             7,
		BaseOrphans:          []string{"data/7/a_b.parquet"},
		InitPromotionRefusal: "orphan set covers 2 of 3 live rows; a partial init export must not replace the base tier",
	}}}.Render(&buf)
	out := buf.String()
	if !strings.Contains(out, "init promotion refused: orphan set covers 2 of 3 live rows") {
		t.Fatalf("missing refusal line in:\n%s", out)
	}

	buf.Reset()
	Report{Schemas: []SchemaReport{{
		SchemaID:     7,
		BaseOrphans:  []string{"data/7/a_b.parquet"},
		PromotedBase: []string{"data/7/a_b.parquet"},
	}}}.Render(&buf)
	if !strings.Contains(buf.String(), "promoted base-init: data/7/a_b.parquet") {
		t.Fatalf("missing promoted line in:\n%s", buf.String())
	}
}

func TestRender_RefusedInitOrphanNotClean(t *testing.T) {
	var buf bytes.Buffer
	Report{Schemas: []SchemaReport{{
		SchemaID:             7,
		InitPromotionRefusal: "orphan set covers 2 of 3 live rows; a partial init export must not replace the base tier",
	}}}.Render(&buf)
	out := buf.String()
	if strings.Contains(out, "schema 7: clean") {
		t.Fatalf("refused init orphan must not render clean:\n%s", out)
	}
	if !strings.Contains(out, "init promotion refused:") {
		t.Fatalf("missing refusal line in:\n%s", out)
	}
}

func TestRender_PromotionOnlyNotClean(t *testing.T) {
	var buf bytes.Buffer
	Report{Schemas: []SchemaReport{{
		SchemaID:     7,
		PromotedBase: []string{"data/7/a_b.parquet"},
	}}}.Render(&buf)
	out := buf.String()
	if strings.Contains(out, "schema 7: clean") {
		t.Fatalf("promotion-only schema must not render clean:\n%s", out)
	}
	if !strings.Contains(out, "promoted base-init: data/7/a_b.parquet") {
		t.Fatalf("missing promoted line in:\n%s", out)
	}
}
