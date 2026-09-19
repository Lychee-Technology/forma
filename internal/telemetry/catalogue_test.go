package telemetry

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// emission is one recorded call through the emitter hook.
type emission struct {
	name   string
	labels map[string]string
	value  any
}

// record swaps in a capturing emitter for the test's lifetime and returns the
// slice every Emit* helper appends to.
func record(t *testing.T) *[]emission {
	t.Helper()
	var got []emission
	RegisterTelemetryEmitter(func(_ context.Context, name string, labels map[string]string, value any) {
		got = append(got, emission{name: name, labels: labels, value: value})
	})
	t.Cleanup(func() { RegisterTelemetryEmitter(nil) })
	return &got
}

// helperCalls exercises every exported Emit* helper once, keyed by the helper's
// Go name. TestEveryEmitHelperIsInTheCatalogue scans telemetry.go for the
// helpers that exist and fails if one is missing from this table, so adding a
// helper without adding it here — and without a catalogue descriptor — cannot
// pass.
var helperCalls = map[string]func(ctx context.Context){
	"EmitLatency":            func(ctx context.Context) { EmitLatency(ctx, "execution", 12) },
	"EmitRowCount":           func(ctx context.Context) { EmitRowCount(ctx, "pg", 3) },
	"EmitPushdownEfficiency": func(ctx context.Context) { EmitPushdownEfficiency(ctx, 7, 0.5) },
	"EmitCompactionManifestContractViolation": func(ctx context.Context) { EmitCompactionManifestContractViolation(ctx, 7) },
	"EmitCompactionDirtyRatio":                func(ctx context.Context) { EmitCompactionDirtyRatio(ctx, 7, 0.25) },
	"EmitCompactionRewritePending":            func(ctx context.Context) { EmitCompactionRewritePending(ctx, 7) },
	"EmitParquetChecksumMismatch":             func(ctx context.Context) { EmitParquetChecksumMismatch(ctx, 7) },
	"EmitCompactionRewriteApplied":            func(ctx context.Context) { EmitCompactionRewriteApplied(ctx, 7) },
	"EmitReportOnlyValidationViolation":       func(ctx context.Context) { EmitReportOnlyValidationViolation(ctx, 7, "lead", "required") },
}

// exportedEmitHelpers parses telemetry.go and returns the names of its exported
// top-level functions whose name starts with "Emit".
func exportedEmitHelpers(t *testing.T) []string {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), "telemetry.go", nil, 0)
	require.NoError(t, err)
	var names []string
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv != nil || !fn.Name.IsExported() || !strings.HasPrefix(fn.Name.Name, "Emit") {
			continue
		}
		names = append(names, fn.Name.Name)
	}
	sort.Strings(names)
	return names
}

// TestEveryEmitHelperIsInTheCatalogue is the label contract (#423): every
// Emit* helper must emit a catalogued name, exactly the descriptor's label keys,
// and a value of the Go type its kind prescribes. An adapter can then map any
// emission onto its backend from the descriptor alone.
func TestEveryEmitHelperIsInTheCatalogue(t *testing.T) {
	helpers := exportedEmitHelpers(t)
	require.NotEmpty(t, helpers, "no Emit* helpers found — wrong file scanned?")

	for _, helper := range helpers {
		call, ok := helperCalls[helper]
		require.Truef(t, ok, "helper %s is not exercised by helperCalls; add it and a catalogue descriptor", helper)

		got := record(t)
		call(context.Background())
		require.Lenf(t, *got, 1, "helper %s emitted %d times, want 1", helper, len(*got))
		e := (*got)[0]

		desc, ok := Lookup(e.name)
		require.Truef(t, ok, "helper %s emitted %q, which is not in the catalogue", helper, e.name)
		require.Equalf(t, desc.Name, e.name, "helper %s", helper)

		gotKeys := make([]string, 0, len(e.labels))
		for k := range e.labels {
			gotKeys = append(gotKeys, k)
		}
		sort.Strings(gotKeys)
		wantKeys := append([]string(nil), desc.Labels...)
		sort.Strings(wantKeys)
		require.Equalf(t, wantKeys, gotKeys, "helper %s: label keys differ from descriptor %s", helper, desc.Name)

		_, err := desc.Kind.Coerce(e.value)
		require.NoErrorf(t, err, "helper %s: value %T does not fit kind %s", helper, e.value, desc.Kind)
	}
}

// TestCatalogueDescriptorsAreWellFormed pins the shape adapters rely on: unique
// names, a known kind, non-empty labels, and label names Prometheus accepts.
func TestCatalogueDescriptorsAreWellFormed(t *testing.T) {
	seen := map[string]bool{}
	for _, d := range Catalogue() {
		require.False(t, seen[d.Name], "duplicate descriptor %s", d.Name)
		seen[d.Name] = true
		require.Contains(t, []Kind{KindCounter, KindGauge, KindHistogram}, d.Kind, d.Name)
		require.NotEmpty(t, d.Help, "%s has no help text", d.Name)
		for _, l := range d.Labels {
			require.Regexp(t, `^[a-zA-Z_][a-zA-Z0-9_]*$`, l, "%s label %q", d.Name, l)
		}
	}
	require.Len(t, Catalogue(), len(helperCalls))
}

// TestKindCoerce pins the value contract per kind: counters and histograms take
// int64, gauges take float64, and anything else is refused rather than guessed.
func TestKindCoerce(t *testing.T) {
	v, err := KindCounter.Coerce(int64(3))
	require.NoError(t, err)
	require.Equal(t, 3.0, v)

	v, err = KindHistogram.Coerce(int64(250))
	require.NoError(t, err)
	require.Equal(t, 250.0, v)

	v, err = KindGauge.Coerce(0.75)
	require.NoError(t, err)
	require.Equal(t, 0.75, v)

	_, err = KindCounter.Coerce(0.5)
	require.Error(t, err)
	_, err = KindGauge.Coerce(int64(1))
	require.Error(t, err)
	_, err = KindCounter.Coerce("1")
	require.Error(t, err)
}
