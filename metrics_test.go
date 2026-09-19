package forma

import (
	"context"
	"os/exec"
	"sort"
	"strings"
	"testing"
)

// TestMetricCatalogueIsWellFormed pins the descriptor contract an embedder
// pre-registers instruments from: every wire name is unique and snake_case,
// every descriptor names a kind and a unit from the fixed sets, and every
// label key is unique within its descriptor. A malformed descriptor is caught
// here, at construction, never at emission time inside a write path.
func TestMetricCatalogueIsWellFormed(t *testing.T) {
	kinds := map[MetricKind]bool{MetricKindCounter: true, MetricKindGauge: true, MetricKindHistogram: true}
	units := map[MetricUnit]bool{MetricUnitCount: true, MetricUnitRatio: true, MetricUnitMilliseconds: true}
	seen := map[string]bool{}
	catalogue := MetricCatalogue()
	if len(catalogue) == 0 {
		t.Fatal("MetricCatalogue is empty")
	}
	for _, d := range catalogue {
		if d.Name == "" || strings.ToLower(d.Name) != d.Name || strings.ContainsAny(d.Name, " -.") {
			t.Errorf("descriptor %q: name must be a lowercase snake_case wire name", d.Name)
		}
		if seen[d.Name] {
			t.Errorf("descriptor %q is listed twice", d.Name)
		}
		seen[d.Name] = true
		if !kinds[d.Kind] {
			t.Errorf("descriptor %q: kind %q is not a MetricKind constant", d.Name, d.Kind)
		}
		if !units[d.Unit] {
			t.Errorf("descriptor %q: unit %q is not a MetricUnit constant", d.Name, d.Unit)
		}
		if d.Help == "" {
			t.Errorf("descriptor %q has no Help text", d.Name)
		}
		labels := map[string]bool{}
		for _, key := range d.Labels {
			if key == "" || labels[key] {
				t.Errorf("descriptor %q: label key %q is empty or repeated", d.Name, key)
			}
			labels[key] = true
		}
	}
}

// TestMetricCatalogueIsACopy: callers may sort or edit what they get back
// without touching Forma's descriptors, and LookupMetric hands out the same
// contract MetricCatalogue lists.
func TestMetricCatalogueIsACopy(t *testing.T) {
	got := MetricCatalogue()
	sort.Slice(got, func(i, j int) bool { return got[i].Name > got[j].Name })
	got[0].Labels[0] = "mutated"
	got[0].Name = "mutated"

	if _, ok := LookupMetric("mutated"); ok {
		t.Fatal("mutating the returned slice reached the catalogue")
	}
	for _, d := range MetricCatalogue() {
		for _, key := range d.Labels {
			if key == "mutated" {
				t.Fatalf("mutating a returned Labels slice reached descriptor %q", d.Name)
			}
		}
		looked, ok := LookupMetric(d.Name)
		if !ok {
			t.Fatalf("LookupMetric(%q) missed a catalogued name", d.Name)
		}
		if looked.Kind != d.Kind || looked.Unit != d.Unit || strings.Join(looked.Labels, ",") != strings.Join(d.Labels, ",") {
			t.Fatalf("LookupMetric(%q) = %+v, catalogue lists %+v", d.Name, looked, d)
		}
	}
	if _, ok := LookupMetric("no_such_metric"); ok {
		t.Fatal("LookupMetric found a name that is not catalogued")
	}
}

func TestMetricDescriptorLabelKeysMatch(t *testing.T) {
	d := MetricDescriptor{Labels: []string{"schema_id", "kind"}}
	for _, tc := range []struct {
		labels map[string]string
		want   bool
	}{
		{map[string]string{"schema_id": "1", "kind": "required"}, true},
		{map[string]string{"schema_id": "1"}, false},
		{map[string]string{"schema_id": "1", "kind": "x", "extra": "y"}, false},
		{map[string]string{"schema_id": "1", "stage": "x"}, false},
		{nil, false},
	} {
		if got := d.LabelKeysMatch(tc.labels); got != tc.want {
			t.Errorf("LabelKeysMatch(%v) = %v, want %v", tc.labels, got, tc.want)
		}
	}
	if !(MetricDescriptor{}).LabelKeysMatch(nil) {
		t.Error("a descriptor with no labels must accept a nil map")
	}
}

func TestMetricEmitterFuncAdapts(t *testing.T) {
	var got Metric
	var e MetricEmitter = MetricEmitterFunc(func(_ context.Context, m Metric) { got = m })
	e.EmitMetric(context.Background(), Metric{Name: "x", Value: 2})
	if got.Name != "x" || got.Value != 2 {
		t.Fatalf("MetricEmitterFunc did not forward the metric: %+v", got)
	}
}

// TestLibraryAPIDependsOnNoTelemetryBackend is the one dependency-level guard
// #423 needs: the public API surface (the root package and the factory an
// embedder builds from) must not link a metrics backend. An embedder adapts
// MetricEmitter onto the backend of its choice; Forma never chooses one for
// it, so neither Prometheus nor an OpenTelemetry SDK or exporter may appear
// in the transitive dependency closure of those two packages.
func TestLibraryAPIDependsOnNoTelemetryBackend(t *testing.T) {
	out, err := exec.Command("go", "list", "-deps", ".", "./factory").CombinedOutput()
	if err != nil {
		t.Fatalf("go list -deps . ./factory: %v\n%s", err, out)
	}
	forbidden := []string{
		"github.com/prometheus/client_golang",
		"go.opentelemetry.io/otel/sdk",
		"go.opentelemetry.io/otel/exporters",
	}
	for _, dep := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		for _, prefix := range forbidden {
			if strings.HasPrefix(dep, prefix) {
				t.Errorf("library API depends on telemetry backend package %s", dep)
			}
		}
	}
}
