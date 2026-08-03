//go:build e2e

package production

import (
	"context"
	"maps"
	"testing"

	"github.com/lychee-technology/forma/internal/manifest"
)

// assertEntriesStampedToByteTruth pins the #256 correctness bar: every
// manifest entry a scenario writes carries a Columns stamp EQUAL to a live
// DESCRIBE of the object it names — a read that trusts the stamp is
// indistinguishable from one that probes the footer.
//
// Equality is the bar, not containment: a stamp that merely satisfies the
// #189 system-column invariant would pass parquetcheck.Check while
// under-reporting the file's attribute columns, and the #255 column union
// would then augment a NULL alias over a column the file really carries.
// That is the one channel where a stamp can still fail a query, so it is the
// one this assertion has to close (see the Validate doc comment in
// internal/federated/parquet_schema_validation.go).
//
// The empty-slice guard is a vacuity guard: a scenario whose writer produced
// no entries would otherwise satisfy this loop silently.
func assertEntriesStampedToByteTruth(ctx context.Context, t *testing.T, env *Env, label string, entries []manifest.FileEntry) {
	t.Helper()
	if len(entries) == 0 {
		t.Fatalf("%s: no manifest entries to check; the stamp assertion would be vacuous", label)
	}
	for _, entry := range entries {
		if len(entry.Columns) == 0 {
			t.Fatalf("%s: manifest entry %s (tier %s) is unstamped", label, entry.Path, entry.Tier)
		}
		probed := describeParquetCols(ctx, t, env, entry.Path)
		if !maps.Equal(entry.Columns, probed) {
			t.Fatalf("%s: manifest entry %s stamp diverges from its parquet footer:\n stamp: %#v\n probe: %#v",
				label, entry.Path, entry.Columns, probed)
		}
	}
	t.Logf("stamp==probe: %s — %d manifest entr(ies) verified against a live DESCRIBE", label, len(entries))
}
