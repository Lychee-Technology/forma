package reconcile

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	"go.uber.org/zap"

	"github.com/lychee-technology/forma/internal/manifest"
)

// absentColumn is the divergence rendering for a column present on only one
// side of the comparison.
const absentColumn = "(absent)"

// verifyStamps compares each manifest-listed entry's #256 column stamp with
// the object's actual parquet footer. The read path trusts a stamp that
// passes the system-column invariant without touching bytes — that is the
// #256 optimization — so an out-of-band overwrite that keeps the stamp
// shape valid (missing deleted_at, a re-typed row_id, a dropped attribute
// column) is invisible there. This offline pass IS the byte truth: a full
// map comparison, strictly stronger than the scan guard, at zero read-path
// cost. A probe failure is a tool failure (exit 1), never a divergence
// verdict — mirroring confirmDangling's storage-outage rule, which is also
// why the skip set is deliberately generous. Skipped, in order:
//
//   - entries with nil Columns — legacy (lazy fallback, no backfill: the
//     #256 contract), so there is nothing to compare against;
//   - paths normalizeKey cannot resolve, and keys outside this schema's data
//     prefix — both are the unverifiable class diffSchema already reports,
//     and neither is covered by the listing that would prove it present;
//   - every dangling **candidate** (the pre-confirmation list), not merely
//     the confirmed ones. The tradeoff is deliberate: a candidate that
//     confirmDangling proves still live loses stamp coverage for this run,
//     which is cheap and self-correcting on the next pass, whereas probing
//     an object a concurrent compactor spliced out and deleted mid-run would
//     fail and escalate the whole schema to a spurious exit 1.
func (r *Reconciler) verifyStamps(ctx context.Context, schemaID int16, m *manifest.Manifest, danglingCandidates []string) ([]string, error) {
	if r.Stats == nil {
		return nil, fmt.Errorf("stamp verification requested for schema %d but stats reader is not configured", schemaID)
	}
	skip := make(map[string]bool, len(danglingCandidates))
	for _, key := range danglingCandidates {
		skip[key] = true
	}
	prefix := schemaDataPrefix(r.DataPrefix, schemaID)

	var divergences []string
	for _, f := range m.Files {
		if len(f.Columns) == 0 {
			continue // legacy unstamped entry: the read path probes it lazily
		}
		key, ok := normalizeKey(r.Bucket, f.Path)
		if !ok || !strings.HasPrefix(key, prefix) || skip[key] {
			continue // unverifiable or dangling candidate: already reported
		}
		cols, err := r.Stats.FileColumns(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("probe footer of %s for stamp verification: %w", key, err)
		}
		if maps.Equal(f.Columns, cols) {
			continue
		}
		col, stamped, actual := firstColumnDivergence(f.Columns, cols)
		divergence := fmt.Sprintf("%s: column %q stamp %q vs footer %q", key, col, stamped, actual)
		r.Logger.Warn("manifest column stamp diverges from parquet footer",
			zap.Int16("schema_id", schemaID),
			zap.String("key", key),
			zap.String("column", col),
			zap.String("stamp_type", stamped),
			zap.String("footer_type", actual))
		divergences = append(divergences, divergence)
	}
	return divergences, nil
}

// firstColumnDivergence names the first column (by sorted union of both key
// sets, so the message is deterministic regardless of map iteration order) on
// which stamp and footer disagree, with absentColumn standing in for a side
// that does not carry the column at all. Callers must only reach it with maps
// already known to differ; the zero return is unreachable in that contract.
func firstColumnDivergence(stamp, footer map[string]string) (col, stamped, actual string) {
	names := make([]string, 0, len(stamp)+len(footer))
	for name := range stamp {
		names = append(names, name)
	}
	for name := range footer {
		if _, ok := stamp[name]; !ok {
			names = append(names, name)
		}
	}
	slices.Sort(names)

	for _, name := range names {
		stampType, inStamp := stamp[name]
		footerType, inFooter := footer[name]
		if inStamp && inFooter && stampType == footerType {
			continue
		}
		if !inStamp {
			stampType = absentColumn
		}
		if !inFooter {
			footerType = absentColumn
		}
		return name, stampType, footerType
	}
	return "", "", ""
}
