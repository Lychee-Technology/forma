package reconcile

import (
	"context"
	"fmt"
	"strings"

	"go.uber.org/zap"

	"github.com/lychee-technology/forma/internal/cdc"
	"github.com/lychee-technology/forma/internal/manifest"
)

// verifyChecksums re-hashes each manifest-listed object and compares the
// digest with the entry's #347 content stamp. It is the sibling of
// verifyStamps, one layer deeper: a stamp proves the object's footer SHAPE
// (which columns, of which types), while a checksum proves its BYTES. A
// rewrite that preserves the column set — a re-encode with different values,
// a truncated row group, a restored-from-the-wrong-generation copy — passes
// every shape check the read path and verifyStamps apply, and then decodes
// into wrong answers with no error anywhere. This pass is the #347 scrub and
// the only detector of that silent mis-decode class outside the compaction
// input gate, which sees a source only on the run that consumes it.
//
// A read failure is a tool failure (exit 1), never a corruption verdict —
// mirroring confirmDangling's storage-outage rule, and the same reason the
// skip set below is deliberately generous. Skipped, in order:
//
//   - entries with an empty Checksum, so there is nothing to compare against
//     (field presence IS the format version signal, the same rule #256 set for
//     Columns). Two causes, both legitimate: the entry predates stamping and
//     is never backfilled, OR its write-side hash failed — all three stamping
//     paths are best-effort and publish unstamped after a WARN rather than
//     failing the export. These are counted and reported: the count is how
//     much of the manifest this scrub cannot cover, which is the coverage
//     signal an operator needs to read the clean verdict correctly;
//   - paths normalizeKey cannot resolve, and keys outside this schema's data
//     prefix — both are the unverifiable class diffSchema already reports,
//     and neither is covered by the listing that would prove it present;
//   - every dangling candidate this run did NOT prove present — a candidate a
//     concurrent compactor spliced out and deleted mid-run is unconfirmed yet
//     would still fail the GET and escalate the whole schema to a spurious
//     exit 1. Candidates confirmDangling probed and found ALIVE keep their
//     coverage: this run already proved their bytes reachable.
//
// The skip list is the caller's job to compute (unprovenDangling); this
// function does not re-derive it.
func (r *Reconciler) verifyChecksums(ctx context.Context, schemaID int16, m *manifest.Manifest,
	unprovenDangling []string) (divergences []string, skippedUnstamped int, err error) {
	if r.Objects == nil {
		return nil, 0, fmt.Errorf("checksum verification requested for schema %d but object reader is not configured", schemaID)
	}
	skip := make(map[string]bool, len(unprovenDangling))
	for _, key := range unprovenDangling {
		skip[key] = true
	}
	prefix := schemaDataPrefix(r.DataPrefix, schemaID)

	for _, f := range m.Files {
		if f.Checksum == "" {
			skippedUnstamped++ // no checksum to compare against: legacy, or a best-effort write-side hash that failed
			continue
		}
		key, ok := normalizeKey(r.Bucket, f.Path)
		if !ok || !strings.HasPrefix(key, prefix) || skip[key] {
			continue // unverifiable, or an unprovable candidate: already reported
		}
		actual, hashErr := cdc.ObjectSHA256(ctx, r.Objects, r.Bucket, key)
		if hashErr != nil {
			return nil, 0, fmt.Errorf("re-hash %s for checksum verification: %w", key, hashErr)
		}
		if actual == f.Checksum {
			continue
		}
		divergence := fmt.Sprintf("%s: checksum stamp %s vs bytes %s", key, f.Checksum, actual)
		r.Logger.Warn("manifest checksum stamp diverges from object bytes",
			zap.Int16("schema_id", schemaID),
			zap.String("key", key),
			zap.String("stamped", f.Checksum),
			zap.String("actual", actual))
		divergences = append(divergences, divergence)
	}
	return divergences, skippedUnstamped, nil
}
