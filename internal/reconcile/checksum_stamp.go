package reconcile

import (
	"context"

	"go.uber.org/zap"

	"github.com/lychee-technology/forma/internal/cdc"
)

// stampChecksum hashes an object's bytes for a manifest entry the reconciler
// is about to publish, so a repaired or promoted entry carries the same #347
// content stamp the write paths (flush, cdc-init, compaction rewrite) put on
// theirs. Reconcile did not write these objects, so the hash necessarily
// blesses the bytes the store holds NOW — which is exactly what a later
// --verify-checksums scrub compares against, and the only stamp that can be
// taken for an object adopted after the fact.
//
// Best-effort, following the same rule as every write-side stamp: a nil
// reader (a caller that wired none — only --verify-checksums requires it) or a
// failed GET leaves Checksum empty and the entry is published anyway. Repair
// and promotion exist to recover data from objects that already exist;
// refusing to adopt one because its hash could not be taken would trade a
// recovery for a scrub-coverage gap the scrub already counts and reports.
//
// The caller supplies the warning message so each stamping site names the
// entry kind it failed to stamp.
func (r *Reconciler) stampChecksum(ctx context.Context, schemaID int16, key, warning string) string {
	if r.Objects == nil {
		return ""
	}
	sum, err := cdc.ObjectSHA256(ctx, r.Objects, r.Bucket, key)
	if err != nil {
		r.Logger.Warn(warning,
			zap.Int16("schema_id", schemaID), zap.String("key", key), zap.Error(err))
		return ""
	}
	return sum
}
