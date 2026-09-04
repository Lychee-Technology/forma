package compaction

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/lychee-technology/forma/internal/cdc"
	"github.com/lychee-technology/forma/internal/manifest"
	"github.com/lychee-technology/forma/internal/telemetry"
	"go.uber.org/zap"
)

// ErrSourceChecksumMismatch marks a rewrite input whose bytes no longer hash
// to its manifest stamp. The rewrite must NOT proceed: runRewrite merges the
// sources into the new base and splices them out of the manifest, so this
// gate is the last moment silent corruption (#347) is both detectable and
// attributable to a manifest-listed object. The source bytes may remain as
// unlisted GC candidates, but the read path will no longer consult that
// named entry after the rewrite. Probe failures never map to this error — a
// failed GET is transient infrastructure, not a corruption verdict.
var ErrSourceChecksumMismatch = errors.New("compaction source checksum mismatch")

// ErrForeignSource marks a rewrite source whose manifest path names a bucket
// other than the compactor's own (#417 ruling: cross-bucket manifest paths
// are not a supported compaction input). The compactor cannot verify, stat or
// delete such an object through its own client, yet MergeToTmp would fold its
// bytes into a new base in this bucket and spliceManifest would drop its
// listed name — an unverified relocation across a bucket boundary past a
// fail-closed gate. Terminal for the pass, exactly like a checksum mismatch:
// the fix is to correct the manifest, not to retry.
var ErrForeignSource = errors.New("compaction source outside compactor bucket")

// verifyRewriteInputs is the fail-closed gate runRewrite runs before anything
// is merged: past it the sources are folded into one object and spliced out
// of the manifest, so a corrupt input would be laundered into the new base
// and lose its listed name — the retained bytes (#461) are unlisted orphans
// on a GC countdown, not a named copy anyone would consult (#347). A source in
// another bucket is refused first (#417): objectURI would hand it to DuckDB
// verbatim, but nothing downstream could verify, stat or delete it. Returns
// the first refusal; both gates log the offending source at ERROR themselves.
func (c *Compactor) verifyRewriteInputs(ctx context.Context, schemaID int16, sources []manifest.FileEntry) error {
	if err := c.rejectForeignSources(schemaID, sources); err != nil {
		return err
	}
	if err := c.verifySourceChecksums(ctx, schemaID, sources); err != nil {
		return err
	}
	return nil
}

// rejectForeignSources refuses a rewrite whose sources include a path outside
// the compactor's bucket. It is a scope check, not a checksum check, so it
// runs regardless of SkipInputChecksumVerify or ObjectReader and applies to
// unstamped entries too. Logs the refused source at ERROR before returning.
func (c *Compactor) rejectForeignSources(schemaID int16, sources []manifest.FileEntry) error {
	for _, f := range sources {
		if _, ok := c.bucketRelativeKey(f.Path); ok {
			continue
		}
		c.Logger.Error("rewrite source lies outside the compactor bucket; refusing to merge (#417)",
			zap.Int16("schema_id", schemaID), zap.String("path", f.Path), zap.String("bucket", c.Bucket))
		return fmt.Errorf("rewrite source %s for schema %d is outside bucket %s: %w",
			f.Path, schemaID, c.Bucket, ErrForeignSource)
	}
	return nil
}

// verifySourceChecksums re-hashes every stamped rewrite source and compares
// against the manifest stamp. Unstamped (legacy) entries are skipped — field
// presence is the format version signal. A stamped source outside the
// compactor's bucket cannot be hashed and is refused (ErrForeignSource).
// Disabled when the config opts out or no ObjectReader is wired. Every
// refusal is logged at ERROR here, so a standalone caller needs no log of
// its own.
func (c *Compactor) verifySourceChecksums(ctx context.Context, schemaID int16, sources []manifest.FileEntry) error {
	if c.Config.SkipInputChecksumVerify || c.ObjectReader == nil {
		return nil
	}
	for _, f := range sources {
		if f.Checksum == "" {
			continue // legacy unstamped entry: nothing to compare against
		}
		key, ok := c.bucketRelativeKey(f.Path)
		if !ok {
			// Unreachable through this compactor's client: a stamped source
			// that cannot be hashed must not merge. runRewrite refuses such
			// paths earlier via rejectForeignSources; this keeps the gate
			// fail-closed on its own (#417).
			c.Logger.Error("stamped rewrite source lies outside the compactor bucket; cannot hash it, refusing to merge (#417)",
				zap.Int16("schema_id", schemaID), zap.String("path", f.Path), zap.String("bucket", c.Bucket))
			return fmt.Errorf("verify rewrite source %s for schema %d: outside bucket %s: %w",
				f.Path, schemaID, c.Bucket, ErrForeignSource)
		}
		actual, err := cdc.ObjectSHA256(ctx, c.ObjectReader, c.Bucket, key)
		if err != nil {
			return fmt.Errorf("verify rewrite source %s for schema %d: %w", f.Path, schemaID, err)
		}
		if actual != f.Checksum {
			telemetry.EmitParquetChecksumMismatch(ctx, schemaID)
			c.Logger.Error("rewrite source failed checksum verification; refusing to merge",
				zap.Int16("schema_id", schemaID), zap.String("key", f.Path),
				zap.String("stamped", f.Checksum), zap.String("actual", actual))
			return fmt.Errorf("rewrite source %s for schema %d: stamped %s, bytes hash to %s: %w",
				f.Path, schemaID, f.Checksum, actual, ErrSourceChecksumMismatch)
		}
	}
	return nil
}

// bucketRelativeKey resolves a manifest path to a key inside the compactor's
// bucket, reporting false for an absolute URI naming a different bucket and
// for a path that resolves to no key at all ("", "/", "s3://<bucket>/"): an
// empty key names nothing the compactor could merge, hash or delete, so it is
// out of scope like a foreign one rather than passed on to fail downstream.
// It is the single path rule shared by the rewrite gates and deleteObjects:
// objectURI's leading-slash trim for relative paths plus the own-bucket
// prefix match for absolute URIs, an exact match so s3://bktX/ never passes
// as bkt.
func (c *Compactor) bucketRelativeKey(path string) (string, bool) {
	key := path
	if strings.HasPrefix(path, "s3://") {
		key = strings.TrimPrefix(path, "s3://"+c.Bucket+"/")
		if key == path {
			return "", false
		}
	}
	key = strings.TrimPrefix(key, "/")
	if key == "" {
		return "", false
	}
	return key, true
}
