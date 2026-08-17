//go:build e2e

package production

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/lychee-technology/forma/internal/cdc"
	"github.com/lychee-technology/forma/internal/compaction"
	"github.com/lychee-technology/forma/internal/manifest"
	"github.com/lychee-technology/forma/internal/reconcile"
)

// TestChecksumChainDetectsSilentDeltaCorruption drives the whole #347 chain
// against real RustFS object storage: writers stamp a content hash, the
// scrub re-hashes at rest and names what diverged, and the compaction gate
// refuses to launder a diverged source into a new base.
//
// The corruption model is #251's, applied at rest: a 64-byte XOR span in the
// middle of a published delta. Nothing about the manifest, the listing, or
// the parquet footer changes — which is precisely why every shape-level
// check (the #189 system-column invariant, the #256 column stamp) still
// passes and only a byte hash can tell.
//
// The stamp assertions here are wiring pins, not decoration: the delta stamp
// proves the cdc flush's checksum seam is really wired in RunOnce, the base
// stamp does the same for cdc-init, and the post-rewrite base stamp does it
// for the Compactor's ObjectReader. Each compares against a SHA-256 the test
// computes itself over bytes it fetched itself, so a seam that stamped a
// constant, or hashed the staging copy instead of the published object, would
// fail.
func TestChecksumChainDetectsSilentDeltaCorruption(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1] // e2e_wide

	// compaction_atomicity's rewrite recipe: five rows exported as base by a
	// real cdc-init, then one update flushed as a delta. Dirty ratio 1/5 =
	// 20% > 5% makes the pass rewrite-eligible, and the delta tier stays far
	// under the 256 MB promotion threshold, so the rewrite (not a promotion)
	// is what runs — and its sources are the base plus the delta.
	creates := seedCompactionBase(ctx, t, env, wide, 5)
	update := UpdateEvent(wide, creates[0].RowID, map[string]any{"title": "checksum-chain-v2"})
	if err := env.ApplyEvents(ctx, update); err != nil {
		t.Fatalf("apply update: %v", err)
	}
	mustFlush(ctx, t, env)

	m := loadSchemaManifest(ctx, t, env, wide)
	deltaEntry := soleTierEntry(t, m, "delta")
	baseEntry := soleTierEntry(t, m, "base")
	deltaKey := mustManifestKey(t, env, deltaEntry.Path)

	var pristine []byte

	t.Run("writers_stamp_published_objects_to_byte_truth", func(t *testing.T) {
		assertEntryChecksumMatchesBytes(ctx, t, env, "flushed delta", deltaEntry)
		assertEntryChecksumMatchesBytes(ctx, t, env, "init-exported base", baseEntry)
		pristine = fetchObjectBytes(ctx, t, env, deltaKey)
	})

	t.Run("scrub_names_the_corrupted_object", func(t *testing.T) {
		overwriteObjectBytes(ctx, t, env, deltaKey, corruptMidFile)
		assertScrubReportsOnlyDivergence(ctx, t, env, wide, deltaKey, deltaEntry.Checksum)
	})

	t.Run("compaction_refuses_the_corrupted_source", func(t *testing.T) {
		assertCompactionRefusesCorruptSource(ctx, t, env, wide, deltaKey)
	})

	t.Run("compaction_opt_out_proceeds_past_the_gate", func(t *testing.T) {
		assertChecksumOptOutRewrites(ctx, t, env, wide, deltaKey, deltaEntry.Path, pristine)
	})
}

// assertScrubReportsOnlyDivergence runs `manifest-reconcile --verify-checksums`
// over the schema and requires exactly one finding — naming the corrupted key
// and quoting the stamp its bytes no longer match — with a discrepant
// (non-zero-exit) verdict. Zero unstamped skips is the coverage control: it
// proves the clean entries were actually hashed rather than passed over, so
// "one divergence" is a complete answer and not a partial one.
func assertScrubReportsOnlyDivergence(ctx context.Context, t *testing.T, env *Env,
	schema SchemaRef, corruptKey, stamped string) {
	t.Helper()
	r, cleanup := newReconcileHarness(t, ctx, env, schema,
		reconcile.Options{VerifyChecksums: true, MaxETagRetries: 3}, false)
	defer cleanup()

	report, err := r.Run(ctx)
	if err != nil {
		t.Fatalf("reconcile --verify-checksums run: %v", err)
	}
	if len(report.Schemas) != 1 {
		t.Fatalf("expected 1 schema report, got %d", len(report.Schemas))
	}
	s := report.Schemas[0]
	if s.Err != nil {
		t.Fatalf("scrub reported a schema failure instead of a corruption verdict: %v", s.Err)
	}
	if len(s.ChecksumDivergences) != 1 {
		t.Fatalf("checksum divergences = %v, want exactly one naming %s", s.ChecksumDivergences, corruptKey)
	}
	divergence := s.ChecksumDivergences[0]
	if !strings.Contains(divergence, corruptKey) {
		t.Errorf("divergence %q does not name the corrupted key %s", divergence, corruptKey)
	}
	if !strings.Contains(divergence, stamped) {
		t.Errorf("divergence %q does not quote the manifest stamp %s", divergence, stamped)
	}
	if s.SkippedUnstamped != 0 {
		t.Errorf("scrub skipped %d unstamped entries; the clean verdict on the rest is not full coverage",
			s.SkippedUnstamped)
	}
	if !report.HasResidualDiscrepancies() {
		t.Error("a checksum divergence must make the run discrepant (non-zero exit)")
	}
}

// assertCompactionRefusesCorruptSource runs the default (verifying) compaction
// pass over the corrupted delta and requires the fail-closed outcome: an
// ErrSourceChecksumMismatch, and a store left exactly as it was. The
// post-conditions are the point — the rewrite merges its sources and then
// deletes them, so a gate that fired but let the pass continue would still
// have destroyed the only named copy of the corrupt bytes.
func assertCompactionRefusesCorruptSource(ctx context.Context, t *testing.T, env *Env,
	schema SchemaRef, corruptKey string) {
	t.Helper()
	before := loadSchemaManifest(ctx, t, env, schema)
	beforeKeys := schemaParquetKeys(ctx, t, env, schema)

	result, err := env.RunCompaction(ctx, schema)
	if err == nil {
		t.Fatalf("compaction over a corrupted source succeeded (outcome %s)", result.Outcome)
	}
	if !errors.Is(err, compaction.ErrSourceChecksumMismatch) {
		t.Fatalf("refusal must classify as ErrSourceChecksumMismatch, got: %v", err)
	}
	if !strings.Contains(err.Error(), corruptKey) {
		t.Errorf("refusal must name the offending object %s, got: %v", corruptKey, err)
	}

	after := loadSchemaManifest(ctx, t, env, schema)
	if after.Version != before.Version {
		t.Errorf("refused pass moved the manifest version %d -> %d", before.Version, after.Version)
	}
	if countTier(after, "delta") != countTier(before, "delta") || countTier(after, "base") != countTier(before, "base") {
		t.Errorf("refused pass changed the manifest tiers:\n before: %+v\n after:  %+v", before.Files, after.Files)
	}
	afterKeys := schemaParquetKeys(ctx, t, env, schema)
	if !slices.Contains(afterKeys, corruptKey) {
		t.Fatalf("refused pass deleted the corrupt object %s; the evidence must survive: %v", corruptKey, afterKeys)
	}
	if len(afterKeys) != len(beforeKeys) {
		t.Errorf("refused pass changed the parquet inventory:\n before: %v\n after:  %v", beforeKeys, afterKeys)
	}
}

// assertChecksumOptOutRewrites documents the SkipInputChecksumVerify escape
// hatch: with the same divergent input, the default pass refuses and the
// opted-out pass rewrites.
//
// The divergence is re-staged rather than reused. #251's byte flip lands
// inside a compressed column chunk, so DuckDB cannot decode the file at all
// (TestParquetCorruption_CorruptBytes pins that) and an opted-out pass would
// die in the merge for a reason that has nothing to do with the gate. So the
// object is restored to its published bytes and the manifest stamp is
// poisoned instead: the gate sees the same input either way — a stamp that
// disagrees with the object's bytes — while the merge can still read it.
// That also makes this the harmful case the gate exists to prevent, run on
// purpose: the opted-out rewrite folds an unverified source into the new base
// and deletes it.
func assertChecksumOptOutRewrites(ctx context.Context, t *testing.T, env *Env,
	schema SchemaRef, deltaKey, deltaPath string, pristine []byte) {
	t.Helper()
	putObjectBytes(ctx, t, env, deltaKey, pristine)
	poisonManifestChecksum(ctx, t, env, schema, deltaPath)

	// Control: the poisoned stamp is a genuine gate trigger, so the opt-out
	// below is the only thing that changes the outcome.
	if _, err := env.RunCompaction(ctx, schema); !errors.Is(err, compaction.ErrSourceChecksumMismatch) {
		t.Fatalf("verifying pass over a poisoned stamp must refuse with ErrSourceChecksumMismatch, got: %v", err)
	}

	result, err := env.RunCompactionWith(ctx, schema, CompactionOverrides{SkipInputChecksumVerify: true})
	if err != nil {
		t.Fatalf("compaction with the checksum gate opted out: %v", err)
	}
	if result.Outcome != compaction.RewriteApplied {
		t.Fatalf("outcome = %s (dirty ratio %.2f), want %s", result.Outcome, result.DirtyRatio, compaction.RewriteApplied)
	}

	m := loadSchemaManifest(ctx, t, env, schema)
	if got := countTier(m, "delta"); got != 0 {
		t.Errorf("delta entries after the rewrite = %d, want 0 (sources folded into the new base)", got)
	}
	// The merged base carries a stamp over its own bytes: the pin for the
	// Compactor's ObjectReader wiring on the production harness path.
	assertEntryChecksumMatchesBytes(ctx, t, env, "compaction-merged base", soleTierEntry(t, m, "base"))
	assertNoDuplicateManifestEntries(t, m)
	assertManifestMatchesInventory(ctx, t, env, schema)
	env.AssertQueryMatches(ctx, Query{Schema: schema, Limit: 100})
}

// assertEntryChecksumMatchesBytes is the byte-truth bar for a #347 stamp: the
// prefixed format, and equality with a SHA-256 the test computes over the
// object's bytes as the store returns them today.
func assertEntryChecksumMatchesBytes(ctx context.Context, t *testing.T, env *Env,
	label string, entry manifest.FileEntry) {
	t.Helper()
	if !strings.HasPrefix(entry.Checksum, cdc.ChecksumSHA256Prefix) {
		t.Fatalf("%s: manifest entry %s has checksum %q, want a %q-prefixed stamp",
			label, entry.Path, entry.Checksum, cdc.ChecksumSHA256Prefix)
	}
	key := mustManifestKey(t, env, entry.Path)
	want := localObjectSHA256(fetchObjectBytes(ctx, t, env, key))
	if entry.Checksum != want {
		t.Fatalf("%s: manifest entry %s stamp %s does not match its bytes (%s)",
			label, entry.Path, entry.Checksum, want)
	}
	t.Logf("stamp==bytes: %s — %s (%d bytes)", label, entry.Checksum, entry.SizeBytes)
}

// localObjectSHA256 recomputes the manifest stamp format independently of
// cdc.ObjectSHA256, so the assertions above are an oracle rather than a
// restatement of the production helper.
func localObjectSHA256(data []byte) string {
	sum := sha256.Sum256(data)
	return cdc.ChecksumSHA256Prefix + hex.EncodeToString(sum[:])
}

// fetchObjectBytes reads one object in full through the harness S3 client.
func fetchObjectBytes(ctx context.Context, t *testing.T, env *Env, key string) []byte {
	t.Helper()
	obj, err := env.Cluster.S3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(env.Cluster.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("get object %s: %v", key, err)
	}
	defer func() { _ = obj.Body.Close() }()
	data, err := io.ReadAll(obj.Body)
	if err != nil {
		t.Fatalf("read object %s: %v", key, err)
	}
	return data
}

// putObjectBytes writes raw bytes back under an existing key, the same way
// overwriteObjectBytes does: a plain body with no content encoding of its
// own, so what the store returns later is byte-identical to what was sent.
func putObjectBytes(ctx context.Context, t *testing.T, env *Env, key string, data []byte) {
	t.Helper()
	if _, err := env.Cluster.S3.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(env.Cluster.Bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	}); err != nil {
		t.Fatalf("put object %s: %v", key, err)
	}
	if restored := fetchObjectBytes(ctx, t, env, key); !bytes.Equal(restored, data) {
		t.Fatalf("object %s came back %d bytes, wrote %d: the store altered the body",
			key, len(restored), len(data))
	}
}

// poisonManifestChecksum replaces one entry's stamp with a well-formed but
// wrong digest — a stamp/bytes divergence whose object is still decodable.
func poisonManifestChecksum(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, path string) {
	t.Helper()
	m, etag := loadManifestWithETag(t, ctx, env, schema)
	poisoned := false
	for i := range m.Files {
		if m.Files[i].Path != path {
			continue
		}
		m.Files[i].Checksum = cdc.ChecksumSHA256Prefix + strings.Repeat("0", 64)
		poisoned = true
	}
	if !poisoned {
		t.Fatalf("manifest has no entry at %s to poison: %+v", path, m.Files)
	}
	saveManifestWithETag(t, ctx, env, schema, m, etag)
}

// soleTierEntry returns the schema's single manifest entry in tier, failing
// when the seed produced any other count (an ambiguous fixture would make the
// assertions above address the wrong object).
func soleTierEntry(t *testing.T, m *manifest.Manifest, tier string) manifest.FileEntry {
	t.Helper()
	var found []manifest.FileEntry
	for _, f := range m.Files {
		if strings.EqualFold(f.Tier, tier) {
			found = append(found, f)
		}
	}
	if len(found) != 1 {
		t.Fatalf("manifest holds %d %s entries, want exactly 1: %+v", len(found), tier, m.Files)
	}
	return found[0]
}

// mustManifestKey resolves a manifest path to a bucket-relative key, failing
// on a path this bucket's client cannot address.
func mustManifestKey(t *testing.T, env *Env, path string) string {
	t.Helper()
	key, ok := normalizedManifestKey(env, path)
	if !ok {
		t.Fatalf("manifest path %s does not resolve to a key in bucket %s", path, env.Cluster.Bucket)
	}
	return key
}
