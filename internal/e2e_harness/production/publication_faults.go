package production

import (
	"context"
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/cdc"
)

// The helpers below stage a fault in a schema's published inventory, each
// named for the state it leaves (#901): an entry that no longer lists its
// object, a stamp that no longer matches its bytes. They are the only place
// a test rewrites the published inventory. Under Protocol V1 each one edits
// the schema's mutable manifest under its ETag; V2 publishes immutable
// generations behind a publication record (docs/publication-protocol-v2.md),
// so #896 rewrites these bodies, and the tests keep the fault they stage.

// unpublishFiles takes every entry drop selects out of the schema's published
// inventory, leaving the objects they name in S3: the post-state of a lost
// publish. It answers the entries it took, in inventory order, and publishes
// nothing when drop selects none. drop sees the entries in inventory order.
func unpublishFiles(ctx context.Context, t *testing.T, env *Env, schema SchemaRef,
	drop func(ManifestEntry) bool) []ManifestEntry {
	t.Helper()
	m, etag := loadManifestWithETag(t, ctx, env, schema)
	var kept, dropped []ManifestEntry
	for _, f := range m.Files {
		if drop(f) {
			dropped = append(dropped, f)
			continue
		}
		kept = append(kept, f)
	}
	if len(dropped) == 0 {
		return nil
	}
	m.Files = kept
	saveManifestWithETag(t, ctx, env, schema, m, etag)
	return dropped
}

// poisonManifestChecksum replaces the stamp of the published entry at path
// with a well-formed but wrong digest: a stamp/bytes divergence whose object
// is still decodable.
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

// restampBaseColumns re-stamps the column set of the published base entry at
// path to columns, after a test rewrote the object in place, leaving its key
// and every other entry alone.
func restampBaseColumns(ctx context.Context, t *testing.T, env *Env, schema SchemaRef,
	path string, columns map[string]string) {
	t.Helper()
	entries := TierEntries(loadSchemaManifest(ctx, t, env, schema), "base")
	for i := range entries {
		if entries[i].Path == path {
			entries[i].Columns = columns
		}
	}
	replaceManifestTier(ctx, t, env, schema, "base", entries)
}
