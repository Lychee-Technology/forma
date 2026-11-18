package production

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/lychee-technology/forma/internal/compaction"
)

// The helpers below are how a test compares a schema's publication over time
// (#901): whether something published, how often, and whether a no-write path
// left the publication exactly as it was. Tests never read the V1 manifest's
// version, ETag or bytes themselves. Under Protocol V1 a publish is one save
// of the schema's mutable manifest, which bumps its version; under V2
// (docs/publication-protocol-v2.md) it is one commit of the publication
// record onto a new immutable generation, and #896 rewrites these bodies.

// publishesBetween answers how many times the schema published between the
// two observations of its inventory, before and after: 0 when nothing
// published, negative when after is the older one.
func publishesBetween(before, after *Manifest) int64 {
	return after.Version - before.Version
}

// compactionPublishesSince answers how many times the schema published
// between the observation before and the publish the compaction pass that
// answered result committed, that one included.
func compactionPublishesSince(before *Manifest, result compaction.CompactionResult) int64 {
	return result.Version - before.Version
}

// publicationSnapshot is one schema's publication exactly as a no-write check
// compares it: under V1 the stored manifest document and its ETag.
type publicationSnapshot struct {
	raw  []byte
	etag string
}

// snapshotPublication captures the schema's publication. Callers snapshot
// only after the schema has published: a load failure here is a test bug,
// not a skip.
func snapshotPublication(t *testing.T, ctx context.Context, env *Env, schema SchemaRef) publicationSnapshot {
	t.Helper()
	store, _ := env.manifestAccess()
	path, err := env.manifestKey(schema.ID)
	if err != nil {
		t.Fatalf("manifest key: %v", err)
	}
	raw, etag, err := store.Load(ctx, path)
	if err != nil {
		t.Fatalf("load manifest %s (caller guarantees it exists by now; a missing one "+
			"was never created or was deleted since): %v", path, err)
	}
	return publicationSnapshot{raw: raw, etag: etag}
}

// publicationChanges answers, one line each, how after differs from before:
// none when the publication is exactly as it was. It fails when either
// snapshot does not decode as a publication.
func publicationChanges(before, after publicationSnapshot) ([]string, error) {
	var changes []string
	if !bytes.Equal(before.raw, after.raw) {
		changes = append(changes, fmt.Sprintf("manifest bytes changed:\n before: %s\n after:  %s", before.raw, after.raw))
	}
	if before.etag != after.etag {
		changes = append(changes, fmt.Sprintf("manifest etag changed: %s -> %s", before.etag, after.etag))
	}
	var mBefore, mAfter Manifest
	if err := json.Unmarshal(before.raw, &mBefore); err != nil {
		return nil, fmt.Errorf("unmarshal before-manifest: %w", err)
	}
	if err := json.Unmarshal(after.raw, &mAfter); err != nil {
		return nil, fmt.Errorf("unmarshal after-manifest: %w", err)
	}
	if n := publishesBetween(&mBefore, &mAfter); n != 0 {
		changes = append(changes, fmt.Sprintf("manifest version changed: %d -> %d", mBefore.Version, mAfter.Version))
	}
	return changes, nil
}
