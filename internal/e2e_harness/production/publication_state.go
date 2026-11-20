package production

import (
	"context"
	"fmt"
	"testing"

	"github.com/lychee-technology/forma/internal/compaction"
	"github.com/lychee-technology/forma/internal/publication"
)

// The helpers below are how a test compares a schema's publication over time
// (#901): whether something published, how often, and whether a no-write path
// left the publication exactly as it was. Under Protocol V2
// (docs/publication-protocol-v2.md) a publish is one commit of the schema's
// publication record onto a new immutable generation, so they compare the
// record: its epoch, state, generation counter, token and manifest.

// publishesBetween answers how many generations the schema committed between
// the two observations of its inventory, before and after: 0 when nothing
// published, negative when after is the older one. Generations of one epoch
// are numbered consecutively, one per commit (manifestv2.Stage), so the
// difference counts commits; it fails when the observations are of different
// epochs, where the numbers do not compare.
func publishesBetween(before, after *Manifest) int64 {
	if before.Epoch != after.Epoch {
		panic(fmt.Sprintf("publishesBetween: schema %d observed at epochs %d and %d; generations of different epochs do not compare",
			before.SchemaID, before.Epoch, after.Epoch))
	}
	return after.Generation - before.Generation
}

// compactionPublishesSince answers how many times the schema published
// between the observation before and the publish the compaction pass that
// answered result committed. Until V2 compaction lands (#890) RunCompaction
// skips before answering a result, so nothing reaches this; the V1
// compactor's result names no V2 generation, so it refuses to count rather
// than compare unrelated numbers.
func compactionPublishesSince(before *Manifest, result compaction.CompactionResult) int64 {
	if v1CompactionStopped {
		panic(fmt.Sprintf("compactionPublishesSince: compaction of schema %d answered %s without a V2 generation; %s",
			before.SchemaID, result.Outcome, compactionSkipReason))
	}
	// #890 replaces this body with the count of generations between before
	// and the one its V2 compaction result names, and clears
	// v1CompactionStopped.
	return -1
}

// publicationSnapshot is one schema's publication exactly as a no-write check
// compares it: the record as one snapshot read it.
type publicationSnapshot struct {
	record publication.Record
}

// snapshotPublication captures the schema's publication record. Every
// registered fixture schema has one (provisioning writes it as init-db does),
// so a read failure here is a test bug, not a skip.
func snapshotPublication(t *testing.T, ctx context.Context, env *Env, schema SchemaRef) publicationSnapshot {
	t.Helper()
	rec, err := env.readRecord(ctx, schema.ID)
	if err != nil {
		t.Fatalf("read the publication record of schema %d: %v", schema.ID, err)
	}
	return publicationSnapshot{record: rec}
}

// publicationChanges answers, one line each, how after differs from before:
// none when the record is exactly as it was. Generations are immutable, so a
// record that names the same manifest key and digest names the same files.
func publicationChanges(before, after publicationSnapshot) ([]string, error) {
	b, a := before.record, after.record
	if b.SchemaID != a.SchemaID {
		return nil, fmt.Errorf("compare the publications of schemas %d and %d", b.SchemaID, a.SchemaID)
	}
	var changes []string
	diff := func(field string, x, y any) {
		if x != y {
			changes = append(changes, fmt.Sprintf("record %s changed: %v -> %v", field, x, y))
		}
	}
	diff("epoch", b.Epoch, a.Epoch)
	diff("state", b.State, a.State)
	diff("generation", b.Generation, a.Generation)
	diff("parent generation", b.ParentGeneration, a.ParentGeneration)
	diff("manifest key", b.ManifestKey, a.ManifestKey)
	diff("manifest sha256", b.ManifestSHA256, a.ManifestSHA256)
	diff("lineage", b.Lineage, a.Lineage)
	diff("token", b.Token, a.Token)
	diff("updated_at", b.UpdatedAt, a.UpdatedAt)
	diff("horizon", b.Horizon, a.Horizon)
	return changes, nil
}
