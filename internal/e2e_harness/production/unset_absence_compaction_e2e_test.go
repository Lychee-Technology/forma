//go:build e2e

package production

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"testing"

	"github.com/lychee-technology/forma/internal/compaction"
)

// compact runs one compaction pass, requires the outcome named, so no stage
// that claims a merge passes over a pass that merged nothing, and checks the
// committed generation and the object the pass wrote (checkCompaction).
func (lc *absenceLifecycle) compact(ctx context.Context, t *testing.T, ov CompactionOverrides, want compaction.CompactionOutcome) {
	t.Helper()
	before := loadSchemaManifest(ctx, t, lc.env, lc.schema)
	res, err := lc.env.RunCompactionWith(ctx, lc.schema, ov)
	if err != nil {
		t.Fatalf("compaction: %v", err)
	}
	if res.Outcome != want || res.NewBaseKey == "" {
		t.Fatalf("compaction outcome = %s (dirty ratio %.2f, new base %q), want %s with a new base",
			res.Outcome, res.DirtyRatio, res.NewBaseKey, want)
	}
	lc.checkCompaction(ctx, t, before, res)
}

// checkCompaction checks the generation a compaction pass committed against
// the one it read, before, and reads the object it wrote directly, so a
// merge that drops an input, or keeps a stale copy the read-side fold would
// hide, cannot pass.
//
// A promotion folds the delta tier alone: the generation it commits lists
// every base before lists, byte for byte, beside the new base, and no delta.
// The first promotion's output holds the old value and is recorded as
// lc.valueBase; a later promotion must leave it listed, so the overlap of the
// cleared image with the old one is real. A rewrite folds every object
// listed: valueBase, while recorded, must be one of its inputs, and the
// generation it commits lists the new base alone.
func (lc *absenceLifecycle) checkCompaction(ctx context.Context, t *testing.T, before *Manifest, res compaction.CompactionResult) {
	t.Helper()
	after := loadSchemaManifest(ctx, t, lc.env, lc.schema)
	stage := fmt.Sprintf("%s generation %d -> %d", res.Outcome, before.Generation, after.Generation)
	if res.NewBaseKey == "" || manifestLists(before, res.NewBaseKey) {
		t.Fatalf("%s: new base %q is not a new object", stage, res.NewBaseKey)
	}
	if len(TierEntries(before, "delta")) == 0 {
		t.Fatalf("%s: the input generation lists no delta object to merge", stage)
	}
	if lc.valueBase != "" && !manifestLists(before, lc.valueBase) {
		t.Fatalf("%s: the input generation no longer lists the base %s that holds the old value", stage, lc.valueBase)
	}
	switch res.Outcome {
	case compaction.PromotionApplied:
		want := append(entryPaths(TierEntries(before, "base")), res.NewBaseKey)
		requireTierObjects(t, stage, after, "base", want)
		for _, f := range TierEntries(before, "base") {
			if got, ok := manifestEntry(after, f.Path); !ok || got.Checksum != f.Checksum {
				t.Fatalf("%s: base %s is not listed unchanged beside the new base", stage, f.Path)
			}
		}
		if lc.valueBase == "" {
			lc.valueBase = res.NewBaseKey
		}
	case compaction.RewriteApplied:
		requireTierObjects(t, stage, after, "base", []string{res.NewBaseKey})
		lc.valueBase = ""
	default:
		t.Fatalf("%s: unexpected compaction outcome", stage)
	}
	requireTierObjects(t, stage, after, "delta", nil)
	lc.checkOutputObject(ctx, t, stage, res.NewBaseKey)
}

// checkOutputObject reads the object a merge wrote and requires it to hold
// exactly one image of every row, the rows' published image: NULL for an
// absent attribute, an empty LIST for [], the value otherwise. Every merge
// of a lifecycle reads a delta tier that holds a newer image of every row,
// so the output, promotion's included, holds them all and no other.
func (lc *absenceLifecycle) checkOutputObject(ctx context.Context, t *testing.T, stage, key string) {
	t.Helper()
	uri := lc.env.ParquetURI(key)
	for _, a := range lc.subject.attrs {
		query := fmt.Sprintf(`SELECT CAST(row_id AS VARCHAR), CAST(%q AS VARCHAR) FROM read_parquet('%s')`, a.name, uri)
		got := map[string][]sql.NullString{}
		rows, err := lc.env.Duck.DB.QueryContext(ctx, query)
		if err != nil {
			t.Fatalf("%s: read %s of the merged object %s: %v", stage, a.name, key, err)
		}
		for rows.Next() {
			var id string
			var v sql.NullString
			if err := rows.Scan(&id, &v); err != nil {
				_ = rows.Close()
				t.Fatalf("%s: scan %s of the merged object %s: %v", stage, a.name, key, err)
			}
			got[id] = append(got[id], v)
		}
		err = rows.Err()
		_ = rows.Close()
		if err != nil {
			t.Fatalf("%s: read %s of the merged object %s: %v", stage, a.name, key, err)
		}
		if len(got) != len(lc.rows) {
			t.Errorf("%s: the merged object holds %d row ids, want the %d rows written", stage, len(got), len(lc.rows))
		}
		for _, r := range lc.rows {
			images := got[r.id.String()]
			if len(images) != 1 {
				t.Errorf("%s: the merged object holds %d images of the %s row, want exactly one", stage, len(images), r.label)
				continue
			}
			lc.compareImage(t, stage+", merged object", r, a.name, images[0])
		}
	}
}

// compareImage requires got, an attribute of a parquet image cast to
// VARCHAR, to be what the row held at the last flush.
func (lc *absenceLifecycle) compareImage(t *testing.T, label string, r *absenceRow, attr string, got sql.NullString) {
	t.Helper()
	want, present := r.image[attr]
	switch {
	case !present && got.Valid:
		t.Errorf("%s: the image of the %s row holds %s = %s, want NULL", label, r.label, attr, got.String)
	case present && (!got.Valid || got.String != parquetImage(want)):
		t.Errorf("%s: the image of the %s row holds %s = %v, want %s", label, r.label, attr, got, parquetImage(want))
	}
}

func manifestLists(m *Manifest, key string) bool {
	_, ok := manifestEntry(m, key)
	return ok
}

func manifestEntry(m *Manifest, key string) (ManifestEntry, bool) {
	for _, f := range m.Files {
		if f.Path == key {
			return f, true
		}
	}
	return ManifestEntry{}, false
}

func entryPaths(entries []ManifestEntry) []string {
	out := make([]string, len(entries))
	for i, f := range entries {
		out[i] = f.Path
	}
	return out
}

// requireTier requires m to list exactly the objects want on tier.
func requireTierObjects(t *testing.T, stage string, m *Manifest, tier string, want []string) {
	t.Helper()
	got := entryPaths(TierEntries(m, tier))
	slices.Sort(got)
	want = slices.Clone(want)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("%s: generation %d lists %s objects %v, want %v", stage, m.Generation, tier, got, want)
	}
}
