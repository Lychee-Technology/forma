package manifestv2

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/publication"
	"github.com/stretchr/testify/require"
)

// TestStageNumbersTheGenerationAsTheCompareAndSetCommitsIt pins Stage to the
// parent rule of Publish's compare-and-set: the record's current generation
// when it is published, 0 when the record has none in this epoch, even
// though the counter is not 0 after a drop and re-activation.
func TestStageNumbersTheGenerationAsTheCompareAndSetCommitsIt(t *testing.T) {
	lineage, token := uuid.New(), uuid.New()
	published := publication.Record{SchemaID: 100, Epoch: 2, State: publication.Published, Generation: 6, Lineage: lineage}
	h, err := Stage(published, publication.KindFlush, lineage, token)
	require.NoError(t, err)
	require.Equal(t, Header{SchemaID: 100, Epoch: 2, Generation: 7, ParentGeneration: 6, Lineage: lineage, Token: token, Kind: publication.KindFlush}, h)
	require.Equal(t, testLayout.ManifestKey(100, 2, 7, token), h.Key(testLayout))

	reactivated := publication.Record{SchemaID: 100, Epoch: 3, State: publication.Unpublished, Generation: 6}
	h, err = Stage(reactivated, publication.KindFlush, lineage, token)
	require.NoError(t, err)
	require.Equal(t, int64(7), h.Generation)
	require.Zero(t, h.ParentGeneration, "the counter alone is no parent")

	_, err = Stage(publication.Record{SchemaID: 100, Epoch: 3, State: publication.Dropped}, publication.KindInit, lineage, token)
	require.ErrorIs(t, err, publication.ErrDropped)
}

func TestNewFlushAppendsDeltasToItsParent(t *testing.T) {
	parent := testManifest(t)
	parent.Files = sortedFiles(parent.Files)
	h := Header{SchemaID: 100, Epoch: 1, Generation: 8, ParentGeneration: 7, Lineage: testLineage, Token: uuid.New(), Kind: publication.KindFlush}
	parent.Generation = 7
	added := testFile(t, TierDelta, 4)
	m, err := NewFlush(&parent, h, []File{added})
	require.NoError(t, err)
	require.Equal(t, append(parent.Keys(), added.Key), m.Keys())
	require.Equal(t, int64(8), m.Generation)
	require.Equal(t, int64(7), m.ParentGeneration)
	require.Equal(t, publication.ManifestFormat, m.Format)
	_, _, err = m.Encode()
	require.NoError(t, err)

	first := Header{SchemaID: 100, Epoch: 1, Generation: 1, Lineage: testLineage, Token: uuid.New(), Kind: publication.KindFlush}
	m, err = NewFlush(nil, first, []File{added})
	require.NoError(t, err, "an epoch's first flush has no parent")
	require.Equal(t, []string{added.Key}, m.Keys())

	refusals := map[string]func() (Manifest, error){
		"no new file":           func() (Manifest, error) { return NewFlush(&parent, h, nil) },
		"a base file":           func() (Manifest, error) { return NewFlush(&parent, h, []File{testFile(t, TierBase, 4)}) },
		"a file already listed": func() (Manifest, error) { return NewFlush(&parent, h, []File{parent.Files[1]}) },
		"a parent not given":    func() (Manifest, error) { return NewFlush(nil, h, []File{added}) },
		"another kind of header": func() (Manifest, error) {
			c := h
			c.Kind = publication.KindInit
			return NewFlush(&parent, c, []File{added})
		},
		"another parent number": func() (Manifest, error) {
			c := h
			c.ParentGeneration = 6
			return NewFlush(&parent, c, []File{added})
		},
		"another lineage": func() (Manifest, error) { c := h; c.Lineage = uuid.New(); return NewFlush(&parent, c, []File{added}) },
		"another epoch's parent": func() (Manifest, error) {
			c := h
			c.Epoch = 2
			return NewFlush(&parent, c, []File{added})
		},
	}
	for name, build := range refusals {
		_, err := build()
		require.Error(t, err, name)
	}
}

func TestNewInitListsTheExportsBaseFilesOnly(t *testing.T) {
	h := Header{SchemaID: 100, Epoch: 1, Generation: 9, ParentGeneration: 8, Lineage: uuid.New(), Token: uuid.New(), Kind: publication.KindInit}
	b2, b1 := testFile(t, TierBase, 2), testFile(t, TierBase, 1)
	m, err := NewInit(h, []File{b2, b1})
	require.NoError(t, err)
	require.Equal(t, []string{b1.Key, b2.Key}, m.Keys())

	empty, err := NewInit(h, nil)
	require.NoError(t, err, "an export of no rows publishes an empty generation, not a placeholder object")
	require.Equal(t, []File{}, empty.Files)

	_, err = NewInit(h, []File{b1, testFile(t, TierDelta, 3)})
	require.ErrorContains(t, err, "an init generation lists the export's base files only")
	h.Kind = publication.KindFlush
	_, err = NewInit(h, []File{b1})
	require.Error(t, err)
}

// TestABuiltManifestSharesNothingWithItsParentOrItsInputs pins that a flush
// built from a Cache's verified parent, and an init built from the export's
// files, own their files: editing the built manifest's columns leaves the
// cached parent as verified, and editing an input after the build leaves the
// built manifest as built.
func TestABuiltManifestSharesNothingWithItsParentOrItsInputs(t *testing.T) {
	ctx := context.Background()
	raw, ref := encoded(t, testManifest(t))
	cache, _ := stored(t, raw, ref, 4)
	parent, err := cache.Get(ctx, ref)
	require.NoError(t, err)

	added := testFile(t, TierDelta, 4)
	h := Header{SchemaID: 100, Epoch: 1, Generation: 8, ParentGeneration: 7, Lineage: testLineage, Token: uuid.New(), Kind: publication.KindFlush}
	m, err := NewFlush(parent, h, []File{added})
	require.NoError(t, err)
	for _, f := range m.Files {
		f.Columns["name"] = "BLOB"
		f.Columns["added"] = "INTEGER"
	}
	again, err := cache.Get(ctx, ref)
	require.NoError(t, err)
	want, err := Verify(raw, ref)
	require.NoError(t, err)
	require.Equal(t, want, again, "the cached parent is the manifest its bytes verify to")
	require.Equal(t, "VARCHAR", added.Columns["name"], "nor is the new delta file's input edited")

	m, err = NewFlush(parent, h, []File{added})
	require.NoError(t, err)
	built := m.Files[len(m.Files)-1]
	require.Equal(t, added.Key, built.Key)
	added.Columns["name"] = "BLOB"
	require.Equal(t, "VARCHAR", built.Columns["name"], "editing an input after the build leaves the flush as built")

	base := testFile(t, TierBase, 2)
	init, err := NewInit(Header{SchemaID: 100, Epoch: 1, Generation: 9, ParentGeneration: 8, Lineage: uuid.New(), Token: uuid.New(), Kind: publication.KindInit}, []File{base})
	require.NoError(t, err)
	base.Columns["name"] = "BLOB"
	require.Equal(t, "VARCHAR", init.Files[0].Columns["name"], "editing an input after the build leaves the init as built")
}
