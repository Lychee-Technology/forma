package manifestv2

import (
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/publication"
	"github.com/stretchr/testify/require"
)

// TestNewCompactReplacesExactlyTheMergedSources pins the compact row of
// "What a generation replaces": the parent's files, less the merged
// sources, plus the outputs. A file the compaction did not merge stays,
// whatever its tier, which is what lets it rebase over a flush.
func TestNewCompactReplacesExactlyTheMergedSources(t *testing.T) {
	parent := testManifest(t)
	parent.Files = sortedFiles(parent.Files)
	base, delta2, delta3 := testFile(t, TierBase, 1), testFile(t, TierDelta, 2), testFile(t, TierDelta, 3)
	out := testFile(t, TierBase, 9)
	h := Header{SchemaID: 100, Epoch: 1, Generation: 8, ParentGeneration: 7, Lineage: testLineage,
		Token: uuid.New(), Kind: publication.KindCompact, InputGeneration: 7}

	m, err := NewCompact(&parent, h, []string{delta2.Key, delta3.Key}, []File{out})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{base.Key, out.Key}, m.Keys())
	require.Equal(t, int64(7), m.InputGeneration)
	_, _, err = m.Encode()
	require.NoError(t, err)

	// A rebase onto a flush: the head lists a delta the compaction never
	// read, and keeps it.
	head := parent
	head.Generation, head.ParentGeneration = 8, 7
	added := testFile(t, TierDelta, 4)
	head.Files = sortedFiles(append(append([]File{}, parent.Files...), added))
	rebased := h
	rebased.Generation, rebased.ParentGeneration = 9, 8
	m, err = NewCompact(&head, rebased, []string{base.Key, delta2.Key, delta3.Key}, []File{out})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{added.Key, out.Key}, m.Keys())
	require.Equal(t, int64(7), m.InputGeneration, "the input stays the generation the compaction merged")
	require.Equal(t, int64(8), m.ParentGeneration)

	m, err = NewCompact(&parent, h, []string{base.Key, delta2.Key, delta3.Key}, nil)
	require.NoError(t, err, "a merge with no output leaves an empty generation, which a compaction may publish")
	require.Empty(t, m.Files)

	refusals := map[string]func() (Manifest, error){
		"no merged source": func() (Manifest, error) { return NewCompact(&parent, h, nil, []File{out}) },
		"a source twice":   func() (Manifest, error) { return NewCompact(&parent, h, []string{base.Key, base.Key}, []File{out}) },
		"a delta output": func() (Manifest, error) {
			return NewCompact(&parent, h, []string{base.Key}, []File{testFile(t, TierDelta, 9)})
		},
		"an output listed":   func() (Manifest, error) { return NewCompact(&parent, h, []string{base.Key}, []File{delta2}) },
		"no parent manifest": func() (Manifest, error) { return NewCompact(nil, h, []string{base.Key}, []File{out}) },
		"another kind": func() (Manifest, error) {
			c := h
			c.Kind = publication.KindFlush
			return NewCompact(&parent, c, []string{base.Key}, []File{out})
		},
		"another lineage": func() (Manifest, error) {
			c := h
			c.Lineage = uuid.New()
			return NewCompact(&parent, c, []string{base.Key}, []File{out})
		},
		"another parent": func() (Manifest, error) {
			c := h
			c.ParentGeneration = 6
			return NewCompact(&parent, c, []string{base.Key}, []File{out})
		},
		"no input generation": func() (Manifest, error) {
			c := h
			c.InputGeneration = 0
			return NewCompact(&parent, c, []string{base.Key}, []File{out})
		},
		"an input after the parent": func() (Manifest, error) {
			c := h
			c.InputGeneration = 8
			return NewCompact(&parent, c, []string{base.Key}, []File{out})
		},
	}
	for name, build := range refusals {
		_, err := build()
		require.Error(t, err, name)
		require.NotErrorIs(t, err, ErrMergedSourceGone, name)
	}

	_, err = NewCompact(&parent, h, []string{testFile(t, TierDelta, 5).Key}, []File{out})
	require.ErrorIs(t, err, ErrMergedSourceGone, "a source the parent no longer lists")
}
