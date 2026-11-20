package cdcv2

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/manifestv2"
)

// compactStager is a CompactStager over the fixture's row-copy objects. It
// merges the input files pick chooses (every file when pick is nil) into one
// base object, keeping each row's newest copy, tombstones included, as the
// protocol requires of a compaction. fail fails the n-th call.
type compactStager struct {
	f     *fixture
	pick  func(m *manifestv2.Manifest) []manifestv2.File
	fail  func(call int) error
	calls int
}

func (s *compactStager) StageCompaction(ctx context.Context, req CompactRequest) (CompactStaging, error) {
	s.calls++
	if s.fail != nil {
		if err := s.fail(s.calls); err != nil {
			return CompactStaging{}, err
		}
	}
	sources := req.Input.Files
	if s.pick != nil {
		sources = s.pick(req.Input)
	}
	if len(sources) == 0 {
		return CompactStaging{}, nil
	}
	newest := map[uuid.UUID]rowCopy{}
	merged := make([]string, 0, len(sources))
	for _, file := range sources {
		raw, err := s.f.store.Get(ctx, file.Key)
		if err != nil {
			return CompactStaging{}, fmt.Errorf("read merge source %s: %w", file.Key, err)
		}
		var copies []rowCopy
		if err := json.Unmarshal(raw, &copies); err != nil {
			return CompactStaging{}, fmt.Errorf("decode merge source %s: %w", file.Key, err)
		}
		for _, c := range copies {
			if old, ok := newest[c.Row]; !ok || c.Version > old.Version || (c.Version == old.Version && c.Deleted) {
				newest[c.Row] = c
			}
		}
		merged = append(merged, file.Key)
	}
	copies := make([]rowCopy, 0, len(newest))
	for _, c := range newest {
		copies = append(copies, c)
	}
	slices.SortFunc(copies, func(a, b rowCopy) int { return bytes.Compare(a.Row[:], b.Row[:]) })
	key, err := req.Layout.NewParquetKey(req.SchemaID, req.Epoch, manifestv2.TierBase)
	if err != nil {
		return CompactStaging{}, err
	}
	raw, err := json.Marshal(copies)
	if err != nil {
		return CompactStaging{}, err
	}
	if err := s.f.store.Put(ctx, key, bytes.NewReader(raw)); err != nil {
		return CompactStaging{}, err
	}
	ids := make([]string, len(copies))
	for i, c := range copies {
		ids[i] = c.Row.String()
	}
	slices.Sort(ids)
	out := manifestv2.File{
		Tier: manifestv2.TierBase, Key: key, SizeBytes: int64(len(raw)), SHA256: manifestv2.Digest(raw), RowCount: int64(len(copies)),
		RowIDMin: uuid.MustParse(ids[0]), RowIDMax: uuid.MustParse(ids[len(ids)-1]),
		Columns: map[string]string{"ltbase_row_id": "UUID", "ltbase_updated_at": "BIGINT", "ltbase_deleted_at": "BIGINT"},
	}
	return CompactStaging{Merged: merged, Outputs: []manifestv2.File{out}}, nil
}

// generationReadable checks that generation g of the history is still
// readable as a reader that chose it would read it: its manifest verifies,
// and every object it lists is there with the digest it lists.
func (f *fixture) generationReadable(t *testing.T, generation int64) {
	t.Helper()
	ctx := context.Background()
	for _, g := range f.history(t) {
		if g.Generation != generation {
			continue
		}
		m, err := manifestv2.Fetch(ctx, f.store, manifestv2.GenerationRef(schemaID, g))
		require.NoError(t, err, "generation %d's manifest", generation)
		for _, file := range m.Files {
			raw, err := f.store.Get(ctx, file.Key)
			require.NoError(t, err, "generation %d lists %s", generation, file.Key)
			require.Equal(t, file.SHA256, manifestv2.Digest(raw))
		}
		return
	}
	t.Fatalf("no history row for generation %d", generation)
}
