package cdcv2

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
)

// rowCopy is what the test stagers write for one row: the row at the version
// its entity_main row carried, live or a tombstone. An object of the tests is
// a JSON array of them, standing in for a parquet file.
type rowCopy struct {
	Row     uuid.UUID `json:"row"`
	Version int64     `json:"version"`
	Deleted bool      `json:"deleted"`
}

// stager is a DeltaStager and a BaseExporter over the fixture: it reads the
// rows' current entity_main state, as an export does, and writes their
// copies once to the key. during runs after the read and before the write,
// to change rows under an export in flight; fail fails the n-th call.
type stager struct {
	f      *fixture
	during func(ctx context.Context, rows []uuid.UUID) error
	fail   func(call int) error
	calls  int
}

func (s *stager) StageDelta(ctx context.Context, req DeltaRequest) ([]manifestv2.File, error) {
	rows := make([]uuid.UUID, len(req.Entries))
	for i, e := range req.Entries {
		rows[i] = e.RowID
	}
	key, err := req.Layout.NewParquetKey(req.SchemaID, req.Epoch, manifestv2.TierDelta)
	if err != nil {
		return nil, err
	}
	file, err := s.write(ctx, s.f.db, key, manifestv2.TierDelta, rows, false)
	if err != nil {
		return nil, err
	}
	return []manifestv2.File{file}, nil
}

func (s *stager) ExportBase(ctx context.Context, snap *sql.Tx, req BaseRequest) (manifestv2.File, error) {
	return s.write(ctx, snap, req.Key, manifestv2.TierBase, req.RowIDs, true)
}

type querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func (s *stager) write(ctx context.Context, q querier, key string, tier manifestv2.Tier, rows []uuid.UUID, liveOnly bool) (manifestv2.File, error) {
	s.calls++
	if s.fail != nil {
		if err := s.fail(s.calls); err != nil {
			return manifestv2.File{}, err
		}
	}
	copies, err := s.f.readCopies(ctx, q, rows, liveOnly)
	if err != nil {
		return manifestv2.File{}, err
	}
	if s.during != nil {
		if err := s.during(ctx, rows); err != nil {
			return manifestv2.File{}, err
		}
	}
	raw, err := json.Marshal(copies)
	if err != nil {
		return manifestv2.File{}, fmt.Errorf("encode %d row copies: %w", len(copies), err)
	}
	if err := s.f.store.Put(ctx, key, bytes.NewReader(raw)); err != nil {
		return manifestv2.File{}, err
	}
	ids := make([]string, len(copies))
	for i, c := range copies {
		ids[i] = c.Row.String()
	}
	slices.Sort(ids)
	return manifestv2.File{
		Tier: tier, Key: key, SizeBytes: int64(len(raw)), SHA256: manifestv2.Digest(raw), RowCount: int64(len(copies)),
		RowIDMin: uuid.MustParse(ids[0]), RowIDMax: uuid.MustParse(ids[len(ids)-1]),
		Columns: map[string]string{"ltbase_row_id": "UUID", "ltbase_updated_at": "BIGINT", "ltbase_deleted_at": "BIGINT"},
	}, nil
}

// readCopies reads rows' entity_main state on q.
func (f *fixture) readCopies(ctx context.Context, q querier, rows []uuid.UUID, liveOnly bool) ([]rowCopy, error) {
	params := make([]string, len(rows))
	args := []any{schemaID}
	for i, id := range rows {
		params[i] = fmt.Sprintf("$%d", len(args)+1)
		args = append(args, id)
	}
	query := fmt.Sprintf(`SELECT ltbase_row_id, ltbase_updated_at, ltbase_deleted_at IS NOT NULL FROM %s
		WHERE ltbase_schema_id = $1 AND ltbase_row_id IN (%s)`, f.entityMain, strings.Join(params, ","))
	if liveOnly {
		query += " AND ltbase_deleted_at IS NULL"
	}
	res, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("read %d rows to export: %w", len(rows), err)
	}
	defer res.Close()
	var copies []rowCopy
	for res.Next() {
		var c rowCopy
		if err := res.Scan(&c.Row, &c.Version, &c.Deleted); err != nil {
			return nil, err
		}
		copies = append(copies, c)
	}
	if err := res.Err(); err != nil {
		return nil, err
	}
	if len(copies) == 0 {
		return nil, errors.New("no row to export")
	}
	return copies, nil
}

// held answers what the head generation holds: each row's newest copy
// across its files. An unpublished record holds nothing.
func (f *fixture) held(t *testing.T) map[uuid.UUID]rowCopy {
	t.Helper()
	out := map[uuid.UUID]rowCopy{}
	rec := f.record(t)
	if rec.State != publication.Published {
		return out
	}
	ref, err := manifestv2.RecordRef(rec)
	require.NoError(t, err)
	m, err := manifestv2.Fetch(context.Background(), f.store, ref)
	require.NoError(t, err)
	for _, file := range m.Files {
		raw, err := f.store.Get(context.Background(), file.Key)
		require.NoError(t, err, "every listed object is there")
		require.Equal(t, file.SHA256, manifestv2.Digest(raw))
		var copies []rowCopy
		require.NoError(t, json.Unmarshal(raw, &copies))
		for _, c := range copies {
			if old, ok := out[c.Row]; !ok || c.Version > old.Version {
				out[c.Row] = c
			}
		}
	}
	return out
}

// latest answers each row's newest change_log entry and whether it is
// flushed.
func (f *fixture) latest(t *testing.T) map[uuid.UUID]struct {
	version         int64
	flushed, delete bool
} {
	t.Helper()
	rows, err := f.db.Query(fmt.Sprintf(`SELECT row_id, changed_at, flushed_at <> 0, deleted_at IS NOT NULL FROM %s WHERE schema_id = $1`, f.changeLog), schemaID)
	require.NoError(t, err)
	defer rows.Close()
	out := map[uuid.UUID]struct {
		version         int64
		flushed, delete bool
	}{}
	for rows.Next() {
		var id uuid.UUID
		var e struct {
			version         int64
			flushed, delete bool
		}
		require.NoError(t, rows.Scan(&id, &e.version, &e.flushed, &e.delete))
		if old, ok := out[id]; !ok || e.version > old.version {
			out[id] = e
		}
	}
	require.NoError(t, rows.Err())
	return out
}

// checkInvariants checks what the protocol promises a reader of the
// committed state, and that no generation was overwritten:
//
//   - UNPUBLISHED: every entry is unflushed.
//   - Invariant B as a reader sees it: a row whose newest entry is flushed
//     at v has its newest parquet copy at v in the head, live as the entry
//     is, or, for a flushed deletion, a tombstone at v or no copy at all
//     (an init exports live rows only).
//   - The history: within an epoch, generations follow one another, each
//     the parent of the next, so exactly one compare-and-set won per
//     parent; every generation's manifest is still the one its row
//     committed; tokens never repeat.
func (f *fixture) checkInvariants(t *testing.T) {
	t.Helper()
	rec := f.record(t)
	held := f.held(t)
	for row, e := range f.latest(t) {
		if rec.State == publication.Unpublished {
			require.False(t, e.flushed, "an UNPUBLISHED schema has no flushed entry, row %s", row)
			continue
		}
		if !e.flushed {
			continue
		}
		c, ok := held[row]
		if e.delete && !ok {
			continue
		}
		require.True(t, ok, "row %s is flushed at %d, and the head holds no copy of it", row, e.version)
		require.Equal(t, rowCopy{Row: row, Version: e.version, Deleted: e.delete}, c, "row %s is flushed at %d", row, e.version)
	}
	f.checkHistory(t)
}

func (f *fixture) checkHistory(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	gens := f.history(t)
	tokens := map[uuid.UUID]bool{}
	for i, g := range gens {
		require.False(t, tokens[g.Token], "generation %d reuses a token", g.Generation)
		tokens[g.Token] = true
		if i > 0 && gens[i-1].Epoch == g.Epoch {
			require.Equal(t, gens[i-1].Generation+1, g.Generation, "generations of one epoch follow one another")
			require.Equal(t, gens[i-1].Generation, g.ParentGeneration, "generation %d replaced its predecessor", g.Generation)
		} else {
			require.Zero(t, g.ParentGeneration, "generation %d is its epoch's first", g.Generation)
		}
		m, err := manifestv2.Fetch(ctx, f.store, manifestv2.GenerationRef(schemaID, g))
		require.NoError(t, err, "generation %d's manifest is the one its row committed", g.Generation)
		require.Equal(t, g.Kind, m.Kind)
	}
}

// history answers the schema's generation rows, oldest first.
func (f *fixture) history(t *testing.T) []publication.Generation {
	t.Helper()
	rows, err := f.db.Query(fmt.Sprintf(`SELECT generation, epoch, kind, parent_generation, manifest_key, manifest_sha256, cas_token
		FROM %s WHERE schema_id = $1 ORDER BY generation`, f.tables.Generation), schemaID)
	require.NoError(t, err)
	defer rows.Close()
	var out []publication.Generation
	for rows.Next() {
		var g publication.Generation
		var kind string
		require.NoError(t, rows.Scan(&g.Generation, &g.Epoch, &kind, &g.ParentGeneration, &g.ManifestKey, &g.ManifestSHA256, &g.Token))
		g.Kind = publication.Kind(kind)
		out = append(out, g)
	}
	require.NoError(t, rows.Err())
	return out
}

// objects counts the objects the store holds for the schema, committed or
// not: its manifest objects and its parquet objects.
func (f *fixture) objects(t *testing.T) (manifests, parquet int) {
	t.Helper()
	err := filepath.WalkDir(f.store.Dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || strings.HasPrefix(d.Name(), ".") {
			return err
		}
		switch {
		case strings.Contains(p, "_manifests"):
			manifests++
		case strings.HasSuffix(p, ".parquet"):
			parquet++
		}
		return nil
	})
	require.NoError(t, err)
	return manifests, parquet
}
