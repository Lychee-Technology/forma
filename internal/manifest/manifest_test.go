package manifest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"testing/fstest"
	"time"

	"github.com/stretchr/testify/require"
)

func TestManifest_Parse(t *testing.T) {
	data := `{
		"schema_id": 42,
		"version": 5,
		"updated_at_ms": 1700000000000,
		"files": [
			{
				"tier": "base",
				"path": "s3://bucket/base.parquet",
				"row_id_min": "00000000-0000-0000-0000-000000000001",
				"row_id_max": "00000000-0000-0000-0000-000000001000",
				"created_min": 1699900000000,
				"created_max": 1699999000000,
				"size_bytes": 1048576,
				"row_count": 1000
			}
		]
	}`

	m, err := Parse([]byte(data))
	require.NoError(t, err)
	require.Equal(t, int16(42), m.SchemaID)
	require.Equal(t, int64(5), m.Version)
	require.Len(t, m.Files, 1)
	require.Equal(t, "base", m.Files[0].Tier)
	require.Equal(t, int64(1048576), m.Files[0].SizeBytes)
}

func TestManifest_Parse_Invalid(t *testing.T) {
	_, err := Parse([]byte(`not json`))
	require.Error(t, err)
}

func TestFilterByTier(t *testing.T) {
	m := &Manifest{
		SchemaID: 1,
		Files: []FileEntry{
			{Tier: "base", Path: "base1.parquet"},
			{Tier: "delta", Path: "delta1.parquet"},
			{Tier: "base", Path: "base2.parquet"},
			{Tier: "DELTA", Path: "delta2.parquet"}, // uppercase
		},
	}

	base := FilterByTier(m, "base")
	require.Len(t, base, 2)

	delta := FilterByTier(m, "delta")
	require.Len(t, delta, 2) // case-insensitive

	all := FilterByTier(m, "")
	require.Len(t, all, 4)
}

func TestFilterByTier_NilManifest(t *testing.T) {
	result := FilterByTier(nil, "base")
	require.Nil(t, result)
}

func TestListPaths(t *testing.T) {
	m := &Manifest{
		SchemaID: 1,
		Files: []FileEntry{
			{Tier: "base", Path: "base1.parquet"},
			{Tier: "delta", Path: "delta1.parquet"},
			{Tier: "base", Path: "base2.parquet"},
		},
	}

	paths := ListPaths(m, "base")
	require.Len(t, paths, 2)
	require.Contains(t, paths, "base1.parquet")
	require.Contains(t, paths, "base2.parquet")
}

func TestPathResolver_Resolve(t *testing.T) {
	r := PathResolver{
		Prefix:       "myproject",
		PathTemplate: "manifest/{{.SchemaID}}.json",
	}

	path, err := r.Resolve(42)
	require.NoError(t, err)
	require.Equal(t, "myproject/manifest/42.json", path)
}

func TestPathResolver_Resolve_DefaultTemplate(t *testing.T) {
	r := PathResolver{
		Prefix: "data",
	}

	path, err := r.Resolve(1)
	require.NoError(t, err)
	require.Equal(t, "data/manifest/1.json", path)
}

func TestPathResolver_Resolve_NoPrefix(t *testing.T) {
	r := PathResolver{}

	path, err := r.Resolve(99)
	require.NoError(t, err)
	require.Equal(t, "manifest/99.json", path)
}

func TestPathResolver_Resolve_InvalidTemplate(t *testing.T) {
	r := PathResolver{
		PathTemplate: "{{.Invalid}}",
	}

	_, err := r.Resolve(1)
	// Should succeed but with <no value>
	require.NoError(t, err)
}

func TestFSStore_Load(t *testing.T) {
	m := &Manifest{
		SchemaID:    1,
		Version:     1,
		UpdatedAtMs: time.Now().UnixMilli(),
		Files:       []FileEntry{},
	}
	data, _ := json.Marshal(m)

	fs := fstest.MapFS{
		"manifest/1.json": &fstest.MapFile{Data: data},
	}

	store := &FSStore{Root: fs}
	loaded, etag, err := store.Load(context.Background(), "manifest/1.json")
	require.NoError(t, err)
	require.Empty(t, etag) // FS store doesn't support etags
	require.NotNil(t, loaded)

	parsed, err := Parse(loaded)
	require.NoError(t, err)
	require.Equal(t, int16(1), parsed.SchemaID)
}

func TestFSStore_Load_NotFound(t *testing.T) {
	fs := fstest.MapFS{}
	store := &FSStore{Root: fs}

	_, _, err := store.Load(context.Background(), "missing.json")
	require.Error(t, err)
}

func TestFSStore_Load_NilStore(t *testing.T) {
	var store *FSStore
	_, _, err := store.Load(context.Background(), "any.json")
	require.Error(t, err)
	require.Contains(t, err.Error(), "not configured")
}

func TestLoad_IntegrationWithStore(t *testing.T) {
	m := &Manifest{
		SchemaID:    42,
		Version:     3,
		UpdatedAtMs: 1700000000000,
		Files: []FileEntry{
			{Tier: "base", Path: "test.parquet", RowCount: 100},
		},
	}
	data, _ := json.MarshalIndent(m, "", "  ")

	fs := fstest.MapFS{
		"test.json": &fstest.MapFile{Data: data},
	}
	store := &FSStore{Root: fs}

	loaded, etag, err := Load(context.Background(), store, "test.json")
	require.NoError(t, err)
	require.Empty(t, etag)
	require.Equal(t, int16(42), loaded.SchemaID)
	require.Len(t, loaded.Files, 1)
}

// memStore is an in-memory Store for exercising load-modify-save flows.
type memStore struct {
	data map[string][]byte
	gen  int
}

func (s *memStore) Load(_ context.Context, path string) ([]byte, string, error) {
	b, ok := s.data[path]
	if !ok {
		return nil, "", errors.New("NoSuchKey: not found")
	}
	return b, fmt.Sprintf("e%d", s.gen), nil
}

func (s *memStore) Save(_ context.Context, path string, data []byte, _ string) (string, error) {
	if s.data == nil {
		s.data = map[string][]byte{}
	}
	s.data[path] = data
	s.gen++
	return fmt.Sprintf("e%d", s.gen), nil
}

func TestReplaceTierFiles_ReplacesTierPreservesOthers(t *testing.T) {
	ctx := context.Background()
	st := &memStore{}
	path := "manifest/1.json"

	// Seed: two delta entries around a base tier that carries a historical
	// duplicate and a stale range — exactly what the old append/upsert
	// semantics could accumulate.
	seed := &Manifest{SchemaID: 1, Files: []FileEntry{
		{Tier: "delta", Path: "p/1/d1.parquet", RowCount: 4},
		{Tier: "base", Path: "p/1/a_b.parquet", RowCount: 10},
		{Tier: "base", Path: "p/1/a_b.parquet", RowCount: 10}, // historical duplicate
		{Tier: "delta", Path: "p/1/d2.parquet", RowCount: 2},
		{Tier: "base", Path: "p/1/c_d.parquet", RowCount: 7}, // stale range
	}}
	if _, err := Save(ctx, st, path, seed, ""); err != nil {
		t.Fatalf("seed save: %v", err)
	}

	if err := ReplaceTierFiles(ctx, st, path, 1, "base", []FileEntry{
		{Tier: "base", Path: "p/1/a_e.parquet", RowCount: 12},
		{Tier: "base", Path: "p/1/f_g.parquet", RowCount: 3},
	}); err != nil {
		t.Fatalf("replace: %v", err)
	}

	m, _, err := Load(ctx, st, path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	wantPaths := []string{"p/1/d1.parquet", "p/1/d2.parquet", "p/1/a_e.parquet", "p/1/f_g.parquet"}
	if len(m.Files) != len(wantPaths) {
		t.Fatalf("manifest has %d files, want %d: %+v", len(m.Files), len(wantPaths), m.Files)
	}
	for i, want := range wantPaths {
		if m.Files[i].Path != want {
			t.Fatalf("entry %d = %s, want %s (delta order preserved, base replaced)", i, m.Files[i].Path, want)
		}
	}
}

func TestReplaceTierFiles_CreatesFreshManifest(t *testing.T) {
	ctx := context.Background()
	st := &memStore{}
	path := "manifest/1.json"

	if err := ReplaceTierFiles(ctx, st, path, 1, "base", []FileEntry{
		{Tier: "base", Path: "p/1/a_b.parquet", RowCount: 10},
	}); err != nil {
		t.Fatalf("replace on empty store: %v", err)
	}
	m, _, err := Load(ctx, st, path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(m.Files) != 1 || m.Files[0].Path != "p/1/a_b.parquet" || m.Version != 1 {
		t.Fatalf("fresh manifest = %+v (version %d), want single a_b.parquet at version 1", m.Files, m.Version)
	}
}
