package cdc

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/manifest"
	"go.uber.org/zap"
)

// A zero-value CDCConfig must not silently disable the init export: without
// defaults, BatchSize=0 turns the batch query into LIMIT 0 and RunInit would
// report success after exporting nothing.
func TestNormalizeInitOptions_ZeroValueConfigGetsUsableBatchSize(t *testing.T) {
	opts := normalizeInitOptions(InitOptions{})

	if opts.Config.BatchSize <= 0 {
		t.Fatalf("normalized BatchSize = %d, want > 0", opts.Config.BatchSize)
	}
	if opts.Logger == nil {
		t.Fatal("normalized Logger is nil")
	}

	runCtx := &initRunContext{cfg: opts.Config, logger: opts.Logger}
	if got := resolveInitBatchSize(runCtx, 1, nil); got <= 0 {
		t.Fatalf("resolveInitBatchSize with defaulted config = %d, want > 0", got)
	}
}

func TestResolveInitBatchSize_RawZeroValueConfigIsZero(t *testing.T) {
	// Documents the trap normalizeInitOptions exists to prevent.
	runCtx := &initRunContext{cfg: CDCConfig{}, logger: zap.NewNop()}
	if got := resolveInitBatchSize(runCtx, 1, nil); got != 0 {
		t.Fatalf("resolveInitBatchSize with raw zero-value config = %d, want 0", got)
	}
}

// failingSaveStore reports "not found" on Load (so AppendFiles takes the
// create path) and fails every Save.
type failingSaveStore struct{ saveErr error }

func (s *failingSaveStore) Load(context.Context, string) ([]byte, string, error) {
	return nil, "", errors.New("NoSuchKey: not found")
}

func (s *failingSaveStore) Save(context.Context, string, []byte, string) (string, error) {
	return "", s.saveErr
}

func initStateWithOneEntry(schemaID int16) *schemaInitState {
	return &schemaInitState{
		schemaID: schemaID,
		fileEntries: []manifest.FileEntry{
			{Tier: "base", Path: "prefix/1/a_b.parquet", RowCount: 1},
		},
	}
}

func TestUpdateSchemaManifest_PropagatesSaveFailure(t *testing.T) {
	saveErr := errors.New("s3 write denied")
	runCtx := &initRunContext{
		manifestStore:    &failingSaveStore{saveErr: saveErr},
		manifestResolver: manifest.PathResolver{PathTemplate: "manifest/{{.SchemaID}}.json"},
		logger:           zap.NewNop(),
	}

	err := updateSchemaManifest(context.Background(), runCtx, initStateWithOneEntry(1))
	if err == nil {
		t.Fatal("updateSchemaManifest returned nil, want save failure to propagate")
	}
	if !errors.Is(err, saveErr) {
		t.Fatalf("updateSchemaManifest error = %v, want wrapped %v", err, saveErr)
	}
}

func TestUpdateSchemaManifest_PropagatesResolverFailure(t *testing.T) {
	runCtx := &initRunContext{
		manifestStore:    &failingSaveStore{saveErr: errors.New("unreached")},
		manifestResolver: manifest.PathResolver{PathTemplate: "{{.Missing"},
		logger:           zap.NewNop(),
	}

	err := updateSchemaManifest(context.Background(), runCtx, initStateWithOneEntry(1))
	if err == nil {
		t.Fatal("updateSchemaManifest returned nil, want resolver failure to propagate")
	}
	if !strings.Contains(err.Error(), "resolve manifest path") {
		t.Fatalf("updateSchemaManifest error = %v, want resolve manifest path context", err)
	}
}

func TestUpdateSchemaManifest_NoStoreOrDryRunIsNoop(t *testing.T) {
	state := initStateWithOneEntry(1)

	noStore := &initRunContext{logger: zap.NewNop()}
	if err := updateSchemaManifest(context.Background(), noStore, state); err != nil {
		t.Fatalf("updateSchemaManifest without store = %v, want nil", err)
	}

	dryRun := &initRunContext{
		manifestStore:    &failingSaveStore{saveErr: errors.New("unreached")},
		manifestResolver: manifest.PathResolver{PathTemplate: "manifest/{{.SchemaID}}.json"},
		logger:           zap.NewNop(),
		dryRun:           true,
	}
	if err := updateSchemaManifest(context.Background(), dryRun, state); err != nil {
		t.Fatalf("updateSchemaManifest dry-run = %v, want nil", err)
	}
}

// memManifestStore is an in-memory manifest.Store: Load reports not-found
// until the first Save, then returns the saved payload.
type memManifestStore struct {
	data map[string][]byte
}

func (s *memManifestStore) Load(_ context.Context, path string) ([]byte, string, error) {
	b, ok := s.data[path]
	if !ok {
		return nil, "", errors.New("NoSuchKey: not found")
	}
	return b, "", nil
}

func (s *memManifestStore) Save(_ context.Context, path string, data []byte, _ string) (string, error) {
	if s.data == nil {
		s.data = map[string][]byte{}
	}
	s.data[path] = data
	return "", nil
}

// A cdc-init rerun is a full re-export: recording it must reconcile the
// base tier to exactly the new run's entries — no duplicates, no stale
// ranges — while leaving delta entries alone (#176).
func TestUpdateSchemaManifest_RerunReconcilesBaseTier(t *testing.T) {
	st := &memManifestStore{}
	seed := &manifest.Manifest{SchemaID: 1, Files: []manifest.FileEntry{
		{Tier: "delta", Path: "prefix/1/d.parquet", RowCount: 5},
		{Tier: "base", Path: "prefix/1/a_b.parquet", RowCount: 1},
		{Tier: "base", Path: "prefix/1/a_b.parquet", RowCount: 1}, // historical duplicate
		{Tier: "base", Path: "prefix/1/stale_range.parquet", RowCount: 9},
	}}
	if _, err := manifest.Save(context.Background(), st, "manifest/1.json", seed, ""); err != nil {
		t.Fatalf("seed save: %v", err)
	}
	runCtx := &initRunContext{
		manifestStore:    st,
		manifestResolver: manifest.PathResolver{PathTemplate: "manifest/{{.SchemaID}}.json"},
		logger:           zap.NewNop(),
	}

	if err := updateSchemaManifest(context.Background(), runCtx, initStateWithOneEntry(1)); err != nil {
		t.Fatalf("rerun update: %v", err)
	}

	m, _, err := manifest.Load(context.Background(), st, "manifest/1.json")
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	if len(m.Files) != 2 {
		t.Fatalf("manifest has %d entries after rerun, want 2 (delta + one reconciled base): %+v", len(m.Files), m.Files)
	}
	if m.Files[0].Path != "prefix/1/d.parquet" || m.Files[1].Path != "prefix/1/a_b.parquet" {
		t.Fatalf("entries = %s,%s want delta preserved then reconciled base", m.Files[0].Path, m.Files[1].Path)
	}
}
