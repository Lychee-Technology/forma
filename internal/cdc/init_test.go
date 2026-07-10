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
