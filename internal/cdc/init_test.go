package cdc

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
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

func TestRecordSchemaBatchResultCarriesStamp(t *testing.T) {
	state := &schemaInitState{schemaID: 7}
	batch := schemaBatchExport{finalKey: "base/7/x.parquet"}
	cols := map[string]string{"row_id": "UUID", "changed_at": "BIGINT", "deleted_at": "BIGINT"}
	recordSchemaBatchResult(state, batch, 123, 456, cols, "sha256:abc")
	if got := state.fileEntries[0].Columns["row_id"]; got != "UUID" {
		t.Fatalf("base entry not stamped: %#v", state.fileEntries[0].Columns)
	}
	if got := state.fileEntries[0].Checksum; got != "sha256:abc" {
		t.Fatalf("base entry checksum = %q, want sha256:abc", got)
	}
}

// initStampColumns is best-effort: a failed probe returns nil (entry stays
// unstamped) and never errors the init run.
func TestInitStampColumnsBestEffort(t *testing.T) {
	runCtx := &initRunContext{
		cfg:           CDCConfig{S3Bucket: "b"},
		logger:        zap.NewNop(),
		manifestStore: &memManifestStore{},
		describeColumns: func(ctx context.Context, uri string) (map[string]string, error) {
			return nil, errors.New("footer read failed")
		},
	}
	if cols := initStampColumns(context.Background(), runCtx, 7, schemaBatchExport{finalKey: "base/7/x.parquet"}); cols != nil {
		t.Fatalf("failed probe must yield nil stamp, got %#v", cols)
	}
	// success path
	runCtx.describeColumns = func(ctx context.Context, uri string) (map[string]string, error) {
		if uri != "s3://b/base/7/x.parquet" {
			t.Fatalf("probe hit %q, want the final object URI", uri)
		}
		return map[string]string{"row_id": "UUID"}, nil
	}
	if cols := initStampColumns(context.Background(), runCtx, 7, schemaBatchExport{finalKey: "base/7/x.parquet"}); cols["row_id"] != "UUID" {
		t.Fatalf("stamp not returned: %#v", cols)
	}
}

// newChecksumInitContext builds the exportSchemaBatch fixture the init
// checksum tests share: a stubbed base export (no DuckDB or Postgres), a mock
// S3 client for the tmp->final copy and the size stat, a manifest store so
// stamping is enabled, and the caller's checksum seam.
func newChecksumInitContext(checksumObject func(ctx context.Context, key string) (string, error)) (*initRunContext, *schemaInitState) {
	runCtx := &initRunContext{
		cfg:            CDCConfig{S3Bucket: "test-bucket", S3Prefix: "cdc"},
		s3Client:       &objectOnlyS3Client{},
		logger:         zap.NewNop(),
		manifestStore:  &memManifestStore{},
		checksumObject: checksumObject,
		describeColumns: func(context.Context, string) (map[string]string, error) {
			return map[string]string{"row_id": "UUID"}, nil
		},
		exportBaseFile: func(context.Context, *schemaInitState, schemaBatchExport) error { return nil },
	}
	return runCtx, &schemaInitState{schemaID: 7}
}

func initBatchRowIDs() []uuid.UUID {
	return []uuid.UUID{uuid.MustParse("018f05c0-0000-7000-8000-000000000001")}
}

// The published base object's content hash is stamped into the manifest entry
// so a later verification pass can detect mutation without re-reading the tier
// (#347).
func TestExportSchemaBatchStampsChecksumOnBaseEntry(t *testing.T) {
	var seenKey string
	runCtx, state := newChecksumInitContext(func(_ context.Context, key string) (string, error) {
		seenKey = key
		return "sha256:deadbeef", nil
	})

	if err := exportSchemaBatch(context.Background(), runCtx, state, initBatchRowIDs()); err != nil {
		t.Fatalf("exportSchemaBatch: %v", err)
	}

	if len(state.fileEntries) != 1 {
		t.Fatalf("recorded %d file entries, want 1", len(state.fileEntries))
	}
	entry := state.fileEntries[0]
	if entry.Checksum != "sha256:deadbeef" {
		t.Fatalf("base entry checksum = %q, want the seam's hash", entry.Checksum)
	}
	// The hash must bless the published object, not the tmp one the export wrote.
	if seenKey != entry.Path {
		t.Fatalf("hashed key %q, want the entry's final key %q", seenKey, entry.Path)
	}
	if strings.Contains(seenKey, "_tmp") {
		t.Fatalf("hashed the tmp object %q instead of the final one", seenKey)
	}
}

// A failed hash must not fail the init run and must leave the entry unstamped —
// verification passes skip an empty Checksum (stampColumns/SizeBytes
// precedent).
func TestExportSchemaBatchChecksumFailureLeavesEntryUnstamped(t *testing.T) {
	// The seam returns a value alongside the error — a partial hash over a
	// truncated read is exactly what must never reach the manifest, so the
	// error, not the emptiness of the value, has to be what discards it.
	runCtx, state := newChecksumInitContext(func(context.Context, string) (string, error) {
		return "sha256:partial", errors.New("get object failed")
	})

	if err := exportSchemaBatch(context.Background(), runCtx, state, initBatchRowIDs()); err != nil {
		t.Fatalf("checksum failure must not fail the init run: %v", err)
	}

	entry := state.fileEntries[0]
	if entry.Checksum != "" {
		t.Fatalf("base entry checksum = %q, want unstamped", entry.Checksum)
	}
	// The rest of the entry is unaffected: only the checksum is best-effort.
	if entry.RowCount != 1 || entry.Columns["row_id"] != "UUID" {
		t.Fatalf("failed checksum damaged the entry: %#v", entry)
	}
	if state.rowsExported != 1 || state.filesCreated != 1 {
		t.Fatalf("rowsExported=%d filesCreated=%d, want 1/1", state.rowsExported, state.filesCreated)
	}
}

// Deployments without the seam wired (no full S3 client) keep the pre-#347
// behavior: the entry is recorded, just unstamped.
func TestExportSchemaBatchWithoutChecksumSeamLeavesEntryUnstamped(t *testing.T) {
	runCtx, state := newChecksumInitContext(nil)

	if err := exportSchemaBatch(context.Background(), runCtx, state, initBatchRowIDs()); err != nil {
		t.Fatalf("exportSchemaBatch: %v", err)
	}
	if got := state.fileEntries[0].Checksum; got != "" {
		t.Fatalf("base entry checksum = %q, want unstamped", got)
	}
}

// initStampChecksum short-circuits when manifestStore is nil: the fileEntries
// are never persisted (updateSchemaManifest no-ops), so hashing would be a
// discarded full-object S3 read per batch.
func TestInitStampChecksumShortCircuitWhenNoManifestStore(t *testing.T) {
	runCtx := &initRunContext{
		cfg:           CDCConfig{S3Bucket: "b"},
		logger:        zap.NewNop(),
		manifestStore: nil,
		checksumObject: func(context.Context, string) (string, error) {
			t.Fatal("checksum must not be computed when manifestStore is nil")
			return "", nil
		},
	}
	if got := initStampChecksum(context.Background(), runCtx, 7, schemaBatchExport{finalKey: "base/7/x.parquet"}); got != "" {
		t.Fatalf("manifest store nil must yield no checksum, got %q", got)
	}
}

// initStampColumns short-circuits when manifestStore is nil (no manifest
// to persist fileEntries to, so footer probe would be wasted).
func TestInitStampColumnsShortCircuitWhenNoManifestStore(t *testing.T) {
	runCtx := &initRunContext{
		cfg:           CDCConfig{S3Bucket: "b"},
		logger:        zap.NewNop(),
		manifestStore: nil,
		describeColumns: func(ctx context.Context, uri string) (map[string]string, error) {
			t.Fatalf("describe must not be called when manifestStore is nil")
			return nil, nil
		},
	}
	if cols := initStampColumns(context.Background(), runCtx, 7, schemaBatchExport{finalKey: "base/7/x.parquet"}); cols != nil {
		t.Fatalf("manifest store nil must yield nil stamp, got %#v", cols)
	}
}
