package cdc

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/lychee-technology/forma/internal/manifest"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// Checksum stamping is best-effort, so its failure must never fail the run —
// but it must not vanish either. An unstamped entry silently costs a later
// verification pass its ability to detect mutation on that file, and a
// GetObject failing on bytes the export just published is a signal about the
// store. Under zap.NewNop() a swallowed error and a reported one look
// identical, so these tests observe the log (#347).

// requireStampWarn asserts the single WARN a failed checksum stamp must emit.
func requireStampWarn(t *testing.T, logs *observer.ObservedLogs, message, errKey, finalKey string) {
	t.Helper()

	entries := logs.All()
	require.Len(t, entries, 1, "the discarded checksum error must be reported exactly once")
	require.Equal(t, message, entries[0].Message)

	fields := entries[0].ContextMap()
	require.Equal(t, int16(7), fields["schema_id"], "the warning must name the schema whose entry went unstamped")
	require.Equal(t, finalKey, fields["final_key"], "the warning must name the object that went unstamped")
	require.Contains(t, fmt.Sprint(fields[errKey]), "get object failed",
		"the cause must survive into the log, not just the fact of failure")
}

func TestInitStampChecksumWarnsWhenHashFails(t *testing.T) {
	core, logs := observer.New(zap.WarnLevel)
	runCtx := &initRunContext{
		cfg:           CDCConfig{S3Bucket: "b"},
		logger:        zap.New(core),
		manifestStore: &memManifestStore{},
		checksumObject: func(context.Context, string) (string, error) {
			return "sha256:partial", errors.New("get object failed")
		},
	}

	got := initStampChecksum(context.Background(), runCtx, 7, schemaBatchExport{finalKey: "base/7/x.parquet"})
	require.Empty(t, got, "a failed hash leaves the entry unstamped")

	requireStampWarn(t, logs, "failed to checksum final base object; manifest entry stays unstamped",
		"error", "base/7/x.parquet")
}

// The warning above is only meaningful if the healthy path is genuinely quiet.
func TestInitStampChecksumStaysSilentOnSuccess(t *testing.T) {
	core, logs := observer.New(zap.WarnLevel)
	runCtx := &initRunContext{
		cfg:           CDCConfig{S3Bucket: "b"},
		logger:        zap.New(core),
		manifestStore: &memManifestStore{},
		checksumObject: func(context.Context, string) (string, error) {
			return "sha256:deadbeef", nil
		},
	}

	got := initStampChecksum(context.Background(), runCtx, 7, schemaBatchExport{finalKey: "base/7/x.parquet"})
	require.Equal(t, "sha256:deadbeef", got)
	require.Zero(t, logs.Len(), "nothing to report when the hash succeeds")
}

func TestFlushStampChecksumWarnsWhenHashFails(t *testing.T) {
	core, logs := observer.New(zap.WarnLevel)
	executor := &flushBatchExecutor{
		cfg:           CDCConfig{S3Bucket: "b"},
		schemaID:      7,
		logger:        zap.New(core),
		manifestStore: &memManifestStore{},
		checksumObject: func(context.Context, string) (string, error) {
			return "sha256:partial", errors.New("get object failed")
		},
	}

	got := executor.stampChecksum(context.Background(), "cdc/7/delta-x.parquet")
	require.Empty(t, got, "a failed hash leaves the entry unstamped")

	requireStampWarn(t, logs, "failed to checksum final delta object; manifest entry stays unstamped",
		"err", "cdc/7/delta-x.parquet")
}

func TestFlushStampChecksumStaysSilentOnSuccess(t *testing.T) {
	core, logs := observer.New(zap.WarnLevel)
	executor := &flushBatchExecutor{
		cfg:           CDCConfig{S3Bucket: "b"},
		schemaID:      7,
		logger:        zap.New(core),
		manifestStore: &memManifestStore{},
		checksumObject: func(context.Context, string) (string, error) {
			return "sha256:deadbeef", nil
		},
	}

	got := executor.stampChecksum(context.Background(), "cdc/7/delta-x.parquet")
	require.Equal(t, "sha256:deadbeef", got)
	require.Zero(t, logs.Len(), "nothing to report when the hash succeeds")
}

// An empty zero-stamped manifest is a hand-made object (no Forma writer has
// ever produced a zero stamp), and cdc-init is the run that gives it an
// identity: manifest.VerifySchemaStamp stamps it in memory and the coming
// save persists that. The conversion must be visible in the log, not happen
// silently between two loads (#522 review).
func TestLoadInitManifestLogsEmptyUnstampedStamp(t *testing.T) {
	st := &memManifestStore{}
	_, err := manifest.Save(context.Background(), st, "manifest/7.json", &manifest.Manifest{Files: []manifest.FileEntry{}}, "")
	require.NoError(t, err)
	core, logs := observer.New(zap.InfoLevel)
	runCtx := &initRunContext{logger: zap.New(core), manifestStore: st}

	m, _, err := loadInitManifest(context.Background(), runCtx, 7, "manifest/7.json")
	require.NoError(t, err)
	require.Equal(t, int16(7), m.SchemaID, "the empty manifest is stamped for the requested schema")

	entries := logs.All()
	require.Len(t, entries, 1, "the in-memory stamp must be reported exactly once")
	require.Equal(t, "empty manifest carries no schema stamp; stamping it for this schema", entries[0].Message)
	fields := entries[0].ContextMap()
	require.Equal(t, int16(7), fields["schema_id"])
	require.Equal(t, "manifest/7.json", fields["manifest_path"])
}

// The log above is only meaningful if an already-stamped manifest loads quietly.
func TestLoadInitManifestStaysSilentForStampedManifest(t *testing.T) {
	st := &memManifestStore{}
	_, err := manifest.Save(context.Background(), st, "manifest/7.json", &manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{}}, "")
	require.NoError(t, err)
	core, logs := observer.New(zap.InfoLevel)
	runCtx := &initRunContext{logger: zap.New(core), manifestStore: st}

	_, _, err = loadInitManifest(context.Background(), runCtx, 7, "manifest/7.json")
	require.NoError(t, err)
	require.Empty(t, logs.All(), "a stamped manifest is not re-stamped and must not log")
}
