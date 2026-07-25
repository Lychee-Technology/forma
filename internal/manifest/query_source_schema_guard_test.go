package manifest

import (
	"context"
	"errors"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// A manifest object addresses one schema by convention only: the path template
// puts the schema ID in the key. Nothing downstream re-checks it — the s3_source
// CTE does not filter parquet rows by schema (files are per-schema by path), and
// the schema projection stamps whatever it scans as the REQUESTED schema. So a
// template collision that points two schemas at one manifest would not merely
// under-read: it would return another schema's rows labelled as this schema's.
//
// Config validation cannot rule that out — it samples two schema IDs, which
// catches collapse but cannot prove injectivity over the whole domain. This
// guard is the enforcement: the stamp inside the manifest must agree with the
// schema being asked for, checked before any path reaches a scan.

func TestQuerySource_ManifestSchemaMismatchFailsBeforeScan(t *testing.T) {
	t.Parallel()

	// A manifest stamped for schema 9, reachable at the path schema 7 resolves.
	store := seedManifestStore(t, "manifest/7.json", &Manifest{
		SchemaID: 9,
		Files:    []FileEntry{{Path: "delta/9/a.parquet", Tier: "delta"}},
	})

	src := &QuerySource{
		Store:    store,
		Resolver: PathResolver{PathTemplate: "manifest/{{.SchemaID}}.json"},
		Bucket:   "b",
	}

	paths, err := src.Paths(context.Background(), 7)
	require.Error(t, err, "a manifest stamped for another schema must not serve paths")
	require.Nil(t, paths, "no path may escape to the scanner")
	require.ErrorIs(t, err, forma.ErrManifestSchemaMismatch)

	var typed *forma.ManifestSchemaMismatchError
	require.True(t, errors.As(err, &typed))
	require.Equal(t, int16(7), typed.RequestedSchemaID)
	require.Equal(t, int16(9), typed.ManifestSchemaID)
}

// A matching stamp is the normal case and must be untouched.
func TestQuerySource_MatchingManifestSchemaResolves(t *testing.T) {
	t.Parallel()

	store := seedManifestStore(t, "manifest/7.json", &Manifest{
		SchemaID: 7,
		Files:    []FileEntry{{Path: "delta/7/a.parquet", Tier: "delta"}},
	})

	src := &QuerySource{
		Store:    store,
		Resolver: PathResolver{PathTemplate: "manifest/{{.SchemaID}}.json"},
		Bucket:   "b",
	}

	paths, err := src.Paths(context.Background(), 7)
	require.NoError(t, err)
	require.Equal(t, []string{"s3://b/delta/7/a.parquet"}, paths)
}

// Compatibility carve-out: schema IDs are always positive, so a zero stamp means
// the manifest predates the field rather than belonging to schema 0. Rejecting
// those would break reads for any deployment holding pre-stamp objects, so the
// guard skips them — every current writer stamps the field (LoadOrCreate is the
// only construction site, and every Save path loads first).
func TestQuerySource_UnstampedManifestIsAccepted(t *testing.T) {
	t.Parallel()

	store := seedManifestStore(t, "manifest/7.json", &Manifest{
		SchemaID: 0,
		Files:    []FileEntry{{Path: "delta/7/a.parquet", Tier: "delta"}},
	})

	src := &QuerySource{
		Store:    store,
		Resolver: PathResolver{PathTemplate: "manifest/{{.SchemaID}}.json"},
		Bucket:   "b",
	}

	paths, err := src.Paths(context.Background(), 7)
	require.NoError(t, err, "an unstamped (pre-existing) manifest must still resolve")
	require.Equal(t, []string{"s3://b/delta/7/a.parquet"}, paths)
}

// seedManifestStore returns a memStore holding one manifest at path.
func seedManifestStore(t *testing.T, path string, m *Manifest) *memStore {
	t.Helper()
	st := &memStore{}
	_, err := Save(context.Background(), st, path, m, "")
	require.NoError(t, err)
	return st
}

// A missing manifest still takes the fallback, not the mismatch path:
// LoadOrCreate synthesizes an empty manifest stamped with the requested schema.
func TestQuerySource_MissingManifestStillFallsBack(t *testing.T) {
	t.Parallel()

	src := &QuerySource{
		Store:    &memStore{},
		Resolver: PathResolver{PathTemplate: "manifest/{{.SchemaID}}.json"},
		Bucket:   "b",
		Fallback: func(schemaID int16) string { return "s3://b/delta/7/*.parquet" },
	}

	paths, err := src.Paths(context.Background(), 7)
	require.NoError(t, err)
	require.Equal(t, []string{"s3://b/delta/7/*.parquet"}, paths)
}
