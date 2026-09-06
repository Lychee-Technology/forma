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

	paths, _, err := src.Paths(context.Background(), 7)
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

	paths, _, err := src.Paths(context.Background(), 7)
	require.NoError(t, err)
	require.Equal(t, []string{"s3://b/delta/7/a.parquet"}, paths)
}

// A zero stamp proves nothing (#522). The two-probe template check renders
// schema IDs 1 and 2 only, so a template that collides only from 3 upward
// passes it; a zero-stamped manifest at the shared path would then be served
// under whichever schema resolves it — the scan does not filter by schema and
// the projection stamps every row as the requested schema. A stamped manifest
// at that path is caught by the mismatch check; the zero stamp used to be the
// blind spot. No Forma writer has ever produced a zero stamp, so refusing it
// costs nothing a Forma-written deployment has.
const collidingOutsideProbesTemplate = "{{if lt .SchemaID 3}}manifest/{{.SchemaID}}.json{{else}}manifest/shared.json{{end}}"

func TestQuerySource_UnstampedManifestAtSharedPathFailsRead(t *testing.T) {
	t.Parallel()
	require.NoError(t, forma.ValidateManifestPathTemplate("manifest-template", collidingOutsideProbesTemplate),
		"the two-probe check cannot see a collision at schema IDs 3 and 4")

	// Schema 3's delta, listed by a manifest that names no owner, at the
	// path schema 4 resolves too.
	store := seedManifestStore(t, "manifest/shared.json", &Manifest{
		SchemaID: 0,
		Files:    []FileEntry{{Path: "delta/3/a.parquet", Tier: "delta"}},
	})

	src := &QuerySource{
		Store:    store,
		Resolver: PathResolver{PathTemplate: collidingOutsideProbesTemplate},
		Bucket:   "b",
	}

	paths, _, err := src.Paths(context.Background(), 4)
	require.Error(t, err, "a zero-stamped manifest with entries must not serve paths for any schema")
	require.Nil(t, paths, "no path may escape to the scanner")
	require.ErrorIs(t, err, forma.ErrManifestUnstamped)
	require.ErrorIs(t, err, forma.ErrManifestSchemaMismatch, "so the read is classified, and not degradable")

	var typed *forma.ManifestUnstampedError
	require.True(t, errors.As(err, &typed))
	require.Equal(t, int16(4), typed.RequestedSchemaID)
	require.Equal(t, "manifest/shared.json", typed.Path)
	require.Equal(t, 1, typed.Entries)

	// Schema 3 is refused the same way: the manifest names no owner, so it
	// is nobody's.
	_, _, err = src.Paths(context.Background(), 3)
	require.ErrorIs(t, err, forma.ErrManifestUnstamped)
}

// An empty zero-stamped manifest has nothing another schema could own; it
// loads as empty and the read takes the fallback exactly as a stamped empty
// manifest would.
func TestQuerySource_EmptyUnstampedManifestFallsBack(t *testing.T) {
	t.Parallel()

	store := seedManifestStore(t, "manifest/7.json", &Manifest{SchemaID: 0, Files: []FileEntry{}})
	src := &QuerySource{
		Store:    store,
		Resolver: PathResolver{PathTemplate: "manifest/{{.SchemaID}}.json"},
		Bucket:   "b",
		Fallback: func(schemaID int16) string { return "s3://b/delta/7/*.parquet" },
	}

	paths, _, err := src.Paths(context.Background(), 7)
	require.NoError(t, err)
	require.Equal(t, []string{"s3://b/delta/7/*.parquet"}, paths)
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

	paths, _, err := src.Paths(context.Background(), 7)
	require.NoError(t, err)
	require.Equal(t, []string{"s3://b/delta/7/*.parquet"}, paths)
}
