package cdc

import (
	"context"
	"errors"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/manifest"
	"github.com/stretchr/testify/require"
)

const (
	guardDeltaKey     = "delta/1/0b2f1b6e-1111-4c1d-8e57-000000000001.parquet"
	guardOtherSchema  = "delta/2/0b2f1b6e-2222-4c1d-8e57-000000000002.parquet"
	guardBaseUnderTag = "base/1/a_b_0b2f1b6e-3333-4c1d-8e57-000000000003.parquet"
)

// The purge trusts the key shape, not the `Tier: "delta"` label: only a
// delta-shaped object inside this schema's delta namespace is deletable.
func TestOwnedDeltaKey(t *testing.T) {
	cases := []struct {
		prefix string
		key    string
		want   bool
	}{
		{"delta", guardDeltaKey, true},
		{"delta/", guardDeltaKey, true},
		{"delta", guardOtherSchema, false},
		{"delta", guardBaseUnderTag, false},
		{"delta", "delta/1/base-0b2f1b6e-1111-4c1d-8e57-000000000001.parquet", false},
		{"delta", "delta/1/_tmp/0b2f1b6e-1111-4c1d-8e57-000000000001.parquet", false},
		{"delta", "other/1/0b2f1b6e-1111-4c1d-8e57-000000000001.parquet", false},
		{"delta", "delta/10/0b2f1b6e-1111-4c1d-8e57-000000000001.parquet", false},
		// No prefix: the prefix part is unconstrained, the /1/<uuid>.parquet tail is not.
		{"", guardDeltaKey, true},
		{"", "lake/v2/delta/1/0b2f1b6e-1111-4c1d-8e57-000000000001.parquet", true},
		{"", "1/0b2f1b6e-1111-4c1d-8e57-000000000001.parquet", true},
		{"", guardOtherSchema, false},
		{"", guardBaseUnderTag, false},
		{"", "delta/11/0b2f1b6e-1111-4c1d-8e57-000000000001.parquet", false},
		{"", "0b2f1b6e-1111-4c1d-8e57-000000000001.parquet", false},
		{"", "delta/1", false},
	}
	for _, c := range cases {
		require.Equal(t, c.want, ownedDeltaKey(c.prefix, 1, c.key), "prefix=%q key=%s", c.prefix, c.key)
	}
}

// A same-bucket entry labelled delta that lives outside the schema's delta
// namespace (another schema's delta, a base-shaped key) counts toward the
// refusal without the flag and fails the schema closed with it — named,
// before any export, never deleted (#371 review P1).
func TestPreflightDeltaTier_ForeignDeltaEntriesFailClosed(t *testing.T) {
	st := &memManifestStore{}
	seedDeltaManifest(t, st, guardOtherSchema, guardBaseUnderTag)
	runCtx := deltaRunContext(st, []string{})

	_, err := preflightDeltaTier(context.Background(), runCtx, 1)
	require.ErrorIs(t, err, ErrDeltaTierPresent)
	require.Contains(t, err.Error(), "0 manifest-listed and 0 unlisted delta objects (2 entries a purge could not honour)")

	runCtx.replaceDelta = true
	_, err = preflightDeltaTier(context.Background(), runCtx, 1)
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrDeltaTierPresent)
	require.Contains(t, err.Error(), "refuses to delete")
	require.Contains(t, err.Error(), "delta namespace delta/1/")
	require.Contains(t, err.Error(), guardOtherSchema)
	require.Contains(t, err.Error(), guardBaseUnderTag)
}

// A manifest whose stamp names another schema is a collided or mis-pointed
// template; the pre-flight refuses it in both flag modes rather than
// inventory (and later purge) that schema's delta tier.
func TestPreflightDeltaTier_RefusesForeignSchemaManifest(t *testing.T) {
	st := &memManifestStore{}
	m := &manifest.Manifest{SchemaID: 2, Files: []manifest.FileEntry{{Tier: "delta", Path: guardDeltaKey}}}
	_, err := manifest.Save(context.Background(), st, "manifest/1.json", m, "")
	require.NoError(t, err)

	for _, flag := range []bool{false, true} {
		runCtx := deltaRunContext(st, []string{})
		runCtx.replaceDelta = flag
		_, err := preflightDeltaTier(context.Background(), runCtx, 1)
		require.ErrorIs(t, err, forma.ErrManifestSchemaMismatch, "replaceDelta=%t", flag)
		var mismatch *forma.ManifestSchemaMismatchError
		require.ErrorAs(t, err, &mismatch)
		require.Equal(t, int16(2), mismatch.ManifestSchemaID)
		require.Equal(t, "manifest/1.json", mismatch.Path)
	}
}

// schemaSwappingStore rejects the first save with a confirmed 412 and, as if
// another writer had re-pointed the object, restamps the stored manifest as
// schema 2.
type schemaSwappingStore struct {
	memManifestStore
	saves int
}

func (s *schemaSwappingStore) Save(ctx context.Context, path string, data []byte, etag string) (string, error) {
	s.saves++
	if s.saves > 1 {
		return s.memManifestStore.Save(ctx, path, data, etag)
	}
	m, _, err := manifest.Load(ctx, &s.memManifestStore, path)
	if err != nil {
		return "", err
	}
	m.SchemaID = 2
	if _, err := manifest.Save(ctx, &s.memManifestStore, path, m, ""); err != nil {
		return "", err
	}
	return "", &preconditionFailedErr{}
}

// The 412 reload goes through the same schema check as the pre-flight, in
// both flag modes: a manifest that changed identity mid-run is not published
// over, and nothing is purged.
func TestUpdateSchemaManifest_ReloadRefusesForeignSchemaManifest(t *testing.T) {
	for _, flag := range []bool{false, true} {
		st := &schemaSwappingStore{}
		seedDeltaManifest(t, &st.memManifestStore, guardDeltaKey)
		runCtx := deltaRunContext(st, nil)
		runCtx.replaceDelta = flag
		state := initStateWithOneEntry(1)

		err := updateSchemaManifest(context.Background(), runCtx, state)
		require.ErrorIs(t, err, forma.ErrManifestSchemaMismatch, "replaceDelta=%t", flag)
		require.Equal(t, 1, st.saves, "the reload must refuse before a second save")
	}
}

// committingLossyStore commits every save and then reports a transport
// error, the shape of a PUT whose 200 was lost on the wire.
type committingLossyStore struct {
	recordingStore
	lossErr error
}

func (s *committingLossyStore) Save(ctx context.Context, path string, data []byte, etag string) (string, error) {
	if _, err := s.recordingStore.Save(ctx, path, data, etag); err != nil {
		return "", err
	}
	return "", s.lossErr
}

// #371 review P1: a swap that committed before its response was lost must
// still be followed by the purge — otherwise the old delta objects become
// unlisted orphans that manifest-reconcile --repair re-adopts. The reload
// proves the commit (this run's write-once base entries, empty delta).
func TestPublishAndPurge_CommittedSaveWithLostResponseStillPurges(t *testing.T) {
	var events []string
	st := &committingLossyStore{recordingStore: recordingStore{events: &events}, lossErr: errors.New("connection reset")}
	seedDeltaManifest(t, &st.memManifestStore, guardDeltaKey)
	runCtx := deltaRunContext(st, nil)
	runCtx.replaceDelta = true
	runCtx.deleteObject = recordingDelete(&events)
	state := initStateWithOneEntry(1)
	state.deltaPurge = []string{guardDeltaKey}

	require.NoError(t, publishAndPurge(context.Background(), runCtx, state))
	require.Equal(t, []string{"save", "delete " + guardDeltaKey}, events)

	// Without --replace-delta there is no purge to protect, so the ambiguous
	// error keeps surfacing as before (no blind retry, no confirmation).
	events = nil
	st = &committingLossyStore{recordingStore: recordingStore{events: &events}, lossErr: errors.New("connection reset")}
	seedDeltaManifest(t, &st.memManifestStore)
	runCtx = deltaRunContext(st, nil)
	err := publishAndPurge(context.Background(), runCtx, initStateWithOneEntry(1))
	require.ErrorIs(t, err, st.lossErr)
	require.Equal(t, []string{"save"}, events)
}

// A save that did not commit stays ambiguous-resolved-as-failed: the reload
// shows the pre-swap manifest, the error says so, and nothing is deleted.
func TestPublishAndPurge_UncommittedSaveDeletesNothing(t *testing.T) {
	var events []string
	saveErr := errors.New("s3 write denied")
	st := &recordingStore{events: &events, saveErr: saveErr}
	seedDeltaManifest(t, &st.memManifestStore, guardDeltaKey)
	runCtx := deltaRunContext(st, nil)
	runCtx.replaceDelta = true
	runCtx.deleteObject = recordingDelete(&events)
	state := initStateWithOneEntry(1)
	state.deltaPurge = []string{guardDeltaKey}

	err := publishAndPurge(context.Background(), runCtx, state)
	require.ErrorIs(t, err, saveErr)
	require.Contains(t, err.Error(), "did not commit")
	require.Equal(t, []string{"save"}, events)
}

// reloadFailingStore fails every save and every load after the first, so
// the confirmation reload cannot tell whether the swap landed.
type reloadFailingStore struct {
	recordingStore
	loads     int
	reloadErr error
}

func (s *reloadFailingStore) Load(ctx context.Context, path string) ([]byte, string, error) {
	s.loads++
	if s.loads > 1 {
		return nil, "", s.reloadErr
	}
	return s.recordingStore.Load(ctx, path)
}

func TestPublishAndPurge_AmbiguousSaveWithFailedReloadDeletesNothing(t *testing.T) {
	var events []string
	saveErr, reloadErr := errors.New("connection reset"), errors.New("access denied")
	st := &reloadFailingStore{recordingStore: recordingStore{events: &events, saveErr: saveErr}, reloadErr: reloadErr}
	seedDeltaManifest(t, &st.memManifestStore, guardDeltaKey)
	runCtx := deltaRunContext(st, nil)
	runCtx.replaceDelta = true
	runCtx.deleteObject = recordingDelete(&events)
	state := initStateWithOneEntry(1)
	state.deltaPurge = []string{guardDeltaKey}

	err := publishAndPurge(context.Background(), runCtx, state)
	require.ErrorIs(t, err, saveErr)
	require.ErrorIs(t, err, reloadErr)
	require.Contains(t, err.Error(), "rerun cdc-init --replace-delta")
	require.Equal(t, []string{"save"}, events)
}

func TestSwapCommitted(t *testing.T) {
	entries := []manifest.FileEntry{{Tier: "base", Path: "prefix/1/a_b.parquet"}}
	committed := &manifest.Manifest{SchemaID: 1, Files: entries}
	require.True(t, swapCommitted(committed, entries))

	withDelta := &manifest.Manifest{SchemaID: 1, Files: append([]manifest.FileEntry{{Tier: "delta", Path: guardDeltaKey}}, entries...)}
	require.False(t, swapCommitted(withDelta, entries), "a listed delta entry means the swap did not land")

	otherBase := &manifest.Manifest{SchemaID: 1, Files: []manifest.FileEntry{{Tier: "base", Path: "base/1/old_range.parquet"}}}
	require.False(t, swapCommitted(otherBase, entries), "someone else's base set is not this run's publish")

	superset := &manifest.Manifest{SchemaID: 1, Files: append([]manifest.FileEntry{{Tier: "base", Path: "base/1/old_range.parquet"}}, entries...)}
	require.False(t, swapCommitted(superset, entries), "the swap replaces the base tier wholesale")
}

// The run validates --manifest-template with the read path's two-probe rule
// before opening a single connection: a template that collapses schemas
// onto one object would otherwise let --replace-delta purge another
// schema's delta tier.
func TestRunInit_RejectsCollapsingManifestTemplate(t *testing.T) {
	for _, tmpl := range []string{"manifest/all.json", "manifest/{{.SchemaId}}.json", "{{"} {
		_, err := RunInit(context.Background(), InitOptions{Config: CDCConfig{ManifestTemplate: tmpl}, ReplaceDelta: true})
		var cfgErr *forma.ConfigError
		require.ErrorAs(t, err, &cfgErr, "template %q", tmpl)
		require.Equal(t, "manifest-template", cfgErr.Field)
		require.Contains(t, err.Error(), "cdc init pre-flight")
	}
}

// collidingOutsideProbesTemplate renders distinct paths for the two probe
// IDs (1 and 2) and one shared path for every other schema, so it passes
// forma.ValidateManifestPathTemplate; a zero-stamped manifest at the shared
// path is then invisible to the schema-identity check, and cdc-init must not
// export, publish or purge on its strength (#371 review, round four).
const collidingOutsideProbesTemplate = "{{if lt .SchemaID 3}}manifest/{{.SchemaID}}.json{{else}}manifest/shared.json{{end}}"

func seedUnstampedSharedManifest(t *testing.T, st manifest.Store, files ...manifest.FileEntry) {
	t.Helper()
	m := &manifest.Manifest{Files: append([]manifest.FileEntry{}, files...)}
	_, err := manifest.Save(context.Background(), st, "manifest/shared.json", m, "")
	require.NoError(t, err)
}

func unstampedRunContext(st manifest.Store, events *[]string) *initRunContext {
	runCtx := deltaRunContext(st, []string{})
	runCtx.manifestResolver = manifest.PathResolver{PathTemplate: collidingOutsideProbesTemplate}
	runCtx.deleteObject = recordingDelete(events)
	return runCtx
}

// The pre-flight for schema 4 reaches schema 3's zero-stamped manifest
// through the shared path and refuses it, named, in both flag modes.
func TestPreflightDeltaTier_RefusesUnstampedManifestWithEntries(t *testing.T) {
	require.NoError(t, forma.ValidateManifestPathTemplate("manifest-template", collidingOutsideProbesTemplate),
		"the two-probe check cannot see a collision at schema IDs 3 and 4")

	for _, flag := range []bool{false, true} {
		var events []string
		st := &recordingStore{events: &events}
		seedUnstampedSharedManifest(t, st, manifest.FileEntry{Tier: "base", Path: "base/3/a_b.parquet"})
		events = nil
		runCtx := unstampedRunContext(st, &events)
		runCtx.replaceDelta = flag

		_, err := preflightDeltaTier(context.Background(), runCtx, 4)
		require.ErrorIs(t, err, forma.ErrManifestUnstamped, "replaceDelta=%t", flag)
		require.Contains(t, err.Error(), "manifest/shared.json")
		require.Contains(t, err.Error(), "1 entries listed under schema_id 0")
		require.Empty(t, events, "replaceDelta=%t: nothing saved or deleted", flag)
	}
}

// The publish for schema 4 — base-only and --replace-delta — refuses the
// same manifest: zero saves, zero deletes, the stored bytes untouched, so
// neither schema 3's base nor its delta is replaced or purged.
func TestUpdateSchemaManifest_RefusesUnstampedManifestWithEntries(t *testing.T) {
	for _, flag := range []bool{false, true} {
		var events []string
		st := &recordingStore{events: &events}
		seedUnstampedSharedManifest(t, st,
			manifest.FileEntry{Tier: "base", Path: "base/3/a_b.parquet"},
			manifest.FileEntry{Tier: "delta", Path: "delta/3/0b2f1b6e-3333-4c1d-8e57-000000000003.parquet"})
		before := append([]byte{}, st.data["manifest/shared.json"]...)
		events = nil
		runCtx := unstampedRunContext(st, &events)
		runCtx.replaceDelta = flag
		state := initStateWithOneEntry(4)
		state.deltaPurge = []string{"delta/4/0b2f1b6e-4444-4c1d-8e57-000000000004.parquet"}

		err := publishAndPurge(context.Background(), runCtx, state)
		require.ErrorIs(t, err, forma.ErrManifestUnstamped, "replaceDelta=%t", flag)
		require.Empty(t, events, "replaceDelta=%t: nothing saved or deleted", flag)
		require.Equal(t, before, st.data["manifest/shared.json"], "replaceDelta=%t", flag)
	}
}

// An empty zero-stamped manifest has nothing another schema could own: the
// publish stamps it, so the saved manifest is checked on every later load.
func TestUpdateSchemaManifest_StampsEmptyUnstampedManifest(t *testing.T) {
	st := &memManifestStore{}
	_, err := manifest.Save(context.Background(), st, "manifest/1.json", &manifest.Manifest{Files: []manifest.FileEntry{}}, "")
	require.NoError(t, err)
	runCtx := deltaRunContext(st, nil)

	require.NoError(t, updateSchemaManifest(context.Background(), runCtx, initStateWithOneEntry(1)))

	m, _, err := manifest.Load(context.Background(), st, "manifest/1.json")
	require.NoError(t, err)
	require.Equal(t, int16(1), m.SchemaID)
	require.Len(t, m.Files, 1)
}
