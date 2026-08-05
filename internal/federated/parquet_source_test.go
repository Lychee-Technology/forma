package federated

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

func hybridDuckConfig() forma.DuckDBConfig {
	return forma.DuckDBConfig{Enabled: true, Routing: forma.RoutingPolicy{Strategy: forma.RoutingStrategyHybrid}}
}

type fakeParquetSource struct {
	paths        []string
	stamps       map[string]map[string]string
	pathsErr     error
	missing      []string
	missingErr   error
	pathsCalls   int
	missingCalls int
	lastScanned  []string
}

func (f *fakeParquetSource) Paths(ctx context.Context, schemaID int16) ([]string, map[string]map[string]string, error) {
	f.pathsCalls++
	return f.paths, f.stamps, f.pathsErr
}

func (f *fakeParquetSource) MissingIn(ctx context.Context, scanned []string) ([]string, error) {
	f.missingCalls++
	f.lastScanned = scanned
	return f.missing, f.missingErr
}

func newParquetSourceTestEngine(t *testing.T, duck DuckDBQueryExecutor, src ParquetSource) *DBFederatedQueryEngine {
	t.Helper()
	return NewDBFederatedQueryEngine(&fakePostgresFederatedSource{}, &fakeDirtyIDFetcher{}, duck, nil,
		hybridDuckConfig(), testMetadataCacheSchema7(t), "host=x", WithParquetSource(src))
}

// TestParquetSource_PathsRenderIntoScan pins #187: with no render hint, the
// manifest source authors the path set and the rendered SQL scans exactly
// the listed objects (an explicit list — a missing one errors instead of
// silently vanishing from a glob expansion).
func TestParquetSource_PathsRenderIntoScan(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	src := &fakeParquetSource{paths: []string{"s3://b/1/a.parquet", "s3://b/1/b.parquet"}}
	duck := &fakeDuckDBExecutor{rows: &singleDuckDBRow{rowID: uuid.New()}}
	engine := newParquetSourceTestEngine(t, duck, src)

	_, err := engine.Query(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"},
		coldTierQuery(), nil)

	require.NoError(t, err)
	require.Equal(t, 1, src.pathsCalls)
	require.Contains(t, duck.lastSQL, "['s3://b/1/a.parquet', 's3://b/1/b.parquet']")
}

// TestParquetSource_HintWinsOverSource pins the precedence contract: an
// explicit caller-supplied S3ParquetPathTemplate directs the scan (#184) and
// the source is not consulted.
func TestParquetSource_HintWinsOverSource(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	src := &fakeParquetSource{paths: []string{"s3://b/1/from-manifest.parquet"}}
	duck := &fakeDuckDBExecutor{rows: &singleDuckDBRow{rowID: uuid.New()}}
	engine := newParquetSourceTestEngine(t, duck, src)

	q := coldTierQuery()
	q.DuckDBHints = &model.DuckDBRenderHints{S3ParquetPathTemplate: "s3://b/override/{{.SchemaID}}/*.parquet"}
	_, err := engine.Query(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"},
		q, nil)

	require.NoError(t, err)
	require.Equal(t, 0, src.pathsCalls, "explicit hint must bypass the parquet source")
	require.Contains(t, duck.lastSQL, "s3://b/override/7/*.parquet")
}

// TestParquetSource_MissingKeysClassifyInconsistent pins the #187 scenario-2
// classification and its non-degradability: a failed scan over
// source-authored paths with listed keys missing from storage is
// ErrParquetSetInconsistent, surfaced even under AllowPartialDegradedMode.
func TestParquetSource_MissingKeysClassifyInconsistent(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	src := &fakeParquetSource{
		paths:   []string{"s3://b/1/gone.parquet"},
		missing: []string{"1/gone.parquet"},
	}
	duck := &fakeDuckDBExecutor{err: fmt.Errorf("IO Error: no files found")}
	pg := &fakePostgresFederatedSource{page: &model.PersistentRecordPage{TotalRecords: 1}}
	engine := NewDBFederatedQueryEngine(pg, &fakeDirtyIDFetcher{}, duck, nil,
		hybridDuckConfig(), testMetadataCacheSchema7(t), "host=x", WithParquetSource(src))

	_, err := engine.Query(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"},
		coldTierQuery(), &model.FederatedQueryOptions{AllowPartialDegradedMode: true})

	require.ErrorIs(t, err, ErrParquetSetInconsistent)
	var typed *ParquetSetInconsistentError
	require.True(t, errors.As(err, &typed))
	require.Equal(t, []string{"1/gone.parquet"}, typed.MissingKeys)
	require.Equal(t, int16(7), typed.SchemaID)
	require.Equal(t, 0, pg.queryCalls, "manifest inconsistency must not degrade to a Postgres-only partial result")
	require.Equal(t, src.paths, src.lastScanned,
		"classification must probe the exact scanned set, not a re-resolved manifest snapshot")
}

// TestParquetSource_InvalidHintTemplateErrs pins the #249 review P1: an
// explicit template that cannot render is invalid input — it must fail the
// query (non-degradable) instead of silently falling through to manifest
// paths, which would serve the caller a different dataset than requested.
func TestParquetSource_InvalidHintTemplateErrs(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	src := &fakeParquetSource{paths: []string{"s3://b/1/from-manifest.parquet"}}
	duck := &fakeDuckDBExecutor{rows: &singleDuckDBRow{rowID: uuid.New()}}
	pg := &fakePostgresFederatedSource{page: &model.PersistentRecordPage{TotalRecords: 1}}
	engine := NewDBFederatedQueryEngine(pg, &fakeDirtyIDFetcher{}, duck, nil,
		hybridDuckConfig(), testMetadataCacheSchema7(t), "host=x", WithParquetSource(src))

	q := coldTierQuery()
	q.DuckDBHints = &model.DuckDBRenderHints{S3ParquetPathTemplate: "s3://b/{{.Broken"}
	_, err := engine.Query(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"},
		q, &model.FederatedQueryOptions{AllowPartialDegradedMode: true})

	require.ErrorIs(t, err, forma.ErrInvalidInput)
	require.Equal(t, 0, src.pathsCalls, "a broken hint must not fall through to the parquet source")
	require.Equal(t, 0, duck.calls, "a broken hint must fail before execution")
	require.Equal(t, 0, pg.queryCalls, "invalid input must not be absorbed by degraded mode")
}

// TestParquetSource_EmptyHintErrs pins the other half of the precedence
// contract (#250 PR review): a hint that renders successfully but yields no
// usable path — "," or whitespace-only segments — is still an explicit hint.
// Treating its empty result as "no hint" would fall through to the manifest
// source and answer the query from a different path set than requested, the
// exact silent provenance switch the invalid-template rule exists to prevent.
func TestParquetSource_EmptyHintErrs(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	src := &fakeParquetSource{paths: []string{"s3://b/1/from-manifest.parquet"}}
	duck := &fakeDuckDBExecutor{rows: &singleDuckDBRow{rowID: uuid.New()}}
	pg := &fakePostgresFederatedSource{page: &model.PersistentRecordPage{TotalRecords: 1}}
	engine := NewDBFederatedQueryEngine(pg, &fakeDirtyIDFetcher{}, duck, nil,
		hybridDuckConfig(), testMetadataCacheSchema7(t), "host=x", WithParquetSource(src))

	q := coldTierQuery()
	q.DuckDBHints = &model.DuckDBRenderHints{S3ParquetPathTemplate: " , "}
	_, err := engine.Query(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"},
		q, &model.FederatedQueryOptions{AllowPartialDegradedMode: true})

	require.ErrorIs(t, err, forma.ErrInvalidInput)
	require.Equal(t, 0, src.pathsCalls, "a degenerate hint must not fall through to the parquet source")
	require.Equal(t, 0, duck.calls, "a degenerate hint must fail before execution")
	require.Equal(t, 0, pg.queryCalls, "invalid input must not be absorbed by degraded mode")
}

// TestDuckDBParquetPathsForQuery_DegenerateHint covers the resolver directly:
// absent hint stays (nil, nil); a present hint that renders to zero usable
// paths is invalid input.
func TestDuckDBParquetPathsForQuery_DegenerateHint(t *testing.T) {
	noHint, err := duckDBParquetPathsForQuery(&model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 7},
	})
	require.NoError(t, err)
	require.Nil(t, noHint)

	for _, tmpl := range []string{",", " , ", "   "} {
		q := &model.FederatedAttributeQuery{
			AttributeQuery: model.AttributeQuery{SchemaID: 7},
			DuckDBHints:    &model.DuckDBRenderHints{S3ParquetPathTemplate: tmpl},
		}
		paths, err := duckDBParquetPathsForQuery(q)
		require.Nil(t, paths)
		require.ErrorIs(t, err, forma.ErrInvalidInput, "template %q", tmpl)
		require.ErrorContains(t, err, tmpl)
	}
}

// TestParquetSource_SourceResolvesZeroPathsIsNoParquetPaths pins #299: a
// configured source whose manifest lists nothing and whose fallback glob is
// disabled resolves an empty path set. That used to render
// read_parquet(<no value>) and classify as the degradable
// ErrFederatedReadFailed — indistinguishable from a transient S3 outage, so
// degraded mode silently answered a misconfiguration from Postgres alone.
// It must now carry its own classification and survive degraded mode.
func TestParquetSource_SourceResolvesZeroPathsIsNoParquetPaths(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	src := &fakeParquetSource{paths: nil}
	duck := &fakeDuckDBExecutor{rows: &singleDuckDBRow{rowID: uuid.New()}}
	pg := &fakePostgresFederatedSource{page: &model.PersistentRecordPage{TotalRecords: 3}}
	engine := NewDBFederatedQueryEngine(pg, &fakeDirtyIDFetcher{}, duck, nil,
		hybridDuckConfig(), testMetadataCacheSchema7(t), "host=x", WithParquetSource(src))

	_, err := engine.Query(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"},
		coldTierQuery(), &model.FederatedQueryOptions{AllowPartialDegradedMode: true})

	require.ErrorIs(t, err, ErrNoParquetPaths)
	// The whole point of #299: not confusable with a transient read failure.
	require.NotErrorIs(t, err, ErrFederatedReadFailed)

	var typed *NoParquetPathsError
	require.True(t, errors.As(err, &typed))
	require.Equal(t, int16(7), typed.SchemaID)
	require.True(t, typed.SourceConfigured)

	require.Equal(t, 0, duck.calls, "an empty path set must fail before DuckDB executes")
	require.Equal(t, 0, pg.queryCalls,
		"a misconfigured read surface must not be absorbed into a Postgres-only answer")
}

// TestParquetSource_NoSourceNoHintIsNoParquetPaths covers the other zero-path
// state (#299): manifest reads off and the caller supplied no render hint, so
// nothing ever authors a path set. Same classification, different remedy —
// the message must distinguish them.
func TestParquetSource_NoSourceNoHintIsNoParquetPaths(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	duck := &fakeDuckDBExecutor{rows: &singleDuckDBRow{rowID: uuid.New()}}
	pg := &fakePostgresFederatedSource{page: &model.PersistentRecordPage{TotalRecords: 3}}
	engine := NewDBFederatedQueryEngine(pg, &fakeDirtyIDFetcher{}, duck, nil,
		hybridDuckConfig(), testMetadataCacheSchema7(t), "host=x")

	_, err := engine.Query(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"},
		coldTierQuery(), &model.FederatedQueryOptions{AllowPartialDegradedMode: true})

	require.ErrorIs(t, err, ErrNoParquetPaths)
	require.NotErrorIs(t, err, ErrFederatedReadFailed)

	var typed *NoParquetPathsError
	require.True(t, errors.As(err, &typed))
	require.False(t, typed.SourceConfigured,
		"no source was consulted; the remedy is to configure one, not to repair a manifest")
	require.Equal(t, 0, duck.calls)
	require.Equal(t, 0, pg.queryCalls)
}

// TestParquetSource_ManifestSchemaMismatchIsNotDegradable pins the review's
// cross-schema finding at the engine seam: a manifest stamped for another
// schema is a misaddressed read surface, not a transient outage. Degrading it
// to Postgres-only would hide a state that can serve another schema's rows
// under this schema's identity, so it must surface — and it must not be
// relabelled as a plain read failure on the way out.
func TestParquetSource_ManifestSchemaMismatchIsNotDegradable(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	src := &fakeParquetSource{pathsErr: &forma.ManifestSchemaMismatchError{
		RequestedSchemaID: 7,
		ManifestSchemaID:  9,
		Path:              "manifest/7.json",
	}}
	duck := &fakeDuckDBExecutor{rows: &singleDuckDBRow{rowID: uuid.New()}}
	pg := &fakePostgresFederatedSource{page: &model.PersistentRecordPage{TotalRecords: 3}}
	engine := NewDBFederatedQueryEngine(pg, &fakeDirtyIDFetcher{}, duck, nil,
		hybridDuckConfig(), testMetadataCacheSchema7(t), "host=x", WithParquetSource(src))

	_, err := engine.Query(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"},
		coldTierQuery(), &model.FederatedQueryOptions{AllowPartialDegradedMode: true})

	require.ErrorIs(t, err, forma.ErrManifestSchemaMismatch)
	require.NotErrorIs(t, err, ErrFederatedReadFailed,
		"a misaddressed manifest is a consistency error, not transient infrastructure")
	require.Equal(t, 0, pg.queryCalls, "degraded mode must not absorb cross-schema contamination")
	require.Equal(t, 0, duck.calls)
}

// TestParquetSource_ZeroPathsNeverRendersNoValue is the regression pin for the
// literal symptom in #299: whatever else changes, the SQL handed to DuckDB
// must never contain Go's unbound-parameter rendering.
func TestParquetSource_ZeroPathsNeverRendersNoValue(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	duck := &fakeDuckDBExecutor{rows: &singleDuckDBRow{rowID: uuid.New()}}
	engine := newParquetSourceTestEngine(t, duck, &fakeParquetSource{paths: nil})

	_, err := engine.Query(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"},
		coldTierQuery(), nil)

	require.Error(t, err)
	require.NotContains(t, duck.lastSQL, "<no value>")
	require.NotContains(t, err.Error(), "<no value>")
}

// TestSentinel_DirtyFetchFailureIsPostgresReadFailed pins the #249 review P1
// for scenario 9: the Postgres side of a federated read (here the dirty-ID
// consistency fetch) classifies as ErrPostgresReadFailed.
func TestSentinel_DirtyFetchFailureIsPostgresReadFailed(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	dirty := &fakeDirtyIDFetcher{err: fmt.Errorf("connection refused")}
	duck := &fakeDuckDBExecutor{rows: &singleDuckDBRow{rowID: uuid.New()}}
	// A resolvable path set is required to reach the dirty fetch: path
	// resolution runs first and, since #299, fails an empty set outright.
	engine := NewDBFederatedQueryEngine(&fakePostgresFederatedSource{}, dirty, duck, nil,
		hybridDuckConfig(), testMetadataCacheSchema7(t), "host=x", withTestParquetPath())

	_, err := engine.Query(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"},
		coldTierQuery(), nil)

	require.ErrorIs(t, err, ErrPostgresReadFailed)
	require.NotErrorIs(t, err, ErrFederatedReadFailed)
	require.Equal(t, 0, duck.calls, "dirty fetch fails before DuckDB executes")
}

// TestParquetSource_NoMissingKeysStaysDegradableReadFailure pins the other
// half: a failed scan whose listed objects all exist (corrupt bytes, bad
// credentials) stays ErrFederatedReadFailed and degrades normally.
func TestParquetSource_NoMissingKeysStaysDegradableReadFailure(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	src := &fakeParquetSource{paths: []string{"s3://b/1/corrupt.parquet"}}
	duck := &fakeDuckDBExecutor{err: fmt.Errorf("Invalid Input: corrupt page")}
	pg := &fakePostgresFederatedSource{page: &model.PersistentRecordPage{TotalRecords: 2}}
	engine := NewDBFederatedQueryEngine(pg, &fakeDirtyIDFetcher{}, duck, nil,
		hybridDuckConfig(), testMetadataCacheSchema7(t), "host=x", WithParquetSource(src))

	// Degraded OFF: classified read failure.
	_, err := engine.Query(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"},
		coldTierQuery(), nil)
	require.ErrorIs(t, err, ErrFederatedReadFailed)
	require.NotErrorIs(t, err, ErrParquetSetInconsistent)

	// Degraded ON: falls back to Postgres-only.
	page, err := engine.Query(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"},
		coldTierQuery(), &model.FederatedQueryOptions{AllowPartialDegradedMode: true})
	require.NoError(t, err)
	require.Equal(t, int64(2), page.TotalRecords)
	require.Equal(t, 1, pg.queryCalls)
}

// TestParquetSource_ProbeErrorStaysReadFailed: an unreachable store cannot
// prove loss, so a MissingKeys probe failure keeps the plain classification.
func TestParquetSource_ProbeErrorStaysReadFailed(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	src := &fakeParquetSource{
		paths:      []string{"s3://b/1/a.parquet"},
		missingErr: fmt.Errorf("connection refused"),
	}
	duck := &fakeDuckDBExecutor{err: fmt.Errorf("IO Error: connection refused")}
	engine := newParquetSourceTestEngine(t, duck, src)

	_, err := engine.Query(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"},
		coldTierQuery(), nil)

	require.ErrorIs(t, err, ErrFederatedReadFailed)
	require.NotErrorIs(t, err, ErrParquetSetInconsistent)
	require.Equal(t, 1, src.missingCalls)
}

// TestParquetSource_PathsErrorClassifiedReadFailed: a manifest resolution
// failure is a federated read failure (degradable transient infrastructure).
func TestParquetSource_PathsErrorClassifiedReadFailed(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	src := &fakeParquetSource{pathsErr: fmt.Errorf("s3 down")}
	duck := &fakeDuckDBExecutor{rows: &singleDuckDBRow{rowID: uuid.New()}}
	engine := newParquetSourceTestEngine(t, duck, src)

	_, err := engine.Query(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"},
		coldTierQuery(), nil)

	require.ErrorIs(t, err, ErrFederatedReadFailed)
	require.ErrorContains(t, err, "resolve parquet paths")
	require.Equal(t, 0, duck.calls, "resolution failure must precede execution")
}

// newExclusionTestEngine builds the minimal engine the resolve-time exclusion
// tests need: only the parquet source and the corrupt-path cache participate,
// so every other seam stays nil.
func newExclusionTestEngine(src ParquetSource) *DBFederatedQueryEngine {
	return NewDBFederatedQueryEngine(nil, nil, nil, nil, forma.DuckDBConfig{}, nil, "", WithParquetSource(src))
}

func exclusionTestQuery() *model.FederatedAttributeQuery {
	return &model.FederatedAttributeQuery{AttributeQuery: model.AttributeQuery{SchemaID: 7}}
}

// TestResolveParquetPathsExcludesCachedCorrupt pins the #251 resolve-time
// exclusion: a verification-confirmed corrupt object drops out of a
// source-authored path set and is reported for the execution-plan note.
func TestResolveParquetPathsExcludesCachedCorrupt(t *testing.T) {
	e := newExclusionTestEngine(&fakeParquetSource{paths: []string{"s3://b/a.parquet", "s3://b/bad.parquet"}})
	e.corruptPaths.Add([]string{"s3://b/bad.parquet"})

	paths, fromSource, _, excluded, err := e.resolveParquetPaths(context.Background(), exclusionTestQuery())
	require.NoError(t, err)
	require.True(t, fromSource, "source-authored set must report fromSource")
	require.Equal(t, []string{"s3://b/a.parquet"}, paths)
	require.Equal(t, []string{"s3://b/bad.parquet"}, excluded)
}

// TestResolveParquetPathsAllCorruptKeepsFullSet: excluding everything would
// turn total corruption into a quiet ErrNoParquetPaths misconfiguration; the
// full set must scan and fail loudly with today's classification instead.
func TestResolveParquetPathsAllCorruptKeepsFullSet(t *testing.T) {
	stamps := map[string]map[string]string{
		"s3://b/bad.parquet": {"row_id": "UUID"},
	}
	e := newExclusionTestEngine(&fakeParquetSource{paths: []string{"s3://b/bad.parquet"}, stamps: stamps})
	e.corruptPaths.Add([]string{"s3://b/bad.parquet"})

	paths, _, gotStamps, excluded, err := e.resolveParquetPaths(context.Background(), exclusionTestQuery())
	require.NoError(t, err)
	require.Equal(t, []string{"s3://b/bad.parquet"}, paths,
		"all-corrupt set must pass through unfiltered")
	require.Equal(t, stamps, gotStamps,
		"the full-set fallback must still forward the source's stamps")
	require.Empty(t, excluded, "an unfiltered scan must not claim a partial read")
}

// TestResolveParquetPathsHintSetNeverFiltered: hint sets are operator-pinned
// (#184) — the caller named the objects, so exclusion must not silently narrow
// them.
func TestResolveParquetPathsHintSetNeverFiltered(t *testing.T) {
	e := newExclusionTestEngine(&fakeParquetSource{paths: []string{"s3://b/a.parquet"}})
	e.corruptPaths.Add([]string{"s3://hinted/x.parquet"})
	q := exclusionTestQuery()
	q.DuckDBHints = &model.DuckDBRenderHints{S3ParquetPathTemplate: "s3://hinted/x.parquet"}

	paths, fromSource, _, excluded, err := e.resolveParquetPaths(context.Background(), q)
	require.NoError(t, err)
	require.False(t, fromSource)
	require.Empty(t, excluded)
	require.Equal(t, []string{"s3://hinted/x.parquet"}, paths)
}

// TestRecordCorruptExclusionNotesExcludedObjects pins the loudness half: the
// internal plan names every excluded object under the exported note prefix the
// e2e harness and Go embedders assert on.
func TestRecordCorruptExclusionNotesExcludedObjects(t *testing.T) {
	opts := &model.FederatedQueryOptions{IncludeExecutionPlan: true}
	planCtx := newDuckDBExecutionPlanContext(opts)

	planCtx.recordCorruptExclusion([]string{"s3://b/bad.parquet"})

	require.Contains(t, opts.ExecutionPlan.Notes[len(opts.ExecutionPlan.Notes)-1], NotePartialParquetExclusion)
	require.Contains(t, opts.ExecutionPlan.Notes[len(opts.ExecutionPlan.Notes)-1], "s3://b/bad.parquet")
}

// TestRecordCorruptExclusionEmptyIsNoop: a full scan must not leave a partial
// read note behind.
func TestRecordCorruptExclusionEmptyIsNoop(t *testing.T) {
	opts := &model.FederatedQueryOptions{IncludeExecutionPlan: true}
	planCtx := newDuckDBExecutionPlanContext(opts)
	before := len(opts.ExecutionPlan.Notes)

	planCtx.recordCorruptExclusion(nil)

	require.Len(t, opts.ExecutionPlan.Notes, before)
}

// The broken-hint 400 must publish the caller's own template and the reason,
// while text/template's parse prose stays operator-only (#313). This is the
// issue's motivating case: before typed publications this mixed chain
// collapsed to an opaque redacted body.
func TestParquetSource_InvalidHintPublishesTemplateWithoutRenderInternals(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	src := &fakeParquetSource{paths: []string{"s3://b/1/from-manifest.parquet"}}
	duck := &fakeDuckDBExecutor{rows: &singleDuckDBRow{rowID: uuid.New()}}
	pg := &fakePostgresFederatedSource{page: &model.PersistentRecordPage{TotalRecords: 1}}
	engine := NewDBFederatedQueryEngine(pg, &fakeDirtyIDFetcher{}, duck, nil,
		hybridDuckConfig(), testMetadataCacheSchema7(t), "host=x", WithParquetSource(src))

	q := coldTierQuery()
	q.DuckDBHints = &model.DuckDBRenderHints{S3ParquetPathTemplate: "s3://b/{{.Broken"}
	_, err := engine.Query(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"},
		q, &model.FederatedQueryOptions{AllowPartialDegradedMode: true})
	require.Error(t, err)

	var pub forma.PublicError
	require.True(t, errors.As(err, &pub), "broken hint publishes no client message: %v", err)
	require.Contains(t, pub.PublicMessage(), `"s3://b/{{.Broken"`)
	require.Contains(t, pub.PublicMessage(), "not renderable")
	require.NotContains(t, pub.PublicMessage(), "unclosed action",
		"text/template internals must stay operator-only")
	require.Contains(t, err.Error(), "unclosed action",
		"the operator copy must keep the render error")
	require.True(t, forma.HasOperatorDetail(err))
}
