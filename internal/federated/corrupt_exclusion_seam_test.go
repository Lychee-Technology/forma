package federated

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/queryplan"
	"github.com/stretchr/testify/require"
)

// The two objects the seam tests resolve; only the second is ever poisoned, so
// every assertion below can distinguish "kept" from "excluded" by substring.
const (
	exclusionKeptPath     = "s3://b/7/a.parquet"
	exclusionCorruptPath  = "s3://b/7/bad.parquet"
	exclusionCorruptToken = "bad.parquet"
)

// runExclusionQuery drives one full engine request over the DuckDB path and
// returns the SQL actually handed to DuckDB plus the execution-plan notes. It
// is the only vantage point that sees BOTH halves of the wiring: the path set
// the scan was rendered from, and the note the Stream path recorded.
func runExclusionQuery(t *testing.T, e *DBFederatedQueryEngine, duck *fakeDuckDBExecutor) (string, []string) {
	t.Helper()
	opts := &model.FederatedQueryOptions{IncludeExecutionPlan: true}
	duck.rows = &singleDuckDBRow{rowID: uuid.New()}
	_, err := e.Query(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"},
		coldTierQuery(), opts)
	require.NoError(t, err)
	require.NotNil(t, opts.ExecutionPlan)
	return duck.lastSQL, opts.ExecutionPlan.Notes
}

// requireExclusionNote asserts the plan carries the partial-read note naming
// the excluded object — the exported prefix the production e2e harness and Go
// embedders assert on.
func requireExclusionNote(t *testing.T, notes []string, path string) {
	t.Helper()
	for _, note := range notes {
		if strings.Contains(note, NotePartialParquetExclusion) && strings.Contains(note, path) {
			return
		}
	}
	t.Fatalf("no %q note naming %q; notes = %v", NotePartialParquetExclusion, path, notes)
}

// TestCorruptExclusionReachesScanAndPlanNote is the engine-seam proof that
// joins the two halves the isolated tests leave apart: resolve-time exclusion
// (resolveParquetPaths) and the loudness recorder (recordCorruptExclusion).
// Neither isolated test can see the wiring in between — deleting
// planCtx.recordCorruptExclusion, or threading a PRE-exclusion path list into
// buildDuckDBQueryWithPlan, leaves both of them green. This one drives the real
// Stream path end to end and fails on either mutation: the rendered scan must
// name only the readable object, and the plan must say the scan was partial.
func TestCorruptExclusionReachesScanAndPlanNote(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	src := &fakeParquetSource{paths: []string{exclusionKeptPath, exclusionCorruptPath}}
	duck := &fakeDuckDBExecutor{}
	e := newParquetSourceTestEngine(t, duck, src)
	e.corruptPaths.Add([]string{exclusionCorruptPath})

	sql, notes := runExclusionQuery(t, e, duck)

	require.Contains(t, sql, exclusionKeptPath,
		"the readable object must still be scanned: exclusion narrows the set, it does not abandon it")
	require.NotContains(t, sql, exclusionCorruptToken,
		"the confirmed-corrupt object must not reach read_parquet — the kept set is what renders")
	requireExclusionNote(t, notes, exclusionCorruptPath)
}

// TestCorruptExclusionRekeysPlanCache is the scope-hash leg, mirroring
// TestEngineColdMissingSetRekeysPlanCache (#255) at the hazard #251 introduces:
// a real plan cache serving repeated requests of the SAME shape. Run 1 compiles
// over both objects and run 2 is a genuine HIT — that reuse is what makes the
// hazard real. Run 3 changes nothing about the query, only the corrupt-path
// cache; because the POST-exclusion path set feeds the scope key, it must MISS
// and recompile without the excluded object. Key the scope on the pre-exclusion
// set instead and run 3 becomes a hit that keeps scanning the corrupt file
// forever.
func TestCorruptExclusionRekeysPlanCache(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	duck := &fakeDuckDBExecutor{}
	e := NewDBFederatedQueryEngine(&fakePostgresFederatedSource{}, &fakeDirtyIDFetcher{}, duck, nil,
		hybridDuckConfig(), testMetadataCacheSchema7(t), "host=x",
		WithPlanCache(queryplan.NewCache(64)),
		WithParquetSource(&fakeParquetSource{paths: []string{exclusionKeptPath, exclusionCorruptPath}}))

	sql1, notes1 := runExclusionQuery(t, e, duck)
	require.Contains(t, notes1, "plan_cache=miss", "first request compiles")
	require.Contains(t, sql1, exclusionCorruptToken, "nothing is excluded yet: both objects scan")

	sql2, notes2 := runExclusionQuery(t, e, duck)
	require.Contains(t, notes2, "plan_cache=hit",
		"same shape, same path set: the skeleton must be reused — this is the reuse that could poison")
	require.Contains(t, sql2, exclusionCorruptToken)

	// Verification confirms the object corrupt (what Task 6's drain does).
	e.corruptPaths.Add([]string{exclusionCorruptPath})

	sql3, notes3 := runExclusionQuery(t, e, duck)
	require.Contains(t, notes3, "plan_cache=miss",
		"the post-exclusion path set must re-key the plan cache, or the cached skeleton keeps scanning the corrupt object")
	require.NotContains(t, sql3, exclusionCorruptToken,
		"the recompiled plan must scan only the readable remainder")
	require.Contains(t, sql3, exclusionKeptPath)
	requireExclusionNote(t, notes3, exclusionCorruptPath)
}

// TestCorruptExclusionNoteAbsentOnFullScan is the negative leg of loudness: a
// scan that excluded nothing must not claim a partial read, so a plan reader
// (or the e2e harness) can trust the note's presence as evidence.
func TestCorruptExclusionNoteAbsentOnFullScan(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	duck := &fakeDuckDBExecutor{}
	e := newParquetSourceTestEngine(t, duck,
		&fakeParquetSource{paths: []string{exclusionKeptPath, exclusionCorruptPath}})

	sql, notes := runExclusionQuery(t, e, duck)

	require.Contains(t, sql, exclusionCorruptToken, "nothing was confirmed corrupt: the full set scans")
	for _, note := range notes {
		require.NotContains(t, note, NotePartialParquetExclusion,
			"a full scan must not be reported as partial")
	}
}
