package federated

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/redact"
	"github.com/stretchr/testify/require"
)

// #306: the credential DuckDB quotes back on a postgres_scan attach failure
// must be scrubbed before the text enters the engine's error chain — asserted
// on the error string returned from the engine, not on any HTTP body (#301
// already gates that boundary). Canary construction follows
// internal/httpapi/canaries_test.go: the ';' makes truncation regressions
// visible as a surviving tail, so all fragments are asserted separately.
const (
	scrubCanaryHead = "SUPERSECRET"
	scrubCanaryTail = "CANARY-TAIL"
	scrubCanary     = scrubCanaryHead + ";" + scrubCanaryTail
)

// attachFailureErr reproduces the DuckDB attach-failure prose shape (verified
// against duckdb-go in #301): the whole given conn string echoed back, quoted.
func attachFailureErr() error {
	return fmt.Errorf(
		`IO Error: Unable to connect to Postgres at "host=h port=5432 user=u password='%s' dbname=d": connection refused`,
		scrubCanary)
}

func requireNoCanary(t *testing.T, s, surface string) {
	t.Helper()
	for _, frag := range []string{"password='" + scrubCanary + "'", scrubCanary, scrubCanaryHead, scrubCanaryTail} {
		require.NotContains(t, s, frag, "credential fragment leaked into %s", surface)
	}
}

func TestCredentialScrub_ExecuteFailureErrorString(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	duck := &fakeDuckDBExecutor{err: attachFailureErr()}
	engine := newSentinelTestEngine(t, duck, nil)

	_, err := engine.Query(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"},
		coldTierQuery(), nil)

	require.Error(t, err)
	msg := err.Error()
	// Positive preconditions: diagnosis prose and the redaction placeholder
	// must be present, so the NotContains checks below cannot pass vacuously.
	require.Contains(t, msg, "Unable to connect to Postgres")
	require.Contains(t, msg, "password="+redact.Placeholder)
	requireNoCanary(t, msg, "engine error string (execute branch)")
	// Classification must survive the scrub wrapper.
	require.ErrorIs(t, err, ErrFederatedReadFailed)
}

func TestCredentialScrub_StreamFailureErrorString(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	// Lazy attach: DuckDB can surface the connect failure mid-stream instead
	// of at Query (see executeAndStreamDuckDB's stream-branch comment).
	duck := &fakeDuckDBExecutor{rows: &erroringRowsIterator{err: attachFailureErr()}}
	engine := newSentinelTestEngine(t, duck, nil)

	_, err := engine.Query(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"},
		coldTierQuery(), nil)

	require.Error(t, err)
	msg := err.Error()
	require.Contains(t, msg, "iterate duckdb rows")
	require.Contains(t, msg, "password="+redact.Placeholder)
	requireNoCanary(t, msg, "engine error string (stream branch)")
	require.ErrorIs(t, err, ErrFederatedReadFailed)
}

func TestCredentialScrub_ExecutionPlanFailureNotes(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	duck := &fakeDuckDBExecutor{err: attachFailureErr()}
	engine := newSentinelTestEngine(t, duck, nil)

	opts := &model.FederatedQueryOptions{IncludeExecutionPlan: true}
	_, err := engine.Query(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"},
		coldTierQuery(), opts)

	require.Error(t, err)
	require.NotNil(t, opts.ExecutionPlan)
	notes := strings.Join(opts.ExecutionPlan.Notes, "\n")
	require.Contains(t, notes, "duckdb query failed",
		"precondition: the failure note must have been recorded")
	requireNoCanary(t, notes, "internal execution plan notes")
}

// The success path leaks too: recordTranslation stores the rendered SQL —
// which embeds postgres_scan('…password=…') — verbatim on the internal plan
// that attachExecutionPlan stitches onto the returned page. The public HTTP
// projection already omits SQL (types.go SECURITY comment); this closes the
// same credential for Go embedders, the audience #306 names.
func TestCredentialScrub_PlanRenderedSQL(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	duck := &fakeDuckDBExecutor{rows: &singleDuckDBRow{rowID: uuid.New()}}
	engine := NewDBFederatedQueryEngine(&fakePostgresFederatedSource{}, &fakeDirtyIDFetcher{}, duck, nil,
		forma.DuckDBConfig{Enabled: true, Routing: forma.RoutingPolicy{Strategy: forma.RoutingStrategyHybrid}},
		testMetadataCacheSchema7(t),
		"host=h port=5432 user=u password='"+scrubCanary+"' dbname=d",
		withTestParquetPath())

	opts := &model.FederatedQueryOptions{IncludeExecutionPlan: true}
	_, err := engine.Query(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"},
		coldTierQuery(), opts)
	require.NoError(t, err)
	require.NotNil(t, opts.ExecutionPlan)

	var duckSQL string
	for _, src := range opts.ExecutionPlan.Sources {
		if src.Engine == "duckdb" && src.SQL != "" {
			duckSQL = src.SQL
		}
	}
	require.NotEmpty(t, duckSQL, "plan must record the rendered duckdb SQL")
	// Positive precondition: the DSN really rendered into this SQL (hot tier
	// present in coldTierQuery keeps the postgres_scan sections, #184), so the
	// NotContains checks cannot pass vacuously.
	require.Contains(t, duckSQL, "postgres_scan")
	require.Contains(t, duckSQL, "password="+redact.Placeholder)
	requireNoCanary(t, duckSQL, "internal execution plan rendered SQL")

	// Regression guard: the engine must execute the unscrubbed SQL. If someone
	// later moves the scrub upstream (into the query builder or plan-cache
	// compile), this test fails while production would emit
	// postgres_scan('…password=***REDACTED***…') and fail at runtime.
	require.Contains(t, duck.lastSQL, scrubCanary,
		"the engine must execute the unscrubbed SQL; only the plan copy is scrubbed")
}
