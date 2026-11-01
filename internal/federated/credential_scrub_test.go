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
	"github.com/lychee-technology/forma/internal/sqlgen"
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

// The success path leaked too: recordTranslation stores the rendered SQL
// verbatim on the internal plan that attachExecutionPlan stitches onto the
// returned page, and before #752 that SQL embedded postgres_scan('…password=…').
// The public HTTP projection already omits SQL (types.go SECURITY comment);
// this keeps the credential out for Go embedders, the audience #306 names.
// Since #752 the rendered SQL names the attached catalog by alias, and only
// the executed batch, which begins with the ATTACH, carries the password.
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
	// Positive precondition: the SQL really scans PostgreSQL (the hot tier
	// present in coldTierQuery keeps the pg sections, #184), so the
	// NotContains check cannot pass vacuously.
	conn := "host=h port=5432 user=u password='" + scrubCanary + "' dbname=d"
	require.Contains(t, duckSQL, sqlgen.PostgresCatalogAlias(conn)+`."public"."change_log"`)
	requireNoCanary(t, duckSQL, "internal execution plan rendered SQL")

	// Regression guard: the engine must execute the attach with the
	// unscrubbed connection string ahead of the query. If someone later
	// scrubs it upstream, this test fails while production would attach
	// password=***REDACTED*** and fail at runtime.
	require.True(t, strings.HasPrefix(duck.lastSQL, sqlgen.PostgresAttachSQL(conn)+"\n"),
		"the engine must run the unscrubbed attach ahead of the query; only the plan copy is clean")
	require.True(t, strings.HasSuffix(duck.lastSQL, duckSQL),
		"the executed batch must end with exactly the rendered query the plan records")
}
