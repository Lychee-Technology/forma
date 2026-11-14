package federated

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/redact"
	"github.com/stretchr/testify/require"
)

// #306: a credential DuckDB quotes back in an error must be scrubbed before
// the text enters the engine's error chain — asserted on the error string
// returned from the engine, not on any HTTP body (#301 already gates that
// boundary). The canary's shape is the attach failure that motivated #306;
// the engine no longer attaches PostgreSQL (#858), but the parquet leg still
// scrubs whatever DuckDB says. Canary construction follows
// internal/httpapi/canaries_test.go: the ';' makes truncation regressions
// visible as a surviving tail, so all fragments are asserted separately.
const (
	scrubCanaryHead = "SUPERSECRET"
	scrubCanaryTail = "CANARY-TAIL"
	scrubCanary     = scrubCanaryHead + ";" + scrubCanaryTail
)

// attachFailureErr reproduces the DuckDB attach-failure prose shape (verified
// against duckdb-go in #301): a whole conn string echoed back, quoted.
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

	// DuckDB can surface a failure mid-stream instead of at Query.
	duck := &fakeDuckDBExecutor{rows: &erroringRowsIterator{err: attachFailureErr()}}
	engine := newSentinelTestEngine(t, duck, nil)

	_, err := engine.Query(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"},
		coldTierQuery(), nil)

	require.Error(t, err)
	msg := err.Error()
	require.Contains(t, msg, "iterate parquet leg rows")
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
