package federated

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/redact"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// The degraded fallback's log line (#638).

func withObservedLogs(engine *DBFederatedQueryEngine) *observer.ObservedLogs {
	core, logs := observer.New(zapcore.DebugLevel)
	WithLogger(zap.New(core))(engine)
	return logs
}

func degradedLogTables() model.StorageTables {
	return model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"}
}

// TestDegradedFallbackLogsTheAbsorbedFailure runs every point at which a
// DuckDB-routed request can degrade. Each fallback writes exactly one line
// that names the schema and carries the absorbed failure. The line is at
// Warn, or at Debug for a breaker rejection, whose cause the trip line
// already logged. No execution plan is requested: the plan note is the only
// other record of the cause, and the line must not depend on it.
func TestDegradedFallbackLogsTheAbsorbedFailure(t *testing.T) {
	for _, tc := range degradedPlanCases() {
		t.Run(tc.name, func(t *testing.T) {
			restore := initTestDescriptors()
			defer restore()

			engine, fq := tc.setup(t)
			logs := withObservedLogs(engine)
			opts := &model.FederatedQueryOptions{AllowPartialDegradedMode: true}
			_, err := engine.Query(context.Background(), degradedLogTables(), fq, opts)
			require.NoError(t, err, "degraded mode must absorb the failure")

			entries := logs.FilterMessage(degradedFallbackMsg).All()
			require.Len(t, entries, 1, "one line per degraded answer; all lines: %v", logs.All())
			want := zapcore.WarnLevel
			if tc.breakerRejected {
				want = zapcore.DebugLevel
			}
			require.Equal(t, want, entries[0].Level)
			fields := entries[0].ContextMap()
			require.Equal(t, fq.SchemaID, fields["schema_id"])
			require.Contains(t, fields["error"], tc.cause)
		})
	}
}

// TestDegradedFallbackLogAcrossABreakerTrip places the fallback line beside
// the breaker's transition lines (#634). The failure that trips the breaker
// leaves the Warn "opened" line and a Warn fallback line, both carrying that
// failure. A request the open breaker then rejects leaves only a Debug
// fallback line, so at the production threshold (Info) the trip is logged
// once, not once per rejected request.
func TestDegradedFallbackLogAcrossABreakerTrip(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	duck := &scriptedDuckDBExecutor{script: []func() (duckDBRowsIterator, error){
		func() (duckDBRowsIterator, error) { return nil, fmt.Errorf("forced execution failure") },
	}}
	engine := newBreakerTestEngine(t, duck, NewCircuitBreaker(1, time.Minute, time.Minute))
	logs := withObservedLogs(engine)
	fq, tables := breakerTestQuery()
	opts := &model.FederatedQueryOptions{AllowPartialDegradedMode: true}

	type line struct {
		level zapcore.Level
		msg   string
		cause string
	}
	steps := []struct {
		name  string
		lines []line
	}{
		{name: "failure trips the breaker", lines: []line{
			{zapcore.WarnLevel, "duckdb circuit breaker opened", "forced execution failure"},
			{zapcore.WarnLevel, degradedFallbackMsg, "forced execution failure"},
		}},
		{name: "open breaker rejects", lines: []line{
			{zapcore.DebugLevel, degradedFallbackMsg, "duckdb circuit breaker open"},
		}},
	}
	for _, step := range steps {
		_, err := engine.Query(context.Background(), tables, fq, opts)
		require.NoError(t, err, step.name)

		entries := logs.TakeAll()
		require.Len(t, entries, len(step.lines), "%s: log lines %v", step.name, entries)
		for i, want := range step.lines {
			require.Equal(t, want.level, entries[i].Level, "%s: line %d", step.name, i)
			require.Equal(t, want.msg, entries[i].Message, "%s: line %d", step.name, i)
			require.Contains(t, entries[i].ContextMap()["error"], want.cause, "%s: line %d", step.name, i)
		}
	}
	require.Equal(t, 1, duck.calls, "the rejected request must not reach DuckDB")
}

// TestDegradedFallbackLogFollowsTheGuardAttribution keeps both lines for an
// absorbed scan-guard violation: the #351 Error line names the offending
// object, and the fallback line then records that the request was answered
// without it.
func TestDegradedFallbackLogFollowsTheGuardAttribution(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	e := newGuardSeamEngine(t, &guardSeamFakeDuck{rogue: guardRoguePath},
		&fakeParquetSource{paths: []string{guardHealthyPath, guardRoguePath}})
	logs := withObservedLogs(e)

	opts := &model.FederatedQueryOptions{AllowPartialDegradedMode: true}
	_, err := e.Query(context.Background(), guardSeamTables(), coldTierQuery(), opts)
	require.NoError(t, err)

	entries := logs.All()
	require.Len(t, entries, 2, "log lines %v", entries)
	require.Equal(t, "parquet scan-guard failure attributed to objects", entries[0].Message)
	require.Equal(t, zapcore.ErrorLevel, entries[0].Level)
	require.Equal(t, degradedFallbackMsg, entries[1].Message)
	require.Equal(t, zapcore.WarnLevel, entries[1].Level)
	require.Contains(t, entries[1].ContextMap()["error"], guardRoguePath)
}

// TestDegradedFallbackLogRedactsTheCause covers a cause that no source scrub
// reaches. #306 scrubs DuckDB's errors where they arise, but the dirty-ID
// fetch is a degradable Postgres failure outside that scrub, so a
// connection-string password in it must be removed from the line.
func TestDegradedFallbackLogRedactsTheCause(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	dirty := &fakeDirtyIDFetcher{err: fmt.Errorf("connect host=pg password=s3cr3t: refused")}
	engine := NewDBFederatedQueryEngine(&fakePostgresFederatedSource{}, dirty, &fakeDuckDBExecutor{}, nil,
		hybridDuckConfig(), testMetadataCacheSchema7(t), "host=x", withTestParquetPath())
	logs := withObservedLogs(engine)

	opts := &model.FederatedQueryOptions{AllowPartialDegradedMode: true}
	_, err := engine.Query(context.Background(), degradedLogTables(), coldTierQuery(), opts)
	require.NoError(t, err)
	require.Equal(t, 1, dirty.calls, "the absorbed failure must be the dirty-ID fetch's")

	entries := logs.FilterMessage(degradedFallbackMsg).All()
	require.Len(t, entries, 1)
	logged := entries[0].ContextMap()["error"]
	require.Contains(t, logged, "password="+redact.Placeholder)
	require.NotContains(t, logged, "s3cr3t")
	require.Contains(t, logged, ErrPostgresReadFailed.Error(), "the rest of the cause survives the scrub")
}

// TestDegradedFallbackLogSurvivesAFailedFallback covers a fallback whose own
// PostgreSQL read fails. The returned error names only that read, so the
// line, written before it, is the one record of the DuckDB failure the
// request was degrading around.
func TestDegradedFallbackLogSurvivesAFailedFallback(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	pg := &fakePostgresFederatedSource{err: fmt.Errorf("forced postgres failure")}
	engine := NewDBFederatedQueryEngine(pg, &fakeDirtyIDFetcher{},
		&fakeDuckDBExecutor{err: fmt.Errorf("forced duck failure")}, nil,
		hybridDuckConfig(), testMetadataCacheSchema7(t), "host=x", withTestParquetPath())
	logs := withObservedLogs(engine)

	opts := &model.FederatedQueryOptions{AllowPartialDegradedMode: true}
	_, err := engine.Query(context.Background(), degradedLogTables(), coldTierQuery(), opts)
	require.ErrorContains(t, err, "degraded postgres-only fallback")
	require.ErrorContains(t, err, "forced postgres failure")
	require.NotContains(t, err.Error(), "forced duck failure", "precondition: the returned error drops the cause")

	entries := logs.FilterMessage(degradedFallbackMsg).All()
	require.Len(t, entries, 1)
	require.Equal(t, zapcore.WarnLevel, entries[0].Level)
	require.Contains(t, entries[0].ContextMap()["error"], "forced duck failure")
}

// TestNoDegradedFallbackLogWhenTheFailureSurfaces: without degraded mode the
// failure is returned to the caller, whose own layer logs it, so the engine
// writes no fallback line.
func TestNoDegradedFallbackLogWhenTheFailureSurfaces(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	engine := newSentinelTestEngine(t, &fakeDuckDBExecutor{err: fmt.Errorf("forced duck failure")}, nil)
	logs := withObservedLogs(engine)

	_, err := engine.Query(context.Background(), degradedLogTables(), coldTierQuery(), nil)
	require.ErrorContains(t, err, "forced duck failure")
	require.Zero(t, logs.FilterMessage(degradedFallbackMsg).Len())
}
