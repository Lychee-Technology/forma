package federated

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// The #351 seam needs a two-object set: one healthy object and one whose
// EXPORT SCHEMA is wrong. The set scan fails as a whole, the #251 bare drains
// read BOTH objects clean (that is precisely the triage gap #351 closes), and
// only the guarded single-file drain separates them.
const (
	guardHealthyPath = "s3://bucket/7/base/healthy.parquet"
	guardRoguePath   = "s3://bucket/7/base/rogue.parquet"
)

// guardSeamFakeDuck reproduces the #351 failure shape end to end: every main
// set scan fails carrying the guard's authored message (as the real engine
// surfaces it through DuckDB), every BARE drain reads clean — including the
// rogue object's, which is what makes #251 verification decline — and the
// GUARDED single-file drain fails only for the rogue path. Schema probes are
// delegated to the standard engine fake, as in retryFakeDuck.
type guardSeamFakeDuck struct {
	fakeDuckDBExecutor
	rogue string

	mainSQL  []string // one entry per main-scan attempt
	drainSQL []string // every per-file drain, bare and guarded alike
}

func (d *guardSeamFakeDuck) Query(ctx context.Context, sqlStr string, args ...any) (duckDBRowsIterator, error) {
	if strings.HasPrefix(sqlStr, "DESCRIBE ") || strings.HasPrefix(sqlStr, "SELECT file FROM glob(") {
		return d.fakeDuckDBExecutor.Query(ctx, sqlStr, args...)
	}
	if isBareParquetDrainSQL(sqlStr) || isGuardedParquetDrainSQL(sqlStr) {
		d.drainSQL = append(d.drainSQL, sqlStr)
		if isGuardedParquetDrainSQL(sqlStr) && d.rogue != "" && strings.Contains(sqlStr, d.rogue) {
			return nil, fmt.Errorf("Invalid Error: %s", sqlgen.ParquetNullRowIDMessage)
		}
		return &verifyFakeRows{rowsLeft: 1}, nil
	}
	d.mainSQL = append(d.mainSQL, sqlStr)
	return nil, fmt.Errorf("Invalid Error: %s", sqlgen.ParquetNullRowIDMessage)
}

func (d *guardSeamFakeDuck) guardedDrains() []string {
	var out []string
	for _, q := range d.drainSQL {
		if isGuardedParquetDrainSQL(q) {
			out = append(out, q)
		}
	}
	return out
}

// newGuardSeamEngine mirrors newParquetSourceTestEngine but forwards extra
// engine options, which the degraded-mode log test needs (WithLogger is the
// only logger setter).
func newGuardSeamEngine(t *testing.T, duck DuckDBQueryExecutor, src ParquetSource, opts ...EngineOption) *DBFederatedQueryEngine {
	t.Helper()
	all := append([]EngineOption{WithParquetSource(src)}, opts...)
	return NewDBFederatedQueryEngine(&fakePostgresFederatedSource{}, &fakeDirtyIDFetcher{}, duck, nil,
		hybridDuckConfig(), testMetadataCacheSchema7(t), "host=x", all...)
}

func guardSeamTables() model.StorageTables {
	return model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"}
}

// TestGuardFailureIdentifiesOffendingObject is the payload: a set-scan failure
// that neither the missing-object classifier nor the #251 verification pass
// claims must come back naming the object(s) that fail the guarded single-file
// scan — and it must do so without retrying or altering the classification.
func TestGuardFailureIdentifiesOffendingObject(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	duck := &guardSeamFakeDuck{rogue: guardRoguePath}
	e := newGuardSeamEngine(t, duck, &fakeParquetSource{paths: []string{guardHealthyPath, guardRoguePath}})

	_, err := e.Query(context.Background(), guardSeamTables(), coldTierQuery(), nil)

	require.Error(t, err)
	var viol *ParquetGuardViolationError
	require.ErrorAs(t, err, &viol, "an unclaimed read failure must carry per-file identification (#351)")
	require.Equal(t, []string{guardRoguePath}, viol.Paths)
	require.Equal(t, int16(7), viol.SchemaID)
	require.ErrorIs(t, err, ErrFederatedReadFailed, "classification chain unchanged")
	require.Len(t, duck.mainSQL, 1, "identification only: no retry pass — exclusion is #251's, not #351's")
}

// TestGuardViolationIsNeverExcluded is the load-bearing negative: the violator
// is identified, NOT cached for exclusion. A schema-wrong object may
// legitimately own rows, so dropping it would silently shorten every answer
// for the retention window. This test goes red the moment someone wires
// identification into corruptPaths.Add.
func TestGuardViolationIsNeverExcluded(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	duck := &guardSeamFakeDuck{rogue: guardRoguePath}
	e := newGuardSeamEngine(t, duck, &fakeParquetSource{paths: []string{guardHealthyPath, guardRoguePath}})

	_, _ = e.Query(context.Background(), guardSeamTables(), coldTierQuery(), nil)
	_, _ = e.Query(context.Background(), guardSeamTables(), coldTierQuery(), nil)

	require.Len(t, duck.mainSQL, 2)
	require.Contains(t, duck.mainSQL[1], guardRoguePath,
		"identification must not shrink the second query's scan set")

	kept, excluded := e.corruptPaths.Split([]string{guardHealthyPath, guardRoguePath})
	require.Empty(t, excluded, "an identified guard violator must never be cached for exclusion")
	require.Len(t, kept, 2)
}

// TestGuardIdentificationLogsUnderDegradedMode pins the loud outlet: degraded
// mode absorbs the error into a Postgres-only answer and toExecutionPlan drops
// plan Notes, so the log line is the only place the attribution survives.
func TestGuardIdentificationLogsUnderDegradedMode(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	core, logs := observer.New(zap.ErrorLevel)
	duck := &guardSeamFakeDuck{rogue: guardRoguePath}
	e := newGuardSeamEngine(t, duck,
		&fakeParquetSource{paths: []string{guardHealthyPath, guardRoguePath}},
		WithLogger(zap.New(core)))

	opts := &model.FederatedQueryOptions{AllowPartialDegradedMode: true}
	page, err := e.Query(context.Background(), guardSeamTables(), coldTierQuery(), opts)

	require.NoError(t, err, "degradable classification must be unchanged by identification")
	require.NotNil(t, page)

	entries := logs.FilterMessage("parquet scan-guard failure attributed to objects").All()
	require.Len(t, entries, 1)
	fields := entries[0].ContextMap()
	require.Equal(t, []any{guardRoguePath}, fields["paths"])
	require.Equal(t, int16(7), fields["schema_id"])
}

// TestMissingObjectClassificationWinsOverIdentification pins the ordering: a
// manifest-listed object missing from storage is #187 scenario 2 —
// non-degradable inconsistency. It must win, and identification must not run
// at all: its drains would spend failing round trips on a store already known
// inconsistent, and a missing object fails the guarded drain too, so it would
// be misattributed as a schema violation.
func TestMissingObjectClassificationWinsOverIdentification(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	duck := &guardSeamFakeDuck{rogue: guardRoguePath}
	src := &fakeParquetSource{
		paths:   []string{guardHealthyPath, guardRoguePath},
		missing: []string{"7/base/healthy.parquet"},
	}
	e := newGuardSeamEngine(t, duck, src)

	_, err := e.Query(context.Background(), guardSeamTables(), coldTierQuery(), nil)

	require.ErrorIs(t, err, ErrParquetSetInconsistent)
	var viol *ParquetGuardViolationError
	require.False(t, errors.As(err, &viol),
		"an inconsistency classification must not be decorated with guard identification")
	require.Empty(t, duck.guardedDrains(),
		"no guarded identification drains after an inconsistency classification")
}

// TestGuardIdentificationSkippedForHintAuthoredSet pins the gating that
// mirrors confirmCorruptPaths: a caller-pinned hint set names objects no
// manifest vouches for, so identification never probes them.
func TestGuardIdentificationSkippedForHintAuthoredSet(t *testing.T) {
	duck := &guardSeamFakeDuck{rogue: guardRoguePath}
	e := NewDBFederatedQueryEngine(nil, nil, duck, nil, hybridDuckConfig(), nil, "",
		WithParquetSource(&fakeParquetSource{}))

	sc := scan{parquetPaths: []string{guardHealthyPath, guardRoguePath}, pathsFromSource: false}
	err := e.failDuckDBScan(context.Background(),
		&model.FederatedAttributeQuery{AttributeQuery: model.AttributeQuery{SchemaID: 7}}, sc,
		fmt.Errorf("scan: %w: guard fired", ErrFederatedReadFailed), "execute duckdb query")

	var viol *ParquetGuardViolationError
	require.False(t, errors.As(err, &viol))
	require.Empty(t, duck.drainSQL, "hint-authored sets must not be probed at all")
}

// TestGuardViolationStillFeedsTheBreaker pins the half of the classification
// contract identification must NOT change. The pressure here is real, not
// hypothetical: #351's own framing — a schema-wrong object is a file problem,
// not engine sickness — is verbatim the argument #251 used two branches above
// to justify handing back the probe slot INSTEAD of recording a failure. So
// this branch ships with a standing invitation to make the same edit, and the
// invitation must be declined: unlike a confirmed-corrupt object, a guard
// violation is not contained by the objects it names — the set scan failed
// outright, no rows were served, and nothing here proves the engine or store
// is healthy. Move the violation return above RecordFailure, or swap
// RecordFailure for ReleaseProbe, and this test goes red.
func TestGuardViolationStillFeedsTheBreaker(t *testing.T) {
	duck := &guardSeamFakeDuck{rogue: guardRoguePath}
	breaker := NewCircuitBreaker(1, time.Minute, time.Minute) // threshold 1: one RecordFailure opens it
	e := NewDBFederatedQueryEngine(nil, nil, duck, breaker, hybridDuckConfig(), nil, "",
		WithParquetSource(&fakeParquetSource{}))

	sc := scan{parquetPaths: []string{guardHealthyPath, guardRoguePath}, pathsFromSource: true}
	err := e.failDuckDBScan(context.Background(),
		&model.FederatedAttributeQuery{AttributeQuery: model.AttributeQuery{SchemaID: 7}}, sc,
		fmt.Errorf("scan: %w: guard fired", ErrFederatedReadFailed), "execute duckdb query")

	var viol *ParquetGuardViolationError
	require.ErrorAs(t, err, &viol,
		"precondition: this must be the guard-violation branch, not one of the earlier ones")
	require.Equal(t, []string{guardRoguePath}, viol.Paths)
	require.True(t, breaker.IsOpen(),
		"an identified guard violation is still a failed scan: the breaker must record it (#351 decorates, it does not reclassify)")
}

// TestGuardIdentificationRunsForSinglePathSet is the one place #351 is
// deliberately WIDER than #251: verification needs a readable remainder and so
// declines below two paths, but naming the single object in the set is exactly
// what an operator needs.
func TestGuardIdentificationRunsForSinglePathSet(t *testing.T) {
	duck := &guardSeamFakeDuck{rogue: guardRoguePath}
	e := NewDBFederatedQueryEngine(nil, nil, duck, nil, hybridDuckConfig(), nil, "",
		WithParquetSource(&fakeParquetSource{}))

	sc := scan{parquetPaths: []string{guardRoguePath}, pathsFromSource: true}
	err := e.failDuckDBScan(context.Background(),
		&model.FederatedAttributeQuery{AttributeQuery: model.AttributeQuery{SchemaID: 7}}, sc,
		fmt.Errorf("scan: %w: guard fired", ErrFederatedReadFailed), "execute duckdb query")

	var viol *ParquetGuardViolationError
	require.ErrorAs(t, err, &viol, "a one-object set must still name its object")
	require.Equal(t, []string{guardRoguePath}, viol.Paths)
	require.ErrorIs(t, err, ErrFederatedReadFailed)
}
