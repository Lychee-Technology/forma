package federated

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

const (
	typeGuardOldPath     = "s3://b/7/old.parquet"
	typeGuardHealthyPath = "s3://b/7/healthy.parquet"
)

// guardEngineDuck is the engine fake with the #732 proof scans scripted:
// schema 7's age (integer on integer_01) is DOUBLE in the old object, so the
// guard proves it before the main scan. drainFail makes the #251 verification
// drain of that object fail as well, i.e. the object is corrupt. blockProofs
// and blockDescribe hold a proof scan, or the footer probe of one path, until
// the request's context is done, as a read that outlives the budget would.
type guardEngineDuck struct {
	fakeDuckDBExecutor
	offending     string // the proof's answer; empty means a clean scan
	proofErr      error
	drainFail     string
	blockProofs   bool
	blockDescribe string
	proofs        int
}

func (d *guardEngineDuck) Query(ctx context.Context, query string, args ...any) (duckDBRowsIterator, error) {
	if d.blockDescribe != "" && strings.HasPrefix(query, "DESCRIBE ") && strings.Contains(query, d.blockDescribe) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if strings.HasPrefix(query, "SELECT CAST(") {
		d.proofs++
		if d.blockProofs {
			<-ctx.Done()
			return nil, fmt.Errorf("INTERRUPT Error: Interrupted!: %w", ctx.Err())
		}
		if d.proofErr != nil {
			return nil, d.proofErr
		}
		if d.offending != "" {
			return &fakeStringRows{vals: []string{d.offending}}, nil
		}
		return &fakeStringRows{}, nil
	}
	if d.drainFail != "" && isBareParquetDrainSQL(query) && strings.Contains(query, d.drainFail) {
		return nil, fmt.Errorf("IO Error: cannot open %s", d.drainFail)
	}
	return d.fakeDuckDBExecutor.Query(ctx, query, args...)
}

func newGuardEngineDuck() *guardEngineDuck {
	return &guardEngineDuck{fakeDuckDBExecutor: fakeDuckDBExecutor{
		rows: &singleDuckDBRow{rowID: uuid.New()},
		describeCols: map[string][][2]string{
			typeGuardOldPath:     append(buildValidSystemCols(), [2]string{"age", "DOUBLE"}),
			typeGuardHealthyPath: append(buildValidSystemCols(), [2]string{"age", "INTEGER"}),
		},
	}}
}

func queryGuardEngine(t *testing.T, duck *guardEngineDuck, pg *fakePostgresFederatedSource, opts *model.FederatedQueryOptions, paths ...string) (*model.PersistentRecordPage, error) {
	t.Helper()
	return queryGuardEngineWith(t, hybridDuckConfig(), duck, pg, opts, paths...)
}

func queryGuardEngineWith(t *testing.T, cfg forma.DuckDBConfig, duck *guardEngineDuck, pg *fakePostgresFederatedSource, opts *model.FederatedQueryOptions, paths ...string) (*model.PersistentRecordPage, error) {
	t.Helper()
	restore := initTestDescriptors()
	t.Cleanup(restore)
	e := NewDBFederatedQueryEngine(pg, &fakeDirtyIDFetcher{}, duck, nil,
		cfg, testMetadataCacheSchema7(t), "host=x", WithParquetSource(&fakeParquetSource{paths: paths}))
	return e.Query(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"}, coldTierQuery(), opts)
}

// A refusal stops the read before the main scan and is not degraded to a
// Postgres-only answer: the declaration cannot read this object on any query,
// and a fallback would hide that behind a silently short page.
func TestTypeRefusalStopsTheReadAndIsNotDegraded(t *testing.T) {
	duck := newGuardEngineDuck()
	duck.offending = "1000.5"
	pg := &fakePostgresFederatedSource{}

	_, err := queryGuardEngine(t, duck, pg, &model.FederatedQueryOptions{AllowPartialDegradedMode: true},
		typeGuardHealthyPath, typeGuardOldPath)

	var refusal *forma.ParquetTypeIncompatibleError
	require.ErrorAs(t, err, &refusal)
	require.Equal(t, int16(7), refusal.SchemaID)
	require.Equal(t, "age", refusal.Attribute)
	require.Equal(t, forma.MainColumn("integer_01"), refusal.Column)
	require.Equal(t, typeGuardOldPath, refusal.Path)
	require.Zero(t, duck.calls, "no main scan may run over the refused object")
	require.Zero(t, pg.queryCalls, "the refusal must not degrade to postgres-only")
}

// A clean proof admits the object and the main scan runs as before.
func TestTypeGuardAdmitsAProvenObject(t *testing.T) {
	duck := newGuardEngineDuck()
	_, err := queryGuardEngine(t, duck, &fakePostgresFederatedSource{}, nil, typeGuardHealthyPath, typeGuardOldPath)
	require.NoError(t, err)
	require.Equal(t, 1, duck.proofs)
	require.Equal(t, 1, duck.calls)
}

// A manifest object the guard cannot prove fails the read closed, classified
// like any read failure: a readable object is ErrFederatedReadFailed (and
// degradable, since nothing proved it wrong), never an admitted scan.
func TestUnverifiedObjectFailsTheReadClosed(t *testing.T) {
	duck := newGuardEngineDuck()
	duck.proofErr = fmt.Errorf("HTTP 503 Service Unavailable")

	_, err := queryGuardEngine(t, duck, &fakePostgresFederatedSource{}, nil, typeGuardHealthyPath, typeGuardOldPath)
	require.ErrorIs(t, err, ErrFederatedReadFailed)
	require.NotErrorIs(t, err, forma.ErrParquetTypeIncompatible)
	require.Contains(t, err.Error(), typeGuardOldPath)
	require.Zero(t, duck.calls, "an unverified object must not reach the main scan")

	pg := &fakePostgresFederatedSource{}
	_, err = queryGuardEngine(t, duck, pg, &model.FederatedQueryOptions{AllowPartialDegradedMode: true},
		typeGuardHealthyPath, typeGuardOldPath)
	require.NoError(t, err, "an unverified object is a read failure the degraded mode may absorb")
	require.Equal(t, 1, pg.queryCalls)
}

// An object that is unverified because it is corrupt keeps #251's partial
// semantics: verification attributes the failure to it, and the readable
// remainder answers with the exclusion reported.
func TestUnverifiedCorruptObjectKeepsPartialSemantics(t *testing.T) {
	duck := newGuardEngineDuck()
	duck.proofErr = fmt.Errorf("parquet page decode failed")
	duck.drainFail = typeGuardOldPath
	opts := &model.FederatedQueryOptions{}

	_, err := queryGuardEngine(t, duck, &fakePostgresFederatedSource{}, opts, typeGuardHealthyPath, typeGuardOldPath)
	require.NoError(t, err, "the readable remainder must still answer")
	require.Equal(t, 1, duck.calls)
	require.NotContains(t, duck.lastSQL, typeGuardOldPath, "the corrupt object is excluded from the scan")
	require.NotNil(t, opts.PartialScan)
	require.Equal(t, []string{typeGuardOldPath}, opts.PartialScan.ExcludedObjects)
}

// A pre-read check the budget cuts short is a timeout, not an unreadable
// object: the read fails with the deadline in its chain and is not degraded,
// exactly as a main scan past the budget is not (#465). Answering from
// Postgres would spend more time past a limit that exists to bound latency.
func TestTypeGuardTimeoutIsNotDegraded(t *testing.T) {
	for name, block := range map[string]func(*guardEngineDuck){
		"proof scan":   func(d *guardEngineDuck) { d.blockProofs = true },
		"footer probe": func(d *guardEngineDuck) { d.blockDescribe = typeGuardOldPath },
	} {
		t.Run(name, func(t *testing.T) {
			duck := newGuardEngineDuck()
			block(duck)
			cfg := hybridDuckConfig()
			cfg.QueryTimeout = 50 * time.Millisecond
			pg := &fakePostgresFederatedSource{}

			start := time.Now()
			_, err := queryGuardEngineWith(t, cfg, duck, pg, &model.FederatedQueryOptions{AllowPartialDegradedMode: true},
				typeGuardHealthyPath, typeGuardOldPath)

			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.Contains(t, err.Error(), typeGuardOldPath)
			require.Less(t, time.Since(start), 5*time.Second, "the check was not cancelled by the budget")
			require.Zero(t, duck.calls, "an unverified object must not reach the main scan")
			require.Zero(t, pg.queryCalls, "a spent budget must not degrade to postgres-only")
		})
	}
}
