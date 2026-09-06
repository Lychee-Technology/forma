package federated

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"github.com/lychee-technology/forma/internal/sqlutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// identifyFakeDuck routes drains by SQL shape: a guarded drain contains the
// REPLACE guard (we assert via the authored message constant, which is ours,
// not the driver's), a bare drain does not. Per-path failure counts model
// deterministic vs transient failures.
type identifyFakeDuck struct {
	guardFailN map[string]int // path → number of guarded drains that fail (-1 = always)
	bareFailN  map[string]int // path → number of bare drains that fail (-1 = always)
	queries    []string
}

func isGuardedSQL(sqlStr string) bool {
	return strings.Contains(sqlStr, sqlgen.ParquetNullRowIDMessage)
}

func (d *identifyFakeDuck) Query(ctx context.Context, sqlStr string, args ...any) (duckDBRowsIterator, error) {
	d.queries = append(d.queries, sqlStr)
	failN := d.bareFailN
	if isGuardedSQL(sqlStr) {
		failN = d.guardFailN
	}
	for p, n := range failN {
		if !strings.Contains(sqlStr, p) {
			continue
		}
		if n != 0 {
			if n > 0 {
				failN[p] = n - 1
			}
			return nil, fmt.Errorf("drain failed for %s", p)
		}
	}
	return &verifyFakeRows{rowsLeft: 1}, nil
}

func TestIdentifyGuardViolationsAttributesSchemaWrongObject(t *testing.T) {
	duck := &identifyFakeDuck{guardFailN: map[string]int{"s3://b/rogue.parquet": -1}}
	got := identifyGuardViolations(context.Background(), duck,
		[]string{"s3://b/healthy.parquet", "s3://b/rogue.parquet"})
	require.Equal(t, []string{"s3://b/rogue.parquet"}, got)

	// Contract: attribution requires guarded-fail ×2 AND bare-pass. The
	// healthy path must cost exactly one guarded drain and zero bare drains.
	var healthyDrains, rogueGuarded, rogueBare int
	for _, q := range duck.queries {
		switch {
		case strings.Contains(q, "healthy.parquet"):
			healthyDrains++
			require.True(t, isGuardedSQL(q), "a healthy path must only ever see the guarded drain")
		case strings.Contains(q, "rogue.parquet") && isGuardedSQL(q):
			rogueGuarded++
		case strings.Contains(q, "rogue.parquet"):
			rogueBare++
		}
	}
	require.Equal(t, 1, healthyDrains)
	require.Equal(t, 2, rogueGuarded, "deterministic confirmation is two consecutive guarded failures (#251 R2-1 mirror)")
	require.Equal(t, 1, rogueBare)
}

func TestIdentifyGuardViolationsRejectsByteCorruptAndSickStore(t *testing.T) {
	// Byte-corrupt / sick store: bare drain fails too → differential does not
	// split → NOT attributed. This is the misattribution firewall.
	duck := &identifyFakeDuck{
		guardFailN: map[string]int{"s3://b/corrupt.parquet": -1},
		bareFailN:  map[string]int{"s3://b/corrupt.parquet": -1},
	}
	got := identifyGuardViolations(context.Background(), duck, []string{"s3://b/corrupt.parquet"})
	require.Empty(t, got)
}

func TestIdentifyGuardViolationsTransientGuardedFailureNotAttributed(t *testing.T) {
	// One guarded failure then clean re-drain = transient blip, not a
	// deterministic invariant violation.
	duck := &identifyFakeDuck{guardFailN: map[string]int{"s3://b/blip.parquet": 1}}
	got := identifyGuardViolations(context.Background(), duck, []string{"s3://b/blip.parquet"})
	require.Empty(t, got)
}

func TestIdentifyGuardViolationsSkipsGlobEntries(t *testing.T) {
	duck := &identifyFakeDuck{}
	got := identifyGuardViolations(context.Background(), duck,
		[]string{"s3://b/glob-*.parquet", "s3://b/part-?.parquet", "s3://b/part-[0-9].parquet"})
	require.Empty(t, got)
	require.Empty(t, duck.queries, "glob entries name a set, not one object — no drains at all")
}

// TestIdentifyGuardViolationsAttributesQuoteBearingPath pins #479: the guarded
// drain escapes the path (#456), so a schema-wrong object whose key carries a
// quote is nameable on its own instead of hiding behind all-or-nothing.
func TestIdentifyGuardViolationsAttributesQuoteBearingPath(t *testing.T) {
	const quoted = "s3://b/it's.parquet"
	escaped := sqlutil.EscapeLiteral(quoted)
	duck := &identifyFakeDuck{guardFailN: map[string]int{escaped: -1}}
	got := identifyGuardViolations(context.Background(), duck,
		[]string{"s3://b/healthy.parquet", quoted})
	require.Equal(t, []string{quoted}, got)
	for _, q := range duck.queries {
		require.NotContains(t, q, "'"+quoted+"'", "quote-bearing path must never render raw: %s", q)
	}
}

func TestIdentifyGuardViolationsCancelledContextIdentifiesNothing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	duck := &identifyFakeDuck{guardFailN: map[string]int{"s3://b/rogue.parquet": -1}}
	got := identifyGuardViolations(ctx, duck, []string{"s3://b/rogue.parquet"})
	require.Empty(t, got, "cancellation is indistinguishable from failure — attribute nothing")
}

func TestParquetGuardViolationErrorChain(t *testing.T) {
	cause := fmt.Errorf("execute duckdb query: %w: boom", ErrFederatedReadFailed)
	err := &ParquetGuardViolationError{SchemaID: 7, Paths: []string{"s3://b/rogue.parquet"}, cause: cause}
	require.ErrorIs(t, err, ErrFederatedReadFailed,
		"identification must not change classification: still a degradable read failure")
	require.Contains(t, err.Error(), "s3://b/rogue.parquet")
	require.Contains(t, err.Error(), "schema 7")
}

func TestWithLoggerFeedsEngineAndValidator(t *testing.T) {
	core, _ := observer.New(zap.WarnLevel)
	l := zap.New(core)
	e := NewDBFederatedQueryEngine(nil, nil, nil, nil, forma.DuckDBConfig{}, nil, "", WithLogger(l))
	require.Same(t, l, e.logger, "WithLogger must feed the engine's own outlet (#351)")
	require.Same(t, l, e.schemaValidator.logger, "…without dropping the validator outlet (#256)")

	var nilEngine *DBFederatedQueryEngine
	require.NotNil(t, nilEngine.log(), "log() must be nil-safe like the validator's")
	require.NotNil(t, (&DBFederatedQueryEngine{}).log())
}
