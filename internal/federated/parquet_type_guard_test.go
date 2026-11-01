package federated

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// sqlDBExecutor runs the guard against a real in-process DuckDB, so the
// proof predicates are judged by the engine that reads the objects.
type sqlDBExecutor struct {
	db     *sql.DB
	proofs int
}

func (s *sqlDBExecutor) Query(ctx context.Context, query string, args ...any) (duckDBRowsIterator, error) {
	if strings.HasPrefix(query, "SELECT CAST(") {
		s.proofs++
	}
	return s.db.QueryContext(ctx, query, args...)
}

func newGuardDuck(t *testing.T) *sqlDBExecutor {
	t.Helper()
	db, err := sql.Open("duckdb", strictLambdaDSN)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return &sqlDBExecutor{db: db}
}

// writeQtyObject writes a parquet object with the system columns and a qty
// column of typ holding values (SQL literals, each cast to typ), as an object
// flushed under an earlier declaration would hold it.
func writeQtyObject(t *testing.T, duck *sqlDBExecutor, dir, name, typ string, values ...string) string {
	t.Helper()
	rows := make([]string, len(values))
	for i, v := range values {
		rows[i] = fmt.Sprintf("SELECT gen_random_uuid() AS row_id, CAST(100 AS BIGINT) AS changed_at, "+
			"CAST(0 AS BIGINT) AS deleted_at, CAST(50 AS BIGINT) AS ltbase_created_at, CAST(%s AS %s) AS qty", v, typ)
	}
	path := filepath.Join(dir, name)
	_, err := duck.db.Exec(fmt.Sprintf("COPY (%s) TO '%s' (FORMAT PARQUET)", strings.Join(rows, " UNION ALL "), path))
	require.NoError(t, err)
	return path
}

func checkQty(t *testing.T, v *parquetSchemaValidator, duck DuckDBQueryExecutor, meta forma.AttributeMetadata,
	stamps map[string]map[string]string, paths ...string) ([]unverifiedProof, error) {
	t.Helper()
	union, _, err := v.Validate(context.Background(), duck, paths, stamps)
	require.NoError(t, err)
	return v.CheckTypes(context.Background(), duck, 7, forma.SchemaAttributeCache{"qty": meta}, union, stamps)
}

// TestCheckTypesProvesValuesUnderTheNewDeclaration is the #732 regression over
// real objects: each case is one object written under the old type and read
// under the new one. Without the guard, DuckDB reads 1000.5 as 1000 and fails
// the statement on 1e30 with its own conversion error.
func TestCheckTypesProvesValuesUnderTheNewDeclaration(t *testing.T) {
	for name, tc := range map[string]struct {
		meta     forma.AttributeMetadata
		typ      string
		values   []string
		evidence string // empty: the read is allowed
	}{
		"numeric→bigint(double_01) fraction":     {bigintOnDouble, "DOUBLE", []string{"1000.0", "1000.5"}, "it holds 1000.5, which is not a whole number"},
		"numeric→bigint(double_01) overflow":     {bigintOnDouble, "DOUBLE", []string{"1e30"}, "which is not a whole number in [-9007199254740992, 9007199254740992]"},
		"numeric→bigint(double_01) past 2^53":    {bigintOnDouble, "DOUBLE", []string{"9007199254740994"}, "9007199254740994"},
		"numeric→bigint(double_01) NaN":          {bigintOnDouble, "DOUBLE", []string{"'nan'"}, "nan"},
		"numeric→bigint(double_01) infinity":     {bigintOnDouble, "DOUBLE", []string{"'inf'"}, "inf"},
		"numeric→bigint(double_01) whole values": {bigintOnDouble, "DOUBLE", []string{"1000", "-5", "9007199254740992", "NULL"}, ""},
		"numeric→bigint EAV past 2^53":           {bigintEAV, "DOUBLE", []string{"9007199254740994"}, ""},
		"numeric→bigint EAV 2^63":                {bigintEAV, "DOUBLE", []string{"9223372036854775807.0"}, "9.223372036854776e+18"},
		"numeric→integer(integer_01) fraction":   {integerOnColumn, "DOUBLE", []string{"2147483647.5"}, "2147483647.5"},
		"numeric→integer(integer_01) overflow":   {integerOnColumn, "DOUBLE", []string{"3e9"}, "3000000000"},
		"numeric→integer(integer_01) whole":      {integerOnColumn, "DOUBLE", []string{"7", "-2147483648"}, ""},
		"bigint→integer(integer_01) overflow":    {integerOnColumn, "BIGINT", []string{"2147483648"}, "2147483648"},
		"bigint→integer(integer_01) in range":    {integerOnColumn, "BIGINT", []string{"5"}, ""},
		"bigint→numeric past 2^53 rounds":        {numericEAV, "BIGINT", []string{"9007199254740993"}, ""}, // D2 allows the rounding (#733)
		"bigint→numeric in range":                {numericEAV, "BIGINT", []string{"42"}, ""},
		"text→bigint canonical digits":           {bigintEAV, "VARCHAR", []string{"'9'", "'-12'"}, ""},
		"text→bigint leading zero":               {bigintEAV, "VARCHAR", []string{"'09'"}, "it holds a text value that is not a whole number"},
		"text→bigint decimal":                    {bigintEAV, "VARCHAR", []string{"'9.5'"}, "it holds a text value"},
		"text→bigint padded":                     {bigintEAV, "VARCHAR", []string{"' 42'"}, "it holds a text value"},
		"text→bigint word":                       {bigintEAV, "VARCHAR", []string{"'n/a'"}, "it holds a text value"},
		"text→numeric canonical":                 {numericEAV, "VARCHAR", []string{"'9.5'", "'-0.25'", "'-12.0'"}, ""},
		"text→numeric NaN":                       {numericEAV, "VARCHAR", []string{"'nan'"}, "it holds a text value that is not a finite number"},
		"text→numeric infinity":                  {numericEAV, "VARCHAR", []string{"'inf'"}, "it holds a text value that is not a finite number"},
		"text→numeric negative infinity":         {numericEAV, "VARCHAR", []string{"'-inf'"}, "it holds a text value that is not a finite number"},
		"text→bool canonical":                    {boolEAV, "VARCHAR", []string{"'true'", "'false'"}, ""},
		"text→bool word":                         {boolEAV, "VARCHAR", []string{"'maybe'"}, "it holds a text value that is not a boolean"},
	} {
		t.Run(name, func(t *testing.T) {
			duck := newGuardDuck(t)
			path := writeQtyObject(t, duck, t.TempDir(), "old.parquet", tc.typ, tc.values...)

			unverified, err := checkQty(t, newParquetSchemaValidator(), duck, tc.meta, nil, path)
			require.Empty(t, unverified)
			if tc.evidence == "" {
				require.NoError(t, err)
				return
			}
			var refusal *forma.ParquetTypeIncompatibleError
			require.ErrorAs(t, err, &refusal)
			require.ErrorIs(t, err, forma.ErrParquetTypeIncompatible)
			require.Equal(t, "qty", refusal.Attribute)
			require.Equal(t, path, refusal.Path)
			require.Equal(t, strings.ToUpper(tc.typ), refusal.FileType)
			require.Contains(t, refusal.Evidence, tc.evidence)
			if tc.typ == "VARCHAR" {
				for _, v := range tc.values {
					require.NotContains(t, refusal.Evidence, strings.Trim(v, "'"), "stored text must not be quoted")
				}
			}
		})
	}
}

// When one object holds the column as DOUBLE, union_by_name reads a BIGINT
// sibling through DOUBLE. A value past 2^53 would then read rounded under a
// declaration — bigint — whose own type it already has.
func TestCheckTypesJudgesTheDoubleHopOfABigintSibling(t *testing.T) {
	duck := newGuardDuck(t)
	dir := t.TempDir()
	wide := writeQtyObject(t, duck, dir, "wide.parquet", "BIGINT", "9007199254740993")

	_, err := checkQty(t, newParquetSchemaValidator(), duck, bigintEAV, nil, wide)
	require.NoError(t, err, "alone, the BIGINT object is the exported type and costs no proof")
	require.Zero(t, duck.proofs)

	old := writeQtyObject(t, duck, dir, "old.parquet", "DOUBLE", "1.0")
	_, err = checkQty(t, newParquetSchemaValidator(), duck, bigintEAV, nil, wide, old)
	var refusal *forma.ParquetTypeIncompatibleError
	require.ErrorAs(t, err, &refusal)
	require.Equal(t, wide, refusal.Path, "the DOUBLE object is whole; the BIGINT one cannot pass through DOUBLE")
}

// The refusal names everything an operator needs to find the redeclaration:
// schema, attribute, both types, the bound column, the object and the value.
func TestParquetTypeRefusalMessage(t *testing.T) {
	duck := newGuardDuck(t)
	path := writeQtyObject(t, duck, t.TempDir(), "old.parquet", "DOUBLE", "1000.5")

	_, err := checkQty(t, newParquetSchemaValidator(), duck, bigintOnDouble, nil, path)
	require.Error(t, err)
	for _, want := range []string{"schema 7", "attribute qty", "declared bigint", "double_01", path, "DOUBLE", "1000.5", "#730", "docs/schema-evolution.md"} {
		require.Contains(t, err.Error(), want)
	}
}

// proofScriptedDuck answers footer probes from scriptedDescribeExecutor and
// proof scans from a per-path script: an offending value, a clean scan
// (absent), or a read failure.
type proofScriptedDuck struct {
	scriptedDescribeExecutor
	offending  map[string]string
	failProofs map[string]bool
	proofs     []string
}

func (d *proofScriptedDuck) Query(ctx context.Context, query string, args ...any) (duckDBRowsIterator, error) {
	if !strings.HasPrefix(query, "SELECT CAST(") {
		return d.scriptedDescribeExecutor.Query(ctx, query, args...)
	}
	d.proofs = append(d.proofs, query)
	for path := range d.failProofs {
		if strings.Contains(query, path) {
			return nil, fmt.Errorf("IO Error: cannot read %s", path)
		}
	}
	for path, value := range d.offending {
		if strings.Contains(query, path) {
			return &fakeStringRows{vals: []string{value}}, nil
		}
	}
	return &fakeStringRows{}, nil
}

func qtyCols(typ string) [][2]string {
	return append(buildValidSystemCols(), [2]string{"qty", typ})
}

// A different family, or a list element this guard has no proof for, is
// refused from the footer alone: no value scan can make it safe.
func TestCheckTypesRefusesWithoutAScan(t *testing.T) {
	for name, tc := range map[string]struct {
		meta     forma.AttributeMetadata
		typ      string
		evidence string
	}{
		"bool under bigint":     {bigintEAV, "BOOLEAN", "different type family"},
		"number under text":     {textEAV, "DOUBLE", "different type family"},
		"scalar under list":     {bigintList, "BIGINT", "different type family"},
		"list element unproven": {bigintList, "DOUBLE[]", "per list element"},
	} {
		t.Run(name, func(t *testing.T) {
			duck := &proofScriptedDuck{scriptedDescribeExecutor: scriptedDescribeExecutor{
				cols: map[string][][2]string{"old.parquet": qtyCols(tc.typ)},
			}}
			_, err := checkQty(t, newParquetSchemaValidator(), duck, tc.meta, nil, "s3://b/7/old.parquet")
			var refusal *forma.ParquetTypeIncompatibleError
			require.ErrorAs(t, err, &refusal)
			require.Contains(t, refusal.Evidence, tc.evidence)
			require.Empty(t, duck.proofs)
		})
	}
}

// An object whose proof scan cannot be read is unverified, not admitted: the
// caller fails the read closed on it, with the read error as the cause. A
// refusal elsewhere in the set wins, because it is a verdict.
func TestCheckTypesReportsUnreadableProofsAsUnverified(t *testing.T) {
	duck := &proofScriptedDuck{
		scriptedDescribeExecutor: scriptedDescribeExecutor{cols: map[string][][2]string{
			"a.parquet": qtyCols("DOUBLE"), "b.parquet": qtyCols("DOUBLE"),
		}},
		failProofs: map[string]bool{"a.parquet": true},
	}
	a, b := "s3://b/7/a.parquet", "s3://b/7/b.parquet"
	unverified, err := checkQty(t, newParquetSchemaValidator(), duck, bigintOnDouble, nil, a, b)
	require.NoError(t, err)
	require.Len(t, unverified, 1)
	require.Equal(t, a, unverified[0].path)
	require.ErrorContains(t, unverified[0].err, "IO Error: cannot read a.parquet")

	duck.offending = map[string]string{"b.parquet": "1000.5"}
	unverified, err = checkQty(t, newParquetSchemaValidator(), duck, bigintOnDouble, nil, a, b)
	require.ErrorIs(t, err, forma.ErrParquetTypeIncompatible)
	require.Empty(t, unverified)
}

// A clean proof is cached under the object's manifest stamp, so a write-once
// object is scanned once; a moved stamp re-proves it. A violation is never
// cached, so a repaired object is re-judged on the next query.
func TestCheckTypesCachesOnlyCleanProofs(t *testing.T) {
	const path = "s3://b/7/old.parquet"
	duck := &proofScriptedDuck{scriptedDescribeExecutor: scriptedDescribeExecutor{
		cols: map[string][][2]string{"old.parquet": qtyCols("DOUBLE")},
	}}
	v := newParquetSchemaValidator()

	for range 2 {
		_, err := checkQty(t, v, duck, bigintOnDouble, nil, path)
		require.NoError(t, err)
	}
	require.Len(t, duck.proofs, 1, "a clean proof answers the next query from the cache")

	moved := map[string]map[string]string{path: stampCols(qtyCols("DOUBLE"))}
	_, err := checkQty(t, v, duck, bigintOnDouble, moved, path)
	require.NoError(t, err)
	require.Len(t, duck.proofs, 2, "a different stamp is a different object and is proved again")

	duck.offending = map[string]string{"old.parquet": "1000.5"}
	other := map[string]map[string]string{path: stampCols(append(qtyCols("DOUBLE"), [2]string{"extra", "BIGINT"}))}
	for range 2 {
		_, err := checkQty(t, v, duck, bigintOnDouble, other, path)
		require.True(t, errors.Is(err, forma.ErrParquetTypeIncompatible))
	}
	require.Len(t, duck.proofs, 4, "a refusal is re-judged every query")
}

// gatedProofDuck holds every proof scan until release closes, so callers that
// miss the cache while the first scan runs overlap it. started closes when
// the first scan begins.
type gatedProofDuck struct {
	scriptedDescribeExecutor
	started chan struct{}
	release chan struct{}
	proofs  atomic.Int32
}

func newGatedProofDuck() *gatedProofDuck {
	return &gatedProofDuck{
		scriptedDescribeExecutor: scriptedDescribeExecutor{cols: map[string][][2]string{"old.parquet": qtyCols("DOUBLE")}},
		started:                  make(chan struct{}),
		release:                  make(chan struct{}),
	}
}

func (d *gatedProofDuck) Query(ctx context.Context, query string, args ...any) (duckDBRowsIterator, error) {
	if !strings.HasPrefix(query, "SELECT CAST(") {
		return d.scriptedDescribeExecutor.Query(ctx, query, args...)
	}
	if d.proofs.Add(1) == 1 {
		close(d.started)
	}
	select {
	case <-d.release:
		return &fakeStringRows{}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// checkGatedQty validates the object once, then returns a guard call over
// that union that can run concurrently with others.
func checkGatedQty(t *testing.T, v *parquetSchemaValidator, duck *gatedProofDuck, path string) func(context.Context) ([]unverifiedProof, error) {
	t.Helper()
	union, _, err := v.Validate(context.Background(), duck, []string{path}, nil)
	require.NoError(t, err)
	return func(ctx context.Context) ([]unverifiedProof, error) {
		return v.CheckTypes(ctx, duck, 7, forma.SchemaAttributeCache{"qty": bigintOnDouble}, union, nil)
	}
}

// A clean proof reads the whole column, so a burst of queries that miss the
// cache together must not each scan the object: one scan answers them all.
func TestCheckTypesCoalescesConcurrentProofs(t *testing.T) {
	duck := newGatedProofDuck()
	check := checkGatedQty(t, newParquetSchemaValidator(), duck, "s3://b/7/old.parquet")

	const callers = 8
	results := make(chan error, callers)
	run := func() {
		unverified, err := check(context.Background())
		if err == nil && len(unverified) > 0 {
			err = fmt.Errorf("unverified: %v", unverified)
		}
		results <- err
	}
	go run()
	<-duck.started
	for range callers - 1 {
		go run()
	}
	time.Sleep(50 * time.Millisecond) // let the other callers reach the leader's scan
	close(duck.release)
	for range callers {
		require.NoError(t, <-results)
	}
	require.EqualValues(t, 1, duck.proofs.Load(), "concurrent misses share the leader's scan")
}

// A caller whose own context ends while it waits on another caller's scan is
// unverified with that context error, which keeps a spent budget
// non-degradable; it is never admitted on a proof it did not see.
func TestCheckTypesWaiterReportsItsOwnContext(t *testing.T) {
	duck := newGatedProofDuck()
	defer close(duck.release)
	check := checkGatedQty(t, newParquetSchemaValidator(), duck, "s3://b/7/old.parquet")

	go func() { _, _ = check(context.Background()) }()
	<-duck.started
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	unverified, err := check(ctx)
	require.NoError(t, err)
	require.Len(t, unverified, 1)
	require.ErrorIs(t, unverified[0].err, context.DeadlineExceeded)
	require.EqualValues(t, 1, duck.proofs.Load(), "the waiter must not start its own scan")
}
