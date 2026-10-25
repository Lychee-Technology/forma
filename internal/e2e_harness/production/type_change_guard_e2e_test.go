//go:build e2e

package production

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"strings"
	"testing"

	"github.com/lychee-technology/forma"
	fedengine "github.com/lychee-technology/forma/internal/federated"
	"github.com/lychee-technology/forma/internal/manifest"
)

// #732: a declaration changed after its parquet was written must not reach
// the read as a silent cast. qty is written under numeric (DOUBLE in parquet)
// and read under bigint. Before the guard, 1000.5 read as 1000 and 1e30
// failed the statement with DuckDB's own conversion error; now the read is
// refused with a ParquetTypeIncompatibleError naming the object, and nothing
// is pruned or rewritten.

const qtyProps = `{
    "name": { "type": "string" },
    "value": { "type": "number" },
    "qty": { "type": "number" }
  }`

func qtyAttrs(valueType string, bound bool) string {
	binding := ""
	if bound {
		binding = `, "column_binding": { "col_name": "double_01" }`
	}
	return fmt.Sprintf(`{
  "name": { "attributeID": 1, "valueType": "text", "column_binding": { "col_name": "text_01" } },
  "value": { "attributeID": 2, "valueType": "numeric" },
  "qty": { "attributeID": 3, "valueType": "%s"%s }
}
`, valueType, binding)
}

// seedQty writes one row per qty under the Env's current declaration, flushes
// them all to one delta object and returns its key. No row stays hot, so the
// read answers from the object.
func seedQty(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, qtys ...float64) string {
	t.Helper()
	profile := func(_ *rand.Rand, ordinal int, _ bool) map[string]any {
		return map[string]any{
			"name":  fmt.Sprintf("row-%04d", ordinal),
			"value": float64(ordinal),
			"qty":   qtys[ordinal%len(qtys)],
		}
	}
	seedGeneration(ctx, t, env, schema, len(qtys), profile)
	return requireSoleParquet(t, "flush", mustFlush(ctx, t, env).NewObjects)
}

// redeclareQty seeds qtys under numeric, then restarts under bigint.
func redeclareQty(ctx context.Context, t *testing.T, bound bool, qtys ...float64) (*Env, SchemaRef, string) {
	t.Helper()
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster, WithSchemaDir(writeSimpleSchemaDir(t, qtyProps, qtyAttrs("numeric", bound))))
	simple := DefaultSchemaFixtures()[0]
	key := seedQty(ctx, t, env, simple, qtys...)
	requireParquetCols(t, "numeric generation", describeParquetCols(ctx, t, env, key), map[string]string{"qty": "DOUBLE"})
	if err := env.EvolveSchema(ctx, writeSimpleSchemaDir(t, qtyProps, qtyAttrs("bigint", bound))); err != nil {
		t.Fatalf("redeclare qty as bigint: %v", err)
	}
	return env, simple, key
}

// requireTypeRefusal asserts a query over the redeclared object is refused
// with the typed error, in both degraded modes, and that the object is still
// stored and listed afterwards.
func requireTypeRefusal(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, key string, column forma.MainColumn, evidence string) {
	t.Helper()
	for _, degraded := range []bool{false, true} {
		_, err := env.Query(ctx, Query{Schema: schema, Limit: 20, AllowPartialDegradedMode: degraded})
		var refusal *forma.ParquetTypeIncompatibleError
		if !errors.As(err, &refusal) {
			t.Fatalf("degraded=%v: want a ParquetTypeIncompatibleError, got: %v", degraded, err)
		}
		if errors.Is(err, fedengine.ErrFederatedReadFailed) {
			t.Errorf("degraded=%v: a type refusal is not a read failure: %v", degraded, err)
		}
		if refusal.Attribute != "qty" || refusal.Declared != "bigint" || refusal.Column != column ||
			refusal.Path != buildParquetS3Path(env, key) || refusal.FileType != "DOUBLE" {
			t.Errorf("degraded=%v: refusal = %+v, want qty declared bigint on %q in %s stored as DOUBLE", degraded, refusal, column, key)
		}
		if !strings.Contains(refusal.Evidence, evidence) {
			t.Errorf("degraded=%v: evidence %q does not name %q", degraded, refusal.Evidence, evidence)
		}
	}
	// Fail closed means fail, not repair: the object keeps its bytes and its
	// manifest entry.
	describeParquetCols(ctx, t, env, key)
	m := loadSchemaManifest(ctx, t, env, schema)
	if !slices.ContainsFunc(m.Files, func(f manifest.FileEntry) bool { return f.Path == key }) {
		t.Errorf("refused object %s must stay listed in the manifest", key)
	}
}

func TestTypeGuardRefusesValuesTheNewDeclarationCannotHold(t *testing.T) {
	for name, tc := range map[string]struct {
		qty      float64
		evidence string
	}{
		"fraction":  {1000.5, "it holds 1000.5"},
		"overflow":  {1e30, "it holds 1e+30"},
		"past 2^53": {9007199254740994, "it holds 9007199254740994"},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			env, simple, key := redeclareQty(ctx, t, true, 7, tc.qty)
			requireTypeRefusal(ctx, t, env, simple, key, "double_01", tc.evidence)
		})
	}
}

// The EAV-only variant: bigint exports BIGINT where numeric wrote DOUBLE, and
// an EAV bigint has the full int64 domain, so only the fraction is refused.
func TestTypeGuardRefusesRedeclaredEAVOnlyAttribute(t *testing.T) {
	ctx := context.Background()
	env, simple, key := redeclareQty(ctx, t, false, 7, 1000.5)
	requireTypeRefusal(ctx, t, env, simple, key, "", "it holds 1000.5")
}

// Whole values inside the declared domain read unchanged, so the read is
// allowed and matches the oracle.
func TestTypeGuardAdmitsWholeValues(t *testing.T) {
	ctx := context.Background()
	env, simple, _ := redeclareQty(ctx, t, true, 1000, -5, 123456789012)
	result := env.AssertQueryMatches(ctx, Query{Schema: simple, Limit: 20})
	assertUsesDuckDB(t, result)
	if result != nil && result.Total != 3 {
		t.Fatalf("total = %d, want the 3 flushed rows", result.Total)
	}
	env.AssertQueryMatches(ctx, Query{Schema: simple, Filters: []Filter{{Attr: "qty", Op: "gte", Value: "0"}}, Limit: 20})
}

// A manifest entry without a column stamp is judged from the object's footer:
// a stale DOUBLE object beside a current BIGINT one is refused by name.
func TestTypeGuardProbesAnUnstampedManifestEntry(t *testing.T) {
	ctx := context.Background()
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster, WithSchemaDir(writeSimpleSchemaDir(t, qtyProps, qtyAttrs("bigint", true))))
	simple := DefaultSchemaFixtures()[0]
	current := seedQty(ctx, t, env, simple, 1, 2)
	requireParquetCols(t, "bigint generation", describeParquetCols(ctx, t, env, current), map[string]string{"qty": "BIGINT"})
	assertUsesDuckDB(t, env.AssertQueryMatches(ctx, Query{Schema: simple, Limit: 20}))

	stale := buildSchemaKeyPrefix(env, simple) + "stale_qty_zzz.parquet"
	writeParquetViaDuck(ctx, t, env, fmt.Sprintf(
		"SELECT * REPLACE (gen_random_uuid() AS row_id, CAST(1000.5 AS DOUBLE) AS qty) FROM read_parquet('%s') LIMIT 1",
		buildParquetS3Path(env, current)), stale)
	if err := env.RegisterParquetInManifest(ctx, simple, stale, "delta"); err != nil {
		t.Fatalf("register stale parquet: %v", err)
	}
	requireTypeRefusal(ctx, t, env, simple, stale, "double_01", "it holds 1000.5")
}

// Base objects are judged like delta ones: a cdc-init export under numeric,
// read under bigint, is refused by name.
func TestTypeGuardJudgesBaseObjects(t *testing.T) {
	ctx := context.Background()
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster, WithSchemaDir(writeSimpleSchemaDir(t, qtyProps, qtyAttrs("numeric", true))))
	simple := DefaultSchemaFixtures()[0]
	seedGeneration(ctx, t, env, simple, 2, func(_ *rand.Rand, ordinal int, _ bool) map[string]any {
		return map[string]any{"name": fmt.Sprintf("row-%04d", ordinal), "value": float64(ordinal), "qty": 1000.5}
	})
	base := runInitBase(ctx, t, env, simple)
	if err := env.EvolveSchema(ctx, writeSimpleSchemaDir(t, qtyProps, qtyAttrs("bigint", true))); err != nil {
		t.Fatalf("redeclare qty as bigint: %v", err)
	}
	requireTypeRefusal(ctx, t, env, simple, base, "double_01", "it holds 1000.5")
}

// A live object the guard cannot read is not skipped: it takes the #251
// path, so the answer comes from the readable remainder and names the
// excluded object as a partial scan, never as a complete result.
func TestTypeGuardDoesNotSkipAnUnreadableObject(t *testing.T) {
	ctx := context.Background()
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster, WithSchemaDir(writeSimpleSchemaDir(t, qtyProps, qtyAttrs("bigint", true))))
	simple := DefaultSchemaFixtures()[0]
	seedQty(ctx, t, env, simple, 1, 2)

	unreadable := buildSchemaKeyPrefix(env, simple) + "unreadable_zzz.parquet"
	putObjectBytes(ctx, t, env, unreadable, []byte("not a parquet object"))
	if err := env.RegisterParquetInManifest(ctx, simple, unreadable, "delta"); err != nil {
		t.Fatalf("register unreadable parquet: %v", err)
	}
	result, err := env.Query(ctx, Query{Schema: simple, Limit: 20})
	if err != nil {
		t.Fatalf("the readable remainder must answer: %v", err)
	}
	if result.Partial == nil || !slices.ContainsFunc(result.Partial.ExcludedObjects, func(p string) bool {
		return strings.HasSuffix(p, unreadable)
	}) {
		t.Fatalf("the unreadable object must be reported as excluded, got partial %+v", result.Partial)
	}
	if result.Total != 2 {
		t.Errorf("total = %d, want the 2 readable rows", result.Total)
	}
}
