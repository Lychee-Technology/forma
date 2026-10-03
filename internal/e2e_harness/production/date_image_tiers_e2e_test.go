//go:build e2e

package production

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/transform"
)

// e2e_wide's seen is an unbound datetime: eav_data.value_numeric on the hot
// tier, the unified BIGINT column seen on the Parquet tiers.
const (
	seenAttrID          = 10
	pastDateImageMillis = 9007199254740993 // 2^53+1
	dateImageCeiling    = 9007199254740992 // 2^53
	pastDateImageRule   = "is outside the epoch milliseconds a float64 image keeps exactly (up to 9007199254740992, 2^53)"
)

// federatedWideQuery reads every e2e_wide row through the public
// EntityManager on the DuckDB path, whatever tier the rows sit on: the
// explicit tier declaration routes it there (#468), and the manifest names
// the Parquet objects it reads.
func federatedWideQuery(ctx context.Context, env *Env, wide SchemaRef) (*forma.QueryResult, error) {
	return env.EntityManager().Query(ctx, &forma.QueryRequest{
		SchemaName:   wide.Name,
		Page:         1,
		ItemsPerPage: 100,
		Federated: &forma.FederatedQueryRequest{
			Enabled:              true,
			PreferredTiers:       []string{"hot", "warm", "cold"},
			IncludeExecutionPlan: true,
		},
	})
}

// rewriteParquetSeen replaces seen with millis on rowID's row of the Parquet
// object at key, keeping every other column and the key itself, so the
// manifest still names the object. The rewrite goes through a _tmp/ object
// because DuckDB cannot read and overwrite one key in a single COPY.
func rewriteParquetSeen(ctx context.Context, t *testing.T, env *Env, key string, rowID uuid.UUID, millis int64) {
	t.Helper()
	src := fmt.Sprintf("s3://%s/%s", env.Cluster.Bucket, key)
	var seenType string
	if err := env.Duck.DB.QueryRowContext(ctx, fmt.Sprintf("SELECT typeof(seen) FROM read_parquet('%s') LIMIT 1", src)).Scan(&seenType); err != nil {
		t.Fatalf("typeof seen in %s: %v", key, err)
	}
	if seenType != "BIGINT" {
		t.Fatalf("seen in %s is %s, want the unified BIGINT", key, seenType)
	}
	tmp := key[:strings.LastIndex(key, "/")+1] + "_tmp/592-" + uuid.NewString() + ".parquet"
	writeParquetViaDuck(ctx, t, env, fmt.Sprintf(
		"SELECT * REPLACE (CASE WHEN row_id::VARCHAR = '%s' THEN %d::BIGINT ELSE seen END AS seen) FROM read_parquet('%s')",
		rowID, millis, src), tmp)
	putObjectBytes(ctx, t, env, key, fetchObjectBytes(ctx, t, env, tmp))
	deleteObject(ctx, t, env, tmp)
}

// parquetSeen reads rowID's seen straight from the Parquet objects at keys.
func parquetSeen(ctx context.Context, t *testing.T, env *Env, keys []string, rowID uuid.UUID) int64 {
	t.Helper()
	paths := make([]string, len(keys))
	for i, key := range keys {
		paths[i] = fmt.Sprintf("'s3://%s/%s'", env.Cluster.Bucket, key)
	}
	var seen int64
	query := fmt.Sprintf("SELECT seen FROM read_parquet([%s]) WHERE row_id::VARCHAR = '%s'", strings.Join(paths, ", "), rowID)
	if err := env.Duck.DB.QueryRowContext(ctx, query).Scan(&seen); err != nil {
		t.Fatalf("read seen of %s from %v: %v", rowID, keys, err)
	}
	return seen
}

// seedColdWide creates rows and moves them onto one Parquet tier with no hot
// version left: delta through a flush, base through cdc-init. It returns the
// rows and the tier's single object key.
func seedColdWide(ctx context.Context, t *testing.T, env *Env, wide SchemaRef, tier string) ([]*Event, string) {
	t.Helper()
	creates := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 3})
	mustApplyEvents(ctx, t, env, tier+" creates", creates...)
	if tier == "base" {
		if _, err := env.RunInit(ctx, wide); err != nil {
			t.Fatalf("run init: %v", err)
		}
	} else {
		mustFlush(ctx, t, env)
	}
	env.ExecSQL(ctx, "DELETE FROM change_log WHERE schema_id = $1", wide.ID)
	keys := schemaParquetKeys(ctx, t, env, wide)
	if len(keys) != 1 {
		t.Fatalf("expected one %s parquet object, got %v", tier, keys)
	}
	return creates, keys[0]
}

// #592 end to end, Parquet tiers: an unbound datetime whose Parquet BIGINT
// is past 2^53 (a file from an earlier exporter, or edited by hand) reaches
// the reader through the real federated path, manifest to read_parquet to
// attributes_json, as its exact digits and is refused as a read-path
// consistency error naming the attribute, the row and the rule. It never
// reads as the 2^53 instant the former CAST to DOUBLE made of it, and never
// as the caller's invalid input.
func TestUnboundDateParquetImagePast2p53IsAConsistencyError(t *testing.T) {
	cluster := SharedCluster(t)
	for _, tier := range []string{"delta", "base"} {
		t.Run(tier, func(t *testing.T) {
			env := NewEnv(t, cluster)
			ctx := context.Background()
			wide := DefaultSchemaFixtures()[1] // e2e_wide
			creates, key := seedColdWide(ctx, t, env, wide, tier)

			res, err := federatedWideQuery(ctx, env, wide)
			if err != nil {
				t.Fatalf("federated query before the rewrite: %v", err)
			}
			assertFactoryUsedDuckDB(t, res)
			if len(res.Data) != len(creates) {
				t.Fatalf("federated query before the rewrite returned %d rows, want %d", len(res.Data), len(creates))
			}

			target := creates[1].RowID
			rewriteParquetSeen(ctx, t, env, key, target, pastDateImageMillis)
			if got := parquetSeen(ctx, t, env, []string{key}, target); got != pastDateImageMillis {
				t.Fatalf("rewritten %s object holds seen=%d, want %d", tier, got, int64(pastDateImageMillis))
			}

			_, err = federatedWideQuery(ctx, env, wide)
			if err == nil {
				t.Fatalf("federated read of a %s seen image past 2^53 succeeded", tier)
			}
			if errors.Is(err, forma.ErrInvalidInput) {
				t.Fatalf("a stored date image is the operator's, not the caller's: %v", err)
			}
			for _, want := range []string{"'seen'", target.String(), fmt.Sprintf("stored value %d", int64(pastDateImageMillis)), pastDateImageRule} {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("expected the error to name %q, got %v", want, err)
				}
			}
		})
	}
}

// seenOf returns rowID's seen from a public query answer.
func seenOf(t *testing.T, res *forma.QueryResult, rowID uuid.UUID) time.Time {
	t.Helper()
	for _, record := range res.Data {
		if record.RowID != rowID {
			continue
		}
		seen, ok := record.Attributes["seen"].(time.Time)
		if !ok {
			t.Fatalf("row %s answered seen=%#v, want a time.Time", rowID, record.Attributes["seen"])
		}
		return seen
	}
	t.Fatalf("query answer has no row %s", rowID)
	return time.Time{}
}

// #592 residual, owned by #621: the DuckDB Postgres scanner types an
// unconstrained NUMERIC as DOUBLE upstream of every expression Forma
// renders, so a seen image of 2^53+1 planted in eav_data reads as the 2^53
// instant through the federated hot leg, and through the delta image the
// CDC export writes from it, while the Postgres direct read refuses it. The
// validate-schema-consistency census is the guard for those tiers: it flags
// the row before and after the flush. This pins both halves, so #621's
// exact transport flips the federated assertions on purpose, not by
// accident.
func TestUnboundDateHotImagePast2p53Residual(t *testing.T) {
	env := NewEnv(t, SharedCluster(t))
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1] // e2e_wide

	// A flushed filler row, so the federated read has a manifest to resolve.
	mustApplyEvents(ctx, t, env, "filler create", env.GenerateScript(ScriptSpec{Schema: wide, Creates: 1})...)
	mustFlush(ctx, t, env)
	planted := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 1})
	mustApplyEvents(ctx, t, env, "planted create", planted...)
	target := planted[0].RowID
	env.ExecSQL(ctx, fmt.Sprintf("UPDATE %s SET value_numeric = %d WHERE schema_id = $1 AND row_id = $2 AND attr_id = $3 AND array_indices = ''",
		env.Tables.EAVData, int64(pastDateImageMillis)), wide.ID, target, seenAttrID)

	assertCensusFlagsSeen(ctx, t, env, wide, target)
	_, err := env.EntityManager().Get(ctx, &forma.QueryRequest{SchemaName: wide.Name, RowID: &target})
	if err == nil || errors.Is(err, forma.ErrInvalidInput) || !strings.Contains(err.Error(), pastDateImageRule) {
		t.Fatalf("Postgres direct read of a planted seen=2^53+1: want the read consistency error, got %v", err)
	}

	ceiling := time.UnixMilli(dateImageCeiling).UTC()
	res, err := federatedWideQuery(ctx, env, wide)
	if err != nil {
		t.Fatalf("federated query over the hot planted row: %v", err)
	}
	assertFactoryUsedDuckDB(t, res)
	if got := seenOf(t, res, target); !got.Equal(ceiling) {
		t.Fatalf("federated hot leg read seen=%v; the #621 residual is %v (if #621 landed, flip this pin)", got, ceiling)
	}

	mustFlush(ctx, t, env)
	if got := parquetSeen(ctx, t, env, schemaParquetKeys(ctx, t, env, wide), target); got != dateImageCeiling {
		t.Fatalf("the CDC export wrote seen=%d for the planted row; the #621 residual is %d", got, int64(dateImageCeiling))
	}
	assertCensusFlagsSeen(ctx, t, env, wide, target)
	res, err = federatedWideQuery(ctx, env, wide)
	if err != nil {
		t.Fatalf("federated query over the flushed planted row: %v", err)
	}
	if got := seenOf(t, res, target); !got.Equal(ceiling) {
		t.Fatalf("federated delta read seen=%v; the #621 residual is %v (if #621 landed, flip this pin)", got, ceiling)
	}
}

// assertCensusFlagsSeen evaluates the validate-schema-consistency date
// predicate on rowID's stored seen row.
func assertCensusFlagsSeen(ctx context.Context, t *testing.T, env *Env, wide SchemaRef, rowID uuid.UUID) {
	t.Helper()
	var flagged bool
	query := fmt.Sprintf("SELECT %s FROM %s WHERE schema_id = $1 AND row_id = $2 AND attr_id = $3 AND array_indices = ''",
		transform.StoredDateImageRefusedSQL("value_numeric"), env.Tables.EAVData)
	if err := env.Pool.QueryRow(ctx, query, wide.ID, rowID, seenAttrID).Scan(&flagged); err != nil {
		t.Fatalf("census predicate over seen of %s: %v", rowID, err)
	}
	if !flagged {
		t.Fatalf("the census does not flag seen of %s, whose image the Postgres read refuses", rowID)
	}
}
