package publication

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestReplacingAGenerationWritesItsRowFromTheRecord is the writer side of
// R9-F2 and R9-F3. The current generation's history row is written outside
// the protocol twice over: it is stamped retired long ago, and it names
// another manifest, digest, token and parent than the record. A
// publication, and a drop, that replace the generation write the row back
// from the record they replaced, so it names what readers obtained from the
// record. Its stamps are written anew: it is superseded by the replacement
// and retired by no one yet, so it stays retained until a collector retires
// it after seeing the replacement committed. The inventory the row named is
// not dropped (R10-F1): the record may be the side that changed, after a
// reader chose the generation through it, so the replacement keeps the
// row's manifest as a displacement, which is forgotten only once the row is
// no longer retained.
func TestReplacingAGenerationWritesItsRowFromTheRecord(t *testing.T) {
	for name, replace := range map[string]func(*testing.T, *fixture, Record){
		"publication": func(t *testing.T, f *fixture, cur Record) {
			_, err := f.publish(context.Background(), cur, f.next(t, cur, KindFlush), nil)
			require.NoError(t, err)
		},
		"drop": func(t *testing.T, f *fixture, cur Record) { f.reset(t, cur) },
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			ctx := context.Background()
			cur := f.initialized(t, 100)
			for range 2 {
				next, err := f.publish(ctx, cur, f.next(t, cur, KindFlush), nil)
				require.NoError(t, err)
				cur = next
			}
			rowToken := uuid.New()
			_, err := f.db.Exec(fmt.Sprintf(`UPDATE %s SET retired_at = $1, superseded_at = 1, manifest_key = 'elsewhere',
				manifest_sha256 = $2, cas_token = $3, parent_generation = 0 WHERE schema_id = 100 AND generation = $4`, f.t.Generation),
				f.serverMillis(t)-time.Hour.Milliseconds(), fmt.Sprintf("%064x", 99), rowToken, cur.Generation)
			require.NoError(t, err)
			expired, err := Expired(ctx, DB(f.db), f.t, 100, time.Minute)
			require.NoError(t, err)
			require.Equal(t, []int64{cur.Generation}, numbers(expired), "the current row looks expired before the replacement")

			before := f.serverMillis(t)
			replace(t, f, cur)

			expired, err = Expired(ctx, DB(f.db), f.t, 100, 0)
			require.NoError(t, err)
			require.Empty(t, expired, "no stamp of the current generation survives its replacement")
			var row Generation
			for _, g := range mustRetained(t, f, time.Minute) {
				if g.Generation == cur.Generation {
					row = g
				}
			}
			require.Equal(t, cur.Generation, row.Generation, "the replaced generation is retained")
			require.Equal(t, []any{cur.Epoch, cur.ParentGeneration, cur.ManifestKey, cur.ManifestSHA256, cur.Token},
				[]any{row.Epoch, row.ParentGeneration, row.ManifestKey, row.ManifestSHA256, row.Token},
				"the row names what the replaced record named")
			require.Zero(t, row.RetiredAt, "retired by no collector yet")
			require.GreaterOrEqual(t, row.SupersededAt, before-StampPrecision.Milliseconds(), "superseded by the replacement")
			displaced, err := Displaced(ctx, DB(f.db), f.t, 100)
			require.NoError(t, err)
			require.Len(t, displaced, 1, "the row's inventory is kept beside the record's")
			d := displaced[0]
			require.Equal(t, []any{cur.Generation, cur.Epoch, "elsewhere", fmt.Sprintf("%064x", 99), rowToken},
				[]any{d.Generation, d.Epoch, d.ManifestKey, d.ManifestSHA256, d.Token}, "the displacement names what the row named")
			require.GreaterOrEqual(t, d.DisplacedAt, before-StampPrecision.Milliseconds())

			n, err := ForgetDisplaced(ctx, DB(f.db), f.t, 100, cur.Generation, time.Minute)
			require.NoError(t, err)
			require.Zero(t, n, "kept while the replaced row is retained")
			ok, err := Retire(ctx, DB(f.db), f.t, 100, cur.Generation)
			require.NoError(t, err)
			require.True(t, ok, "a collector retires it once it saw the replacement")
			n, err = ForgetDisplaced(ctx, DB(f.db), f.t, 100, cur.Generation, time.Minute)
			require.NoError(t, err)
			require.Zero(t, n, "kept for the grace after the retirement")
			_, err = f.db.Exec(fmt.Sprintf("UPDATE %s SET retired_at = retired_at - $1 WHERE schema_id = 100 AND generation = $2",
				f.t.Generation), time.Hour.Milliseconds(), cur.Generation)
			require.NoError(t, err)
			n, err = ForgetDisplaced(ctx, DB(f.db), f.t, 100, cur.Generation, time.Minute)
			require.NoError(t, err)
			require.EqualValues(t, 1, n, "forgotten once the replaced row expired")
		})
	}
}

// TestReplacingAnAgreeingRowDisplacesNothing pins the normal path: a row
// that names what the record names keeps no second inventory, and so fences
// no collection.
func TestReplacingAnAgreeingRowDisplacesNothing(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	cur := f.initialized(t, 100)
	for range 3 {
		next, err := f.publish(ctx, cur, f.next(t, cur, KindFlush), nil)
		require.NoError(t, err)
		cur = next
	}
	f.reset(t, cur)
	displaced, err := Displaced(ctx, DB(f.db), f.t, 100)
	require.NoError(t, err)
	require.Empty(t, displaced)
}
