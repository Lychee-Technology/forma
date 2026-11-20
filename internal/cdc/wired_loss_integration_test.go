package cdc

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/cdcv2"
	"github.com/lychee-technology/forma/internal/publication"
)

// loseAndRecover deletes the schema's record row, then recovers it as
// init-db does for an id it registers again: DROPPED above the id's
// lifecycle epoch (publication.InitializeAfterLoss), then activated.
func (f *wiredFixture) loseAndRecover(t *testing.T) publication.Record {
	t.Helper()
	ctx := context.Background()
	_, err := f.pool.Exec(ctx, "DELETE FROM "+f.tables.Record+" WHERE schema_id = $1", wiredSchema)
	require.NoError(t, err)
	var active publication.Record
	f.inTx(t, func(tx pgx.Tx) error {
		h, err := publication.ReadHistory(ctx, publication.PgxTx(tx), f.tables, wiredSchema)
		if err != nil {
			return err
		}
		dropped, wrote, err := publication.InitializeAfterLoss(ctx, publication.PgxTx(tx), f.tables, wiredSchema, h, publication.Dropped)
		if err != nil {
			return err
		}
		require.True(t, wrote)
		active, err = publication.Activate(ctx, publication.PgxTx(tx), f.tables, dropped, nil)
		return err
	})
	return active
}

// A cdc-flush or cdc-init that read the epochs (3, after a drop and a
// re-registration) and loaded its registry before the record was lost holds
// metadata of epoch 3. The lost record's generation history, when it has
// one, names epoch 1 only: recovered at that epoch + 1 and activated, the
// record would be back at 3 and the writer would export and publish under
// the metadata it holds. Recovered after the id's lifecycle epoch, it is at
// 5, and the writer refuses the schema before any export: nothing staged,
// the record unchanged, every entry still dirty.
func TestWiredWritersRefuseAPinTakenBeforeARecordWasLost(t *testing.T) {
	type writer func(f *wiredFixture, mods ...func(*CDCConfig)) error
	writers := map[string]writer{
		"flush": func(f *wiredFixture, mods ...func(*CDCConfig)) error {
			_, err := f.flush(t, nil, false, mods...)
			return err
		},
		"init": func(f *wiredFixture, mods ...func(*CDCConfig)) error {
			_, err := f.runInit(t, nil, false, mods...)
			return err
		},
	}
	for name, run := range writers {
		for _, published := range []bool{true, false} {
			label := name + "/without generation history"
			if published {
				label = name + "/with generation history"
			}
			t.Run(label, func(t *testing.T) {
				f := newWiredFixture(t)
				f.register(t, publication.Unpublished)
				if published {
					f.seed(t, 2, time.Now().UnixMilli()-60_000)
					_, err := f.flush(t, nil, false)
					require.NoError(t, err)
				}
				f.redrop(t)
				epochs, err := ReadRegistryEpochs(context.Background(), f.db, f.cfg.EntityMainTable)
				require.NoError(t, err)
				require.Equal(t, int64(3), epochs[wiredSchema], "the writer's pin: dropped, then registered again")
				pinned := func(c *CDCConfig) { c.RegistryEpochs = epochs }

				recovered := f.loseAndRecover(t)
				require.Equal(t, int64(5), recovered.Epoch, "recovered DROPPED at 4 above the lifecycle epoch 3, then activated")
				f.seed(t, 2, time.Now().UnixMilli()-30_000)
				dirty := f.dirty(t)
				parquetBefore, manifestsBefore := f.objects(t)

				err = run(f, pinned)
				require.ErrorIs(t, err, cdcv2.ErrEpochChanged)
				require.ErrorContains(t, err, "the run's schema metadata was loaded at epoch 3")
				rec := mustRecord(t, f)
				require.Equal(t, recovered.Epoch, rec.Epoch)
				require.Equal(t, publication.Unpublished, rec.State)
				parquet, manifests := f.objects(t)
				require.Equal(t, [2]int{parquetBefore, manifestsBefore}, [2]int{parquet, manifests}, "nothing staged")
				require.Equal(t, dirty, f.dirty(t))
			})
		}
	}
}
