package cdc

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/cdcv2"
	"github.com/lychee-technology/forma/internal/publication"
)

// racingRegistry answers the stub registry's metadata, and runs during, once,
// when the run resolves a schema's attributes: a drop and re-registration
// that commits after the run has the metadata and before it reads the record
// under the schema lock.
type racingRegistry struct {
	stubSchemaRegistry
	once   sync.Once
	during func()
}

func (r *racingRegistry) GetSchemaAttributeCacheByID(id int16) (string, forma.SchemaAttributeCache, error) {
	if r.during != nil {
		r.once.Do(r.during)
	}
	return r.stubSchemaRegistry.GetSchemaAttributeCacheByID(id)
}

// redrop drops the schema and registers it again under its id: Reset, then
// Activate, each in its own transaction as the drop and init-db commit them.
// The record ends two epochs on.
func (f *wiredFixture) redrop(t *testing.T) {
	t.Helper()
	require.NoError(t, f.drop(context.Background()))
	f.inTx(t, func(tx pgx.Tx) error {
		snap, err := publication.Read(context.Background(), publication.PgxTx(tx), f.tables, wiredSchema)
		if err != nil {
			return err
		}
		_, err = publication.Activate(context.Background(), publication.PgxTx(tx), f.tables, snap.Record, nil)
		return err
	})
}

// A drop and re-registration that commits between the run loading the
// schema's metadata and reading its record under the schema lock leaves the
// run holding the dropped schema's metadata and the record at a new epoch.
// cdc-flush and cdc-init refuse the schema with cdcv2.ErrEpochChanged before
// any export: no object or manifest is staged, the record is the
// re-registration's, and every entry stays dirty. The epochs bound to the
// metadata are the run's own reading, before it resolves the attributes, or
// those the caller read before loading its registry (RegistryEpochs).
func TestWiredWritersRefuseMetadataOfAnEpochARedropMoved(t *testing.T) {
	type writer func(f *wiredFixture, reg forma.SchemaRegistry, mods ...func(*CDCConfig)) error
	writers := map[string]writer{
		"flush": func(f *wiredFixture, reg forma.SchemaRegistry, mods ...func(*CDCConfig)) error {
			_, err := f.flush(t, reg, false, mods...)
			return err
		},
		"init": func(f *wiredFixture, reg forma.SchemaRegistry, mods ...func(*CDCConfig)) error {
			_, err := f.runInit(t, reg, false, mods...)
			return err
		},
	}
	for name, run := range writers {
		for _, supplied := range []bool{false, true} {
			label := name + "/epochs read by the run"
			if supplied {
				label = name + "/epochs read before the registry loaded"
			}
			t.Run(label, func(t *testing.T) {
				f := newWiredFixture(t)
				f.register(t, publication.Unpublished)
				f.seed(t, 2, time.Now().UnixMilli()-60_000)
				dirty := f.dirty(t)
				var mods []func(*CDCConfig)
				reg := &racingRegistry{stubSchemaRegistry: stubSchemaRegistry{cache: testAttrCache()}}
				if supplied {
					epochs, err := publication.Epochs(context.Background(), publication.DB(f.db), f.tables)
					require.NoError(t, err)
					mods = append(mods, func(c *CDCConfig) { c.RegistryEpochs = epochs })
					f.redrop(t) // after the caller read the epochs and loaded its registry
				} else {
					reg.during = func() { f.redrop(t) }
				}

				err := run(f, reg, mods...)
				require.ErrorIs(t, err, cdcv2.ErrEpochChanged)
				require.ErrorContains(t, err, "the run's schema metadata was loaded at epoch 1")
				rec := mustRecord(t, f)
				require.Equal(t, int64(3), rec.Epoch, "dropped, then registered again")
				require.Equal(t, publication.Unpublished, rec.State)
				parquet, manifests := f.objects(t)
				require.Zero(t, parquet+manifests, "nothing staged")
				require.Equal(t, dirty, f.dirty(t))
			})
		}
	}
}
