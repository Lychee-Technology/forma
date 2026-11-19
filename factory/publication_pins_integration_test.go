package factory

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/federated"
	"github.com/lychee-technology/forma/internal/federatedv2"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/publication"
	"github.com/lychee-technology/forma/internal/schemameta"
)

// racingLoader answers meta, after running during first: a drop and
// re-registration that commits while the process loads its metadata.
type racingLoader struct {
	meta   *schemameta.MetadataCache
	during func()
}

func (l *racingLoader) LoadMetadata(context.Context) (*schemameta.MetadataCache, error) {
	if l.during != nil {
		l.during()
	}
	return l.meta, nil
}

// redrop drops schema id and registers it again under the same id: Reset
// then Activate, each in its own transaction as the drop and init-db commit
// them. The record ends two epochs on.
func (w *wiredEngine) redrop(t *testing.T, id int16) {
	t.Helper()
	for _, step := range []func(publication.Tx, publication.Record) (publication.Record, error){
		func(tx publication.Tx, cur publication.Record) (publication.Record, error) {
			return publication.Reset(w.ctx, tx, w.pub, cur)
		},
		func(tx publication.Tx, cur publication.Record) (publication.Record, error) {
			return publication.Activate(w.ctx, tx, w.pub, cur, nil)
		},
	} {
		tx, err := w.pool.Begin(w.ctx)
		require.NoError(t, err)
		snap, err := publication.Read(w.ctx, publication.PgxTx(tx), w.pub, id)
		require.NoError(t, err)
		_, err = step(publication.PgxTx(tx), snap.Record)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(w.ctx))
	}
}

// The factory pins a schema's epoch only when it is the epoch the record had
// before the metadata loaded. A drop and re-registration of schema 7 that
// commits while the metadata loads leaves the process holding the dropped
// schema's metadata and the record at a new epoch: the schema gets no pin,
// and its reads fail closed instead of serving the new schema's rows through
// the old metadata. Schema 7 untouched is pinned and served.
func TestFactoryPinsNoEpochARedropDuringTheMetadataLoadMoved(t *testing.T) {
	for _, raced := range []bool{false, true} {
		name := "metadata loaded undisturbed"
		if raced {
			name = "dropped and registered again during the load"
		}
		t.Run(name, func(t *testing.T) {
			w := newWiredEngine(t)
			meta := schemameta.NewMetadataCache()
			require.NoError(t, meta.RegisterSchema("published_later", unpublishedSchema, forma.SchemaAttributeCache{
				"name": {AttributeID: 1, ValueType: forma.ValueTypeText, ColumnBinding: &forma.MainColumnBinding{ColumnName: "text_01"}},
			}))
			loader := &racingLoader{meta: meta}
			if raced {
				loader.during = func() { w.redrop(t, unpublishedSchema) }
			}
			cfg := forma.DefaultConfig(newMockSchemaRegistry())
			cfg.Database.TableNames = forma.TableNames{EntityMain: w.tables.EntityMain, EAVData: w.tables.EAVData, ChangeLog: w.tables.ChangeLog}
			cfg.DuckDB = duckDBReadConfig()
			cfg.DuckDB.AllowPartialDegradedMode = true

			loaded, epochs, err := loadMetadataWithEpochs(w.ctx, loader, cfg.DuckDB, w.pool, w.tables.EntityMain)
			require.NoError(t, err)
			reader, err := newPublicationReader(w.ctx, publicationReaderSpec{
				duck: cfg.DuckDB, pool: w.pool, meta: loaded, metadataEpochs: epochs, entityMain: w.tables.EntityMain,
			})
			require.NoError(t, err)
			_, engine := newRepositoryAndEngine(w.pool, loaded, cfg, closedDuckDBClient(t), reader)

			page, err := engine.Query(w.ctx, w.tables, query(unpublishedSchema), &model.FederatedQueryOptions{AllowPartialDegradedMode: true})
			if !raced {
				require.NoError(t, err)
				require.Len(t, page.Records, 3)
				return
			}
			require.ErrorIs(t, err, forma.ErrPublicationInconsistent)
			require.ErrorIs(t, err, federatedv2.ErrNotPinned)
			require.NotErrorIs(t, err, federated.ErrFederatedReadFailed)
			require.Zero(t, w.s3.called())
		})
	}
}
