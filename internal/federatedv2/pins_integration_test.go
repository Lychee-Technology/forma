package federatedv2

import (
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/publication"
)

// LoadEpochPins pins each schema with a record at the record's epoch, gives a
// schema without one no pin, and pins a row the read refuses at the epoch it
// holds: each of the last two fails every read closed.
func TestLoadEpochPinsPinsTheRecordsTheSnapshotHolds(t *testing.T) {
	f := newFixture(t)
	f.initialize(t, publication.Unpublished)
	const invalidID, missingID int16 = schemaID + 1, schemaID + 2
	// A copy of the record at another schema and epoch, turned PUBLISHED
	// without a manifest: a row no transition writes.
	_, err := f.pool.Exec(f.ctx, "INSERT INTO "+f.pub.Record+
		" SELECT $1, 4, $2, 4, parent_generation, manifest_key, manifest_sha256, lineage, cas_token, updated_at, gc_horizon FROM "+
		f.pub.Record+" WHERE schema_id = $3", invalidID, string(publication.Published), schemaID)
	require.NoError(t, err)

	tx, err := f.pool.BeginTx(f.ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(f.ctx) }()
	pins, err := LoadEpochPins(f.ctx, tx, f.tables.EntityMain, []int16{schemaID, invalidID, missingID})
	require.NoError(t, err)
	require.Equal(t, map[int16]int64{schemaID: f.record(t).Epoch, invalidID: 4}, pins)
}

// A snapshot that cannot read the publication tables refuses the pins: a
// process must not start a reader that would fail every read.
func TestLoadEpochPinsRefusesAMissingPublicationTable(t *testing.T) {
	f := newFixture(t)
	tx, err := f.pool.BeginTx(f.ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(f.ctx) }()
	_, err = LoadEpochPins(f.ctx, tx, "no_such_entity_main", []int16{schemaID})
	require.ErrorContains(t, err, "pin the publication epoch of schema 7")
}
