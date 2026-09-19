package internal

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/testdb"
	"github.com/lychee-technology/forma/internal/transform"
)

// dateDestinationRegistry binds one date attribute to each physical
// destination the write funnel can choose (#582): eav_data value_numeric
// (unbound), a bigint column under unix_ms and under the default encoding,
// and a text column under iso8601.
func dateDestinationRegistry() forma.SchemaRegistry {
	return &stubSchemaRegistry{schemaID: 301, schemaName: "date_dest", cache: forma.SchemaAttributeCache{
		"seenAt":    {AttributeID: 20, ValueType: forma.ValueTypeDateTime},
		"bornOn":    {AttributeID: 21, ValueType: forma.ValueTypeDate},
		"flushedAt": {AttributeID: 22, ValueType: forma.ValueTypeDateTime, ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnBigint01, Encoding: forma.MainColumnEncodingUnixMs}},
		"openedAt":  {AttributeID: 23, ValueType: forma.ValueTypeDateTime, ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnBigint02}},
		"expiresAt": {AttributeID: 24, ValueType: forma.ValueTypeDateTime, ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnText01, Encoding: forma.MainColumnEncodingISO8601}},
	}}
}

// eavDateFixture is one physical variant of the eav_data table: the unit
// DDL's DOUBLE PRECISION value_numeric and production's NUMERIC (#205).
type eavDateFixture struct {
	pool   *pgxpool.Pool
	tables model.StorageTables
	repo   *DBPersistentRecordRepository
	tr     model.PersistentRecordTransformer
	ctx    context.Context
}

func newEAVDateFixture(t *testing.T, ctx context.Context, pool *pgxpool.Pool, numeric bool) *eavDateFixture {
	t.Helper()
	tables := createTempPersistentTables(t, ctx, pool)
	if numeric {
		_, err := pool.Exec(ctx, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN value_numeric TYPE NUMERIC", sanitizeIdentifier(tables.EAVData)))
		require.NoError(t, err)
	}
	return &eavDateFixture{pool: pool, tables: tables, repo: NewDBPersistentRecordRepository(pool, nil), tr: transform.NewPersistentRecordTransformer(dateDestinationRegistry()), ctx: ctx}
}

// eavImage returns the stored value_numeric of attr for row as Postgres
// prints it, and as the float64 the read path decodes.
func (f *eavDateFixture) eavImage(t *testing.T, rowID uuid.UUID, attrID int16) (string, float64) {
	t.Helper()
	var text string
	var num float64
	err := f.pool.QueryRow(f.ctx, fmt.Sprintf("SELECT value_numeric::text, value_numeric::float8 FROM %s WHERE schema_id = 301 AND row_id = $1 AND attr_id = $2", sanitizeIdentifier(f.tables.EAVData)), rowID, attrID).Scan(&text, &num)
	require.NoError(t, err)
	return text, num
}

func (f *eavDateFixture) writeAndRead(t *testing.T, attrs map[string]any) (uuid.UUID, map[string]any) {
	t.Helper()
	rowID := uuid.New()
	record, err := f.tr.ToPersistentRecord(f.ctx, 301, rowID, attrs)
	require.NoError(t, err)
	require.NoError(t, f.repo.InsertPersistentRecord(f.ctx, f.tables, record))
	stored, err := f.repo.GetPersistentRecord(f.ctx, f.tables, 301, rowID)
	require.NoError(t, err)
	got, err := f.tr.FromPersistentRecord(f.ctx, stored)
	require.NoError(t, err)
	return rowID, got
}

// Physical fidelity of the unbound destination: every epoch-ms value within
// the float64 image's exact range (|ms| <= 2^53, #205) comes back from the
// real eav_data table as the same instant, on both value_numeric column
// types, and its stored decimal image is the value itself. The rule for an
// image past that range, on the write and on every read route, is #592.
func TestEAVDateRoundTripIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool := testdb.Connect(t, ctx)
	exact := []int64{1704067200123, 0, -1, -62135596800000, 9007199254740991, -9007199254740991, 9007199254740992, -9007199254740992}
	plus14 := time.FixedZone("plus14", 14*3600)

	for _, numeric := range []bool{false, true} {
		f := newEAVDateFixture(t, ctx, pool, numeric)
		name := map[bool]string{false: "double precision", true: "numeric"}[numeric]
		for _, ms := range exact {
			t.Run(fmt.Sprintf("%s exact %d", name, ms), func(t *testing.T) {
				want := time.UnixMilli(ms).UTC()
				rowID, got := f.writeAndRead(t, map[string]any{"seenAt": want.In(plus14), "bornOn": strconv.FormatInt(ms, 10)})
				require.Equal(t, want, got["seenAt"])
				require.Equal(t, want, got["bornOn"])
				for _, attrID := range []int16{20, 21} {
					text, num := f.eavImage(t, rowID, attrID)
					require.Equal(t, float64(ms), num)
					if numeric {
						require.Equal(t, strconv.FormatInt(ms, 10), text, "decimal image of attr %d", attrID)
					}
				}
			})
		}
	}
}

// Physical fidelity of the bound destinations (#582): a bigint column under
// unix_ms or the default encoding keeps the full int64 range exactly, and an
// iso8601 text column keeps whole-second instants from year 0000 to 9999 as
// a canonical UTC image, whatever offset the caller wrote.
func TestBoundDateRoundTripIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool := testdb.Connect(t, ctx)
	f := newEAVDateFixture(t, ctx, pool, true)
	plus14 := time.FixedZone("plus14", 14*3600)

	for _, ms := range []int64{math.MaxInt64, math.MinInt64, 9007199254740993, -9007199254740993, 1704067200123, -1, -62135596800000} {
		t.Run(fmt.Sprintf("bigint %d", ms), func(t *testing.T) {
			want := time.UnixMilli(ms).UTC()
			rowID, got := f.writeAndRead(t, map[string]any{"flushedAt": want.In(plus14), "openedAt": strconv.FormatInt(ms, 10)})
			require.Equal(t, want, got["flushedAt"])
			require.Equal(t, want, got["openedAt"])
			var b1, b2 int64
			require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf("SELECT bigint_01, bigint_02 FROM %s WHERE ltbase_row_id = $1", sanitizeIdentifier(f.tables.EntityMain)), rowID).Scan(&b1, &b2))
			require.Equal(t, ms, b1)
			require.Equal(t, ms, b2)
		})
	}

	for _, tc := range []struct{ in, image string }{
		{"0000-01-01T00:00:00Z", "0000-01-01T00:00:00Z"},
		{"9999-12-31T23:59:59Z", "9999-12-31T23:59:59Z"},
		{"2024-01-01T02:00:00+02:00", "2024-01-01T00:00:00Z"},
		{"1969-12-31T23:59:59-05:00", "1970-01-01T04:59:59Z"},
		{"0001-01-01T00:00:00+14:00", "0000-12-31T10:00:00Z"},
	} {
		t.Run("iso8601 "+tc.in, func(t *testing.T) {
			want, err := time.Parse(time.RFC3339, tc.image)
			require.NoError(t, err)
			rowID, got := f.writeAndRead(t, map[string]any{"expiresAt": tc.in})
			require.Equal(t, want, got["expiresAt"])
			var text string
			require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf("SELECT text_01 FROM %s WHERE ltbase_row_id = $1", sanitizeIdentifier(f.tables.EntityMain)), rowID).Scan(&text))
			require.Equal(t, tc.image, text)
		})
	}

	for _, in := range []string{"2024-01-01T00:00:00.001Z", "10000-01-01T00:00:00Z", "-0001-12-31T23:59:59Z", "1704067200123"} {
		t.Run("iso8601 refused "+in, func(t *testing.T) {
			_, err := f.tr.ToPersistentRecord(ctx, 301, uuid.New(), map[string]any{"expiresAt": in})
			require.ErrorIs(t, err, forma.ErrInvalidInput)
		})
	}
}
