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

const bigintDestSchema = 302

// bigintDestinationRegistry binds one bigint attribute to each destination
// that keeps only the float64 image (#590): eav_data value_numeric (scalar
// and list item) and a double_* column; plus a bigint column, which keeps
// the exact int64.
func bigintDestinationRegistry() forma.SchemaRegistry {
	return &stubSchemaRegistry{schemaID: bigintDestSchema, schemaName: "bigint_dest", cache: forma.SchemaAttributeCache{
		"total":  {AttributeID: 30, ValueType: forma.ValueTypeBigInt},
		"totals": {AttributeID: 31, ValueType: forma.ValueTypeList, ItemsType: forma.ValueTypeBigInt},
		"ratio":  {AttributeID: 32, ValueType: forma.ValueTypeBigInt, ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnDouble01}},
		"amount": {AttributeID: 33, ValueType: forma.ValueTypeBigInt, ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnBigint01}},
	}}
}

// eavBigintFixture is one physical variant of the eav_data table: the unit
// DDL's DOUBLE PRECISION value_numeric and production's NUMERIC (#205).
type eavBigintFixture struct {
	pool   *pgxpool.Pool
	tables model.StorageTables
	repo   *DBPersistentRecordRepository
	tr     model.PersistentRecordTransformer
	ctx    context.Context
}

func newEAVBigintFixture(t *testing.T, ctx context.Context, pool *pgxpool.Pool, numeric bool) *eavBigintFixture {
	t.Helper()
	tables := createTempPersistentTables(t, ctx, pool)
	if numeric {
		_, err := pool.Exec(ctx, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN value_numeric TYPE NUMERIC", sanitizeIdentifier(tables.EAVData)))
		require.NoError(t, err)
	}
	return &eavBigintFixture{pool: pool, tables: tables, repo: NewDBPersistentRecordRepository(pool, nil), tr: transform.NewPersistentRecordTransformer(bigintDestinationRegistry()), ctx: ctx}
}

// eavImage returns the stored value_numeric of attr for row as Postgres
// prints it, and as the float64 the read path decodes.
func (f *eavBigintFixture) eavImage(t *testing.T, rowID uuid.UUID, attrID int16, indices string) (string, float64) {
	t.Helper()
	var text string
	var num float64
	err := f.pool.QueryRow(f.ctx, fmt.Sprintf("SELECT value_numeric::text, value_numeric::float8 FROM %s WHERE schema_id = $1 AND row_id = $2 AND attr_id = $3 AND array_indices = $4", sanitizeIdentifier(f.tables.EAVData)), bigintDestSchema, rowID, attrID, indices).Scan(&text, &num)
	require.NoError(t, err)
	return text, num
}

func (f *eavBigintFixture) write(t *testing.T, attrs map[string]any) uuid.UUID {
	t.Helper()
	rowID := uuid.New()
	record, err := f.tr.ToPersistentRecord(f.ctx, bigintDestSchema, rowID, attrs)
	require.NoError(t, err)
	require.NoError(t, f.repo.InsertPersistentRecord(f.ctx, f.tables, record))
	return rowID
}

func (f *eavBigintFixture) read(t *testing.T, rowID uuid.UUID) (map[string]any, error) {
	t.Helper()
	stored, err := f.repo.GetPersistentRecord(f.ctx, f.tables, bigintDestSchema, rowID)
	require.NoError(t, err)
	return f.tr.FromPersistentRecord(f.ctx, stored)
}

// plant overwrites the stored image of attr for row the way a write from
// before #590 (or one around the funnel) left it.
func (f *eavBigintFixture) plant(t *testing.T, rowID uuid.UUID, attrID int16, image string) {
	t.Helper()
	tag, err := f.pool.Exec(f.ctx, fmt.Sprintf("UPDATE %s SET value_numeric = %s WHERE schema_id = $1 AND row_id = $2 AND attr_id = $3", sanitizeIdentifier(f.tables.EAVData), image), bigintDestSchema, rowID, attrID)
	require.NoError(t, err)
	require.EqualValues(t, 1, tag.RowsAffected())
}

// Physical fidelity of the image destinations (#590): every bigint within
// the float64 image's exact range, |v| <= 2^53, comes back from the real
// eav_data table (scalar and list item) and from a double_* column as the
// same value on both value_numeric column types, and its stored decimal
// image is the value itself. Every value past that range is refused as
// invalid input before anything is written, in every spelling that used to
// slip through on the strength of the exact sidecar, while a bigint column
// keeps the full int64 range.
func TestEAVBigintRoundTripIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool := testdb.Connect(t, ctx)
	exact := []int64{9007199254740992, -9007199254740992, 9007199254740991, -9007199254740991, 1 << 40, 42, 0, -1}
	refused := []any{
		int64(9007199254740993), "9007199254740993", int64(-9007199254740993),
		int64(math.MaxInt64), "9223372036854775807", int64(math.MinInt64), "-9223372036854775808",
		int64(9223372036854775295), float64(1e18), math.Ldexp(1, 54),
	}

	for _, numeric := range []bool{false, true} {
		f := newEAVBigintFixture(t, ctx, pool, numeric)
		name := map[bool]string{false: "double precision", true: "numeric"}[numeric]
		for _, v := range exact {
			t.Run(fmt.Sprintf("%s exact %d", name, v), func(t *testing.T) {
				rowID := f.write(t, map[string]any{"total": v, "totals": []any{v, strconv.FormatInt(v, 10)}, "ratio": v, "amount": v})
				got, err := f.read(t, rowID)
				require.NoError(t, err)
				require.Equal(t, v, got["total"])
				require.Equal(t, []any{v, v}, got["totals"])
				require.Equal(t, v, got["ratio"])
				require.Equal(t, v, got["amount"])
				for _, slot := range []struct {
					attrID  int16
					indices string
				}{{30, ""}, {31, "0"}, {31, "1"}} {
					text, num := f.eavImage(t, rowID, slot.attrID, slot.indices)
					require.Equal(t, float64(v), num)
					if numeric {
						require.Equal(t, strconv.FormatInt(v, 10), text, "decimal image of attr %d[%s]", slot.attrID, slot.indices)
					}
				}
			})
		}
		for _, v := range refused {
			for _, attr := range []string{"total", "totals", "ratio"} {
				t.Run(fmt.Sprintf("%s refused %s %v(%T)", name, attr, v, v), func(t *testing.T) {
					in := v
					if attr == "totals" {
						in = []any{int64(1), v}
					}
					_, err := f.tr.ToPersistentRecord(ctx, bigintDestSchema, uuid.New(), map[string]any{attr: in})
					require.ErrorIs(t, err, forma.ErrInvalidInput)
					msg, published := forma.ResolvePublicMessage(err)
					require.True(t, published)
					require.Contains(t, msg, "float64 image")
					require.Contains(t, msg, "allowed [-9007199254740992, 9007199254740992]")
				})
			}
		}
	}

	f := newEAVBigintFixture(t, ctx, pool, true)
	for _, v := range []int64{math.MaxInt64, math.MinInt64, 9007199254740993, -9007199254740993} {
		t.Run(fmt.Sprintf("bigint column %d", v), func(t *testing.T) {
			rowID := f.write(t, map[string]any{"amount": v})
			got, err := f.read(t, rowID)
			require.NoError(t, err)
			require.Equal(t, v, got["amount"])
		})
	}
}

// The read side of a stored image (#590): a whole image inside int64 that
// the funnel no longer admits (a row written before this change) still reads
// as the value the table holds, and the #501 census is what names it. An
// image that names no int64, a fraction or a magnitude int64() would wrap,
// is refused on the read as a consistency error that names the attribute
// and the row, never returned as a made-up value.
func TestEAVBigintLegacyImageReadIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool := testdb.Connect(t, ctx)

	for _, numeric := range []bool{false, true} {
		f := newEAVBigintFixture(t, ctx, pool, numeric)
		name := map[bool]string{false: "double precision", true: "numeric"}[numeric]

		t.Run(name+" legacy image past 2^53 reads as stored", func(t *testing.T) {
			rowID := f.write(t, map[string]any{"total": int64(1)})
			f.plant(t, rowID, 30, "9007199254740994")
			got, err := f.read(t, rowID)
			require.NoError(t, err)
			require.Equal(t, int64(9007199254740994), got["total"])
		})

		for _, tc := range []struct{ image, want string }{
			{"9223372036854775808", "stored bigint image 9223372036854775808 is outside the bigint range"},
			{"1000.5", "stored bigint image 1000.5 is not a whole number"},
		} {
			t.Run(fmt.Sprintf("%s planted %s refused on read", name, tc.image), func(t *testing.T) {
				rowID := f.write(t, map[string]any{"total": int64(1)})
				f.plant(t, rowID, 30, tc.image)
				_, err := f.read(t, rowID)
				require.Error(t, err)
				require.NotErrorIs(t, err, forma.ErrInvalidInput, "a read-path consistency error is not a 4xx")
				require.Contains(t, err.Error(), tc.want)
				require.Contains(t, err.Error(), "total")
				require.Contains(t, err.Error(), rowID.String())
			})
		}
	}
}
