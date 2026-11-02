package cdc

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/sqlutil"
	"github.com/lychee-technology/forma/internal/testdb"
	"github.com/stretchr/testify/require"
)

// legacyPostgresQueryTransport is the export's transport before #785, kept
// only as the reference the pgx staging is compared against: row ids spliced
// into the PostgreSQL text as UUID literals (FALSE for a zero-row base
// export, #519), and every result read by DuckDB through postgres_query in a
// MATERIALIZED CTE (#752). The projection, casts and COPY are production's.
func legacyPostgresQueryTransport(rowIDs []uuid.UUID) exportTransport {
	literals := make([]string, len(rowIDs))
	for i, id := range rowIDs {
		literals[i] = fmt.Sprintf("UUID '%s'", id)
	}
	return exportTransport{
		rowFilter: func(column string) string {
			if len(rowIDs) == 0 {
				return "FALSE"
			}
			return fmt.Sprintf("%s IN (%s)", column, strings.Join(literals, ", "))
		},
		source: func(stage exportStage) string {
			return fmt.Sprintf("%s AS MATERIALIZED (SELECT * FROM postgres_query('pg_db', '%s'))",
				stage.alias, sqlutil.EscapeLiteral(stage.query))
		},
	}
}

// parityFixture is one PostgreSQL schema holding the full-width CDC tables,
// seeded with the value shapes the export has to carry unchanged.
type parityFixture struct {
	cfg CDCConfig
	db  *sql.DB
	dsn string
	ids []uuid.UUID // parityRowCount rows, see seedParityRows
}

const (
	paritySchemaID = 7
	paritySnapshot = 5000
	parityRowCount = 10
)

func newParityFixture(t *testing.T, ctx context.Context) parityFixture {
	t.Helper()
	pool := testdb.Connect(t, ctx)
	schema := "cdc_parity_" + strings.ReplaceAll(uuid.NewString()[:8], "-", "")
	qs := pgx.Identifier{schema}.Sanitize()
	ids := make([]uuid.UUID, parityRowCount)
	for i := range ids {
		ids[i] = uuid.Must(uuid.NewV7())
	}
	stmts := append([]string{
		"CREATE SCHEMA " + qs,
		"CREATE TABLE " + qs + ".change_log (schema_id smallint NOT NULL, row_id uuid NOT NULL, flushed_at bigint NOT NULL DEFAULT 0, changed_at bigint NOT NULL, deleted_at bigint, PRIMARY KEY (schema_id, row_id, flushed_at))",
		"CREATE TABLE " + qs + ".entity_main (ltbase_schema_id smallint NOT NULL, ltbase_row_id uuid NOT NULL, " +
			"text_01 text, text_02 text, text_03 text, smallint_01 smallint, smallint_02 smallint, smallint_03 smallint, " +
			"integer_01 integer, bigint_01 bigint, bigint_02 bigint, double_01 double precision, uuid_01 uuid, " +
			"ltbase_created_at bigint NOT NULL, ltbase_updated_at bigint NOT NULL, ltbase_deleted_at bigint, " +
			"PRIMARY KEY (ltbase_schema_id, ltbase_row_id))",
		"CREATE TABLE " + qs + ".eav_data (schema_id smallint NOT NULL, row_id uuid NOT NULL, attr_id smallint NOT NULL, array_indices text NOT NULL DEFAULT '', value_text text, value_numeric numeric, PRIMARY KEY (schema_id, row_id, attr_id, array_indices))",
	}, seedParityRows(qs, ids)...)
	for _, stmt := range stmts {
		_, err := pool.Exec(ctx, stmt)
		require.NoError(t, err, stmt)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DROP SCHEMA "+qs+" CASCADE")
	})

	db := stdlib.OpenDBFromPool(pool)
	t.Cleanup(func() { _ = db.Close() })
	cc := pool.Config().ConnConfig
	return parityFixture{
		cfg: CDCConfig{
			ChangeLogTable:  schema + ".change_log",
			EntityMainTable: schema + ".entity_main",
			EAVDataTable:    schema + ".eav_data",
		},
		db:  db,
		dsn: BuildPGDSN(PGDSNParams{Host: cc.Host, Port: int(cc.Port), User: cc.User, Password: cc.Password, DB: cc.Database, SSLMode: "disable"}),
		ids: ids,
	}
}

// seedParityRows writes ten rows of schema 7:
//
//	ids[0..5] live rows: every bound column set (0), all NULL (1), and
//	          edge values: integer widths, bool spellings, iso8601 dates,
//	          LIST shapes (elements out of index order, the empty-list
//	          marker, a legacy scalar row) and NUMERIC NaN, ±Infinity,
//	          past the double range and below it
//	ids[6]    soft-deleted (in the delta, not in the base)
//	ids[7]    hard-deleted: a change_log tombstone without main or EAV rows
//	ids[8]    already flushed, ids[9] changed after the snapshot: neither
//	          is in the delta
//
// ids[0] also has an older flushed change_log version and an EAV row under
// another schema id, which no export may pick up.
func seedParityRows(qs string, ids []uuid.UUID) []string {
	main := func(i int, values string) string {
		return fmt.Sprintf("INSERT INTO %s.entity_main (ltbase_schema_id, ltbase_row_id, text_01, text_02, text_03, smallint_01, smallint_02, smallint_03, integer_01, bigint_01, bigint_02, double_01, uuid_01, ltbase_created_at, ltbase_updated_at, ltbase_deleted_at) VALUES (7, '%s', %s)", qs, ids[i], values)
	}
	eav := func(i int, attr int, idx, text, num string) string {
		return fmt.Sprintf("INSERT INTO %s.eav_data VALUES (7, '%s', %d, '%s', %s, %s)", qs, ids[i], attr, idx, text, num)
	}
	stmts := []string{
		main(0, "'alpha', '2024-02-29T12:34:56Z', '1', 1, 1, 7, 2147483647, 9007199254740993, 1700000000123, 3.25, 'b3c1e4d2-2f6a-4c6e-9b1d-0c2e5f7a8b9c', 100, 1000, NULL"),
		main(1, "NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 101, 1001, NULL"),
		main(2, "'héllo ✓', '1999-12-31T23:59:59.999+05:30', '0', 0, 0, -32768, -2147483648, -9223372036854775808, 0, -0.5, NULL, 102, 1002, NULL"),
		main(3, "'', 'not a date', 'yes', 2, 3, 32767, 0, 9223372036854775807, -1, 'Infinity', NULL, 103, 1003, NULL"),
		main(4, "'it''s', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'NaN', NULL, 104, 1004, NULL"),
		main(5, "'e', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 1e300, NULL, 105, 1005, NULL"),
		main(6, "'gone', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 106, 1006, 4000"),
		main(8, "'flushed', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 108, 1008, NULL"),
		main(9, "'later', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 109, 6000, NULL"),

		eav(0, 20, "", "'nick'", "NULL"), eav(0, 21, "", "NULL", "12"), eav(0, 22, "", "NULL", "-7"),
		eav(0, 23, "", "NULL", "9007199254740993"), eav(0, 24, "", "NULL", "'NaN'"), eav(0, 25, "", "NULL", "1"),
		eav(0, 26, "", "NULL", "1700000000000"), eav(0, 27, "", "'5d0b5d6e-6c55-4f7b-8f0a-3c1d2e3f4a5b'", "NULL"),
		eav(0, 28, "0", "'a'", "NULL"), eav(0, 28, "1", "'b'", "NULL"),
		eav(0, 29, "2", "NULL", "2"), eav(0, 29, "0", "NULL", "3"), eav(0, 29, "1", "NULL", "1"),
		eav(1, 24, "", "NULL", "'Infinity'"), eav(1, 25, "", "NULL", "0"), eav(1, 28, "", "NULL", "NULL"),
		eav(2, 21, "", "NULL", "40000"), eav(2, 22, "", "NULL", "1.5"), eav(2, 24, "", "NULL", "'-Infinity'"),
		eav(2, 25, "", "NULL", "0.7"), eav(2, 26, "", "NULL", "1000.5"), eav(2, 29, "", "NULL", "5"),
		eav(3, 23, "", "NULL", "-9223372036854775808"), eav(3, 24, "", "NULL", "1e400"), eav(3, 20, "", "''", "NULL"),
		eav(4, 24, "", "NULL", "1e-400"), eav(4, 21, "", "NULL", "-0"), eav(4, 27, "", "'not-a-uuid'", "NULL"),
		eav(5, 24, "", "NULL", "0.1"), eav(5, 26, "", "NULL", "1e30"), eav(5, 28, "0", "''", "NULL"),
		eav(6, 20, "", "'soft'", "NULL"),
		eav(8, 20, "", "'flushed'", "NULL"), eav(9, 20, "", "'later'", "NULL"),
		fmt.Sprintf("INSERT INTO %s.eav_data VALUES (8, '%s', 20, '', 'other schema', NULL)", qs, ids[0]),
		fmt.Sprintf("INSERT INTO %s.change_log VALUES (7, '%s', 999, 500, NULL)", qs, ids[0]),
		fmt.Sprintf("INSERT INTO %s.change_log VALUES (7, '%s', 123, 1008, NULL)", qs, ids[8]),
		fmt.Sprintf("INSERT INTO %s.change_log VALUES (7, '%s', 0, 6000, NULL)", qs, ids[9]),
		fmt.Sprintf("INSERT INTO %s.change_log VALUES (7, '%s', 0, 4000, 4000)", qs, ids[6]),
		fmt.Sprintf("INSERT INTO %s.change_log VALUES (7, '%s', 0, 4500, 4500)", qs, ids[7]),
	}
	for i := 0; i <= 5; i++ {
		stmts = append(stmts, fmt.Sprintf("INSERT INTO %s.change_log VALUES (7, '%s', 0, %d, NULL)", qs, ids[i], 1000+i))
	}
	return stmts
}

// parityAttrCache binds an attribute to every main column the fixture has,
// covering the three bool encodings and both date encodings, and keeps one
// EAV attribute per value type plus a text and an integer LIST.
func parityAttrCache() forma.SchemaAttributeCache {
	bound := func(id int16, vt forma.ValueType, col forma.MainColumn, enc forma.MainColumnEncoding) forma.AttributeMetadata {
		return forma.AttributeMetadata{AttributeID: id, ValueType: vt, ColumnBinding: &forma.MainColumnBinding{ColumnName: col, Encoding: enc}}
	}
	eav := func(id int16, vt forma.ValueType) forma.AttributeMetadata {
		return forma.AttributeMetadata{AttributeID: id, ValueType: vt}
	}
	cache := forma.SchemaAttributeCache{
		"title":    bound(1, forma.ValueTypeText, forma.MainColumnText01, ""),
		"born":     bound(2, forma.ValueTypeDate, forma.MainColumnText02, forma.MainColumnEncodingISO8601),
		"active_t": bound(3, forma.ValueTypeBool, forma.MainColumnText03, forma.MainColumnEncodingBoolText),
		"active_i": bound(4, forma.ValueTypeBool, forma.MainColumnSmallint01, forma.MainColumnEncodingBoolInt),
		"active_d": bound(5, forma.ValueTypeBool, forma.MainColumnSmallint02, ""),
		"count_s":  bound(6, forma.ValueTypeSmallInt, forma.MainColumnSmallint03, ""),
		"count_i":  bound(7, forma.ValueTypeInteger, forma.MainColumnInteger01, ""),
		"count_b":  bound(8, forma.ValueTypeBigInt, forma.MainColumnBigint01, ""),
		"seen":     bound(9, forma.ValueTypeDateTime, forma.MainColumnBigint02, forma.MainColumnEncodingUnixMs),
		"score":    bound(10, forma.ValueTypeNumeric, forma.MainColumnDouble01, ""),
		"ext_id":   bound(11, forma.ValueTypeUUID, forma.MainColumnUUID01, ""),
		"nick":     eav(20, forma.ValueTypeText),
		"age":      eav(21, forma.ValueTypeSmallInt),
		"rank":     eav(22, forma.ValueTypeInteger),
		"big":      eav(23, forma.ValueTypeBigInt),
		"ratio":    eav(24, forma.ValueTypeNumeric),
		"flag":     eav(25, forma.ValueTypeBool),
		"since":    eav(26, forma.ValueTypeDate),
		"ref":      eav(27, forma.ValueTypeUUID),
		"tags":     eav(28, forma.ValueTypeList),
	}
	nums := eav(29, forma.ValueTypeList)
	nums.ItemsType = forma.ValueTypeInteger
	cache["nums"] = nums
	for name, meta := range cache {
		meta.AttributeName = name
		cache[name] = meta
	}
	return cache
}
