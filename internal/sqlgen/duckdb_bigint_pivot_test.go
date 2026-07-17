package sqlgen

import (
	"database/sql"
	"math"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// TestBuildEAVPivotEmitsTypedCasts pins the #205 Hop-2 fix: the EAV pivot must
// emit per-type TRY_CASTs mirroring the CDC export side (cdc.castEAVValue), so
// COALESCE(pivot, m.<col>) and the UNION ALL with parquet legs type-unify to
// the attribute's native type instead of DOUBLE.
func TestBuildEAVPivotEmitsTypedCasts(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"amount": {AttributeID: 5, ValueType: forma.ValueTypeBigInt,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnBigint01}},
		"joined": {AttributeID: 6, ValueType: forma.ValueTypeDateTime,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnBigint02, Encoding: forma.MainColumnEncodingUnixMs}},
		"total": {AttributeID: 15, ValueType: forma.ValueTypeBigInt},
		"qty":   {AttributeID: 16, ValueType: forma.ValueTypeInteger},
		"level": {AttributeID: 17, ValueType: forma.ValueTypeSmallInt},
		"ratio": {AttributeID: 18, ValueType: forma.ValueTypeNumeric},
	}
	sp, err := BuildSchemaProjection(7, cache)
	require.NoError(t, err)

	// bigint / date pivot 出 BIGINT;integer/smallint 出各自原生类型
	require.Contains(t, sp.EAVPivotSelect,
		"TRY_CAST(MAX(CASE WHEN attr_id = 5 THEN value_numeric END) AS BIGINT) AS amount")
	require.Contains(t, sp.EAVPivotSelect,
		"TRY_CAST(MAX(CASE WHEN attr_id = 6 THEN value_numeric END) AS BIGINT) AS joined")
	require.Contains(t, sp.EAVPivotSelect,
		"TRY_CAST(MAX(CASE WHEN attr_id = 15 THEN value_numeric END) AS BIGINT) AS total")
	require.Contains(t, sp.EAVPivotSelect,
		"TRY_CAST(MAX(CASE WHEN attr_id = 16 THEN value_numeric END) AS INTEGER) AS qty")
	require.Contains(t, sp.EAVPivotSelect,
		"TRY_CAST(MAX(CASE WHEN attr_id = 17 THEN value_numeric END) AS SMALLINT) AS level")
	// numeric 保持 DOUBLE 语义:不 cast
	require.Contains(t, sp.EAVPivotSelect,
		"MAX(CASE WHEN attr_id = 18 THEN value_numeric END) AS ratio")
	require.NotContains(t, sp.EAVPivotSelect, "attr_id = 18 THEN value_numeric END) AS ratio)")

	// COALESCE 形态本身不变(修复靠 pivot 侧类型)
	require.True(t, strings.Contains(sp.PGSourceSelect,
		"COALESCE(ANY_VALUE(hot_vals.amount), m.bigint_01) AS amount"))
}

// TestDuckDBBigintTypeUnification pins the DuckDB engine facts #205 rests on:
// the old DOUBLE-pivot shape crashes CAST-back at MaxInt64, the typed-pivot
// shape is exact, and UNION ALL unification is what drags BIGINT to DOUBLE.
func TestDuckDBBigintTypeUnification(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	// (1) 旧形态崩溃机制:COALESCE(DOUBLE, BIGINT) → DOUBLE,CAST 回 BIGINT 溢出。
	var v int64
	err = db.QueryRow(`SELECT CAST(COALESCE(CAST(NULL AS DOUBLE), 9223372036854775807::BIGINT) AS BIGINT)`).Scan(&v)
	require.Error(t, err, "DOUBLE pivot 形态必须复现生产崩溃")
	require.Contains(t, err.Error(), "out of range")

	// (2) 新形态:BIGINT pivot → COALESCE 保 BIGINT,MaxInt64 精确。
	require.NoError(t, db.QueryRow(`SELECT CAST(COALESCE(TRY_CAST(NULL AS BIGINT), 9223372036854775807::BIGINT) AS BIGINT)`).Scan(&v))
	require.Equal(t, int64(math.MaxInt64), v)

	// (3) UNION ALL 统一:BIGINT+DOUBLE→DOUBLE(污染路径),BIGINT+BIGINT→BIGINT。
	var typ string
	require.NoError(t, db.QueryRow(`SELECT typeof(x) FROM (SELECT 1::BIGINT AS x UNION ALL SELECT 1::DOUBLE) LIMIT 1`).Scan(&typ))
	require.Equal(t, "DOUBLE", typ)
	require.NoError(t, db.QueryRow(`SELECT typeof(x) FROM (SELECT 9223372036854775807::BIGINT AS x UNION ALL SELECT 0::BIGINT) LIMIT 1`).Scan(&typ))
	require.Equal(t, "BIGINT", typ)

	// (4) iso8601 护栏:date 属性 pivot 从 DOUBLE 改 BIGINT 后,与 VARCHAR 主列
	// (iso8601 编码绑定 text 列)的 COALESCE 行为必须不变——两种 NULL 类型下
	// 行为逐位一致(同成功同值,或同失败)。
	q := func(sql string) (string, error) {
		var s string
		e := db.QueryRow(sql).Scan(&s)
		return s, e
	}
	s1, e1 := q(`SELECT COALESCE(CAST(NULL AS DOUBLE), '2024-01-02T03:04:05Z'::VARCHAR)`)
	s2, e2 := q(`SELECT COALESCE(TRY_CAST(NULL AS BIGINT), '2024-01-02T03:04:05Z'::VARCHAR)`)
	require.Equal(t, e1 == nil, e2 == nil, "iso8601 热腿 COALESCE 行为不得因 pivot 类型改变")
	if e1 == nil {
		require.Equal(t, s1, s2)
	}
}
