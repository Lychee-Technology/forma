package internal

import (
	"fmt"
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildInsertMainStatement(t *testing.T) {
	rowID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	uuid2 := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	deleted := int64(300)

	record := &model.PersistentRecord{
		SchemaID:  7,
		RowID:     rowID,
		CreatedAt: 100,
		UpdatedAt: 200,
		DeletedAt: &deleted,
		TextItems: map[string]string{"text_02": "b", "text_01": "a"},
		Int16Items: map[string]int16{
			"smallint_02": 2,
		},
		Int32Items: map[string]int32{
			"integer_01": 10,
		},
		Int64Items: map[string]int64{
			"bigint_01": 1000,
		},
		Float64Items: map[string]float64{
			"double_03": 3.3,
			"double_01": 1.1,
		},
		UUIDItems: map[string]uuid.UUID{
			"uuid_02": uuid2,
		},
	}

	query, args, err := buildInsertMainStatement("entity_main", record)
	require.NoError(t, err)

	expectedColumns := []string{
		"ltbase_schema_id",
		"ltbase_row_id",
		"ltbase_created_at",
		"ltbase_updated_at",
		"ltbase_deleted_at",
		"text_01",
		"text_02",
		"smallint_02",
		"integer_01",
		"bigint_01",
		"double_01",
		"double_03",
		"uuid_02",
	}
	placeholders := make([]string, len(expectedColumns))
	for i := range expectedColumns {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}

	expectedQuery := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		sanitizeIdentifier("entity_main"),
		strings.Join(expectedColumns, ", "),
		strings.Join(placeholders, ", "),
	)

	assert.Equal(t, expectedQuery, query)

	expectedArgs := []any{
		int16(7),
		rowID,
		int64(100),
		int64(200),
		int64(300),
		"a",
		"b",
		int16(2),
		int32(10),
		int64(1000),
		float64(1.1),
		float64(3.3),
		uuid2,
	}
	assert.Equal(t, expectedArgs, args)
}

func TestBuildInsertMainStatementRejectsUnknownColumn(t *testing.T) {
	record := &model.PersistentRecord{
		SchemaID: 1,
		RowID:    uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		TextItems: map[string]string{
			"unknown": "nope",
		},
	}

	_, _, err := buildInsertMainStatement("entity_main", record)
	require.Error(t, err)
}

func TestBuildUpdateMainStatement(t *testing.T) {
	rowID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	record := &model.PersistentRecord{
		SchemaID:  7,
		RowID:     rowID,
		UpdatedAt: 200,
		TextItems: map[string]string{"text_02": "b", "text_01": "a"},
		Int16Items: map[string]int16{
			"smallint_01": 1,
		},
		Float64Items: map[string]float64{
			"double_02": 2.5,
		},
	}

	query, args, err := buildUpdateMainStatement("entity_main", record)
	require.NoError(t, err)

	// GREATEST + RETURNING (#274): the stored version must advance past the
	// row's previous one even on a same-millisecond (or backwards) clock, and
	// the caller adopts the effective value for change_log.
	expectedQuery := "UPDATE " + sanitizeIdentifier("entity_main") + " SET " +
		"ltbase_updated_at = GREATEST($1, ltbase_updated_at + 1), ltbase_deleted_at = $2, text_01 = $3, text_02 = $4, smallint_01 = $5, double_02 = $6 " +
		"WHERE ltbase_schema_id = $7 AND ltbase_row_id = $8 RETURNING ltbase_updated_at"

	assert.Equal(t, expectedQuery, query)

	expectedArgs := []any{
		int64(200),
		nil,
		"a",
		"b",
		int16(1),
		float64(2.5),
		int16(7),
		rowID,
	}
	assert.Equal(t, expectedArgs, args)
}

func TestBuildAttributeValuesClause(t *testing.T) {
	rowID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	text := "foo"
	num := 12.5
	attrs := []model.EAVRecord{
		{SchemaID: 1, RowID: rowID, AttrID: 10, ArrayIndices: "", ValueText: &text},
		{SchemaID: 1, RowID: rowID, AttrID: 11, ArrayIndices: "0", ValueNumeric: &num},
	}

	values, args, err := buildAttributeValuesClause(attrs)
	require.NoError(t, err)

	assert.Equal(t, "($1, $2, $3, $4, $5, $6), ($7, $8, $9, $10, $11, $12)", values)
	expectedArgs := []any{
		int16(1),
		rowID,
		int16(10),
		"",
		&text,
		(*float64)(nil),
		int16(1),
		rowID,
		int16(11),
		"0",
		(*string)(nil),
		&num,
	}
	assert.Equal(t, expectedArgs, args)

	values, args, err = buildAttributeValuesClause(nil)
	require.NoError(t, err)
	assert.Equal(t, "", values)
	assert.Nil(t, args)
}

// TestComputeTombstoneStamp pins the delete-side monotonicity rule (#274): the
// tombstone version is the clock read unless the deleted row's version has
// run ahead of it, in which case it lands strictly past that version.
func TestComputeTombstoneStamp(t *testing.T) {
	assert.Equal(t, int64(100), computeTombstoneStamp(100, 50), "clock ahead of the row: stamp the clock read")
	assert.Equal(t, int64(100), computeTombstoneStamp(100, 99), "prev+1 == now: the clock read already outranks the row")
	assert.Equal(t, int64(101), computeTombstoneStamp(100, 100), "same millisecond: advance past the live version")
	assert.Equal(t, int64(201), computeTombstoneStamp(100, 200), "clock-ahead row: advance past it, not the clock")
}
