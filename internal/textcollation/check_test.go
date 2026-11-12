package textcollation

import (
	"context"
	"errors"
	"regexp"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRequire_ReadsEachTableAndReportsItsColumnsOutsideC(t *testing.T) {
	mock, err := pgxmock.NewConn()
	require.NoError(t, err)
	defer func() { _ = mock.Close(context.Background()) }()
	cols := []string{"attname", "format_type", "collation"}
	mock.ExpectQuery(regexp.QuoteMeta(ColumnsSQL)).WithArgs(`"schema_registry"`).WillReturnRows(pgxmock.NewRows(cols))
	mock.ExpectQuery(regexp.QuoteMeta(ColumnsSQL)).WithArgs(`"entity_main"`).WillReturnRows(pgxmock.NewRows(cols).
		AddRow("text_01", "text", `pg_catalog."default"`).
		AddRow("ltbase_created_by", "text", `pg_catalog."POSIX"`))

	err = Require(context.Background(), mock, []string{`"schema_registry"`, `"entity_main"`})
	var drift *DriftError
	require.True(t, errors.As(err, &drift), "a column outside C is a *DriftError: %v", err)
	assert.Equal(t, []Violation{
		{Table: `"entity_main"`, Column: "text_01", Type: "text", Collation: `pg_catalog."default"`},
		{Table: `"entity_main"`, Column: "ltbase_created_by", Type: "text", Collation: `pg_catalog."POSIX"`},
	}, drift.Violations)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRequire_PassesWhenNoColumnIsOutsideC(t *testing.T) {
	mock, err := pgxmock.NewConn()
	require.NoError(t, err)
	defer func() { _ = mock.Close(context.Background()) }()
	mock.ExpectQuery(regexp.QuoteMeta(ColumnsSQL)).WithArgs(`"eav_data"`).
		WillReturnRows(pgxmock.NewRows([]string{"attname", "format_type", "collation"}))
	require.NoError(t, Require(context.Background(), mock, []string{`"eav_data"`}))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRequire_WrapsACatalogReadFailure(t *testing.T) {
	mock, err := pgxmock.NewConn()
	require.NoError(t, err)
	defer func() { _ = mock.Close(context.Background()) }()
	boom := errors.New("boom")
	mock.ExpectQuery(regexp.QuoteMeta(ColumnsSQL)).WithArgs(`"eav_data"`).WillReturnError(boom)
	err = Require(context.Background(), mock, []string{`"eav_data"`})
	require.ErrorIs(t, err, boom)
	assert.Contains(t, err.Error(), `read the column collations of "eav_data"`)
}
