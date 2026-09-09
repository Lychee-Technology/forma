package internal

import (
	"context"
	"errors"
	"regexp"
	"testing"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lychee-technology/forma"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInsertAndUpdateMainRow(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rowID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	record := &model.PersistentRecord{
		SchemaID:  1,
		RowID:     rowID,
		CreatedAt: 10,
		UpdatedAt: 20,
		TextItems: map[string]string{"text_01": "hello"},
	}

	insertQuery, insertArgs, err := buildInsertMainStatement("entity_main", record)
	require.NoError(t, err)
	mock.ExpectExec("^" + regexp.QuoteMeta(insertQuery) + "$").
		WithArgs(insertArgs...).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	updateQuery, updateArgs, err := buildUpdateMainStatement("entity_main", record)
	require.NoError(t, err)
	// PG computes the effective version (GREATEST over the previous row
	// version, #274) and updateMainRow adopts it into record.UpdatedAt.
	mock.ExpectQuery("^" + regexp.QuoteMeta(updateQuery) + "$").
		WithArgs(updateArgs...).
		WillReturnRows(pgxmock.NewRows([]string{"ltbase_updated_at"}).AddRow(int64(21)))

	repo := &DBPersistentRecordRepository{}
	require.NoError(t, repo.insertMainRow(ctx, mock, "entity_main", record))
	require.NoError(t, repo.updateMainRow(ctx, mock, "entity_main", record))
	assert.Equal(t, int64(21), record.UpdatedAt, "updateMainRow must adopt the RETURNING'd effective version")

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestUpdateMainRowClassifiesUniqueViolationAsConflict(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rowID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	record := &model.PersistentRecord{
		SchemaID:  1,
		RowID:     rowID,
		UpdatedAt: 20,
		TextItems: map[string]string{"text_01": "hello"},
	}

	updateQuery, updateArgs, err := buildUpdateMainStatement("entity_main", record)
	require.NoError(t, err)
	mock.ExpectQuery("^" + regexp.QuoteMeta(updateQuery) + "$").
		WithArgs(updateArgs...).
		WillReturnError(&pgconn.PgError{Code: "23505", Detail: "duplicate key value violates unique constraint"})

	repo := &DBPersistentRecordRepository{}
	err = repo.updateMainRow(ctx, mock, "entity_main", record)
	require.Error(t, err)
	assert.ErrorIs(t, err, forma.ErrConflict)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestClassifyPgError(t *testing.T) {
	t.Run("maps 23505 to conflict", func(t *testing.T) {
		err := classifyPgError(&pgconn.PgError{Code: "23505", Detail: "duplicate key value violates unique constraint"})
		require.Error(t, err)
		assert.ErrorIs(t, err, forma.ErrConflict)
	})

	t.Run("publishes a summary, not pg detail", func(t *testing.T) {
		pgDetail := "Key (schema_id, row_id)=(7, abc) already exists."
		err := classifyPgError(&pgconn.PgError{Code: "23505", Detail: pgDetail})
		require.Error(t, err)

		var pub forma.PublicError
		require.True(t, errors.As(err, &pub), "conflict publishes no client message: %v", err)
		assert.Equal(t, "the write conflicts with a row that already exists", pub.PublicMessage())
		assert.NotContains(t, pub.PublicMessage(), "Key (")
		assert.Contains(t, err.Error(), pgDetail, "the operator copy must keep the driver detail")
		assert.True(t, forma.HasOperatorDetail(err))
	})

	t.Run("leaves other pg errors unchanged", func(t *testing.T) {
		original := &pgconn.PgError{Code: "23503", Detail: "violates foreign key constraint"}
		assert.Same(t, original, classifyPgError(original))
	})

	t.Run("leaves non pg errors unchanged", func(t *testing.T) {
		original := errors.New("boom")
		assert.Same(t, original, classifyPgError(original))
	})
}
