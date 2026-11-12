package federated

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/model"
)

// A hand-built boundary whose type does not match its column's export type
// is refused at every seam before either leg runs (#853): 9.6 on created_at
// (BIGINT) is neither rounded nor compared as a double.
func TestKeysetSeamsRefuseAMistypedBoundary(t *testing.T) {
	mistyped := func() *model.FederatedAttributeQuery {
		fq := keysetTestQuery()
		fq.KeysetCursor.Values[0] = 9.6
		return fq
	}
	seams := map[string]func(*DBFederatedQueryEngine) error{
		"query": func(e *DBFederatedQueryEngine) error {
			_, err := e.Query(context.Background(), testTables, mistyped(), &model.FederatedQueryOptions{})
			return err
		},
		"keyset coordinator": func(e *DBFederatedQueryEngine) error {
			_, _, err := e.ExecuteFederatedKeysetQuery(context.Background(), testTables, mistyped(), 10, nil, &model.FederatedQueryOptions{})
			return err
		},
		"buffered pass": func(e *DBFederatedQueryEngine) error {
			_, _, err := e.ExecuteDuckDBFederatedQuery(context.Background(), testTables, mistyped(), 10, 0, nil, &model.FederatedQueryOptions{})
			return err
		},
		"streamed pass": func(e *DBFederatedQueryEngine) error {
			_, err := e.StreamDuckDBFederatedQuery(context.Background(), testTables, mistyped(), 10, 0, nil, &model.FederatedQueryOptions{},
				func(context.Context, *model.PersistentRecord) error { return nil })
			return err
		},
	}
	for name, call := range seams {
		t.Run(name, func(t *testing.T) {
			pg := &fakePostgresFederatedSource{page: &model.PersistentRecordPage{}}
			duck := &fakeDuckDBExecutor{}
			engine := NewDBFederatedQueryEngine(pg, nil, duck, nil, hybridEnabledCfg(), nil, "")

			err := call(engine)

			require.ErrorContains(t, err, `keyset cursor value 1 (for column "created_at", exported as BIGINT) is refused`)
			require.Zero(t, duck.calls)
			require.Zero(t, pg.queryCalls)
		})
	}
}

// keysetQuery hands the seam a normalized copy of the cursor and leaves the
// caller's query as it was built.
func TestKeysetQueryNormalizesACopy(t *testing.T) {
	engine := NewDBFederatedQueryEngine(&fakePostgresFederatedSource{}, nil, &fakeDuckDBExecutor{}, nil, hybridEnabledCfg(), nil, "")
	fq := keysetTestQuery()
	fq.KeysetCursor.Values[0] = int32(5)

	got, err := engine.keysetQuery(fq, nil)

	require.NoError(t, err)
	require.Equal(t, []any{int64(5), uuid.MustParse("11111111-1111-1111-1111-111111111111")}, got.KeysetCursor.Values)
	require.Equal(t, "BIGINT", got.KeysetCursor.Columns[0].ExportType)
	require.Equal(t, "UUID", got.KeysetCursor.Columns[1].ExportType)
	require.Equal(t, int32(5), fq.KeysetCursor.Values[0], "the caller's cursor is untouched")
	require.Empty(t, fq.KeysetCursor.Columns[0].ExportType)

	open := keysetTestQuery()
	open.KeysetCursor = nil
	same, err := engine.keysetQuery(open, nil)
	require.NoError(t, err)
	require.Same(t, open, same)
}
