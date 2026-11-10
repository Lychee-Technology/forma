package sqlgen

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/rfc3339"
	"github.com/lychee-technology/forma/internal/testdb"
)

// TestServerTimeOperand_RFC3339ImageIsTheWriteImage runs the $now operand of
// an rfc3339 column on PostgreSQL (#842): within one transaction now() is
// fixed, so each offset names one known instant, and the SQL image of it
// must be byte for byte the image rfc3339.Image writes, millisecond
// included. Offsets reach before the epoch, where the remainder of a
// negative millisecond count must not go negative, and to millisecond
// boundaries either side of a whole second.
func TestServerTimeOperand_RFC3339ImageIsTheWriteImage(t *testing.T) {
	ctx := context.Background()
	pool := testdb.Connect(t, ctx)
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(ctx) }()

	var now int64
	require.NoError(t, tx.QueryRow(ctx, "SELECT "+ServerTimeMillisSQL).Scan(&now))
	targets := []int64{now, now - now%1000, now - now%1000 + 1, now - now%1000 + 999, 0, -1, -999, -1000, -1001,
		time.Date(2024, 1, 2, 3, 4, 5, 250e6, time.UTC).UnixMilli(),
		time.Date(1900, 6, 30, 23, 59, 59, 999e6, time.UTC).UnixMilli(),
		time.Date(9999, 12, 31, 23, 59, 59, 999e6, time.UTC).UnixMilli()}
	for _, target := range targets {
		want, rule := rfc3339.Image(target)
		require.Empty(t, rule)
		for _, op := range []struct {
			sqlOp string
			holds bool
		}{{"=", true}, {"<=", true}, {">=", true}, {"<", false}, {">", false}, {"!=", false}} {
			clause := ServerTimeOperand{OffsetMs: target - now, RFC3339: true}.RenderComparison("$1::text", op.sqlOp, "$2")
			var holds bool
			require.NoError(t, tx.QueryRow(ctx, "SELECT "+clause, want, target-now).Scan(&holds))
			require.Equal(t, op.holds, holds, "%s %s $now%+dms", want, op.sqlOp, target-now)
		}
		var image string
		require.NoError(t, tx.QueryRow(ctx, "SELECT "+rfc3339MillisImage("($1::bigint)"), target).Scan(&image))
		require.Equal(t, want, image)
	}
}
