package internal

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/testdb"
	"github.com/lychee-technology/forma/internal/transform"
)

const (
	pastFloat64DateImage = "is outside the epoch milliseconds a float64 image keeps exactly (up to 9007199254740992, 2^53)"
	fractionDateImage    = "(not a whole number of epoch milliseconds) names no epoch millisecond instant"
	beyondDateImage      = "(beyond any epoch millisecond instant) names no epoch millisecond instant"
)

// plantedDateImage is a value_numeric literal planted under the write path
// and what the read makes of it: the instant ms when rule is empty, a
// consistency error containing rule otherwise.
type plantedDateImage struct {
	literal string
	ms      int64
	rule    string
}

// Both column types hold the admitted range and the images that name no
// instant; Postgres keeps NaN and ±Infinity in either.
var commonDateImages = []plantedDateImage{
	{"0", 0, ""},
	{"-1", -1, ""},
	{"1704067200123", 1704067200123, ""},
	{"9007199254740991", 9007199254740991, ""},
	{"-9007199254740991", -9007199254740991, ""},
	{"9007199254740992", 9007199254740992, ""},
	{"-9007199254740992", -9007199254740992, ""},
	{"9007199254740994", 0, "(287396-10-12T08:59:00.994Z) " + pastFloat64DateImage},
	{"-9007199254740994", 0, pastFloat64DateImage},
	{"1000.5", 0, "stored value 1000.5 " + fractionDateImage},
	{"1e300", 0, beyondDateImage},
	{"NaN", 0, "stored value NaN " + fractionDateImage},
	{"Infinity", 0, "stored value Infinity " + fractionDateImage},
	{"-Infinity", 0, "stored value -Infinity " + fractionDateImage},
}

// A DOUBLE PRECISION column keeps what its float64 holds: MaxInt64 is 2^63
// once stored, past any instant, and MinInt64 reads back in the shortest
// spelling of -2^63, whose digits (-9223372036854776000) are past it too.
var doubleOnlyDateImages = []plantedDateImage{
	{"9223372036854775807", 0, beyondDateImage},
	{"-9223372036854775808", 0, beyondDateImage},
}

// A NUMERIC column keeps the digits, so the read sees images whose float64
// rounds onto an admitted instant (2^53+1 and 2^53-0.5 both round to 2^53)
// and judges them by the digits (#592), and a scale is no fraction.
var numericOnlyDateImages = []plantedDateImage{
	{"1704067200123.000", 1704067200123, ""},
	{"9007199254740993", 0, "stored value 9007199254740993 (287396-10-12T08:59:00.993Z) " + pastFloat64DateImage},
	{"-9007199254740993", 0, pastFloat64DateImage},
	{"9223372036854775807", 0, pastFloat64DateImage},
	{"-9223372036854775808", 0, pastFloat64DateImage},
	{"9223372036854775808", 0, "stored value 9223372036854775808 " + beyondDateImage},
	{"1e400", 0, beyondDateImage},
	{"9007199254740991.5", 0, "stored value 9007199254740991.5 " + fractionDateImage},
	{"0.001", 0, fractionDateImage},
}

// plantedDateSlots are the unbound date rows of a date_dest row: the scalar
// seenAt and the second item of seenOn.
var plantedDateSlots = []struct {
	attr    string
	attrID  int16
	indices string
}{
	{"seenAt", 20, ""},
	{"seenOn", 25, "1"},
}

// readDateRow reads rowID through both Postgres read routes, the single-row
// load (loadRecordWithAttributes) and the list query (scanOptimizedRow), and
// transforms each the way a GET and a list do.
func (f *eavDateFixture) readDateRow(t *testing.T, rowID uuid.UUID) map[string]readOutcome {
	t.Helper()
	single, err := f.repo.GetPersistentRecord(f.ctx, f.tables, dateDestSchema, rowID)
	require.NoError(t, err)
	page, err := f.repo.QueryPersistentRecords(f.ctx, &model.PersistentRecordQuery{Tables: f.tables, SchemaID: dateDestSchema})
	require.NoError(t, err)
	require.Len(t, page.Records, 1)
	require.Equal(t, rowID, page.Records[0].RowID)

	outcomes := map[string]readOutcome{}
	for route, record := range map[string]*model.PersistentRecord{"single row": single, "list": page.Records[0]} {
		attrs, err := f.tr.FromPersistentRecord(f.ctx, record)
		outcomes[route] = readOutcome{attrs: attrs, err: err}
	}
	return outcomes
}

type readOutcome struct {
	attrs map[string]any
	err   error
}

// censusFlags evaluates the validate-schema-consistency predicate on the
// planted eav_data row.
func (f *eavDateFixture) censusFlags(t *testing.T, rowID uuid.UUID, attrID int16, indices string) bool {
	t.Helper()
	var flagged bool
	query := fmt.Sprintf("SELECT %s FROM %s WHERE schema_id = $1 AND row_id = $2 AND attr_id = $3 AND array_indices = $4",
		transform.StoredDateImageRefusedSQL("value_numeric"), sanitizeIdentifier(f.tables.EAVData))
	require.NoError(t, f.pool.QueryRow(f.ctx, query, dateDestSchema, rowID, attrID, indices).Scan(&flagged))
	return flagged
}

func (f *eavDateFixture) deleteRow(t *testing.T, rowID uuid.UUID) {
	t.Helper()
	for table, column := range map[string]string{f.tables.EAVData: "row_id", f.tables.EntityMain: "ltbase_row_id"} {
		_, err := f.pool.Exec(f.ctx, fmt.Sprintf("DELETE FROM %s WHERE %s = $1", sanitizeIdentifier(table), column), rowID)
		require.NoError(t, err)
	}
}

// #592 regression matrix, Postgres direct routes: an unbound date image
// planted under the write path (a row from before the rule, or edited by
// hand), scalar or list item, on the unit DDL's DOUBLE PRECISION and
// production's NUMERIC, reads through the single-row load and the list
// query as the instant the digits name when the write would admit it, and
// as a consistency error naming the attribute, the row and the rule
// otherwise: never as a rounded, wrapped or absent value, never as the
// caller's invalid input. The validate-schema-consistency predicate flags
// exactly the images the read refuses, so the pre-upgrade census finds
// every row the upgraded read rejects.
func TestEAVDateLegacyImageReadIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	pool := testdb.Connect(t, ctx)
	kept := time.UnixMilli(1700000000000).UTC()

	for _, numeric := range []bool{false, true} {
		f := newEAVDateFixture(t, ctx, pool, numeric)
		name := map[bool]string{false: "double precision", true: "numeric"}[numeric]
		images := append(append([]plantedDateImage{}, commonDateImages...), map[bool][]plantedDateImage{false: doubleOnlyDateImages, true: numericOnlyDateImages}[numeric]...)
		for _, slot := range plantedDateSlots {
			for _, image := range images {
				t.Run(fmt.Sprintf("%s %s %s", name, slot.attr, image.literal), func(t *testing.T) {
					rowID, _ := f.writeAndRead(t, map[string]any{"seenAt": kept, "seenOn": []any{kept, kept}})
					defer f.deleteRow(t, rowID)
					tag, err := pool.Exec(ctx, fmt.Sprintf("UPDATE %s SET value_numeric = '%s' WHERE schema_id = $1 AND row_id = $2 AND attr_id = $3 AND array_indices = $4",
						sanitizeIdentifier(f.tables.EAVData), image.literal), dateDestSchema, rowID, slot.attrID, slot.indices)
					require.NoError(t, err)
					require.EqualValues(t, 1, tag.RowsAffected())

					for route, got := range f.readDateRow(t, rowID) {
						if image.rule == "" {
							require.NoError(t, got.err, route)
							want := any(time.UnixMilli(image.ms).UTC())
							if slot.attr == "seenOn" {
								want = []any{kept, want}
							}
							require.Equal(t, want, got.attrs[slot.attr], route)
							continue
						}
						require.Error(t, got.err, route)
						require.Nil(t, got.attrs, route)
						require.NotErrorIs(t, got.err, forma.ErrInvalidInput, "%s: a stored image is the operator's, not the caller's", route)
						for _, want := range []string{"'" + slot.attr + "'", rowID.String(), image.rule} {
							require.Contains(t, got.err.Error(), want, route)
						}
					}
					require.Equal(t, image.rule != "", f.censusFlags(t, rowID, slot.attrID, slot.indices), "census and read disagree on %s", image.literal)
				})
			}
		}
	}
}

// The write funnel refuses every date past the float64 image's exact range
// for the unbound destination, scalar and list item, in each spelling, and
// writes nothing (#592); the message names the destination, so the caller
// learns where the range comes from.
func TestEAVDateRefusedWriteIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool := testdb.Connect(t, ctx)
	f := newEAVDateFixture(t, ctx, pool, true)
	plus14 := time.FixedZone("plus14", 14*3600)

	for _, ms := range []int64{9007199254740993, 9007199254740994, -9007199254740993, 1 << 62, -(1 << 62) - 1} {
		for _, attr := range []string{"seenAt", "bornOn", "seenOn", "visit.endAt"} {
			t.Run(fmt.Sprintf("%s %d", attr, ms), func(t *testing.T) {
				for _, in := range []any{time.UnixMilli(ms).UTC(), time.UnixMilli(ms).In(plus14), fmt.Sprint(ms)} {
					payload := map[string]any{attr: in}
					if attr == "seenOn" {
						payload = map[string]any{attr: []any{time.UnixMilli(0).UTC(), in}}
					}
					if head, leaf, nested := strings.Cut(attr, "."); nested {
						payload = map[string]any{head: map[string]any{leaf: in}}
					}
					_, err := f.tr.ToPersistentRecord(ctx, dateDestSchema, uuid.New(), payload)
					require.ErrorIs(t, err, forma.ErrInvalidInput, "%v", in)
					msg, ok := forma.ResolvePublicMessage(err)
					require.True(t, ok)
					require.Contains(t, msg, fmt.Sprintf("value %d", ms))
					require.Contains(t, msg, "cannot be stored in eav_data.value_numeric, which keeps epoch milliseconds exactly up to 9007199254740992 (2^53)")
				}
			})
		}
	}
}
