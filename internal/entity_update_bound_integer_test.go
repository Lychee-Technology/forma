package internal

import (
	"context"
	"errors"
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/transform"
)

// #708: a smallint or integer bound to a column wider than its declared type
// is held to that type on the OLTP read. An image the type cannot hold is a
// read-path consistency error, so an update that does not name the attribute
// is blocked instead of rewriting the column with a wrapped or truncated
// value, and an update that names it repairs the row.

const boundIntSchema = 708

func boundIntRegistry() forma.SchemaRegistry {
	bind := func(col forma.MainColumn) *forma.MainColumnBinding { return &forma.MainColumnBinding{ColumnName: col} }
	return &stubSchemaRegistry{schemaID: boundIntSchema, schemaName: "bound_int", cache: forma.SchemaAttributeCache{
		"name":  {AttributeID: 1, ValueType: forma.ValueTypeText, ColumnBinding: bind(forma.MainColumnText01)},
		"qty":   {AttributeID: 2, ValueType: forma.ValueTypeInteger, ColumnBinding: bind(forma.MainColumnBigint01)},
		"cnt":   {AttributeID: 3, ValueType: forma.ValueTypeInteger, ColumnBinding: bind(forma.MainColumnDouble02)},
		"tiny":  {AttributeID: 4, ValueType: forma.ValueTypeSmallInt, ColumnBinding: bind(forma.MainColumnInteger01)},
		"small": {AttributeID: 5, ValueType: forma.ValueTypeSmallInt, ColumnBinding: bind(forma.MainColumnBigint02)},
		"frac":  {AttributeID: 6, ValueType: forma.ValueTypeSmallInt, ColumnBinding: bind(forma.MainColumnDouble03)},
		"same":  {AttributeID: 7, ValueType: forma.ValueTypeInteger, ColumnBinding: bind(forma.MainColumnInteger02)},
		"loose": {AttributeID: 8, ValueType: forma.ValueTypeInteger},
	}}
}

// boundIntSlot is one bound attribute, the column it is planted in, and
// how a value is planted there.
type boundIntSlot struct {
	attr, column string
	declared     forma.ValueType
	plant        func(record *model.PersistentRecord, image float64)
}

func plantInt64(col string) func(*model.PersistentRecord, float64) {
	return func(r *model.PersistentRecord, image float64) { r.Int64Items[col] = int64(image) }
}

func plantInt32(col string) func(*model.PersistentRecord, float64) {
	return func(r *model.PersistentRecord, image float64) { r.Int32Items[col] = int32(image) }
}

func plantFloat64(col string) func(*model.PersistentRecord, float64) {
	return func(r *model.PersistentRecord, image float64) { r.Float64Items[col] = image }
}

var boundIntSlots = []boundIntSlot{
	{"qty", "bigint_01", forma.ValueTypeInteger, plantInt64("bigint_01")},
	{"cnt", "double_02", forma.ValueTypeInteger, plantFloat64("double_02")},
	{"tiny", "integer_01", forma.ValueTypeSmallInt, plantInt32("integer_01")},
	{"small", "bigint_02", forma.ValueTypeSmallInt, plantInt64("bigint_02")},
	{"frac", "double_03", forma.ValueTypeSmallInt, plantFloat64("double_03")},
}

// boundIntInvalidImages answers the images slot's column can hold that its
// declared type cannot, each with the text the error names. A fraction,
// NaN and the infinities are planted only in a double_* column; an integer
// column cannot hold them.
func boundIntInvalidImages(slot boundIntSlot) []struct {
	image float64
	text  string
} {
	type img = struct {
		image float64
		text  string
	}
	lo, hi := int64(math.MinInt32), int64(math.MaxInt32)
	if slot.declared == forma.ValueTypeSmallInt {
		lo, hi = math.MinInt16, math.MaxInt16
	}
	images := []img{
		{float64(hi + 1), strconv.FormatInt(hi+1, 10)},
		{float64(lo - 1), strconv.FormatInt(lo-1, 10)},
	}
	if slot.declared == forma.ValueTypeInteger && slot.column != "double_02" {
		images = append(images, img{4294967296, "4294967296"})
	}
	if strings.HasPrefix(slot.column, "double_") {
		images = append(images,
			img{1001.5, "1001.5"}, img{-0.5, "-0.5"},
			img{math.NaN(), "NaN"}, img{math.Inf(1), "Infinity"}, img{math.Inf(-1), "-Infinity"})
	}
	return images
}

// seedBoundIntRow stores a row with every slot at 1, then plants image in
// slot's column underneath the write path, as history written before the
// funnel or by direct SQL left it.
func seedBoundIntRow(t *testing.T, slot *boundIntSlot, image float64) legacyBigintRow {
	t.Helper()
	registry := boundIntRegistry()
	tr := transform.NewPersistentRecordTransformer(registry)
	repo := newMockPersistentRecordRepository()
	rowID := uuid.New()
	record := buildPersistentRecord(t, tr, boundIntSchema, rowID, map[string]any{
		"name": "kept", "qty": int64(1), "cnt": int64(1), "tiny": int64(1), "small": int64(1), "frac": int64(1), "same": int64(1),
	})
	if slot != nil {
		slot.plant(record, image)
	}
	repo.storeRecord(record)
	em := mustNewEntityManager(t, tr, repo, nil, registry, createTestConfig(), nil)
	return legacyBigintRow{em: em, repo: repo, tr: tr, rowID: rowID}
}

// storedBoundIntRow reads the row back from the repository the way a GET
// would.
func storedBoundIntRow(t *testing.T, row legacyBigintRow) map[string]any {
	t.Helper()
	attrs, err := row.tr.FromPersistentRecord(context.Background(), row.repo.records[boundIntSchema][row.rowID])
	if err != nil {
		t.Fatalf("read of the stored row: %v", err)
	}
	return attrs
}

func boundIntUpdate(rowID uuid.UUID, updates map[string]any) *forma.EntityOperation {
	return &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: "bound_int", RowID: rowID},
		Type:             forma.OperationUpdate,
		Updates:          updates,
	}
}

// assertBoundIntReadError checks the read-path error an invalid image
// raises: plain (never a 4xx), naming the attribute, the row, the column,
// the declared type and the image.
func assertBoundIntReadError(t *testing.T, err error, slot boundIntSlot, text string, rowID uuid.UUID) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s holding %s in %s: no error", slot.attr, text, slot.column)
	}
	if errors.Is(err, forma.ErrInvalidInput) {
		t.Fatalf("an invalid stored image is read-path drift, not input: %v", err)
	}
	for _, want := range []string{
		"'" + slot.attr + "'", rowID.String(), "bound column " + slot.column,
		"stored " + string(slot.declared) + " image " + text + " ",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("expected the error to name %q, got %v", want, err)
		}
	}
}

func TestReadRefusesBoundIntegerImageOutsideDeclaredType(t *testing.T) {
	ctx := context.Background()
	for _, slot := range boundIntSlots {
		for _, legacy := range boundIntInvalidImages(slot) {
			t.Run(slot.attr+"/"+legacy.text, func(t *testing.T) {
				row := seedBoundIntRow(t, &slot, legacy.image)
				_, err := row.tr.FromPersistentRecord(ctx, row.repo.records[boundIntSchema][row.rowID])
				assertBoundIntReadError(t, err, slot, legacy.text, row.rowID)
				_, err = row.em.Get(ctx, &forma.QueryRequest{SchemaName: "bound_int", RowID: &row.rowID})
				assertBoundIntReadError(t, err, slot, legacy.text, row.rowID)
			})
		}
	}
}

// The declared type's own bounds, and every value between, read exactly:
// the refusal is not an off-by-one at either end. A same-width binding is
// never refused.
func TestReadKeepsBoundIntegerImageInsideDeclaredType(t *testing.T) {
	for _, slot := range boundIntSlots {
		var values []int64
		if slot.declared == forma.ValueTypeSmallInt {
			values = []int64{math.MinInt16, math.MaxInt16, 0, -1}
		} else {
			values = []int64{math.MinInt32, math.MaxInt32, 0, -1}
		}
		for _, v := range values {
			t.Run(slot.attr+"/"+strconv.FormatInt(v, 10), func(t *testing.T) {
				row := seedBoundIntRow(t, &slot, float64(v))
				attrs := storedBoundIntRow(t, row)
				var want any = int32(v)
				if slot.declared == forma.ValueTypeSmallInt {
					want = int16(v)
				}
				assertEqualValue(t, slot.attr, want, attrs[slot.attr])
				assertEqualValue(t, "same", int32(1), attrs["same"])
			})
		}
	}
}

// An update that does not name the attribute carries its stored value into
// the write, so over an invalid image it fails with the read's error and
// leaves the row as stored, on every update surface. Before #708 it wrote
// the wrapped or truncated value back.
func TestUnrelatedUpdateOverBoundIntegerImageFails(t *testing.T) {
	ctx := context.Background()
	for _, surface := range updateSurfaces {
		for _, slot := range boundIntSlots {
			for _, legacy := range boundIntInvalidImages(slot) {
				t.Run(surface.name+"/"+slot.attr+"/"+legacy.text, func(t *testing.T) {
					row := seedBoundIntRow(t, &slot, legacy.image)
					before := row.repo.records[boundIntSchema][row.rowID]
					_, err := surface.update(ctx, row.em, boundIntUpdate(row.rowID, map[string]any{"name": "unrelated"}))
					var reported bestEffortFailure
					if errors.As(err, &reported) {
						if reported.failure.Error != undisclosedBatchError {
							t.Fatalf("a read-path error must not be published, got %q", reported.failure.Error)
						}
					} else {
						assertBoundIntReadError(t, err, slot, legacy.text, row.rowID)
					}
					if row.repo.records[boundIntSchema][row.rowID] != before {
						t.Fatalf("failed update stored a new row")
					}
				})
			}
		}
	}
}

// An update naming the attribute discards the stored image undecoded, so it
// repairs every invalid image and keeps the attributes it does not name.
func TestUpdateNamingBoundIntegerImageRepairsIt(t *testing.T) {
	ctx := context.Background()
	for _, surface := range updateSurfaces {
		for _, slot := range boundIntSlots {
			for _, legacy := range boundIntInvalidImages(slot) {
				t.Run(surface.name+"/"+slot.attr+"/"+legacy.text, func(t *testing.T) {
					row := seedBoundIntRow(t, &slot, legacy.image)
					if _, err := surface.update(ctx, row.em, boundIntUpdate(row.rowID, map[string]any{slot.attr: int64(5)})); err != nil {
						t.Fatalf("update naming %s over a stored %s image: %v", slot.attr, legacy.text, err)
					}
					attrs := storedBoundIntRow(t, row)
					var want any = int32(5)
					if slot.declared == forma.ValueTypeSmallInt {
						want = int16(5)
					}
					assertEqualValue(t, slot.attr, want, attrs[slot.attr])
					assertEqualValue(t, "name", "kept", attrs["name"])
				})
			}
		}
	}
}

// #708 covers bound columns only. An EAV-only integer is read from eav_data,
// whose records name no bound column, so a fraction there still converts
// (#384) instead of being refused; whether it should is #729's decision.
// This pins the scope: a change that lets eav_data records reach the
// bound-column check fails here.
func TestEAVOnlyIntegerImageStaysOutsideBoundCheck(t *testing.T) {
	row := seedBoundIntRow(t, nil, 0)
	record := row.repo.records[boundIntSchema][row.rowID]
	image := 1001.5
	record.OtherAttributes = append(record.OtherAttributes, model.EAVRecord{
		SchemaID: boundIntSchema, RowID: row.rowID, AttrID: 8, ValueNumeric: &image,
	})
	assertEqualValue(t, "loose", int32(1001), storedBoundIntRow(t, row)["loose"])
}
