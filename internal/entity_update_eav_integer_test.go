package internal

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/transform"
)

// #729: an EAV-only smallint or integer is held to its declared type on the
// OLTP read, as #708 holds one bound to a wider column. An eav_data image
// the type cannot hold is a read-path consistency error, so an update that
// does not name the attribute is blocked instead of writing the wrapped or
// truncated value back, and an update that names it repairs the row.

const eavIntSchema = 729

func eavIntRegistry() forma.SchemaRegistry {
	return &stubSchemaRegistry{schemaID: eavIntSchema, schemaName: "eav_int", cache: forma.SchemaAttributeCache{
		"name":   {AttributeID: 1, ValueType: forma.ValueTypeText, ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnText01}},
		"loose":  {AttributeID: 2, ValueType: forma.ValueTypeInteger},
		"tiny":   {AttributeID: 3, ValueType: forma.ValueTypeSmallInt},
		"levels": {AttributeID: 4, ValueType: forma.ValueTypeList, ItemsType: forma.ValueTypeInteger},
		"qty":    {AttributeID: 5, ValueType: forma.ValueTypeInteger, ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnBigint01}},
	}}
}

// eavIntImage is a value_numeric image planted in an eav_data record: its
// float64 and, as the Postgres JSON_AGG read carries it, its NUMERIC token.
type eavIntImage struct {
	image float64
	token string
}

// eavIntInvalidImages answers the images eav_data can hold that declared
// cannot. The last integer one is a NUMERIC whose float64 rounds onto a
// whole number in range; only its token shows the fraction.
func eavIntInvalidImages(declared forma.ValueType) []eavIntImage {
	if declared == forma.ValueTypeSmallInt {
		return []eavIntImage{{40000, "40000"}, {-40000, "-40000"}, {32768, "32768"}, {-32769, "-32769"}, {1.5, "1.5"}}
	}
	return []eavIntImage{
		{4294967296, "4294967296"}, {2147483648, "2147483648"}, {-2147483649, "-2147483649"},
		{1001.5, "1001.5"}, {-0.5, "-0.5"},
		{math.NaN(), "NaN"}, {math.Inf(1), "Infinity"}, {math.Inf(-1), "-Infinity"},
		{1001, "1001.0000000000000001"},
	}
}

var eavIntSlots = []struct {
	attr     string
	declared forma.ValueType
}{{"loose", forma.ValueTypeInteger}, {"tiny", forma.ValueTypeSmallInt}}

// seedEAVIntRow stores a row with every attribute at 1 and levels at [1, 1],
// as the write path leaves it.
func seedEAVIntRow(t *testing.T) legacyBigintRow {
	t.Helper()
	registry := eavIntRegistry()
	tr := transform.NewPersistentRecordTransformer(registry)
	repo := newMockPersistentRecordRepository()
	rowID := uuid.New()
	repo.storeRecord(buildPersistentRecord(t, tr, eavIntSchema, rowID, map[string]any{
		"name": "kept", "loose": int64(1), "tiny": int64(1), "levels": []any{int64(1), int64(1)}, "qty": int64(1),
	}))
	em := mustNewEntityManager(t, tr, repo, nil, registry, createTestConfig(), nil)
	return legacyBigintRow{em: em, repo: repo, tr: tr, rowID: rowID}
}

// plantEAVImage overwrites the eav_data record of attrID at indices
// underneath the write path, as history written before the funnel or by
// direct SQL left it, or appends one when the row has none (a retained
// record of a column-bound attribute).
func plantEAVImage(row legacyBigintRow, attrID int16, indices string, img eavIntImage) {
	record := row.repo.records[eavIntSchema][row.rowID]
	image := img.image
	planted := model.EAVRecord{
		SchemaID: eavIntSchema, RowID: row.rowID, AttrID: attrID, ArrayIndices: indices,
		ValueNumeric: &image, ValueNumericRaw: img.token,
	}
	for i, attr := range record.OtherAttributes {
		if attr.AttrID == attrID && attr.ArrayIndices == indices {
			record.OtherAttributes[i] = planted
			return
		}
	}
	record.OtherAttributes = append(record.OtherAttributes, planted)
}

func eavIntUpdate(rowID uuid.UUID, updates map[string]any) *forma.EntityOperation {
	return &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: "eav_int", RowID: rowID},
		Type:             forma.OperationUpdate,
		Updates:          updates,
	}
}

// assertEAVIntReadError checks the read-path error an invalid eav_data image
// raises: plain (never a 4xx), naming the attribute, the row, the storage,
// the declared type and the stored token.
func assertEAVIntReadError(t *testing.T, err error, attr string, declared forma.ValueType, token string, rowID uuid.UUID) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s holding %s in value_numeric: no error", attr, token)
	}
	if errors.Is(err, forma.ErrInvalidInput) {
		t.Fatalf("an invalid stored image is read-path drift, not input: %v", err)
	}
	for _, want := range []string{
		"'" + attr + "'", rowID.String(),
		"stored " + string(declared) + " image " + token + " in value_numeric does not fit declared type " + string(declared),
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("expected the error to name %q, got %v", want, err)
		}
	}
}

func TestReadRefusesEAVIntegerImageOutsideDeclaredType(t *testing.T) {
	ctx := context.Background()
	for _, slot := range eavIntSlots {
		for _, img := range eavIntInvalidImages(slot.declared) {
			t.Run(slot.attr+"/"+img.token, func(t *testing.T) {
				row := seedEAVIntRow(t)
				meta := eavIntRegistry().(*stubSchemaRegistry).cache[slot.attr]
				plantEAVImage(row, meta.AttributeID, "", img)
				_, err := row.tr.FromPersistentRecord(ctx, row.repo.records[eavIntSchema][row.rowID])
				assertEAVIntReadError(t, err, slot.attr, slot.declared, img.token, row.rowID)
				_, err = row.em.Get(ctx, &forma.QueryRequest{SchemaName: "eav_int", RowID: &row.rowID})
				assertEAVIntReadError(t, err, slot.attr, slot.declared, img.token, row.rowID)
			})
		}
	}
}

// A list element is its own eav_data record, held to the items type; an
// eav_data record a column-bound attribute kept from before an EAV-to-bound
// promotion is decoded under the declared type too, so it is held to it.
func TestReadRefusesListElementAndRetainedEAVIntegerImages(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name, attr string
		attrID     int16
		indices    string
	}{
		{"list element", "levels", 4, "1"},
		{"retained record of a bound attribute", "qty", 5, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			row := seedEAVIntRow(t)
			plantEAVImage(row, tc.attrID, tc.indices, eavIntImage{1001.5, "1001.5"})
			_, err := row.em.Get(ctx, &forma.QueryRequest{SchemaName: "eav_int", RowID: &row.rowID})
			assertEAVIntReadError(t, err, tc.attr, forma.ValueTypeInteger, "1001.5", row.rowID)
		})
	}
}

// The declared type's own bounds read exactly, from a plain token and from
// the trailing-zero spelling a DuckDB DOUBLE renders.
func TestReadKeepsEAVIntegerImageInsideDeclaredType(t *testing.T) {
	for _, tc := range []struct {
		attr  string
		img   eavIntImage
		want  any
		label string
	}{
		{"loose", eavIntImage{math.MaxInt32, "2147483647"}, int32(math.MaxInt32), "integer max"},
		{"loose", eavIntImage{math.MinInt32, "-2147483648"}, int32(math.MinInt32), "integer min"},
		{"loose", eavIntImage{1001, "1001.0"}, int32(1001), "whole with a zero fraction"},
		{"tiny", eavIntImage{math.MaxInt16, "32767"}, int16(math.MaxInt16), "smallint max"},
		{"tiny", eavIntImage{math.MinInt16, "-32768"}, int16(math.MinInt16), "smallint min"},
	} {
		t.Run(tc.label, func(t *testing.T) {
			row := seedEAVIntRow(t)
			meta := eavIntRegistry().(*stubSchemaRegistry).cache[tc.attr]
			plantEAVImage(row, meta.AttributeID, "", tc.img)
			assertEqualValue(t, tc.attr, tc.want, storedEAVIntRow(t, row)[tc.attr])
		})
	}
}

// An update that does not name the attribute carries its stored record into
// the write, so over an invalid image it fails with the read's error and
// stores nothing, on every update surface. Before #729 it wrote the wrapped
// or truncated value back.
func TestUnrelatedUpdateOverEAVIntegerImageFails(t *testing.T) {
	ctx := context.Background()
	for _, surface := range updateSurfaces {
		for _, slot := range eavIntSlots {
			for _, img := range eavIntInvalidImages(slot.declared) {
				t.Run(surface.name+"/"+slot.attr+"/"+img.token, func(t *testing.T) {
					row := seedEAVIntRow(t)
					meta := eavIntRegistry().(*stubSchemaRegistry).cache[slot.attr]
					plantEAVImage(row, meta.AttributeID, "", img)
					before := row.repo.records[eavIntSchema][row.rowID]
					_, err := surface.update(ctx, row.em, eavIntUpdate(row.rowID, map[string]any{"name": "unrelated"}))
					assertUnrelatedEAVIntUpdateFailed(t, err, slot.attr, slot.declared, img.token, row.rowID)
					if row.repo.records[eavIntSchema][row.rowID] != before {
						t.Fatalf("failed update stored a new row")
					}
				})
			}
		}
	}
}

func assertUnrelatedEAVIntUpdateFailed(t *testing.T, err error, attr string, declared forma.ValueType, token string, rowID uuid.UUID) {
	t.Helper()
	var reported bestEffortFailure
	if errors.As(err, &reported) {
		if reported.failure.Error != undisclosedBatchError {
			t.Fatalf("a read-path error must not be published, got %q", reported.failure.Error)
		}
		return
	}
	assertEAVIntReadError(t, err, attr, declared, token, rowID)
}

// An update naming the attribute discards the stored image undecoded, so it
// repairs every invalid image and keeps the attributes it does not name. A
// list is repaired by sending the whole list.
func TestUpdateNamingEAVIntegerImageRepairsIt(t *testing.T) {
	ctx := context.Background()
	for _, surface := range updateSurfaces {
		for _, slot := range eavIntSlots {
			for _, img := range eavIntInvalidImages(slot.declared) {
				t.Run(surface.name+"/"+slot.attr+"/"+img.token, func(t *testing.T) {
					row := seedEAVIntRow(t)
					meta := eavIntRegistry().(*stubSchemaRegistry).cache[slot.attr]
					plantEAVImage(row, meta.AttributeID, "", img)
					if _, err := surface.update(ctx, row.em, eavIntUpdate(row.rowID, map[string]any{slot.attr: int64(5)})); err != nil {
						t.Fatalf("update naming %s over a stored %s image: %v", slot.attr, img.token, err)
					}
					attrs := storedEAVIntRow(t, row)
					var want any = int32(5)
					if slot.declared == forma.ValueTypeSmallInt {
						want = int16(5)
					}
					assertEqualValue(t, slot.attr, want, attrs[slot.attr])
					assertEqualValue(t, "name", "kept", attrs["name"])
					assertEqualValue(t, "levels", []any{int32(1), int32(1)}, attrs["levels"])
				})
			}
		}
	}
	t.Run("list element", func(t *testing.T) {
		row := seedEAVIntRow(t)
		plantEAVImage(row, 4, "1", eavIntImage{4294967296, "4294967296"})
		if _, err := row.em.Update(ctx, eavIntUpdate(row.rowID, map[string]any{"levels": []any{int64(1), int64(2)}})); err != nil {
			t.Fatalf("update replacing the list: %v", err)
		}
		assertEqualValue(t, "levels", []any{int32(1), int32(2)}, storedEAVIntRow(t, row)["levels"])
	})
}

// With two invalid images in one row, naming one of them still keeps the
// other, so the update fails on it and stores nothing; naming both repairs
// the row.
func TestUpdateOverTwoInvalidEAVIntegerImages(t *testing.T) {
	ctx := context.Background()
	row := seedEAVIntRow(t)
	plantEAVImage(row, 2, "", eavIntImage{1001.5, "1001.5"})
	plantEAVImage(row, 3, "", eavIntImage{40000, "40000"})
	before := row.repo.records[eavIntSchema][row.rowID]

	_, err := row.em.Update(ctx, eavIntUpdate(row.rowID, map[string]any{"loose": int64(7)}))
	assertEAVIntReadError(t, err, "tiny", forma.ValueTypeSmallInt, "40000", row.rowID)
	if row.repo.records[eavIntSchema][row.rowID] != before {
		t.Fatalf("failed update stored a new row")
	}

	if _, err := row.em.Update(ctx, eavIntUpdate(row.rowID, map[string]any{"loose": int64(7), "tiny": int64(8)})); err != nil {
		t.Fatalf("update naming both attributes: %v", err)
	}
	attrs := storedEAVIntRow(t, row)
	assertEqualValue(t, "loose", int32(7), attrs["loose"])
	assertEqualValue(t, "tiny", int16(8), attrs["tiny"])
}

func storedEAVIntRow(t *testing.T, row legacyBigintRow) map[string]any {
	t.Helper()
	attrs, err := row.tr.FromPersistentRecord(context.Background(), row.repo.records[eavIntSchema][row.rowID])
	if err != nil {
		t.Fatalf("read of the stored row: %v", err)
	}
	return attrs
}
