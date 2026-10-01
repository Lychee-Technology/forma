package internal

import (
	"context"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/transform"
)

// legacyBigintImages are stored images of a declared bigint that the #590
// funnel refuses, as a write from before it (or one around it) left them.
// The read still decodes a whole number past 2^53. It refuses a fraction
// and a number past int64.
var legacyBigintImages = []struct {
	name     string
	image    float64
	text     string
	readable bool
}{
	{"whole past 2^53", 9007199254740994, "9007199254740994", true},
	{"fraction", 1000.5, "1000.5", false},
	{"past int64", math.Ldexp(1, 63), "9223372036854775808", false},
}

// legacyBigintSlots are the float64 image destinations of
// bigintDestinationRegistry, each with the update that rewrites it and the
// value that update stores.
var legacyBigintSlots = []struct {
	attr   string
	repair any
	want   any
}{
	{"total", int64(5), int64(5)},
	{"totals", []any{int64(5)}, []any{int64(5)}},
	{"ratio", int64(5), int64(5)},
}

// plantLegacyBigintImage overwrites attr's stored image in record: the scalar
// EAV row of total, the second item of totals, the double_01 column of ratio.
func plantLegacyBigintImage(t *testing.T, record *model.PersistentRecord, attr string, image float64) {
	t.Helper()
	if attr == "ratio" {
		record.Float64Items[string(forma.MainColumnDouble01)] = image
		return
	}
	attrID, indices := map[string]int16{"total": 30, "totals": 31}[attr], map[string]string{"total": "", "totals": "1"}[attr]
	for i := range record.OtherAttributes {
		if eav := &record.OtherAttributes[i]; eav.AttrID == attrID && eav.ArrayIndices == indices {
			eav.ValueNumeric = &image
			return
		}
	}
	t.Fatalf("no stored row for %s[%s]", attr, indices)
}

type legacyBigintRow struct {
	em    forma.EntityManager
	repo  *mockPersistentRecordRepository
	tr    model.PersistentRecordTransformer
	rowID uuid.UUID
}

func seedLegacyBigintRow(t *testing.T, attr string, image float64) legacyBigintRow {
	t.Helper()
	registry := bigintDestinationRegistry()
	tr := transform.NewPersistentRecordTransformer(registry)
	repo := newMockPersistentRecordRepository()
	rowID := uuid.New()
	record := buildPersistentRecord(t, tr, bigintDestSchema, rowID, map[string]any{
		"total": int64(1), "totals": []any{int64(1), int64(2)}, "ratio": int64(1), "amount": int64(1),
	})
	plantLegacyBigintImage(t, record, attr, image)
	repo.storeRecord(record)
	em := mustNewEntityManager(t, tr, repo, nil, registry, createTestConfig(), nil)
	return legacyBigintRow{em: em, repo: repo, tr: tr, rowID: rowID}
}

// legacyBigintUpdaters are the two merge sites: Update and an atomic
// BatchUpdate, each answering the stored row's attributes.
var legacyBigintUpdaters = []struct {
	name   string
	update func(ctx context.Context, row legacyBigintRow, updates map[string]any) (map[string]any, error)
}{
	{"Update", func(ctx context.Context, row legacyBigintRow, updates map[string]any) (map[string]any, error) {
		record, err := row.em.Update(ctx, legacyBigintOp(row.rowID, updates))
		if err != nil {
			return nil, err
		}
		return record.Attributes, nil
	}},
	{"atomic BatchUpdate", func(ctx context.Context, row legacyBigintRow, updates map[string]any) (map[string]any, error) {
		result, err := row.em.BatchUpdate(ctx, &forma.BatchOperation{Atomic: true, Operations: []forma.EntityOperation{*legacyBigintOp(row.rowID, updates)}})
		if err != nil {
			return nil, err
		}
		return result.Successful[0].Attributes, nil
	}},
}

func legacyBigintOp(rowID uuid.UUID, updates map[string]any) *forma.EntityOperation {
	return &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: "bigint_dest", RowID: rowID},
		Type:             forma.OperationUpdate,
		Updates:          updates,
	}
}

// The documented repair for a census-reported image (#590 review) is an
// update that names the attribute. The merge does not convert the stored
// value it replaces, so the repair succeeds for every image the funnel
// refuses, including the ones the read cannot decode, and it keeps the
// attributes it does not name.
func TestUpdateNamingLegacyBigintImageRepairsIt(t *testing.T) {
	ctx := context.Background()
	for _, updater := range legacyBigintUpdaters {
		for _, slot := range legacyBigintSlots {
			for _, legacy := range legacyBigintImages {
				t.Run(updater.name+"/"+slot.attr+"/"+legacy.name, func(t *testing.T) {
					row := seedLegacyBigintRow(t, slot.attr, legacy.image)
					got, err := updater.update(ctx, row, map[string]any{slot.attr: slot.repair})
					if err != nil {
						t.Fatalf("update naming %s over a stored %s image: %v", slot.attr, legacy.text, err)
					}
					stored, err := row.tr.FromPersistentRecord(ctx, row.repo.records[bigintDestSchema][row.rowID])
					if err != nil {
						t.Fatalf("read of the repaired row: %v", err)
					}
					for name, attrs := range map[string]map[string]any{"answered": got, "stored": stored} {
						assertEqualValue(t, name+" "+slot.attr, slot.want, attrs[slot.attr])
						assertEqualValue(t, name+" amount", int64(1), attrs["amount"])
					}
				})
			}
		}
	}
}

// An update that does not name the attribute still carries its stored value
// into the write, so it fails and leaves the row as stored. A whole image
// past 2^53 decodes, and the funnel then refuses it as input that names the
// attribute and the image. A fraction or a number past int64 fails the read
// itself, which is a plain read-path consistency error and never a 4xx.
func TestUnrelatedUpdateOverLegacyBigintImageFails(t *testing.T) {
	ctx := context.Background()
	for _, updater := range legacyBigintUpdaters {
		for _, slot := range legacyBigintSlots {
			for _, legacy := range legacyBigintImages {
				t.Run(updater.name+"/"+slot.attr+"/"+legacy.name, func(t *testing.T) {
					row := seedLegacyBigintRow(t, slot.attr, legacy.image)
					_, err := updater.update(ctx, row, map[string]any{"amount": int64(8)})
					if err == nil {
						t.Fatalf("update of amount over a stored %s image of %s succeeded", legacy.text, slot.attr)
					}
					assertLegacyBigintUpdateError(t, err, slot.attr, legacy.text, legacy.readable, row.rowID)
					if amount := row.repo.records[bigintDestSchema][row.rowID].Int64Items[string(forma.MainColumnBigint01)]; amount != 1 {
						t.Fatalf("failed update stored amount %d", amount)
					}
				})
			}
		}
	}
}

func assertLegacyBigintUpdateError(t *testing.T, err error, attr, image string, readable bool, rowID uuid.UUID) {
	t.Helper()
	if !readable {
		if errors.Is(err, forma.ErrInvalidInput) {
			t.Fatalf("an unreadable stored image is read-path drift, not input: %v", err)
		}
		for _, want := range []string{"'" + attr + "'", rowID.String(), "stored bigint image " + image} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("expected the error to name %q, got %v", want, err)
			}
		}
		return
	}
	if !errors.Is(err, forma.ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput for a stored image the funnel refuses, got %v", err)
	}
	msg, ok := forma.ResolvePublicMessage(err)
	if !ok {
		t.Fatalf("expected a published message, got %v", err)
	}
	for _, want := range []string{attr, image} {
		if !strings.Contains(msg, want) {
			t.Fatalf("expected the published message to name %q, got %q", want, msg)
		}
	}
}

func assertEqualValue(t *testing.T, what string, want, got any) {
	t.Helper()
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("%s = %#v, want %#v", what, got, want)
	}
}
