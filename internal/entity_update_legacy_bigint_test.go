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
// bigintDestinationRegistry, each with an update that rewrites it and the
// value that update stores. contact.total is rewritten in both spellings of
// its name (#312).
var legacyBigintSlots = []struct {
	name   string
	attr   string
	repair map[string]any
	want   any
}{
	{"total", "total", map[string]any{"total": int64(5)}, int64(5)},
	{"totals", "totals", map[string]any{"totals": []any{int64(5)}}, []any{int64(5)}},
	{"ratio", "ratio", map[string]any{"ratio": int64(5)}, int64(5)},
	{"nested contact.total", "contact.total", map[string]any{"contact": map[string]any{"total": int64(5)}}, int64(5)},
	{"literal contact.total", "contact.total", map[string]any{"contact.total": int64(5)}, int64(5)},
}

// unrelatedUpdates name neither the planted attribute nor anything
// containing it: a column, and contact.total's sibling in both spellings.
var unrelatedUpdates = []struct {
	name    string
	updates map[string]any
}{
	{"amount", map[string]any{"amount": int64(8)}},
	{"nested sibling", map[string]any{"contact": map[string]any{"name": "x"}}},
	{"literal sibling", map[string]any{"contact.name": "x"}},
}

var allRequiredPolicies = []forma.RequiredPolicy{
	forma.RequiredPolicyOptional, forma.RequiredPolicyAlways, forma.RequiredPolicyIfParentPresent,
}

// plantLegacyBigintImage overwrites attr's stored image in record: the scalar
// EAV row of total and contact.total, the second item of totals, the
// double_01 column of ratio.
func plantLegacyBigintImage(t *testing.T, record *model.PersistentRecord, attr string, image float64) {
	t.Helper()
	if attr == "ratio" {
		record.Float64Items[string(forma.MainColumnDouble01)] = image
		return
	}
	attrID := map[string]int16{"total": 30, "totals": 31, "contact.total": 34}[attr]
	indices := map[string]string{"totals": "1"}[attr]
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

// stored reads the row back from the repository the way a GET would.
func (row legacyBigintRow) stored(t *testing.T) map[string]any {
	t.Helper()
	attrs, err := row.tr.FromPersistentRecord(context.Background(), row.repo.records[bigintDestSchema][row.rowID])
	if err != nil {
		t.Fatalf("read of the stored row: %v", err)
	}
	return attrs
}

// bigintDestinationRegistryWith is bigintDestinationRegistry with
// contact.total under policy.
func bigintDestinationRegistryWith(policy forma.RequiredPolicy) forma.SchemaRegistry {
	registry := bigintDestinationRegistry().(*stubSchemaRegistry)
	meta := registry.cache["contact.total"]
	meta.RequiredPolicy = policy
	registry.cache["contact.total"] = meta
	return registry
}

// seedBigintRow stores data under bigintDestinationRegistryWith(policy),
// after plant has rewritten the record underneath the write path.
func seedBigintRow(t *testing.T, policy forma.RequiredPolicy, data map[string]any, plant func(*model.PersistentRecord)) legacyBigintRow {
	t.Helper()
	registry := bigintDestinationRegistryWith(policy)
	tr := transform.NewPersistentRecordTransformer(registry)
	repo := newMockPersistentRecordRepository()
	rowID := uuid.New()
	record := buildPersistentRecord(t, tr, bigintDestSchema, rowID, data)
	if plant != nil {
		plant(record)
	}
	repo.storeRecord(record)
	em := mustNewEntityManager(t, tr, repo, nil, registry, createTestConfig(), nil)
	return legacyBigintRow{em: em, repo: repo, tr: tr, rowID: rowID}
}

func seedLegacyBigintRow(t *testing.T, policy forma.RequiredPolicy, attr string, image float64) legacyBigintRow {
	t.Helper()
	return seedBigintRow(t, policy, map[string]any{
		"total": int64(1), "totals": []any{int64(1), int64(2)}, "ratio": int64(1), "amount": int64(1),
		"contact": map[string]any{"total": int64(1), "name": "kept"},
	}, func(record *model.PersistentRecord) {
		plantLegacyBigintImage(t, record, attr, image)
	})
}

// bestEffortFailure is a failed operation a best-effort BatchUpdate reports
// in its result rather than as an error.
type bestEffortFailure struct{ failure forma.OperationError }

func (f bestEffortFailure) Error() string { return f.failure.Code + ": " + f.failure.Error }

// updateSurfaces are the ways an update reaches the merge: Update, an atomic
// BatchUpdate (mergeBatchUpdateRecord), and a best-effort BatchUpdate, which
// runs each operation through Update and reports its failure in the result.
// Each answers the written row's attributes.
var updateSurfaces = []struct {
	name   string
	update func(ctx context.Context, em forma.EntityManager, op *forma.EntityOperation) (map[string]any, error)
}{
	{"Update", func(ctx context.Context, em forma.EntityManager, op *forma.EntityOperation) (map[string]any, error) {
		record, err := em.Update(ctx, op)
		if err != nil {
			return nil, err
		}
		return record.Attributes, nil
	}},
	{"atomic BatchUpdate", func(ctx context.Context, em forma.EntityManager, op *forma.EntityOperation) (map[string]any, error) {
		result, err := em.BatchUpdate(ctx, &forma.BatchOperation{Atomic: true, Operations: []forma.EntityOperation{*op}})
		if err != nil {
			return nil, err
		}
		return result.Successful[0].Attributes, nil
	}},
	{"best-effort BatchUpdate", func(ctx context.Context, em forma.EntityManager, op *forma.EntityOperation) (map[string]any, error) {
		result, err := em.BatchUpdate(ctx, &forma.BatchOperation{Operations: []forma.EntityOperation{*op}})
		if err != nil {
			return nil, err
		}
		if len(result.Failed) > 0 {
			return nil, bestEffortFailure{failure: result.Failed[0]}
		}
		return result.Successful[0].Attributes, nil
	}},
}

func bigintDestUpdate(rowID uuid.UUID, updates map[string]any) *forma.EntityOperation {
	return &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: "bigint_dest", RowID: rowID},
		Type:             forma.OperationUpdate,
		Updates:          updates,
	}
}

// The documented repair for a census-reported image (#590 review) is an
// update that names the attribute, in either spelling. The stored value it
// replaces is never decoded, so the repair succeeds for every image the
// funnel refuses, including the ones the read cannot decode, under every
// required policy; it keeps the attributes it does not name, contact.total's
// sibling included; and the row then reads normally.
func TestUpdateNamingLegacyBigintImageRepairsIt(t *testing.T) {
	ctx := context.Background()
	for _, surface := range updateSurfaces {
		for _, policy := range allRequiredPolicies {
			for _, slot := range legacyBigintSlots {
				for _, legacy := range legacyBigintImages {
					t.Run(surface.name+"/"+string(policy)+"/"+slot.name+"/"+legacy.name, func(t *testing.T) {
						row := seedLegacyBigintRow(t, policy, slot.attr, legacy.image)
						got, err := surface.update(ctx, row.em, bigintDestUpdate(row.rowID, slot.repair))
						if err != nil {
							t.Fatalf("update naming %s over a stored %s image: %v", slot.attr, legacy.text, err)
						}
						for name, attrs := range map[string]map[string]any{"answered": got, "stored": row.stored(t)} {
							assertEqualValue(t, name+" "+slot.attr, slot.want, valueAtPath(attrs, slot.attr))
							assertEqualValue(t, name+" amount", int64(1), attrs["amount"])
							assertEqualValue(t, name+" contact.name", "kept", valueAtPath(attrs, "contact.name"))
						}
					})
				}
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
	for _, surface := range updateSurfaces {
		for _, slot := range legacyBigintSlots[:4] {
			for _, legacy := range legacyBigintImages {
				for _, unrelated := range unrelatedUpdates {
					t.Run(surface.name+"/"+slot.attr+"/"+legacy.name+"/"+unrelated.name, func(t *testing.T) {
						row := seedLegacyBigintRow(t, forma.RequiredPolicyOptional, slot.attr, legacy.image)
						before := row.repo.records[bigintDestSchema][row.rowID]
						_, err := surface.update(ctx, row.em, bigintDestUpdate(row.rowID, unrelated.updates))
						if err == nil {
							t.Fatalf("update of %s over a stored %s image of %s succeeded", unrelated.name, legacy.text, slot.attr)
						}
						assertLegacyBigintUpdateError(t, err, slot.attr, legacy.text, legacy.readable, row.rowID)
						if row.repo.records[bigintDestSchema][row.rowID] != before {
							t.Fatalf("failed update of %s stored a new row", unrelated.name)
						}
					})
				}
			}
		}
	}
}

// A repair whose own value the funnel refuses fails on that value, in either
// spelling: the stored image it replaces is never decoded, so an image the
// read refuses cannot answer in place of the caller's 400.
func TestRefusedRepairOverLegacyBigintImageReportsTheCallersValue(t *testing.T) {
	ctx := context.Background()
	const refused = "9007199254740993"
	repairs := map[string]map[string]any{
		"nested":  {"contact": map[string]any{"total": int64(9007199254740993)}},
		"literal": {"contact.total": int64(9007199254740993)},
	}
	for _, surface := range updateSurfaces {
		for spelling, repair := range repairs {
			for _, legacy := range legacyBigintImages {
				t.Run(surface.name+"/"+spelling+"/"+legacy.name, func(t *testing.T) {
					row := seedLegacyBigintRow(t, forma.RequiredPolicyAlways, "contact.total", legacy.image)
					before := row.repo.records[bigintDestSchema][row.rowID]
					_, err := surface.update(ctx, row.em, bigintDestUpdate(row.rowID, repair))
					assertLegacyBigintUpdateError(t, err, "contact.total", refused, true, row.rowID)
					if row.repo.records[bigintDestSchema][row.rowID] != before {
						t.Fatalf("refused repair stored a new row")
					}
				})
			}
		}
	}
}

func assertLegacyBigintUpdateError(t *testing.T, err error, attr, image string, readable bool, rowID uuid.UUID) {
	t.Helper()
	var reported bestEffortFailure
	if errors.As(err, &reported) {
		assertBestEffortLegacyFailure(t, reported.failure, attr, image, readable)
		return
	}
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

// assertBestEffortLegacyFailure checks what a best-effort batch reports: the
// funnel's published message, or nothing at all for a read-path error.
func assertBestEffortLegacyFailure(t *testing.T, failure forma.OperationError, attr, image string, readable bool) {
	t.Helper()
	if failure.Code != "UPDATE_FAILED" {
		t.Fatalf("failure code %q, want UPDATE_FAILED", failure.Code)
	}
	if !readable {
		if failure.Error != undisclosedBatchError {
			t.Fatalf("a read-path error must not be published, got %q", failure.Error)
		}
		return
	}
	for _, want := range []string{attr, image} {
		if !strings.Contains(failure.Error, want) {
			t.Fatalf("expected the reported failure to name %q, got %q", want, failure.Error)
		}
	}
}

// valueAtPath returns the value at a dotted attribute path of a read
// document.
func valueAtPath(attrs map[string]any, path string) any {
	var value any = attrs
	for _, segment := range strings.Split(path, ".") {
		object, ok := value.(map[string]any)
		if !ok {
			return nil
		}
		value = object[segment]
	}
	return value
}

func assertEqualValue(t *testing.T, what string, want, got any) {
	t.Helper()
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("%s = %#v, want %#v", what, got, want)
	}
}
