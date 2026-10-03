package internal

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/transform"
)

// legacyDateImages are stored images of an unbound date the #592 contract
// refuses, as a write from before it (or one around the funnel) left them.
// Every one is a read-path consistency error: the whole number past 2^53
// because the funnel would refuse it on the rewrite, the rest because they
// name no instant at all.
var legacyDateImages = []struct {
	name  string
	image float64
	rule  string
}{
	{"whole past 2^53", 9007199254740994, "stored value 9007199254740994 (287396-10-12T08:59:00.994Z) is outside the epoch milliseconds a float64 image keeps exactly (up to 9007199254740992, 2^53)"},
	{"negative whole past 2^53", -9007199254740994, "is outside the epoch milliseconds a float64 image keeps exactly"},
	{"fraction", 1000.5, "stored value 1000.5 (not a whole number of epoch milliseconds) names no epoch millisecond instant"},
	{"NaN", math.NaN(), "(not a whole number of epoch milliseconds) names no epoch millisecond instant"},
	{"infinity", math.Inf(1), "(not a whole number of epoch milliseconds) names no epoch millisecond instant"},
	{"past int64", math.Ldexp(1, 63), "stored value 9223372036854775808 (beyond any epoch millisecond instant) names no epoch millisecond instant"},
}

var (
	repairedInstant = time.UnixMilli(1704067200123).UTC()
	keptInstant     = time.UnixMilli(1700000000000).UTC()
)

// legacyDateSlots are the eav_data date destinations of
// dateDestinationRegistry, each with an update that rewrites it and the
// value the row then reads. visit.endAt is rewritten in both spellings of
// its name (#312).
var legacyDateSlots = []struct {
	name   string
	attr   string
	repair map[string]any
	want   any
}{
	{"seenAt", "seenAt", map[string]any{"seenAt": repairedInstant}, repairedInstant},
	{"seenOn", "seenOn", map[string]any{"seenOn": []any{repairedInstant}}, []any{repairedInstant}},
	{"nested visit.endAt", "visit.endAt", map[string]any{"visit": map[string]any{"endAt": repairedInstant}}, repairedInstant},
	{"literal visit.endAt", "visit.endAt", map[string]any{"visit.endAt": repairedInstant}, repairedInstant},
}

// unrelatedDateUpdates name neither the planted attribute nor anything
// containing it: a bound bigint column, and visit.endAt's sibling in both
// spellings.
var unrelatedDateUpdates = []struct {
	name    string
	updates map[string]any
}{
	{"openedAt", map[string]any{"openedAt": keptInstant}},
	{"nested sibling", map[string]any{"visit": map[string]any{"note": "x"}}},
	{"literal sibling", map[string]any{"visit.note": "x"}},
}

// plantLegacyDateImage overwrites attr's stored image in record the way a
// row read back from eav_data carries it: the float64 slot only, no exact
// sidecar. seenOn's planted image is its second item.
func plantLegacyDateImage(t *testing.T, record *model.PersistentRecord, attr string, image float64) {
	t.Helper()
	attrID := map[string]int16{"seenAt": 20, "seenOn": 25, "visit.endAt": 26}[attr]
	indices := map[string]string{"seenOn": "1"}[attr]
	for i := range record.OtherAttributes {
		if eav := &record.OtherAttributes[i]; eav.AttrID == attrID && eav.ArrayIndices == indices {
			eav.ValueNumeric = &image
			eav.ValueInt64 = nil
			return
		}
	}
	t.Fatalf("no stored row for %s[%s]", attr, indices)
}

type legacyDateRow struct {
	em    forma.EntityManager
	repo  *mockPersistentRecordRepository
	tr    model.PersistentRecordTransformer
	rowID uuid.UUID
}

// stored reads the row back from the repository the way a GET would.
func (row legacyDateRow) stored(t *testing.T) (map[string]any, error) {
	t.Helper()
	return row.tr.FromPersistentRecord(context.Background(), row.repo.records[dateDestSchema][row.rowID])
}

// seedLegacyDateRow stores a date_dest row whose visit.endAt is under
// policy, after planting image into attr underneath the write path.
func seedLegacyDateRow(t *testing.T, policy forma.RequiredPolicy, attr string, image float64) legacyDateRow {
	t.Helper()
	registry := dateDestinationRegistry().(*stubSchemaRegistry)
	meta := registry.cache["visit.endAt"]
	meta.RequiredPolicy = policy
	registry.cache["visit.endAt"] = meta
	tr := transform.NewPersistentRecordTransformer(registry)
	repo := newMockPersistentRecordRepository()
	rowID := uuid.New()
	record := buildPersistentRecord(t, tr, dateDestSchema, rowID, map[string]any{
		"seenAt": keptInstant, "seenOn": []any{keptInstant, keptInstant}, "openedAt": keptInstant,
		"visit": map[string]any{"endAt": keptInstant, "note": "kept"},
	})
	plantLegacyDateImage(t, record, attr, image)
	repo.storeRecord(record)
	em := mustNewEntityManager(t, tr, repo, nil, registry, createTestConfig(), nil)
	return legacyDateRow{em: em, repo: repo, tr: tr, rowID: rowID}
}

func dateDestUpdate(rowID uuid.UUID, updates map[string]any) *forma.EntityOperation {
	return &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: "date_dest", RowID: rowID},
		Type:             forma.OperationUpdate,
		Updates:          updates,
	}
}

// A legacy date image is a read-side consistency error from the first GET
// (#592): the read applies the rule the write admits by, so the row never
// reads as an instant it cannot be rewritten with, and never as a 4xx.
func TestGetOfLegacyDateImageIsAConsistencyError(t *testing.T) {
	for _, slot := range legacyDateSlots[:3] {
		for _, legacy := range legacyDateImages {
			t.Run(slot.attr+"/"+legacy.name, func(t *testing.T) {
				row := seedLegacyDateRow(t, forma.RequiredPolicyOptional, slot.attr, legacy.image)
				_, err := row.em.Get(context.Background(), &forma.QueryRequest{SchemaName: "date_dest", RowID: &row.rowID})
				assertLegacyDateReadError(t, err, slot.attr, legacy.rule, row.rowID)
			})
		}
	}
}

// The documented repair (docs/schema-consistency-migration.md) is an update
// that names the attribute, in either spelling. The stored image it
// replaces is never decoded (#590's merge), so the repair succeeds for
// every image the read refuses, under every required policy; it keeps the
// attributes it does not name, visit.endAt's sibling included; and the row
// then reads normally.
func TestUpdateNamingLegacyDateImageRepairsIt(t *testing.T) {
	ctx := context.Background()
	for _, surface := range updateSurfaces {
		for _, policy := range allRequiredPolicies {
			for _, slot := range legacyDateSlots {
				for _, legacy := range legacyDateImages {
					t.Run(surface.name+"/"+string(policy)+"/"+slot.name+"/"+legacy.name, func(t *testing.T) {
						row := seedLegacyDateRow(t, policy, slot.attr, legacy.image)
						got, err := surface.update(ctx, row.em, dateDestUpdate(row.rowID, slot.repair))
						if err != nil {
							t.Fatalf("update naming %s over a stored %s image: %v", slot.attr, legacy.name, err)
						}
						stored, err := row.stored(t)
						if err != nil {
							t.Fatalf("read of the repaired row: %v", err)
						}
						for name, attrs := range map[string]map[string]any{"answered": got, "stored": stored} {
							assertEqualValue(t, name+" "+slot.attr, slot.want, valueAtPath(attrs, slot.attr))
							assertEqualValue(t, name+" openedAt", keptInstant, attrs["openedAt"])
							assertEqualValue(t, name+" visit.note", "kept", valueAtPath(attrs, "visit.note"))
						}
					})
				}
			}
		}
	}
}

// An update that does not name the attribute carries its stored value into
// the write, so the read of that value fails first: a plain read-path
// consistency error naming the attribute, the row and the rule, never the
// caller's invalid input (the #587 review defect, where the read accepted
// 2^53+2 and the rewrite then refused it as the caller's 400). A
// best-effort batch publishes nothing of it, and the row is left as stored
// for the operator's repair.
func TestUnrelatedUpdateOverLegacyDateImageFails(t *testing.T) {
	ctx := context.Background()
	for _, surface := range updateSurfaces {
		for _, slot := range legacyDateSlots[:3] {
			for _, legacy := range legacyDateImages {
				for _, unrelated := range unrelatedDateUpdates {
					t.Run(surface.name+"/"+slot.attr+"/"+legacy.name+"/"+unrelated.name, func(t *testing.T) {
						row := seedLegacyDateRow(t, forma.RequiredPolicyOptional, slot.attr, legacy.image)
						before := row.repo.records[dateDestSchema][row.rowID]
						_, err := surface.update(ctx, row.em, dateDestUpdate(row.rowID, unrelated.updates))
						var reported bestEffortFailure
						if errors.As(err, &reported) {
							assertEqualValue(t, "failure code", "UPDATE_FAILED", reported.failure.Code)
							assertEqualValue(t, "published failure", undisclosedBatchError, reported.failure.Error)
						} else {
							assertLegacyDateReadError(t, err, slot.attr, legacy.rule, row.rowID)
						}
						if row.repo.records[dateDestSchema][row.rowID] != before {
							t.Fatalf("failed update of %s stored a new row", unrelated.name)
						}
					})
				}
			}
		}
	}
}

// The last images the destination keeps exactly read and rewrite: an
// unrelated update carries ±2^53 through the merged document unchanged.
func TestDateImageAt2p53SurvivesAnUnrelatedUpdate(t *testing.T) {
	ctx := context.Background()
	for _, surface := range updateSurfaces {
		for _, slot := range legacyDateSlots[:3] {
			for _, ms := range []int64{9007199254740992, -9007199254740992} {
				t.Run(surface.name+"/"+slot.attr+"/"+time.UnixMilli(ms).UTC().Format(time.RFC3339), func(t *testing.T) {
					row := seedLegacyDateRow(t, forma.RequiredPolicyOptional, slot.attr, float64(ms))
					got, err := surface.update(ctx, row.em, dateDestUpdate(row.rowID, map[string]any{"visit.note": "x"}))
					if err != nil {
						t.Fatalf("unrelated update over a stored %d image of %s: %v", ms, slot.attr, err)
					}
					want := any(time.UnixMilli(ms).UTC())
					if slot.attr == "seenOn" {
						want = []any{keptInstant, want}
					}
					assertEqualValue(t, "answered "+slot.attr, want, valueAtPath(got, slot.attr))
					assertEqualValue(t, "answered visit.note", "x", valueAtPath(got, "visit.note"))
				})
			}
		}
	}
}

func assertLegacyDateReadError(t *testing.T, err error, attr, rule string, rowID uuid.UUID) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected a read consistency error for %s", attr)
	}
	if errors.Is(err, forma.ErrInvalidInput) {
		t.Fatalf("a stored date image is the operator's, not the caller's: %v", err)
	}
	for _, want := range []string{"'" + attr + "'", rowID.String(), rule} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("expected the error to name %q, got %v", want, err)
		}
	}
}
