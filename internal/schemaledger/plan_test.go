package schemaledger

import (
	"strings"
	"testing"

	"github.com/lychee-technology/forma"
)

func gen(number int64, hash string, entries ...Entry) Generation {
	return Generation{SchemaID: 9, Number: number, Hash: hash, Entries: entries}
}

func decl(hash string, entries ...Entry) Declaration {
	return Declaration{SchemaName: "orders", SchemaID: 9, Hash: hash, Entries: entries}
}

func TestPlanApplyWithTheActiveHashPlansNothing(t *testing.T) {
	active := gen(3, "h1", bound(1, "a", forma.ValueTypeBigInt, "bigint_1"))
	plan := PlanApply(active, decl("h1", bound(1, "a", forma.ValueTypeBigInt, "bigint_1")))
	if !plan.Unchanged() {
		t.Fatalf("a declaration with the active hash must plan nothing, got %+v", plan)
	}
}

// TestPlanApplyKeepsARemovedAttributeAsRetired: a removed attribute's entry
// is kept permanently, so its id can never be rebound to another type.
func TestPlanApplyKeepsARemovedAttributeAsRetired(t *testing.T) {
	active := gen(2, "h1", eav(1, "a", forma.ValueTypeText), eav(2, "b", forma.ValueTypeInteger))
	plan := PlanApply(active, decl("h2", eav(1, "a", forma.ValueTypeText), eav(3, "c", forma.ValueTypeBigInt)))
	if plan.Unchanged() || len(plan.Refusals()) != 0 {
		t.Fatalf("an add and a remove must plan an allowed generation, got refusals %v", plan.Refusals())
	}
	if len(plan.Entries) != 3 {
		t.Fatalf("plan records %d entries, want 3 (a, the retired b, c): %+v", len(plan.Entries), plan.Entries)
	}
	kept := plan.Entries[1]
	if kept.AttrID != 2 || !kept.Retired || kept.ValueType != forma.ValueTypeInteger {
		t.Fatalf("the removed attribute is recorded as %+v, want b kept retired with its recorded type", kept)
	}
	if len(plan.Changes) != 2 || plan.Changes[0].New.AttrID != 2 || plan.Changes[1].New.AttrID != 3 {
		t.Fatalf("changes = %+v, want the removal of b and the addition of c, by id", plan.Changes)
	}

	// The next apply carries it again, without reporting a change.
	next := PlanApply(Generation{SchemaID: 9, Number: 3, Hash: "h2", Entries: plan.Entries},
		decl("h3", eav(1, "a", forma.ValueTypeText), eav(3, "c", forma.ValueTypeBigInt), eav(4, "d", forma.ValueTypeText)))
	if len(next.Entries) != 4 || !next.Entries[1].Retired {
		t.Fatalf("a later generation must keep the retired entry, got %+v", next.Entries)
	}
	if len(next.Changes) != 1 || next.Changes[0].New.AttrID != 4 {
		t.Fatalf("changes = %+v, want only the addition of d", next.Changes)
	}
}

// TestPlanApplyRefusesAReAddUnderARetiredID: an id stays bound to its type
// after removal, so re-adding it under another type is judged as a change.
func TestPlanApplyRefusesAReAddUnderARetiredID(t *testing.T) {
	retired := eav(2, "b", forma.ValueTypeNumeric)
	retired.Retired = true
	plan := PlanApply(gen(4, "h1", retired), decl("h2", eav(2, "b", forma.ValueTypeBigInt)))
	refusals := plan.Refusals()
	if len(refusals) != 1 || !strings.Contains(refusals[0], "narrows") {
		t.Fatalf("re-adding a retired numeric id as bigint must be refused as a narrowing, got %v", refusals)
	}
}

// TestPlanApplyReportsEveryRefusalAndWarning: a refusal names the schema,
// the attribute, the old and new types and the remediation.
func TestPlanApplyReportsEveryRefusalAndWarning(t *testing.T) {
	active := gen(1, "h1",
		bound(1, "price", forma.ValueTypeNumeric, "double_1"),
		bound(2, "total", forma.ValueTypeBigInt, "bigint_1"),
		eav(3, "count", forma.ValueTypeSmallInt))
	plan := PlanApply(active, decl("h2",
		bound(1, "price", forma.ValueTypeBigInt, "double_1"),
		bound(2, "total", forma.ValueTypeNumeric, "bigint_1"),
		eav(3, "count", forma.ValueTypeInteger)))
	refusals := plan.Refusals()
	if len(refusals) != 2 {
		t.Fatalf("refusals = %v, want price and count", refusals)
	}
	for i, want := range [][]string{
		{"schema orders (id 9)", "price (id 1)", "numeric bound to double_1", "bigint bound to double_1", "#730"},
		{"count (id 3)", "smallint EAV-only", "integer EAV-only", "#729"},
	} {
		for _, part := range want {
			if !strings.Contains(refusals[i], part) {
				t.Fatalf("refusal %q does not mention %q", refusals[i], part)
			}
		}
	}
	if warnings := plan.Warnings(); len(warnings) != 1 || !strings.Contains(warnings[0], "total (id 2)") {
		t.Fatalf("warnings = %v, want the bigint_1 rounding warning for total", warnings)
	}
	if got := len(plan.Describe()); got != 3 {
		t.Fatalf("Describe has %d lines, want one per change", got)
	}
}

// TestPlanApplyRecordsAFileOnlyEdit: an edit that changes no attribute, such
// as a new index in the JSON Schema file, is still a new generation.
func TestPlanApplyRecordsAFileOnlyEdit(t *testing.T) {
	active := gen(1, "h1", eav(1, "a", forma.ValueTypeText))
	plan := PlanApply(active, decl("h2", eav(1, "a", forma.ValueTypeText)))
	if plan.Unchanged() || len(plan.Changes) != 0 || plan.Hash != "h2" {
		t.Fatalf("a file-only edit must plan a new generation with no attribute change, got %+v", plan)
	}
}

func TestContainedAllowsAnOlderProcess(t *testing.T) {
	active := gen(5, "h", eav(1, "a", forma.ValueTypeText), eav(2, "b", forma.ValueTypeBigInt))
	if problems := Contained(decl("old", eav(1, "a", forma.ValueTypeText)), active); len(problems) != 0 {
		t.Fatalf("lacking an attribute the generation holds must be allowed, got %v", problems)
	}
	removed := eav(2, "b", forma.ValueTypeBigInt)
	removed.Retired = true
	retiredActive := gen(6, "h", eav(1, "a", forma.ValueTypeText), removed)
	if problems := Contained(decl("old", eav(1, "a", forma.ValueTypeText), eav(2, "b", forma.ValueTypeBigInt)), retiredActive); len(problems) != 0 {
		t.Fatalf("a process from before a removal must be allowed, got %v", problems)
	}
}

func TestContainedRefusesAMissingOrDifferingAttribute(t *testing.T) {
	active := gen(5, "h", bound(1, "a", forma.ValueTypeInteger, "integer_1"), list(2, "tags", forma.ValueTypeText))
	cases := []struct {
		name string
		decl Declaration
		want string
	}{
		{"missing", decl("x", eav(3, "c", forma.ValueTypeText)), "not in active generation 5"},
		{"type", decl("x", bound(1, "a", forma.ValueTypeSmallInt, "integer_1")), "declared as smallint bound to integer_1"},
		{"binding", decl("x", eav(1, "a", forma.ValueTypeInteger)), "records integer bound to integer_1"},
		{"items", decl("x", list(2, "tags", forma.ValueTypeInteger)), "list<integer>"},
		{"name", decl("x", bound(1, "z", forma.ValueTypeInteger, "integer_1")), "records it as a"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			problems := Contained(tc.decl, active)
			if len(problems) != 1 || !strings.Contains(problems[0], tc.want) {
				t.Fatalf("Contained = %v, want one problem mentioning %q", problems, tc.want)
			}
		})
	}
}
