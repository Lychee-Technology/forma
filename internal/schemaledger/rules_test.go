package schemaledger

import (
	"strings"
	"testing"

	"github.com/lychee-technology/forma"
)

func eav(id int16, name string, vt forma.ValueType) Entry {
	return Entry{AttrID: id, Name: name, ValueType: vt}
}

func bound(id int16, name string, vt forma.ValueType, col forma.MainColumn) Entry {
	return Entry{AttrID: id, Name: name, ValueType: vt, Column: col, Encoding: forma.MainColumnEncodingDefault}
}

func list(id int16, name string, items forma.ValueType) Entry {
	return Entry{AttrID: id, Name: name, ValueType: forma.ValueTypeList, ItemsType: items}
}

// TestJudgeFollowsTheD2Table pins every row of the D2 table
// (docs/schema-evolution.md) and the owner's rulings for the cases it leaves
// open: whether a change is allowed follows from the two entries alone.
func TestJudgeFollowsTheD2Table(t *testing.T) {
	const (
		small, integer, big, num = forma.ValueTypeSmallInt, forma.ValueTypeInteger, forma.ValueTypeBigInt, forma.ValueTypeNumeric
	)
	cases := []struct {
		name      string
		old, next Entry
		want      Verdict
		reason    string
	}{
		{"unchanged", bound(1, "a", big, "bigint_1"), bound(1, "a", big, "bigint_1"), Unchanged, ""},
		{"smallint to integer, bound", bound(1, "a", small, "smallint_1"), bound(1, "a", integer, "smallint_1"), Allowed, ""},
		{"integer to bigint, bound", bound(1, "a", integer, "integer_1"), bound(1, "a", big, "integer_1"), Allowed, ""},
		{"smallint to bigint, bound", bound(1, "a", small, "smallint_1"), bound(1, "a", big, "smallint_1"), Allowed, ""},
		{"smallint to numeric, double", bound(1, "a", small, "double_1"), bound(1, "a", num, "double_1"), Allowed, ""},
		{"integer to numeric, bound", bound(1, "a", integer, "integer_1"), bound(1, "a", num, "integer_1"), Allowed, ""},
		{"bigint to numeric, EAV-only", eav(1, "a", big), eav(1, "a", num), Allowed, ""},
		{"bigint to numeric, double", bound(1, "a", big, "double_1"), bound(1, "a", num, "double_1"), Allowed, ""},
		{"bigint to numeric, integer column", bound(1, "a", big, "integer_1"), bound(1, "a", num, "integer_1"), Allowed, ""},
		{"bigint to numeric, bigint column", bound(1, "a", big, "bigint_1"), bound(1, "a", num, "bigint_1"), AllowedWithWarning, "±2^53"},
		{"numeric to bigint", bound(1, "a", num, "double_1"), bound(1, "a", big, "double_1"), Refused, "narrows"},
		{"numeric to smallint, EAV-only", eav(1, "a", num), eav(1, "a", small), Refused, "narrows"},
		{"bigint to integer", bound(1, "a", big, "bigint_1"), bound(1, "a", integer, "bigint_1"), Refused, "#730"},
		{"integer to smallint", bound(1, "a", integer, "integer_1"), bound(1, "a", small, "integer_1"), Refused, "narrows"},
		{"EAV-only smallint to integer", eav(1, "a", small), eav(1, "a", integer), Refused, "#729"},
		{"EAV-only integer to numeric", eav(1, "a", integer), eav(1, "a", num), Refused, "#729"},
		{"EAV-only smallint to bigint", eav(1, "a", small), eav(1, "a", big), Refused, "#729"},
		// #729 gates only the widenings: a narrowing or a family change of an
		// EAV-only integer stays refused after it, so its reason names #730.
		{"EAV-only integer to smallint", eav(1, "a", integer), eav(1, "a", small), Refused, "narrows"},
		{"EAV-only integer to text", eav(1, "a", integer), eav(1, "a", forma.ValueTypeText), Refused, "#730"},
		{"EAV-only smallint to date", eav(1, "a", small), eav(1, "a", forma.ValueTypeDate), Refused, "#730"},
		{"family change", bound(1, "a", big, "bigint_1"), bound(1, "a", forma.ValueTypeDate, "bigint_1"), Refused, "not a type change D2 allows"},
		{"date to datetime", bound(1, "a", forma.ValueTypeDate, "bigint_1"), bound(1, "a", forma.ValueTypeDateTime, "bigint_1"), Refused, "not a type change"},
		{"text to uuid", eav(1, "a", forma.ValueTypeText), eav(1, "a", forma.ValueTypeUUID), Refused, "not a type change"},
		{"items_type change", list(1, "a", small), list(1, "a", integer), Refused, "items_type"},
		{"scalar to list", eav(1, "a", forma.ValueTypeText), list(1, "a", forma.ValueTypeText), Refused, "not a type change"},
		{"EAV to bound, same type", eav(1, "a", integer), bound(1, "a", integer, "integer_1"), Allowed, ""},
		{"bound to EAV, same type", bound(1, "a", big, "bigint_1"), eav(1, "a", big), Allowed, ""},
		{"encoding change", bound(1, "a", forma.ValueTypeBool, "smallint_1"),
			Entry{AttrID: 1, Name: "a", ValueType: forma.ValueTypeBool, Column: "smallint_1", Encoding: forma.MainColumnEncodingBoolInt}, Allowed, ""},
		{"type and binding at once", eav(1, "a", big), bound(1, "a", num, "double_1"), Refused, "separately"},
		{"id rebound to another name", bound(1, "a", big, "bigint_1"), bound(1, "b", big, "bigint_1"), Refused, "never rebound"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Judge(tc.old, tc.next)
			if got.Verdict != tc.want {
				t.Fatalf("Judge(%s, %s) = %s (%q), want %s", tc.old.describe(), tc.next.describe(), got.Verdict, got.Reason, tc.want)
			}
			if !strings.Contains(got.Reason, tc.reason) {
				t.Fatalf("Judge reason %q does not mention %q", got.Reason, tc.reason)
			}
		})
	}
}

// TestBigintToNumericWarningNamesTheColumn: the warning is what the operator
// sees before rounding becomes possible, so it names the column it concerns
// and says the change is one-way.
func TestBigintToNumericWarningNamesTheColumn(t *testing.T) {
	got := Judge(bound(7, "amount", forma.ValueTypeBigInt, "bigint_3"), bound(7, "amount", forma.ValueTypeNumeric, "bigint_3"))
	for _, want := range []string{"bigint_3", "rounded", "one-way", "#730"} {
		if !strings.Contains(got.Reason, want) {
			t.Fatalf("warning %q does not mention %q", got.Reason, want)
		}
	}
}

func TestEntryOfRecordsTheDefaultEncodingAndListItems(t *testing.T) {
	bound := EntryOf(forma.AttributeMetadata{AttributeName: "a", AttributeID: 3, ValueType: forma.ValueTypeBigInt,
		ColumnBinding: &forma.MainColumnBinding{ColumnName: "bigint_1"}})
	if bound.Encoding != forma.MainColumnEncodingDefault {
		t.Fatalf("an omitted encoding is recorded as %q, want %q", bound.Encoding, forma.MainColumnEncodingDefault)
	}
	items := EntryOf(forma.AttributeMetadata{AttributeName: "l", AttributeID: 4, ValueType: forma.ValueTypeList})
	if items.ItemsType != forma.ValueTypeText {
		t.Fatalf("a list with no items_type is recorded with items %q, want the effective %q", items.ItemsType, forma.ValueTypeText)
	}
	scalar := EntryOf(forma.AttributeMetadata{AttributeName: "s", AttributeID: 5, ValueType: forma.ValueTypeText, ItemsType: forma.ValueTypeInteger})
	if scalar.ItemsType != "" {
		t.Fatalf("a scalar records items %q, want none", scalar.ItemsType)
	}
}

func TestContentHashSeparatesTheTwoFiles(t *testing.T) {
	if ContentHash([]byte("ab"), []byte("c")) == ContentHash([]byte("a"), []byte("bc")) {
		t.Fatal("moving bytes between the two files must change the hash")
	}
	// Pinned: recorded generations carry this hash, and apply-schema compares
	// a declaration's hash with the active one's to plan nothing.
	const want = "facdde7abf1eac5b301273ab2e282f79bab0100f058833a7b8bdf9f80741e149"
	if got := ContentHash([]byte("a"), []byte("b")); got != want {
		t.Fatalf("ContentHash = %q, want %q", got, want)
	}
}
