package conditionexpr

import (
	"testing"
	"time"
)

func TestParseOperatorValueStrict(t *testing.T) {
	tests := []struct {
		name      string
		raw       string
		wantOp    string
		wantValue string
		wantErr   bool
	}{
		{name: "no separator", raw: "abc", wantOp: "equals", wantValue: "abc"},
		{name: "with operator", raw: "gt:10", wantOp: "gt", wantValue: "10"},
		{name: "empty operator", raw: ":10", wantErr: true},
		{name: "empty value", raw: "gt:", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseOperatorValueStrict(tt.raw)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q", tt.raw)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.Operator != tt.wantOp || got.Value != tt.wantValue {
				t.Fatalf("unexpected parse result %+v", got)
			}
		})
	}
}

func TestToSQLOperator(t *testing.T) {
	got, err := ToSQLOperator("eq", "42")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.SQLOperator != "=" || got.Value != "42" {
		t.Fatalf("unexpected sql operator result %+v", got)
	}

	got, err = ToSQLOperator("contains", "x")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.SQLOperator != "LIKE" || got.Value != "%x%" {
		t.Fatalf("unexpected LIKE result %+v", got)
	}
}

func TestParseNumeric(t *testing.T) {
	v, err := ParseNumeric("10")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if n, ok := v.(int64); !ok || n != 10 {
		t.Fatalf("expected int64(10), got %v", v)
	}

	v, err = ParseNumeric("10.5")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if n, ok := v.(float64); !ok || n != 10.5 {
		t.Fatalf("expected float64(10.5), got %v", v)
	}

	if _, err := ParseNumeric("abc"); err == nil {
		t.Fatalf("expected parse error")
	}
}

func TestParseRFC3339OrUnixMs(t *testing.T) {
	iso := "2023-03-14T15:09:26Z"
	ts, err := ParseRFC3339OrUnixMs(iso)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if ts.Format(time.RFC3339) != iso {
		t.Fatalf("unexpected time %v", ts)
	}

	ts, err = ParseRFC3339OrUnixMs("1700000000000")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if ts.UnixMilli() != 1700000000000 {
		t.Fatalf("unexpected unix ms %d", ts.UnixMilli())
	}

	if _, err := ParseRFC3339OrUnixMs("bad"); err == nil {
		t.Fatalf("expected parse error")
	}
}
