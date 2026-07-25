package pgdsn

import "testing"

// TestQuote pins the libpq escaping rule. Order matters: backslashes are doubled
// first, so the backslash introduced for an escaped quote is not doubled again.
func TestQuote(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "plain", in: "forma", want: `'forma'`},
		{name: "empty", in: "", want: `''`},
		{name: "space", in: "two words", want: `'two words'`},
		{name: "single quote", in: `p'w`, want: `'p\'w'`},
		{name: "backslash", in: `p\w`, want: `'p\\w'`},
		{name: "both, quote after backslash", in: `p'w\d`, want: `'p\'w\\d'`},
		{name: "separators libpq does not treat as delimiters", in: "a,b;c)d", want: `'a,b;c)d'`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Quote(tt.in); got != tt.want {
				t.Fatalf("Quote(%q) = %s, want %s", tt.in, got, tt.want)
			}
		})
	}
}

// TestBuild pins the full DSN shape, including that every string field is quoted
// and the port is not.
func TestBuild(t *testing.T) {
	got := Build(Params{
		Host: "h", Port: 5432, User: "u u", Password: `p'w\d`, DB: "d", SSLMode: "require",
	})
	want := `host='h' port=5432 user='u u' password='p\'w\\d' dbname='d' sslmode='require'`
	if got != want {
		t.Fatalf("Build =\n %s\nwant\n %s", got, want)
	}
}
