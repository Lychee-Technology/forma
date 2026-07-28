package redact

import (
	"strings"
	"testing"
)

// TestConnStringPassword_Forms is the package's own gate.
//
// The #290 regressions live in internal/cdc/redact_test.go because they need that
// package's DSN builder, and the #301 boundary cases live in internal/httpapi.
// Neither owns this pattern. Without these, deleting either consumer's suite would
// silently remove the only coverage of a security control.
func TestConnStringPassword_Forms(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "bare value",
			in:   "host=h port=5432 user=u password=s3cr3t dbname=d",
			want: "host=h port=5432 user=u password=***REDACTED*** dbname=d",
		},
		{
			name: "bare value at end of string",
			in:   "host=h password=s3cr3t",
			want: "host=h password=***REDACTED***",
		},
		{
			name: "libpq-quoted value",
			in:   `host='h' password='s3cr3t' dbname='d'`,
			want: "host='h' password=***REDACTED*** dbname='d'",
		},
		{
			name: "libpq-quoted value containing an escaped quote and backslash",
			in:   `host='h' password='p\'w\\d' dbname='d'`,
			want: "host='h' password=***REDACTED*** dbname='d'",
		},
		{
			name: "EscapeLiteral doubled-quote form inside a SQL literal",
			in:   `ATTACH 'host=''h'' password=''s3cr3t'' dbname=''d''' AS pg`,
			want: `ATTACH 'host=''h'' password=***REDACTED*** dbname=''d''' AS pg`,
		},
		{
			name: "case-insensitive key, key case preserved",
			in:   "PASSWORD=s3cr3t dbname=d",
			want: "PASSWORD=***REDACTED*** dbname=d",
		},
		{
			name: "every occurrence is replaced",
			in:   "a password=one b password=two c",
			want: "a password=***REDACTED*** b password=***REDACTED*** c",
		},
		{
			name: "empty value leaves following keys intact",
			in:   "password= dbname=d",
			want: "password=***REDACTED*** dbname=d",
		},
		{
			name: "no password at all is returned unchanged",
			in:   "host=h port=5432 dbname=d",
			want: "host=h port=5432 dbname=d",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ConnStringPassword(tt.in); got != tt.want {
				t.Fatalf("ConnStringPassword(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

// TestConnStringPassword_UnquotedSeparators pins the branch-3 widening.
//
// The pattern used to terminate the unquoted value on `;`, `,` and `)` as well as
// whitespace. None of those is a separator in a libpq keyword/value DSN, so a
// password containing one had its tail survive redaction — and `,` `;` `)` are
// ordinary in generated secrets. Each case here leaked before the widening.
func TestConnStringPassword_UnquotedSeparators(t *testing.T) {
	// Fragments are distinctive strings that appear nowhere in the surrounding
	// DSN scaffolding, so a partial match cannot hide behind "password=" or
	// "dbname=".
	for _, secret := range []string{
		"ZQX,VKW", "ZQX;VKW", "ZQX)VKW", "ZQX(VKW", "ZQX]VKW", "ZQX,VKW;JHT)MRN",
	} {
		t.Run(secret, func(t *testing.T) {
			got := ConnStringPassword("host=h password=" + secret + " dbname=d")
			if strings.Contains(got, secret) {
				t.Fatalf("password %q survived redaction: %q", secret, got)
			}
			// A partial match that stops at the separator is still a leak, and is
			// exactly what the old terminator set produced.
			for _, frag := range strings.FieldsFunc(secret, func(r rune) bool {
				return strings.ContainsRune(",;()]", r)
			}) {
				if strings.Contains(got, frag) {
					t.Fatalf("fragment %q of password %q survived redaction: %q", frag, secret, got)
				}
			}
			if !strings.Contains(got, "dbname=d") {
				t.Fatalf("redaction consumed the following keys: %q", got)
			}
		})
	}
}

// TestConnStringPassword_KnownGaps records what the pattern does not cover, so the
// boundary of the control is written down rather than rediscovered.
func TestConnStringPassword_KnownGaps(t *testing.T) {
	t.Run("whitespace around the equals is not matched", func(t *testing.T) {
		in := "password = s3cr3t dbname=d"
		if got := ConnStringPassword(in); got != in {
			t.Fatalf("gap closed — update the package docs and this test: %q", got)
		}
	})

	t.Run("an unquoted value containing a space cannot be fully matched", func(t *testing.T) {
		// Nothing marks where such a value ends. This is why #301 made both DSN
		// builders quote their values: the fix belongs at the producer.
		got := ConnStringPassword("host=h password=my secret dbname=d")
		if !strings.Contains(got, "secret") {
			t.Fatalf("residual closed — update the package docs and this test: %q", got)
		}
		// Quoting at the producer is what actually covers it.
		quoted := ConnStringPassword(`host='h' password='my secret' dbname='d'`)
		if strings.Contains(quoted, "secret") {
			t.Fatalf("the quoted form must be fully redacted, got %q", quoted)
		}
	})
}

// TestConnStringPassword_KeepsOperatorDetail is the other half of the contract:
// only the credential goes. A redacted log line is usually the operator's only
// remaining copy of the diagnosis.
func TestConnStringPassword_KeepsOperatorDetail(t *testing.T) {
	in := `execute duckdb query: schema 22 manifest lists base/schema_22/k.parquet: ` +
		`HTTP Error: Unable to connect to URL "https://bucket.s3.amazonaws.com/k.parquet": 404 (Not Found)`
	if got := ConnStringPassword(in); got != in {
		t.Fatalf("altered an error with no credential in it:\n got: %s\nwant: %s", got, in)
	}
}

// TestConnStringPassword_LeavesUnrelatedWordsAlone guards against the pattern
// widening into prose that merely mentions passwords.
func TestConnStringPassword_LeavesUnrelatedWordsAlone(t *testing.T) {
	for _, in := range []string{
		`password authentication failed for user "u"`,
		"the password_hash column is not populated",
		"missing attribute 'password' in schema 'account'",
	} {
		if got := ConnStringPassword(in); got != in {
			t.Fatalf("ConnStringPassword(%q) = %q, want it unchanged", in, got)
		}
	}
}
