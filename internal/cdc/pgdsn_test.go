package cdc

import (
	"testing"
)

func TestBuildPGDSN_QuotesValues(t *testing.T) {
	got := BuildPGDSN(PGDSNParams{Host: "h", Port: 5432, User: "u u", Password: `p'w\d`, DB: "d", SSLMode: "require"})
	want := `host='h' port=5432 user='u u' password='p\'w\\d' dbname='d' sslmode='require'`
	if got != want {
		t.Fatalf("BuildPGDSN = %s, want %s", got, want)
	}
}
