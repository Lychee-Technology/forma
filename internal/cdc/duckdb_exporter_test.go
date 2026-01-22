package cdc

import (
	"strings"
	"testing"
)

func TestEscapePgConnAndS3(t *testing.T) {
	pg := "host=foo user=bar password=pa'ss dbname=baz"
	s3 := "s3://bucket/prefix/with'quote/tmp.parquet"
	pgEsc := strings.ReplaceAll(pg, "'", "''")
	s3Esc := strings.ReplaceAll(s3, "'", "''")
	if !strings.Contains(pgEsc, "''") {
		t.Fatalf("pg escaped not containing doubled quotes: %s", pgEsc)
	}
	if !strings.Contains(s3Esc, "''") {
		t.Fatalf("s3 escaped not containing doubled quotes: %s", s3Esc)
	}
}
