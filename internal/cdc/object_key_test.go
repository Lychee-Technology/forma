package cdc

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizeObjectKey(t *testing.T) {
	cases := []struct {
		path   string
		key    string
		wantOK bool
	}{
		{"delta/1/a.parquet", "delta/1/a.parquet", true},
		{"s3://bkt/delta/1/a.parquet", "delta/1/a.parquet", true},
		{"s3://other/delta/1/a.parquet", "", false},
		{"s3://bkt-2/delta/1/a.parquet", "", false},
		{"delta/1/*.parquet", "", false},
		{"delta/1/a?.parquet", "", false},
		{"delta/1/[ab].parquet", "", false},
	}
	for _, c := range cases {
		key, ok := NormalizeObjectKey("bkt", c.path)
		require.Equal(t, c.wantOK, ok, c.path)
		require.Equal(t, c.key, key, c.path)
	}
}
