package production

import "testing"

// TestParquetURIRendersKeyVerbatim pins the leading slash no scenario can
// exercise: MinIO rejects such keys and every Env has a non-empty prefix, so
// the harness never sees one (#540).
func TestParquetURIRendersKeyVerbatim(t *testing.T) {
	env := &Env{Cluster: &Cluster{Bucket: "bucket"}}
	cases := map[string]string{
		"e2e/run/env0/1/a.parquet": "s3://bucket/e2e/run/env0/1/a.parquet",
		"/1/a.parquet":             "s3://bucket//1/a.parquet",
	}
	for key, want := range cases {
		if got := env.ParquetURI(key); got != want {
			t.Errorf("ParquetURI(%q) = %q, want %q", key, got, want)
		}
	}
}
