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

// TestManifestLayoutIsTheWritersPrefix pins Env.manifestLayout, the layout
// the fault helpers stage test-made generations under: it is the data prefix
// cdc-flush and cdc-init write under (CDCConfig.S3Prefix), which is the Env's
// own S3 scope, so a staged object never lands outside it (#427, #896).
func TestManifestLayoutIsTheWritersPrefix(t *testing.T) {
	env := &Env{Cluster: &Cluster{}, S3Prefix: "e2e/run/env3"}
	env.CDC = env.buildCDCConfig()
	if got := env.manifestLayout().Prefix; got != "e2e/run/env3" || got != env.CDC.S3Prefix {
		t.Errorf("manifestLayout().Prefix = %q, want the Env's data prefix e2e/run/env3 (CDC prefix %q)", got, env.CDC.S3Prefix)
	}
}
