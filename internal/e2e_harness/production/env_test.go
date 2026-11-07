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

// TestManifestKeyFollowsResolver pins Env.manifestKey, the one source of the
// manifest object key the e2e tests list, fault and compare against (#427):
// the default Env config yields the "{S3Prefix}/manifest/{id}.json" layout
// the tests used to hardcode, a ManifestTemplate override moves the key with
// it, and an Env built WithoutManifest is refused rather than resolved to an
// unprefixed key outside its S3 scope.
func TestManifestKeyFollowsResolver(t *testing.T) {
	env := &Env{Cluster: &Cluster{}, S3Prefix: "e2e/run/env3"}
	env.CDC = env.buildCDCConfig()
	if got, err := env.manifestKey(22); err != nil || got != "e2e/run/env3/manifest/22.json" {
		t.Errorf("default manifestKey(22) = %q, %v; want e2e/run/env3/manifest/22.json", got, err)
	}

	env.CDC.ManifestTemplate = "manifests/v2/{{.SchemaID}}.json"
	if got, err := env.manifestKey(22); err != nil || got != "e2e/run/env3/manifests/v2/22.json" {
		t.Errorf("overridden manifestKey(22) = %q, %v; want e2e/run/env3/manifests/v2/22.json", got, err)
	}

	bare := &Env{Cluster: &Cluster{}, S3Prefix: "e2e/run/env4"}
	bare.opts.withoutManifest = true
	bare.CDC = bare.buildCDCConfig()
	if got, err := bare.manifestKey(22); err == nil {
		t.Errorf("manifestKey on a WithoutManifest Env = %q, want an error", got)
	}
}
