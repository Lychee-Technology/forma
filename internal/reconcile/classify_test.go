package reconcile

import (
	"testing"
	"time"

	"github.com/lychee-technology/forma/internal/manifest"
)

const (
	uuidA = "019bed54-48eb-7cdc-aed3-8d38ec9c1394"
	uuidB = "019bed54-48eb-7cdc-aed3-8d38ec9c1395"
	uuidC = "019bed54-48eb-7cdc-aed3-8d38ec9c1396"
)

func TestClassifyObjectKey(t *testing.T) {
	prefix := "data/7/"
	tests := []struct {
		name  string
		key   string
		class OrphanClass
		ok    bool
	}{
		{"delta uuid", "data/7/" + uuidA + ".parquet", ClassDelta, true},
		{"init base min_max", "data/7/" + uuidA + "_" + uuidB + ".parquet", ClassBaseInit, true},
		{"merged base", "data/7/base-" + uuidA + ".parquet", ClassBaseMerged, true},
		{"tmp staged", "data/7/_tmp/" + uuidA + ".parquet", ClassTmp, true},
		{"unrecognized stem", "data/7/weird.parquet", ClassUnknown, true},
		{"nested non-tmp dir", "data/7/sub/" + uuidA + ".parquet", ClassUnknown, true},
		{"non parquet ignored", "data/7/notes.txt", 0, false},
		{"outside prefix ignored", "data/8/" + uuidA + ".parquet", 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			class, ok := classifyObjectKey(prefix, tt.key)
			if ok != tt.ok {
				t.Fatalf("classifyObjectKey(%q) ok = %v, want %v", tt.key, ok, tt.ok)
			}
			if ok && class != tt.class {
				t.Fatalf("classifyObjectKey(%q) class = %v, want %v", tt.key, class, tt.class)
			}
		})
	}
}

func TestNormalizeKey(t *testing.T) {
	tests := []struct {
		name string
		path string
		want string
		ok   bool
	}{
		{"relative key", "data/7/a.parquet", "data/7/a.parquet", true},
		{"absolute same bucket", "s3://bkt/data/7/a.parquet", "data/7/a.parquet", true},
		// An empty data prefix makes cdc.Build*Path emit keys with a
		// leading slash (`/7/a.parquet`); the manifest stores the same
		// literal key, so it must NOT be stripped or listing comparison
		// breaks and --gc could delete manifest-listed objects.
		{"leading slash preserved", "/7/a.parquet", "/7/a.parquet", true},
		{"foreign bucket", "s3://other/data/7/a.parquet", "", false},
		{"glob entry", "data/7/*.parquet", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := normalizeKey("bkt", tt.path)
			if ok != tt.ok {
				t.Fatalf("normalizeKey(%q) ok = %v, want %v", tt.path, ok, tt.ok)
			}
			if ok && got != tt.want {
				t.Fatalf("normalizeKey(%q) = %q, want %q", tt.path, got, tt.want)
			}
		})
	}
}

func TestDiffSchema_OrphansDanglingUnverifiable(t *testing.T) {
	now := time.Now()
	listedDelta := ObjectInfo{Key: "data/7/" + uuidA + ".parquet", Size: 10, LastModified: now}
	listedBase := ObjectInfo{Key: "data/7/base-" + uuidB + ".parquet", Size: 20, LastModified: now}
	listedTmp := ObjectInfo{Key: "data/7/_tmp/" + uuidB + ".parquet", Size: 5, LastModified: now}
	listedKnown := ObjectInfo{Key: "data/7/" + uuidB + ".parquet", Size: 30, LastModified: now}
	listedUnknown := ObjectInfo{Key: "data/7/weird.parquet", Size: 1, LastModified: now}
	ignoredTxt := ObjectInfo{Key: "data/7/notes.txt", Size: 1, LastModified: now}

	m := &manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{
		// Absolute form must match the listed relative key (normalization).
		{Tier: "delta", Path: "s3://bkt/data/7/" + uuidB + ".parquet"},
		// Dangling: no live object under this key.
		{Tier: "base", Path: "data/7/base-" + uuidA + ".parquet"},
		// Foreign bucket: unverifiable, never dangling.
		{Tier: "base", Path: "s3://other/data/7/gone.parquet"},
		// Outside the listed schema prefix: this listing cannot prove
		// absence, so unverifiable — never dangling.
		{Tier: "base", Path: "olddata/7/legacy.parquet"},
	}}

	d := diffSchema("bkt", "data", 7,
		[]ObjectInfo{listedDelta, listedBase, listedTmp, listedKnown, listedUnknown, ignoredTxt}, m)

	assertKeys(t, "delta orphans", objectKeys(d.deltaOrphans), []string{listedDelta.Key})
	assertKeys(t, "merged base orphans", objectKeys(d.baseMergedOrphans), []string{listedBase.Key})
	assertKeys(t, "tmp orphans", objectKeys(d.tmpOrphans), []string{listedTmp.Key})
	assertKeys(t, "unknown", objectKeys(d.unknown), []string{listedUnknown.Key})
	assertKeys(t, "dangling", d.dangling, []string{"data/7/base-" + uuidA + ".parquet"})
	assertKeys(t, "unverifiable", d.unverifiable, []string{
		"s3://other/data/7/gone.parquet",
		"olddata/7/legacy.parquet",
	})
}

func TestDiffSchema_CleanManifestNoFindings(t *testing.T) {
	m := &manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{
		{Tier: "delta", Path: "data/7/" + uuidA + ".parquet"},
	}}
	d := diffSchema("bkt", "data", 7,
		[]ObjectInfo{{Key: "data/7/" + uuidA + ".parquet"}}, m)

	if len(d.deltaOrphans)+len(d.baseInitOrphans)+len(d.baseMergedOrphans)+len(d.tmpOrphans)+len(d.unknown) != 0 {
		t.Fatalf("expected no orphans, got %+v", d)
	}
	if len(d.dangling)+len(d.unverifiable) != 0 {
		t.Fatalf("expected no dangling/unverifiable, got %+v", d)
	}
}

func TestDiffSchema_EmptyDataPrefixKeysMatch(t *testing.T) {
	// cdc.BuildDeltaPath("", 7, uuid) emits "/7/{uuid}.parquet"; the same
	// literal key lands in the manifest. The diff must treat them as the
	// same object — a mismatch here turns every real file into an orphan
	// and every manifest entry into a dangling report.
	listed := ObjectInfo{Key: "/7/" + uuidA + ".parquet"}
	m := &manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{
		{Tier: "delta", Path: "/7/" + uuidA + ".parquet"},
	}}
	d := diffSchema("bkt", "", 7, []ObjectInfo{listed}, m)
	if len(d.deltaOrphans) != 0 {
		t.Fatalf("manifest-listed key reported as orphan under empty prefix: %+v", d.deltaOrphans)
	}
	if len(d.dangling) != 0 {
		t.Fatalf("live key reported dangling under empty prefix: %+v", d.dangling)
	}
}

func TestClassifyObjectKey_RequiresUUIDShapes(t *testing.T) {
	prefix := "data/7/"
	tests := []struct {
		name  string
		key   string
		class OrphanClass
	}{
		// base- prefix without a UUID is NOT a merged base — GC must never
		// delete an unrecognized object.
		{"base prefix junk", "data/7/base-foo.parquet", ClassUnknown},
		{"base prefix valid uuid", "data/7/base-" + uuidA + ".parquet", ClassBaseMerged},
		{"underscore junk", "data/7/a_b.parquet", ClassUnknown},
		{"underscore valid uuids", "data/7/" + uuidA + "_" + uuidB + ".parquet", ClassBaseInit},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			class, ok := classifyObjectKey(prefix, tt.key)
			if !ok {
				t.Fatalf("classifyObjectKey(%q) unexpectedly ignored", tt.key)
			}
			if class != tt.class {
				t.Fatalf("classifyObjectKey(%q) = %v, want %v", tt.key, class, tt.class)
			}
		})
	}
}

func objectKeys(objs []ObjectInfo) []string {
	keys := make([]string, 0, len(objs))
	for _, o := range objs {
		keys = append(keys, o.Key)
	}
	return keys
}

func assertKeys(t *testing.T, label string, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s = %v, want %v", label, got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s = %v, want %v", label, got, want)
		}
	}
}
