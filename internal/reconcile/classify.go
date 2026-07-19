// Package reconcile diffs a schema's S3 parquet objects against its manifest
// (issue #203). It reports orphans — live objects the manifest does not list,
// classified by filename shape — and dangling entries — manifest paths whose
// object is gone. Optional repair appends delta-shaped orphans back to the
// manifest (the #197 flush failure mode: rows already marked flushed, data
// exists only in the orphaned file); optional GC deletes base-shaped and
// _tmp/ orphans left behind by compaction rewrites (#188).
package reconcile

import (
	"fmt"
	"strings"

	"github.com/google/uuid"

	"github.com/lychee-technology/forma/internal/manifest"
)

// OrphanClass is the filename-shape class of an unlisted parquet object. The
// class decides the recovery direction: delta orphans carry data that exists
// nowhere else and are appended back to the manifest, base and _tmp orphans
// are compaction leftovers whose data already lives in the merged base and
// are garbage-collected. Unknown shapes are reported and never touched.
type OrphanClass int

const (
	ClassDelta OrphanClass = iota
	ClassBase
	ClassTmp
	ClassUnknown
)

// schemaDataPrefix returns the S3 listing prefix owning one schema's parquet
// objects, mirroring cdc.BuildDeltaPath / BuildBasePath layout.
func schemaDataPrefix(dataPrefix string, schemaID int16) string {
	return fmt.Sprintf("%s/%d/", strings.TrimSuffix(dataPrefix, "/"), schemaID)
}

// classifyObjectKey classifies a listed object key relative to the schema
// data prefix. ok is false for keys the reconciler ignores entirely:
// non-parquet objects and keys outside the prefix.
func classifyObjectKey(prefix, key string) (OrphanClass, bool) {
	rel, found := strings.CutPrefix(key, prefix)
	if !found || !strings.HasSuffix(rel, ".parquet") {
		return 0, false
	}
	if dir, _, nested := strings.Cut(rel, "/"); nested {
		if dir == "_tmp" {
			return ClassTmp, true
		}
		return ClassUnknown, true
	}
	stem := strings.TrimSuffix(rel, ".parquet")
	switch {
	case strings.HasPrefix(stem, "base-"):
		return ClassBase, true // cdc.BuildMergedBasePath: base-{uuid}.parquet
	case strings.Contains(stem, "_"):
		return ClassBase, true // cdc.BuildBasePath: {minRowID}_{maxRowID}.parquet
	case uuid.Validate(stem) == nil:
		return ClassDelta, true // cdc.BuildDeltaPath: {uuid}.parquet
	default:
		return ClassUnknown, true
	}
}

// normalizeKey reduces a manifest FileEntry.Path to a bucket-relative key.
// Manifest paths appear both bucket-relative and as absolute s3:// URIs
// (manifest/query_source.go tolerates either). ok is false for paths whose
// existence this bucket's listing cannot prove — foreign-bucket URIs and
// glob entries — which must surface as unverifiable, never as dangling.
func normalizeKey(bucket, path string) (string, bool) {
	if strings.ContainsAny(path, "*?[") {
		return "", false
	}
	if strings.HasPrefix(path, "s3://") {
		key, found := strings.CutPrefix(path, "s3://"+bucket+"/")
		if !found {
			return "", false
		}
		return key, true
	}
	return strings.TrimPrefix(path, "/"), true
}

// diffResult is one schema's raw two-way diff between listed objects and
// manifest entries, before repair or GC acts on it.
type diffResult struct {
	deltaOrphans []ObjectInfo
	baseOrphans  []ObjectInfo
	tmpOrphans   []ObjectInfo
	unknown      []ObjectInfo
	dangling     []string
	unverifiable []string
}

// diffSchema computes the two-way diff for one schema: objects absent from
// the manifest become orphans classified by shape, and in-bucket manifest
// entries absent from the listing become dangling.
func diffSchema(bucket, dataPrefix string, schemaID int16, objects []ObjectInfo, m *manifest.Manifest) diffResult {
	prefix := schemaDataPrefix(dataPrefix, schemaID)

	manifestKeys := make(map[string]struct{}, len(m.Files))
	var d diffResult
	for _, f := range m.Files {
		key, ok := normalizeKey(bucket, f.Path)
		if !ok {
			d.unverifiable = append(d.unverifiable, f.Path)
			continue
		}
		manifestKeys[key] = struct{}{}
	}

	listedKeys := make(map[string]struct{}, len(objects))
	for _, obj := range objects {
		listedKeys[obj.Key] = struct{}{}
		class, ok := classifyObjectKey(prefix, obj.Key)
		if !ok {
			continue
		}
		if _, listed := manifestKeys[obj.Key]; listed {
			continue
		}
		switch class {
		case ClassDelta:
			d.deltaOrphans = append(d.deltaOrphans, obj)
		case ClassBase:
			d.baseOrphans = append(d.baseOrphans, obj)
		case ClassTmp:
			d.tmpOrphans = append(d.tmpOrphans, obj)
		case ClassUnknown:
			d.unknown = append(d.unknown, obj)
		}
	}

	for _, f := range m.Files {
		key, ok := normalizeKey(bucket, f.Path)
		if !ok {
			continue // already reported unverifiable above
		}
		if !strings.HasPrefix(key, prefix) {
			// The listing only covers this schema's data prefix; absence of
			// an out-of-prefix key cannot be proven from it.
			d.unverifiable = append(d.unverifiable, f.Path)
			continue
		}
		if _, live := listedKeys[key]; !live {
			d.dangling = append(d.dangling, key)
		}
	}
	return d
}
