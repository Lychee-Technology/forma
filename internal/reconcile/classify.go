// Package reconcile diffs a schema's S3 parquet objects against its manifest
// (issue #203). It reports orphans — live objects the manifest does not list,
// classified by filename shape — and dangling entries — manifest paths whose
// object is gone. Optional repair appends delta-shaped orphans back to the
// manifest when the coverage + Postgres-liveness guard proves they are the
// #197 flush failure mode (data exists nowhere else), and promotes complete
// init-shaped base orphan sets into the base tier once coverage, eviction
// safety, and the no-resurrection proof all hold (#292); provable compaction
// leftovers are instead classified for GC, which also deletes merged-base
// and _tmp/ orphans left behind by compaction rewrites (#188).
package reconcile

import (
	"fmt"
	"strings"

	"github.com/google/uuid"

	"github.com/lychee-technology/forma/internal/manifest"
)

// OrphanClass is the filename-shape class of an unlisted parquet object. The
// class decides the recovery direction: delta orphans may carry data that
// exists nowhere else and are candidates for repair (guarded by row coverage
// and Postgres liveness), while merged-base, _tmp, and init-shaped base
// orphans are all eligible for GC. Init-shaped orphans became GC candidates in
// #290: cdc-init now holds the same per-schema advisory lock reconcile takes,
// so under that lock an init-shaped orphan is provably not from an in-flight
// init — it is either a failed manifest publish or a file superseded by a
// later init run. Since #292 an init-shaped set that --repair can prove
// complete is promoted into the base tier instead of collected. Unknown
// shapes are reported and never touched.
type OrphanClass int

const (
	ClassDelta      OrphanClass = iota
	ClassBaseInit               // {minRowID}_{maxRowID}.parquet (cdc-init export)
	ClassBaseMerged             // base-{uuid}.parquet (compaction rewrite)
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
	// Shapes are matched strictly — the class drives repair/GC actions, so
	// anything that merely resembles a known shape must stay Unknown
	// (report-only) rather than become deletable.
	stem := strings.TrimSuffix(rel, ".parquet")
	if rest, found := strings.CutPrefix(stem, "base-"); found {
		if uuid.Validate(rest) == nil {
			return ClassBaseMerged, true // cdc.BuildMergedBasePath: base-{uuid}.parquet
		}
		return ClassUnknown, true
	}
	if minID, maxID, found := strings.Cut(stem, "_"); found {
		if uuid.Validate(minID) == nil && uuid.Validate(maxID) == nil {
			return ClassBaseInit, true // cdc.BuildBasePath: {minRowID}_{maxRowID}.parquet
		}
		return ClassUnknown, true
	}
	if uuid.Validate(stem) == nil {
		return ClassDelta, true // cdc.BuildDeltaPath: {uuid}.parquet
	}
	return ClassUnknown, true
}

// normalizeKey reduces a manifest FileEntry.Path to a bucket-relative key.
// Manifest paths appear both bucket-relative and as absolute s3:// URIs
// (manifest/query_source.go tolerates either). Relative keys are compared
// verbatim: with an empty data prefix cdc.Build*Path emits keys with a
// leading slash, and the manifest stores that literal key, so stripping it
// would break the match against the S3 listing. ok is false for paths whose
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
	return path, true
}

// diffResult is one schema's raw two-way diff between listed objects and
// manifest entries, before repair or GC acts on it.
type diffResult struct {
	deltaOrphans      []ObjectInfo
	baseInitOrphans   []ObjectInfo
	baseMergedOrphans []ObjectInfo
	tmpOrphans        []ObjectInfo
	unknown           []ObjectInfo
	dangling          []string
	unverifiable      []string
	// objectsSeen counts the classified parquet objects under the schema
	// prefix; manifestEntries counts the manifest's file entries as loaded,
	// and manifestEntriesInPrefix the subset that normalizes into this
	// schema's data prefix — the entries this run can actually verify
	// against the listing. All feed the per-schema inventory report and the
	// #463/#481 GC guard, which must quote the numbers the report prints.
	objectsSeen             int
	manifestEntries         int
	manifestEntriesInPrefix int
}

// diffSchema computes the two-way diff for one schema: objects absent from
// the manifest become orphans classified by shape, and in-bucket manifest
// entries absent from the listing become dangling.
func diffSchema(bucket, dataPrefix string, schemaID int16, objects []ObjectInfo, m *manifest.Manifest) diffResult {
	prefix := schemaDataPrefix(dataPrefix, schemaID)

	manifestKeys := make(map[string]struct{}, len(m.Files))
	var d diffResult
	d.manifestEntries = len(m.Files)
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
		d.objectsSeen++
		if _, listed := manifestKeys[obj.Key]; listed {
			continue
		}
		switch class {
		case ClassDelta:
			d.deltaOrphans = append(d.deltaOrphans, obj)
		case ClassBaseInit:
			d.baseInitOrphans = append(d.baseInitOrphans, obj)
		case ClassBaseMerged:
			d.baseMergedOrphans = append(d.baseMergedOrphans, obj)
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
		d.manifestEntriesInPrefix++
		if _, live := listedKeys[key]; !live {
			d.dangling = append(d.dangling, key)
		}
	}
	return d
}
