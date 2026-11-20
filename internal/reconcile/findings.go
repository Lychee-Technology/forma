package reconcile

import (
	"github.com/lychee-technology/forma/internal/publication"
)

// The safe operator action of each finding (docs/manifest-reconcile.md,
// "Findings and recovery"). None of them rebuilds a manifest or an object
// from what S3 still holds: under V2 the recovery of lost publication state
// is a re-init, which exports every live row from PostgreSQL and publishes
// it as a new generation.
const (
	actionNoRecord = "run init-db, which recovers a lost record under the schema lock at a fresh epoch " +
		"(docs/publication-protocol-v2.md, \"A lost record\"); garbage collection deletes nothing for the schema until it has one"
	actionRecordInvalid = "restore the record row from a backup: no transition writes this row, readers and writers fail closed on it, " +
		"and garbage collection deletes nothing for the schema meanwhile"
	actionMissingLifecycle = "write the id's row of the publication epoch table back at the record's epoch, with updated_at " +
		"the database time in epoch milliseconds (docs/manifest-reconcile.md, \"Findings and recovery\"); garbage collection " +
		"deletes nothing for the schema until it has one, and then for the retention grace after its stamp"
	actionAwaitingInit      = "run cdc-init for the schema: readers fail closed until its first V2 generation publishes"
	actionMissingGeneration = "re-initialize the schema with cdc-init, which publishes a new generation from PostgreSQL " +
		"(the lost manifest cannot be rebuilt from S3); garbage collection deletes nothing for the schema until " +
		"the lost generation expires, the retention grace after a pass retires it"
	actionMissingHistory = "restore the generation history row from a backup. Otherwise garbage collection deletes nothing " +
		"for the schema until a generation above this one expires, which takes two publications: the next one (a flush, " +
		"a compaction or a cdc-init) supersedes this generation, which is then reported as missing_history; a later one " +
		"must supersede that publication's generation in turn, a pass retire it, and the retention grace pass. Waiting " +
		"the retention grace after a single publication or re-init is not enough: nothing names the unrecorded " +
		"generation's objects, and only an expired generation above it proves no reader still holds it"
	actionHistoryGap = "restore the generation history rows from a backup; otherwise garbage collection resumes by itself " +
		"once a generation above the gap expires (a later publication, a cdc-init included, supersedes it, a pass retires it, " +
		"and the retention grace passes), and deletes nothing for the schema until then"
	actionMissingRetained = "no action for new reads, which scan the current generation; garbage collection deletes nothing " +
		"for the schema until this generation expires, the retention grace after it was retired"
	actionInvalidManifest = "restore the object from a versioned or backed-up copy whose SHA-256 is the history row's " +
		"manifest_sha256; for the current generation, re-initializing with cdc-init restores reads. Garbage " +
		"collection deletes nothing for the schema until the object verifies"
	actionForeignManifest = "run manifest-reconcile with the --data-prefix the writers publish under; garbage collection " +
		"deletes nothing for the schema under this prefix"
	actionLostCurrent = "re-initialize the schema with cdc-init: reads fail closed while the current generation lists " +
		"a missing object, and the object cannot be recovered from its manifest"
	actionLostSuperseded = "no action for new reads, which scan the current generation; a read still pinned to this " +
		"generation fails closed, and the generation expires the retention grace after it was retired"
	actionCorrupt = "re-initialize the schema with cdc-init if the current generation lists the object (compaction " +
		"refuses it as a merge source, #347); otherwise it leaves with its generation's expiry"
	actionUnrecognized = "garbage collection never deletes a key no V2 writer stages (a V1 leftover, or another " +
		"tool's object); remove it by hand once you know what wrote it"
)

func recordFinding(kind FindingKind, err error) Finding {
	action := actionNoRecord
	if kind == RecordInvalid {
		action = actionRecordInvalid
	}
	return Finding{Kind: kind, Detail: err.Error(), Action: action}
}

func generationFinding(kind FindingKind, g publication.Generation, current bool, detail string) Finding {
	f := Finding{Kind: kind, Epoch: g.Epoch, Generation: g.Generation, Current: current, Key: g.ManifestKey, Detail: detail}
	switch {
	case kind == MissingGeneration:
		f.Action = actionMissingGeneration
	case kind == MissingRetainedManifest:
		f.Action = actionMissingRetained
	case kind == InvalidManifest:
		f.Action = actionInvalidManifest
	case kind == ForeignManifest:
		f.Action = actionForeignManifest
	}
	return f
}

// absentManifest is the finding of g's manifest object confirmed missing.
func absentManifest(kind FindingKind, g publication.Generation, current bool) Finding {
	f := generationFinding(kind, g, current, "the manifest object is missing")
	f.absent = true
	return f
}

func objectFinding(kind FindingKind, g publication.Generation, current bool, key, detail string) Finding {
	f := Finding{Kind: kind, Epoch: g.Epoch, Generation: g.Generation, Current: current, Key: key, Detail: detail,
		absent: kind == LostObject}
	switch {
	case kind == CorruptObject:
		f.Action = actionCorrupt
	case current:
		f.Action = actionLostCurrent
	default:
		f.Action = actionLostSuperseded
	}
	return f
}
