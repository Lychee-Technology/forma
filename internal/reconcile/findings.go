package reconcile

import (
	"github.com/lychee-technology/forma/internal/publication"
)

// The safe operator action of each finding (docs/manifest-reconcile.md,
// "Findings and recovery"). None of them rebuilds a manifest or an object
// from what S3 still holds, and none installs an older copy of the record:
// under V2 the recovery of a lost manifest or object is a re-init, which
// exports every live row from PostgreSQL and publishes it as a new
// generation, and that of a lost or invalid record is init-db's recovery at
// a fresh epoch, then a re-init.
const (
	actionNoRecord = "run init-db, which recovers a lost record under the schema lock at a fresh epoch " +
		"(docs/publication-protocol-v2.md, \"A lost record\"); garbage collection deletes nothing for the schema until it has one"
	actionRecordInvalid = "recover the record at a fresh epoch, never from a backup copy of the row: an older pointer of the same " +
		"epoch can still verify and name an intact manifest while newer flush marks stay committed, and readers would " +
		"then return old values or a deleted row. Readers, writers and garbage collection fail closed on the invalid row " +
		"meanwhile. Delete the schema's row of the publication record table, run init-db, which recovers " +
		"the record under the schema lock one epoch above the id's lifecycle epoch, AWAITING_INIT while generation history " +
		"survives, then run cdc-init, which publishes every live row from PostgreSQL (docs/manifest-reconcile.md, " +
		"\"An invalid record\"); garbage collection resumes the retention grace after the recovery"
	actionMissingLifecycle = "write the id's row of the publication epoch table back at the record's epoch, with updated_at " +
		"the database time in epoch milliseconds (docs/manifest-reconcile.md, \"Findings and recovery\"); garbage collection " +
		"deletes nothing for the schema until it has one, and then for the retention grace after its stamp"
	actionStaleLifecycle = "write the id's row of the publication epoch table back at the record's epoch, with updated_at " +
		"the database time in epoch milliseconds, never a backup copy of the row (docs/manifest-reconcile.md, \"Findings and " +
		"recovery\"); garbage collection deletes nothing for the schema until the row names the record's epoch, and then for " +
		"the retention grace after its stamp"
	actionRecordBehindLifecycle = "treat the record as invalid: " + actionRecordInvalid
	actionUnboundStorage        = "confirm that the S3 endpoint the run's client uses (--s3-endpoint, or without it the AWS SDK's " +
		"endpoint configuration such as AWS_ENDPOINT_URL_S3, or --s3-region's AWS endpoint), --s3-bucket and --data-prefix are " +
		"those this database's writers (cdc-flush, cdc-init, the compactor) publish into, taken from their own configuration " +
		"and environment, then run manifest-reconcile --bind-storage with them once (docs/manifest-reconcile.md, \"Storage " +
		"binding\"); garbage collection deletes and forgets nothing until the run's target is the bound one"
	actionRebindStorage = "run manifest-reconcile so that its client uses the bound endpoint, with the bound --s3-bucket and " +
		"--data-prefix: give the bound --s3-endpoint, and clear an endpoint override (AWS_ENDPOINT_URL_S3, AWS_ENDPOINT_URL, " +
		"a profile's endpoint_url) the writers do not use. If the writers really moved to this target, delete the row of the " +
		"storage binding table by hand and bind again (docs/manifest-reconcile.md, \"Storage binding\"); garbage collection " +
		"deletes and forgets nothing under another target"
	actionAwaitingInit      = "run cdc-init for the schema: readers fail closed until its first V2 generation publishes"
	actionMissingGeneration = "re-initialize the schema with cdc-init, which publishes a new generation from PostgreSQL " +
		"(the lost manifest cannot be rebuilt from S3); garbage collection deletes nothing for the schema until " +
		"the lost generation expires, the retention grace after a pass retires it"
	actionMissingHistory = "do not restore the generation history row from a backup: a copy taken while the generation was " +
		"current carries superseded_at 0, the publication that replaced it already ran the update that supersedes it, and no " +
		"later one supersedes it again, so the restored row would be retained forever (docs/manifest-reconcile.md, \"Lost " +
		"history rows\"). Garbage collection deletes nothing for the schema until a generation above this one expires, which " +
		"takes two publications: the next one (a flush, a compaction or a cdc-init) supersedes this generation, which is then " +
		"reported as missing_history; a later one must supersede that publication's generation in turn, a pass retire it, " +
		"and the retention grace pass. Waiting the retention grace after a single publication or re-init is not enough: " +
		"nothing names the unrecorded generation's objects, and only an expired generation above it proves no reader still holds it"
	actionHistoryGap = "do not restore the generation history rows from a backup, whose copy of a row may carry a " +
		"superseded_at of 0 that nothing supersedes again (docs/manifest-reconcile.md, \"Lost history rows\"); garbage " +
		"collection resumes by itself once a generation above the gap expires (a later publication, a cdc-init included, " +
		"supersedes it, a pass retires it, and the retention grace passes), and deletes nothing for the schema until then"
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
