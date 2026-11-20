package reconcile

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"

	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
)

// inspect checks what the pass read (step 4's checks) and records a finding
// for every fault: a run target that is not the deployment's bound storage
// (checkStorage), the record's state, the current generation's history
// row, each manifest against its row, the keys under the prefix no V2
// writer stages, and every object a retained generation lists that S3
// confirms missing or, under VerifyChecksums, holds other bytes.
func (r *Reconciler) inspect(ctx context.Context, p *pass) error {
	p.verified = map[string]*manifestv2.Manifest{}
	if r.storageFault != nil {
		p.findings = append(p.findings, *r.storageFault)
	}
	current := r.checkRecord(p)
	r.checkHistory(p)
	for _, g := range p.retained {
		if p.byRecord && g.Generation == current {
			continue // checked as the record names it, below
		}
		r.checkManifest(p, g, true, g.Generation == current)
	}
	for _, g := range p.expired {
		if p.byRecord && g.Generation == current {
			continue
		}
		r.checkManifest(p, g, false, false)
	}
	if p.byRecord {
		r.checkRecordManifest(p)
	}
	for _, o := range p.listed {
		if !r.Layout.Owns(p.id, o.Key) {
			p.findings = append(p.findings, Finding{Kind: UnrecognizedObject, Key: o.Key,
				Detail: "not a key a V2 writer stages under this prefix", Action: actionUnrecognized})
		}
	}
	lost, err := r.probeLost(ctx, p, current)
	if err != nil {
		return err
	}
	if r.Opts.VerifyChecksums {
		return r.verifyChecksums(ctx, p, current, lost)
	}
	return nil
}

// checkRecord reports a record a reader fails closed on, or one whose
// lifecycle epoch row is missing or at another epoch, and checks that a
// published record's generation has its history row naming the same
// manifest: without it nothing protects the current generation's objects.
// A missing row (missing_generation) and a present row that disagrees with
// the record (history_mismatch) are told apart: the first leaves a gap a
// later expiry closes, the second stays in the history. A present row is
// looked for among the expired ones too: a retired_at on the current
// generation is never a retirement, however old (R9-F2). It answers the
// current generation number, 0 when there is none.
func (r *Reconciler) checkRecord(p *pass) int64 {
	if p.record == nil {
		return 0
	}
	rec := p.record.Record
	r.checkLifecycle(p, rec)
	switch rec.State {
	case publication.AwaitingInit:
		p.findings = append(p.findings, Finding{Kind: AwaitingInit, Epoch: rec.Epoch,
			Detail: "the V2 cutover found parquet history and no V2 generation exists yet", Action: actionAwaitingInit})
		return 0
	case publication.Published:
	default:
		return 0
	}
	cur := p.recordGeneration()
	row, ok := p.historyRow(rec.Generation)
	if !ok {
		f := generationFinding(MissingGeneration, cur, true, "the generation history has no row for the record's generation")
		f.Action = actionMissingHistory
		p.byRecord, p.rowFinding = true, len(p.findings)
		p.findings = append(p.findings, f)
		return rec.Generation
	}
	if row.Epoch == rec.Epoch && row.ManifestKey == rec.ManifestKey && row.ManifestSHA256 == rec.ManifestSHA256 &&
		row.Token == rec.Token && row.ParentGeneration == rec.ParentGeneration && row.SupersededAt == 0 && row.RetiredAt == 0 {
		return rec.Generation
	}
	p.byRecord, p.rowFinding = true, len(p.findings)
	f := generationFinding(HistoryMismatch, cur, true, fmt.Sprintf(
		"the generation's history row is present but disagrees with the record: the row names epoch %d, parent generation %d, "+
			"manifest %s, digest %s and token %s, superseded at %d and retired at %d; the record names epoch %d, parent generation %d, "+
			"manifest %s, digest %s and token %s",
		row.Epoch, row.ParentGeneration, row.ManifestKey, row.ManifestSHA256, row.Token, row.SupersededAt, row.RetiredAt,
		rec.Epoch, rec.ParentGeneration, rec.ManifestKey, rec.ManifestSHA256, rec.Token))
	f.Action = actionHistoryMismatch
	p.findings = append(p.findings, f)
	return rec.Generation
}

// checkRecordManifest checks the manifest the record names, as readers
// verify it, when the generation's history row is missing or disagrees with
// the record, and says in the row's finding whether reads succeed. Readers
// never read the row, so a lost or inconsistent row says nothing about the
// manifest: its loss is reported on its own (missing_generation of the
// manifest key, with the re-init that restores reads), beside the row's
// finding and its own recovery (R10-F3). The row's manifest, when the row
// names another, is not checked against the record: no reader reads it, and
// its bytes say nothing about the record's.
func (r *Reconciler) checkRecordManifest(p *pass) {
	cur := p.recordGeneration()
	r.checkManifest(p, cur, true, true)
	reads := "the pass could not verify the manifest the record names against it (the generation's other finding says why)"
	if p.verified[cur.ManifestKey] != nil {
		reads = "the manifest the record names verifies against it, as readers verify it, so reads succeed"
	}
	p.findings[p.rowFinding].Detail += "; " + reads
}

// historyRow answers the history row of generation g the snapshot read,
// retained or expired.
func (p *pass) historyRow(g int64) (publication.Generation, bool) {
	for _, rows := range [][]publication.Generation{p.retained, p.expired} {
		if i := slices.IndexFunc(rows, func(row publication.Generation) bool { return row.Generation == g }); i >= 0 {
			return rows[i], true
		}
	}
	return publication.Generation{}, false
}

// recordGeneration answers the published record's generation as the record
// names it, which is what readers verify and scan. It carries no kind,
// which only a history row names, and stamps neither superseded_at nor
// retired_at.
func (p *pass) recordGeneration() publication.Generation {
	rec := p.record.Record
	return publication.Generation{Generation: rec.Generation, Epoch: rec.Epoch, ParentGeneration: rec.ParentGeneration,
		ManifestKey: rec.ManifestKey, ManifestSHA256: rec.ManifestSHA256, Token: rec.Token}
}

// inspected answers the retained generations as a reader may scan them: the
// current generation as the record names it, in place of its history row
// when that disagrees with the record (history_mismatch), or added when the
// row is missing. Readers verify the record's manifest, so its objects are
// the current generation's, and the row's manifest is no reader's.
func (p *pass) inspected() []publication.Generation {
	if !p.byRecord {
		return p.retained
	}
	cur := p.recordGeneration()
	out := slices.DeleteFunc(slices.Clone(p.retained), func(g publication.Generation) bool { return g.Generation == cur.Generation })
	out = append(out, cur)
	slices.SortFunc(out, func(a, b publication.Generation) int { return cmp.Compare(a.Generation, b.Generation) })
	return out
}

// checkLifecycle reports a lifecycle epoch row that cannot fence the record:
// one that is missing, or one whose epoch is not the record's. Every
// transition writes the record's epoch and the row in one transaction, so
// the two differ only when one of them was restored or written outside the
// protocol, and then the row's stamp is not the record's: a lifecycle row
// restored from before a lost-record recovery carries the old epoch and a
// stamp long past the grace, while the recovered record, one epoch above,
// is younger than it, and the lost head's history row may be gone too.
func (r *Reconciler) checkLifecycle(p *pass, rec publication.Record) {
	l := p.lifecycle
	switch {
	case !l.Present:
		p.findings = append(p.findings, Finding{Kind: MissingLifecycleEpoch, Epoch: rec.Epoch, Action: actionMissingLifecycle,
			Detail: "the record has no lifecycle epoch row, which every transition that moves its epoch writes and " +
				"a lost record's recovery stamps: nothing shows when the schema was last recovered"})
	case l.Epoch < rec.Epoch:
		p.findings = append(p.findings, Finding{Kind: StaleLifecycleEpoch, Epoch: rec.Epoch, Action: actionStaleLifecycle,
			Detail: fmt.Sprintf("the lifecycle epoch row names epoch %d, stamped at %d, below the record's epoch %d, which every "+
				"transition raises the row to in the transaction that moves the record: the row was restored or written outside "+
				"the protocol, and its stamp does not show when the record's epoch, or a lost-record recovery, last moved", l.Epoch, l.MovedAt, rec.Epoch)})
	case l.Epoch > rec.Epoch:
		p.findings = append(p.findings, Finding{Kind: StaleLifecycleEpoch, Epoch: rec.Epoch, Action: actionRecordBehindLifecycle,
			Detail: fmt.Sprintf("the lifecycle epoch row names epoch %d, above the record's epoch %d: a record of the id carried "+
				"epoch %d before, so the record row went back (restored from an older copy, or written outside the protocol), and "+
				"neither its pointer nor the row's stamp %d can be trusted", l.Epoch, rec.Epoch, l.Epoch, l.MovedAt)})
	}
}

// checkHistory proves that the history holds a row for every generation a
// reader could still hold, so that no key is called an orphan because the
// row of the generation that lists it was lost. Generation numbers come
// from the record's counter, one per committed generation, so the rows a
// pass may not miss are every number from just above the highest expired
// row (the floor) up to the counter. Every generation below an expired row
// is past every reader: it was replaced before that row's generation
// committed, and that row was retired, after its own replacement was seen
// committed, at least the retention grace ago. Collection keeps the highest
// expired row for this reason (keepFloor). A number in the range without a
// row cannot be told from a forgotten one, and stops the collection.
func (r *Reconciler) checkHistory(p *pass) {
	if p.record == nil {
		return
	}
	var floor int64
	for _, g := range p.expired {
		floor = max(floor, g.Generation)
	}
	top := p.record.Generation
	if p.record.State == publication.Published {
		top-- // checkRecord reports the current generation's own row
	}
	next := floor + 1
	report := func(last int64) {
		if last < next {
			return
		}
		p.findings = append(p.findings, Finding{Kind: HistoryGap, Generation: next, Action: actionHistoryGap,
			Detail: fmt.Sprintf("the generation history has no row for generations %d through %d, above the highest expired generation %d "+
				"and at or below the record's counter %d: a reader may still hold one of them, and nothing names the objects it lists",
				next, last, floor, p.record.Generation)})
	}
	for _, g := range p.retained { // ascending
		if g.Generation < next || g.Generation > top {
			continue
		}
		report(g.Generation - 1)
		next = g.Generation + 1
	}
	report(top)
}

// checkManifest verifies g's manifest with the reader's full check
// (manifestv2.Verify: digest, identity, wire form, and every listed key
// spelling g's schema and epoch under the manifest's own prefix). A missing
// expired manifest is not a fault: a pass deletes it only after the objects
// it alone listed.
func (r *Reconciler) checkManifest(p *pass, g publication.Generation, retained, current bool) {
	if !r.Layout.Owns(p.id, g.ManifestKey) {
		p.findings = append(p.findings, generationFinding(ForeignManifest, g, current,
			fmt.Sprintf("the manifest key is not one the layout of prefix %q builds for schema %d", r.Layout.Prefix, p.id)))
		return
	}
	raw, ok := p.raw[g.ManifestKey]
	switch {
	case !ok && current:
		p.findings = append(p.findings, absentManifest(MissingGeneration, g, true))
		return
	case !ok && retained:
		p.findings = append(p.findings, absentManifest(MissingRetainedManifest, g, false))
		return
	case !ok:
		return
	}
	m, err := manifestv2.Verify(raw, manifestv2.GenerationRef(p.id, g))
	if current && err == nil {
		err = verifyAgainstRecord(raw, p.record.Record)
	}
	if err != nil {
		p.findings = append(p.findings, generationFinding(InvalidManifest, g, current, err.Error()))
		return
	}
	p.verified[g.ManifestKey] = m
}

// verifyAgainstRecord runs the reader's own check of the current manifest,
// against the record (manifestv2.RecordRef): the history row carries no
// lineage, and a record whose parent or lineage differs from its manifest's
// fails every reader even when the row verifies.
func verifyAgainstRecord(raw []byte, rec publication.Record) error {
	ref, err := manifestv2.RecordRef(rec)
	if err != nil {
		return fmt.Errorf("the record cannot be read as a reader reads it: %w", err)
	}
	if _, err := manifestv2.Verify(raw, ref); err != nil {
		return fmt.Errorf("the manifest does not verify against the record, as readers verify it: %w", err)
	}
	return nil
}

// probeLost answers the keys retained generations list that S3 confirms
// missing, and reports each against the newest generation that lists it,
// which is the current one when that lists it. A key the listing lacks is
// probed before it is called lost, so an object staged by a generation that
// committed after the listing is not.
func (r *Reconciler) probeLost(ctx context.Context, p *pass, current int64) (map[string]bool, error) {
	listed := make(map[string]bool, len(p.listed))
	for _, o := range p.listed {
		listed[o.Key] = true
	}
	listers := map[string]publication.Generation{}
	for _, g := range p.inspected() {
		m := p.verified[g.ManifestKey]
		if m == nil {
			continue
		}
		for _, f := range m.Files {
			if !listed[f.Key] {
				listers[f.Key] = g // generations ascend, so the newest lister wins
			}
		}
	}
	lost := map[string]bool{}
	for _, key := range slices.Sorted(maps.Keys(listers)) {
		ok, err := r.Objects.Exists(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("schema %d: probe %s, which a retained generation lists: %w", p.id, key, err)
		}
		if ok {
			continue
		}
		g := listers[key]
		lost[key] = true
		p.findings = append(p.findings, objectFinding(LostObject, g, g.Generation == current, key,
			"a retained generation lists the object and S3 confirms it missing"))
	}
	return lost, nil
}

// listing is one retained generation's manifest entry for a key.
type listing struct {
	g    publication.Generation
	file manifestv2.File
}

// verifyChecksums hashes every object a retained generation lists, except
// those already found lost, once per key, and checks the result against
// every retained entry that names the key: a shared base object is reported
// against each generation, the current one included, whose entry its bytes
// do not match, and an entry only the current generation committed wrongly
// is caught although an older entry matches. An object a hash finds gone is
// probed, and reported lost against its newest lister.
func (r *Reconciler) verifyChecksums(ctx context.Context, p *pass, current int64, lost map[string]bool) error {
	byKey := map[string][]listing{}
	for _, g := range p.inspected() {
		m := p.verified[g.ManifestKey]
		if m == nil {
			continue
		}
		for _, f := range m.Files {
			if !lost[f.Key] {
				byKey[f.Key] = append(byKey[f.Key], listing{g: g, file: f})
			}
		}
	}
	for _, key := range slices.Sorted(maps.Keys(byKey)) {
		entries := byKey[key]
		sum, size, err := r.Objects.Hash(ctx, key)
		if err != nil {
			newest := entries[len(entries)-1].g // generations ascend
			if ok, perr := r.Objects.Exists(ctx, key); perr == nil && !ok {
				p.findings = append(p.findings, objectFinding(LostObject, newest, newest.Generation == current, key,
					"a retained generation lists the object and S3 confirms it missing"))
				continue
			}
			return fmt.Errorf("schema %d: hash %s, which generation %d lists: %w", p.id, key, newest.Generation, err)
		}
		for _, e := range entries {
			if sum == e.file.SHA256 && size == e.file.SizeBytes {
				continue
			}
			p.findings = append(p.findings, objectFinding(CorruptObject, e.g, e.g.Generation == current, key,
				fmt.Sprintf("generation %d's manifest lists SHA-256 %s and %d bytes, the object has SHA-256 %s and %d bytes",
					e.g.Generation, e.file.SHA256, e.file.SizeBytes, sum, size)))
		}
	}
	return nil
}
