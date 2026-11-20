package reconcile

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
)

// inspect checks what the pass read (step 4's checks) and records a finding
// for every fault: the record's state, the current generation's history
// row, each manifest against its row, the keys under the prefix no V2
// writer stages, and every object a retained generation lists that S3
// confirms missing or, under VerifyChecksums, holds other bytes.
func (r *Reconciler) inspect(ctx context.Context, p *pass) error {
	p.verified = map[string]*manifestv2.Manifest{}
	current := r.checkRecord(p)
	r.checkHistory(p)
	for _, g := range p.retained {
		r.checkManifest(p, g, true, g.Generation == current)
	}
	for _, g := range p.expired {
		r.checkManifest(p, g, false, false)
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

// checkRecord reports a record a reader fails closed on, and checks that a
// published record's generation has its history row naming the same
// manifest: without it nothing protects the current generation's objects.
// It answers the current generation number, 0 when there is none.
func (r *Reconciler) checkRecord(p *pass) int64 {
	if p.record == nil {
		return 0
	}
	rec := p.record.Record
	switch rec.State {
	case publication.AwaitingInit:
		p.findings = append(p.findings, Finding{Kind: AwaitingInit, Epoch: rec.Epoch,
			Detail: "the V2 cutover found parquet history and no V2 generation exists yet", Action: actionAwaitingInit})
		return 0
	case publication.Published:
	default:
		return 0
	}
	cur := publication.Generation{Generation: rec.Generation, Epoch: rec.Epoch, ManifestKey: rec.ManifestKey}
	i := slices.IndexFunc(p.retained, func(g publication.Generation) bool { return g.Generation == rec.Generation })
	detail := ""
	switch {
	case i < 0:
		detail = "the generation history has no row for the record's generation"
	case p.retained[i].Epoch != rec.Epoch || p.retained[i].ManifestKey != rec.ManifestKey ||
		p.retained[i].ManifestSHA256 != rec.ManifestSHA256 || p.retained[i].Token != rec.Token ||
		p.retained[i].ParentGeneration != rec.ParentGeneration || p.retained[i].SupersededAt != 0:
		row := p.retained[i]
		detail = fmt.Sprintf("the generation's history row names epoch %d, parent generation %d, manifest %s, digest %s and token %s, "+
			"superseded at %d; the record names epoch %d, parent generation %d, manifest %s, digest %s and token %s",
			row.Epoch, row.ParentGeneration, row.ManifestKey, row.ManifestSHA256, row.Token, row.SupersededAt,
			rec.Epoch, rec.ParentGeneration, rec.ManifestKey, rec.ManifestSHA256, rec.Token)
	default:
		return rec.Generation
	}
	f := generationFinding(MissingGeneration, cur, true, detail)
	f.Action = actionMissingHistory
	p.findings = append(p.findings, f)
	return rec.Generation
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
		p.findings = append(p.findings, generationFinding(MissingGeneration, g, true, "the manifest object is missing"))
		return
	case !ok && retained:
		p.findings = append(p.findings, generationFinding(MissingRetainedManifest, g, false, "the manifest object is missing"))
		return
	case !ok:
		return
	}
	m, err := manifestv2.Verify(raw, manifestv2.GenerationRef(p.id, g))
	if err == nil && current {
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
	for _, g := range p.retained {
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
	for _, g := range p.retained {
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
