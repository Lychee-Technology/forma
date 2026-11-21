package reconcile

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
)

func (r *Reconciler) inspect(ctx context.Context, p *pass) error {
	p.verified = map[string]*manifestv2.Manifest{}
	if r.storageFault != nil {
		p.findings = append(p.findings, *r.storageFault)
	}
	current := r.checkRecord(p)
	r.checkHistory(p)
	for _, g := range p.retained {
		if p.byRecord && g.Generation == current {
			continue
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
			p.findings = append(p.findings, Finding{
				Kind:   UnrecognizedObject,
				Key:    o.Key,
				Detail: "not a key a V2 writer stages under this prefix",
				Action: actionUnrecognized,
			})
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

func (r *Reconciler) checkRecord(p *pass) int64 {
	if p.record == nil {
		return 0
	}
	rec := p.record.Record
	r.checkLifecycle(p, rec)
	switch rec.State {
	case publication.AwaitingInit:
		p.findings = append(p.findings, Finding{
			Kind:   AwaitingInit,
			Epoch:  rec.Epoch,
			Detail: "the schema awaits a cdc-init publication before reads or flushes proceed",
			Action: actionAwaitingInit,
		})
		return 0
	case publication.Published:
	default:
		return 0
	}
	cur := p.recordGeneration()
	row, ok := p.historyRow(rec.Generation)
	if !ok {
		f := generationFinding(MissingGeneration, cur, true, "the generation history has no row for the record's current generation")
		f.Action = actionHistoryMismatch
		p.byRecord, p.rowFinding = true, len(p.findings)
		p.findings = append(p.findings, f)
		return rec.Generation
	}
	if row.Epoch == rec.Epoch && row.ParentGeneration == rec.ParentGeneration &&
		row.ManifestKey == rec.ManifestKey && row.ManifestSHA256 == rec.ManifestSHA256 &&
		row.Lineage == rec.Lineage && row.Token == rec.Token &&
		row.SupersededAt == 0 && row.RetiredAt == 0 {
		return rec.Generation
	}
	p.byRecord, p.rowFinding = true, len(p.findings)
	f := generationFinding(HistoryMismatch, cur, true, fmt.Sprintf(
		"history row disagrees with record: row names epoch %d, parent %d, manifest %s, digest %s, lineage %s, token %s, superseded_at %d, retired_at %d; "+
			"record names epoch %d, parent %d, manifest %s, digest %s, lineage %s, token %s",
		row.Epoch, row.ParentGeneration, row.ManifestKey, row.ManifestSHA256, row.Lineage, row.Token, row.SupersededAt, row.RetiredAt,
		rec.Epoch, rec.ParentGeneration, rec.ManifestKey, rec.ManifestSHA256, rec.Lineage, rec.Token))
	f.Action = actionHistoryMismatch
	p.findings = append(p.findings, f)
	return rec.Generation
}

func (r *Reconciler) checkRecordManifest(p *pass) {
	cur := p.recordGeneration()
	r.checkManifest(p, cur, true, true)
	reads := readsUnverified
	if p.verified[cur.ManifestKey] != nil {
		reads = readsManifestVerified
	}
	p.rowDetail = p.findings[p.rowFinding].Detail
	p.findings[p.rowFinding].Detail += "; " + reads
}

const (
	readsManifestVerified = "the manifest named by the record verifies against the record as readers verify it; " +
		"this checks the manifest only, not that listed objects exist or match checksums: a lost_object finding " +
		"means reads fail closed, while a corrupt_object finding means reads (which do not hash parquet objects) may return incorrect results or scan errors"
	readsUnverified = "the manifest named by the record did not verify against the record: " +
		"the finding on its manifest key explains why, and reads fail closed"
	readsCollected = "the manifest named by the record was missing when read and the generation expired while diagnosis ran (finding left out): " +
		"a collection may have deleted it or it was missing before; run diagnosis again"
)

func (p *pass) historyRow(g int64) (publication.Generation, bool) {
	for _, rows := range [][]publication.Generation{p.retained, p.expired} {
		if i := slices.IndexFunc(rows, func(row publication.Generation) bool { return row.Generation == g }); i >= 0 {
			return rows[i], true
		}
	}
	return publication.Generation{}, false
}

func (p *pass) recordGeneration() publication.Generation {
	rec := p.record.Record
	return publication.Generation{
		Generation: rec.Generation, Epoch: rec.Epoch, ParentGeneration: rec.ParentGeneration,
		ManifestKey: rec.ManifestKey, ManifestSHA256: rec.ManifestSHA256,
		Lineage: rec.Lineage, Token: rec.Token,
	}
}

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

func (r *Reconciler) checkLifecycle(p *pass, rec publication.Record) {
	l := p.lifecycle
	switch {
	case !l.Present:
		p.findings = append(p.findings, Finding{
			Kind:   MissingLifecycleEpoch,
			Epoch:  rec.Epoch,
			Action: actionMissingLifecycle,
			Detail: "the record has no lifecycle epoch row; neither the forgotten floor nor any recovery fence can be proven",
		})
	case l.Epoch < rec.Epoch:
		p.findings = append(p.findings, Finding{
			Kind:   StaleLifecycleEpoch,
			Epoch:  rec.Epoch,
			Action: actionStaleLifecycle,
			Detail: fmt.Sprintf("lifecycle epoch row is at epoch %d, below the record's epoch %d", l.Epoch, rec.Epoch),
		})
	case l.Epoch > rec.Epoch:
		p.findings = append(p.findings, Finding{
			Kind:   StaleLifecycleEpoch,
			Epoch:  rec.Epoch,
			Action: actionRecordBehindLifecycle,
			Detail: fmt.Sprintf("lifecycle epoch row is at epoch %d, above the record's epoch %d (record went backwards)", l.Epoch, rec.Epoch),
		})
	}
}

// checkHistory verifies that every generation in (floor, top] exists in either
// p.expired or p.retained with no gaps, where floor is the higher of the
// durable forgotten_generation and the highest non-current expired generation
// (an expired generation proves every earlier generation's readers finished
// past G). Any missing number above floor blocks garbage collection.
func (r *Reconciler) checkHistory(p *pass) {
	if p.record == nil || !p.lifecycle.Present {
		return
	}
	floor := p.lifecycle.ForgottenGeneration
	for _, g := range p.expired {
		if !p.isCurrent(g) && g.Generation > floor {
			floor = g.Generation
		}
	}
	top := p.record.Generation
	if p.record.State == publication.Published {
		top-- // checkRecord inspects the current generation's own row
	}
	if top <= floor {
		return
	}
	present := make(map[int64]bool, len(p.retained)+len(p.expired))
	for _, g := range p.retained {
		present[g.Generation] = true
	}
	for _, g := range p.expired {
		present[g.Generation] = true
	}
	for gen := floor + 1; gen <= top; {
		if present[gen] {
			gen++
			continue
		}
		start := gen
		for gen <= top && !present[gen] {
			gen++
		}
		p.findings = append(p.findings, Finding{
			Kind:       HistoryGap,
			Generation: start,
			Action:     actionHistoryGap,
			Detail: fmt.Sprintf("generation history is missing row(s) %d through %d above forgotten_generation %d and at or below record counter %d",
				start, gen-1, floor, p.record.Generation),
		})
	}
}

func (r *Reconciler) checkManifest(p *pass, g publication.Generation, retained, current bool) {
	if !r.Layout.Owns(p.id, g.ManifestKey) {
		p.findings = append(p.findings, generationFinding(ForeignManifest, g, current,
			fmt.Sprintf("manifest key is not built by layout prefix %q for schema %d", r.Layout.Prefix, p.id)))
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

func verifyAgainstRecord(raw []byte, rec publication.Record) error {
	ref, err := manifestv2.RecordRef(rec)
	if err != nil {
		return fmt.Errorf("build reader reference from record: %w", err)
	}
	if _, err := manifestv2.Verify(raw, ref); err != nil {
		return fmt.Errorf("manifest does not verify against record as readers verify it: %w", err)
	}
	return nil
}

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
				listers[f.Key] = g
			}
		}
	}
	lost := map[string]bool{}
	for _, key := range slices.Sorted(maps.Keys(listers)) {
		ok, err := r.Objects.Exists(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("schema %d: probe %s listed by a retained generation: %w", p.id, key, err)
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

type listing struct {
	g    publication.Generation
	file manifestv2.File
}

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
			gone, herr := r.hashedGone(ctx, p, key, entries, current, err)
			if herr != nil {
				return herr
			}
			if gone {
				continue
			}
		}
		for _, e := range entries {
			if sum == e.file.SHA256 && size == e.file.SizeBytes {
				continue
			}
			p.findings = append(p.findings, objectFinding(CorruptObject, e.g, e.g.Generation == current, key,
				fmt.Sprintf("generation %d manifest lists SHA-256 %s (%d bytes), object has SHA-256 %s (%d bytes): "+
					"integrity fault that federated readers do not detect (reads may return incorrect results or fail with a scan error)",
					e.g.Generation, e.file.SHA256, e.file.SizeBytes, sum, size)))
		}
	}
	return nil
}

// hashedGone converts a failed Hash into a LostObject finding only when Hash
// returned manifestv2.ErrObjectNotFound AND a confirming HEAD confirms absence.
// Any non-NotFound Hash error, or any confirming HEAD error (e.g.
// ErrRedirectRefused, 403, timeout), is returned verbatim with key and
// generation context so its error chain is never lost (R14-F1, R15-F2).
func (r *Reconciler) hashedGone(ctx context.Context, p *pass, key string, entries []listing, current int64, err error) (bool, error) {
	newest := entries[len(entries)-1].g
	if !errors.Is(err, manifestv2.ErrObjectNotFound) {
		return false, fmt.Errorf("schema %d: hash %s listed by generation %d: %w", p.id, key, newest.Generation, err)
	}
	ok, perr := r.Objects.Exists(ctx, key)
	if perr != nil {
		return false, fmt.Errorf("schema %d: confirm missing object %s listed by generation %d: %w", p.id, key, newest.Generation, perr)
	}
	if ok {
		return false, fmt.Errorf("schema %d: hash %s listed by generation %d returned not found, but HEAD found it present: %w",
			p.id, key, newest.Generation, err)
	}
	p.findings = append(p.findings, objectFinding(LostObject, newest, newest.Generation == current, key,
		"a retained generation lists the object and S3 confirms it missing"))
	return true, nil
}
