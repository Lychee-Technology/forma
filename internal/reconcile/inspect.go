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
		p.retained[i].ManifestSHA256 != rec.ManifestSHA256 || p.retained[i].Token != rec.Token || p.retained[i].SupersededAt != 0:
		detail = fmt.Sprintf("the generation's history row names epoch %d, manifest %s, digest %s and token %s, superseded at %d",
			p.retained[i].Epoch, p.retained[i].ManifestKey, p.retained[i].ManifestSHA256, p.retained[i].Token, p.retained[i].SupersededAt)
	default:
		return rec.Generation
	}
	f := generationFinding(MissingGeneration, cur, true, detail)
	f.Action = actionMissingHistory
	p.findings = append(p.findings, f)
	return rec.Generation
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
	if err != nil {
		p.findings = append(p.findings, generationFinding(InvalidManifest, g, current, err.Error()))
		return
	}
	p.verified[g.ManifestKey] = m
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

// verifyChecksums hashes every object a retained generation lists, except
// those already found lost, and reports one whose bytes do not have the
// digest and size its manifest lists.
func (r *Reconciler) verifyChecksums(ctx context.Context, p *pass, current int64, lost map[string]bool) error {
	seen := map[string]bool{}
	for _, g := range p.retained {
		m := p.verified[g.ManifestKey]
		if m == nil {
			continue
		}
		for _, f := range m.Files {
			if seen[f.Key] || lost[f.Key] {
				continue
			}
			seen[f.Key] = true
			sum, size, err := r.Objects.Hash(ctx, f.Key)
			if err != nil {
				return fmt.Errorf("schema %d: hash %s, which generation %d lists: %w", p.id, f.Key, g.Generation, err)
			}
			if sum == f.SHA256 && size == f.SizeBytes {
				continue
			}
			p.findings = append(p.findings, objectFinding(CorruptObject, g, g.Generation == current, f.Key,
				fmt.Sprintf("the manifest lists SHA-256 %s and %d bytes, the object has SHA-256 %s and %d bytes", f.SHA256, f.SizeBytes, sum, size)))
		}
	}
	return nil
}
