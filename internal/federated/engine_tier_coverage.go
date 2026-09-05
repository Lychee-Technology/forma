package federated

import "github.com/lychee-technology/forma/internal/model"

// unconsultedTiers lists the tiers a Postgres-only answer skipped for fq
// (#468): everything the request asked for beyond hot. An empty
// PreferredTiers is the default all-tier form (#184), so it asks for warm and
// cold too. A hot-only request never reaches this — the caller asked for hot
// and got hot — which is why the hot-only gate does not call it.
func unconsultedTiers(fq *model.FederatedAttributeQuery) []model.DataTier {
	requested := []model.DataTier{model.DataTierHot, model.DataTierWarm, model.DataTierCold}
	if fq != nil && len(fq.PreferredTiers) > 0 {
		requested = fq.PreferredTiers
	}
	out := make([]model.DataTier, 0, len(requested))
	for _, tier := range requested {
		if tier != model.DataTierHot {
			out = append(out, tier)
		}
	}
	return out
}

// markHotTierOnly stamps a Postgres-only page with the #468 coverage marker
// for the tiers fq asked for and never got, and mirrors it onto the
// out-parameter so opts.PartialScan and page.Partial describe the same
// answer. The marker replaces anything a failed DuckDB pass recorded: the
// #251 corrupt-exclusion scope never describes a postgres-only answer. A
// request whose only asked-for tier is hot (an explicit [hot] that slipped
// past the gate, or a caller-built query) leaves the page unmarked.
func markHotTierOnly(page *model.PersistentRecordPage, opts *model.FederatedQueryOptions, fq *model.FederatedAttributeQuery) {
	if page == nil {
		return
	}
	skipped := unconsultedTiers(fq)
	var marker *model.PartialScan
	if len(skipped) > 0 {
		marker = &model.PartialScan{UnconsultedTiers: skipped}
	}
	page.Partial = marker
	if opts != nil {
		opts.PartialScan = marker
	}
}
