package benchmark

import (
	"context"
	"fmt"
	"sort"

	"github.com/google/uuid"
	federated "github.com/lychee-technology/forma/internal/e2e_harness/federated"
)

// TierMixProfile defines the cold/warm/hot allocation ratios.
type TierMixProfile struct {
	Name       string  `json:"name"`
	BaseRatio  float64 `json:"base_ratio"`
	DeltaRatio float64 `json:"delta_ratio"`
	HotRatio   float64 `json:"hot_ratio"`
}

// TieredDataset holds benchmark records split by storage tier.
type TieredDataset struct {
	Profile TierMixProfile     `json:"profile"`
	Base    []GeneratedRecord  `json:"base"`
	Delta   []GeneratedRecord  `json:"delta"`
	Hot     []GeneratedRecord  `json:"hot"`
	Summary TieredDatasetStats `json:"summary"`
}

// TieredDatasetStats summarizes split results.
type TieredDatasetStats struct {
	BaseCount       int `json:"base_count"`
	DeltaCount      int `json:"delta_count"`
	HotCount        int `json:"hot_count"`
	OverlappingKeys int `json:"overlapping_keys"`
	DeletedInHot    int `json:"deleted_in_hot"`
}

// TierLoader is the harness contract needed to load a tiered dataset.
type TierLoader interface {
	SetupSchema(schemaID int16, schemaName string) error
	ClearAllData(ctx context.Context) error
	WriteParquet(ctx context.Context, tier, filename string, records []federated.TestRecord) error
	SeedHotRecordsWithData(ctx context.Context, records []federated.TestRecord) error
}

var (
	TierMixBalanced    = TierMixProfile{Name: "balanced-60-30-10", BaseRatio: 0.60, DeltaRatio: 0.30, HotRatio: 0.10}
	TierMixHighHot     = TierMixProfile{Name: "high-hot-40-20-40", BaseRatio: 0.40, DeltaRatio: 0.20, HotRatio: 0.40}
	TierMixLongHistory = TierMixProfile{Name: "long-history-85-10-5", BaseRatio: 0.85, DeltaRatio: 0.10, HotRatio: 0.05}
)

// DefaultTierMixProfiles returns the supported tier layouts.
func DefaultTierMixProfiles() []TierMixProfile {
	return []TierMixProfile{TierMixBalanced, TierMixHighHot, TierMixLongHistory}
}

// SplitIntoTiers partitions the generated dataset into base, delta, and hot tiers.
func SplitIntoTiers(dataset *GeneratedDataset, profile TierMixProfile) (*TieredDataset, error) {
	if dataset == nil {
		return nil, fmt.Errorf("dataset cannot be nil")
	}
	if err := validateTierMixProfile(profile); err != nil {
		return nil, err
	}
	base := make([]GeneratedRecord, 0)
	delta := make([]GeneratedRecord, 0)
	hot := make([]GeneratedRecord, 0)
	base = append(base, filterRecordsBySchema(dataset.Records, "customer")...)
	base = append(base, filterRecordsBySchema(dataset.Records, "security")...)
	tradeRecords := dataset.RecordsForSchema("trade")
	groups := groupBySchemaRowKey(tradeRecords)
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	baseCutoff, deltaCutoff := tierCutoffs(len(keys), profile)
	stats := TieredDatasetStats{}
	for idx, key := range keys {
		versions := groups[key]
		sortGeneratedRecords(versions)
		latest := versions[len(versions)-1]
		hasOverlap := len(versions) > 1
		switch {
		case idx < baseCutoff:
			base = append(base, cloneGeneratedRecord(latest))
		case idx < deltaCutoff:
			delta = append(delta, cloneGeneratedRecord(latest))
		default:
			hot = append(hot, cloneGeneratedRecord(latest))
		}
		if hasOverlap {
			stats.OverlappingKeys++
			baseVersion := cloneGeneratedRecord(versions[0])
			switch {
			case idx < baseCutoff:
				base = append(base, baseVersion)
				if latest.Version > baseVersion.Version {
					hot = append(hot, cloneGeneratedRecord(latest))
				}
			case idx < deltaCutoff:
				base = append(base, baseVersion)
				delta = append(delta, cloneGeneratedRecord(latest))
			default:
				base = append(base, baseVersion)
				hot = append(hot, cloneGeneratedRecord(latest))
			}
		}
	}
	stats.BaseCount = len(base)
	stats.DeltaCount = len(delta)
	stats.HotCount = len(hot)
	for _, record := range hot {
		if record.DeletedAt > 0 {
			stats.DeletedInHot++
		}
	}
	sortGeneratedRecords(base)
	sortGeneratedRecords(delta)
	sortGeneratedRecords(hot)
	return &TieredDataset{Profile: profile, Base: base, Delta: delta, Hot: hot, Summary: stats}, nil
}

// LoadTieredDataset loads a tiered dataset into the federated harness.
func LoadTieredDataset(ctx context.Context, loader TierLoader, dataset *TieredDataset) error {
	if loader == nil {
		return fmt.Errorf("loader cannot be nil")
	}
	if dataset == nil {
		return fmt.Errorf("dataset cannot be nil")
	}
	if err := loader.ClearAllData(ctx); err != nil {
		return fmt.Errorf("clear existing data: %w", err)
	}
	baseBySchema := groupRecordsBySchema(dataset.Base)
	deltaBySchema := groupRecordsBySchema(dataset.Delta)
	hotBySchema := groupRecordsBySchema(dataset.Hot)
	for _, fixture := range DefaultSchemaFixtures() {
		if err := loader.SetupSchema(fixture.ID, fixture.Name); err != nil {
			return fmt.Errorf("setup schema %s: %w", fixture.Name, err)
		}
		if records := baseBySchema[fixture.ID]; len(records) > 0 {
			if err := loader.WriteParquet(ctx, "base", fmt.Sprintf("benchmark_base_%s.parquet", fixture.Name), ToTestRecords(records)); err != nil {
				return fmt.Errorf("write base parquet for %s: %w", fixture.Name, err)
			}
		}
		if records := deltaBySchema[fixture.ID]; len(records) > 0 {
			if err := loader.WriteParquet(ctx, "delta", fmt.Sprintf("benchmark_delta_%s.parquet", fixture.Name), ToTestRecords(records)); err != nil {
				return fmt.Errorf("write delta parquet for %s: %w", fixture.Name, err)
			}
		}
		if records := hotBySchema[fixture.ID]; len(records) > 0 {
			if err := loader.SeedHotRecordsWithData(ctx, ToTestRecords(records)); err != nil {
				return fmt.Errorf("seed hot records for %s: %w", fixture.Name, err)
			}
		}
	}
	return nil
}

// ToTestRecords converts generated records into harness test records.
func ToTestRecords(records []GeneratedRecord) []federated.TestRecord {
	out := make([]federated.TestRecord, 0, len(records))
	for _, record := range records {
		attrs := make(map[string]any, len(record.Attributes)+1)
		for key, value := range record.Attributes {
			attrs[key] = value
		}
		attrs["version"] = record.Version
		if _, ok := attrs["name"]; !ok {
			attrs["name"] = benchmarkDisplayName(record)
		}
		out = append(out, federated.TestRecord{
			RowID:      record.RowID,
			SchemaID:   record.SchemaID,
			Attributes: attrs,
			ChangedAt:  record.ChangedAt,
			DeletedAt:  record.DeletedAt,
			FlushedAt:  0,
		})
	}
	return out
}

func validateTierMixProfile(profile TierMixProfile) error {
	if profile.Name == "" {
		return fmt.Errorf("tier profile name is required")
	}
	total := profile.BaseRatio + profile.DeltaRatio + profile.HotRatio
	if profile.BaseRatio < 0 || profile.DeltaRatio < 0 || profile.HotRatio < 0 {
		return fmt.Errorf("tier ratios must be non-negative")
	}
	if total < 0.999 || total > 1.001 {
		return fmt.Errorf("tier ratios must sum to 1.0")
	}
	return nil
}

func tierCutoffs(total int, profile TierMixProfile) (int, int) {
	baseCutoff := int(float64(total) * profile.BaseRatio)
	deltaCutoff := baseCutoff + int(float64(total)*profile.DeltaRatio)
	if total > 0 && baseCutoff == 0 && profile.BaseRatio > 0 {
		baseCutoff = 1
	}
	if deltaCutoff < baseCutoff {
		deltaCutoff = baseCutoff
	}
	if deltaCutoff > total {
		deltaCutoff = total
	}
	return baseCutoff, deltaCutoff
}

func groupBySchemaRowKey(records []GeneratedRecord) map[string][]GeneratedRecord {
	groups := make(map[string][]GeneratedRecord)
	for _, record := range records {
		key := schemaRowKey(record.SchemaID, record.RowID)
		groups[key] = append(groups[key], cloneGeneratedRecord(record))
	}
	return groups
}

func schemaRowKey(schemaID int16, rowID uuid.UUID) string {
	return fmt.Sprintf("%d:%s", schemaID, rowID)
}

func benchmarkDisplayName(record GeneratedRecord) string {
	switch record.SchemaName {
	case "customer":
		return fmt.Sprint(record.Attributes["name"])
	case "security":
		return fmt.Sprint(record.Attributes["companyName"])
	case "trade":
		return fmt.Sprintf("trade-%s", record.RowID.String()[:8])
	default:
		return record.SchemaName
	}
}

func groupRecordsBySchema(records []GeneratedRecord) map[int16][]GeneratedRecord {
	out := make(map[int16][]GeneratedRecord)
	for _, record := range records {
		out[record.SchemaID] = append(out[record.SchemaID], cloneGeneratedRecord(record))
	}
	return out
}
