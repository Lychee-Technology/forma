package benchmark

import (
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

var defaultBaseTime = time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)

// ScalePreset defines the default entity counts and generation ratios for a scale.
type ScalePreset struct {
	Scale          Scale   `json:"scale"`
	TradeCount     int     `json:"trade_count"`
	CustomerCount  int     `json:"customer_count"`
	SecurityCount  int     `json:"security_count"`
	OverlapRatio   float64 `json:"overlap_ratio"`
	DeleteRatio    float64 `json:"delete_ratio"`
	TimeWindowDays int     `json:"time_window_days"`
}

// GeneratorConfig controls dataset generation.
type GeneratorConfig struct {
	Scale          Scale        `json:"scale"`
	Distribution   Distribution `json:"distribution"`
	Seed           int64        `json:"seed"`
	TradeCount     int          `json:"trade_count"`
	CustomerCount  int          `json:"customer_count"`
	SecurityCount  int          `json:"security_count"`
	OverlapRatio   float64      `json:"overlap_ratio"`
	DeleteRatio    float64      `json:"delete_ratio"`
	TimeWindowDays int          `json:"time_window_days"`
	BaseTime       time.Time    `json:"base_time"`
}

// GeneratedRecord is the canonical benchmark row representation before tier assignment.
type GeneratedRecord struct {
	SchemaID   int16          `json:"schema_id"`
	SchemaName string         `json:"schema_name"`
	RowID      uuid.UUID      `json:"row_id"`
	Version    int            `json:"version"`
	ChangedAt  int64          `json:"changed_at"`
	DeletedAt  int64          `json:"deleted_at,omitempty"`
	Attributes map[string]any `json:"attributes"`
}

// DatasetSummary captures high-level dataset statistics.
type DatasetSummary struct {
	TotalRecords      int            `json:"total_records"`
	UniqueRows        int            `json:"unique_rows"`
	DuplicateVersions int            `json:"duplicate_versions"`
	DeletedRecords    int            `json:"deleted_records"`
	OverlapRecords    int            `json:"overlap_records"`
	CountsBySchema    map[string]int `json:"counts_by_schema"`
}

// GeneratedDataset contains the rows and summary for a generated dataset.
type GeneratedDataset struct {
	Config  GeneratorConfig   `json:"config"`
	Records []GeneratedRecord `json:"records"`
	Summary DatasetSummary    `json:"summary"`
}

// DefaultGeneratorConfig returns defaults for small-scale generation.
func DefaultGeneratorConfig() GeneratorConfig {
	preset := MustScalePreset(ScaleSmall)
	return GeneratorConfig{
		Scale:          preset.Scale,
		Distribution:   DistributionUniform,
		Seed:           42,
		TradeCount:     preset.TradeCount,
		CustomerCount:  preset.CustomerCount,
		SecurityCount:  preset.SecurityCount,
		OverlapRatio:   preset.OverlapRatio,
		DeleteRatio:    preset.DeleteRatio,
		TimeWindowDays: preset.TimeWindowDays,
		BaseTime:       defaultBaseTime,
	}
}

// GeneratorConfigFromBenchmark derives a generator config from the benchmark config.
func GeneratorConfigFromBenchmark(cfg Config) GeneratorConfig {
	resolved := cfg.WithDefaults()
	genCfg := DefaultGeneratorConfig()
	genCfg.Scale = resolved.Scale
	genCfg.Distribution = resolved.Distribution
	genCfg.Seed = resolved.Seed
	return genCfg.WithDefaults()
}

// WithDefaults applies scale defaults to missing fields.
func (c GeneratorConfig) WithDefaults() GeneratorConfig {
	defaults := DefaultGeneratorConfig()
	if c.Scale == "" {
		c.Scale = defaults.Scale
	}
	preset := MustScalePreset(c.Scale)
	if c.Distribution == "" {
		c.Distribution = defaults.Distribution
	}
	if c.Seed == 0 {
		c.Seed = defaults.Seed
	}
	if c.TradeCount <= 0 {
		c.TradeCount = preset.TradeCount
	}
	if c.CustomerCount <= 0 {
		c.CustomerCount = preset.CustomerCount
	}
	if c.SecurityCount <= 0 {
		c.SecurityCount = preset.SecurityCount
	}
	if c.OverlapRatio <= 0 {
		c.OverlapRatio = preset.OverlapRatio
	}
	if c.DeleteRatio <= 0 {
		c.DeleteRatio = preset.DeleteRatio
	}
	if c.TimeWindowDays <= 0 {
		c.TimeWindowDays = preset.TimeWindowDays
	}
	if c.BaseTime.IsZero() {
		c.BaseTime = defaults.BaseTime
	}
	return c
}

// Validate ensures generation inputs are coherent.
func (c GeneratorConfig) Validate() error {
	if !isValidScale(c.Scale) {
		return fmt.Errorf("invalid scale %q", c.Scale)
	}
	if !isValidDistribution(c.Distribution) {
		return fmt.Errorf("invalid distribution %q", c.Distribution)
	}
	if c.TradeCount <= 0 {
		return fmt.Errorf("trade count must be greater than zero")
	}
	if c.CustomerCount <= 0 {
		return fmt.Errorf("customer count must be greater than zero")
	}
	if c.SecurityCount <= 0 {
		return fmt.Errorf("security count must be greater than zero")
	}
	if c.OverlapRatio < 0 || c.OverlapRatio >= 1 {
		return fmt.Errorf("overlap ratio must be in [0,1)")
	}
	if c.DeleteRatio < 0 || c.DeleteRatio >= 1 {
		return fmt.Errorf("delete ratio must be in [0,1)")
	}
	if c.TimeWindowDays <= 0 {
		return fmt.Errorf("time window days must be greater than zero")
	}
	if c.BaseTime.IsZero() {
		return fmt.Errorf("base time must be set")
	}
	return nil
}

// MustScalePreset returns the preset or panics if it is unknown.
func MustScalePreset(scale Scale) ScalePreset {
	preset, err := ScalePresetFor(scale)
	if err != nil {
		panic(err)
	}
	return preset
}

// ScalePresetFor returns the preset associated with a scale.
func ScalePresetFor(scale Scale) (ScalePreset, error) {
	switch scale {
	case ScaleSmall:
		return ScalePreset{Scale: ScaleSmall, TradeCount: 100000, CustomerCount: 10000, SecurityCount: 1000, OverlapRatio: 0.05, DeleteRatio: 0.03, TimeWindowDays: 30}, nil
	case ScaleMedium:
		return ScalePreset{Scale: ScaleMedium, TradeCount: 1000000, CustomerCount: 100000, SecurityCount: 10000, OverlapRatio: 0.05, DeleteRatio: 0.03, TimeWindowDays: 30}, nil
	case ScaleLarge:
		return ScalePreset{Scale: ScaleLarge, TradeCount: 10000000, CustomerCount: 1000000, SecurityCount: 100000, OverlapRatio: 0.05, DeleteRatio: 0.03, TimeWindowDays: 30}, nil
	default:
		return ScalePreset{}, fmt.Errorf("unknown scale %q", scale)
	}
}

// DefaultScalePresets returns all known scale presets.
func DefaultScalePresets() []ScalePreset {
	return []ScalePreset{MustScalePreset(ScaleSmall), MustScalePreset(ScaleMedium), MustScalePreset(ScaleLarge)}
}

// RecordsForSchema returns the records for one schema, sorted deterministically.
func (d GeneratedDataset) RecordsForSchema(schemaName string) []GeneratedRecord {
	var out []GeneratedRecord
	for _, record := range d.Records {
		if record.SchemaName == schemaName {
			out = append(out, record)
		}
	}
	sortGeneratedRecords(out)
	return out
}

// LatestRecords keeps only the latest version of each row ID.
func LatestRecords(records []GeneratedRecord) []GeneratedRecord {
	latest := make(map[string]GeneratedRecord, len(records))
	for _, record := range records {
		key := fmt.Sprintf("%d:%s", record.SchemaID, record.RowID)
		existing, ok := latest[key]
		if !ok || record.ChangedAt > existing.ChangedAt || (record.ChangedAt == existing.ChangedAt && record.Version > existing.Version) {
			latest[key] = cloneGeneratedRecord(record)
		}
	}
	out := make([]GeneratedRecord, 0, len(latest))
	for _, record := range latest {
		out = append(out, record)
	}
	sortGeneratedRecords(out)
	return out
}

func summarizeDataset(records []GeneratedRecord) DatasetSummary {
	summary := DatasetSummary{CountsBySchema: make(map[string]int)}
	seen := make(map[string]int)
	for _, record := range records {
		summary.TotalRecords++
		summary.CountsBySchema[record.SchemaName]++
		if record.DeletedAt > 0 {
			summary.DeletedRecords++
		}
		key := fmt.Sprintf("%d:%s", record.SchemaID, record.RowID)
		seen[key]++
	}
	for _, count := range seen {
		summary.UniqueRows++
		if count > 1 {
			summary.DuplicateVersions += count - 1
			summary.OverlapRecords += count
		}
	}
	return summary
}

func sortGeneratedRecords(records []GeneratedRecord) {
	sort.Slice(records, func(i, j int) bool {
		if records[i].SchemaID != records[j].SchemaID {
			return records[i].SchemaID < records[j].SchemaID
		}
		if records[i].RowID != records[j].RowID {
			return records[i].RowID.String() < records[j].RowID.String()
		}
		return records[i].Version < records[j].Version
	})
}

func cloneGeneratedRecord(record GeneratedRecord) GeneratedRecord {
	clone := record
	if record.Attributes != nil {
		clone.Attributes = make(map[string]any, len(record.Attributes))
		for key, value := range record.Attributes {
			clone.Attributes[key] = value
		}
	}
	return clone
}
