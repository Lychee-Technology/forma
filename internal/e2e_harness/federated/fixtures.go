// Package federated provides test fixtures and data generators for E2E testing.
package federated

import (
	"fmt"
	"math/rand"
	"sort"
	"time"

	"github.com/google/uuid"
)

// GeneratorOptions configures test record generation.
type GeneratorOptions struct {
	SchemaID       int16
	TimeRangeHours int     // Range of time for records (hours ago)
	TimeOffset     int     // Offset from now (hours ago)
	DeletedRatio   float64 // Ratio of records to mark as deleted (0.0-1.0)
	Seed           int64   // Random seed for reproducibility (0 = use time)
}

// DefaultGeneratorOptions returns sensible defaults for test generation.
func DefaultGeneratorOptions() *GeneratorOptions {
	return &GeneratorOptions{
		SchemaID:       1,
		TimeRangeHours: 24,
		TimeOffset:     0,
		DeletedRatio:   0.0,
		Seed:           0,
	}
}

// GenerateTestRecords creates test records with realistic data distribution.
func GenerateTestRecords(count int, opts *GeneratorOptions) []TestRecord {
	if opts == nil {
		opts = DefaultGeneratorOptions()
	}

	// Initialize random with seed
	var r *rand.Rand
	if opts.Seed != 0 {
		r = rand.New(rand.NewSource(opts.Seed))
	} else {
		r = rand.New(rand.NewSource(time.Now().UnixNano()))
	}

	records := make([]TestRecord, count)
	baseTime := time.Now().Add(-time.Duration(opts.TimeOffset) * time.Hour)

	for i := 0; i < count; i++ {
		// Generate UUID v7 with time-based ordering
		rowID := uuid.Must(uuid.NewV7())

		// Calculate changed_at within the time range
		offsetHours := r.Intn(max(opts.TimeRangeHours, 1))
		changedAt := baseTime.Add(-time.Duration(offsetHours) * time.Hour).UnixMilli()

		// Determine if this record should be deleted
		deletedAt := int64(0)
		if opts.DeletedRatio > 0 && r.Float64() < opts.DeletedRatio {
			deletedAt = changedAt + int64(r.Intn(3600000)) // Deleted within 1 hour after creation
		}

		records[i] = TestRecord{
			RowID:    rowID,
			SchemaID: opts.SchemaID,
			Attributes: map[string]any{
				"name":       generateName(r),
				"email":      generateEmail(r),
				"score":      r.Intn(100),
				"created_at": changedAt / 1000, // Unix seconds
				"tags":       generateTags(r),
				"version":    1,
			},
			ChangedAt: changedAt,
			DeletedAt: deletedAt,
			FlushedAt: 0,
		}
	}

	return records
}

// GenerateOverlappingRecords creates multiple versions of the same row_id.
func GenerateOverlappingRecords(rowID uuid.UUID, versions int, schemaID int16) []TestRecord {
	records := make([]TestRecord, versions)
	baseTime := time.Now().Add(-time.Duration(versions) * time.Hour)

	for i := 0; i < versions; i++ {
		records[i] = TestRecord{
			RowID:    rowID,
			SchemaID: schemaID,
			Attributes: map[string]any{
				"name":    fmt.Sprintf("Version %d", i+1),
				"version": i + 1,
			},
			ChangedAt: baseTime.Add(time.Duration(i) * time.Hour).UnixMilli(),
			DeletedAt: 0,
			FlushedAt: 0,
		}
	}

	return records
}

// GenerateBulkRecords creates a large number of records for performance testing.
func GenerateBulkRecords(count int, schemaID int16, duplicateRatio float64) []TestRecord {
	opts := &GeneratorOptions{
		SchemaID:       schemaID,
		TimeRangeHours: 720, // 30 days
		TimeOffset:     0,
		DeletedRatio:   0.05, // 5% deleted
		Seed:           42,   // Reproducible
	}

	records := GenerateTestRecords(count, opts)

	// Add duplicates if requested
	if duplicateRatio > 0 && duplicateRatio < 1.0 {
		numDuplicates := int(float64(count) * duplicateRatio)
		for i := 0; i < numDuplicates; i++ {
			// Pick a random record to duplicate
			srcIdx := rand.Intn(len(records))
			srcRecord := records[srcIdx]

			// Create a newer version
			newRecord := TestRecord{
				RowID:    srcRecord.RowID, // Same row_id
				SchemaID: srcRecord.SchemaID,
				Attributes: map[string]any{
					"name":    fmt.Sprintf("%s (updated)", srcRecord.Attributes["name"]),
					"version": srcRecord.Attributes["version"].(int) + 1,
				},
				ChangedAt: srcRecord.ChangedAt + int64(rand.Intn(3600000)), // Newer
				DeletedAt: 0,
				FlushedAt: 0,
			}
			records = append(records, newRecord)
		}
	}

	return records
}

// PresetScenarios provides predefined test scenarios.
type PresetScenarios struct{}

// ThreeTierNoOverlap returns records for a scenario with data in all tiers but no overlap.
func (PresetScenarios) ThreeTierNoOverlap(schemaID int16) (base, delta, hot []TestRecord) {
	// Base: 1000 records from 30 days ago
	base = GenerateTestRecords(1000, &GeneratorOptions{
		SchemaID:       schemaID,
		TimeRangeHours: 720,
		TimeOffset:     720,
		Seed:           100,
	})

	// Delta: 500 records from 2 days ago
	delta = GenerateTestRecords(500, &GeneratorOptions{
		SchemaID:       schemaID,
		TimeRangeHours: 48,
		TimeOffset:     48,
		Seed:           200,
	})

	// Hot: 200 records from last hour
	hot = GenerateTestRecords(200, &GeneratorOptions{
		SchemaID:       schemaID,
		TimeRangeHours: 1,
		TimeOffset:     0,
		Seed:           300,
	})

	return
}

// DeduplicationScenario returns records with overlapping row_ids for dedup testing.
func (PresetScenarios) DeduplicationScenario(schemaID int16) (base, delta, hot []TestRecord) {
	sharedRowID := uuid.Must(uuid.NewV7())
	baseTime := time.Now()

	// Base: oldest version
	base = []TestRecord{{
		RowID:    sharedRowID,
		SchemaID: schemaID,
		Attributes: map[string]any{
			"name":    "Original",
			"version": 1,
		},
		ChangedAt: baseTime.Add(-100 * time.Hour).UnixMilli(),
	}}

	// Delta: middle version
	delta = []TestRecord{{
		RowID:    sharedRowID,
		SchemaID: schemaID,
		Attributes: map[string]any{
			"name":    "Updated in Delta",
			"version": 2,
		},
		ChangedAt: baseTime.Add(-10 * time.Hour).UnixMilli(),
	}}

	// Hot: newest version
	hot = []TestRecord{{
		RowID:    sharedRowID,
		SchemaID: schemaID,
		Attributes: map[string]any{
			"name":    "Latest in Hot",
			"version": 3,
		},
		ChangedAt: baseTime.UnixMilli(),
	}}

	return
}

// SoftDeleteScenario returns records for soft delete testing.
func (PresetScenarios) SoftDeleteScenario(schemaID int16) []TestRecord {
	now := time.Now().UnixMilli()
	records := make([]TestRecord, 10)

	for i := 0; i < 10; i++ {
		deletedAt := int64(0)
		if i >= 5 { // Last 5 records are deleted
			deletedAt = now - int64(rand.Intn(3600000))
		}

		records[i] = TestRecord{
			RowID:    uuid.Must(uuid.NewV7()),
			SchemaID: schemaID,
			Attributes: map[string]any{
				"name":    fmt.Sprintf("Record-%d", i),
				"version": 1,
			},
			ChangedAt: now - int64((i+1)*3600000),
			DeletedAt: deletedAt,
		}
	}

	return records
}

// CDCFlushScenario returns records for CDC flush threshold testing.
func (PresetScenarios) CDCFlushScenario(schemaID int16, count int, ageHours int) []TestRecord {
	oldest := time.Now().Add(-time.Duration(ageHours) * time.Hour)

	records := GenerateTestRecords(count, &GeneratorOptions{
		SchemaID:       schemaID,
		TimeRangeHours: ageHours,
		TimeOffset:     0,
	})

	// Ensure at least one record is at the oldest time
	if len(records) > 0 {
		records[0].ChangedAt = oldest.UnixMilli()
	}

	return records
}

// PerformanceScenario returns large datasets for performance testing.
type PerformanceDataset struct {
	Base  []TestRecord
	Delta []TestRecord
	Hot   []TestRecord
}

// MediumScale returns a 100K total record dataset.
func (PresetScenarios) MediumScale(schemaID int16) *PerformanceDataset {
	return &PerformanceDataset{
		Base:  GenerateBulkRecords(60000, schemaID, 0.05),
		Delta: GenerateBulkRecords(30000, schemaID, 0.03),
		Hot:   GenerateBulkRecords(10000, schemaID, 0.02),
	}
}

// LargeScale returns a 500K total record dataset.
func (PresetScenarios) LargeScale(schemaID int16) *PerformanceDataset {
	return &PerformanceDataset{
		Base:  GenerateBulkRecords(300000, schemaID, 0.05),
		Delta: GenerateBulkRecords(150000, schemaID, 0.03),
		Hot:   GenerateBulkRecords(50000, schemaID, 0.02),
	}
}

// ============================================================================
// Helper Functions
// ============================================================================

var firstNames = []string{
	"Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry",
	"Iris", "Jack", "Kate", "Liam", "Mary", "Nick", "Olivia", "Paul",
	"Quinn", "Rose", "Sam", "Tina", "Uma", "Victor", "Wendy", "Xavier", "Yara", "Zoe",
}

var lastNames = []string{
	"Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
	"Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
	"Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
}

var domains = []string{
	"gmail.com", "yahoo.com", "outlook.com", "example.com", "test.io", "company.org",
}

var tagOptions = []string{
	"developer", "designer", "manager", "intern", "senior", "lead", "junior",
	"frontend", "backend", "fullstack", "devops", "qa", "data", "ml",
}

func generateName(r *rand.Rand) string {
	return fmt.Sprintf("%s %s",
		firstNames[r.Intn(len(firstNames))],
		lastNames[r.Intn(len(lastNames))])
}

func generateEmail(r *rand.Rand) string {
	return fmt.Sprintf("%s.%s@%s",
		firstNames[r.Intn(len(firstNames))],
		lastNames[r.Intn(len(lastNames))],
		domains[r.Intn(len(domains))])
}

func generateTags(r *rand.Rand) []string {
	count := r.Intn(3) + 1
	tags := make([]string, count)
	for i := 0; i < count; i++ {
		tags[i] = tagOptions[r.Intn(len(tagOptions))]
	}
	return tags
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// SortByChangedAt sorts records by changed_at in ascending order.
func SortByChangedAt(records []TestRecord) {
	sort.Slice(records, func(i, j int) bool {
		return records[i].ChangedAt < records[j].ChangedAt
	})
}

// SortByChangedAtDesc sorts records by changed_at in descending order.
func SortByChangedAtDesc(records []TestRecord) {
	sort.Slice(records, func(i, j int) bool {
		return records[i].ChangedAt > records[j].ChangedAt
	})
}

// FilterDeleted returns only non-deleted records.
func FilterDeleted(records []TestRecord) []TestRecord {
	var result []TestRecord
	for _, r := range records {
		if r.DeletedAt == 0 {
			result = append(result, r)
		}
	}
	return result
}

// GroupByRowID groups records by row_id.
func GroupByRowID(records []TestRecord) map[uuid.UUID][]TestRecord {
	groups := make(map[uuid.UUID][]TestRecord)
	for _, r := range records {
		groups[r.RowID] = append(groups[r.RowID], r)
	}
	return groups
}

// DeduplicateByRowID keeps only the latest version of each row_id.
func DeduplicateByRowID(records []TestRecord) []TestRecord {
	latest := make(map[uuid.UUID]TestRecord)
	for _, r := range records {
		if existing, ok := latest[r.RowID]; !ok || r.ChangedAt > existing.ChangedAt {
			latest[r.RowID] = r
		}
	}

	result := make([]TestRecord, 0, len(latest))
	for _, r := range latest {
		result = append(result, r)
	}
	return result
}
