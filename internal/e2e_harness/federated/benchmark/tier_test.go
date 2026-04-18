package benchmark

import (
	"context"
	"testing"

	federated "github.com/lychee-technology/forma/internal/e2e_harness/federated"
)

func TestSplitIntoTiersBalanced(t *testing.T) {
	g, err := NewGenerator(GeneratorConfig{Scale: ScaleSmall, Distribution: DistributionHotspot, Seed: 42, TradeCount: 120, CustomerCount: 10, SecurityCount: 5, OverlapRatio: 0.10, BaseTime: defaultBaseTime})
	if err != nil {
		t.Fatalf("NewGenerator failed: %v", err)
	}
	dataset, err := g.Generate()
	if err != nil {
		t.Fatalf("Generate failed: %v", err)
	}
	tiered, err := SplitIntoTiers(dataset, TierMixBalanced)
	if err != nil {
		t.Fatalf("SplitIntoTiers failed: %v", err)
	}
	if len(tiered.Base) == 0 || len(tiered.Delta) == 0 || len(tiered.Hot) == 0 {
		t.Fatalf("expected all three tiers to be populated: %+v", tiered.Summary)
	}
	if tiered.Summary.OverlappingKeys == 0 {
		t.Fatalf("expected overlap keys to be tracked")
	}
}

func TestToTestRecordsPreservesKeyFields(t *testing.T) {
	records := []GeneratedRecord{{
		SchemaID:   SchemaIDTrade,
		SchemaName: "trade",
		RowID:      deterministicRowID(1, "trade", 1),
		Version:    2,
		ChangedAt:  defaultBaseTime.UnixMilli(),
		Attributes: map[string]any{"symbol": "SYM00001", "tradeType": 1},
	}}
	converted := ToTestRecords(records)
	if len(converted) != 1 {
		t.Fatalf("expected one converted record")
	}
	if converted[0].Attributes["version"] != 2 {
		t.Fatalf("expected version to be preserved")
	}
	if converted[0].SchemaID != SchemaIDTrade {
		t.Fatalf("unexpected schema ID %d", converted[0].SchemaID)
	}
}

func TestLoadTieredDatasetUsesLoader(t *testing.T) {
	loader := &captureTierLoader{}
	dataset := &TieredDataset{
		Profile: TierMixBalanced,
		Base:    []GeneratedRecord{{SchemaID: SchemaIDCustomer, SchemaName: "customer", RowID: deterministicRowID(1, "customer", 1), Attributes: map[string]any{"name": "customer"}}},
		Delta:   []GeneratedRecord{{SchemaID: SchemaIDTrade, SchemaName: "trade", RowID: deterministicRowID(1, "trade", 1), Attributes: map[string]any{"symbol": "SYM00001"}}},
		Hot:     []GeneratedRecord{{SchemaID: SchemaIDTrade, SchemaName: "trade", RowID: deterministicRowID(1, "trade", 2), Attributes: map[string]any{"symbol": "SYM00002"}}},
	}
	if err := LoadTieredDataset(context.Background(), loader, dataset); err != nil {
		t.Fatalf("LoadTieredDataset failed: %v", err)
	}
	if !loader.cleared {
		t.Fatalf("expected ClearAllData to be called")
	}
	if len(loader.schemas) != len(DefaultSchemaFixtures()) {
		t.Fatalf("expected fixture schemas to be registered")
	}
	if len(loader.baseWrites) != 1 || len(loader.deltaWrites) != 1 || len(loader.hotWrites) != 1 {
		t.Fatalf("expected one write per tier bucket")
	}
	if loader.baseWrites[0][0].SchemaID != SchemaIDCustomer {
		t.Fatalf("expected base write to stay schema-scoped")
	}
}

func TestResolveTierMixProfile(t *testing.T) {
	profile, err := ResolveTierMixProfile("high-hot")
	if err != nil {
		t.Fatalf("ResolveTierMixProfile failed: %v", err)
	}
	if profile.Name != TierMixHighHot.Name {
		t.Fatalf("unexpected profile: %+v", profile)
	}
	if _, err := ResolveTierMixProfile("unknown"); err == nil {
		t.Fatalf("expected unknown tier profile to fail")
	}
}

type captureTierLoader struct {
	cleared     bool
	schemas     []SchemaFixture
	baseWrites  [][]federated.TestRecord
	deltaWrites [][]federated.TestRecord
	hotWrites   [][]federated.TestRecord
}

func (c *captureTierLoader) SetupSchema(schemaID int16, schemaName string) error {
	c.schemas = append(c.schemas, SchemaFixture{ID: schemaID, Name: schemaName})
	return nil
}

func (c *captureTierLoader) ClearAllData(context.Context) error {
	c.cleared = true
	return nil
}

func (c *captureTierLoader) WriteParquet(_ context.Context, tier, _ string, records []federated.TestRecord) error {
	copyRecords := append([]federated.TestRecord(nil), records...)
	switch tier {
	case "base":
		c.baseWrites = append(c.baseWrites, copyRecords)
	case "delta":
		c.deltaWrites = append(c.deltaWrites, copyRecords)
	}
	return nil
}

func (c *captureTierLoader) SeedHotRecordsWithData(_ context.Context, records []federated.TestRecord) error {
	c.hotWrites = append(c.hotWrites, append([]federated.TestRecord(nil), records...))
	return nil
}
