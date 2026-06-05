package benchmark

import (
	"reflect"
	"strconv"
	"testing"
)

func TestScalePresetForScale(t *testing.T) {
	preset, err := ScalePresetFor(ScaleSmall)
	if err != nil {
		t.Fatalf("ScalePresetFor failed: %v", err)
	}
	if preset.TradeCount != 100000 {
		t.Fatalf("unexpected small trade count: %d", preset.TradeCount)
	}
}

func TestGeneratorDeterministic(t *testing.T) {
	cfg := GeneratorConfig{Scale: ScaleSmall, Distribution: DistributionUniform, Seed: 99, TradeCount: 40, CustomerCount: 10, SecurityCount: 5, OverlapRatio: 0.05, DeleteRatio: 0.03, BaseTime: defaultBaseTime}.WithDefaults()
	g1, err := NewGenerator(cfg)
	if err != nil {
		t.Fatalf("NewGenerator failed: %v", err)
	}
	g2, err := NewGenerator(cfg)
	if err != nil {
		t.Fatalf("NewGenerator failed: %v", err)
	}
	d1, err := g1.Generate()
	if err != nil {
		t.Fatalf("Generate failed: %v", err)
	}
	d2, err := g2.Generate()
	if err != nil {
		t.Fatalf("Generate failed: %v", err)
	}
	if !reflect.DeepEqual(d1.Summary, d2.Summary) {
		t.Fatalf("deterministic summaries differ: %+v vs %+v", d1.Summary, d2.Summary)
	}
	if len(d1.Records) != len(d2.Records) {
		t.Fatalf("record lengths differ")
	}
	for i := range d1.Records {
		if d1.Records[i].RowID != d2.Records[i].RowID || d1.Records[i].Version != d2.Records[i].Version || d1.Records[i].ChangedAt != d2.Records[i].ChangedAt {
			t.Fatalf("record %d differs between deterministic runs", i)
		}
	}
}

func TestGeneratorProducesAllSchemas(t *testing.T) {
	g, err := NewGenerator(GeneratorConfig{Scale: ScaleSmall, Distribution: DistributionUniform, Seed: 42, TradeCount: 60, CustomerCount: 12, SecurityCount: 6, BaseTime: defaultBaseTime})
	if err != nil {
		t.Fatalf("NewGenerator failed: %v", err)
	}
	dataset, err := g.Generate()
	if err != nil {
		t.Fatalf("Generate failed: %v", err)
	}
	if dataset.Summary.CountsBySchema["trade"] == 0 || dataset.Summary.CountsBySchema["customer"] == 0 || dataset.Summary.CountsBySchema["security"] == 0 {
		t.Fatalf("expected non-zero counts for all schemas: %+v", dataset.Summary.CountsBySchema)
	}
}

func TestHotspotDistributionCreatesDuplicateVersions(t *testing.T) {
	g, err := NewGenerator(GeneratorConfig{Scale: ScaleSmall, Distribution: DistributionHotspot, Seed: 7, TradeCount: 100, CustomerCount: 20, SecurityCount: 10, OverlapRatio: 0.10, BaseTime: defaultBaseTime})
	if err != nil {
		t.Fatalf("NewGenerator failed: %v", err)
	}
	dataset, err := g.Generate()
	if err != nil {
		t.Fatalf("Generate failed: %v", err)
	}
	if dataset.Summary.DuplicateVersions == 0 {
		t.Fatalf("expected duplicate versions for hotspot distribution")
	}
	tradeRecords := dataset.RecordsForSchema("trade")
	if len(LatestRecords(tradeRecords)) >= len(tradeRecords) {
		t.Fatalf("expected deduped trade records to be fewer than raw trade records")
	}
}

func TestGeneratorEncodesTradeTimeAsUnixMillisString(t *testing.T) {
	g, err := NewGenerator(GeneratorConfig{Scale: ScaleSmall, Distribution: DistributionUniform, Seed: 42, TradeCount: 10, CustomerCount: 4, SecurityCount: 3, BaseTime: defaultBaseTime})
	if err != nil {
		t.Fatalf("NewGenerator failed: %v", err)
	}
	dataset, err := g.Generate()
	if err != nil {
		t.Fatalf("Generate failed: %v", err)
	}
	tradeRecords := dataset.RecordsForSchema("trade")
	if len(tradeRecords) == 0 {
		t.Fatal("expected generated trade records")
	}
	tradeTime, ok := tradeRecords[0].Attributes["tradeTime"].(string)
	if !ok {
		t.Fatalf("expected tradeTime to be stored as string, got %T", tradeRecords[0].Attributes["tradeTime"])
	}
	if _, err := strconv.ParseInt(tradeTime, 10, 64); err != nil {
		t.Fatalf("expected tradeTime to be unix millis string, got %q: %v", tradeTime, err)
	}
}

func TestCalibrateTradeFilters(t *testing.T) {
	g, err := NewGenerator(GeneratorConfig{Scale: ScaleSmall, Distribution: DistributionUniform, Seed: 42, TradeCount: 200, CustomerCount: 40, SecurityCount: 20, BaseTime: defaultBaseTime})
	if err != nil {
		t.Fatalf("NewGenerator failed: %v", err)
	}
	dataset, err := g.Generate()
	if err != nil {
		t.Fatalf("Generate failed: %v", err)
	}
	calibration, err := CalibrateTradeFilters(dataset.Records)
	if err != nil {
		t.Fatalf("CalibrateTradeFilters failed: %v", err)
	}
	if calibration.High.Band != SelectivityBandHigh || calibration.Medium.Band != SelectivityBandMedium || calibration.Low.Band != SelectivityBandLow {
		t.Fatalf("unexpected selectivity bands: %+v", calibration)
	}
	if calibration.High.Matches == 0 || calibration.Medium.Matches == 0 || calibration.Low.Matches == 0 {
		t.Fatalf("expected all calibration buckets to have matches: %+v", calibration)
	}
}
