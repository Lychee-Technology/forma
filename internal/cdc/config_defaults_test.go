package cdc

import "testing"

func TestCDCConfigWithDefaults_UsesSpecThresholds(t *testing.T) {
	cfg := CDCConfig{}.WithDefaults()

	if cfg.MinRecords != 20000 {
		t.Fatalf("MinRecords default mismatch: got %d", cfg.MinRecords)
	}
	if cfg.MaxAgeMs != 3600000 {
		t.Fatalf("MaxAgeMs default mismatch: got %d", cfg.MaxAgeMs)
	}
	if cfg.EstimatedRowBytes != DefaultEstimatedRowBytes {
		t.Fatalf("EstimatedRowBytes default mismatch: got %d", cfg.EstimatedRowBytes)
	}
	if cfg.MaxBatchBytes != DefaultMaxBatchBytes {
		t.Fatalf("MaxBatchBytes default mismatch: got %d", cfg.MaxBatchBytes)
	}
}

func TestCDCConfigWithDefaults_PreservesProvidedValues(t *testing.T) {
	cfg := CDCConfig{MinRecords: 10, MaxAgeMs: 500}.WithDefaults()

	if cfg.MinRecords != 10 {
		t.Fatalf("MinRecords should preserve explicit value, got %d", cfg.MinRecords)
	}
	if cfg.MaxAgeMs != 500 {
		t.Fatalf("MaxAgeMs should preserve explicit value, got %d", cfg.MaxAgeMs)
	}
}
