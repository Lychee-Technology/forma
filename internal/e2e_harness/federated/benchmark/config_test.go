package benchmark

import "testing"

func TestDefaultConfigValidate(t *testing.T) {
	cfg := DefaultConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("DefaultConfig should validate: %v", err)
	}
}

func TestConfigValidateRejectsUnknownWorkload(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Workloads = []string{"does-not-exist"}
	if err := cfg.Validate(); err == nil {
		t.Fatalf("Validate should reject unknown workloads")
	}
}

func TestConfigValidateAcceptsLiveMode(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Mode = ExecutionModeLive
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate should accept live mode: %v", err)
	}
}

func TestConfigValidateRejectsUnknownTierProfile(t *testing.T) {
	cfg := DefaultConfig()
	cfg.TierProfile = "does-not-exist"
	if err := cfg.Validate(); err == nil {
		t.Fatalf("Validate should reject unknown tier profile")
	}
}
