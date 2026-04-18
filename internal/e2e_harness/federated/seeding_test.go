package federated

import "testing"

func TestDeterministicAttributeIDStable(t *testing.T) {
	first := DeterministicAttributeID(102, "symbol")
	second := DeterministicAttributeID(102, "symbol")
	if first != second {
		t.Fatalf("expected deterministic attribute IDs to match")
	}
	third := DeterministicAttributeID(102, "price")
	if first == third {
		t.Fatalf("expected different attributes to hash differently")
	}
}
