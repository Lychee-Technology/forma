package production

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestValidateFixtureSchemaID(t *testing.T) {
	for _, id := range []int16{100, 101, 102} {
		if err := ValidateFixtureSchemaID(id); err == nil {
			t.Errorf("ValidateFixtureSchemaID(%d) = nil, want benchmark-range error", id)
		}
	}
	if err := ValidateFixtureSchemaID(0); err == nil {
		t.Error("ValidateFixtureSchemaID(0) = nil, want error")
	}
	for _, ref := range DefaultSchemaFixtures() {
		if err := ValidateFixtureSchemaID(ref.ID); err != nil {
			t.Errorf("fixture %s: %v", ref.Name, err)
		}
	}
}

// TestFixtureSchemaFilesParse ensures every bundled fixture has parseable
// schema and attribute JSON files.
func TestFixtureSchemaFilesParse(t *testing.T) {
	dir := FixtureSchemasDir()
	for _, ref := range DefaultSchemaFixtures() {
		for _, name := range []string{ref.Name + ".json", ref.Name + "_attributes.json"} {
			data, err := os.ReadFile(filepath.Join(dir, name))
			if err != nil {
				t.Fatalf("read fixture %s: %v", name, err)
			}
			var payload map[string]any
			if err := json.Unmarshal(data, &payload); err != nil {
				t.Fatalf("parse fixture %s: %v", name, err)
			}
			if len(payload) == 0 {
				t.Fatalf("fixture %s is empty", name)
			}
		}
	}
}
