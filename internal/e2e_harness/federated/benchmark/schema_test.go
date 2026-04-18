package benchmark

import "testing"

func TestLoadFixtureRegistry(t *testing.T) {
	registry, err := LoadFixtureRegistry()
	if err != nil {
		t.Fatalf("LoadFixtureRegistry failed: %v", err)
	}
	for _, fixture := range DefaultSchemaFixtures() {
		id, cache, err := registry.GetSchemaAttributeCacheByName(fixture.Name)
		if err != nil {
			t.Fatalf("GetSchemaAttributeCacheByName(%q) failed: %v", fixture.Name, err)
		}
		if id != fixture.ID {
			t.Fatalf("fixture %s expected schema ID %d, got %d", fixture.Name, fixture.ID, id)
		}
		if len(cache) == 0 {
			t.Fatalf("fixture %s loaded an empty attribute cache", fixture.Name)
		}
	}
}

func TestRegisterFixtureSchemas(t *testing.T) {
	registrar := &captureRegistrar{}
	if err := RegisterFixtureSchemas(registrar); err != nil {
		t.Fatalf("RegisterFixtureSchemas failed: %v", err)
	}
	fixtures := DefaultSchemaFixtures()
	if len(registrar.calls) != len(fixtures) {
		t.Fatalf("expected %d calls, got %d", len(fixtures), len(registrar.calls))
	}
	for i, fixture := range fixtures {
		if registrar.calls[i].ID != fixture.ID || registrar.calls[i].Name != fixture.Name {
			t.Fatalf("call %d mismatch: got %+v want ID=%d Name=%s", i, registrar.calls[i], fixture.ID, fixture.Name)
		}
	}
}

type captureRegistrar struct {
	calls []SchemaFixture
}

func (r *captureRegistrar) SetupSchema(schemaID int16, schemaName string) error {
	r.calls = append(r.calls, SchemaFixture{ID: schemaID, Name: schemaName})
	return nil
}
