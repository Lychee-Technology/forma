package manifestv2

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/lychee-technology/forma/internal/publication"
)

const inputGenerationField = "input_generation"

var (
	manifestFields = []string{"format", "schema_id", "epoch", "generation", "parent_generation", "lineage", "token", "kind", "files"}
	fileFields     = []string{"tier", "key", "size_bytes", "sha256", "row_count", "row_id_min", "row_id_max", "columns"}
)

// checkWire refuses a document whose members are not exactly the V2 wire
// format's, before a typed decode reads away what tells them apart: a member
// named twice anywhere (encoding/json keeps the last), a member spelled other
// than exactly (the struct decoder folds case, so "Kind" would pass as
// "kind"), an unknown member, and a required member absent or null (a typed
// decode reads it as a zero value: a parent of 0, an unset lineage).
// input_generation is required on a compact manifest and absent from any
// other, even as 0 or null.
func checkWire(raw []byte) error {
	if err := noDuplicateMembers(json.NewDecoder(bytes.NewReader(raw))); err != nil {
		return err
	}
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(raw, &doc); err != nil {
		return fmt.Errorf("decode: %w", err)
	}
	fields := manifestFields
	var kind publication.Kind
	if err := json.Unmarshal(doc["kind"], &kind); err == nil && kind == publication.KindCompact {
		fields = append(slices.Clone(manifestFields), inputGenerationField)
	}
	if err := exactMembers(doc, fields); err != nil {
		return err
	}
	var files []map[string]json.RawMessage
	if err := json.Unmarshal(doc["files"], &files); err != nil {
		return fmt.Errorf("decode files: %w", err)
	}
	for i, f := range files {
		if err := exactMembers(f, fileFields); err != nil {
			return fmt.Errorf("file %d: %w", i, err)
		}
	}
	return nil
}

// exactMembers refuses an object that lacks one of fields, holds it as null,
// or holds a member fields does not name. Map keys are the members' exact
// names, so a case-folded alias is an unknown member here.
func exactMembers(obj map[string]json.RawMessage, fields []string) error {
	for _, name := range fields {
		if v, ok := obj[name]; !ok || string(v) == "null" {
			return fmt.Errorf("required field %s is absent or null", name)
		}
	}
	for name := range obj {
		if !slices.Contains(fields, name) {
			return fmt.Errorf("field %q is not one the %s format has here", name, publication.ManifestFormat)
		}
	}
	return nil
}

// noDuplicateMembers reads one JSON value from dec and refuses it when any
// object in it, at any depth, names a member twice.
func noDuplicateMembers(dec *json.Decoder) error {
	tok, err := dec.Token()
	if err != nil {
		return fmt.Errorf("decode: %w", err)
	}
	switch tok {
	case json.Delim('{'):
		seen := make(map[string]bool)
		for dec.More() {
			name, err := dec.Token()
			if err != nil {
				return fmt.Errorf("decode: %w", err)
			}
			key, _ := name.(string)
			if seen[key] {
				return fmt.Errorf("member %q appears twice in one object", key)
			}
			seen[key] = true
			if err := noDuplicateMembers(dec); err != nil {
				return err
			}
		}
	case json.Delim('['):
		for dec.More() {
			if err := noDuplicateMembers(dec); err != nil {
				return err
			}
		}
	default:
		return nil
	}
	// The closing delimiter.
	if _, err := dec.Token(); err != nil {
		return fmt.Errorf("decode: %w", err)
	}
	return nil
}
