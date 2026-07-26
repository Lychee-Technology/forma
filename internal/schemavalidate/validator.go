// Package schemavalidate resolves entity JSON Schemas once and validates write
// payloads against them.
//
// Resolution is done exactly once per schema, at construction. Measured against
// the largest shipped schema, resolving costs ~1.58ms while validating an
// already-resolved schema costs ~5.5us — a ~250x ratio, so re-resolving per
// request would add ~0.7s to a 500-record batch create (#314).
package schemavalidate

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"

	"github.com/google/jsonschema-go/jsonschema"
)

// fileLoader resolves cross-file $refs such as "lead.json#/$defs/lead_id" by
// reading the sibling file out of the schema directory. Without a Loader the
// library refuses remote references outright, which is why visit.json and
// log.json could never be validated before #314.
func fileLoader(dir string) jsonschema.Loader {
	return func(u *url.URL) (*jsonschema.Schema, error) {
		name := path.Base(u.Path)
		if name == "" || name == "." || name == "/" {
			return nil, fmt.Errorf("failed to resolve schema reference %q: no file name in path", u.String())
		}
		return loadSchemaFile(filepath.Join(dir, name))
	}
}

func loadSchemaFile(fullPath string) (*jsonschema.Schema, error) {
	data, err := os.ReadFile(filepath.Clean(fullPath))
	if err != nil {
		return nil, fmt.Errorf("failed to read schema file %s: %w", fullPath, err)
	}
	var s jsonschema.Schema
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("failed to parse schema file %s: %w", fullPath, err)
	}
	return &s, nil
}

// resolveOptions builds the options used for every resolve. BaseURI must be
// absolute with a scheme, so relative refs like "lead.json#/$defs/x" resolve to
// a file URL the loader can turn back into a sibling path.
func resolveOptions(dir string) *jsonschema.ResolveOptions {
	return &jsonschema.ResolveOptions{
		BaseURI: "file://" + filepath.ToSlash(dir) + "/",
		Loader:  fileLoader(dir),
	}
}

// resolveSchemaFile resolves one schema file from dir. Used by construction and
// by tests that assert the shipped schemas are resolvable.
func resolveSchemaFile(dir, fileName string) (*jsonschema.Resolved, error) {
	s, err := loadSchemaFile(filepath.Join(dir, fileName))
	if err != nil {
		return nil, err
	}
	resolved, err := s.Resolve(resolveOptions(dir))
	if err != nil {
		return nil, fmt.Errorf("failed to resolve schema %s: %w", fileName, err)
	}
	return resolved, nil
}
