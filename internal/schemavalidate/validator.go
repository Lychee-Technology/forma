// Package schemavalidate resolves entity JSON Schemas once and validates write
// payloads against them.
//
// Resolution is done exactly once per schema, at construction. Measured against
// the largest shipped schema, resolving costs ~1.58ms while validating an
// already-resolved schema costs ~5.5us — a ~250x ratio, so re-resolving per
// request would add ~0.7s to a 500-record batch create (#314).
//
// Cross-file references are restricted to plain siblings inside the schema
// directory. A schema may say "lead.json#/properties/contact"; it may not reach
// a subdirectory, a parent, or the network. Anything else is rejected loudly
// rather than being rewritten to a same-named local file, which would validate
// a document against unrelated constraints.
package schemavalidate

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/jsonschema-go/jsonschema"
)

// fileLoader resolves cross-file $refs such as "lead.json#/$defs/lead_id" by
// reading the sibling file out of the schema directory. Without a Loader the
// library refuses remote references outright, which is why visit.json and
// log.json could never be validated before #314.
//
// absDir must be absolute and cleaned; the sibling check compares against it
// directly. The URL arriving here has already been resolved against BaseURI, so
// a sibling ref presents as an absolute file path inside absDir.
func fileLoader(absDir string) jsonschema.Loader {
	return func(u *url.URL) (*jsonschema.Schema, error) {
		target, err := siblingPath(absDir, u)
		if err != nil {
			return nil, err
		}
		return loadSchemaFile(target)
	}
}

// siblingPath maps a resolved reference URL to a file in absDir, rejecting any
// reference that is not a plain sibling. The error names the reference so a
// misconfigured schema is diagnosable without reading the loader.
func siblingPath(absDir string, u *url.URL) (string, error) {
	if u.Scheme != "" && u.Scheme != "file" {
		return "", fmt.Errorf(
			"failed to resolve schema reference %q: only sibling file references are supported, got scheme %q",
			u.String(), u.Scheme)
	}
	if u.Host != "" {
		return "", fmt.Errorf(
			"failed to resolve schema reference %q: only sibling file references are supported, got host %q",
			u.String(), u.Host)
	}

	target := filepath.Clean(filepath.FromSlash(u.Path))
	if filepath.Dir(target) != absDir {
		return "", fmt.Errorf(
			"failed to resolve schema reference %q: only sibling files in %s may be referenced, got %s",
			u.String(), absDir, target)
	}
	if filepath.Base(target) == "." || filepath.Base(target) == string(filepath.Separator) {
		return "", fmt.Errorf("failed to resolve schema reference %q: no file name in path", u.String())
	}
	return target, nil
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
//
// The directory is made absolute here rather than by the caller: a relative
// SCHEMA_DIR containing ".." would otherwise be parsed with the ".." as the URI
// *host*, and the loader would read a path that appears in no schema. The URI
// is built through url.URL so that '#', '%' and spaces in the directory name
// are escaped instead of corrupting the URI.
func resolveOptions(dir string) (*jsonschema.ResolveOptions, error) {
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve schema directory %s to an absolute path: %w", dir, err)
	}

	basePath := filepath.ToSlash(absDir)
	if !strings.HasPrefix(basePath, "/") {
		// Windows volume paths ("C:/x") need a leading slash to form file:///C:/x.
		basePath = "/" + basePath
	}
	base := &url.URL{Scheme: "file", Path: basePath + "/"}

	return &jsonschema.ResolveOptions{
		BaseURI: base.String(),
		Loader:  fileLoader(absDir),
	}, nil
}

// resolveSchemaFile resolves one schema file from dir. Used by construction and
// by tests that assert the shipped schemas are resolvable.
func resolveSchemaFile(dir, fileName string) (*jsonschema.Resolved, error) {
	s, err := loadSchemaFile(filepath.Join(dir, fileName))
	if err != nil {
		return nil, fmt.Errorf("failed to load schema %s: %w", fileName, err)
	}
	opts, err := resolveOptions(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to build resolve options for schema %s: %w", fileName, err)
	}
	resolved, err := s.Resolve(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve schema %s: %w", fileName, err)
	}
	return resolved, nil
}
