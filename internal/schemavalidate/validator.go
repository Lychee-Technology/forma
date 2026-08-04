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
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/lychee-technology/forma"
)

// fileLoader resolves cross-file $refs such as "lead.json#/$defs/lead_id" by
// reading the sibling file out of the schema directory. Without a Loader the
// library refuses remote references outright, which is why visit.json and
// log.json could never be validated before #314.
//
// absDir must be absolute and cleaned; the sibling check compares against it
// directly. The URL arriving here has already been resolved against BaseURI, so
// a sibling ref presents as an absolute file path inside absDir.
//
// Every loaded document gets the same construction guard the registered roots
// get. New checks only what the registry holds, so without this a root
// referencing a file with a typo'd "type" constructs cleanly and the fault
// surfaces inside Validate — where the blanket wrap calls it caller input and
// answers 400 to every create, while report-only updates absorb it and write
// anyway. A configuration fault belongs at startup, whichever file holds it.
//
// The check runs once per resolved path. Within one Resolve the library's own
// r.loaded cache already calls a loader at most once per URI, so cycle
// termination and diamond dedupe are defence-in-depth here rather than
// load-bearing. What the memo actually buys is reuse across the several Resolve
// calls that share the loader New builds: one walk per file for the whole
// registry, not one per referring root.
//
// The map needs no lock — loaders run inside Resolve, which New drives
// sequentially, and a fresh loader is built per resolveOptions call.
func fileLoader(absDir string) jsonschema.Loader {
	checked := make(map[string]error)
	return func(u *url.URL) (*jsonschema.Schema, error) {
		target, err := siblingPath(absDir, u)
		if err != nil {
			return nil, fmt.Errorf("failed to load cross-file schema reference: %w", err)
		}
		s, err := loadSchemaFile(target)
		if err != nil {
			return nil, fmt.Errorf("failed to load cross-file schema reference: %w", err)
		}
		if err := checkLoadedSchema(checked, target, s); err != nil {
			return nil, err
		}
		return s, nil
	}
}

// checkLoadedSchema applies the construction guard to a referenced document,
// memoising the verdict under its resolved path so a file referenced from
// several roots is walked once (see fileLoader for what that is and is not
// worth).
//
// The verdict is cached, not the *jsonschema.Schema. Handing one parsed document
// to two Resolve calls would be unsafe for a reason that is easy to miss:
// resolve.go:550 writes the *referring* schema's $schema into the loaded
// document when the loaded one has none, so the second resolve would inherit the
// first's draft and detect a different version.
//
// The error names the file: the operator otherwise sees only the registered root
// that referenced it, which may be several hops away from the typo.
func checkLoadedSchema(checked map[string]error, path string, s *jsonschema.Schema) error {
	if err, seen := checked[path]; seen {
		return err
	}

	var err error
	if checkErr := checkSchemaSupported(s); checkErr != nil {
		err = fmt.Errorf("failed to accept referenced schema file %s for validation: %w", path, checkErr)
	}
	checked[path] = err
	return err
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

// resolveSchemaFile resolves one schema file from dir, straight off disk.
//
// New does not call it: New resolves the document the registry holds, not the
// file, so it inlines the same steps against that document. This is the
// file-based counterpart, used by the tests that assert the shipped schemas on
// disk are resolvable.
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

// Validator holds one resolved schema per schema ID. It is built once at
// startup and is safe for concurrent use: the map is never written after New
// returns, and jsonschema.Resolved is read-only during Validate.
type Validator struct {
	resolved map[int16]*jsonschema.Resolved
	// arrays is derived alongside resolution because the schema is the only
	// loaded source of truth for which paths are arrays (see array_paths.go).
	arrays map[int16]ArrayPaths
}

// New resolves every schema the registry knows about. It fails closed: any
// schema that cannot be resolved aborts construction and names the schema, so
// a broken $ref is a deploy-time error rather than a silent loss of validation
// at runtime (#314).
//
// The resolve options are built once for the whole registry: they carry only
// the base URI and the sibling-restricted loader, both fixed by schemaDir.
func New(registry forma.SchemaRegistry, schemaDir string) (*Validator, error) {
	dir, err := filepath.Abs(schemaDir)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve schema directory %s: %w", schemaDir, err)
	}
	opts, err := resolveOptions(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to build resolve options for schema directory %s: %w", dir, err)
	}

	v := &Validator{
		resolved: make(map[int16]*jsonschema.Resolved),
		arrays:   make(map[int16]ArrayPaths),
	}
	for _, name := range registry.ListSchemas() {
		id, js, err := registry.GetSchemaByName(name)
		if err != nil {
			return nil, fmt.Errorf("failed to load schema %q for validation: %w", name, err)
		}

		var s jsonschema.Schema
		if err := json.Unmarshal([]byte(js.Schema), &s); err != nil {
			return nil, fmt.Errorf("failed to parse schema %q (id %d): %w", name, id, err)
		}
		if err := checkSchemaSupported(&s); err != nil {
			return nil, fmt.Errorf("failed to accept schema %q (id %d) for validation: %w", name, id, err)
		}
		resolved, err := s.Resolve(opts)
		if err != nil {
			return nil, fmt.Errorf(
				"failed to resolve schema %q (id %d) against schema directory %s: %w", name, id, dir, err)
		}
		v.resolved[id] = resolved
		if paths := deriveArrayPaths(&s); paths != nil {
			v.arrays[id] = paths
		}
	}
	return v, nil
}

// Validate checks doc against the schema registered for schemaID.
//
// A violation wraps forma.ErrInvalidInput: it is caller input and must surface
// as 4xx. A missing resolved schema does not — that is a server configuration
// fault and must stay operator-visible (docs/error-handling.md).
//
// doc is marshalled before validating. Native Go values do not carry their JSON
// types: time.Time presents as an object and fails a "type":"string" property
// until round-tripped, and two production call sites assign time.Now() to
// string-typed properties. The round-trip decodes with UseNumber and rewrites
// numbers via exactNumberInstance so constraint checks stay exact above 2^53.
//
// A nil receiver is treated as "no schema resolved" rather than panicking:
// callers may hold a *Validator that is nil when validation is unconfigured.
func (v *Validator) Validate(schemaID int16, doc any) error {
	if v == nil {
		return fmt.Errorf("no resolved JSON schema for schema id %d: validator is not configured", schemaID)
	}

	resolved, ok := v.resolved[schemaID]
	if !ok {
		return fmt.Errorf("no resolved JSON schema for schema id %d", schemaID)
	}

	raw, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("failed to marshal payload for schema %d: %w", schemaID, err)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var instance any
	if err := dec.Decode(&instance); err != nil {
		return fmt.Errorf("failed to decode payload for schema %d: %w", schemaID, err)
	}
	instance, err = exactNumberInstance(instance)
	if err != nil {
		return fmt.Errorf("failed to decode payload for schema %d: %w", schemaID, err)
	}

	if err := resolved.Validate(instance); err != nil {
		return fmt.Errorf("schema validation failed: %v: %w", err, forma.ErrInvalidInput)
	}
	return nil
}
