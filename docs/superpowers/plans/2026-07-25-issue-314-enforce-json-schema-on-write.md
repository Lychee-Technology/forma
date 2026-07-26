# #314 Enforce JSON Schema On The Write Path — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the JSON Schema constraints that every shipped schema declares — `enum`, `pattern`, `type`, `minimum`/`maximum`, `required` — actually enforced when entities are written, instead of decorative.

**Architecture:** A new `internal/schemavalidate` package resolves every registered schema once at startup (failing the boot if any cannot resolve) and caches the resolved form, because resolving costs ~250× a validate. `internal/transform` gains dotted-key normalization so a literal `"contact.email"` cannot bypass validation. The four write-path service call sites then run normalize → validate → transform, rejecting on create and logging only on update.

**Tech Stack:** Go, `github.com/google/jsonschema-go v0.4.2` (already a dependency), `go.uber.org/zap`, `testify/require` outside `internal/httpapi`.

**Spec:** `docs/superpowers/specs/2026-07-25-issue-314-enforce-json-schema-on-write-design.md`

## Global Constraints

- Source files ≤500 lines, functions ≤100 lines (`coding-standard.md`).
- Always wrap errors with context: `fmt.Errorf("failed to X: %w", err)` — never bare `return err`.
- Match errors with `errors.Is` / `errors.As`. **Never** compare error strings.
- Write-path validation wraps `forma.ErrInvalidInput` (→ 4xx); read-path consistency errors stay plain (→ operator-visible 5xx). After #307, an unwrapped validator answers 500 with a redacted body.
- `make lint` uses golangci-lint pinned to **v1.64.8** — do not upgrade the pin.
- Test style per package: `internal/httpapi` uses stdlib `testing` with `t.Fatalf`; `internal/`, `internal/transform`, `internal/schemameta` and new `internal/*` packages use `testify/require`.
- Run single tests with the Makefile's env: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./pkg -run TestName -v`.
- Derive any file survey from `git diff --name-only` **plus** `git ls-files --others --exclude-standard`. Hand-assembled lists missed files repeatedly on the previous PR.
- Mutation-check a fix by running the **full package**, never a `-run` filter — a filter can select only the unit tests that call a function directly and is structurally blind to wiring changes.
- Conventional commits. End every commit message body with:
  `Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>`

## Measured facts the plan depends on

Do not re-derive these; they were measured against the real schemas and the real library.

| Fact | Consequence |
| --- | --- |
| `visit.json` / `log.json` cannot resolve — `lead.json#/$defs/contact` does not exist | Task 1 must repair before anything works |
| Resolve 1.58 ms vs Validate 5.5 µs (~250×) | Cache is mandatory, not an optimization |
| `format` is annotation-only, no assertion switch | `date-time`/`uuid`/`email` stay inert; document it |
| No schema sets `additionalProperties` | Unknown keys pass; only dotted keys are addressed |
| `time.Time` in a native map fails `"type":"string"` | Validate must JSON round-trip first |
| `watch.json` requires `[id,name,brand]`, its metadata requires nothing | `watch` gains real rejections |

## File Structure

| File | Change | Responsibility |
| --- | --- | --- |
| `internal/schemavalidate/validator.go` | Create | Resolved-schema cache, file `Loader`, `Validate` |
| `internal/schemavalidate/validator_test.go` | Create | Fail-closed construction, round-trip, caching |
| `cmd/server/schemas/lead.json` | Modify | Hoist `contact` into `$defs`; `properties.contact` `$ref`s it |
| `internal/transform/normalize_keys.go` | Create | Dotted-key → nested normalization |
| `internal/transform/normalize_keys_test.go` | Create | Normalization semantics + determinism |
| `config.go` | Modify | `Entity.ValidateUpdatesStrict` flag |
| `factory/factory.go` | Modify | Build the validator, hand it to the services |
| `internal/entity_crud_service.go` | Modify | Create `:68`, update `:180` hooks |
| `internal/entity_batch_service.go` | Modify | Create `:209`, update `:284` hooks |
| `internal/entity_write_validation_test.go` | Create | End-to-end create-rejects / update-logs |
| `docs/error-handling.md` | Modify | What is and is not enforced |

---

### Task 1: Resolved-schema cache, and the schema repair it exposes

**Files:**
- Create: `internal/schemavalidate/validator.go`, `internal/schemavalidate/validator_test.go`
- Modify: `cmd/server/schemas/lead.json`

**Interfaces:**
- Consumes: `forma.SchemaRegistry` (`ListSchemas() []string`, `GetSchemaByName(string) (int16, forma.JSONSchema, error)`); `forma.JSONSchema.Schema` is the schema JSON as a `string`.
- Produces: `schemavalidate.New(registry forma.SchemaRegistry, schemaDir string) (*Validator, error)` and `(*Validator).Validate(schemaID int16, doc any) error`. Task 3 constructs it; Tasks 4 and 5 call `Validate`.

- [ ] **Step 1: Write the failing test**

Create `internal/schemavalidate/validator_test.go`:

```go
package schemavalidate

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// shippedSchemaDir is the real server schema directory. Resolving it is the
// point: two schemas there use cross-file $refs, and a nil Loader (the state
// before #314) cannot resolve them at all.
func shippedSchemaDir(t *testing.T) string {
	t.Helper()
	dir, err := filepath.Abs("../../cmd/server/schemas")
	require.NoError(t, err)
	return dir
}

// TestResolveShippedSchemas is the fail-closed contract: every schema the
// server ships must resolve, including the cross-file $refs in visit.json and
// log.json. It fails before the lead.json repair with a dangling-$defs error.
func TestResolveShippedSchemas(t *testing.T) {
	dir := shippedSchemaDir(t)
	for _, name := range []string{"lead", "lead_full", "visit", "visit_full", "log", "log_full"} {
		t.Run(name, func(t *testing.T) {
			_, err := resolveSchemaFile(dir, name+".json")
			require.NoError(t, err, "shipped schema %s must resolve", name)
		})
	}
}

// TestValidateRejectsEnumViolation is the issue's own example: lead.json
// declares status as a four-value enum and "banana" is not one of them.
func TestValidateRejectsEnumViolation(t *testing.T) {
	dir := shippedSchemaDir(t)
	resolved, err := resolveSchemaFile(dir, "lead.json")
	require.NoError(t, err)

	err = resolved.Validate(map[string]any{"status": "banana"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "banana")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./internal/schemavalidate -v`

Expected: FAIL to compile — `undefined: resolveSchemaFile`.

- [ ] **Step 3: Write the loader and resolver**

Create `internal/schemavalidate/validator.go`:

```go
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
	"github.com/lychee-technology/forma"
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
```

- [ ] **Step 4: Run the test — it must still fail, on the schema bug**

Run: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./internal/schemavalidate -run TestResolveShippedSchemas -v`

Expected: `lead`, `lead_full`, `visit_full`, `log_full` PASS; **`visit` and `log` FAIL** with a message naming `/$defs/contact`. Record this output — it is the evidence that the schema bug is real and previously invisible.

- [ ] **Step 5: Commit the resolver**

```bash
git add internal/schemavalidate/
git commit -m "feat(schemavalidate): #314 resolve entity schemas with a file loader

Cross-file \$refs need a Loader and an absolute BaseURI; without them
visit.json and log.json cannot be resolved at all. Resolving is cached because
it costs ~250x a validate.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

- [ ] **Step 6: Repair the dangling `$ref` — its own commit**

`visit.json:30` references `lead.json#/$defs/contact`, but `lead.json` defines `contact` **inline** at `properties.contact` and its `$defs` holds only `lead_id`.

In `cmd/server/schemas/lead.json`: move the entire object currently at `properties.contact` into `$defs.contact`, and replace `properties.contact` with:

```json
"contact": { "$ref": "#/$defs/contact" }
```

Do not change any constraint while moving it — the object must be byte-identical apart from its location. Verify with `git diff` that the only textual change is the relocation plus the new `$ref`.

- [ ] **Step 7: Run the test to verify it passes**

Run: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./internal/schemavalidate -v`

Expected: all subtests PASS, including `visit` and `log`, plus `TestValidateRejectsEnumViolation`.

- [ ] **Step 8: Commit the repair separately**

```bash
git add cmd/server/schemas/lead.json
git commit -m "fix(schemas): #314 define contact in lead.json \$defs

visit.json referenced lead.json#/\$defs/contact, which never existed — contact
was defined inline under properties. Nothing resolved these schemas, so the
dangling reference was invisible. Hoisted to \$defs with properties.contact
now referencing it; no constraint changed.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: The validator type — fail-closed construction and round-tripping

**Files:**
- Modify: `internal/schemavalidate/validator.go`, `internal/schemavalidate/validator_test.go`

**Interfaces:**
- Consumes: `resolveSchemaFile` from Task 1.
- Produces: `New(registry forma.SchemaRegistry, schemaDir string) (*Validator, error)`; `(*Validator).Validate(schemaID int16, doc any) error`. Tasks 3-5 use exactly these.

- [ ] **Step 1: Write the failing tests**

Append to `internal/schemavalidate/validator_test.go` (extend the import block with `"encoding/json"`, `"os"`, `"testing"` is already there, `"time"`, and `"github.com/lychee-technology/forma"`):

```go
// stubRegistry is the smallest forma.SchemaRegistry that New needs.
type stubRegistry struct {
	names   []string
	byName  map[string]forma.JSONSchema
	idsByNm map[string]int16
}

func (r *stubRegistry) ListSchemas() []string { return r.names }
func (r *stubRegistry) GetSchemaByName(name string) (int16, forma.JSONSchema, error) {
	js, ok := r.byName[name]
	if !ok {
		return 0, forma.JSONSchema{}, fmt.Errorf("no schema %q", name)
	}
	return r.idsByNm[name], js, nil
}
func (r *stubRegistry) GetSchemaByID(int16) (string, forma.JSONSchema, error) {
	return "", forma.JSONSchema{}, fmt.Errorf("not used")
}
func (r *stubRegistry) GetSchemaAttributeCacheByName(string) (int16, forma.SchemaAttributeCache, error) {
	return 0, nil, fmt.Errorf("not used")
}
func (r *stubRegistry) GetSchemaAttributeCacheByID(int16) (string, forma.SchemaAttributeCache, error) {
	return "", nil, fmt.Errorf("not used")
}

func registryWith(t *testing.T, name string, schemaJSON string, id int16) *stubRegistry {
	t.Helper()
	return &stubRegistry{
		names:   []string{name},
		byName:  map[string]forma.JSONSchema{name: {ID: id, Name: name, Schema: schemaJSON}},
		idsByNm: map[string]int16{name: id},
	}
}

// TestNewFailsClosedOnUnresolvableSchema pins the #314 decision: a schema that
// cannot resolve must stop the process at construction, naming the schema. A
// schema that silently stops validating is the failure this issue exists to fix.
func TestNewFailsClosedOnUnresolvableSchema(t *testing.T) {
	dir := t.TempDir()
	broken := `{"type":"object","properties":{"x":{"$ref":"missing.json#/$defs/nope"}}}`
	_, err := New(registryWith(t, "broken", broken, 7), dir)
	require.Error(t, err)
	require.Contains(t, err.Error(), "broken")
}

// TestValidateRoundTripsNativeValues pins that Validate marshals before
// validating. time.Time in a native map has Go type "object" to the validator
// and fails a "type":"string" property until it is round-tripped; two real
// call sites pass time.Now() for string-typed properties.
func TestValidateRoundTripsNativeValues(t *testing.T) {
	dir := t.TempDir()
	schema := `{"type":"object","properties":{"at":{"type":"string"}}}`
	v, err := New(registryWith(t, "ev", schema, 3), dir)
	require.NoError(t, err)

	require.NoError(t, v.Validate(3, map[string]any{"at": time.Now()}))
}

// TestValidateWrapsErrInvalidInput pins the error class: a schema violation is
// caller input, so it must surface as 4xx rather than a redacted 500 (#307).
func TestValidateWrapsErrInvalidInput(t *testing.T) {
	dir := t.TempDir()
	schema := `{"type":"object","properties":{"status":{"enum":["open","won"]}}}`
	v, err := New(registryWith(t, "ev", schema, 3), dir)
	require.NoError(t, err)

	err = v.Validate(3, map[string]any{"status": "banana"})
	require.ErrorIs(t, err, forma.ErrInvalidInput)
	require.Contains(t, err.Error(), "banana")
}

// TestValidateUnknownSchemaIDIsNotClientError pins that a missing resolved
// schema is an operator problem, not the caller's: it must NOT wrap
// ErrInvalidInput, or a server misconfiguration would answer 400.
func TestValidateUnknownSchemaIDIsNotClientError(t *testing.T) {
	dir := t.TempDir()
	v, err := New(registryWith(t, "ev", `{"type":"object"}`, 3), dir)
	require.NoError(t, err)

	err = v.Validate(99, map[string]any{})
	require.Error(t, err)
	require.NotErrorIs(t, err, forma.ErrInvalidInput)
}

// TestValidateResolvesOnceIsCached pins that Validate does not re-resolve.
// Resolving is ~250x the cost of validating, so a regression here is a
// throughput cliff rather than a correctness bug.
func TestValidateResolvesOnceIsCached(t *testing.T) {
	dir := t.TempDir()
	v, err := New(registryWith(t, "ev", `{"type":"object"}`, 3), dir)
	require.NoError(t, err)

	before := v.resolved[3]
	require.NoError(t, v.Validate(3, map[string]any{}))
	require.Same(t, before, v.resolved[3], "Validate must reuse the resolved schema")
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./internal/schemavalidate -v`

Expected: FAIL to compile — `undefined: New`, `undefined: Validator`.

- [ ] **Step 3: Implement the validator**

Append to `internal/schemavalidate/validator.go`:

```go
// Validator holds one resolved schema per schema ID. It is built once at
// startup and is safe for concurrent use: the map is never written after New
// returns, and jsonschema.Resolved is read-only during Validate.
type Validator struct {
	resolved map[int16]*jsonschema.Resolved
}

// New resolves every schema the registry knows about. It fails closed: any
// schema that cannot be resolved aborts construction and names the schema, so
// a broken $ref is a deploy-time error rather than a silent loss of validation
// at runtime (#314).
func New(registry forma.SchemaRegistry, schemaDir string) (*Validator, error) {
	dir, err := filepath.Abs(schemaDir)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve schema directory %s: %w", schemaDir, err)
	}

	v := &Validator{resolved: make(map[int16]*jsonschema.Resolved)}
	for _, name := range registry.ListSchemas() {
		id, js, err := registry.GetSchemaByName(name)
		if err != nil {
			return nil, fmt.Errorf("failed to load schema %q for validation: %w", name, err)
		}

		var s jsonschema.Schema
		if err := json.Unmarshal([]byte(js.Schema), &s); err != nil {
			return nil, fmt.Errorf("failed to parse schema %q (id %d): %w", name, id, err)
		}
		resolved, err := s.Resolve(resolveOptions(dir))
		if err != nil {
			return nil, fmt.Errorf("failed to resolve schema %q (id %d): %w", name, id, err)
		}
		v.resolved[id] = resolved
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
// string-typed properties.
func (v *Validator) Validate(schemaID int16, doc any) error {
	resolved, ok := v.resolved[schemaID]
	if !ok {
		return fmt.Errorf("no resolved JSON schema for schema id %d", schemaID)
	}

	raw, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("failed to marshal payload for schema %d: %w", schemaID, err)
	}
	var instance any
	if err := json.Unmarshal(raw, &instance); err != nil {
		return fmt.Errorf("failed to decode payload for schema %d: %w", schemaID, err)
	}

	if err := resolved.Validate(instance); err != nil {
		return fmt.Errorf("schema validation failed: %v: %w", err, forma.ErrInvalidInput)
	}
	return nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./internal/schemavalidate -v`

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/schemavalidate/
git commit -m "feat(schemavalidate): #314 fail-closed validator with a resolved-schema cache

Construction resolves every registered schema and aborts on the first failure,
naming it. Violations wrap forma.ErrInvalidInput so they surface as 4xx; a
missing resolved schema stays a plain operator error. Payloads are JSON
round-tripped because native time.Time does not satisfy \"type\":\"string\".

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Dotted-key normalization

**Files:**
- Create: `internal/transform/normalize_keys.go`, `internal/transform/normalize_keys_test.go`

**Interfaces:**
- Consumes: `forma.SchemaAttributeCache` (a `map[string]forma.AttributeMetadata` keyed by dotted attribute name).
- Produces: `transform.NormalizeDottedKeys(data map[string]any, cache forma.SchemaAttributeCache) map[string]any`. Exported because Tasks 4-5 call it from `internal` (a different package).

- [ ] **Step 1: Write the failing tests**

Create `internal/transform/normalize_keys_test.go`:

```go
package transform

import (
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

func dottedCache() forma.SchemaAttributeCache {
	return forma.SchemaAttributeCache{
		"contact.email": {AttributeID: 8, ValueType: forma.ValueTypeText},
		"name":          {AttributeID: 1, ValueType: forma.ValueTypeText},
	}
}

// TestNormalizeExpandsDottedKey pins the core rule: a literal dotted key that
// names a leaf attribute becomes its nested path, so schema validation sees a
// well-formed document and actually checks the value. Left flat, the key is an
// unknown property and its value is never examined at all (#314).
func TestNormalizeExpandsDottedKey(t *testing.T) {
	out := NormalizeDottedKeys(map[string]any{"contact.email": "x"}, dottedCache())
	require.Equal(t, map[string]any{"contact": map[string]any{"email": "x"}}, out)
}

// TestNormalizeLiteralWinsOverNested pins last-spelling-wins, matching
// encoding/json duplicate-key semantics and #312. On the update path the
// literal key is the caller's explicit value while the nested one was rebuilt
// from storage, so the literal must win.
func TestNormalizeLiteralWinsOverNested(t *testing.T) {
	in := map[string]any{
		"contact":       map[string]any{"email": "old"},
		"contact.email": "x",
	}
	out := NormalizeDottedKeys(in, dottedCache())
	require.Equal(t, map[string]any{"contact": map[string]any{"email": "x"}}, out)
}

// TestNormalizePreservesSiblings pins that expanding one leaf does not discard
// other properties of the same parent object.
func TestNormalizePreservesSiblings(t *testing.T) {
	cache := dottedCache()
	cache["contact.phone"] = forma.AttributeMetadata{AttributeID: 9, ValueType: forma.ValueTypeText}

	in := map[string]any{
		"contact":       map[string]any{"phone": "555", "email": "old"},
		"contact.email": "x",
	}
	out := NormalizeDottedKeys(in, cache)
	require.Equal(t, map[string]any{
		"contact": map[string]any{"phone": "555", "email": "x"},
	}, out)
}

// TestNormalizeLeavesUnknownDottedKeyAlone pins that only keys the metadata
// cache knows are expanded. An unknown dotted key stays put so the existing
// "attribute is not defined for schema" error still fires with its own message.
func TestNormalizeLeavesUnknownDottedKeyAlone(t *testing.T) {
	out := NormalizeDottedKeys(map[string]any{"nope.missing": 1}, dottedCache())
	require.Equal(t, map[string]any{"nope.missing": 1}, out)
}

// TestNormalizeIsDeterministic pins that the result does not depend on Go map
// iteration order. Run repeatedly because a map-order dependency is flaky by
// nature and would otherwise pass most runs.
func TestNormalizeIsDeterministic(t *testing.T) {
	in := map[string]any{
		"contact":       map[string]any{"email": "old"},
		"contact.email": "x",
	}
	for i := 0; i < 200; i++ {
		out := NormalizeDottedKeys(in, dottedCache())
		require.Equal(t, map[string]any{"contact": map[string]any{"email": "x"}}, out)
	}
}

// TestNormalizeDoesNotMutateInput pins that the caller's map is untouched. The
// update path reuses the merged map, and mutating it would corrupt the caller.
func TestNormalizeDoesNotMutateInput(t *testing.T) {
	in := map[string]any{"contact.email": "x"}
	NormalizeDottedKeys(in, dottedCache())
	require.Equal(t, map[string]any{"contact.email": "x"}, in)
}

// TestNormalizeExpandsNestedDottedKey pins that a dotted key is expanded even
// when it appears below the root, e.g. {"contact": {"email": ...}} spelled as
// a dotted key inside another object.
func TestNormalizeExpandsNestedDottedKey(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"a.b.c": {AttributeID: 4, ValueType: forma.ValueTypeText},
	}
	out := NormalizeDottedKeys(map[string]any{"a": map[string]any{"b.c": "v"}}, cache)
	require.Equal(t, map[string]any{
		"a": map[string]any{"b": map[string]any{"c": "v"}},
	}, out)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./internal/transform -run TestNormalize -v`

Expected: FAIL to compile — `undefined: NormalizeDottedKeys`.

- [ ] **Step 3: Implement normalization**

Create `internal/transform/normalize_keys.go`:

```go
package transform

import (
	"sort"
	"strings"

	"github.com/lychee-technology/forma"
)

// NormalizeDottedKeys rewrites literal dotted keys into their nested paths.
//
// Attribute names in this codebase are dotted, so a caller reading the metadata
// may address contact.email either as a nested object or as a literal key. Left
// literal, JSON Schema treats it as an unknown property: with no
// additionalProperties declared it passes validation and its value is never
// examined, which would make validation trivially bypassable (#314). Expanding
// it first means the value is validated like any other.
//
// When both spellings are present the literal wins, matching encoding/json's
// duplicate-key semantics and the rule established in #312 — on the update path
// the literal is the caller's explicit value while the nested form was rebuilt
// from storage. Keys are walked in sorted order, and for any dotted name X.Y the
// nested key X sorts before the literal X.Y, so the literal is applied last.
//
// Only keys the metadata cache knows as leaf attributes are expanded; an unknown
// dotted key is left alone so the existing "attribute is not defined" error
// still reports it. A schema property literally named "a.b" would be ambiguous
// with the nested path a -> b; no shipped schema has one.
//
// The input is never mutated.
func NormalizeDottedKeys(data map[string]any, cache forma.SchemaAttributeCache) map[string]any {
	return normalizeInto(data, nil, cache)
}

func normalizeInto(src map[string]any, path []string, cache forma.SchemaAttributeCache) map[string]any {
	dst := make(map[string]any, len(src))

	keys := make([]string, 0, len(src))
	for key := range src {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		value := src[key]
		if nested, ok := value.(map[string]any); ok {
			value = normalizeInto(nested, append(path, key), cache)
		}

		parts := strings.Split(key, ".")
		if len(parts) > 1 && isLeafAttribute(append(path, parts...), cache) {
			setNestedValue(dst, parts, value)
			continue
		}
		mergeValue(dst, key, value)
	}
	return dst
}

// isLeafAttribute reports whether the joined path names an attribute the schema
// defines, which is what makes expansion safe rather than a guess.
func isLeafAttribute(path []string, cache forma.SchemaAttributeCache) bool {
	_, ok := cache[strings.Join(path, ".")]
	return ok
}

// setNestedValue walks parts, creating intermediate maps, and assigns the leaf.
// An existing non-map value along the path is replaced, because the dotted
// spelling is the later — and therefore winning — one.
func setNestedValue(dst map[string]any, parts []string, value any) {
	current := dst
	for _, part := range parts[:len(parts)-1] {
		next, ok := current[part].(map[string]any)
		if !ok {
			next = make(map[string]any)
			current[part] = next
		}
		current = next
	}
	current[parts[len(parts)-1]] = value
}

// mergeValue assigns key, merging into an existing nested map rather than
// replacing it, so a nested object already built by an earlier expansion keeps
// its siblings.
func mergeValue(dst map[string]any, key string, value any) {
	existing, haveMap := dst[key].(map[string]any)
	incoming, incomingMap := value.(map[string]any)
	if haveMap && incomingMap {
		for k, v := range incoming {
			existing[k] = v
		}
		return
	}
	dst[key] = value
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./internal/transform -run TestNormalize -v -count=5`

Expected: all PASS, including the determinism test.

- [ ] **Step 5: Confirm #312 still holds**

Run: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test -count=1 ./internal/transform ./internal`

Expected: PASS. Normalization is not yet wired into the write path, so nothing should change — this is the baseline before Task 4.

- [ ] **Step 6: Commit**

```bash
git add internal/transform/normalize_keys.go internal/transform/normalize_keys_test.go
git commit -m "feat(transform): #314 normalize literal dotted keys into nested paths

A literal dotted key is an unknown property to JSON Schema, so with no
additionalProperties declared it passes validation and its value is never
checked. Expanding it first closes that bypass. Last spelling wins, matching
encoding/json and #312.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: Config flag and factory wiring

**Files:**
- Modify: `config.go` (the `Entity` config struct), `internal/entity_manager.go:13-25` (struct) and `:30-63` (`NewEntityManager`), `internal/entity_crud_service.go:24-37` (`newEntityCRUDService`), `internal/entity_batch_service.go:29-45` (`newEntityBatchService`), `factory/factory.go:197`, plus the two harness call sites
- Create: `internal/entity_write_validation_test.go`

**Interfaces:**
- Consumes: `schemavalidate.New` from Task 2.
- Produces: `forma.Config.Entity.ValidateUpdatesStrict bool`; `internal.NewEntityManager` gains a trailing `validator *schemavalidate.Validator` parameter; `entityManager`, `entityCRUDService`, and `entityBatchService` each gain `validator *schemavalidate.Validator` and `validateUpdatesStrict bool`. Task 5 reads those fields. A **nil validator disables validation**, which is what keeps every existing test and both harness call sites working unchanged.

**Note the naming and the construction pattern before you start.** The type is `entityCRUDService` (capital CRUD), not `entityCRUDService`. Services are not built by standalone constructors — `newEntityCRUDService(em)` and `newEntityBatchService(em, crud)` copy fields off `*entityManager` (`entity_manager.go:56-59`). So the validator goes onto `entityManager` and is copied down, and it must be set **before** those constructors run.

`NewEntityManager` returns `forma.EntityManager` with **no error**, so fail-closed cannot surface there. `factory.NewEntityManagerWithConfig` does return an error (`factory/factory.go:197`), so that is where `schemavalidate.New` is called and where a bad schema aborts startup.

- [ ] **Step 1: Write the failing test**

Create `internal/entity_write_validation_test.go`:

```go
package internal

import (
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// TestNewEntityManagerAcceptsNilValidator pins that validation is opt-in at the
// wiring layer. Both e2e harnesses and every existing test construct a manager
// without a validator, and none of them may start failing.
func TestNewEntityManagerAcceptsNilValidator(t *testing.T) {
	require.NotPanics(t, func() {
		_ = NewEntityManager(nil, nil, nil, nil, forma.DefaultConfig(nil), nil)
	})
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./internal -run TestNewEntityManagerAcceptsNilValidator -v`

Expected: FAIL to compile — `too many arguments in call to NewEntityManager`.

- [ ] **Step 3: Add the config flag**

In `config.go`, add to the `Entity` config struct:

```go
	// ValidateUpdatesStrict makes update payloads that violate the entity's
	// JSON Schema fail with 4xx instead of only being logged (#314).
	//
	// Default false: rows written before schema enforcement may already violate
	// their schema, and rejecting on update would make them un-updatable — a
	// caller touching one unrelated field would be refused for a pre-existing
	// violation elsewhere. Creates are always enforced; they have no legacy data.
	ValidateUpdatesStrict bool `json:"validateUpdatesStrict" yaml:"validateUpdatesStrict"`
```

Match the surrounding fields' tag style exactly — read the struct first.

- [ ] **Step 4: Thread the validator through the manager**

In `internal/entity_manager.go`, add to the `entityManager` struct:

```go
	validator            *schemavalidate.Validator
	validateUpdatesStrict bool
```

Add a trailing parameter to `NewEntityManager` and populate both fields in the `&entityManager{...}` literal, **before** the four `newEntity*Service` calls:

```go
func NewEntityManager(
	transformer model.PersistentRecordTransformer,
	repository model.PersistentRecordRepository,
	federatedQueryEngine model.FederatedQueryEngine,
	registry forma.SchemaRegistry,
	config *forma.Config,
	validator *schemavalidate.Validator,
) forma.EntityManager {
```

```go
		validator:             validator,
		validateUpdatesStrict: config.Entity.ValidateUpdatesStrict,
```

Read `config.Entity.ValidateUpdatesStrict` after the existing nil-config fallback, so a nil config still works.

Then copy both fields down in `newEntityCRUDService` (`:28-36`) and `newEntityBatchService` (`:41-...`), adding the matching fields to each service struct.

- [ ] **Step 5: Build the validator in the factory — this is the fail-closed point**

In `factory/factory.go`, before the `internal.NewEntityManager` call at `:197`:

```go
	schemaValidator, err := schemavalidate.New(registry, effectiveConfig.Entity.SchemaDirectory)
	if err != nil {
		return nil, fmt.Errorf("failed to build schema validator: %w", err)
	}
```

then pass `schemaValidator` as the new final argument. Read the surrounding code for the exact variable names — `registry` and `effectiveConfig` are indicative, not guaranteed.

Update the two harness call sites to pass `nil`:
- `internal/e2e_harness/production/engine.go:126`
- `internal/e2e_harness/federated/benchmark/execute.go:334`

Passing `nil` keeps the harnesses behaving exactly as today. Task 6's e2e run exercises the real factory path, which does have a validator, so coverage is not lost.

- [ ] **Step 6: Run the test to verify it passes**

Run: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./internal ./factory -run 'TestNewEntityManagerAcceptsNilValidator|TestFactory' -v`

Expected: PASS.

- [ ] **Step 7: Run the full suite**

Run: `make test`

Expected: PASS. This step only adds plumbing; any failure means a call site was missed. `go build ./...` does **not** compile e2e-tagged files, so also run:

```bash
GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go vet -tags=e2e ./internal/e2e_harness/...
```

- [ ] **Step 8: Commit**

```bash
git add config.go factory/factory.go internal/entity_manager.go internal/entity_crud_service.go internal/entity_batch_service.go internal/e2e_harness/ internal/entity_write_validation_test.go
git commit -m "feat(config,factory): #314 build the schema validator and thread it to the write services

Construction fails closed if any registered schema cannot resolve.
Entity.ValidateUpdatesStrict selects report-only or enforcing for updates and
defaults to report-only; creates are always enforced.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: Enforce on create, report on update

**Files:**
- Modify: `internal/entity_crud_service.go:68` (create) and `:180` (update), `internal/entity_batch_service.go:209` (create) and `:284` (update)
- Modify: `internal/entity_write_validation_test.go`

**Interfaces:**
- Consumes: `transform.NormalizeDottedKeys` (Task 3), `(*schemavalidate.Validator).Validate` (Task 2), the service fields from Task 4.
- Produces: no new exported surface.

- [ ] **Step 1: Write the failing tests**

Append to `internal/entity_write_validation_test.go`. This package already has the harness these tests need: `createTestConfig()`, `newMockPersistentRecordRepository()`, and `transform.NewPersistentRecordTransformer(registry)` (see `internal/entity_manager_services_test.go:14-19`).

The shared `newStubSchemaRegistry()` returns schema `test`/id `100` but its `JSONSchema.Schema` is not shaped for these assertions, so define a local registry rather than mutating the shared stub:

```go
// validationRegistry is a SchemaRegistry whose attribute cache and JSON Schema
// agree, so the write path and the validator see the same entity. Schema name
// and id match the package's shared stub so the rest of the harness fits.
type validationRegistry struct{}

const validationSchemaJSON = `{
  "type": "object",
  "properties": {
    "name":   {"type": "string", "enum": ["open", "won"]},
    "person": {"type": "object", "properties": {"name": {"type": "string"}}}
  },
  "required": ["name"]
}`

func (validationRegistry) GetSchemaAttributeCacheByName(name string) (int16, forma.SchemaAttributeCache, error) {
	if name != "test" {
		return 0, nil, fmt.Errorf("schema %s not found", name)
	}
	return 100, forma.SchemaAttributeCache{
		"name":        {AttributeID: 1, ValueType: forma.ValueTypeText},
		"person.name": {AttributeID: 3, ValueType: forma.ValueTypeText},
	}, nil
}
func (v validationRegistry) GetSchemaAttributeCacheByID(int16) (string, forma.SchemaAttributeCache, error) {
	_, cache, err := v.GetSchemaAttributeCacheByName("test")
	return "test", cache, err
}
func (validationRegistry) ListSchemas() []string { return []string{"test"} }
func (validationRegistry) GetSchemaByName(string) (int16, forma.JSONSchema, error) {
	return 100, forma.JSONSchema{ID: 100, Name: "test", Schema: validationSchemaJSON}, nil
}
func (validationRegistry) GetSchemaByID(int16) (string, forma.JSONSchema, error) {
	return "test", forma.JSONSchema{ID: 100, Name: "test", Schema: validationSchemaJSON}, nil
}

func newValidatingManager(t *testing.T, strict bool) (forma.EntityManager, *mockPersistentRecordRepository) {
	t.Helper()
	registry := validationRegistry{}
	validator, err := schemavalidate.New(registry, t.TempDir())
	require.NoError(t, err)

	config := createTestConfig()
	config.Entity.ValidateUpdatesStrict = strict

	repo := newMockPersistentRecordRepository()
	manager := NewEntityManager(transform.NewPersistentRecordTransformer(registry), repo, nil, registry, config, validator)
	return manager, repo
}

func createOp(data map[string]any) *forma.EntityOperation {
	return &forma.EntityOperation{
		Type:             forma.OperationCreate,
		EntityIdentifier: forma.EntityIdentifier{SchemaName: "test"},
		Data:             data,
	}
}

// TestCreateRejectsEnumViolation is issue #314's own example end to end: a
// declared enum must actually reject a value outside it. Before this change the
// value was a known attribute, satisfied required, coerced to text, and
// persisted — the enum was decorative.
func TestCreateRejectsEnumViolation(t *testing.T) {
	manager, _ := newValidatingManager(t, false)

	_, err := manager.Create(context.Background(), createOp(map[string]any{"name": "banana"}))

	require.ErrorIs(t, err, forma.ErrInvalidInput)
	require.Contains(t, err.Error(), "banana")
}

// TestCreateAcceptsValidPayload pins that enforcement does not reject good data.
func TestCreateAcceptsValidPayload(t *testing.T) {
	manager, _ := newValidatingManager(t, false)

	_, err := manager.Create(context.Background(), createOp(map[string]any{"name": "open"}))

	require.NoError(t, err)
}

// TestCreateRejectsDottedKeyTypeViolation pins the closed bypass. A literal
// dotted key is an unknown property to JSON Schema, so before normalization
// {"person.name": 99999} passed validation with its value never examined.
func TestCreateRejectsDottedKeyTypeViolation(t *testing.T) {
	manager, _ := newValidatingManager(t, false)

	_, err := manager.Create(context.Background(), createOp(map[string]any{
		"name":        "open",
		"person.name": 99999,
	}))

	require.ErrorIs(t, err, forma.ErrInvalidInput)
}

// TestUpdateReportOnlyAcceptsViolation pins the staged rollout: with
// ValidateUpdatesStrict false, a merged document that violates the schema is
// accepted and only logged, because rows written before #314 may already
// violate it and must stay updatable.
func TestUpdateReportOnlyAcceptsViolation(t *testing.T) {
	manager, _ := newValidatingManager(t, false)
	created, err := manager.Create(context.Background(), createOp(map[string]any{"name": "open"}))
	require.NoError(t, err)

	_, err = manager.Update(context.Background(), &forma.EntityOperation{
		Type:             forma.OperationUpdate,
		EntityIdentifier: forma.EntityIdentifier{SchemaName: "test", RowID: created.RowID},
		Updates:          map[string]any{"name": "banana"},
	})

	require.NoError(t, err)
}

// TestUpdateStrictRejectsViolation pins the other half of the flag.
func TestUpdateStrictRejectsViolation(t *testing.T) {
	manager, _ := newValidatingManager(t, true)
	created, err := manager.Create(context.Background(), createOp(map[string]any{"name": "open"}))
	require.NoError(t, err)

	_, err = manager.Update(context.Background(), &forma.EntityOperation{
		Type:             forma.OperationUpdate,
		EntityIdentifier: forma.EntityIdentifier{SchemaName: "test", RowID: created.RowID},
		Updates:          map[string]any{"name": "banana"},
	})

	require.ErrorIs(t, err, forma.ErrInvalidInput)
}

// TestUpdateOfUnrelatedFieldKeepsRequiredSatisfied is the load-bearing update
// test. The schema requires "name", and a partial update that does not mention
// it must still succeed — which holds only because the *merged* document is
// validated, not the request fragment. Validating the fragment would reject
// essentially every update.
func TestUpdateOfUnrelatedFieldKeepsRequiredSatisfied(t *testing.T) {
	manager, _ := newValidatingManager(t, true)
	created, err := manager.Create(context.Background(), createOp(map[string]any{"name": "open"}))
	require.NoError(t, err)

	_, err = manager.Update(context.Background(), &forma.EntityOperation{
		Type:             forma.OperationUpdate,
		EntityIdentifier: forma.EntityIdentifier{SchemaName: "test", RowID: created.RowID},
		Updates:          map[string]any{"person": map[string]any{"name": "ada"}},
	})

	require.NoError(t, err)
}
```

If `mockPersistentRecordRepository` does not round-trip a created record well enough for the update tests to find it, read how the existing update tests in this package seed the repository and mirror that — do not weaken the assertions to fit the mock.

- [ ] **Step 2: Run tests to verify they fail**

Run: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./internal -run 'TestCreateRejects|TestUpdate|TestNilValidator' -v`

Expected: the create and strict-update tests FAIL (violations are currently accepted). Record the output — it is the evidence the constraints were decorative.

- [ ] **Step 3: Add the create hook at both sites**

At `internal/entity_crud_service.go:68` and `internal/entity_batch_service.go:209`, before `ToPersistentRecord`. Both sites already resolve the schema; capture the attribute cache instead of discarding it:

```go
	inputData, err = s.validateWrite(ctx, schemaID, schemaCache, inputData, true)
	if err != nil {
		return nil, err
	}
```

Add one shared helper to `internal/entity_crud_service.go` so both services use identical semantics:

```go
// validateWrite normalizes dotted keys and validates the payload against the
// entity's JSON Schema, returning the normalized payload for transformation.
//
// enforce is true on create and follows ValidateUpdatesStrict on update. When
// enforcement is off a violation is logged and the write proceeds: rows written
// before #314 may already violate their schema, and rejecting on update would
// make them un-updatable over an unrelated field.
//
// A nil validator disables both steps, so services constructed without one
// behave exactly as before.
func (s *entityCRUDService) validateWrite(
	ctx context.Context,
	schemaID int16,
	cache forma.SchemaAttributeCache,
	data any,
	enforce bool,
) (any, error) {
	if s.validator == nil {
		return data, nil
	}
	doc, ok := data.(map[string]any)
	if !ok {
		return data, nil
	}

	normalized := transform.NormalizeDottedKeys(doc, cache)
	if err := s.validator.Validate(schemaID, normalized); err != nil {
		if enforce {
			return nil, fmt.Errorf("schema validation failed for schema %d: %w", schemaID, err)
		}
		zap.S().Warnw("write violates entity JSON schema; accepted because strict update validation is off",
			"schemaID", schemaID, "error", err.Error())
	}
	return normalized, nil
}
```

Give `entityBatchService` the same method verbatim, or extract it to a shared file if both services can reach it — decide based on how the two services already share code, and say which you chose.

- [ ] **Step 4: Add the update hook at both sites**

At `internal/entity_crud_service.go:180` and `internal/entity_batch_service.go:284`, after `mergeMaps` and after `StripComputedFields`, passing `s.validateUpdatesStrict` as `enforce`. Feed the returned normalized document to `ToPersistentRecord` — validating the merged document is what makes partial updates legal.

- [ ] **Step 5: Run tests to verify they pass**

Run: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test -count=1 ./internal/... .`

Expected: PASS, including the whole pre-existing suite.

- [ ] **Step 6: Mutation-check the hooks**

Temporarily make `validateWrite` return `(data, nil)` immediately. Run the **full package** (`go test -count=1 ./internal`), confirm the create and strict-update tests FAIL, then restore and confirm green. Do not use a `-run` filter — a filter that selects only direct-call unit tests is blind to wiring.

- [ ] **Step 7: Commit**

```bash
git add internal/entity_crud_service.go internal/entity_batch_service.go internal/entity_write_validation_test.go
git commit -m "feat(entity): #314 validate write payloads against the entity JSON schema

Creates reject violations as 4xx; updates validate the merged document and log
only, until Entity.ValidateUpdatesStrict is set. Dotted keys are normalized
first so their values cannot bypass validation.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 6: Documentation and full verification

**Files:**
- Modify: `docs/error-handling.md`

**Interfaces:** none.

- [ ] **Step 1: Document what is and is not enforced**

Add a section to `docs/error-handling.md` stating:

- **Enforced on create:** `enum`, `pattern`, `type`, `minimum`/`maximum`, and the schema's `required` — in addition to the metadata's `required_policy`, which is a separate mechanism that also still applies.
- **Not enforced, deliberately:** `format` is annotation-only in `github.com/google/jsonschema-go` and there is no option to assert it, so `date-time`, `uuid`, and `email` formats are inert. Note that `contact.json`'s email is a real `pattern` and *is* enforced. Anyone authoring a schema must know this or they will trust a constraint that does nothing.
- **Unknown properties are accepted**, because no shipped schema sets `additionalProperties`. Dotted keys are normalized into nested paths first, so their values *are* validated; other unknown keys are not.
- **Updates report, creates reject**, with `Entity.ValidateUpdatesStrict` flipping updates to enforcing, and why the default is report-only.
- **Behaviour change:** `watch` payloads without `id`, `name`, or `brand` are now rejected — `watch.json` requires them while `watch_attributes.json` requires nothing. The bundled CSV importer already supplies all three.
- **Startup fails closed** if any registered schema cannot resolve.

Reconcile by grepping the **concept** across the whole file, not just adding a section — previous rounds on `docs/error-handling.md` shipped false claims by editing only the passage that was pointed at.

- [ ] **Step 2: Run the full verification**

```bash
make lint
make test
GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test -count=1 ./internal/... .
make test-e2e-production
go test -v ./internal/e2e_harness/federated/... -tags=e2e -timeout=30m
```

Expected: all PASS. The e2e suites are required here, not optional: this changes what the write path accepts, and `tests/e2e` payloads flow through the real HTTP create path. If a payload starts failing, that is a finding to report, not a test to loosen.

- [ ] **Step 3: Check file sizes and formatting**

```bash
git diff --name-only f4017db...HEAD > /tmp/changed.txt
git ls-files --others --exclude-standard >> /tmp/changed.txt
xargs wc -l < /tmp/changed.txt | awk '$1>500'
gofmt -l $(grep '\.go$' /tmp/changed.txt)
git diff --check f4017db...HEAD
```

Expected: no file over 500 lines, no gofmt output, no whitespace errors.

- [ ] **Step 4: Commit**

```bash
git add docs/error-handling.md
git commit -m "docs(errors): #314 document which schema constraints are enforced

format is annotation-only in jsonschema-go with no assertion switch, and
unknown properties are accepted because no schema sets additionalProperties.
Both must be stated or a schema author will trust a constraint that does
nothing.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

## Self-Review

**Spec coverage.** Prerequisite schema repair → Task 1 Steps 6-8. `internal/schemavalidate` with loader, cache, fail-closed → Tasks 1-2. Dotted normalization → Task 3. Config flag and wiring → Task 4. Four write-path hooks, create-rejects / update-logs → Task 5. Error semantics (`ErrInvalidInput` on violation, plain on missing resolved schema) → Task 2. Documentation → Task 6.

Spec tests all present: enum rejection (T2, T5), dotted normalization and the closed bypass (T3, T5), merged-document `required` (T5), update report-only and strict (T5), startup fail-closed (T2), caching (T2), `#312` regression stays green (T3 Step 5, T5 Step 5).

**Two gaps, both deliberate and stated rather than hidden.**

1. The spec asks for a tripwire test that `format` stays inert, so a library upgrade that starts asserting formats is caught. Add it to Task 2 Step 1 if wanted: validate a malformed `date-time` against a temp schema and assert it passes. Omitted from the task body because it pins the *absence* of a feature, and a reviewer should decide whether that earns a test.
2. `watch` gaining real rejections (`watch.json` requires `[id,name,brand]`, its metadata requires nothing) is documented in Task 6 but has no dedicated test. The generic required-enforcement path is covered by Task 5; a `watch`-specific test would need the `cmd/sample` registry wired into a Go test, which is disproportionate. The bundled CSV importer already supplies all three fields, so nothing shipped breaks.

**Type consistency.** `New(registry, schemaDir) (*Validator, error)` and `Validate(schemaID int16, doc any) error` (Task 2) are used unchanged in Tasks 4-5. `NormalizeDottedKeys(map[string]any, forma.SchemaAttributeCache) map[string]any` (Task 3) is called in Task 5. `validateWrite(ctx, schemaID, cache, data, enforce) (any, error)` is defined and used only inside Task 5. The service type is `entityCRUDService` throughout, and services are populated by copying fields off `*entityManager` — no standalone constructors.

**Verified against the real code before writing:** `NewEntityManager` returns no error (so the factory owns fail-closed), `entityManager` already carries `config`, `newEntityCRUDService(em)` / `newEntityBatchService(em, crud)` copy fields, and the test harness in `internal/` is `createTestConfig()` + `newMockPersistentRecordRepository()` + `transform.NewPersistentRecordTransformer(registry)`.
