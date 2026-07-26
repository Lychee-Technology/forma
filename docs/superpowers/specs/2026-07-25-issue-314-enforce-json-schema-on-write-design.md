# Enforce JSON Schema on the write path

**Issue:** [#314](https://github.com/Lychee-Technology/forma/issues/314)
**Origin:** discovered while investigating #312 during PR #307
**Date:** 2026-07-25

## Problem

Forma's headline is "entities defined by JSON Schema, no DDL migrations". The write path never validates a request body against that schema.

`transformer.ValidateAgainstSchema` (`internal/transform/transformer.go:201`) exists and works, but has **zero non-test callers** and is not on any interface. What the write path actually validates, all via the *metadata cache* rather than the schema document:

- the attribute exists in the cache (`transformer.go:327-330`)
- `required_policy` (`validateRequiredAttributesFromInput`, `transformer.go:360`)
- type coercion (`populateTypedValue`)

Everything expressible only in JSON Schema is unenforced. `enum`, `pattern`, `format`, `minimum`, `maximum` appear in **every** shipped schema under `cmd/server/schemas/` and `cmd/sample/schemas/`. `cmd/server/schemas/lead.json` declares `status` as `enum: [open, won, lost, junk]`; `lead_attributes.json:312` makes it `attributeID 71, valueType text`. So `POST /api/v1/lead` with `"status": "banana"` is a known attribute, satisfies `required_always`, coerces to text, and persists. The enum is decorative.

## Measurements

Everything in this section was measured against the real schema files and the real library (`github.com/google/jsonschema-go v0.4.2`, already a dependency), not inferred.

### A blocking schema bug

`visit.json` and `log.json` **cannot be resolved at all**:

```
visit.json  RESOLVE FAIL: loading /lead.json: cannot resolve remote schemas: no loader passed to Schema.Resolve
log.json    RESOLVE FAIL: loading /lead.json: cannot resolve remote schemas: no loader passed to Schema.Resolve
```

`ValidateAgainstSchema` calls `schema.Resolve(&jsonschema.ResolveOptions{})` — nil `Loader`, empty `BaseURI` — while `visit.json:27,30` and `log.json:20,23` use cross-file relative `$ref`s.

Supplying a loader does **not** fix it:

```
visit.json  RESOLVE FAIL: JSON Pointer "/$defs/contact": no key "contact" in map
lead.json $defs actually present: [lead_id]
```

`visit.json:30` references `lead.json#/$defs/contact`, which does not exist. This is a live, currently-invisible bug that enforcement merely exposes. Every `visit` and `log` write would fail 100% of the time.

### Cost — caching is mandatory

`lead.json` (13.5 KB, largest shipped), Apple M1 Pro:

| Operation | ns/op |
| --- | --- |
| unmarshal + `Resolve` | 1,581,127 (~1.58 ms) |
| `Validate` on a pre-resolved schema | 5,505 (~5.5 µs) |

A ~250× ratio; resolution is ~99.6% of the cost. Uncached, a 500-record `BatchCreate` would spend ~0.7 s re-parsing schemas.

### `format` is annotation-only

Measured: violations of `uuid`, `email`, `date-time`, and `date` all **pass**. Controls confirm the validator is live — a `pattern` violation and an `enum` violation both fail. `Format` appears once in the library, as a struct field never read by `validate.go`; `ResolveOptions` has no assertion knob.

Consequence: every `format` in the shipped schemas is inert, and there is no way to turn it on. Good for blast radius — test ids like `"test-id-1"` survive — but it must be documented or someone will trust it. Note `contact.json`'s email is a real `pattern`, so that one *is* enforced.

### `additionalProperties` and the dotted-key bypass

No shipped schema sets `additionalProperties`. Measured against `lead.json`:

```
unknown top-level property                    -> PASS
DOTTED key alongside a present contact object -> PASS
dotted key w/ type-violating value (int)      -> PASS   <- value never checked
FULLY FLAT payload (no nested contact object) -> FAIL: required: missing properties: ["contact"]
```

So `{"contact.email": 99999}` passes schema validation **entirely** — its value is never examined — and then flows into the EAV layer, because `flattenToAttributes` joins the path and matches `contact.email` against the metadata cache.

**Enabling validation naively would therefore create a silent bypass** for optional attributes spelled with dots. The hole is bounded: for a *required* attribute the dotted spelling is already rejected today, because `isRequiredAttributeMissingInInput` (`transformer.go:403-433`) walks nested parents and reports the attribute missing.

### The `required` divergence is small

11 of 13 schema pairs are congruent — `required_always` in the metadata equals the schema's top-level `required`.

- **`lead` only looks divergent.** The schema requires the `contact` object; the metadata requires the leaf `contact.isAnonymous`. These are equivalent for nested payloads, because `required_always` enforces even when the parent is absent (`transformer.go:383`). `requirement.budget.currency` is `required_if_parent_present` and `lead.json` mirrors it with `"required": ["currency"]` on the `budget` subschema. `lead` would not newly reject anything.
- **`watch` is the one real gap.** `cmd/sample/schemas/watch.json` requires `[id, name, brand]`; `watch_attributes.json` has zero `required_policy` entries. Measured: a payload without them currently passes and would newly fail. The CSV importer guards all three (`cmd/sample/watch_mapper.go:214-216`), so the shipped importer is safe; any other caller of `watch` is not.

### Native-map type divergence

`ValidateAgainstSchema`'s `map[string]any` branch passes the caller's map straight to `Validate` with no JSON round-trip, while its `default:` branch marshals first. Create payloads are `map[string]any`, so they take the no-round-trip path. Measured:

```
activity payload NATIVE (time.Time for "at")  -> FAIL: has type "object", want "string"
activity payload JSON round-tripped           -> PASS
```

`internal/integration_suite_test.go:120-141` and `cmd/sample/mapper.go:120` both assign `time.Now()` to a `"type": "string"` property. Go `int`, `int64`, and `float64` are handled correctly either way; `time.Time` is the only type that diverges.

### Everything else is clean

No enum, pattern, minimum, maximum, or type constraint is violated by any generated payload found in `tests/e2e/scripts/gen-data.ts`, `internal/e2e_harness/production/`, or the benchmark fixtures. The federated harness and benchmark never reach a create path — they seed by raw SQL and Parquet.

`cmd/sample` emits explicit `null` at typed leaves for 23 of 49 sample rows (`mapper.go:152` calls `setNestedValue` even when the transformer returns `(nil, nil)`), but the write path **already** rejects those (`transformer.go:296`). Validation would produce a second, earlier error on rows that already fail. Not new breakage. *This one is read from source, not observed end to end — it needs a live Postgres to confirm.*

## Decisions taken

Settled with the issue owner. Do not re-litigate during implementation.

1. **Enforce on create; report-only on update.** Create rejects violations. Update validates the merged document and logs violations without rejecting, behind a config flag, until logs are clean. Rejecting on update immediately would make any row that already violates a constraint un-updatable — a caller touching one unrelated field would be rejected for a pre-existing violation elsewhere. That is the failure shape this cycle already rejected in #294.
2. **Schema `required` and metadata `required_policy` both apply, independently.** No reconciliation. Each keeps its own message; a payload must satisfy both.
3. **Fail closed at startup.** Every registered schema resolves at registry construction; a failure refuses the boot and names the schema and the unresolvable ref. A schema that cannot resolve must never silently stop validating.
4. **Normalize dotted keys to nested before validating.** Closes the bypass, keeps dotted keys working, and subsumes #312.

## Design

### Prerequisite: repair the schemas — its own commit

Add the missing `contact` definition to `lead.json`'s `$defs`, or repoint `visit.json:30` and `log.json:23` at whatever they were meant to reference. Until this lands nothing resolves, so it ships as a separate, reviewable commit rather than buried inside the feature — a reviewer should be able to see the schema bug fixed on its own.

### `internal/schemavalidate` (new package)

Owns the compiled-schema cache and nothing else.

- Built once at registry construction, with a file `Loader` and a `BaseURI` pointing at the schema directory, so cross-file `$ref`s resolve.
- Holds `schemaID -> *jsonschema.Resolved`.
- Exposes `Validate(schemaID int16, doc map[string]any) error`.
- Construction returns an error if any schema fails to resolve (decision 3).

It lives in its own package rather than inside `internal/transform` because its dependency is the schema *document* and the resolver, while `transform`'s is the attribute *cache* — different inputs, different lifecycles, and the cache must be built at startup while transform is per-request.

### Dotted-key normalization, in `internal/transform`

Expands any literal key that resolves in the metadata cache as a leaf attribute into its nested path, merging into the document. Last spelling wins, matching `encoding/json`'s duplicate-key semantics and the rule already established in #312.

```
in:   {"contact":{"email":"old"}, "contact.email":"x"}
out:  {"contact":{"email":"x"}}
```

Determinism comes from the same property #312 relies on: `flattenToAttributes` sorts map keys (`transformer.go:279-283`), and for any dotted name `X.Y` the nested top-level key `X` sorts before the literal `X.Y`, so the literal — the caller's explicit value on update — is last.

**Known edge:** a schema property literally named `a.b` at top level is ambiguous with the nested path `a -> b`. No shipped schema has one. Document it; do not build machinery for it.

### Write-path hook

Four call sites, already symmetric:

| Path | Site | Input |
| --- | --- | --- |
| create | `internal/entity_crud_service.go:68` | `inputData` |
| create | `internal/entity_batch_service.go:209` | `inputData` |
| update | `internal/entity_crud_service.go:180` | `mergedData` |
| update | `internal/entity_batch_service.go:284` | `mergedData` |

Order at each site: **normalize → JSON round-trip → validate → transform**.

The round-trip is required, not cosmetic — see the native-map measurement above.

Validation sits at the service layer rather than inside `ToPersistentRecord` because that is where create and update are distinguishable, and the two modes differ. `transform` stays free of mode knowledge.

### Update mode flag

A single value on `forma.Config` — alongside the existing federated/DuckDB settings — selects report-only or enforcing for the **update** path, threaded to the services the same way other config reaches them. Create is always enforcing. Report-only logs at `Warn` with the schema name, row id, and the violation — enough for an operator to find offending rows without dumping the payload.

Default: report-only, so upgrading does not start rejecting updates against pre-existing data.

The validator itself reaches the services as a constructor dependency, mirroring how the transformer and registry already do; it is built once during `factory` wiring, not per request.

### Error semantics

Validation failures on create are caller input and wrap `forma.ErrInvalidInput`, surfacing as 400 (`AGENTS.md`, `docs/error-handling.md`). After #307 an unwrapped validator would answer 500 with a redacted body.

The library's message already names the property, the constraint, and the offending value — `enum: lease does not equal any of: [buy rent sell landlord]`. Surface it as-is: the value is the caller's own input being echoed back, which is exactly what a 400 is for, and it is what makes the error actionable. This is consistent with #307's boundary, where a *provable* client error keeps its verbatim message.

Startup resolution failures are operator-facing plain errors, not `ErrInvalidInput`.

## Testing

- **The enum case from the issue:** `POST` with `"status": "banana"` against `lead` is rejected 400, naming `status` and `enum`. Red first.
- **`watch` gains rejections:** a payload without `[id, name, brand]` now fails; the CSV importer's own payloads still pass.
- **Dotted normalization:** `{"contact":{"email":"old"}, "contact.email":"x"}` validates as `x`, and `{"contact.email": 99999}` is now **rejected** on type — the bypass measured above is closed.
- **Merged-document `required`:** a partial update omitting a required property succeeds, because the merged document carries it from storage. This is the explicit test for the concern that whole-document `required` would false-positive on every update.
- **Update report-only:** a merged document violating a constraint is accepted and logged; flipping the flag rejects it.
- **Startup fail-closed:** a deliberately dangling `$ref` in a test fixture fails registry construction with the schema name and ref in the message.
- **`format` stays inert:** a malformed `date-time` is accepted, pinning the documented behaviour so a library upgrade that starts asserting formats is caught.
- **Cost:** a benchmark asserting the resolved schema is cached — a second validate must not re-resolve.
- **#312 regression suite stays green**, proving normalization did not reintroduce duplicate primary keys.

## Documentation

`docs/error-handling.md` and the schema-authoring docs must state:

- which constraints are enforced (`enum`, `pattern`, `type`, `minimum`/`maximum`, `required`) and which are **not** (`format`, and unknown properties since `additionalProperties` is unset)
- that create rejects and update reports, with the flag that changes it
- the `watch` behaviour change
- the dotted-key normalization rule and its ambiguous edge

## Out of scope

- Setting `additionalProperties: false`. It would reject any unknown key — a far wider behaviour change than this issue, and it belongs to a schema-authoring decision, not a validation-wiring one.
- Making `format` assertive. The library cannot, and swapping libraries is its own project.
- Unifying schema `required` with metadata `required_policy` (decision 2 keeps both).
- Removing #312's dedupe. Normalization makes duplicate spellings unreachable, but the dedupe stays as a safety net with its comment updated to say so — it has already caught one real bug and costs nothing.
- Retrofitting validation to `cmd/sample`'s nil-emitting mapper (`mapper.go:152`). Pre-existing, already rejected by the write path, tracked separately if desired.

## Verification

```bash
make lint
make test
go test -count=1 ./internal/... .
make test-e2e-production
go test -v ./internal/e2e_harness/federated/... -tags=e2e -timeout=30m
```

The e2e suites matter here because this changes what the write path accepts, and `tests/e2e` payloads flow through the real HTTP create path.
