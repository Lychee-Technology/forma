# PR #316 review fix wave — report

Branch `feat/314-enforce-json-schema`, worktree
`/Users/ruoshi/code/Lychee/LTBase/forma/.claude/worktrees/314-schema-validation`.

Status: **complete**. All three findings fixed red-first, standards items done,
spec reconciled, `format` tripwire added.

Note on continuity: this wave was interrupted by an API error partway through.
`89ea832` (Finding 1) was already committed before the interruption; everything
below it in the table was done after resuming. The record here is complete
regardless.

## Commits

| SHA | Subject |
| --- | --- |
| `89ea832` | fix(transform): #314 merge array elements element-wise for the validator view |
| `25cf5a7` | fix(schemavalidate,factory): #314 preflight referenced schemas; name the offending path |
| `9bd5bcd` | fix(transform): #314 preserve typed nil containers in the validator view |
| `1944e2f` | test(schemavalidate,factory): #314 format inertness tripwire; extract the factory mock registry |
| `8b67988` | docs(spec): #314 reconcile the design doc with the shipped architecture |

## Finding 1 [High] — validator view dropped a value the writer persists

Red first, against shipped `lead_full`:

```
=== RUN   TestNormalizeMergesArrayElementsAcrossSpellings
    normalize_keys_arrays_test.go:272:
        Error:      Expected error with "invalid input" in chain but got nil.
        Messages:   a value the writer persists must not be invisible to the validator
--- FAIL: TestNormalizeMergesArrayElementsAcrossSpellings (0.00s)
=== RUN   TestWriterPersistsBothArraySpellings
--- PASS: TestWriterPersistsBothArraySpellings (0.00s)
```

The second test is the other half and passed from the start: the writer really
does emit both `requirement.areas.note` (id 81) and `requirement.areas.city`
(id 80) for that payload, which is what makes the first a defect rather than a
preference.

Fix: restored element-wise `mergeSlices` in `internal/transform/normalize_keys.go`
— recursing into elements that are both maps, extending to the longer length,
replacing non-map elements. `TestNormalizeArrayMergeKeepsLastSpellingAtSameIndex`
pins that last-spelling-wins still holds where both spellings name the same
attribute at the same index.

## Finding 2 [High] — configuration faults in referenced schemas bypassed fail-closed

Red first:

```
=== RUN   TestNewRejectsUnknownTypeInReferencedSchema
    validator_test.go:394: Error: An error is expected but got nil.
--- FAIL: TestNewRejectsUnknownTypeInReferencedSchema (0.00s)
=== RUN   TestNewRejectsUnknownTypeInNestedReference
    validator_test.go:410: Error: An error is expected but got nil.
--- FAIL: TestNewRejectsUnknownTypeInNestedReference (0.00s)
=== RUN   TestNewAcceptsCyclicAndRepeatedReferences
--- PASS: TestNewAcceptsCyclicAndRepeatedReferences (0.00s)
```

Fix: `fileLoader` now runs `checkSchemaSupported` on every document it loads, via
`checkLoadedSchema`, which memoises the verdict under the resolved path. That
bounds a diamond reference to one walk and makes a reference cycle terminate.

Two deliberate choices worth flagging:

- **The cache holds the verdict, not the parsed schema.** Caching the
  `*jsonschema.Schema` would hand the same pointer to two different `Resolve`
  calls; the library annotates resolved schemas, so sharing them across resolves
  is a risk with no upside here. Caching `map[string]error` gives the same
  once-per-path and cycle-termination properties with no sharing.
- **The check includes `checkSchemaVersion`,** as instructed, even though
  `checkSchemaVersion`'s own doc argues only the root's `$schema` affects
  validation. Verified safe before shipping: every one of the 12 shipped schema
  files declares `https://json-schema.org/draft/2020-12/schema`, so no referenced
  file can be refused at boot. `TestShippedSchemasPassConstructionGuards` is the
  standing guard.
- **No mutex on the cache.** Loaders run inside `Resolve`, which `New` drives
  sequentially, and a fresh loader is built per `resolveOptions` call. Stated in
  the comment so a future concurrent `New` does not inherit it silently.

## Finding 3 [Medium] — typed-nil containers materialised

Red first, both halves:

```
=== RUN   TestNormalizeTypedNilObjectStaysRejected
    normalize_keys_arrays_test.go:331:
        Error:      Expected error with "invalid input" in chain but got nil.
        Messages:   a typed nil must not pass an object-typed property as {}
--- FAIL: TestNormalizeTypedNilObjectStaysRejected (0.00s)
=== RUN   TestWriterStoresNothingForTypedNilObject
--- PASS: TestWriterStoresNothingForTypedNilObject (0.00s)
=== RUN   TestNormalizePreservesTypedNilContainers
    normalize_keys_test.go:336:
        Error:      Expected nil, but got: map[string]interface {}{}
        Messages:   a typed nil map must stay nil, not become {}
--- FAIL: TestNormalizePreservesTypedNilContainers (0.00s)
```

Fix: `normalizeValue` copies a typed nil `map[string]any` / `[]any` through
instead of recursing, so `Validate`'s round-trip presents `null` and the schema's
own `type` decides.

**One correction to the brief's test suggestion.** The reviewer proposed a
"typed-nil *required* object" case. Written against `lead_full`'s `contact` —
the obvious required object — that test **passes against the bug**: `contact`
declares its own `required: ["name"]`, so `{}` is rejected there whether or not
the typed nil is preserved. It proves nothing. The shipped test uses
`requirement` instead: `"type": "object"`, optional at root, nothing required
beneath it, so `{}` passes and `null` does not, and the assertion can only be
satisfied by the fix. The red output above is from that version. The reasoning
is recorded in the test's own doc comment so the fixture choice is not
"simplified" back later.

**One hazard the fix created, found and fixed in the same commit.** Preserving
typed nils makes a nil map a real inhabitant of the document for the first time,
and both the expansion walk (`setNestedValue`) and the merge (`mergeAny`) would
write into one:

```
panic: assignment to entry in nil map
  transform.mergeValue(...)      normalize_keys.go:263
  transform.setNestedValue(...)  normalize_keys.go:249
  transform.normalizeMap(...)    normalize_keys.go:122
```

Both now replace a nil map rather than writing into it — it holds nothing a
merge could preserve, and the dotted spelling is the later one either way.
Pinned by `TestNormalizeExpandsOverTypedNilContainer` (interior segment, the
walk) and `TestNormalizeMergesObjectOverTypedNilContainer` (leaf, the merge).

## Standards items

- **Bare `return err`** wrapped at `factory/factory.go:164`
  (`failed to open the federated read surface`),
  `internal/schemavalidate/validator.go` `fileLoader` (both returns), and
  `internal/schemavalidate/schema_check.go` `checkSchemaSupported` (both
  branches, now naming version-check vs type-keyword-walk).
- **Invalid `type` now names its path.** The walk carries position;
  `unknownTypeError` reports e.g. `/properties/a/properties/b`, `/$defs/d`,
  `/allOf/1`, or `the schema root`. Keyword names come from the `json` struct tag,
  falling back to the lowercased field name for tags the library marks `-`
  (`Items` is the one that matters — it is on every array). Pinned by seven
  cases in `TestNewRejectsUnknownTypeKeyword`, including two new ones for depth
  and for slice indices.
- **`factory/factory_test.go` 985 → 887.** `mockSchemaRegistry`, `mockSchemaBody`
  and the `body()` helper moved to `factory/mock_schema_registry_test.go` (104
  lines). Contained refactor only; the file was not split further.
- **Waived, as directed and confirmed against the diff:**
  `internal/entity_manager_test.go` (1196) and
  `internal/entity_manager_update_batch_test.go` (508). The diff against
  `f4017db` shows only `, nil` appended to existing `NewEntityManager` /
  service call sites — mechanical parameter propagation, not meaningful
  extension — so splitting them would bury this PR's diff in unrelated churn.
  Not touched.
- **One more file in the same category, not waived explicitly but treated the
  same:** `internal/e2e_harness/federated/benchmark/execute.go` holds the only
  two over-100-line functions among all changed Go files
  (`executeKeysetServiceQuery` 167, `executeServiceQueryWithPlan` 124). Both are
  pre-existing; this PR's entire change to that file is a single `, nil`
  (`git diff f4017db...HEAD` shows `1 insertion(+), 1 deletion(-)`). Not touched,
  for the same reason.

## Spec reconciliation

`docs/superpowers/specs/2026-07-25-issue-314-enforce-json-schema-on-write-design.md`,
commit `8b67988`. Original decisions kept visible and annotated rather than
rewritten, per this branch's convention:

- **Decision 4** gains a "Qualified during implementation" block naming both
  exemptions — a dotted attribute written *above* a schema array, and anything
  beneath a relation root — each with the test that pins it, and each with the
  reason closing the bypass there costs more than the bypass.
- **New subsection "Deviation: the normalized document is for the validator
  only"** under Dotted-key normalization: the writer receives the caller's
  original map, because `eav_dedupe.go` resolves precedence by spelling tags a
  merged document no longer carries. It states the correctness standard that
  replaces the old one — the view must contain every value the writer will
  persist — and derives both Finding 1's and Finding 3's fixes from it.
- **Write-path hook**: the `normalize → validate → transform` line is kept and
  marked superseded, with a diagram of the fork the sites actually implement and
  a note that the JSON round-trip moved inside `Validate`.
- **Decision 4's "subsumes #312" and the matching "Out of scope" bullet** are
  marked false: the dedupe is the authority on precedence, not a safety net.
- **Testing list** records the two guards widened during review, the format
  tripwire's shipped location, and why the `watch` test was not written.

## The one missing test

`internal/schemavalidate/format_inertness_test.go`:
`TestFormatKeywordStaysInert` validates a malformed `date-time`, `uuid` and
`email` against a temp schema and asserts they *pass*. `TestShippedSchemasUseFormat`
keeps it load-bearing by asserting shipped schemas actually declare formats — a
tripwire guarding nothing is worse than none.

**Waived, as directed:** the `watch`-specific test. The behaviour change is
documented in `docs/error-handling.md`, the bundled CSV importer supplies all
three required fields, and construction coverage for every shipped schema exists
via `TestShippedSchemasPassConstructionGuards`. A `watch` test would restate the
generic `required` case against one more fixture. Recorded in the spec.

## Verification

| Command | Result |
| --- | --- |
| `make lint` | pass (exit 0, no findings) |
| `make test` | pass, all packages |
| `go test -count=1 ./internal/... .` | pass, no failures |
| `go vet -tags=e2e ./internal/e2e_harness/...` | pass (exit 0) |
| `make test-e2e-production` | pass |
| `git diff --check f4017db...HEAD` | clean (exit 0) |

File and function caps checked by hand across every changed Go file. No Go file
this wave created or grew exceeds 500 lines; the three over the cap are the two
waived test files plus `factory/factory_test.go`, which this wave *reduced*.
No function this wave wrote exceeds 40 lines; the only two over 100 among all
changed files are pre-existing in `benchmark/execute.go` as noted above.
`config.go` untouched (495).

No e2e fixture failed.
