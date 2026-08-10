# Error Handling

## Overview

Forma uses two error classes in the schema/metadata pipeline.

## Write-path validation errors

Write operations build a `forma.InvalidInputf` carrier when the caller provides
data that cannot be accepted. The carrier does two things at once: it wraps
`forma.ErrInvalidInput` (so `errors.Is` classification holds everywhere), and
it records the message as *deliberately published* (`forma.PublicError`), which
is what the HTTP boundary emits on the 4xx body (#313).

Examples:

- unknown write attribute names in `transformer.flattenToAttributes`
- invalid value conversion in `populateTypedValue`
- explicit `null` writes to schema-defined fields
- a payload that violates the entity's JSON Schema (`internal/schemavalidate`) —
  see "JSON Schema enforcement on write" below

These errors are intended to surface as user-facing `4xx` responses. A bare
`fmt.Errorf("…: %w", forma.ErrInvalidInput)` still earns the 400 status but
answers a **redacted** body — the deny-by-default shape (#313) — and is
rejected at build time by `TestNoBareSentinelWraps` (root package).

## JSON Schema enforcement on write

Until #314 the JSON Schema documents in `SCHEMA_DIR` drove metadata generation
and read-path shaping only. `enum`, `pattern`, `type` and `minimum`/`maximum`
were declared across the shipped schemas and enforced **nowhere**.
`internal/schemavalidate` now resolves each entity schema once at startup, and
all four write paths (`Create`, `Update`, `BatchCreate`, `BatchUpdate`) validate
against it through `internal/entity_write_validation.go`.

### What is enforced

The validator is `github.com/google/jsonschema-go`, so every assertion keyword
that library implements applies. The ones the shipped schemas actually use:

| keyword | shipped example |
| --- | --- |
| `type` | `lead.json` `contact.annualIncome` must be a number |
| `enum` | `lead.json` `pipeline` ∈ `buy`/`rent`/`sell`/`landlord` |
| `pattern` | `contact.json` `email`; `lead.json` `contact.primaryPhone` |
| `minimum`/`maximum` | `lead.json` `contact.employmentYear` 1900–2100 |
| `required` | the schema's own `required` list, at the root and at any nested object (`lead.json` `contact.isAnonymous`) |

Cross-file `$ref` is resolved, restricted to plain siblings inside `SCHEMA_DIR`.
That is what makes `visit.json`'s `leadId` (`lead.json#/$defs/lead_id`) carry
lead's pattern at all: before #314 the reference could not be loaded, so
`visit.json` and `log.json` were never validated against anything.

It does **not** mean `visit.json`'s `contactSnapshot`
(`lead.json#/properties/contact`) is checked against lead's contact object.
`contactSnapshot` carries `x-relation`, and relation-backed properties are not
validated in any spelling — see "Relation subtrees are never caller-writable"
below.

**This is in addition to `required_policy`, which is a separate mechanism and
still applies independently.** `required_policy` lives in
`<name>_attributes.json` (`forma.AttributeMetadata.RequiredPolicy`) and is
checked by `validateRequiredAttributesFromInput` in `internal/transform`. It is
per *attribute* and understands `required_if_parent_present`; the schema's
`required` is per *object* and understands nothing else. Neither subsumes the
other, and a write must satisfy both.

### What is not enforced, and why

**`format` is inert — every one of them.** `format` is annotation-only in
`github.com/google/jsonschema-go`: the keyword is parsed into `Schema.Format`
and never read by the validator, and `Resolved.Validate(instance any) error`
takes no options, so there is no switch to assert it. Every `date-time`,
`uuid`, `date` and `email` format across the shipped schemas therefore
constrains nothing.

The trap this sets is `email`. `lead.json` declares `contact.email` with
`"format": "email"` — inert. `contact.json` declares its `email` with a
`"pattern"` — enforced. Same logical field, two schemas, one real constraint. An
author who wants a format asserted must write it as a `pattern`.

**Unknown properties are accepted**, because no shipped schema sets
`additionalProperties`. A key the schema does not define passes validation. That
is not a hole for unknown *attributes* — `flattenToAttributes` still rejects
those with `attribute is not defined`, so they answer `400` by a different route.
What passes unchecked is a key the attribute metadata defines and the JSON
Schema does not describe at that position.

Literal dotted keys are why that distinction matters. Attribute names in this
system are dotted, so a caller may spell `contact.email` either nested or as one
literal key, and a literal key is an unknown property to JSON Schema.
`transform.NormalizeDottedKeys` expands literal dotted keys into their nested
paths before validating, so their values *are* checked — except for a dotted key
written above a schema array, the one documented gap below. Anything at or
beneath a relation root is not validated either, but it is not stored unchecked:
the whole subtree is removed from the payload before either step (see "Relation
subtrees are never caller-writable").

Normalization errs in the other direction in one case: at an array it can check
slightly *more* than the writer stores, and so reject a value the write would
have discarded. See "False rejection: a shrinking list over already-invalid
data".

### Creates reject, updates report

A create that violates its schema is rejected. An update that violates its
schema is **logged and written** unless `Entity.ValidateUpdatesStrict` (env
`VALIDATE_UPDATES_STRICT`, honoured by both `cmd/server` and `cmd/lambda`) is
set, which flips updates to enforcing.

Report-only is the default because rows written before enforcement existed may
already violate their schema, and rejecting on update would make them
**un-updatable**: a caller touching one unrelated field would be refused for a
pre-existing violation elsewhere, with no way to repair the row through the API.
Creates have no legacy data and so always enforce.

**Do not flip strict mode before an e2e pass on real data.** The update path
validates the *merged* document — `FromPersistentRecord` reconstructs the stored
entity out of EAV rows and `mergeMaps` overlays the caller's fragment — so what
is judged is the EAV round-trip, not what the caller sent. Shipped schemas put a
`pattern` — plus an inert `format: uuid` — on required identifiers (`lead.json`
`$defs/lead_id`, `visit.json` `$defs/visit_id`), and the pattern is the half that
bites, so any reconstruction that is lossy for such a field turns a legitimate
partial update into a rejection the caller cannot diagnose from their own
payload.

### Which errors are which class

`validateWritePayload` splits on sentinel evidence, not on the enforce flag:

- A genuine violation wraps `forma.ErrInvalidInput` → `400`. Report-only mode
  absorbs exactly this case, logging it at `Warn` and proceeding.
- **Everything else is returned regardless of enforcement** — a missing resolved
  schema, or a payload that will not marshal (`NaN`/`Inf`). Those are plain
  errors, therefore `500`, therefore operator-visible. Absorbing them into
  report-only would write the document with *zero* validation while a log line
  claimed it had merely failed a check, and would blame the caller for a server
  fault.

### Startup fails closed

Construction resolves **every** schema name `registry.ListSchemas()` returns.
Any name that will not resolve — an unparseable document, a `$ref` outside the
schema directory, a missing file — aborts `factory.NewEntityManagerWithConfig*`
and the server does not start. A broken `$ref` is a deploy-time failure rather
than a silent loss of validation at runtime.

**Upgrade risk, precisely:** a `schema_registry` row whose name has a
`<name>_attributes.json` on disk but **no `<name>.json`** will now refuse server
startup. `internal/schemameta`'s file registry deliberately tolerates that
today — `loadSchemaArtifacts` treats a missing schema document as "no JSON
Schema" and registers the attribute cache alone — so such a deployment is
currently running and healthy. Validator construction calls
`GetSchemaByName` for every listed name, which answers
`schema data not found: <name>` for exactly that shape.

The operator preflight is one sentence: **every schema name registered in
`schema_registry` has a resolvable `<name>.json` in `SCHEMA_DIR`.** No database
column is involved — `schema_registry` holds only `schema_id` and `schema_name`;
the document lives on disk.

A second fail-closed check sits beside validator construction in the factory:
`internal.ValidateRelationSchemas` aborts startup for a schema that lists a
relation root in `required` — see "Relation subtrees are never caller-writable".

Its reach is wider than that one rule. It propagates **every** relation-index
load failure, so an unreadable or missing `SCHEMA_DIR`, or any `.json` file in
it other than the `*_attributes.json` ledgers that does not parse as a JSON
object, now aborts startup as well — where `NewEntityManager` previously logged
a warning and continued with relation stripping disabled. **A stray malformed
`.json` left in `SCHEMA_DIR` is therefore fatal.** An unconfigured (empty)
`Entity.SchemaDirectory` is still not an error.

### Validation gap: a dotted key written above a schema array

A literal dotted key naming an attribute that lies **under a schema array** is
not validated when it is written at a level *above* that array. Nesting it would
put an object where the schema declares an array, so the caller would get a
`400` naming a type they never sent; `NormalizeDottedKeys` leaves the key literal
instead, JSON Schema treats it as an unknown property, and its value is never
examined. This happens **identically whether or not the array itself appears in
the payload** — the decision is derived from the schema, not from the document's
shape.

The discriminator is *where the caller writes the key*, not which attribute it
names. Written **inside** an array element the same name is expanded and
validated normally: `{"propertyInterests": [{"snapshot.price": "x"}]}` is
rejected with `.../snapshot/properties/price: type: x has type "string", want
"number"`, while `{"propertyInterests.snapshot.price": "x"}` at the root raises no
complaint about that value at all — the writer stores it, and the schema never
sees it.

On the shipped schemas the exposed surface is exactly **15 attribute names**,
all in `lead.json` (and its `lead_full.json` twin, which repeats them):

- `requirement.areas.` `city`, `note`, `prefecture`, `ward`
- `propertyInterests.` `propertyId`, `status`, `isPrimary`, `notes`,
  `firstSeenAt`, `lastActivityAt`
- `propertyInterests.snapshot.` `address`, `code`, `price`, `rent`, `title`

The "inside an element is fine" half holds unconditionally only because **no
shipped schema nests an array inside an array**; a dotted key inside an outer
element that crossed an inner array would fall back into the gap.

### False rejection: a shrinking list over already-invalid data

The gap above is a value going unchecked. This is the opposite error, and it is
**live, not latent**: one shape is checked more strictly than it is stored, and
can be rejected on a value the write would have thrown away.

The cause is a rule mismatch between the two halves of the write path.
`NormalizeDottedKeys` builds the document the validator sees; the writer receives
the caller's original map, and `transform`'s dedupe resolves duplicate spellings
there. The dedupe drops a losing spelling's records for the **whole logical
attribute** — every array index. The validator's view merges two arrays **per
index**. So when the nested spelling is the longer one, its surplus elements
survive into the view and never into storage:

```json
{"requirement": {"areas": [{"city": "A"}, {"city": 12345}]},
 "requirement.areas": [{"city": "NEW"}]}
```

The writer persists exactly one record, `requirement.areas.city[0] = "NEW"`. The
view is `[{"city":"NEW"},{"city":12345}]`, and `requirement.areas.city` is
declared `string`, so validation reports
`type: 12345 has type "integer", want "string"` — about an index that will not
exist after the write.

**What an operator sees.** The rejection needs the surplus stored data to violate
the schema *already*. On the default update path that surfaces as a report-only
`Warn` naming the schema and row and the write proceeds; it becomes a `400` only
under `Entity.ValidateUpdatesStrict`, which is exactly the mode meant to surface
pre-existing violations. On create there is no stored surplus to inherit, so the
only way to hit it is to send both spellings with a longer nested one in the same
body. If it appears in strict mode, the fix is the same as for any pre-existing
violation: correct the stored data, or send the list under one spelling.

**Why it is not fixed.** Making the view replace instead of merge trades this for
the worse error — a *persisted* value going unvalidated, which is the bypass #314
exists to close. Over-approximating is the safer direction: the failure mode is a
visible rejection rather than silent non-enforcement. Reproducing the writer's
rule exactly is possible in principle and deliberately not attempted; every
previous refinement of that merge introduced a new defect. Pinned by
`TestNormalizeArrayMergeOverApproximatesShrinkingList`.

### Latent, not currently reachable

An array declared behind a `$ref` or inside `$defs` is invisible to the array-path
derivation (`schemavalidate.deriveArrayPaths` does not follow `$ref`, because the
library keeps resolved targets in an unexported side table). A dotted key under
such an array would therefore be expanded, placing an object where the schema
says array, and the caller would receive a **false `400` naming a type they never
sent**. The same applies to arrays reached through `if`/`then`, `prefixItems`,
`patternProperties`, `dependentSchemas` or `contains`, none of which the
derivation walks.

No shipped schema reaches this. None uses `if`/`then`, `prefixItems`,
`patternProperties`, `dependentSchemas` or `contains` at all. The one `$ref` that
does hide an array — `visit.json`'s `contactSnapshot` → `lead.json`'s `contact`,
which contains `phones` — has no attribute registered beneath that array in
`visit_attributes.json`, so no dotted key can name one. `NormalizeDottedKeys`
also carries a payload-shape backstop (`arrayOnPath`) that catches the case where
the caller does send the array.

### Behaviour change: `watch` payloads now need `id`, `name` and `brand`

`cmd/sample/schemas/watch.json` declares `"required": ["id", "name", "brand"]`
while `watch_attributes.json` marks nothing required. Before #314 the schema's
`required` was decorative, so a `watch` payload omitting any of the three was
accepted; it is now rejected on create. The bundled CSV importer supplies all
three, so nothing shipped breaks.

### Relation subtrees are never caller-writable

**The rule: nothing at or beneath an `x-relation` property is written by the
caller or schema-validated, in either spelling.**

`RelationIndex.StripComputedFields` removes the relation root and every dotted
descendant from the payload — `{"contactSnapshot": {...}}` and
`"contactSnapshot.name"` alike — before creates and updates validate, so what is
checked is what is stored. The values are derived on read from the parent
entity, which replaces the whole subtree, so a caller-written value there is
unreadable wherever that enrichment applies.

Dropping is silent — unless an attribute policy beneath the root demands the
value; see below. Short of that, neither spelling produces a `4xx`: the nested
spelling has always been dropped without a rejection, and #318 brought the
dotted spelling into line rather than adding a new rejection to payloads that
were accepted before.

Because the subtree never reaches the validator, constraints declared under an
`x-relation` `$ref` are **decorative on the child**. They still apply on the
parent entity, where the data actually lives.

**A relation root listed in the schema's root-level `required` is rejected at
startup.** The root is stripped from every payload before validation, so the
entity would fail every create and update with a missing-required error that the
caller cannot fix by sending the field. `internal.ValidateRelationSchemas` —
called from the factory alongside the schema validator — refuses to start,
naming the schema and the property.

**A `required_always` attribute policy beneath a relation root breaks the entity
the same way, and this one is *not* caught at startup.** The attribute-metadata
required check that runs on write — `validateRequiredAttributesFromInput`,
called from `transform`'s `ToAttributes` after the strip and after JSON Schema
validation — walks the whole attribute cache with no relation carve-out, so it
looks for the value the strip has just removed and answers
`400 missing required attribute '<name>'` on every create and update, again
unfixably by sending the field. `ValidateRelationSchemas` cannot see this one:
it reads the schema's `required` list and never the `<name>_attributes.json`
ledger.

Note the asymmetry with the *other* required check on the same write.
`ToAttributes` also runs `AttributeConverter.FromEAVRecords` over the flattened
records, and that check *does* carve relation roots out (#315). The carve-out
belongs to that check, not to the read path: `FromEAVRecords` runs on both
paths — `ToAttributes` on every create and update, `FromPersistentRecord` on
read. This is pre-existing behaviour, not something #318 introduced.

The shipped schemas are safe. The only policy beneath `contactSnapshot` is
`contactSnapshot.isAnonymous`, and it is `required_if_parent_present` — with the
parent stripped away it reports nothing missing. `required_always` is the shape
to keep out from under a relation root.

Only `visit.json`'s `contactSnapshot` carries `x-relation` today.

#### Behaviour change (#318): the dotted spelling no longer persists

Before #318 the strip matched by exact key, so `{"contactSnapshot.name": "Ada"}`
survived it and was written to the EAV table while the nested spelling was
discarded. That value was unreadable wherever relation enrichment applied — it
replaces the whole `contactSnapshot` object with the parent's fragment — and the
next update deleted it: the update path rebuilds the dotted attribute name into
a nested object (`FromPersistentRecord` → `FromAttributes`), the strip removes
that object from the merged document, and the update's scoped EAV replace
deletes every attribute id registered for the schema before writing back only
what survived. It is now dropped on write, like the nested spelling.

Rows written before #318 keep whatever `contactSnapshot.*` values they hold. No
migration is performed. They are not returned wherever relation enrichment
applies, and they are removed the next time the row is updated; until then they
remain visible to attribute filters and to Parquet exports, neither of which
knows about relations.

If an operator wants them gone sooner, the cleanup must go **through the write
path** — an update per row. A direct `DELETE` against `eav_data` writes no
`change_log` row: nothing but the repository's own write paths writes one, and
there are no database triggers. So a row whose **latest** `change_log` entry is
already flushed does not re-enter the federated dirty set on a hand-delete, and
DuckDB keeps serving the stale copy from `/delta/` or `/base/` while
PostgreSQL-only reads see the value gone.

## Read-path consistency errors

Read operations return plain errors when persisted data and metadata disagree.

Examples:

- duplicate schema IDs or duplicate attribute IDs during metadata loading
- storage column mismatches such as a text attribute stored in `value_numeric`

These errors indicate metadata drift, corrupted state, or an incomplete
deployment, and should be treated as operator-visible consistency failures.

EAV records whose attribute id is no longer present in the schema metadata are
not an error: they are skipped on read and preserved on update (#294
tolerate-and-preserve), so removing an attribute from a schema is
non-destructive and re-adding it restores the stored values. An attributeID
freed by removing an attribute must never be reused for a different attribute:
preserved EAV rows would silently bind to the new attribute's name (same value
type) or make the row unreadable with a storage type mismatch (different value
type).

Since #342 that rule is enforced. `generate-attributes` keeps the removed
attribute's entry in `<schema>_attributes.json` marked `"retired": true` and
forced optional — the file is the attributeID ledger, not just the active
attribute list. Every production registration path — the DB-backed
`MetadataLoader` file path, and the file registry in both its registry-table
and directory modes (`cmd/sample` uses the latter) — validates the **full**
cache with retired entries included, and fails when an active attribute rebinds
a retired attributeID, a retired main-column binding, or a retired attribute's
folded parquet column. `MetadataCache.RegisterSchema` applies the same
validation but has no production callers; it is defence in depth for
programmatic registration. Retired entries are stripped only after that check
and never reach a consumer — with one sanctioned exception,
`schemameta.MetadataCache.RetiredAttributeIDs`, which hands validation tooling
attributeID → attribute name only (no `forma.AttributeMetadata`, and not the
live attribute map), so a retired attribute still reads, writes, flushes, and
projects exactly as if its entry were absent — the #294 skip-and-preserve
behavior above is unchanged. Re-adding the same name with the same `valueType`
and `items_type` clears the marker and restores the preserved values **for rows
still in the hot tier only**: CDC derives its EAV `attr_id` filter from the
active cache, so a retired attribute is never exported to parquet, and rows
already flushed are served from warm/cold where the column is absent. On
lakehouse (CDC-enabled) deployments treat retirement as effectively
irreversible for flushed rows — and note the reverse asymmetry, that a
preserved Postgres row re-flushes on the next write after an un-retire, so a
value that read `NULL` can reappear later. This is inherited #294 semantics,
not new to #342; the operator-facing detail is in
[`schema-consistency-migration.md`](./schema-consistency-migration.md#removing-an-attribute-342).
Re-adding under a different type is rejected by the generator, which names the
attribute and both the old and the new type. The guard has no signal for generations
whose entries were hand-deleted from the ledger before #342 — for those files
the rule remains documentation-only. Full removal workflow:
[`schema-consistency-migration.md`](./schema-consistency-migration.md#removing-an-attribute-342).

### `ErrParquetSetInconsistent`

`forma.ErrParquetSetInconsistent`, carried by
`forma.ParquetSetInconsistentError{SchemaID, MissingKeys}`, marks a
federated read whose manifest lists parquet objects that do not exist in
storage. The manifest is the authoritative record of the schema's cold/warm
tier, so a listed-but-absent object means that tier **has lost data** — the
rows in that object are simply gone from the result set.

- **Plain error, not `ErrInvalidInput`.** Nothing about the request is wrong;
  persisted data and the metadata indexing it disagree. It is an
  operator-visible consistency failure, not a user-facing `4xx`.
- **Not degradable.** It surfaces even under
  `federated.allow_partial_degraded_mode`, unlike the transient DuckDB/S3
  failures that fall back to Postgres-only. Degrading here would return
  exactly the silently short answer this classification exists to make loud.
- **Trigger precondition.** Only reachable when the server resolves parquet
  paths from the manifest source (`duckdb.manifestTemplate` configured — see
  `docs/federated-query/design.md` §4.3.1). With per-request path hints, or
  with glob paths, the classification is skipped: a hinted path set whose
  object is missing still fails the scan, but as a plain
  `ErrFederatedReadFailed`, while a glob quietly expands to whatever objects
  survive and returns a shorter result set with no error at all.
- **Direction of the contract.** Only `manifest ⊆ live objects` is enforced.
  Extra unlisted objects are tolerated and invisible to reads.
- **One bounded exception (#251).** An object confirmed corrupt by per-file
  verification is excluded from path resolution for a retention window
  (default 5 minutes), and an excluded object is not scanned — so if it is
  *also deleted* from storage inside that window, the inconsistency
  classification fires only after the cache entry expires and the object is
  rescanned. Delay, not suppression: the loss still surfaces, up to one
  retention window late. See `docs/federated-query/design.md` §7.3.
- **Operator action.** The message names the schema and the missing
  bucket-relative keys. Run the reconciliation tool to diagnose and repair:
  see `docs/manifest-reconcile.md`.

### `ErrNoParquetPaths`

`forma.ErrNoParquetPaths`, carried by
`forma.NoParquetPathsError{SchemaID, SourceConfigured}`, marks a
DuckDB-routed federated read whose parquet path set resolved **empty**: no
per-request render hint, and either no configured parquet source or a source
whose manifest lists no files while the fallback glob is disabled (see
`docs/federated-query/design.md` §4.3.1).

- **Plain error, not `ErrInvalidInput`.** With a per-request hint the caller
  can be at fault and the error wraps `ErrInvalidInput` instead (an
  unrenderable or degenerate `s3_parquet_path_template`). Reaching *this*
  error means no hint was supplied, so the read surface — server
  configuration, or manifest state — is what cannot answer the query.
- **Not degradable.** Like `ErrParquetSetInconsistent`, it surfaces even under
  `federated.allow_partial_degraded_mode`. Every query that reaches the DuckDB
  engine wants warm and/or cold data (hot-only and `prefer_hot` requests
  short-circuit to Postgres before this point), so a Postgres-only fallback
  would be silently short precisely where the cold tier was requested.
- **Why it is not a read failure.** Before #299 the empty set rendered
  `read_parquet(<no value>)`, and the resulting DuckDB parser error classified
  as the degradable `ErrFederatedReadFailed` — loud, but indistinguishable
  from a transient S3 outage to any programmatic discriminator, so degraded
  mode turned a configuration mistake into a quietly incomplete answer.
- **Not a normal empty state.** A never-flushed schema resolves the level-3
  fallback glob, not an empty set; reaching this error means the fallback is
  disabled too (`duckdb.s3DataPrefix` empty).
- **Operator action.** The message names the schema and distinguishes the two
  causes: configure the read surface (`duckdb.manifestTemplate` +
  `duckdb.s3Bucket`), set `duckdb.s3DataPrefix` to re-enable the fallback
  glob, or repair the schema's manifest (`docs/manifest-reconcile.md`).

### `ErrManifestSchemaMismatch`

`forma.ErrManifestSchemaMismatch`, carried by
`forma.ManifestSchemaMismatchError{RequestedSchemaID, ManifestSchemaID, Path}`,
marks a manifest object whose recorded `schema_id` disagrees with the schema
being read. It is raised before any path reaches a scan.

- **Why it is loud rather than a short read.** A manifest addresses one schema
  by path convention alone. Nothing downstream re-checks it: the parquet scan
  does not filter rows by schema (files are per-schema by path) and the
  projection stamps whatever it scans as the *requested* schema. So a path
  collision between two schemas would not merely under-read — it would serve
  another schema's rows under this schema's identity.
- **Not degradable.** Cross-schema contamination is the opposite of a partial
  answer, so `allow_partial_degraded_mode` does not absorb it. It is also not
  relabelled as `ErrFederatedReadFailed` on the way out of path resolution,
  which would have handed it to the degraded fallback.
- **Relationship to config validation.** `duckdb.manifestTemplate` is
  probe-validated at startup against two schema IDs, which catches a collapsed
  template (a constant path, a `{{.SchemaId}}` typo) but cannot prove
  injectivity over the whole schema domain. This error is the runtime
  enforcement of what that check can only sample.
- **Compatibility.** A manifest whose `schema_id` is zero is treated as
  unstamped rather than as schema 0 — schema IDs are always positive, and
  rejecting zero would break reads for deployments still holding objects
  written before the field existed.
- **Operator action.** The message names both schema IDs and the manifest key.
  Fix the template so each schema resolves a distinct object, then repair the
  affected manifests (`docs/manifest-reconcile.md`).

## Public HTTP error surface

`internal/httpapi` treats the response body as an untrusted destination. The
split follows the two error classes above, and is enforced by
`respondErrorWithStatus` in `internal/httpapi/error_response.go`. That is the
only gate **for manager-layer errors**: `respondError` merely classifies and
delegates to it, and `executeGet` (`internal/httpapi/server.go:175`) calls
`respondErrorWithStatus` directly so it can choose its own 404 wording.

Handlers also call `writeError` directly — 33 sites in `server.go` — and those
bodies are verbatim without passing the gate. They are safe because every one of
them reports a request-parsing failure (`parsePath`, `readJSONBody`,
`parseUUID`, `parseCreateObjects`, `parseSortParams`); none touches the manager,
the engine, S3, or `PG_CONN`. What holds that line is the source-level guard
`TestWriteErrorAlwaysCarriesALiteral4xxStatus`
(`internal/httpapi/error_leak_test.go`), which fails the build unless every
direct site passes a literal 4xx constant from an allowlist — so a new handler
cannot echo a runtime-classified status without going through `respondError`.

**The status is decided by sentinel evidence and by nothing else.**
`classifyManagerError` matches `errors.Is` against `forma.ErrNotFound` (404),
`forma.ErrConflict` (409), and `forma.ErrInvalidInput` (400); everything else —
including a `nil` error — is `500`.

**Disclosure needs the same evidence plus a deliberately published message
(#313).** The gate is `isClientError(err)` plus `resolvePublicMessage(err)`: a
4xx body carries text only when the error *provably* wraps one of those three
sentinels *and* the chain holds a `forma.PublicError` — a carrier built by
`forma.InvalidInputf`/`NotFoundf`/`Conflictf`, optionally prefixed through
`forma.WrapPublicf`. What crosses is `PublicMessage()`, never `err.Error()`.
Everything else is redacted. See "Only published text crosses" below for how
this replaced the earlier chain-shape rule.

There is no substring heuristic. An earlier version classified on message text
(`not found` → 404, `duplicate` → 409, `invalid`/`required`/`must be` → 400) for
errors that wrapped no sentinel. It was removed for two reasons:

- **It produced wrong statuses, and redaction did not fix that.** Driver prose
  trips those words. DuckDB renders a missing S3 object as
  `HTTP Error: … 404 (Not Found).`, so an S3 or credential failure answered HTTP
  `404`. Hiding the body leaves the protocol lie in place: clients, caches, and
  alerting read `404` as "the resource is absent", stop retrying, and may cache
  the negative result. #301 asked for read-path consistency errors to answer a
  generic 5xx, and `AGENTS.md` classes them as operator-visible failures, not
  4xx.
- **It was the last site classifying an error by string comparison**, which
  `AGENTS.md` forbids.

The consequence is that a genuine client error earns its 4xx only by carrying a
sentinel. Removing the heuristic therefore required a sweep of the sites that
had been relying on it — every one of them now builds a `forma.InvalidInputf`
carrier, and the message each publishes is the same human-authored text it
always rendered:

| site | caller mistake |
| --- | --- |
| `internal/entity_query_sort.go` | sorting by an attribute the schema does not define (#296) |
| `internal/transform/transformer.go` (`validateRequiredAttributesFromInput`) | create/update body omitting an attribute whose metadata `required_policy` demands it |
| `internal/sqlgen/predicate_normalizer.go` | filtering on an unknown attribute; unparseable numeric/bool filter value; unsupported operator; an operator the attribute's type does not accept (`starts_with`/`contains` on a non-text column, an inequality on a boolean) |
| `internal/sqlgen/dualpath_sql_helpers.go` | unparseable numeric/date/bool literal in a main-column or federated predicate |
| `internal/conditionexpr/parser.go` | malformed `"op:value"`; unknown operator; unparseable date |

The write-path entry matters most: without it, a `POST` omitting a required
attribute would answer `500` with an opaque body instead of naming the attribute.
The `sqlgen`/`conditionexpr` group is reachable through `POST
/api/v1/advanced_query`, whose `condition` payload is entirely caller-supplied.

**The sentinel suffix no longer reaches bodies — #309's clause is overturned by
#313.** `Error()` still renders `<message>: invalid input` (likewise
`: not found`, `: conflict`), byte-identical to the old `%w` wraps, so logs and
Go embedders see the same text as before. But the body now carries
`PublicMessage()`, which is the human-authored message alone — e.g.
`unsupported operator: equals`, no suffix. #309 declined to strip the suffix
because doing so meant touching every wrap site; #313 touched every wrap site
anyway, so the removal came for free. The standing advice is unchanged and now
enforced by construction: clients must not assert on the exact full message —
match a substring (this repo's tests use `Contains`) or, better, key on the
status code.

**Authorship rule for wrap layers.** `forma.WrapPublicf` prefixes both the
operator message and the published one; use it only where the layer adds
caller-actionable identification — a batch index (`operation[%d]`), a
caller-supplied name. A layer that adds operator context (an internal phase
name like `pg sql generation`, an internal schema id) stays a plain
`fmt.Errorf`, so its prefix reaches the log and never the body. Operator-only
facts that belong *on the same error* ride `forma.WithOperatorDetail`: they
stay in `Error()` and in the chain for `errors.Is`/`As`, and never in
`PublicMessage()`.

Since #314 there is a **second** write-path validator on the same footing,
`internal/schemavalidate`'s `Validator.Validate`, which builds an
`InvalidInputf` carrier for a JSON Schema violation — `enum`, `pattern`,
`type`, `minimum`/`maximum`, and the schema's own `required`. It is independent
of the `required_policy` row above: the two check different things and both
run. Its *non*-violation errors deliberately stay plain, so a `500`; see "JSON
Schema enforcement on write" for the split.

Its published message deliberately includes the third-party `jsonschema-go`
violation prose (decision recorded at the wrap site, `validator.go`): that text
is `type`/`enum`/`pattern`/`minimum` prose over the caller's own instance and
the schema's own constraints — no paths, no credentials. Recorded trigger for
revisiting: the library's `anyOf`/`oneOf` branches render schema objects,
`$ref` fragments included. No shipped schema uses `anyOf`/`oneOf` today; if one
ever does, that site switches to `forma.WithOperatorDetail`.

**The sweep also underreached once.** It missed
`normalizePgEavPayload`'s operator whitelist — the two rejections that pair an
operator with an attribute type it does not accept — so `starts_with` on a UUID
column answered an opaque `500` while the "condition-DSL errors stay 400" claim
below said otherwise. Both now wrap the sentinel — prefix unchanged, same
appended suffix as the sweep above — and are pinned by the `clientError` column of
`TestToDualClauses_Characterization_Errors` plus
`TestAdvancedQueryOperatorWhitelistIs400AndVerbatim`.

Two neighbouring errors in the same function deliberately stay plain, and they
are the boundary worth remembering: `unsupported value_type '%s' for attribute
'%s'` names the *schema's declared type*, and `unknown main table column` names a
column resolved from `entity_main` descriptors or from a column binding — neither
is anything the caller sent, so `500` is the truthful answer. The test for what
gets a sentinel is provenance of the offending value, not which package raised
it.

**The sweep overreached once, and the overreach has been reverted.** It also
wrapped the identical `missing required attribute …` message inside
`AttributeConverter.FromEAVRecords`
(`internal/transform/attribute_converter.go`). That converter is *not*
write-only: `FromPersistentRecord`
(`internal/transform/persistent_record.go`) rebuilds already-stored records
through it on the read path, so a persisted row missing a required EAV row
satisfied `errors.Is(err, forma.ErrInvalidInput)` and the boundary answered a
verbatim `400` for state the caller cannot fix — exactly the inversion the two
error classes above exist to prevent. That error is plain again. The write
path's `400` never depended on it: `ToAttributes` runs
`validateRequiredAttributesFromInput` against the caller's input *before*
flattening, and that is the sentinel-carrying validator. A sentinel belongs on a
validator that only the write path can reach; if a converter is shared, the
check has to move rather than the sentinel be added.

Errors the heuristic used to catch that were **not** given sentinels are either
unreachable from a handler (registry load, `internal/postgres_health.go`, `cmd/`
startup validation) or internal invariants — `schema id must be positive`,
`duplicate attribute id`, `unsupported column %q`, metadata drift — for which
`500` is the more truthful answer than the misleading 4xx they used to return.

The full disclosure condition is `status < http.StatusInternalServerError &&
isClientError(err)` plus a resolved publication (`resolvePublicMessage`). The
status conjunct can only hold disclosure back, never grant it: a caller that
passes an explicit 5xx to `respondErrorWithStatus` gets a redacted body even if
the error wraps a publishing carrier. Pinned by `TestCarrierAtA5xxIsRedacted`.

Status and disclosure are separately decided in a way that a client must not
read as coupled: an error that carries a client sentinel but publishes nothing
— a bare sentinel wrap, or a carrier-less mixed chain — classifies `4xx` on
that sentinel and **still redacts**, producing a `400` body with `error_class`
and `error_id`. Clients must key on `error_class`/`error_id` being present,
never on the status, to know whether a body is redacted.

**Redacted bodies (#301)** carry a fixed message, a stable `error_class` token,
an `error_id`, and — when the chain holds a typed read-path carrier — a
`schema_id`. No error text crosses. There are exactly two fixed messages
(`publicErrorMessage`): `internal read error` for the three typed read-path
classes, and `internal error` for `errorClassInternal`. **`internal` is the
common case in production** — it absorbs `ErrFederatedReadFailed`,
`ErrPostgresReadFailed`, metadata drift, and transform failures — so a client
asserting on the literal `internal read error` would break on the majority of
redacted responses. Discriminate on `error_class`, never on `error`. The chain
goes to `zap.S().Errorw`; the disclosed branch logs the same text at `Debugw` —
or at `Warnw` when the error withholds operator detail, see "Log levels" below.
An operator retrieves the detail from the `error_id` the caller quotes.

#### `schema_id` on a redacted body — a reversed decision

`errorSchemaID` (`internal/httpapi/error_response.go`) resolves the schema with
`errors.As` against the three typed read-path carriers —
`ParquetSetInconsistentError.SchemaID`, `NoParquetPathsError.SchemaID`, and
`ManifestSchemaMismatchError.RequestedSchemaID`. Anything else resolves 0 and
the field is omitted. `errors.As` rather than a type assertion, because the
carriers reach the boundary wrapped several levels deep.

For the mismatch carrier it is deliberately the *requested* id, not the
`ManifestSchemaID` stamped on the object. That stamp belongs to whichever other
schema misaddressed the manifest; returning it would answer a request about one
schema with another schema's identity, and would name a schema the caller never
asked about. Operators still see both ids — the full message is on the log line.

**This reverses a decision recorded in this document.** Issue #301 asked for
"error class + schema id"; the design settled on `error_class` + `error_id` and
this section previously stated "no schema id" as a constraint on redacted bodies;
the issue owner reinstated the schema id. The reasoning that justified excluding
it is what now permits it — a schema ID is a low-value opaque integer. (When
this was decided one also crossed inside ID-keyed 404 prose; #313 has since
closed that, see "Formerly accepted disclosures" below, but the structured
field here stands on the same low-value reasoning.) What it buys is a redacted
body a client can correlate without an operator round-trip.

`schema_id` is `omitempty` on `APIResponse`, and that encoding is lossless rather
than lossy only because **schema IDs are always positive** — the same invariant
that lets a manifest `schema_id` of zero read as *unstamped* rather than as
schema 0 (see `ErrManifestSchemaMismatch` → Compatibility, above). A zero can
therefore only mean "the error named no schema". Pinned on the serialized bytes,
not the struct field, by `TestRedactedBodyOmitsSchemaIDWithoutACarrier`.

The same value is logged as its **own structured field**, not folded into the
message, so operator log queries filter on `schema_id` instead of parsing prose.
It is omitted from the log line too when zero, so a log entry never asserts a
schema the error did not name.

Published 4xx bodies are unaffected: population happens on the redacted branch
only, so a disclosed body carries message text and nothing else — even when the
chain holds a resolvable carrier as operator detail. Pinned by
`TestPublished4xxBodyCarriesNoSchemaID`.

### Credentials are scrubbed before anything is written

`redactCredentials` (`internal/httpapi/error_response.go`) runs on every string
this boundary emits — response body *and* log line — replacing the value of any
`password=…` assignment with `***REDACTED***`.

The log half is the point. Before #301 this package logged no errors at all, so
routing the full chain to `Errorw` newly put the Postgres password — which DuckDB
quotes back inside its own attach-failure prose — into whatever log collector and
retention the deployment runs. Scrubbing at the source of those wraps is tracked
by #306; this boundary scrub is what protects deployments in the meantime.

**The matcher is not local to `httpapi`.** It lives in `internal/redact`
(`ConnStringPassword`), shared with the CDC logger, which has needed the same
scrub since #290. Sharing is a correctness requirement, not tidying: a naive
`'[^']*'` branch mistakes libpq's escaped `\'` for the closing quote and emits
the password tail past the placeholder. That was a real bug fixed in #290, and
its regression tests (`internal/cdc/redact_test.go`) exercise the shared matcher
through `redactConnStr`, and `internal/redact/connstring_test.go` gates the
pattern in the package that owns it. Three forms are covered — the
`sqlutil.EscapeLiteral`-doubled `password=''…''` used inside DuckDB `ATTACH` literals,
the libpq-quoted `password='…'`, and the bare `password=value` of legacy or
third-party text.

**A scrubber alone was not enough, because it cannot see where an unquoted value
ends.** The bare branch has to stop somewhere, and whatever it stops on truncates
a password containing that character, leaving the tail in the log — which is the
exposure this whole section exists to prevent. Two changes closed it:

- The bare branch now terminates **only** on a quote or whitespace. It used to
  also stop on `;`, `,` and `)`, none of which is a separator in a libpq
  keyword/value DSN, so any password containing one leaked its tail. Pinned by
  `TestConnStringPassword_UnquotedSeparators`.
- `federated.DuckDBPostgresConnStringFromPool` now **quotes its values**, via the
  shared `internal/pgdsn`, as `internal/cdc`'s builder already did since #290. It
  previously emitted `host=%s … password=%s dbname=%s` raw. That was two bugs in
  one: a password containing a space produced a DSN libpq cannot parse (so
  `postgres_scan` could not attach at all), and it was unredactable. Quoting is
  what makes the value's extent unambiguous.

Because the DSN is interpolated into a single-quoted SQL literal
(`postgres_scan('{{.PG_CONN}}', …)`), the renderer escapes it —
`sqlutil.EscapeLiteral`, called from
`internal/sqlgen/duckdb_template_renderer.go` — the same shared helper
`internal/cdc` uses, consolidated in #310. Without that, the newly-added
quotes would terminate the literal early. It also closes a hole that
predates the quoting: a Postgres password containing a single quote used to
be interpolated raw into query structure.

One shape is deliberately **not** matched, recorded in
`TestRedactCredentialsKnownGaps` and `TestConnStringPassword_KnownGaps`:
whitespace around the `=` (`password = secret`). No producer emits it. The
residual beyond that is an *unquoted* value containing a space, which no pattern
can match because nothing marks its end — fixed at the producer instead, so every
password this repo generates now lands on a quoted branch.

It is deliberately narrow, and **non-secret operator detail is kept on purpose**:
S3 object keys, schema ids, endpoint URLs, and the driver's own diagnosis all
survive verbatim in the log. A redacted response leaves the operator with an
`error_id`, a `schema_id`, and that log line, and a blanket scrub would leave
nothing to correlate the id against. Pinned by
`TestRedactCredentialsKeepsOperatorDetail`.

Of that list only the schema id also crosses to the client, as its own
`schema_id` field (see above). Object keys, endpoint URLs and driver prose are
log-only.

### Only published text crosses

`isClientError` uses `errors.Is`, which matches any leaf. A multi-cause chain —
`errors.Join(forma.ErrInvalidInput, driverErr)`, or the `fmt.Errorf("%w: %w", …)`
shape used throughout `internal/federated` — used to take the **verbatim**
branch on that leaf alone and echo the driver cause alongside the client one.
`redactCredentials` covered the credential half, but a non-credential operator
detail (an S3 object key) reached a public `400`.

#307's answer was a chain-*shape* rule, `canDiscloseVerbatim = isClientError &&
!hasMultipleCauses`, recorded here with the claim **"Blast radius: nil — no
production error joins a client sentinel to an operator cause."** That claim
was false when written: `internal/federated/duckdb_query_build.go` joined
`forma.ErrInvalidInput` to `text/template`'s render error for an unrenderable
caller-supplied path template, HTTP-reachable through `POST
/api/v1/advanced_query`. The rule was safe there but harmful: the caller's own
broken template answered an opaque `"internal error"` 400. Shape was also a
weak proxy in the other direction — a single-cause wrap can embed driver or
third-party prose in its own message text and disclosed it verbatim.

#313 replaces shape with **provenance of the text itself**. A client error is
built as a `forma.PublicError` carrier (`InvalidInputf`/`NotFoundf`/
`Conflictf`, prefixed via `WrapPublicf`, operator facts attached via
`WithOperatorDetail`), and the boundary emits `PublicMessage()` — text some
wrap site deliberately authored for the caller — or nothing.
`hasMultipleCauses` and `canDiscloseVerbatim` are **deleted**: chain shape
stopped carrying information once raw chain text stopped crossing. A carrier
joined anywhere in a mixed chain publishes only its own message
(`TestMixedChainPublishesClientTextOnly`); a sentinel without a carrier is
denied whatever its shape (`TestUnconvertedSentinelIsRedacted4xx`,
`TestMixedChainIsRedacted`, `TestMultiVerbWrapChainIsRedacted`).

**Resolution is provenance-bound and canonical (#362/#363 reviews, P1).**
`forma.ResolvePublicMessage` is the one traversal: the HTTP boundary calls it,
`WrapPublicf`/`WithOperatorDetail` qualify their input with it, and the
decorators' own `PublicMessage()` delegates through it. A `PublicError` node
qualifies only when a client sentinel is reachable from that node's *own*
subtree — the forma constructors guarantee this by construction. Two gates
searching the whole tree independently would let a mixed chain borrow:
`errors.Join(bareSentinelWrap, foreignPublicError)` has sentinel evidence in
one branch and a `PublicMessage()` in the other, and the foreign text would
cross on the 400. Confining the rule to the boundary was not enough — the
first fix left the decorators qualifying and delegating via whole-tree
`errors.As`, so `forma.WrapPublicf(thatSameJoin, "operation[0]")`
reconstructed the borrow one level up; sharing the traversal closes the class,
not the instance. Node matching follows `errors.As` semantics (direct
implementation or the node's `As(any) bool` protocol), and non-qualifying
nodes are stepped over rather than terminal, so a legitimate carrier behind a
foreign publisher still resolves. Pinned by
`TestForeignPublicationCannotBorrowSentinelBranch`,
`TestForeignNodeDoesNotBlockCarrierResolution`,
`TestDecoratedForeignPublicationIsRedacted`,
`TestDecoratorsDoNotAdoptForeignPublications`, and
`TestAsProvidedPublicationResolves`.

Note the deliberate semantic shift from #307: `errors.Join(operatorErr,
carrier)` now *publishes* the carrier's message at a 4xx, where the shape rule
would have redacted it. That is the intended reading of deny-by-default on the
disclosure axis — the join proves nothing about the carrier's own text, which
was authored for the caller regardless of what it is joined to. The operator
branch of the join still never crosses.

```json
{
  "success": false,
  "error": "internal read error",
  "error_class": "parquet_set_inconsistent",
  "error_id": "9f2c1a7e-…",
  "schema_id": 22
}
```

That example is the `parquet_set_inconsistent` shape; the far more frequent one
pairs `"error_class": "internal"` with `"error": "internal error"` and carries
**no** `schema_id` key at all, because an unclassified chain holds no typed
carrier to read one from.

Classes: `parquet_set_inconsistent`, `no_parquet_paths`,
`manifest_schema_mismatch`, and `internal` for everything else. They resolve via
`errors.Is`, never message text.

### Why an allowlist

Redaction cannot be a blocklist of known-sensitive error types, because the
sharpest leak does not come from one. The federated template interpolates
`postgres_scan('{{.PG_CONN}}', …)`, and `PG_CONN` is built by
`federated.DuckDBPostgresConnStringFromPool` as
`host=… user=… password=… dbname=…`. When DuckDB cannot attach, its own message
is:

```
IO Error: Unable to connect to Postgres at "host=… user=… password=… dbname=…": …
```

That text is driver-authored, so only a deny-by-default rule contains it. A
missing parquet object likewise yields the full `s3://bucket/prefix/key` and the
resolved endpoint URL.

The success path has the matching rule: `toExecutionPlan`
(`internal/entity_query_service.go:138`) allowlists plan fields and drops
`src.SQL`, `src.Params`, `plan.Notes`, and `merge.Notes` for the same reason —
the source SQL embeds the `postgres_scan` connection string, and notes can echo
raw engine errors. Pinned by `TestToExecutionPlan_DoesNotLeakCredentials`.

### Formerly accepted disclosures — both closed by #313

Under the verbatim regime the allowlist was a gate on *provenance*, not on
content, and two live cases put more than the caller's own input into a 4xx
body. Both were recorded here as accepted; both are now **closed**, each by the
remedy this section itself prescribed — summarise at the wrap site, not widen
the redaction gate.

- **Postgres driver prose on a 409 — closed.** `classifyPgError`
  (`internal/postgres_persistent_repository_main_table.go`) used to wrap
  `pgErr.Detail`, so a unique-violation body carried driver-authored text
  naming physical columns (`Key (schema_id, row_id)=(…) already exists.`). It
  now publishes a curated summary — `the write conflicts with a row that
  already exists` — and pushes the whole driver error, `Detail` included, into
  operator detail. Pinned by `TestClassifyPgError`.
- **Internal schema identifiers on a 404 — closed.** The seven ID-keyed
  `forma.ErrNotFound` chains in `internal/schemameta/file_registry.go` used to
  render the internal `int16` (`schema not found for ID: 402`) into the body —
  an identifier the caller never supplied (the URL carries a schema *name*).
  They now publish `schema not found` (likewise `schema data not found` /
  `attribute id index not found`) and carry `schema id N` as operator detail,
  so the log keeps it. The four name-keyed chains still publish the name — it
  is the caller's own URL segment. The three repository not-found sites
  (`postgres_persistent_repository{,_main_table,_batch}.go`) follow the same
  split: the caller-supplied row id (and batch `key[%d]`) is published, the
  internal schema id is operator detail. Pinned by
  `TestRegistryNotFoundPublications`.

A bare schema ID may still cross on a **redacted** body as the structured
`schema_id` field (see "`schema_id` on a redacted body" above) — that decision
predates #313 and stands; what #313 removed is internal ids riding inside
published prose.

### Status change: create errors are now classified

`handleCreate` previously answered every `BatchCreate` failure with a hardcoded
`500`. It now calls `respondError`, so create failures answer their *classified*
status like every other handler. This is a public contract change, and it is
independent of redaction.

The clearest case is an unknown schema. `POST /api/v1/nosuchschema` returns
**`404` instead of `500`**, and its body is **published, not redacted**:

```json
{
  "success": false,
  "error": "batch create failed: operation[0]: schema not found: nosuchschema"
}
```

That follows from the gate rather than contradicting it. The registry's
name-keyed `forma.NotFoundf` leaf publishes the schema name the caller sent,
`batchCreateAtomic` prefixes it through `forma.WrapPublicf` with the batch
index, so `classifyManagerError` returns `404` on sentinel evidence and the
boundary emits the accumulated publication — logged at `Debugw`, with no
`error_class`, no `error_id` and no `schema_id`. (Before #313 the body ended in
`: not found`; the sentinel suffix no longer crosses. The `failed to get
schema` phase context is a plain wrap per the authorship rule — #362 review,
P2 — so it stays in `Error()` and the log, not the body.) Pinned by
`TestCreateUnknownSchemaIs404AndVerbatim`.

Clients keying on `500` to detect create failures must key on `success: false`
plus the status instead.

### Status change: an unknown filter attribute is now 400, not 404

Baseline for this section is the **pre-#301** contract, the same baseline the
create-error section above uses.

`POST /api/v1/advanced_query` with a `condition` naming an attribute the schema
does not define used to answer **`404`**. The error
(`attribute not found in cache: …`, `internal/sqlgen/predicate_normalizer.go`)
carried no sentinel, so the substring heuristic saw `not found` in its text and
classified it as a missing *resource*. It now answers **`400`**, because that
error wraps `forma.ErrInvalidInput` as part of the sweep above.

| | before (pre-#301) | after |
| --- | --- | --- |
| status | `404` | `400` |
| body | verbatim | disclosed (published since #313) |

**Only the status changed at #301.** The message a caller reads is the same
human-authored text throughout; since #313 it arrives as the carrier's
published message (minus the sentinel suffix) rather than as raw chain text.
No `error_class`, `error_id` or `schema_id` appeared on this response before,
and none appears now. A client keying on `404` to detect a filter typo breaks
silently, and that is the whole of the migration impact.

**Most other condition-DSL errors did not change.** Unparseable filter values,
unknown operators and malformed `"op:value"` all contain `invalid` or
`unsupported`, so the heuristic already classified them `400` with a usable
body, and they still answer `400` with the same message published. For those,
carrying the sentinel *preserved* the existing contract rather than altering
it — without it, deleting the heuristic would have regressed them to a
redacted `500`. That is what the sweep was for.

### Status change: an operator the attribute's type rejects is now 400, not 500

Baseline for this section is the **pre-#301** contract, as above.

`normalizePgEavPayload`'s operator whitelist raises two messages —
`operator '…' only supported for text attributes, not '…'` and
`operator '…' not supported for boolean attributes`. Neither contains any of the
heuristic's trigger substrings: `only supported` and `not supported` are not
`unsupported`, and nothing else matched. So `POST /api/v1/advanced_query` with
`starts_with` on a UUID column answered **`500`** before #301 too, with a
verbatim body; after #301 removed the heuristic it answered `500` with a
*redacted* body, which is when the claim above stopped being true of them.

| | before (pre-#301) | after #307, before round 4 | now |
| --- | --- | --- | --- |
| status | `500` | `500` | **`400`** |
| body | verbatim | redacted | published (#313) |

The sweep simply missed this function; the operator is caller-supplied and the
message names exactly what to change, so `400` is what the DSL contract always
intended. A client keying on `500` to detect a rejected operator must key on
`400` plus `success: false` instead.

### Status change: a duplicate-attribute payload now succeeds

Baseline for this section is the **pre-#301** contract, as above.

Attribute names in this system are dotted, so a single request body can spell one
attribute two ways — nested (`{"contact":{"email":…}}`) and literal
(`{"contact.email":…}`). `flattenToAttributes` reached both spellings and emitted
two `eav_data` records sharing the primary key
`(schema_id, row_id, attr_id, array_indices)`, and `insertEAVAttributes` sends
them in one multi-row `INSERT` with no `ON CONFLICT`. The write failed on
PostgreSQL `23505`.

This is not an exotic payload. On update, `mergeMaps` is key-literal while
`FromPersistentRecord` re-nests stored attributes, so an ordinary
`PUT {"contact.email":"x"}` against an entity that already holds that attribute
merges to *both* shapes — using the attribute name the schema advertises.

| | before (pre-#301) | after #307 | now |
| --- | --- | --- | --- |
| status | `409` | `500` | **`200`** |
| body | verbatim `duplicate key…` | redacted | the written entity |

The write failed in both earlier columns; only its status moved, because #301
removed the substring heuristic that had been matching `duplicate` in the driver
text. The functional bug predates #307.

Since #314 that `200` is conditional on the payload satisfying the entity's JSON
Schema. `transform.NormalizeDottedKeys` merges the two spellings into one
document for the validator, applying keys in sorted order so the literal spelling
lands last — the same last-wins rule the writer applies — and the winning value is
type- and constraint-checked like any other. A create carrying a duplicate
spelling whose surviving value violates the schema now answers `400`. The update
case in the paragraph above is unaffected by default, since updates are
report-only unless `VALIDATE_UPDATES_STRICT` is set.

`transform.dedupeEAVRecords` now resolves the duplicate spellings before the
records leave `ToAttributes`, keeping the **last** spelling — the same
duplicate-key rule `encoding/json` applies. This is deterministic rather than
map-order dependent: `flattenToAttributes` sorts each map's keys, and for any
dotted name the nested spelling's top-level key is a proper prefix of the literal
one, so it sorts first and the literal key's records — the caller's explicit
value — are emitted last.

**The unit of replacement is the whole logical attribute, not the primary key.**
When one spelling wins, every record the losing spellings produced for that
attribute is discarded — all array indices, and the empty-list marker. Collapsing
per `(schema_id, row_id, attr_id, array_indices)` would look right for scalars
and be silently wrong for lists: a stored `["old0","old1"]` replaced by a literal
`["new0"]` collides only at index 0, so `old1` would survive into a list the
caller replaced; and a literal `[]` emits only the marker row (`array_indices`
empty, both value columns `NULL`), which collides with no element index at all,
so the clear would persist nothing. Both would answer `200` with stale rows —
quietly wrong, where the duplicate-key failure was at least loud. To tell the two
spellings apart, `flattenToAttributes` tags each record it emits with the
concrete key path that produced it; `strings.Join(path, ".")` cannot serve as
that tag, because collapsing `["contact","emails"]` and `["contact.emails"]` into
one name is exactly the ambiguity the tag has to resolve. The tag is flatten-time
provenance and never reaches `model.EAVRecord`.

A residual primary-key collision *within* one spelling is still collapsed
last-wins. That is a backstop that keeps the slice insertable, not a policy: one
spelling cannot legitimately emit a key twice, since JSON objects have unique
keys and array indices are unique per list.

**This converts a failure into a success**, which is the widest kind of contract
change in this document. A client that treated a duplicate-attribute write as
rejected now gets an accepted write carrying the literal key's value — the whole
of it, including a shortened or emptied list.

`insertEAVAttributes` additionally routes its `tx.Exec` error through
`classifyPgError`, matching the two `entity_main` sites. Any residual `23505` the
dedupe cannot reach — a concurrent writer racing the same row, which could not be
settled without a live database — therefore answers `409` with the published
conflict summary rather than a redacted `500`.

### Status change: service-wiring guards are now 500, not 400 (#313)

Four guards in `internal/entity_query_service.go` — `entity manager config is
required` and `entity query service is not initialized`, at the head of `Query`
and `CrossSchemaSearch` — used to wrap `forma.ErrInvalidInput` and answer
`400`. Nothing about the request is wrong there: they report a mis-wired
service, which is precisely the operator-fault class. They are plain errors
now, so they answer a redacted `500`, matching the crud service's existing
wiring guard. Unreachable through `factory`-wired production servers; visible
only to embedders that construct the service partially. Pinned by
`TestQueryServiceWiringGuardsAreNotClientErrors`.

### Log levels are contract

Three levels, decided by what the body withheld (`respondErrorWithStatus`):

| branch | level | why |
| --- | --- | --- |
| redacted (any status) | `Errorw` | the body carries no text; the log line is the operator's only copy, and production runs at Info (`cmd/server/main.go`) |
| disclosed 4xx, no withheld detail | `Debugw` | the caller already has everything; client mistakes must not page anyone |
| disclosed 4xx with withheld detail (`forma.HasOperatorDetail`) | `Warnw` | the boundary just withheld text whose only remaining copy is this line — it must clear the Info threshold, but `Errorw` would hand callers an alert trigger they can pull at will |

The standing hazard this creates: a disclosed 4xx that withholds detail carries
**no `error_id`** for the caller to quote back (correlation fields are a
redacted-branch shape, pinned by six assertions). If withheld-detail 4xxs turn
out to need operator correlation in practice, adding `error_id` to disclosed
bodies is a separate contract change — recorded as a follow-up candidate, not
done here.

### Known gap

Credentials still reach error strings *inside* the process. `redactCredentials`
scrubs them at the HTTP boundary, so nothing served or logged by
`internal/httpapi` carries one — but a Go embedder using
`factory.NewEntityManager*` receives the raw `error` and can capture the password
in its own logs. Scrubbing at the engine's error wraps is tracked by #306.

The same applies to publications: `PublicMessage()` is scrubbed at the HTTP
boundary as defence in depth, but an embedder reading it directly gets the
wrap site's text as authored.

## Message style

Use explicit messages that name:

- the logical value type
- the column or attribute that is wrong
- the expected state

Examples:

- `storage type mismatch for numeric: value_text should not be populated (expected value_numeric)`
- `missing required attribute 'name' (attrID=1) in EAV records`
- `attribute 'age' (attrID=2): invalid value: cannot convert string to float64`

For client errors, the published message is the whole body the caller sees, so
the same rules apply to it directly. Anything an operator needs but a caller
cannot act on — internal ids, driver detail, third-party render prose — goes
behind `forma.WithOperatorDetail`, which keeps it in `Error()` and the log
while withholding it from the publication.
