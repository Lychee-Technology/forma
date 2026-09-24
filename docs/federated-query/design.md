# **Design Document: Federated Query Engine for Hybrid EAV/OLAP Architecture**

## **1. Executive Summary**

This document defines the architecture for a **Federated Query System** designed to bridge the gap between transactional flexibility (PostgreSQL EAV) and analytical performance (S3 Parquet).

The system utilizes **DuckDB** as a stateless, read-only compute engine to execute **Merge-on-Read** operations. It unifies historical data stored in S3 (Cold/Warm) with real-time buffered data in PostgreSQL (Hot), ensuring sub-second data freshness while leveraging columnar storage performance for complex filtering and sorting.

## **2. System Architecture**

The architecture implements a **Real-Time Lakehouse** pattern. DuckDB acts as the virtualization layer, exposing a unified wide-table view of the underlying disparate storage engines.

### **2.1 Core Components**

1. **Search API / Orchestrator:**
   * Parses incoming JSON DSL.
   * Determines query routing (OLTP vs. OLAP vs. Hybrid).
   * Manages the lifecycle of the DuckDB connection.
2. **Query Translator:**
   * Converts JSON filter trees into dialect-specific SQL fragments (DuckDB SQL vs. PostgreSQL SQL).
   * Manages Schema Mapping (Logical JSON Path $\leftrightarrow$ Physical EAV Columns).
3. **Compute Engine (DuckDB):**
   * Embedded, stateless SQL engine.
   * Extensions: postgres_scanner, httpfs (S3).
   * Configuration: Read-Only, Memory-Limited.
4. **Storage Layer:**
   * **S3 (Parquet):** Flattened, columnar data (Base + Delta files).
   * **PostgreSQL (Row):** change_log (Buffer pointer), entity_main (Fixed attributes), eav_data (Dynamic attributes).

## **3. Data Consistency Model**

To satisfy **Requirement 1.4 (Last-Write-Wins)** and **Requirement 3.1 (Real-time Integration)**, the system implements a strict **Anti-Join Strategy**.

### **3.1 Data Tiers & Definition**

| Tier | Source | Condition | Characteristics |
| :---- | :---- | :---- | :---- |
| **Hot (L0)** | PostgreSQL | change_log.flushed_at = 0 | Mutable, High IOPS, Row-oriented. |
| **Warm (L1)** | S3 (Delta) | Parquet in /delta/ path | Immutable, Small files, Recent history. |
| **Cold (L2)** | S3 (Base) | Parquet in /base/ path | Immutable, Large files (Compacted). |

### **3.2 The Anti-Join Logic**

Mere timestamp comparison is insufficient due to potential clock skew or race conditions. We treat the PostgreSQL "Dirty Buffer" as the source of truth for record existence.

Formula:

$$Result = (S3_{Data} \notin DirtySet) \cup PG_{HotData}$$

* **DirtySet:** The set of row_ids currently present in the PostgreSQL change_log with flushed_at = 0, widened by the **flush-visibility grace** (#252): rows marked flushed at or after the instant this query resolved its parquet path set (minus the `FlushVisibilityGraceMs` clock-skew margin, default 0) also count as dirty — inclusive, because millisecond stamps cannot order a mark and a path resolution landing in the same tick. Because the flush appends the manifest before marking, a row flushed before path resolution already has its delta listed — the widening therefore catches exactly the rows racing this query (their delta may be missing from the resolved set) and keeps them hot-readable, while the steady state is never widened. The widening renders only in the HasHot tier form: it is safe solely because pg_source serves the discarded rows, so hot-excluded queries keep the strict flushed_at = 0 barrier.
* Any record found in S3 that also exists in the DirtySet is **discarded** immediately during the read phase, regardless of its timestamp.
* **Flush ordering (#252):** the CDC flush publishes `copy tmp→final → manifest-append → mark-flushed`, so a batch is never simultaneously absent from the hot tier and from the manifest-listed delta set. The listed-but-unmarked middle state is resolved by this anti-join: the S3 copies of still-dirty rows are discarded and the hot versions serve.

## **4. Query Translation Layer**

The **Query Translator** is responsible for "Dual-Path Translation" to enable **Predicate Pushdown (Requirement 4.1)**.

### **4.1 Input DSL (JSON)**

The federated query path is activated by including a `"federated"` block on the standard `QueryRequest` payload:

```JSON
{
  "schema_name": "trade",
  "page": 1,
  "items_per_page": 50,
  "condition": {
    "l": "and",
    "c": [
      { "a": "symbol", "v": "eq:AAPL" },
      { "a": "trade_type", "v": "eq:2" }
    ]
  },
  "federated": {
    "enabled": true,
    "preferred_tiers": ["hot", "warm", "cold"],
    "prefer_hot": false,
    "use_main_as_anchor": true,
    "s3_parquet_path_template": "s3://bucket/prefix/{{.SchemaID}}/base/*.parquet, s3://bucket/prefix/{{.SchemaID}}/delta/*.parquet",
    "allow_partial_degraded_mode": true,
    "consistency_mode": "strict",
    "include_execution_plan": true
  }
}
```

When the federated block is absent or `enabled` is false, the query executes on the standard PostgreSQL-only OLTP path.

### **4.2 Translation Output**

The translator must traverse the filter tree and generate two distinct SQL fragments:

**A. PostgreSQL Pushdown Fragment ($PG_WHERE_CLAUSE)**

* **Target:** pg_source `WHERE` clause (DuckDB's postgres scanner pushes it down into the scan).
* **Scope:** Only attributes mapping to entity_main.
* **Syntax:** Physical Column Names.
* **Sanitization:** Strict literal escaping to prevent SQL injection.
* *Example:* (integer_01 > 18 AND text_01 LIKE 'John%')

**B. DuckDB Logical Fragment ($LOGICAL_WHERE_CLAUSE)**

* **Target:** WHERE clauses in CTEs and final projection.
* **Scope:** All attributes (Main + EAV).
* **Syntax:** Logical JSON Paths / Parquet Column Names.
* *Example:* (age > 18 AND name LIKE 'John%' AND tag = 'developer')

### **4.3 Federated Request Controls**

The `"federated"` object carries optional controls that affect execution routing and failure semantics:

| Field | Type | Default | Description |
| :---- | :---- | :---- | :---- |
| `enabled` | bool | false | Routes the query through the federated (DuckDB/S3) path. |
| `preferred_tiers` | []string | `["hot","warm","cold"]` | Ordered list of data tiers to query. **Omitted vs. explicit matters (#468):** an omitted list is the default all-tier form and is subject to the routing cost heuristics (under the default `hybrid` strategy every page of `items_per_page < 1000` — so every API page — is answered from Postgres alone, and the response carries `partial: {reason: "hot_tier_only", unconsulted_tiers: [...]}`). An explicit list naming any tier beyond `hot` is a coverage declaration that overrides those heuristics and reaches the federated path, as a keyset cursor does (#354). `["hot"]` is the hot-only gate, same as `prefer_hot`. |
| `prefer_hot` | bool | false | Strong preference for Postgres hot tier. |
| `use_main_as_anchor` | bool | true | Use entity_main as the anchor for predicate pushdown. |
| `s3_parquet_path_template` | string | — | Template for locating Parquet files in S3. Wins over the server's manifest-driven resolution (§4.3.1). **Disabled by default (#456):** honored only when the server sets `duckdb.allowCallerParquetPaths`, and only for paths inside the configured `duckdb.s3Bucket` (under `duckdb.s3DataPrefix` when set) that obey the fallback glob's shape rules — no `**`, wildcards only in the object-name segment, no `_tmp/` segment (#477); otherwise the request is rejected (`ErrInvalidInput` → 4xx). |
| `allow_partial_degraded_mode` | bool | false | Permit execution with a subset of available tiers. |
| `consistency_mode` | string | `"strict"` | Freshness/availability contract (`"strict"` or `"eventual"`). |
| `include_execution_plan` | bool | false | Attach diagnostic execution plan to the response. |

**Consistency modes:**

* **`strict`** (default): Requires PostgreSQL availability for dirty-set evaluation and hot-tier reads. Federated queries fail when PostgreSQL is unreachable.
* **`eventual`**: Permits S3-only degraded execution when PostgreSQL is unavailable, accepting possible ghost reads (deleted data reappearing) and missing hot-tier rows. Suitable for best-effort analytics where bounded staleness is acceptable.

These controls are part of the request payload; they are not conveyed via HTTP headers.

### **4.3.1 Parquet Path Resolution and Manifest-Driven Reads**

`$S3_PATHS` (§5) is resolved once per query, before the DuckDB template renders. Four levels, first match wins:

1. **Per-request hint** — `federated.s3_parquet_path_template`, rendered for the query's schema (comma-separated templates become a path list). An explicit hint directs `read_parquet` at exactly the requested location and, when honored, always wins. **This level is disabled by default (#456):** the hint is a caller-controlled scan target, so it is honored only when the server opts in with `duckdb.allowCallerParquetPaths` (`DUCKDB_ALLOW_CALLER_PARQUET_PATHS`), and then only for rendered paths that fall inside the configured `duckdb.s3Bucket` (exact `s3://<bucket>/…` prefix; local, `http(s)://`, and cross-bucket targets are rejected). **In-bucket scope is further narrowed (#477):** when `duckdb.s3DataPrefix` is set, the path must sit under `s3://<bucket>/<s3DataPrefix>/…` (the prefix is canonicalized with the same single trailing-`/` strip the writers and the level-3 glob apply; an empty prefix keeps bucket granularity), and every honored path must obey the shape rules the level-3 glob is held to — `**` is refused, wildcards (`*`, `?`, `[`) may appear only in the final (object-name) segment so a pattern can never match a directory, a literal `_tmp` segment is refused, and a path ending in `/` is refused. Together these keep in-flight `_tmp/` staging objects unreachable from a hint. The configured `s3DataPrefix` is spliced verbatim into that scope and the shape rules inspect only the key beneath it, so with the opt-in on the prefix itself must not contain a glob metacharacter (`*`, `?`, `[`) — `data/*` would hand every hint a directory wildcard the caller never wrote, and `data/**` would reach `_tmp/` outright; `ValidateCallerParquetPaths` rejects such a prefix at startup, and the engine refuses every hint on a config that skipped validation. With the opt-in off, or for an out-of-scope or ill-shaped path, the hint is rejected as invalid input (`forma.ErrInvalidInput` → 4xx) rather than ignored — the request never silently falls through to the manifest source. **A hint that fails to render — or that renders to no usable path at all (`","`, whitespace-only) — is likewise invalid input** and never falls through: silently serving a different path set than the caller asked for would misreport the answer. Enabling the opt-in requires a non-empty `duckdb.s3Bucket`; the combination is rejected at startup by `DuckDBConfig.ValidateCallerParquetPaths`, called from both the `cmd/server` bootstrap and the factory preflight beside `ValidateManifestRead` (and also from `Config.Validate`), consistent with the manifest-read bucket requirement.
2. **Manifest source** — when the server is configured for manifest reads, the schema's manifest is loaded and its file entries become the scan set as full `s3://` URIs. Reads then scan exactly the listed objects rather than expanding a storage glob, which is what makes cold-tier loss detectable (§4.3.1 *Inconsistency detection*). The loaded manifest must carry the requested schema's `schema_id` stamp (`manifest.VerifySchemaStamp`, shared with every writer): a stamp naming another schema fails the read with `ErrManifestSchemaMismatch`, and a zero stamp over any entries fails it with `ErrManifestUnstamped` (#522) — a zero stamp is not a legacy format, since every Forma writer has always stamped the field, so it proves nothing about which schema owns the entries, and the two-probe template check cannot see a template that collides only at schema IDs it never renders. An empty zero-stamped manifest loads as empty (stamped in memory) and takes level 3 like any empty manifest. **Manifest path contract (#516).** A relative `path` on a manifest entry *is* the bucket-relative object key, **verbatim**: every consumer renders or compares it byte for byte — the read path prefixes it as `s3://<bucket>/<path>` (`manifest.QuerySource.Paths`), the missing-object probe cuts exactly that prefix back off (`QuerySource.MissingIn`), `manifest-reconcile` matches it against the listing unchanged (`cdc.NormalizeObjectKey`), and compaction hashes, merges and deletes the same key (`Compactor.objectURI` / `bucketRelativeKey`). The producers hold to the same rule: the tmp→final promotion every writer goes through (`cdc.CopyTmpToFinal`, used by `cdc-flush`, `cdc-init` and the compactor) names the tmp key the exporter wrote in its `CopySource` (`<bucket>//1/_tmp/<uuid>.parquet` under an empty prefix), so the object the exporter wrote is the one that gets promoted. No consumer or producer trims, joins or canonicalizes the key. The one transformation that is not a change of key is transport encoding: S3 URL-decodes the `x-amz-copy-source` header and the SDK sets it verbatim, so `CopyTmpToFinal` percent-encodes the key for the header (`encodeCopySourceKey`) — `/` and RFC 3986 unreserved characters pass through, so every key the `Build*Path` helpers mint under an ordinary prefix is byte-identical on the wire and a leading slash survives, while a prefix carrying `?`, `#`, `%`, `+`, whitespace or non-ASCII is escaped rather than misread as a `versionId` delimiter or a space. The decoded source key S3 acts on is always the logical key; the destination `Key` is a plain SDK field and is never encoded. That matters because the writers' `cdc.Build*Path` helpers strip only a trailing `/` from the data prefix before formatting `<prefix>/<schema>/...`, so an **empty data prefix yields keys with a leading slash** (`/1/<uuid>.parquet`) — the flush stores that literal key in S3 and lists it as-is, and it is a valid key everywhere (the read scans `s3://<bucket>//1/<uuid>.parquet`). Before #516 compaction alone trimmed the slash and so hashed and merged a sibling key that was never written, failing every rewrite of such a schema with a probe error naming the wrong object. Absolute `s3://` entries pass through unchanged (#249 review); those naming a foreign bucket are refused by compaction (#417) and reported as unverifiable by reconcile. The only relative shapes that name no object — `""` and a bare `/` — are refused by compaction's `bucketRelativeKey` rather than passed on; the refusal is relative-only, so the own-bucket URI `s3://<bucket>//` resolves to the key `/` in compaction exactly as it does in `MissingIn` and reconcile, while `s3://<bucket>/` (an empty key) is refused.
3. **Fallback glob** — a schema whose manifest is missing or empty resolves to `s3://{s3Bucket}/{s3DataPrefix}/{schemaID}/*.parquet`, preserving pre-manifest read behavior for never-flushed schemas. "Missing" means the store confirmed the key is absent (`manifest.ErrObjectNotFound`, which `S3Store` derives only from the SDK's typed `NoSuchKey`, never from a message substring or a bare 404 status); any other manifest-load failure — `NoSuchBucket`, access denied, a transport error — fails the read instead of taking the glob, because the glob can never classify inconsistency and would turn a store outage into a silently partial answer (#464). A trailing `/` on `s3DataPrefix` is stripped exactly as the writers strip it (`internal/cdc.BuildDeltaPath`), so `"delta/"` and `"delta"` address the same objects; a prefix of `/` alone disables the level like an empty one. The single `*` does not cross `/`, so in-flight `_tmp/` staging objects stay excluded; it must never be widened to `**`. An empty `s3DataPrefix` disables this level. **The fallback is not a general recovery path for schemas that already have data:** a schema whose parquet exists but whose manifest is empty or absent (writers previously ran with manifests disabled, or the manifest was lost) takes this same glob, and the glob spans exactly one prefix — the configured `s3DataPrefix`. The CLI tools do not share one root by default (`cdc-init --s3-prefix base`, `cdc-flush --s3-prefix delta`, `compactor --data-prefix data`), so objects written under any root other than the reader's `s3DataPrefix` are simply not scanned and their rows go missing without an error. Repair the manifest (`forma-tools manifest-reconcile`, or re-run `cdc-init`) rather than relying on the glob to find the data. The write/GC side of the same misconfiguration is guarded since #463: `manifest-reconcile --gc` refuses to treat live base objects as orphans for a schema whose manifest resolves to zero entries (see `docs/manifest-reconcile.md`).
4. **No paths — the read fails fast, with its own classification.** With no hint, no configured source, and no fallback, the path set resolves empty and the engine rejects the query at resolution with `ErrNoParquetPaths` (`NoParquetPathsError{SchemaID, SourceConfigured}` — see `docs/error-handling.md`), before rendering, before the pre-read schema validator, and before DuckDB is touched. It is **not degradable**: `allow_partial_degraded_mode` does not absorb it, because every query reaching the DuckDB engine wants warm and/or cold data (hot-only and `prefer_hot` short-circuit to Postgres earlier), so a Postgres-only answer would be silently short exactly where the cold tier was requested. The message names the schema and distinguishes "no source configured" from "source consulted, manifest empty, fallback disabled", which have different remedies.

   Note that an empty path set is not a normal state: a never-flushed schema takes the level-3 fallback glob, so reaching level 4 means the fallback is disabled too. Before #299 this case rendered `read_parquet(<no value>)` and died as a DuckDB parser error classified `ErrFederatedReadFailed` — loud, but indistinguishable from a transient S3 outage to any programmatic discriminator, so degraded mode converted a configuration mistake into a quietly incomplete answer. The renderer now also refuses to render an unbound `$S3_PATHS` at all (`sqlgen.requireS3Paths`), so no caller can reconstruct that statement.

**Server configuration.** Manifest-driven reads are configured on `DuckDBConfig` and, for `cmd/server`, from the environment. The `DUCKDB_*` names override the shared names in parentheses; those shared names are **reserved as the single-stack configuration point** — nothing reads them today (the CDC tools take `--manifest-prefix` / `--manifest-template` flags), and they exist so a future runner can drive writer and reader from one value:

| Config field | Env var | Meaning |
| :---- | :---- | :---- |
| `duckdb.s3Bucket` | `DUCKDB_S3_BUCKET` (`S3_BUCKET`) | Bucket holding both the parquet objects and the manifests that index them. Required whenever `manifestTemplate` is set. |
| `duckdb.s3DataPrefix` | `DUCKDB_S3_PREFIX` (`S3_PREFIX`) | Mirrors the CDC write side's parquet prefix: the reader's parquet root. Builds the level-3 fallback glob and, when `allowCallerParquetPaths` is on, narrows level-1 hints to `s3://<bucket>/<prefix>/…` (#477). |
| `duckdb.manifestPrefix` | `DUCKDB_MANIFEST_PREFIX` (`MANIFEST_PREFIX`) | Root prefix for manifest objects. Must match the CDC/compaction write side. |
| `duckdb.manifestTemplate` | `DUCKDB_MANIFEST_TEMPLATE` (`MANIFEST_TEMPLATE`) | Per-schema manifest path template, e.g. `manifest/{{.SchemaID}}.json`. **Non-empty is the single enable gate** for manifest reads. |
| `duckdb.allowCallerParquetPaths` | `DUCKDB_ALLOW_CALLER_PARQUET_PATHS` | Opt in to honoring the per-request `federated.s3_parquet_path_template` hint (§4.3.1 level 1). Default `false`: the hint is rejected. When `true`, rendered hint paths must fall inside `duckdb.s3Bucket` (under `duckdb.s3DataPrefix` when set) and obey the level-3 glob's shape rules — no `**`, wildcards only in the object-name segment, no `_tmp/` (#477); enabling it without a bucket fails at startup (#456), as does enabling it with an `s3DataPrefix` containing `*`, `?`, or `[` (#526 review). |

* **Env vars are read only when `DUCKDB_ENABLED` is truthy:** with DuckDB off, `cmd/server` returns the base config untouched, so setting `DUCKDB_MANIFEST_TEMPLATE` alone is a silent no-op — not a startup failure and not a behavior change.
* **Gate:** the source is built only when `duckdb.enabled` *and* `manifestTemplate` is non-empty. All four fields default to empty, so an upgrade never flips an existing deployment from glob reads to manifest reads on its own.
* **Shared prefix names are all-or-nothing with the template.** `S3_PREFIX` and `MANIFEST_PREFIX` are adopted **only when the effective `manifestTemplate` (resolved first, from `DUCKDB_MANIFEST_TEMPLATE` then `MANIFEST_TEMPLATE`) is non-empty**; otherwise they are ignored and the field stays at its base value. Without this, a stack that already exports `S3_PREFIX` for its CDC tooling and merely sets `DUCKDB_ENABLED=1` would produce a prefix-without-template config — precisely the inert combination the startup validation rejects — and would stop booting on upgrade. `S3_BUCKET` carries no such condition: a bucket alone is never inert. The explicit `DUCKDB_S3_PREFIX` / `DUCKDB_MANIFEST_PREFIX` names bypass the condition and are **always** adopted, so an operator who names one by hand without a template gets the startup rejection rather than a silently dropped value.
* **Must match the writer:** `manifestPrefix` / `manifestTemplate` must be identical to the CDC and compaction write side, and `s3DataPrefix` must match the writers' parquet prefix. A reader pointed at a manifest path the writers never use resolves an empty manifest for every schema and falls back to the glob — or, without `s3DataPrefix`, resolves no paths at all — then every read that *reaches the DuckDB engine* fails per level 4 with `ErrNoParquetPaths`, while PG-routed reads (hybrid routing's hot-only and *cursor-free, tier-implicit* small-page cases, which never render the DuckDB template — since #354 a small-page read carrying a keyset cursor is rerouted onto DuckDB and does render it, and since #468 so is one carrying an explicit multi-tier `preferred_tiers`) keep returning hot-tier-only rows; since #468 they are at least marked `hot_tier_only`, so the shortfall is visible if not explained. A level-4 misconfiguration is therefore loud only for the queries that were going to touch the cold tier. **The template is probe-validated at startup** so the most common way to point the reader at a path the writers never use — a field-name typo such as `{{.SchemaId}}`, which parses but renders `<no value>` for every schema — is rejected before the server boots, along with any template that does not vary by schema.
* **Invalid configuration fails at startup**, and the failure is fatal to server construction (a half-configured read surface would silently drop the cold tier). Where exactly it fails differs by entry point: `cmd/server` resolves and validates the DuckDB config as its first act, **before it opens any connection**, so a bad value never reaches the database; programmatic callers of `factory.NewEntityManagerWithConfigContext` are validated at the top of the factory, **before the factory's own I/O** (table collection, metadata load) but after they have built the pool they pass in. Rejected combinations: `manifestTemplate` set without `s3Bucket`; `manifestPrefix` or `s3DataPrefix` set without `manifestTemplate` (they would sit inert while reading to an operator as "manifest reads are on"); a `manifestTemplate` that is not a parsable `text/template`; **any of the four fields carrying leading or trailing whitespace** — the write side never trims them, so a padded value would resolve a different object key on each side, and the divergence would surface only as a silently empty cold tier (byte parity with the writer is the contract, not reader-side normalization); `allowCallerParquetPaths` enabled without `s3Bucket`, or with an `s3DataPrefix` containing a glob metacharacter (`*`, `?`, `[`).
* **Endpoint addressing:** when `s3Endpoint` is set, the manifest client uses path-style addressing, mirroring the `s3_url_style='path'` DuckDB httpfs receives — both must address the same objects the same way.
* **Boundary — no startup bucket probe:** the S3 client is constructed at startup but not exercised (no `HeadBucket`). A mistyped bucket, a wrong endpoint, or bad credentials therefore surface as **query-time** read failures, not as a startup failure.

**Inconsistency detection.** `ErrParquetSetInconsistent` (see `docs/error-handling.md`) can only ever trigger on **level 2** paths. A failed read is classified by probing the exact scanned set for listed-but-absent objects; that classification is skipped when the paths came from a per-request hint or when no source is configured, and the probe additionally skips glob entries (a glob's absence is unprovable), so a level-3 fallback can never classify. A deployment reading via globs alone therefore keeps the pre-manifest failure mode — a shrinking glob silently returns fewer rows.

**Migration note — the silent-loss window.** The write side is already on for most deployments: both `forma-tools cdc-flush` and `forma-tools compactor` default `--manifest-template` to `manifest/{{.SchemaID}}.json`. CDC gates manifest writing on that template being non-empty; compaction does not gate at all — its manifest provider always loads and saves. A schema whose manifest is confirmed absent (never flushed) compacts to `noop` rather than failing the run; any other manifest-load failure — `NoSuchBucket`, access denied, a transport error — fails it, by the same typed classification the read side uses (#524). Since #520 every writer refuses a manifest stamped for another schema and rejects a collapsed `--manifest-template` at startup, so a mis-pointed template fails loudly instead of cross-contaminating manifests. Either way, a stack running the CLI tools with default flags **has been writing manifests all along**. The read side is off by default. Upgrading without setting `DUCKDB_MANIFEST_TEMPLATE` (plus `DUCKDB_S3_BUCKET`) is therefore not a no-op-by-choice: the deployment stays in the pre-existing window where a lost cold-tier object degrades to a silently short result set instead of a loud, classified error. Turn the read side on with the same prefix/template values the flusher uses.

An upgrade is nevertheless boot-safe in both directions. Setting `DUCKDB_ENABLED=1` with only the shared CDC vars exported (the single-stack `.env` shape) leaves both prefix fields empty per the all-or-nothing rule, so the config validates and the server starts with the pre-existing glob read path. Setting `DUCKDB_MANIFEST_TEMPLATE` is what flips the deployment to manifest reads, and it pulls the shared prefixes in with it — one variable, one behavior change, no half state.

### **4.4 Attribute → Column Naming Contract**

Attribute names are logical and may contain dots (nested JSON objects flatten
to `contact.annualIncome`). Physical parquet columns and every unified-CTE
column use the folded form produced by `sqlgen.ParquetAttrColumn` (dots and
spaces → underscores, backticks/brackets stripped): `contact_annualIncome`.

The fold is shared by the CDC exporter (write side) and the federated SQL
generator (read side) and cannot diverge: the logical WHERE clause is applied
both against raw `read_parquet` output (physical columns) and against the
`visible` CTE (unified columns), so both must expose the same names (#260).
The fold is lossy, so both sides fail fast if two attributes of one schema
collide on the same folded column. `sqlgen.ValidateParquetAttrColumns`, the
registration guard behind that, compares **case-insensitively** for the same
reason the keyset and filter guards below do: DuckDB resolves an unquoted
identifier without regard to case, so `Created_At` reaches the `created_at`
system column and `Contact_Name` reaches the same column as `contact_name`
(#532). The emitted names stay byte-stable either way — `ParquetAttrColumn`
preserves the caller's spelling, and so do the diagnostics, which name the
resolved lower-case column beside it. This is what makes the premise stated
under the filter rule below — "registered names never reach it" — hold for
case variants and not merely for exact-case ones.

That case-insensitivity is **ASCII-only**, because DuckDB's is: the engine
folds `A`–`Z` and nothing else, so `Á` and `á`, `Ж` and `ж`, and `cafÉ` and
`café` are distinct identifiers to it, and U+212A KELVIN SIGN does not
resolve onto `k`. The guard therefore keys on `sqlgen.DuckDBFoldIdentifier`,
not on `strings.ToLower`: Go's Unicode case mappings would merge every one of
those pairs and reject a schema DuckDB serves correctly, and because they also
map U+0130 (`İ`) onto `i`, they would read the legitimate attribute `row_İd`
as the reserved `row_id`. Since a registration failure fails the whole
registry, a false rejection here is a boot failure, so the boundary is pinned
against the engine itself rather than its documentation
(`TestDuckDBIdentifierFoldIsASCIIOnly`). The two runtime guards below key on
the same primitive, with `sqlgen.DuckDBEqualFold` as its `strings.EqualFold`
counterpart, so one identifier fold backs every seam of this contract (#550).
Before that they folded with `strings.ToLower`, which refused a filter on
`row.İd` as the reserved `row_id` and, on the keyset side, read a cursor
column `row_İd` as the system column under an identity fold and admitted it
past the identifier barrier; DuckDB keeps both distinct from `row_id`. Each
guard is pinned against the engine the same way
(`TestValidateUnregisteredParquetAttrColumn_FoldsLikeDuckDB`,
`federated.TestKeysetCursorFoldsIdentifiersLikeDuckDB`). The one comparison
outside `sqlgen`'s reach — `model.KeysetCursor.ValidateShape` matching the
trailing tiebreak with `strings.EqualFold` against the literal `row_id` — is
exactly the ASCII fold for that literal, because no non-ASCII rune sits in
Go's simple case-fold orbit of `r`, `o`, `w`, `i` or `d`.

**Retired entries are in scope for the collision half only.** The
registration guard runs over the full attribute cache before
`activeAttributeCache` strips retired entries (#342), and its two halves
treat a retired entry differently because the two hazards live in different
places (#549). A fold collision is physical: `read_parquet(...,
union_by_name=true)` resolves case-variant column names from different files
onto one column, and the compaction merge's `SELECT *`
(`internal/compaction/merge_sql.go`) rewrites them as one, so a retired
attribute's flushed column interferes with an active or another retired
attribute's column whether or not anything projects it
(`sqlgen.TestParquetScan_FoldCollisionMergesAcrossFiles`). Such a pair is
refused, and the diagnostic names the retired side as the attributeID ledger
with its id and valueType: renaming it in place would desynchronize the
ledger from the flushed data and from the #294-preserved EAV rows its id
still owns, so a retired/active pair is told to rename the active attribute
and a retired/retired pair to migrate the flushed column and the ledger entry
together, keeping the id. A reserved-column hit is a projection hazard
instead. No flushed file holds two spellings of a system column: the CDC
writer emits `ltbase_created_at`, never `created_at`, and DuckDB
deduplicates a case-variant of a physical export column (`Row_Id` beside
`row_id`) to `Row_Id_1` at COPY time. The ambiguity arises only when the
attribute column is projected beside the system column — in the exporter's
SELECT list and in `s3_source`, `pg_source` and the outer select — and every
one of those projections is built from the active cache, so an active
`Created_At` is refused and a retired one is exempt: it sits unreferenced in
the raw `SELECT *` scans of the cold-scan source and the compaction merge,
and binds nothing
(`sqlgen.TestParquetScan_RetiredReservedColumnIsInert`,
`compaction.TestDuckMerger_CarriesRetiredReservedNameColumnsThrough`).
Failing boot for it, as the guard did between #548 and #549, imposed a
migration that fixed nothing. Aggregating every failing schema into one
startup report, so an operator sees the blast radius in one pass, is tracked
separately (#604).

Keyset cursor columns obey the same contract as every other column reference,
and it is one contract, not a per-seam one (#381). A single validator,
`federated.validateKeysetCursor`, binds every entry point onto the keyset
renderer — `DBFederatedQueryEngine.Query`, `ExecuteFederatedPaginatedQuery`, and
the exported `ExecuteDuckDBFederatedQuery` beneath them — and admits
a column when it is one of the four system columns the `visible` CTE projects
(`row_id`, `created_at`, `ver_ts`, `deleted_ts`) or an attribute whose
`ParquetAttrColumn` fold is a bare SQL identifier. Cursor columns are emitted
folded by `generateKeysetWhereClause` and `buildKeysetOrderBy`, so a cursor over
`contact.annualIncome` references `contact_annualIncome`, the name the CTE
actually exposes. Two columns of `visible` are refused by name despite being
bare identifiers — `rn` and `source_tier_priority`, the dedup machinery, on
which a cursor would bind and then paginate over the dedup rank — as is any
name that folds onto `ParquetAttrColumn`'s `attr` placeholder, whether because
the fold empties the name or because it strips the name down onto that literal.
The reject set, the system-column set and the `attr` placeholder are all matched
on the folded name **case-insensitively**: DuckDB resolves an unquoted identifier
without regard to case, so `RN` reaches the same dedup column as `rn`, `ROW_ID`
the same system column as `row_id`, and a folded `Attr` the same column as
`attr`. The placeholder's exemption is folded case-insensitively too, so an
attribute genuinely named `Attr` — which folds to its own name — stays admitted
while `[Attr]` is refused.

Case is the whole of that latitude on the system-column set. A name the fold
merely **transforms** onto one of the four — `created.at` onto `created_at`,
`ver.ts` onto `ver_ts`, `[row_id]` onto `row_id` — is refused, because the
generator would emit the real system column and answer a page ordered and
filtered on a key the caller never named. Schema registration does not stand in
for that check: it rejects a registered *attribute* whose fold collides with a
reserved parquet column, but a cursor column is an arbitrary string that was
never registered.

Five rules travel with the cursor type itself
(`model.KeysetCursor.ValidateShape`), so `internal/sqlgen` can enforce them
without reaching into `internal/federated`. Two govern the columns: `Values`
must align one-for-one with `Columns` — a short slice used to bind SQL NULL and
return a silently empty page — and the final column must be `row_id`, the
trailing tiebreak that makes each page boundary resolvable (#183). That last
comparison is case-insensitive for the same reason the sets above are, and no
more so: `ROW_ID` is the tiebreak, `row.id` is not.

The other three close the same silent-answer family from the value and
direction side. No boundary value may be `nil`: alignment alone does not stop
one, and a nil binds the very SQL NULL an unfilled arm did, so the comparison
is unknown and every row tied at the boundary silently drops (a typed nil such
as `(*string)(nil)` binds NULL too, and is refused with the untyped one).
`Mode` must be `after`, and each column's `Direction` must be `asc`, `desc`,
or empty for the documented `asc` default. Both enums are matched
**byte-exactly**, unlike `row_id`: a mode and a direction are Go constants the
renderer compares exactly and never identifiers DuckDB resolves, so admitting
`After` or `DESC` would hand the renderer a spelling it reads as the
fall-through default — *before*, and ascending — and the page would come back
successfully in the wrong direction. A caller-facing decode boundary that
accepts other spellings normalizes them before building the cursor, as
`normalizeSortOrder` does for the sort surface.

`before` is declared (`model.KeysetCursorModeBefore`) but refused as well,
until #513 lands its other half. The renderer flips the comparison operator
for a `before` cursor but still fetches in the **forward** order under the
`LIMIT`, so `key < 7 ORDER BY key ASC LIMIT 2` answers `[1, 2]` — the start
of the prior range — where a caller stepping backwards expects `[5, 6]`;
descending order has the symmetric defect. A backward page needs the reversed
order under the `LIMIT` and the requested order restored outside it. Until
that exists, a `before` cursor is a successfully-answered wrong page, and is
refused for the same reason an unset mode is.

An inactive cursor is exempt from all five: the open first page carries no
continuation obligation. `IsActive` is the shared spelling of that predicate,
used by every site that decides whether the clause is rendered and every site
that decides whether a cursor is honoured or refused.

**A cursor continues the request's order; it does not replace it.** The keyset
`ORDER BY` renders from the cursor's columns alone, and the renderer never
reads `AttributeOrders` while a cursor is active, so without a rule a request
sorted on `count ASC` and continued with a valid `created_at DESC, row_id ASC`
cursor answered a page ordered and filtered on `created_at`.
`model.KeysetCursor.ValidateContinuation(orders)` closes that: when the request
resolved `AttributeOrders`, the cursor must be exactly those orders — same
attribute name at each position, matched byte-exactly, same direction —
followed by an ascending `row_id`, because `row_id ASC` is the tiebreak the
non-keyset `ORDER BY` appended to page one. A wrong attribute, a flipped
direction, a `row_id DESC` tiebreak, or a longer or shorter cursor is refused.
Every seam judges the order it is about to render (`fq.AttributeOrders` at
`Query`, the `attributeOrders` argument elsewhere), and
`sqlgen.injectDuckDBTemplateParams` repeats the check against the query it
renders, so a direct `sqlgen` caller cannot bypass it either.

When the request resolved **no** `AttributeOrders`, the cursor is honoured as
written. That boundary is deliberate: the default `created_at DESC, row_id
ASC` is the renderer's fallback rather than a caller statement, and
`AttributeOrders` cannot express a system-column order at all (sort keys
resolve against the schema; `created_at` is not a schema attribute). A
cursor-only walk — an open first page bounded by a far-future `created_at`,
then cursors on `created_at` in either direction with a `row_id` tiebreak in
either direction — is a complete order specification with nothing to
contradict, and it is how the production e2e walks and the federated
benchmark page. Requiring an order-less request to match the default would
make every `created_at` / `ver_ts` / `deleted_ts` cursor unreachable.

**The explicit `limit`, `offset` and `attributeOrders` arguments are the
pagination contract of every DuckDB seam.** The advanced template reads
`LIMIT`, `OFFSET` and the non-keyset `ORDER BY` from the *query object*
(`q.Limit`, `q.Offset`, `q.AttributeOrders`), so a caller that normalised or
clamped its limit but passed the query unchanged used to have the clamp
silently ignored: the keyset branch of `ExecuteFederatedPaginatedQuery`
rendered `LIMIT 0` for a zero `fq.Limit` and an over-`MaxRows` `fq.Limit`
verbatim, with only its in-memory slice honouring the clamp.
`buildDuckDBQueryWithPlan` — the one point the direct render and the compiled
plan cache both flow through — now renders a copy of the query carrying the
dispatched arguments (`federated.dispatchedQuery`), so the rendered skeleton,
the shape hash and the plan scope all see the query that was dispatched.
`engine.Query` passes the query's own fields and is unaffected.

The validator does not check that an attribute is registered in the schema:
that needs the metadata cache, and an unregistered but well-formed name fails
loudly at DuckDB's binder rather than silently — which holds without exception
only because a fold onto a `visible` column, system or dedup, is refused before
it can reach the generator. It is deferred to the caller-facing cursor surface
the Postgres-side keyset feature introduces.

Filter attributes follow the same fold rule (#512). The predicate normalizer
does not require a filter attribute to be registered either: the PG EAV
payload refuses an unregistered name with `attribute not found in cache`, and
because `ToDualClauses` renders that payload before the DuckDB one, the
federated route answers 4xx before any DuckDB clause exists. The DuckDB payload
is nevertheless guarded on its own, so the rule holds whatever emitter order a
caller uses: for an unregistered name, `sqlgen.ValidateUnregisteredParquetAttrColumn`
refuses a non-identity fold onto the `attr` placeholder or onto any reserved
parquet column, refuses `rn` and `source_tier_priority` under any spelling, and
admits an identity-up-to-case fold, exactly as `validateKeysetCursor` does for
cursor columns. Registered names never reach it: `ValidateParquetAttrColumns`
already rejected any whose fold collides with a reserved column.

## **5. SQL Execution Template**

This SQL template represents the core logic of the Federated Query Engine.

It is a simplified sketch of the runtime template (`internal/sqlgen/advanced_query_template_duckdb.go`) and is kept executable: `internal/federated/design_doc_sql_test.go` extracts this block, runs it on DuckDB, and pins its scan shapes, source aliases, and LWW/filter semantics — update that test when editing this section.

```SQL

-- 1. Configuration
PRAGMA memory_limit='4GB';
PRAGMA threads=4;

-- 2. Define Query Parameters (To be interpolated by the Host Application)
-- $SCHEMA_ID:       Integer (e.g., 1)
-- $PG_CONN:         String (Postgres Connection String)
-- $PG_WHERE_CLAUSE: String (Generated Physical SQL for Pushdown)
-- $S3_PATHS:        List (e.g., ['s3://bucket/base/*.parquet'])
-- $FLUSH_GRACE_CUTOFF_MS: BIGINT (the instant this query resolved $S3_PATHS,
--                    minus the clock-skew margin, #252; MaxInt64 disables
--                    the widening — hot-excluded renders omit it entirely)

WITH
-- =========================================================================
-- CTE 1: The Dirty Set
-- Identifies records currently in the transaction buffer.
-- =========================================================================
dirty_ids AS (
    SELECT row_id
    FROM postgres_scan($PG_CONN, 'public', 'change_log')
    WHERE schema_id = $SCHEMA_ID
        AND (flushed_at = 0 OR flushed_at >= $FLUSH_GRACE_CUTOFF_MS)
),

-- =========================================================================
-- CTE 2: S3 Source (Cold & Warm)
-- Reads historical data with the dirty-set anti-join and semijoin pushdown.
-- =========================================================================
s3_source AS (
    SELECT
        row_id,
        -- created_at and ver_ts are DIFFERENT quantities (#460):
        -- ltbase_created_at is the row's creation time, which both exporters
        -- write into every parquet generation; changed_at is the LWW version
        -- stamp. Aliasing changed_at into the created_at slot made this leg
        -- report a different quantity from pg_source's m.ltbase_created_at
        -- through the same UNION ALL column, so created_at was wrong for
        -- parquet-winning rows and the default sort key changed value the
        -- moment a row was flushed.
        ltbase_created_at AS created_at,
        changed_at AS ver_ts,
        deleted_at AS deleted_ts,
        -- Logical Columns (Native in Parquet)
        name,
        age,
        tag,
        1 AS source_tier_priority
    -- union_by_name resolves the schema UNION across parquet generations
    -- (#189): files written before an attribute existed contribute NULL,
    -- and same-named columns widen to the common supertype. Corruption
    -- loudness is preserved by the pre-read system-column invariant
    -- validator (internal/federated/parquet_schema_validation.go) and, at
    -- byte level, by the system-column guard below: a manifest stamp (#256)
    -- can spare an object its footer probe, so a rogue overwrite could put an
    -- object missing row_id or changed_at in this set and have union_by_name
    -- NULL-fill it — NULL row_id drops those rows out of the anti-join, NULL
    -- changed_at misorders the LWW merge. The CAST re-pins BIGINT so a
    -- VARCHAR changed_at cannot widen the union and make ordering
    -- lexicographic. deleted_at is type-pinned but NOT presence-guarded:
    -- pre-#274 legacy delta objects encode live rows as NULL and stay
    -- readable until compaction retires them (#365 residual).
    -- ltbase_created_at is type-pinned AND presence-guarded CONDITIONALLY
    -- (#460): a NULL on a LIVE row is an error, while a tombstone's NULL
    -- passes — hard-deleted rows legitimately carry no creation stamp. The
    -- condition is what makes a presence guard possible here at all, and it
    -- is needed because a trusted manifest stamp (#256) can admit an object
    -- whose real bytes lack the column: union_by_name then NULL-pads it and
    -- a live row would otherwise reach the caller with a NULL created_at,
    -- ordered by its NULL bucket rather than its creation time. Its COLUMN
    -- presence is independently enforced before the read by the parquetcheck
    -- invariant. Attribute columns get the same re-pin SELECTIVELY (#371):
    -- when the pre-read validator sees a column under two parquet types
    -- across the scan set, or under a type other than the current schema's
    -- export type, a `CAST(col AS <type>) AS col` item joins this REPLACE
    -- list so a stale-typed generation cannot widen the union (a VARCHAR
    -- generation of an INTEGER attribute would otherwise make ORDER BY
    -- lexicographic without any error). A healthy set renders no attribute
    -- item, so this text is unchanged for it. Both scan sites render
    -- sqlgen.BuildParquetScanSource, which also appends the #255 typed
    -- NULLs for never-flushed columns.
    FROM (SELECT * REPLACE (COALESCE(row_id, error('parquet scan produced NULL row_id: a scanned object violates the export schema invariant (#189/#256)')) AS row_id, CAST(COALESCE(changed_at, error('parquet scan produced NULL changed_at: a scanned object violates the export schema invariant (#189/#256)')) AS BIGINT) AS changed_at, CAST(deleted_at AS BIGINT) AS deleted_at, CASE WHEN ltbase_created_at IS NULL AND COALESCE(CAST(deleted_at AS BIGINT), 0) = 0 THEN error('parquet scan produced NULL ltbase_created_at on a live row: a scanned object violates the export schema invariant (#189/#256/#460)') ELSE CAST(ltbase_created_at AS BIGINT) END AS ltbase_created_at) FROM read_parquet($S3_PATHS, union_by_name=true)) AS cold_scan
    WHERE
        -- 1. Anti-Join: Exclude if a newer version exists in PG
        row_id NOT IN (SELECT row_id FROM dirty_ids)
        -- 2. Predicate pushdown as a row_id SEMIJOIN: a row qualifies when
        --    ANY of its parquet versions matches; ALL of its versions then
        --    enter dedup so the latest wins BEFORE the real filter below.
        --    Filtering versions directly here drops newer non-matching
        --    versions pre-dedup and resurrects stale rows (#173/#178).
        AND row_id IN (
            SELECT row_id FROM (SELECT * REPLACE (COALESCE(row_id, error('parquet scan produced NULL row_id: a scanned object violates the export schema invariant (#189/#256)')) AS row_id, CAST(COALESCE(changed_at, error('parquet scan produced NULL changed_at: a scanned object violates the export schema invariant (#189/#256)')) AS BIGINT) AS changed_at, CAST(deleted_at AS BIGINT) AS deleted_at, CASE WHEN ltbase_created_at IS NULL AND COALESCE(CAST(deleted_at AS BIGINT), 0) = 0 THEN error('parquet scan produced NULL ltbase_created_at on a live row: a scanned object violates the export schema invariant (#189/#256/#460)') ELSE CAST(ltbase_created_at AS BIGINT) END AS ltbase_created_at) FROM read_parquet($S3_PATHS, union_by_name=true)) AS cold_scan
            WHERE (age > 18 AND name LIKE 'John%' AND tag = 'developer')
        )
),

-- =========================================================================
-- CTE 3: PostgreSQL Source (Hot)
-- Performs Dynamic Pivoting and Predicate Pushdown.
-- =========================================================================
pg_source AS (
    SELECT
        m.ltbase_row_id AS row_id,
        m.ltbase_created_at AS created_at,
        cl.changed_at AS ver_ts,
        cl.deleted_at AS deleted_ts,

        -- [Type Casting] MANDATORY: Cast PG types to match Parquet Schema
        CAST(m.text_01 AS VARCHAR) AS name,
        CAST(m.integer_01 AS INTEGER) AS age,

        -- [EAV Pivot] Aggregation for dynamic attributes
        -- Note: EAV filtering is done in the WHERE clause below, not pushed to EAV scan
        MAX(CASE WHEN e.attr_id = 205 THEN e.value_text END) AS tag,
        3 AS source_tier_priority

    FROM postgres_scan($PG_CONN, 'public', 'change_log') cl

    -- [Optimization] PUSHDOWN: $PG_WHERE_CLAUSE is a plain predicate in the
    -- WHERE clause below; DuckDB's postgres scanner pushes it down to
    -- PostgreSQL so entity_main is filtered by PG indexes, not in DuckDB.
    JOIN postgres_scan($PG_CONN, 'public', 'entity_main_dev') m
      ON cl.schema_id = m.ltbase_schema_id AND cl.row_id = m.ltbase_row_id

    LEFT JOIN postgres_scan($PG_CONN, 'public', 'eav_data_dev') e
      ON cl.schema_id = e.schema_id AND cl.row_id = e.row_id

    WHERE cl.schema_id = $SCHEMA_ID
        AND (cl.flushed_at = 0 OR cl.flushed_at >= $FLUSH_GRACE_CUTOFF_MS)
        AND m.ltbase_schema_id = $SCHEMA_ID
        AND ($PG_WHERE_CLAUSE)
    GROUP BY m.ltbase_row_id, m.ltbase_created_at, cl.changed_at, cl.deleted_at, m.text_01, m.integer_01
),

-- =========================================================================
-- CTE 4: Unified View
-- =========================================================================
unified AS (
    SELECT * FROM s3_source
    UNION ALL
    SELECT * FROM pg_source
),

-- =========================================================================
-- CTE 5: Ranked (Last-Write-Wins Deduplication)
-- =========================================================================
ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY row_id
            ORDER BY ver_ts DESC, source_tier_priority DESC, deleted_ts DESC, row_id ASC
        ) AS rn
    FROM unified
)

SELECT
    row_id, name, age, tag, created_at
FROM ranked
WHERE rn = 1
    -- Exclude Soft Deletes (the tombstone must WIN dedup first, then be dropped)
    AND (deleted_ts IS NULL OR deleted_ts = 0)
    -- Final logical filter, applied to the LWW WINNER only (#173/#178):
    -- a newer non-matching version must never expose an older matching one.
    AND (age > 18 AND name LIKE 'John%' AND tag = 'developer')

-- Sorting & Pagination
-- A trailing row_id ASC tiebreak gives equal-key rows a total order, so
-- LIMIT/OFFSET page windows stay stable across requests (#183). This mirrors
-- buildNonKeysetOrderBy, the PG optimized template's trailing m.ltbase_row_id,
-- and the production-harness oracle.
ORDER BY created_at DESC, row_id ASC
LIMIT $PAGE_SIZE OFFSET $OFFSET;
```

**The intra-partition tie-break contract (#274).** Inside the `ranked` window
the terms do decreasing amounts of work: `ver_ts DESC` decides almost every
partition; `source_tier_priority DESC` resolves hot-vs-cold at an equal
`ver_ts` (hot is 3, every parquet row is 1 — base and delta are
indistinguishable at rank time); `deleted_ts DESC` makes a tombstone
(`deleted_ts = T > 0`) beat a live copy (`0` from any cold export since #274,
`NULL` from the hot leg, a pre-#274 legacy delta object, or the benchmark
harness shape — which deliberately keeps the raw legacy encoding and stays
#365-tolerant) so the delete wins
the fold and is then dropped by the visibility filter — this delete-wins arm
is a hard contract. The trailing `row_id ASC` is inert here: `row_id` is the
partition key, so it is constant within every partition (it earns its keep in
the outer pagination ORDER BY, not in the window).

That leaves the equal-`ver_ts` **live/live cold tie** with no discriminating
term at all, and that is deliberate: the winner identity is **unspecified**,
because the write path guarantees the copies are value-identical. Two
mechanisms carry that guarantee. #210 stamps base `ver_ts` from the same
clock write as `change_log.changed_at`, so a base copy and a delta copy of
the same version always agree. #274 makes per-row versions **strictly
ordered at write time**, across all three writers: updates compute
`ltbase_updated_at = GREATEST($now, ltbase_updated_at + 1)` and stamp
`change_log` from the RETURNING'd value in the same transaction; hard
deletes stamp their tombstone strictly past the deleted row's version the
same way; and a (re)create of a reused `row_id` stamps strictly above every
version `change_log` retains for the row (`nextRowVersion`) — a recreate
that stamped bare wall time would land BELOW its own clock-ahead tombstone
and lose the LWW fold to it forever. Create and delete allocate under a
per-row transaction advisory lock (`lockRowVersion`; the two share no table
row to lock), so a recreate can never read the row's history while a
concurrent delete's tombstone is in flight — without it the two could tie
and the tombstone would win the fold. Two serialized writes to one row —
even in the same millisecond, even across a failed-init snapshot boundary,
even through a delete/recreate — can never share or regress a `ver_ts`. An
equal-ver_ts
live/live tie is therefore always the same version exported twice (create →
flush → init with no intervening update, or a base copy tying its own newest
flushed delta), and whichever copy `ROW_NUMBER()` picks, the served row is
identical — the contract is row multiplicity and value, never scan order.

Two residuals bound this guarantee. First, objects written **before** the
#274 ordering fix may still hold divergent same-`ver_ts` copies from
pre-fix same-millisecond writes; the #292 promotion fences
(`internal/reconcile/promote_fence.go`) refuse to publish unverifiable
entries from that era, and compaction's fold of such a legacy pair is
arbitrary (the copies were already unordered on disk). Second, during the
mixed-vintage window a legacy delta object (live `NULL`) tying a new-vintage
object (live `0`) resolves to the new object under `NULLS LAST` — the copies
encode the same version, so this is a curiosity, not a regression. CDC is
version-aware, not wall-clock-gated: the flush snapshot is the MAX of the
wall clock and the batch's listed versions, and marking is exact against
the listed `(row_id, version)` pairs (`cdc.MarkFlushedVersions`), so a
clock-ahead version exports and marks in the same run — a wall-clock-only
cutoff would have starved such rows indefinitely under sustained
faster-than-one-write-per-millisecond traffic. The only wall-clock trace
left is the age-based flush *trigger* (`now - oldest >= MaxAgeMs`), which
starts counting once the clock reaches the version; a lone clock-ahead row
waits out its drift plus MaxAge before age-triggering, but flushes with any
run triggered by count or by other rows.

Keyset (cursor) pagination carries the same total-order requirement, and it is
now **enforced**, not merely documented: the engine rejects any cursor whose
final column is not `row_id`. The rule lives on the cursor type
(`model.KeysetCursor.ValidateShape`) and is applied by the single validator
`federated.validateKeysetCursor` at both entry points — the live renderer path
in `DBFederatedQueryEngine.Query` and the keyset branch of
`ExecuteFederatedPaginatedQuery`, which an active cursor alone now selects
since the `KeysetEnabled` flag was retired (#381) — and again inside
`generateKeysetWhereClause`, so a direct `internal/sqlgen` caller cannot bypass
it. A cursor ending on a non-unique key applies a strict inequality on that key
at the boundary, which silently skips every row tied there; the trailing
`row_id` gives the composite key a unique tiebreak so each boundary tie is
resolvable (#183).

**Never-flushed columns (#255).** `union_by_name` can only union columns that
exist in *some* file. An attribute added to the schema before its first flush is
absent from the entire scan set, so the scan source (both `s3_source` and the
semijoin) is wrapped as
`(SELECT *, NULL::<type> AS <col> … FROM read_parquet(…)) AS cold_scan`,
projecting each such column as a typed NULL — computed per query from the
pre-read validator's footer-column union, and only when that union is complete
(an incomplete union falls back to the unaugmented scan and today's loud
classified failure). The missing-column set participates in the compiled-plan
scope hash, so a skeleton compiled while a column was cold-absent is re-keyed the
moment the first flush lands it (the plan-cache poisoning hazard the issue
mandates addressing). With no missing columns the rendered SQL is byte-identical
to this document's §5 sketch. One residual race is accepted: for glob-hint path
sets the validator's listing and the scan's listing are separate S3 LISTs, so a
flush landing between them can collide the NULL alias with the newly-landed real
column — a one-time loud classified failure that self-heals on the next query,
since the missing set is recomputed per query.

**Stale-typed attribute generations (#371).** The system-column guard above
re-pins `changed_at`, `deleted_at` and `ltbase_created_at`, but attribute
columns used to be left to `union_by_name` widening. A parquet generation
written under an older attribute type (a `cdc-init` re-run after an
`integer`→`text` schema edit, or a delta file that survived a re-init) then
either fails loudly (`text`→`list`, a DuckDB Conversion Error before the
REPLACE list is reached) or drifts quietly: an INTEGER attribute with one
VARCHAR generation binds as VARCHAR and sorts lexicographically. The pre-read
validator now records, per column, whether the scan set carries more than one
parquet type, and `coldScanColumns` compares the union type with
`sqlgen.DuckDBNullScanType(meta)`, the type the exporters write. A column that
is mixed across files, or whose union type differs from the export type, is
rendered `CAST(col AS <type>) AS col` inside the same `* REPLACE (…)` list as
the system items:
`(SELECT * REPLACE (<system items>, CAST(score AS INTEGER) AS score) FROM read_parquet(…)) AS cold_scan`.
The pin is selective: a healthy scan set renders no attribute item and the
SQL stays byte-identical to the §5 sketch, so attribute filters are not wrapped
in a CAST on the common path and the compiled-plan cache is unaffected.
VARCHAR and UUID are treated as compatible because a column-bound `uuid`
attribute exports as parquet UUID while the null-scan type is VARCHAR. Numeric
strings coerce value-preservingly (`'9'` → 9, restoring numeric order); a
value the pin cannot convert fails with DuckDB's own Conversion Error, but only
on a read that consumes the column, because DuckDB prunes an unused CAST —
the same scope as the `changed_at` re-pin. The pinned set is a separate
`cold-pinned` component of the plan-scope hash next to `cold-missing`, so a
pinned shape never shares a skeleton with an unpinned one. The pin is a read
guard, not the fix: the stale generation itself is retired by `cdc-init
--replace-delta` (see `docs/manifest-reconcile.md`), which is what makes the
scan set single-typed again.

The pin's trust boundary is the same as the #256 stamp trust below: the union
it compares against is fed by manifest column stamps that pass the
system-column invariant, with no footer probe, and `parquetcheck.Check`
validates system columns only. A stamp that reports an attribute at the export
type is therefore believed for that attribute even if the bytes at the path
carry another type, and the drift would then bind unpinned. That state needs a
same-key rewrite under an unchanged stamp — a legacy deterministic init key
overwritten in place, or a manual or old-writer mutation — which no current
writer performs: since #416 every init batch lands on a write-once key, flush
and compaction never reuse a key, and re-runs go through `cdc-init
--replace-delta`. Probing attribute columns on every read would give back the
cost #256 removed, and binding a stamp to an object version is a manifest
format change; the offline detector for a stamp that no longer matches its
object is `manifest-reconcile --verify-stamps`, which re-describes each listed
object and reports stamp/footer disagreements.

**Manifest schema stamping (#256).** The writers (CDC flush, CDC init,
compaction merge) `DESCRIBE` each parquet object they publish and record its
footer columns (name → DuckDB type) on the manifest entry
(`FileEntry.columns`) — flush and init describe the final key, while
compaction describes the tmp object it staged, which is sound because tmp→final
is a byte-identical `CopyObject` and the e2e suite pins the published object's
footer against the stamp. The pre-read validator consults the stamp first: a stamp
satisfying the system-column invariant short-circuits the footer probe and
feeds the column union above; a stamp that is absent (an entry predating
stamping) or fails the invariant falls back to the probe. On that invariant
verdict a stamp may only short-circuit **success** — a rejected stamp costs one
probe and never authors a failure, so byte truth (#187) alone decides whether a
file is malformed, and corruption detection (unreadable footers, the #251
verify-and-exclude pass in §7.3) is untouched because it never ran on `DESCRIBE`
results to begin with. The column union is the one named exception to that
guarantee: the invariant check inspects only the system columns, so a stamp
that passes it while *under-reporting* the file's attribute columns yields a
short union that still counts as complete, and the NULL alias then collides
with a column the file really carries. Unlike the glob-listing race above,
which self-heals on the next query, that failure **persists until the manifest
entry is corrected** — but correcting it is enough: the rewritten stamp is a
new cache key (below), so the fix lands on the next query without a restart.
Accepted, because a stamp is a write-time `DESCRIBE` of the bytes just written:
under-reporting takes a corrupted or tampered manifest, and the outcome is a
loud classified failure rather than silent data loss. Stamping is best-effort
at write time — a failed self-describe leaves the entry unstamped at the cost
of one probe on first read, and is logged rather than swallowed.

**Scan-level system-column guard.** Trusting a stamp means not looking at the
bytes, which opens one channel the probe path did not have: if the object behind
a stamped key does not actually carry the system columns — a rogue overwrite, a
tampered manifest, the wrong file restored — `union_by_name` NULL-fills them
from the sibling objects' schema and the query **succeeds while reading garbage
or nothing**. A NULL-filled `row_id` drops those rows out of the dirty anti-join
(silent data loss); a NULL-filled `changed_at` keeps them but feeds NULL into
LWW version ordering (a silently wrong winner). Either way it is the exact
inversion of #187's contract. The scan source therefore rewrites the system
columns in place (`sqlgen.BuildParquetScanSource`, rendered at both scan sites
in §5), classified `ErrFederatedReadFailed` and degradable like every other
read-side schema violation. Two guard shapes, because the two channels differ:

- **Presence** (`COALESCE(col, error(…))`) on `row_id` and `changed_at`, neither
  of which is ever legitimately NULL — flush exports `cl.changed_at` from a
  `NOT NULL` change_log column, init/base exports the equally `NOT NULL`
  `m.ltbase_updated_at` (#210), and the benchmark shape carries `changed_at`
  directly. The `row_id` guard stays **untyped** on purpose: `error()` carries
  no type of its own, so `COALESCE` adopts the column's — UUID for production
  exports, VARCHAR for the benchmark shape — and nothing coerces `row_id`
  anywhere (#147).
- **Type** (`CAST(… AS BIGINT)`) on `changed_at` and `deleted_at`, both BIGINT
  in the production *and* benchmark shapes. Without it a rogue file carrying
  either as VARCHAR widens the whole `union_by_name` result and LWW ordering
  silently goes lexicographic (`'9' > '100'`). The CAST re-pins BIGINT: numeric
  strings coerce value-preservingly, garbage fails loudly.

A scan set where *no* object carries a guarded column fails to bind instead;
different message, same contract.

**Residual: `deleted_at` presence (#365).** `deleted_at` gets the type pin but
**no** presence guard. Both exporters now encode live rows as
`COALESCE(…, 0)` (#274), but delta objects written *before* #274 still carry a
literal NULL for live rows and remain readable until compaction retires them —
a NULL-based presence guard would fail every healthy scan touching one. The
consequence is that an object missing `deleted_at` entirely still reaches the
merge with a NULL, indistinguishable at the scan from a legacy live row; that
divergence is covered only by the pre-read footer probe and the manifest
stamp. Extending the presence guard to `deleted_at` is gated on the legacy
delta objects being retired and is tracked in #365. The residual is pinned in
the test suite, not only here:
`sqlgen.TestParquetScanGuardTolerateNullDeletedAt` characterizes today's
behavior and is the test that should go red when #365 lands.

Note that #251 verification drains `SELECT *` without the guard, so a
schema-wrong (as opposed to byte-corrupt) object is never confirmed corrupt and
never excluded — correct: exclusion is for unreadable bytes, while an
export-schema violation is an operator-visible consistency fault, not something
to route around. The guarded per-file identification pass (#351) is the
deliberate counterpart: it uses the guard precisely to *name* such an object,
while still never excluding it.

**Trust boundary.** A stamp is trusted without reading bytes only because every
object behind one was written by a Forma writer under manifest transactionality:
flush and compaction mint fresh keys and stamp what they just wrote, an init
rerun likewise mints fresh write-once keys (#416) and stamps them in the same
`ReplaceTierFiles` publish that replaces the base tier, and `manifest-reconcile --repair`'s init promotion
(#292) recomputes the stamp from the footer rather than inventing one. Outside
that boundary — an out-of-band overwrite of a listed object's bytes while its
entry keeps a stamp that still satisfies the system-column invariant — the read
path cannot see the change, and three things bound the exposure: (a) the scan
guards above, which re-derive `row_id`/`changed_at` presence and the
`changed_at`/`deleted_at` types from the bytes on every scan regardless of what
the stamp claimed; (b) `manifest-reconcile --verify-stamps`, the offline
full-map comparison of every listed entry's stamp against its object's real
footer — strictly stronger than the scan guards (it sees dropped attribute
columns and a re-typed `row_id` too) and at zero read-path cost, since it runs
in the tool; and (c) the `deleted_at` presence channel, which stays gated at
the read path until pre-#274 legacy delta objects are retired (#365) and is
therefore reachable *only* through (b) until then.

**Triage: the guard failure names its objects.** The set-scan error itself
cannot name a path — the guard runs inside a single `union_by_name` scan over
the whole resolved set, so DuckDB raises one error for the set — but on a read
failure that neither the missing-object classification (#187) nor the
corruption confirmation (#251) claims, the engine now runs a guarded per-file
drain over the manifest-listed set (#351): each object is re-read alone through
the same guarded scan source, and an object whose guarded drain fails twice
while its bare `SELECT *` drain reads clean is attributed as a schema-invariant
violator. Note that the trigger is deliberately *not* a guard-specific
classification — no such classification exists. Recognizing a fired guard would
mean matching driver error text, which would miss the BIGINT `CAST` channel
entirely, whose wording is DuckDB's own rather than one of the authored guard
messages. Identification therefore runs on any unclaimed read failure and lets
the differential decide; that same differential separates the three failure
families — byte corruption fails both drains (#251 territory), a missing object
never gets here (#187 classification wins first), and only a schema-wrong
object splits them. On a failure no object owns, every guarded drain passes and
nothing is attributed. Attribution surfaces in two places: the returned
`ParquetGuardViolationError` names the schema and the offending storage keys,
and the engine logs an Error with the same paths — the log matters because
under `AllowPartialDegradedMode` the error is absorbed by the postgres-only
fallback. Identification never excludes: unlike byte corruption, a
schema-wrong object may legitimately own rows that exclusion would silently
drop once the object is repaired, so the retry-after-exclusion machinery
stays #251-only. Note the deliberate breadth: a single-file guarded scan is
strictly stricter than the set scan (a file missing only `deleted_at` fails
alone but is tolerated in a set where a sibling carries the column), so the
error's wording is an invariant statement — "fails the guarded single-file
scan" — not a causation claim. Hint-authored path sets are not identified
(no manifest vouches for them); for those the manual path remains: list the
schema's manifest paths, then run the guarded scan against one path at a time.
The cost envelope stays on the failure path: identification runs only when a
read has already failed, adds at most ~3 drains per manifest-listed object
(the guarded pair plus the bare confirmation) on top of #251's verification
drains, runs the paths sequentially on the engine's DuckDB pool (one open
connection by default, #285), and bails on context cancellation.

**Cache invalidation.** The validator caches each path it validates, keyed by
path **and by the stamp it was validated under** (nil for probe-validated). Path
alone would be wrong: Forma's writers mint write-once keys (flush and compaction
always did; `cdc.BuildBasePath` mints `{min}_{max}_{uuid}.parquet` since #416,
before which an init rerun overwrote its deterministic `{min}_{max}.parquet` key
in place), but a rewrite under a listed path is still reachable — legacy
deployments, or an operator repair that republishes bytes under an existing
key — and each re-stamps the entry under the same key. A path-keyed cache would
serve a warmed server the pre-rewrite columns for the life of the process. The manifest
rewrite is therefore the invalidation signal: same stamp, keep the entry; new
stamp, re-validate. An unchanged stamp still costs zero probes, so the
cold-start win is intact. When a probe *does* run on a stamped path — which
happens only when the stamp failed the invariant — the two are cross-checked
and any divergence is logged with the footer winning the union; that is a log
and not an error because the read succeeds, so nothing in the caller's result
would ever mention it.

**Cache bounds.** The cache is a pure performance memo — a miss falls through to
the stamp check or the footer probe — so it is bounded rather than kept for the
life of the process (#466). An entry expires after 30 minutes without a lookup,
and every hit slides that deadline, so an object queries keep scanning stays
warm while one that compaction retired ages out: lookups and inserts alike
sweep expired entries (at most once per TTL window), so while the validator
sees any traffic — even hits alone — a retired path's entry is gone within
about two TTL windows of its last use. Independently, the cache holds at most
8192 entries; a new path arriving at the bound evicts one arbitrary entry, so a
caller-chosen path set cannot grow the heap past it even inside one TTL window.
One-entry eviction instead of `queryplan.Cache`'s wholesale clear keeps the bound
from sending every live object back to a footer probe at once. Eviction can only
cost a re-validation, never correctness.

**Backfill contract: lazy fallback, no backfill.** There is no migration pass
and no manifest version bump — field presence is the format signal. Legacy
entries acquire stamps only when a writer rewrites them (compaction merging
them into a new base, or an init rerun), and an entry that is never rewritten
stays probe-based **indefinitely, by design**: an unstamped path costs one
footer probe per cache residency (see Cache bounds above; while queries keep
scanning it, that is once per process), which is the pre-#256 steady state, so
there is nothing to repair and no window in which correctness depends on the
stamp existing.

## **6. Optimization Strategies**

### **6.1 Predicate Pushdown (Critical)**

* **Mechanism:** `$PG_WHERE_CLAUSE` is rendered as a plain predicate in the pg_source `WHERE` clause; DuckDB's postgres scanner pushes supported predicates down into the PostgreSQL scan.
* **Rationale:** entity_main may contain millions of rows. Pulling all rows to DuckDB for filtering is unacceptable. Pushdown leverages PostgreSQL indexes.
* **Limitation:** Only applicable to entity_main columns. EAV columns and complex functions must be filtered in DuckDB memory after the join.

### **6.2 Streaming Result Processing**

* **Requirement:** 4.2 (Memory Management).
* **Implementation:** The Go/Java application **MUST NOT** load the full DuckDB result set into a slice/array.
* **Pattern:** Use database/sql (Go) or JDBC ResultSet iterator patterns to stream row-by-row JSON serialization to the HTTP response writer.

### **6.3 Smart Type Casting**

* PostgreSQL numeric $\rightarrow$ DuckDB DOUBLE (Precision loss acceptable for search, not for finance).
* PostgreSQL smallint/integer $\rightarrow$ **binding-dependent**: column-bound
  attributes keep the physical SMALLINT/INTEGER width, EAV-only attributes
  carry storage width DOUBLE — the precise per-surface contract is §6.4 (#384).
* PostgreSQL text (containing UUID) $\rightarrow$ DuckDB UUID (Explicit cast required).

### **6.4 Numeric Width Contract (#384)**

`eav_data.value_numeric` is an unconstrained PG `NUMERIC`, but every value the
write funnel puts there is a **float64 image** (`transform.populateTypedValue`
narrows the whole numeric family through `numutil.Float64`), so the effective
EAV storage width is DOUBLE regardless of the declared type. The ruling,
applied on both sides:

* **Write side**: the EAV write funnel rejects numeric-family values that do
  not fit the declared integer type — out of range or non-integral for
  `smallint`/`integer`/`bigint` — as user-facing invalid input. A `bigint`
  is judged by the float64 image the row stores, so an int64 whose image is
  2^63 (from 2^63−512 up) is refused as well (#612). `numeric`
  stays unconstrained (its float64 ceiling is #205). Main-column-bound write
  fidelity is tracked separately (#459).
* **Projection**: EAV-only `integer`/`smallint` project by **storage width** —
  `TRY_CAST(value_numeric AS DOUBLE)`, exactly like the `numeric` class — on
  every DuckDB surface (hot EAV pivot, list elements, CDC parquet export, cold
  NULL augmentation). DOUBLE reproduces every float64 image faithfully, so
  historical out-of-declared-range *and* non-integral rows answer identically
  on all tiers (an integer-width cast NULLed the former and rounded the
  latter, only on the DuckDB legs). Column-bound attributes keep declared
  width: their storage is physically int4/int2.
* **Predicate operands**: DuckDB operand casts follow column storage width —
  `integer`/`smallint`/`numeric` operands cast to `DOUBLE` (an operand of any
  magnitude compares instead of raising a Conversion Error; the former
  `DECIMAL(38,10)` numeric cast also truncated operand scale at 10 fractional
  digits and overflowed past ~1e28). To keep the Postgres route on the same
  verdict, integral operands for these DOUBLE-width classes **narrow through
  the same float64 funnel the stored data took**
  (`sqlgen.NarrowEAVNumericOperand`, applied by the EAV predicate binder and
  the batch attr-value anchor alike): above 2^53 both engines compare the
  operand's float64 image, rather than Postgres comparing the exact integer
  while DuckDB matches its rounded neighbor. Float64 operands bind in
  shortest round-trip form (`strconv.FormatFloat(v, 'g', -1, 64)`), so the
  DuckDB side recovers the identical float64 the PG side binds — `%.15g`
  dropped the 16th-17th significant digits. `bigint` stays `BIGINT` with
  exact int64/decimal-string binds (#281/#357), so it cannot take the DOUBLE
  widening: an integral `bigint` operand outside int64 range (`gt:1e30`,
  `equals:9223372036854775808`, and the `Inf`/`NaN` spellings) is instead
  rejected as user-facing invalid input by every predicate binder
  (`sqlgen.checkBigIntOperandRange`, #502) — the same bound the write funnel
  enforces, so no legal data sits on either side of such a comparison and
  every rejected literal has an exact in-range equivalent
  (`gt:9223372036854775807`). Before #502 the Postgres route answered against
  `NUMERIC` while the DuckDB route raised a Conversion Error on
  `CAST('1e+30' AS BIGINT)`. Fractional `bigint` operands in range are
  unaffected.
* **Bool**: both engines compare the `value_numeric > 0.5` truthiness
  (`sqlgen.BoolTruthiness`) — the PG EAV EXISTS predicate renders
  `(x.value_numeric > 0.5) =/!= <bool>` — and parse operands under one shared
  rule (`ParseBool` spellings, else any integer with `>0` truthiness), so no
  spelling errors on exactly one route. The threshold is the #404 read-side
  rule shared with the Go read path (`transform.float64ToBool`): a persisted
  image is the nearest of 0/1, so float noise around either end does not flip
  the answer. The write side is strict (`transform.boolFromAny`): a bool
  input is a Go bool, a `ParseBool` string, or a number that is exactly 0 or
  1 — any width the numeric funnel accepts (`numutil.Float64`: int, int16,
  int32, int64, float32, float64, pointers to each; int8 and the unsigned
  widths are rejected on every funnel until #566), and a `json.Number` is
  decided on its decimal text, so a literal that merely rounds to 0 or 1
  (`1.0000000000000001`) is rejected too. Anything else is rejected rather
  than coerced. A `bool_text` main column is read by its `"1"`/`"0"` contract
  on every leg (Go, DuckDB hot leg, CDC export); other text is a storage
  consistency error on the Go read path. A column-bound bool keeps its
  nullability end to end: the CDC export emits the bare truthiness (never
  `CASE … ELSE FALSE`), so an unset optional bool is NULL in parquet exactly
  as it is in `entity_main`, and `equals:false` does not start matching it
  after a flush. The no-EAV hot projection (`BuildPGSelectNoEAV`, live for
  any schema whose attributes are all column-bound) derives the bool through
  the same `mainColBoolExpr` as the EAV-joined projection, and the outer
  select casts the verdict back to the physical column's shape (`SMALLINT`
  1/0, `VARCHAR` '1'/'0') so the federated reader scans it by column kind.
  The entity_main pushdown is on the same contract (#565): a filter on a
  column-bound bool reaches `entity_main` through the pg-main clause the
  DuckDB hot leg embeds in its `postgres_scan` and through the hybrid
  builder's main branch on the Postgres route, and both render
  `sqlgen.BoolMainPredicate` rather than a raw `= 1` / `= '0'` compare —
  `<col> BETWEEN $lo AND $hi` with bounds `(1, 32767)` for the truthy side
  and `(-32768, 0)` for the falsy side of a `bool_smallint` column (exactly
  `> 0.5` / `<= 0.5` on a SMALLINT), `(<col> = '1') = $b` with a bool bind
  for a `bool_text` column. `not_equals:X` is `equals:!X`. The clause text
  is the same for every operand because the federated plan cache keys on
  the query shape without operands and reuses the cached pg-main clause on
  a hit; only the binds may differ. The smallint bounds are integer binds so
  the predicate stays on the `integer_ops` btree family (a `0.5` literal
  would cast the column). So a stored image outside the write contract (a
  `2`, a `'true'`) answers `equals:true` / `equals:false` the way the
  projection, the export and the Go read path already read it, on the
  unflushed hot routes exactly as on parquet. A bool attribute under any
  other operator (`gt`, `lte`, `starts_with`, …) is a 400 on every route:
  the EAV and pg-main normalizers always refused it, and the hybrid main
  branch, which used to compare the raw 1/0 image, refuses it through the
  same classifier.

Parquet files written before this contract carry INT32/INT16 attribute columns
and NULLs where a value exceeded the declared width; `union_by_name=true` scans
promote the mixed widths losslessly, and the NULLs are unrecoverable from
parquet alone. `validate-schema-consistency` finds the affected rows and
`--requeue-stale-width-exports` re-queues them for a re-flush from PG (#501;
see `docs/schema-consistency-migration.md`). Declared bigint still projects at
BIGINT, so a bigint value past int64 diverges on every DuckDB leg; the same
census reports it for a rewrite.
The remaining asymmetry class is #205's float64 ceiling: values only a full
NUMERIC can hold (planted by direct SQL, never by the funnel) still read
exactly on Postgres and as their float64 image on DuckDB.

## **7. Resilience and Error Handling**

### **7.1 Circuit Breaker**

* **Trigger:** 5 consecutive failures (Timeout or OOM) within 30 seconds.
* **Action:** Immediately fail requests with storage=['olap']. Fallback to storage=['oltp'] (Postgres only) if allowed by the request.

### **7.2 Degraded Modes**

1. **S3 Unavailable:**
   * Log Error.
   * Rewrite query to select *only* from pg_source.
   * Return HTTP 200 with metadata: `{"partial_result": true, "warning": "Historical data unavailable"}`.
2. **PostgreSQL Unavailable:**
   * Cannot query dirty_ids or hot-tier rows.
   * Rewrite query to select *only* from s3_source.
   * **Risk:** "Ghost Reads" (Deleted data reappearing) and missing unflushed hot rows.
   * Action: Only permissible if `federated.consistency_mode = "eventual"` is set on the request; otherwise return HTTP 503.
   * S3-only responses carry metadata: `{"partial_result": true, "warning": "Hot-tier data unavailable, results may be stale"}`, plus an `X-Forma-Consistency: eventual` HTTP warning header.

### **7.3 Partial-Read Resilience (#251)**

One unreadable parquet object no longer fails a whole manifest-authored scan. The mechanism is **verify-and-exclude**, and it runs only on the failure path — a healthy query pays nothing.

**Mechanism chain.** A DuckDB read failure (at `Query` time or mid-stream — listed objects are opened lazily, so either is possible) enters `failDuckDBScan`, which resolves it in a fixed order:

1. **Missing-object classification first.** `classifyDuckDBReadError` probes the exact scanned set via `ParquetSource.MissingIn`. A listed-but-absent object is manifest **inconsistency** (§4.3.1, #187 scenario 2) — non-degradable, breaker-worthy, **never retried**. Missing is not corrupt, and this ordering is a contract: a deleted object must never be quietly excluded as "unreadable".
2. **Per-file verification.** Otherwise the engine re-reads each object of the failed set individually (`verifyParquetPaths` → `SELECT * FROM read_parquet('<one object>')`, drained to exhaustion). An object is confirmed corrupt only after **two consecutive solo drains fail**: deterministic corruption fails every drain — same bytes, same decode — while a transient object-level fault (an S3 timeout, a reset connection) that reads clean on the immediate re-drain must not be cached as corruption, which would convert a blip into a retention window of unmarked short answers while bypassing the breaker. A single failed drain is inconclusive and leaves the query on the ordinary `RecordFailure` path. Verification only runs for source-authored sets of **two or more** objects; glob entries are skipped (a glob names a set, not one object, so unverifiable means unexcludable — quote-bearing keys render safely since #456 and are verified like any other, #479), and a cancelled context confirms nothing, since cancellation is indistinguishable from corruption for the paths not yet drained.
3. **TTL cache.** Confirmed-corrupt objects are recorded in an engine-level `corruptParquetCache` with a bounded lifetime — default 5 minutes, overridable with `WithCorruptPathRetention`. Entries always expire: a terminal verdict must never be memoized forever, because repair, compaction retiring the key, or a manifest reconcile can only self-heal through re-verification after expiry.
4. **Resolve-time exclusion.** `resolveParquetPaths` filters the source-authored set through the cache before the template renders, and reports what it dropped.
5. **One retry.** `ExecuteDuckDBFederatedQuery` retries the query exactly once against the readable remainder. A second retryable failure surfaces to the caller: corruption appearing mid-flight is indistinguishable from a sick store and must not loop.

**Why a drain, not a footer probe.** DuckDB answers `DESCRIBE` and `COUNT(*)` from the Thrift footer and row-group metadata without touching data pages, so a footer probe passes cleanly on a page-corrupt object — the pre-read schema validator already footer-probes, and scenario 7 still failed. Only a drain reads the pages. A **solo drain reads a superset of the columns the scan projects**, so whatever per-file failure made the set scan fail is deterministically reproduced by the per-file pass; there is no failure that the set scan can detect and the verification pass cannot. That superset property is what makes attribution sound: an object is excluded because it was proven unreadable on its own, not because it was in the set when something went wrong.

**Operator note — failure-path cost.** Verification drains every object of the failed set sequentially on the engine's pooled connection (one open connection by default, #285), and the retry then re-scans the remainder: for a tier of N listed objects, the first query after a corruption pays roughly N solo drains (one extra re-drain per failing object, for the two-failure confirmation) plus a second scan — and because cache entries expire, the same bill recurs on the first query of each retention window for as long as the corrupt object stays listed. This is the deliberate trade (cost bounded by failures, not by queries), but on large tiers it surfaces as a periodic latency blip, once per corrupt object per TTL window, until compaction or a manifest reconcile retires the object. A footer-probe pre-filter would not reduce it: footer-dead objects already fail the drain at open time (the drain's bind IS a footer read), and the dominant term — draining the healthy objects — cannot be certified by any metadata-only probe.

**Why not `ignore_errors`.** It does not exist. On the pinned engine (DuckDB **v1.4.5** via `github.com/duckdb/duckdb-go/v2`) `read_parquet(..., ignore_errors := true)` fails at bind time with `Binder Error: Invalid named parameter "ignore_errors" for function read_parquet`, and DuckDB enumerates the complete accepted parameter list — it contains no error-tolerance knob of any kind (`ignore_errors` is a `read_csv` parameter). The option therefore cannot be adopted today, and it would be rejected even if a later version added it: a reader-level skip is **silent** — no execution-plan marker, no classification, no per-object attribution — which recreates exactly the scenario-2 silent-loss class this subsystem exists to prevent. Depending on a flag whose appearance in a future upgrade would silently change read semantics is the wrong shape of dependency. See `docs/superpowers/plans/2026-08-02-issue-251-spike-findings.md` for the raw evidence.

**Loudness contract.** A partial answer is never quiet. Path resolution records `NotePartialParquetExclusion` on the internal execution plan, naming every excluded object, and the exported constant — not a retyped literal — is what tests assert on (#197). Notes remain **embedder-only by design**: `toExecutionPlan` projects `Sources` and drops `Notes` at the HTTP boundary (#301/#306), so object keys never reach an API caller. Because Notes cannot explain anything to an HTTP consumer, the plan must be truthful by construction rather than by annotation: `ExecuteDuckDBFederatedQuery` snapshots the plan's `Sources`/`Notes` length **and its `Timings` entries** before the first pass (`markExecutionPlan`) and truncates/restores back to that mark (`rewind`) before retrying, so the attached plan describes only the pass that actually produced the returned page — `Timings`, unlike `Notes`, does cross the HTTP boundary, so the restore is what keeps a retried request from publicly reporting both `plan_cache_hit` and `plan_cache_miss` (#348). Without the rewind a successful retry would report both passes — two identically-labelled DuckDB scans, the failed one carrying `ActualRows=0` and therefore indistinguishable from a scan that legitimately matched nothing, plus a double-counted hot-tier row estimate. Everything recorded *before* the first pass survives the rewind: the routing decision and its note are the caller's, not the failed pass's. The retry re-records the exclusion note itself, through path resolution. Since #348 the partial answer is also **HTTP-visible**: the engine records the excluded set as a `PartialScan` out-parameter (per pass, last pass wins — not gated on `include_execution_plan`), the page carries it, and the service projects it into `QueryResult.partial` as `{reason: "corrupt_parquet_excluded", excluded_object_count: N}` — reason and count only; object keys remain embedder-only. The marker is deliberately not `Routing.Reason`: the route does not change on a partial read.

**Coverage marker (#468).** The same `partial` field carries a second, mutually exclusive reason: `{reason: "hot_tier_only", unconsulted_tiers: ["warm","cold"]}` marks a page answered from the Postgres hot tier alone although the request asked for more — explicitly, or for all three by omitting `preferred_tiers`. Three Postgres-only paths set it (`markHotTierOnly`, `engine_tier_coverage.go`): the routing heuristics keeping an implicit request on the small-page shortcut, a globally disabled DuckDB engine, and the §7.2 degraded fallback, whose `partial_result` promise was until then visible only on the plan-gated `Routing.Reason`. The hot-only gate (`prefer_hot`, `["hot"]`) never marks: the caller asked for hot and got hot. The two reasons cannot coincide — corrupt exclusion happens only on a DuckDB pass, a coverage gap only on a Postgres-only answer — and a degraded answer replaces any corrupt-exclusion marker the abandoned pass left on the out-parameter. An explicit multi-tier `preferred_tiers` is not marked but *honored*: it overrides the cost heuristics onto the federated path (routing.go, next to the #354 cursor override), which is also what keeps the three-tier merge exercised by ordinary page-sized traffic. The service forwards an omitted `preferred_tiers` as empty rather than filling the default, so the engine can tell the two apart.

**Breaker contract.** Confirmed corruption is the one read failure that is **not** engine sickness, so it calls `ReleaseProbe` — handing back a half-open probe reservation — instead of `RecordFailure`. The justification is empirical rather than assumed: the verification pass just read every *other* object of the set through the same engine, the same session, and the same store, which is a live proof of health. Without this, a permanently corrupt object would drive the breaker open on every query and hold the DuckDB route off indefinitely. **Store-wide unreadability still records a failure:** if verification finds that *every* object in the set fails (`len(corrupt) >= len(paths)`), the store or engine is sick, not the files, so nothing is cached, nothing is excluded, and the failure takes the ordinary `RecordFailure` path with its original classification. Missing-object inconsistency (branch 1 above) likewise still records a failure.

**Scope.** Exclusion applies only to **level-2, source-authored** path sets (§4.3.1):

* **Hint-authored sets keep all-or-nothing.** An explicit `federated.s3_parquet_path_template` means the operator pinned the set; silently scanning a subset of a pinned set would misreport the answer for the same reason a hint never falls through to the manifest source.
* **Glob sets keep all-or-nothing.** Exclusion is inexpressible in a glob — you cannot write "this prefix minus that object" — so the level-3 fallback keeps today's behavior, consistent with it never being able to classify inconsistency either.
* **Excluding everything is forbidden.** If the cache would empty the set, resolution passes the *full* set through unfiltered. Total corruption must fail loudly with its own classification, not decay into a quiet `ErrNoParquetPaths` misconfiguration.
* **Plan-cache safety is free.** The resolved path set is already part of the `scope-v2` cache key (`internal/federated/duckdb_query_build.go`, #255), because the paths render into the skeleton as SQL literals. Removing an object changes the key, so the excluded-set query compiles its own plan and can never reuse a plan built over the corrupt object.

**Interaction — a cached object escapes the missing-object probe until the entry expires.** `MissingIn` probes the *scanned* set, and an excluded object is by definition not in it. So if a confirmed-corrupt object is subsequently deleted from storage while its cache entry is live, that deletion does not classify as manifest inconsistency during the retention window; it surfaces only after the entry expires and the object is scanned again. This is a bounded, deliberate consequence of the TTL: the window is `WithCorruptPathRetention` (default 5 minutes), and it is why the retention is bounded at all. The rows of a corrupt object are already absent from the answer either way, so the exposure is a delay in *classification*, not a new class of silent loss.

**Retention interplay.** This is what makes bounded PostgreSQL retention survivable. Before #251, a single unreadable cold object failed the entire federated scan, and the only remaining answer was the §7.2 degraded fallback to Postgres alone — complete only for as long as Postgres still happened to hold everything that had been flushed. Deployments were relying on that coincidence: shortening hot-tier retention would have converted one corrupt object from "degraded but complete" into "degraded and silently short". With verify-and-exclude, the query is answered from the readable parquet remainder **plus** the hot tier, so completeness no longer depends on how much history Postgres retains, and the answer's shortfall is exactly the corrupt object's rows — named on the plan.

**Known limitation — silent mis-decode (pre-existing; detected out of band since #347).** DuckDB v1.4.5 neither writes nor verifies Parquet page checksums, and `parquet_metadata()` exposes no CRC column, so byte corruption inside an incompressible column chunk can decode into *different valid values* with no error at all. The spike proved this concretely: a 64-byte XOR at the 50% offset of a 500-row file returned 500 rows with **5 `row_id` values lost and 5 fabricated**, zero errors raised. A mis-decoded `row_id` defeats the dirty-set anti-join (§3.2), so a stale S3 row can escape masking by its hot version. Whether damage is caught at all is a function of *which bytes* are hit — the same corruption landing in a delta-encoded integer column raises `Snappy decompression failure`, and in the footer raises a Thrift error. This class is invisible to **every** reader, including the all-or-nothing scan that preceded #251, so it is neither introduced nor worsened here; #251 covers scan-detectable failures only. What #347 adds is **out-of-band detection**, not read-time prevention.

**Manifest content checksums (#347).** A manifest entry may carry `FileEntry.Checksum`, a `"sha256:<hex>"` digest over the object's bytes (`internal/cdc/checksum.go`). The digest is taken by reading the object back from the store *after* publish, so it blesses the published state. An absent checksum means the entry was never stamped — legacy, or a best-effort hash that failed — and every consumer skips it: **field presence is the format-version signal**, the same rule `Columns` follows (#256), so there is no manifest migration and no version bump. Five publish paths stamp: the CDC flush (delta entries), `cdc-init` (base entries), the compaction rewrite's merged base, and `manifest-reconcile`'s own two entry-creating paths — `--repair` delta-orphan adoption and #292 init-orphan promotion. All five are **best-effort by design**: a failed hash logs `failed to checksum …; … stays unstamped` and publishes the entry regardless, because failing an export — or abandoning a recovery of data that already exists — over an unavailable GET would trade real data for a coverage gap the scrub already counts and reports.

**Two verifiers, both out of band.** (1) `manifest-reconcile --verify-checksums` (`docs/manifest-reconcile.md`) re-GETs every stamped, resolvable, in-prefix entry **in full**, re-hashes, and compares; dangling candidates the run could not prove present are skipped, exactly as `--verify-stamps` skips them. A divergence is a residual discrepancy and exits **2**; a failed GET is a *tool* failure and exits **1**, never a corruption verdict. Unstamped entries are skipped and **counted** (`SkippedUnstamped`), reported as missing coverage rather than as a finding and deliberately not affecting the exit code — a "clean" verdict over mostly-unstamped entries means far less than one over stamped entries. The scrub's channel is the report, the exit code, and a per-divergence WARN naming the key and both digests; it emits no telemetry. (2) The compaction **pre-merge input gate** re-hashes every stamped rewrite source before `runRewrite` merges anything and refuses the pass on mismatch (`ErrSourceChecksumMismatch`, `internal/compaction/verify_inputs.go`). That is the last moment at which corruption is both detectable and still attributable to a named object: the rewrite folds the sources into one new base and splices their entries out of the manifest, so the listed name disappears even though the objects are retained for in-flight readers (#461) and only reclaimed later by `manifest-reconcile --gc`. The error is deliberately absent from `isRetryable`'s allowlist, so a corrupt source is refused once instead of re-probed under backoff, and the gate emits `EmitParquetChecksumMismatch`. Verification is **on by default** (and requires an object reader wired — the one production construction site, `cmd/tools`, does, as does the e2e harness's) — the zero value of `SkipInputChecksumVerify` verifies, so a deployment that never sets the field is still covered — with `--skip-input-checksum-verify` as the escape hatch for unwedging compaction once a known-corrupt source has been triaged. Its reach is narrower than the scrub's: only stamped sources, and only on the run that actually consumes them, so a base object compaction never touches again has the scrub as its sole detector. **Cross-bucket sources are refused, not skipped (#417).** A manifest entry whose absolute `s3://` path names a bucket other than the compactor's own is not a supported compaction input: the compactor cannot verify, stat or delete such an object through its own client, yet `MergeToTmp` would hand the URI to DuckDB verbatim and `spliceManifest` would drop its listed name — an unverified relocation across a bucket boundary past a fail-closed gate. `runRewrite` therefore refuses the pass with `ErrForeignSource` (`rejectForeignSources`, `internal/compaction/verify_inputs.go`) before anything is merged, whether or not the entry is stamped, and independently of `--skip-input-checksum-verify`, which opts out of hashing but not of scope; the checksum gate itself returns the same error for a stamped foreign path as defence in depth. Like a mismatch, the refusal is terminal for the pass (absent from `isRetryable`), and the ERROR names the path and bucket. No writer produces cross-bucket entries — CDC flush, `cdc-init`, the compactor and `manifest-reconcile` all emit bucket-relative keys; the format merely tolerates absolute URIs (#249 review) — so the only affected shape is a hand-built manifest, and the remedy is to correct it.

**Residual — what is still unverified.** Query-time reads are **not** checked. No federated read compares bytes against a checksum, and per-query verification was rejected outright (#347 D4): it would re-download every scanned object on every query, turning a metadata-driven scan into a full-bytes transfer. The upload window is uncovered for a structural reason — the digest comes from a read-back of the *published* object, so anything that mangles bytes between the DuckDB encoder and the store's durable copy is hashed **as** the truth; the checksum detects later mutation, not upload mangling. Detection latency is consequently bounded by operational cadence rather than by query traffic: a corrupt object surfaces on the next `--verify-checksums` run or the next rewrite that consumes it, whichever comes first. The honest summary is a changed failure mode, not an eliminated one — from *invisible forever* to *detected within one scrub or compaction cycle for stamped objects, and attributable to a named object after the fact*.

**Writer- and reader-side CRCs stay dead on the pinned engine.** Both directions were probed on DuckDB **v1.4.5** during #347 and neither is available, which is why the digest lives in the manifest at all. The parquet `COPY` writer accepts no checksum option — `WRITE_PAGE_CHECKSUM`, `PAGE_CHECKSUM`, `CHECKSUM` and `CRC` are each rejected — and no reader-side setting asks DuckDB to verify a page CRC it did not write; `parquet_metadata()` still exposes no CRC column, so a CRC written by some other producer could not be read out and checked in SQL either. An engine upgrade that adds either capability should re-open this deliberately rather than by accident: writer page CRCs would shrink the upload window, and reader-side verification would close the query-time hole D4 declined to pay for by re-downloading.

## **8. Observability**

The following metrics MUST be emitted through the engine's metric sink, which
delivers them to the `forma.MetricEmitter` the embedder set on
`Config.Metrics.Emitter` (backend-neutral; see `docs/telemetry.md`):

* `fed_query_latency_histogram`: Labeled by `{stage: "translation", "execution", "streaming"}`. The stages are disjoint wall-time intervals: `translation` is SQL rendering, `execution` is the DuckDB `Query` call, `streaming` is the row iteration and handler loop. The execution plan's `duckdb_fetch` timing is `execution + streaming`. Emitted for every successful DuckDB pass, whether or not the caller requested an execution plan.
* `fed_query_row_count`: Labeled by `{source: "pg", "duckdb"}`. `pg` is the size of the dirty set fetched from Postgres for the anti-join (§3.2); `duckdb` is the row count returned by the merged DuckDB scan. A `pg` series that stays large relative to `duckdb` means the hot tier is carrying rows that a CDC flush or compaction should have moved out (helps tune flush and compaction frequency). There is no `s3` series.
* `fed_query_pushdown_efficiency`: Labeled by the queried `schema_id`. Dirty-set size (the `pg` row count above) over the final matching row count. This is a proxy for `PG_Scan_Rows / Final_Result_Rows`: Forma never observes how many rows the `postgres_scan` inside the `pg_source` CTE touched, and the dirty set is the upper bound of hot rows that scan can return when nothing is pushed down. A high value means the hot tier is large relative to what the query returns; it does not by itself prove the predicate was not pushed down. Measuring the real scan count, or retiring the gauge, is #596.

All six samples are emitted together once the pass has succeeded. A pass that fails at rendering, at the DuckDB `Query` call or mid-stream emits nothing, so a query answered by the corrupt-parquet retry (§7.3, #251) is counted once, from the pass that produced the returned page — the same pass the rewound execution plan describes.

The execution plan and response metadata MUST include:

* `consistency_mode`: The requested freshness contract (`strict` or `eventual`).
* `degraded_mode`: Boolean indicating whether results are partial due to a degraded data source.
* `circuit_breaker_state`: Current breaker state (`closed`, `open`, `half_open`) when relevant.
* `source_availability`: Per-source status snapshot (PG available, S3 available).
* `warning`: Human-readable warning when results are partial or consistency is reduced.