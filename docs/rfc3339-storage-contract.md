# The canonical rfc3339 image

A `date` or `datetime` attribute bound to a text column of `entity_main` with
the `rfc3339` encoding stores one image per instant. This page records what
that image is, which reads depend on it, what happens to a row that holds
any other string, and how a deployment moves off the `iso8601` encoding the
`rfc3339` one replaced. The image was ruled on #763 and given its millisecond
shape on #842.

## The image

The canonical image is RFC 3339 in UTC with exactly three fractional digits
and a four-digit year: `2024-01-02T03:04:05.000Z`,
`2024-01-02T03:04:05.250Z`. Every one is exactly 24 bytes,
`YYYY-MM-DDThh:mm:ss.sssZ`, between `0000-01-01T00:00:00.000Z` and
`9999-12-31T23:59:59.999Z`. `rfc3339.Image` is the only function that
renders one.

The three digits are always written, `.000` included. A minimal spelling
(`…05Z`, `…05.25Z`) was rejected on #842: images of different lengths do not
sort as their instants, since `.` sorts before `Z` and `…05.25Z` would sort
before `…05Z`.

The contract is about the image's shape, not about which instant a `date`
holds. The write funnel treats `date` and `datetime` alike: it stores the
instant the caller gave, and it does not floor a `date` to its day. A `date`
written as a calendar day (`2024-01-02`) names that day's UTC midnight and is
stored as `2024-01-02T00:00:00.000Z`. A `date` written as a full instant
(`2024-01-02T10:00:00.250Z`) is stored at that instant. A `server_time` write
(`$now`) on a `date` is the one write that floors to the UTC day, as a `$now`
comparison on a `date` does.

Every Forma write stores the canonical image, in two steps.

1. **Normalization** turns whatever spelling the caller sent into epoch
   milliseconds, the precision of both types (#589). An offset is resolved
   to UTC, and a fraction finer than a millisecond is floored. Only a value
   that is not a time, or an instant milliseconds cannot name, is refused
   here.
2. **Narrowing** to the column judges those milliseconds and never changes
   them. When the instant is inside years 0000 to 9999, the write funnel
   (`internal/transform`) renders it through `rfc3339.Image`. Otherwise the
   write is refused as invalid input (`… which keeps years 0000 to 9999`),
   not stored with a year the read cannot parse.

So `2024-01-02T11:04:06.250+08:00`, `2024-01-02T03:04:06.25Z` and
`2024-01-02T03:04:06.2509Z` are all stored as `2024-01-02T03:04:06.250Z`.
The last one floors to `.250` at normalization. A `server_time` write on a
`datetime` keeps its millisecond.

Before #842 the encoding was named `iso8601` and kept whole seconds: it
stored `2024-01-02T03:04:06Z` and refused any instant off a whole second.
Before #582 an instant outside the four-digit years was written with a wider
year, which no read can parse. Any other non-canonical image in a database
was written by something other than Forma, such as direct SQL or another
application.

## What depends on it

The PostgreSQL route compares and sorts the column as text. A filter literal
is bound as its canonical image (`rfc3339FilterImage`, #588), and `ORDER BY`
orders the stored text. The DuckDB tiers compare and sort the instant: the
hot leg and the CDC export read the image as epoch milliseconds
(`sqlgen.RFC3339EpochMs`, #756, #755), and the DuckDB outer select renders
the instant back as the canonical image, three digits included
(`rfc3339ImageFromEpochMs`). Parquet holds epoch milliseconds, never the
image.

For canonical images the two agree. All of them have the same length and put
their separators at the same positions, so two images first differ at a
digit, and the digit that differs first is the most significant field that
differs. Any collation that orders the digits 0 to 9 ascending therefore
orders canonical images by instant, whether or not it weighs the
separators. `TestCanonicalRFC3339ContractNoEAVAllTiers` and its EAV-joined
twin pin this for every comparison operator and both sort directions, with
images that differ only in milliseconds, on the PostgreSQL route and on the
DuckDB route over hot, delta and base rows.

The text comparison is kept because it is what an index on the column can
seek. A declared index (`x-forma-indexes`) on an rfc3339 date-time keeps the
column's collation, which is why #728's `COLLATE "C"` range does not apply to
it (#814). A `$now` comparison compares the column with the canonical image
of the transaction time, to the millisecond, with the same operator
(`rfc3339MillisImage`), including the lease conditions of claims and
predicate mutations on an attribute bound this way. Comparing an instant
expression instead would make all of these scan. Revisit that only with
benchmark evidence.

## Rows outside the contract

Any other image, such as `2024-01-02T11:04:06+08:00`,
`2024-01-02T03:04:05.5Z`, the whole-second `2024-01-02T03:04:05Z` or
`2024-01-02 03:04:06`, is outside the query contract. Which rows a filter on
such a row returns, and where a sort places it, is undefined, and the
PostgreSQL route and the DuckDB tiers can disagree. Today the PostgreSQL
route compares the text: `equals:2024-01-02T03:04:06Z` (bound as
`…06.000Z`) misses the offset image above, and `gt:2024-01-02T03:04:05.999Z`
matches the whole-second `…05Z`, because `Z` sorts after `.`. An ascending
sort places the offset image by its wall clock, after `10:00:00.000Z`. The DuckDB tiers
compare and sort the instant each image names. A `GET` returns that instant
whenever the image is RFC 3339.

Forma cannot prevent such rows. The main columns (`text_01`, …) are shared by
every schema, and each schema binds them differently, so no `CHECK`
constraint on the table can say that a column is rfc3339 for one schema.
Direct SQL is not stopped. Between two validator runs a non-canonical row can
answer differently on the two routes, with no signal. The contract makes
these rows detectable and gives them a repair, but does not prevent them.

**Detection.** `rfc3339.Judge` decides whether a stored image is canonical:
it parses the image as the read does and compares it with `rfc3339.Image`
of the millisecond it names. The storage-domain census
(`widthaudit.DomainCensus`) judges every image of every rfc3339 column
through it and reports each one that is not canonical, with its schema,
attribute, row, stored image and repair kind below, apart from images the
read refuses. `validate-schema-consistency` fails on both kinds;
`baseline-schema` refuses the images the read refuses and reports the
others without refusing, since the read serves their rows. Shape alone is
never proof: `2024-02-30T03:04:05.250Z` has the canonical shape and names
no day, so it is reported as an image the read refuses.
`migrate-rfc3339` lists, through the same rule, every image in the columns
it migrates that its rewrite does not make canonical. After the migration
and the repair of the rows it lists, the census finds nothing
(`TestRFC3339MigrationKeepsEveryTierConsistent`).

**Repair.** How a row is repaired depends on whether the read parses its
image. An image the read parses names an instant, and the API can rewrite
it. An image the read refuses names none, so there is no instant to keep,
and the one of them that blocks the migration from iso8601 has to be
repaired before any release can write it; it is repaired offline
([below](#repairing-an-image-the-read-refuses)).

Rewrite a row whose image the read parses through the API, with an update
that names the attribute and gives the instant the image names. The write
funnel stores the canonical image and stamps `change_log`, so the next
flush replaces the row's parquet copy. A direct SQL `UPDATE` alone corrects
PostgreSQL only: it leaves the old copy on the warm and cold tiers until
the row's next write. Since the repair writes a canonical image,
re-exporting a row without changing its PostgreSQL value does not repair
it.

That the API accepts the rewrite does not mean the row keeps the instant its
image names: normalization can move the instant before narrowing judges it.
A repair of an image the read parses is one of three kinds.

- **Spelling only.** The instant the image names is a whole millisecond
  inside years 0000 to 9999: an in-range offset image
  (`2024-01-02T11:04:06.250+08:00`), a fraction of one to three digits
  (`2024-01-02T03:04:06.5Z`), or the whole-second image of the iso8601
  encoding, which `migrate-rfc3339` rewrites. The rewrite stores that same
  instant and changes only its spelling.
- **Accepted, but the instant moves.** A fraction finer than a millisecond
  that is not zero beyond the third digit: `2024-01-02T03:04:06.0000009Z`
  names an instant 900 nanoseconds after the second. Normalization floors it
  to `.000`, so the API accepts the rewrite but stores
  `2024-01-02T03:04:06.000Z`. The change is below the types' millisecond
  precision, and it is the value a write of the same image through the API
  would have stored, but it is not only a change of spelling.
- **Refused.** An offset carries the instant outside years 0000 to 9999
  (`9999-12-31T23:30:00-01:00`). The API refuses the instant the image
  names, so the value has to change, by clamping, before any rewrite can
  store it.

Only the first kind repairs without a decision. For the other two the stored
instant changes, so repairing them is a per-row decision Forma does not make
for you. The census reports the kind of each image (`spelling only`,
`sub-millisecond`, `no canonical image`), so the first kind can be told
apart from the other two.

### Repairing an image the read refuses

`rfc3339.Parse` refuses an image with no `T` or no zone, such as the
space-separated `2024-01-02 03:04:06`, and one whose fields name no time,
such as `2024-02-30T03:04:05.250Z`, `2024-01-02T25:04:05.000Z` or the
whole-second `2023-02-29T00:00:00Z`. None of them is a respelling: every
`GET` of the row fails, and so does every update that leaves the attribute
out. The census reports them apart from the three kinds above, and
`migrate-rfc3339` lists each as `not an RFC 3339 image the read parses`.

The DuckDB tiers read the same image through `TRY_CAST`
(`sqlgen.RFC3339EpochMs`), on the hot leg and in the CDC export. An image
naming no day, such as `2023-02-29T00:00:00Z`, reads there as an absent
attribute, and the space-separated image as its wall clock in UTC. So the
row's parquet copy holds a value PostgreSQL does not, and the repair has to
re-export the row whatever value it chooses.

What the attribute should hold is a per-row decision Forma does not make
for you: the instant the writer meant, when the source of the data can tell
(for `2023-02-29T00:00:00Z` that may be `2023-02-28` or `2023-03-01`), or no
value. Write an instant as its canonical image (`YYYY-MM-DDThh:mm:ss.sssZ`),
never in the iso8601 shape, or write `NULL`.

With the schema's writers stopped, rewrite each row in one transaction that
also queues it for the CDC flush, as an API write does:

```sql
-- one transaction per row; text_02 stands for the bound column, and
-- entity_main and change_log for the deployment's tables
BEGIN;
UPDATE entity_main
   SET text_02 = '2023-03-01T00:00:00.000Z',
       ltbase_updated_at = GREATEST(floor(extract(epoch from now()) * 1000)::bigint,
                                    ltbase_updated_at + 1)
 WHERE ltbase_schema_id = <schema id> AND ltbase_row_id = '<row id>'
   AND text_02 = '2023-02-29T00:00:00Z';
INSERT INTO change_log (schema_id, row_id, flushed_at, changed_at, deleted_at)
SELECT ltbase_schema_id, ltbase_row_id, 0, ltbase_updated_at, ltbase_deleted_at
  FROM entity_main
 WHERE ltbase_schema_id = <schema id> AND ltbase_row_id = '<row id>'
ON CONFLICT (schema_id, row_id, flushed_at)
DO UPDATE SET changed_at = EXCLUDED.changed_at, deleted_at = EXCLUDED.deleted_at;
COMMIT;
```

The `UPDATE` must report one row. If it reports none, the row no longer
holds the image the listing named: `ROLLBACK` and list again. The match on
the image is what keeps the repair from overwriting a value someone else
already corrected.

The `change_log` half is what keeps the tiers consistent. The row's
version, `ltbase_updated_at`, advances by the rule every Forma write applies
(#274), and the slot-0 entry carries it as `changed_at`, which becomes the
new parquet copy's `ver_ts`. That is strictly above the `ver_ts` of every
copy already exported, so the new copy wins the merge on the warm and cold
tiers; an export at the old version could tie with the stale copy, and a
tie has no defined winner. From the commit on, the entry puts the row in
the dirty set, so the DuckDB route discards its parquet copy and reads it
from PostgreSQL. The next `cdc-flush` exports it, and compaction carries
that copy into base. `deleted_at` copies the row's delete state, so a
deleted row stays deleted. The row's `updatedAt` advances, as on any write.
Without the `INSERT`, the DuckDB route keeps serving the old copy (an
absent attribute for `2023-02-29T00:00:00Z`) until the row's next write.

The repair does not touch the unique-key table. If the attribute is part of
a unique key, build the key again with writers still stopped:
`build-unique-keys --schema <name> --key <key> --rebuild`, or, in the
migration below, step 3, which rebuilds every such key. A value the repair
made a duplicate fails that build. After the repair the census no longer
reports the row (`TestRFC3339UnreadableImageOfflineRepair` runs this block
over a row already exported, then checks both routes on every tier).

## Migrating from iso8601 (#842)

The `iso8601` encoding stored `YYYY-MM-DDThh:mm:ssZ`. Its images and the
`rfc3339` ones cannot share a column, because the text comparison would
misorder them, so every whole-second image is rewritten before the new
release starts. Registration refuses a declaration that still says
`iso8601`, with no alias, and names the migration tool: a server whose
attributes files say `iso8601` does not start. A generation the ledger
recorded with `iso8601` still loads; the refusal applies to declarations,
not to recorded history.

Every writer of a migrated schema must be stopped from the rewrite until the
new release starts. The procedure:

1. **Run `forma-tools migrate-rfc3339` with writers stopped.** It reads its
   targets from each schema's active generation in the ledger
   (`<registry>_generation_attr`, encoding `iso8601`, retired attributes
   included), not from the attributes files. It rewrites each whole-second
   image (`^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$`) that
   the read parses to its first 19 characters plus `.000Z`, which is the
   canonical image of the same instant. It reads the whole-second-shaped
   images in pages of at most 1,000 rows (`--batch-size`, under Aurora
   DSQL's 3,000-row transaction limit), judges each one with
   `rfc3339.Judge`, and rewrites the page's valid ones in one statement, its
   own transaction, matching each row on the image it read. It judges every
   other image in those columns the same way, and lists each one that is
   not canonical with its schema, attribute, row and repair kind, leaving it
   unchanged. An image the read parses (an offset, a short fraction) does
   not block the cutover: repair it through the new release's API once it
   runs. An image the read refuses, such as the canonical shape naming no
   time (`2024-01-02T25:04:05.000Z`), does not block it either; repair it
   offline as below, with the schema's writers stopped. One of the
   whole-second shape that names no instant (`2023-02-29T00:00:00Z`) is
   listed and left unchanged too, but it still counts as a whole-second
   image: under the text comparison it misorders against the rfc3339 images
   like any other. So the tool keeps exiting non-zero, and `apply-schema`
   keeps refusing the rename, until that row is repaired. No release can
   repair it through the API in that window: the old release must not write
   once the rewrite has begun, and the new one does not start before step 4.
   Repair it offline with writers still stopped
   ([Repairing an image the read refuses](#repairing-an-image-the-read-refuses)),
   then run the tool again. A run lists after its rewrite, so the listing
   names what the column holds when the tool exits; `--dry-run` lists the
   same rows, so a dry run before writers stop gives time to decide each
   row's value. A re-run finds nothing to rewrite. `--dry-run` reports the
   per-column counts and changes nothing. The tool exits non-zero while any
   whole-second-shaped image remains in a target column, a dry run
   included, so a scripted cutover stops before step 2. Its error says how
   many of those images name no instant and need the offline repair, apart
   from those a writer may have stored again. Like `build-unique-keys`, it
   cannot detect a running writer.

   The rewrite does not stamp `change_log`. The instant does not change, and
   a parquet copy holds the instant as epoch milliseconds, not the image:
   the CDC export reads `…05Z` and `…05.000Z` as the same milliseconds
   (`TestCastDateMainValue_ExecutesToEpochMs`), so no copy goes stale.
   `TestRFC3339MigrationKeepsEveryTierConsistent` migrates rows already on
   the cold, warm and hot tiers and checks that both routes answer alike.
2. **Change `iso8601` to `rfc3339`** in the attributes files and run
   `apply-schema`. It judges `iso8601 → rfc3339` on the same column as a
   binding-only change, which the type rules allow (#733). It refuses the
   change while the column still holds a whole-second-shaped image, valid
   or not, naming the
   schema, attribute, column and remaining row count and pointing at the
   tool, which it counts with (`--entity-main-table` names the table). This
   is the one apply-schema judgement that reads stored data.
3. **Rebuild every unique key that includes such an attribute**: run
   `build-unique-keys --schema <name>` for each such schema.
   `uniquekey.AttributesHash` includes the column encoding, so the server
   refuses to start until the key is built again, and the build rebuilds
   every key whose storage changed ([unique-keys.md](unique-keys.md)).
4. **Start the new release.** The data-plane gate refuses an old-release
   process, because its declared binding (`iso8601`) differs from the
   active generation.

## The federated hot leg over pgx

Under the planned hot leg over pgx (`federated-query/design.md` §6.1.1),
PostgreSQL filters a schema's unflushed rows and DuckDB filters its flushed
ones, within one answer. A non-canonical row could then change membership at
its flush. That is a consequence of the row being outside the contract, not a
parity defect. The tier parity suite uses canonical images only, and the
switch of the default transport does not wait on #763. DuckDB keeps comparing
the instant (#761).

The alternative was to have DuckDB compare rfc3339 columns as text as well.
It was not chosen: it reverts part of #761, and a DuckDB filter would again
disagree with the instant a read of the same row returns.
