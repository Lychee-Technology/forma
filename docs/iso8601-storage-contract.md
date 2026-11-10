# The canonical iso8601 image

A `date` or `datetime` attribute bound to a text column of `entity_main` with
the `iso8601` encoding stores one image per instant. This page records what
that image is, which reads depend on it, and what happens to a row that holds
any other string. The ruling was made on #763.

## The image

The canonical image is RFC 3339 in UTC at whole seconds, with a four-digit
year: `2024-01-02T03:04:05Z`. Every one is exactly 20 bytes,
`YYYY-MM-DDThh:mm:ssZ`, between `0000-01-01T00:00:00Z` and
`9999-12-31T23:59:59Z`. A `date` stores the image of its UTC midnight.
`iso8601.Image` is the only function that renders one.

Every Forma write stores the canonical image. The write funnel
(`internal/transform`) renders the value's epoch milliseconds through
`iso8601.Image`, whatever spelling the caller sent: an offset such as
`2024-01-02T11:04:06+08:00` is stored as `2024-01-02T03:04:06Z`. A value the
image cannot hold is refused as invalid input, not truncated: a fraction of a
second (`… which keeps whole seconds`) or an instant outside years 0000 to
9999. A `$now` write is floored to its second first. Forma has written the
UTC image at whole seconds since the encoding was introduced. Before #582 an
instant outside the four-digit years was written with a wider year, which no
read can parse. Any other non-canonical image in a database was written by
something other than Forma, such as direct SQL or another application.

## What depends on it

The PostgreSQL route compares and sorts the column as text. A filter literal
is bound as its canonical image (`iso8601FilterImage`, #588), and `ORDER BY`
orders the stored text. The DuckDB tiers compare and sort the instant: the
hot leg and the CDC export read the image as epoch milliseconds
(`sqlgen.ISO8601EpochMs`, #756, #755).

For canonical images the two agree. All of them have the same length and put
their separators at the same positions, so two images first differ at a
digit, and the digit that differs first is the most significant field that
differs. Any collation that orders the digits 0 to 9 ascending therefore
orders canonical images by instant, whether or not it weighs the
separators.
`TestCanonicalISO8601ContractNoEAVAllTiers` and its EAV-joined twin pin this
for every comparison operator and both sort directions, on the PostgreSQL
route and on the DuckDB route over hot, delta and base rows.

The text comparison is kept because it is what an index on the column can
seek. A declared index (`x-forma-indexes`) on an iso8601 date-time keeps the
column's collation, which is why #728's `COLLATE "C"` range does not apply to
it (#814). A `$now` comparison maps each operator onto the image of a whole
second and compares text too (`iso8601ServerTimeComparison`), including the
lease conditions of claims and predicate mutations on an attribute bound this
way. Comparing an instant expression instead would make all of these scan.
Revisit that only with benchmark evidence.

## Rows outside the contract

Any other image, such as `2024-01-02T11:04:06+08:00`,
`2024-01-02T03:04:05.5Z` or `2024-01-02 03:04:06`, is outside the query
contract. Which rows a filter on such a row returns, and where a sort places
it, is undefined, and the PostgreSQL route and the DuckDB tiers can disagree.
Today the PostgreSQL route compares the text: `equals:2024-01-02T03:04:06Z`
misses the offset image above, and `gt:2024-01-02T03:04:05Z` misses the
fraction, because `.` sorts before `Z`. An ascending sort places the offset
image by its wall clock, after `10:00:00Z`. The DuckDB tiers compare and sort
the instant each image names. A `GET` returns that instant whenever the image
is RFC 3339.

Forma cannot prevent such rows. The main columns (`text_01`, …) are shared by
every schema, and each schema binds them differently, so no `CHECK`
constraint on the table can say that a column is iso8601 for one schema.
Direct SQL is not stopped. Between two validator runs a non-canonical row can
answer differently on the two routes, with no signal. The contract makes
these rows detectable and gives them a repair, but does not prevent them.

**Detection.** The validator does not report them yet. #762 tracks adding
the check, extended from offset images to every image that differs from
`iso8601.Image` of the instant it parses to. Until then,
[`schema-consistency-migration.md`](schema-consistency-migration.md#iso8601-images-with-a-utc-offset-756)
lists offset images per bound column.

**Repair.** Rewrite the row through the API, with an update that names the
attribute and gives the instant the image names. The write funnel stores the
canonical image and stamps `change_log`, so the next flush replaces the
row's parquet copy. A direct SQL `UPDATE` corrects PostgreSQL only: it
leaves the old copy on the warm and cold tiers until the row's next write.
The API cannot store two kinds of image at the instant they name: a fraction,
and an offset that carries the instant outside years 0000 to 9999. For those
the stored value has to change, by rounding or clamping, and that is a
per-row decision Forma does not make for you. Since the repair writes a
canonical image, re-exporting a row without changing its PostgreSQL value
does not repair it.

## The federated hot leg over pgx

Under the planned hot leg over pgx (`federated-query/design.md` §6.1.1),
PostgreSQL filters a schema's unflushed rows and DuckDB filters its flushed
ones, within one answer. A non-canonical row could then change membership at
its flush. That is a consequence of the row being outside the contract, not a
parity defect. The tier parity suite uses canonical images only, and the
switch of the default transport does not wait on #763. DuckDB keeps comparing
the instant (#761).

The alternative was to have DuckDB compare iso8601 columns as text as well.
It was not chosen: it reverts part of #761, and a DuckDB filter would again
disagree with the instant a read of the same row returns.
