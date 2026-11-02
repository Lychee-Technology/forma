# Implementation plan: federated hot leg over pgx (#719)

This plan builds the design in [design.md §6.1.1](design.md). The federated pass stops reading PostgreSQL through DuckDB. It reads the dirty set and the hot keys over pgx in one `REPEATABLE READ READ ONLY` snapshot, and DuckDB reads only the parquet tiers. Go merges the two results. Once this lands and #785 is in place, no Forma deployment needs `postgres_scanner`. #631 is then superseded, and #691 and #754 have nothing left to assess.

## Before the first change

Three inputs gate the work. The first two gate the switch of the default path, not the code.

- **#728 and #763 rulings.** Under this design PostgreSQL filters the hot rows and DuckDB filters the parquet rows of one answer. Until each difference has a ruling and the ruling ships on both dialects, a row's inclusion depends on whether it has been flushed. The tier parity suite (step 6) fails on both differences until then.
- **The latency baseline** that #719 asked for. It measures how federated latency splits between the PostgreSQL copy and the parquet scan on today's path, with the benchmark harness, at several `entity_main` sizes for a fixed hot set. Steps 3 and 5 compare against it. Without it, "cheaper" in the design is a claim nobody has checked.

## Steps

Each step is one PR and leaves `main` releasable. Steps 1 to 5 change no answer: the new path stays off until step 6.

1. **The snapshot reader and the `change_log` index.** Replace `PostgresDirtyIDFetcher`'s strict fetch with a reader that, in one read-only `REPEATABLE READ` transaction, returns the dirty set for either tier form. The hot form reads the widened `flushed_at = 0 OR flushed_at >= cutoff`, the hot-excluded form the strict set. The reader also returns `HasParquetHistory` from the same snapshot. Add the `(schema_id, flushed_at)` index, including `row_id` and `changed_at`, to `init-db`. Measure staging the dirty set through the Appender against binding it as a `UUID[]` parameter, at 1,000, 10,000 and 100,000 ids. The result decides the transport in step 3. Validation: `EXPLAIN` in an integration test shows the dirty-set read using the index, and the result matches today's sets.

2. **Sort keys and the comparator, in Go.** Derive each hot sort key from its stored value by the CDC export's rules (design.md §6.1.1, the table under *Ordering parity*). Reuse #785's stage decode where it applies. Add the comparator, with `NULLS LAST` in both directions, and the three-valued keyset predicate. Validation: a lockstep test writes rows that reach each derivation's edges, flushes them, and requires the parquet leg's values and `ORDER BY` order to match the Go derivation and comparator. Before writing the `DOUBLE` comparator, a test against DuckDB checks how the engine orders NaN and -0. **Decision point:** sort keys on list attributes. Either implement DuckDB's `LIST` order, or refuse such a sort on the federated route with a 4xx.

3. **The parquet-only template.** Add a template without `pg_source` and without the `ATTACH`. It reads `dirty_ids` from the staged set, renders explicit `NULLS LAST`, and takes its parquet window (`OFFSET max(0, offset-h)`) as a parameter. The plan-cache scope of this template drops the connection string and the dirty-set flag. Today's template stays. Validation: the §5 sketch test gains a parquet-only twin. The compiled-plan cache keeps one entry per shape, whatever the dirty set's size.

4. **The hot leg's PostgreSQL queries.** Add the hot-keys query: the PostgreSQL route's condition (`ToDualClauses`' PostgreSQL payload), restricted to `entity_main` rows with an entry in the dirty window, `ver_ts` from the newest entry. Add the page read by `row_id`, reusing the PostgreSQL route's reader. Validation: the condition-DSL parity cases run against the hot-keys query and the PostgreSQL route, and must select the same rows.

5. **Engine wiring, behind a switch.** Add `DuckDBConfig.HotLegTransport` (`attach`, the default, or `pgx`). The `pgx` pass runs the six steps of design.md §6.1.1: snapshot, sort, stage, DuckDB, merge, page read. The same step also changes the following:
   - totals and the #181 recount;
   - the keyset coordinator's cursor-free recount;
   - the #720 empty-set short-circuit;
   - classification: snapshot failures are `ErrPostgresReadFailed` and stay out of the breaker;
   - the disjointness guard;
   - `HotLegMaxDirtyRows`;
   - the execution plan's sources: the hot leg is recorded as a PostgreSQL source whose predicate PostgreSQL evaluates, in place of `hot-leg predicate (evaluated in DuckDB)`;
   - metrics: a latency stage for the snapshot reads, and a hot-match count beside `fed_query_row_count{source="pg"}`. Both change `docs/telemetry.md`.

   Validation: the federated e2e suite and the production harness run under both values of the switch.

6. **Parity and the default.** Add the tier parity suite: each condition-DSL case over the same rows before and after their flush must match the same rows. Add a dual-run test that answers each request under both transports and requires equal pages, order and totals. Its fixtures put rows in every tier, at every sort-key edge from step 2, and in the flush race (a mark landing between path resolution and the snapshot). Flip the default to `pgx` once both pass with #728 and #763 shipped. Rolling back is setting the switch to `attach`.

7. **Retire the attach path.** Remove the `attach` value, `pg_source`, the `ATTACH`, its guard and `queryWithPostgres`, the catalog alias, the DuckDB consumer of the main-table fragment, and `postgres_scanner` from the federated client's required steps. Replace `TestHotLegCopiesWholeTables` with a test that the federated client neither loads `postgres_scanner` nor attaches. Then update the docs: design.md §5, §6.1 and §7.5 describe the new path, §6.1.1 folds into them, and `aurora-dsql.md` drops the extension row. Close #631, #691 and #754 with a pointer here.

## Risks

- **Parity gaps the suites miss.** A predicate or ordering difference that no fixture reaches splits answers by tier silently: a row appears or disappears, or changes position, when it is flushed. The suites are the only guard, so fixtures come from the condition-DSL catalogue and from every derivation row, not from examples.
- **Hot rows that now fail the read.** A hot row whose stored image the OLTP read refuses (#708, #592) fails a federated page that answered before. The PostgreSQL route already fails on that row. Run the census before flipping the default, and repair what it reports.
- **Connection use.** A `pgx` pass holds one pooled PostgreSQL connection for its whole duration, DuckDB included. Size the pool for concurrent federated passes before flipping the default. If that is not affordable, use the re-read-and-retry variant from design.md §6.1.1.
- **A large backlog.** With CDC stalled, every pass reads and stages the whole dirty set. That is no worse than today, which copies all of `change_log`. `HotLegMaxDirtyRows` lets a deployment send such passes to PostgreSQL alone.

## Fallback

If the parity rulings or the tier parity suite stall, the alternative recorded in design.md §6.1.1 still removes `postgres_scanner`: stage the dirty rows in DuckDB over pgx and keep the merge there. Step 1, the staging half of step 3, and step 7 carry over. Steps 2, 4 and 6 are not needed for it.
