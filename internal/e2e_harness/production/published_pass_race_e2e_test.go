//go:build e2e

package production

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"

	fedengine "github.com/lychee-technology/forma/internal/federated"
)

// pausingPublicationReader decorates the engine's publication reader: once
// armed, the first ReadRecord reads the record on the pass's snapshot,
// signals Reached, then blocks until Release (or ctx cancellation). A test
// can therefore run a whole flush strictly between the pass's
// snapshot and everything the pass reads after it: its hot keys, its
// generation's manifest and the DuckDB scan.
type pausingPublicationReader struct {
	fedengine.PublicationReader
	armed       atomic.Bool
	reached     chan struct{}
	release     chan struct{}
	reachOnce   sync.Once
	releaseOnce sync.Once

	mu sync.Mutex
	// resolved are the generations ResolveGeneration answered while armed.
	resolved []fedengine.PublishedGeneration
}

func newPausingPublicationReader() *pausingPublicationReader {
	return &pausingPublicationReader{reached: make(chan struct{}), release: make(chan struct{})}
}

// install wires the reader into env's engine. It must run before the Env's
// first query, which assembles the engine.
func (p *pausingPublicationReader) install(env *Env) {
	env.PublicationReaderWrap = func(inner fedengine.PublicationReader) fedengine.PublicationReader {
		p.PublicationReader = inner
		return p
	}
}

func (p *pausingPublicationReader) Reached() <-chan struct{} { return p.reached }

func (p *pausingPublicationReader) Release() {
	p.releaseOnce.Do(func() { close(p.release) })
}

func (p *pausingPublicationReader) ReadRecord(ctx context.Context, tx pgx.Tx, schemaID int16) (fedengine.PublicationRecord, error) {
	rec, err := p.PublicationReader.ReadRecord(ctx, tx, schemaID)
	first := false
	if p.armed.Load() {
		p.reachOnce.Do(func() { first = true })
	}
	if first {
		close(p.reached)
		select {
		case <-p.release:
		case <-ctx.Done():
		}
	}
	return rec, err
}

// ResolveGeneration records, while armed, the generation the engine's pass
// scans: exactly the objects it answers.
func (p *pausingPublicationReader) ResolveGeneration(ctx context.Context, rec fedengine.PublicationRecord) (fedengine.PublishedGeneration, error) {
	gen, err := p.PublicationReader.ResolveGeneration(ctx, rec)
	if err == nil && p.armed.Load() {
		p.mu.Lock()
		p.resolved = append(p.resolved, gen)
		p.mu.Unlock()
	}
	return gen, err
}

// Resolved answers the generations resolved while armed, in order.
func (p *pausingPublicationReader) Resolved() []fedengine.PublishedGeneration {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]fedengine.PublishedGeneration(nil), p.resolved...)
}

// A flush that publishes while a pass over a published schema is in flight,
// after the pass's snapshot read the record (docs/publication-protocol-v2.md,
// "Readers"): the pass answers from the generation and the dirty set of its
// own snapshot, which agree, so no row is dropped or read twice. Under V1 the
// pass resolved a mutable path set before its dirty scan, and only the #252
// widening covered the gap. The never-published counterpart, a first flush
// or an init handoff racing an UNPUBLISHED pass, is in
// never_flushed_loss_e2e_test.go.
func TestPublishedPassRacingAPublication(t *testing.T) {
	t.Parallel()
	wide := DefaultSchemaFixtures()[1] // e2e_wide

	t.Run("FlushOfNewRows", func(t *testing.T) {
		t.Parallel()
		env := NewEnv(t, SharedCluster(t))
		res := runRacedPublicationQuery(t, env, racedCreates, mustFlushInRace)
		if got := len(res.Records); got != 8 {
			t.Errorf("raced query saw %d rows, want 8: rows a flush marks after the pass's snapshot stay hot in it", got)
		}
	})

	t.Run("FlushOfUpdatedRows", func(t *testing.T) {
		t.Parallel()
		env := NewEnv(t, SharedCluster(t))
		res := runRacedPublicationQuery(t, env, buildClosedUpdates, mustFlushInRace)
		if len(res.Records) != 4 {
			t.Fatalf("raced query saw %d rows, want the 4 updated rows", len(res.Records))
		}
		for _, rec := range res.Records {
			if title := recordTitle(t, env, wide, rec); len(title) < 6 || title[:6] != "closed" {
				t.Errorf("row %s reads title %q, want its raced update (closed-*), not the version the snapshot's generation lists", rec.RowID, title)
			}
		}
	})
}

// mustFlushInRace runs a full flush racing the paused pass.
func mustFlushInRace(ctx context.Context, t *testing.T, env *Env) {
	t.Helper()
	if _, err := env.RunFlushWith(ctx, FlushOverrides{}); err != nil {
		t.Errorf("racing flush: %v", err)
	}
}

// racedCreates is a raced generation: four new rows.
func racedCreates(wide SchemaRef, _ []*Event) []*Event {
	gen2 := make([]*Event, 0, 4)
	for i := 0; i < 4; i++ {
		gen2 = append(gen2, CreateEvent(wide, map[string]any{
			"title": fmt.Sprintf("raced-%02d", i),
			"count": float64(500000 + i),
		}))
	}
	return gen2
}

// runRacedPublicationQuery seeds four rows of e2e_wide and flushes them, so
// the schema is published, applies the second generation raced(gen1), then queries while
// publish runs: the query's pass reads the record on its snapshot, THEN
// publish commits, THEN the pass reads its hot keys and scans. It returns
// the query's first page of ten rows.
func runRacedPublicationQuery(t *testing.T, env *Env, raced func(SchemaRef, []*Event) []*Event,
	publish func(context.Context, *testing.T, *Env)) *QueryResult {
	t.Helper()
	ctx := context.Background()
	// Installed before any event: seeding goes through the EntityManager,
	// which assembles the engine on first use. The reader pauses only once
	// armed, right before the raced query.
	pauser := newPausingPublicationReader()
	pauser.install(env)

	wide := DefaultSchemaFixtures()[1]
	gen1 := buildOpenCreates(wide, 4)
	mustApplyEvents(ctx, t, env, "seed generation 1", gen1...)
	mustFlush(ctx, t, env)
	mustApplyEvents(ctx, t, env, "seed generation 2", raced(wide, gen1)...)

	type outcome struct {
		res *QueryResult
		err error
	}
	qctx, cancel := context.WithTimeout(ctx, pausedFlushTimeout)
	done := make(chan outcome, 1)
	finished := make(chan struct{})
	pauser.armed.Store(true)
	go func() {
		defer close(finished)
		res, err := env.Query(qctx, Query{Schema: wide, Limit: 10})
		done <- outcome{res: res, err: err}
	}()
	t.Cleanup(func() {
		pauser.Release()
		cancel()
		<-finished
	})

	select {
	case <-pauser.Reached():
	case out := <-done:
		t.Fatalf("query finished before pausing after its record read: err=%v", out.err)
	}
	publish(ctx, t, env)
	pauser.Release()
	out := <-done
	if out.err != nil {
		t.Fatalf("query racing the publication: %v", out.err)
	}
	if !out.res.Plan.Routing.UseDuckDB {
		t.Errorf("raced query did not take the federated pass: %+v", out.res.Plan.Routing)
	}
	return out.res
}
