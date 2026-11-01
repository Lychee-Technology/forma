package federated

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// testPhase is one step of a test that runs under a deadline of its own, so
// that a test with several costly steps reports which one ran out of time
// instead of failing every later step on one shared, already-expired context
// (#781).
type testPhase struct {
	name   string
	budget time.Duration
	start  time.Time
	ctx    context.Context
	cancel context.CancelFunc
}

// startPhase starts the named phase with a deadline budget from now. The
// phase's context is detached from any other, so time an earlier phase used
// does not count against this one. Call end once the phase is over.
func startPhase(name string, budget time.Duration) *testPhase {
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	return &testPhase{name: name, budget: budget, start: time.Now(), ctx: ctx, cancel: cancel}
}

// end releases the phase's context and reports how long the phase ran.
func (p *testPhase) end() time.Duration {
	p.cancel()
	return time.Since(p.start)
}

// expired reports whether the phase's deadline has passed.
func (p *testPhase) expired() bool {
	return errors.Is(p.ctx.Err(), context.DeadlineExceeded)
}

// describe states how the phase failed with err: whether it ran out of its
// budget or failed within it, and after how long.
func (p *testPhase) describe(err error) string {
	elapsed := time.Since(p.start).Round(time.Millisecond)
	if p.expired() || errors.Is(err, context.DeadlineExceeded) {
		return fmt.Sprintf("phase %q ran out of its %v budget after %v: %v", p.name, p.budget, elapsed, err)
	}
	return fmt.Sprintf("phase %q failed after %v: %v", p.name, elapsed, err)
}

// finish ends the phase, failing tb with describe(err) when err is non-nil
// and logging the phase's duration otherwise.
func (p *testPhase) finish(tb testing.TB, err error) {
	tb.Helper()
	if err != nil {
		msg := p.describe(err)
		p.end()
		tb.Fatal(msg)
	}
	tb.Logf("phase %q took %v of its %v budget", p.name, p.end().Round(time.Millisecond), p.budget)
}

func TestPhaseDescribeNamesThePhaseThatRanOutOfTime(t *testing.T) {
	p := startPhase("seed 5000 hot records", time.Millisecond)
	defer p.end()
	<-p.ctx.Done()

	if !p.expired() {
		t.Fatal("expired() = false after the phase deadline passed")
	}
	got := p.describe(fmt.Errorf("insert entity_main: %w", p.ctx.Err()))
	want := `phase "seed 5000 hot records" ran out of its 1ms budget after `
	if !strings.HasPrefix(got, want) {
		t.Fatalf("describe() = %q, want prefix %q", got, want)
	}
}

func TestPhaseDescribeSeparatesAFailureWithinTheBudget(t *testing.T) {
	p := startPhase("CDC flush 2", time.Minute)
	defer p.end()

	if p.expired() {
		t.Fatal("expired() = true before the phase deadline")
	}
	got := p.describe(errors.New("export delta: access denied"))
	want := `phase "CDC flush 2" failed after `
	if !strings.HasPrefix(got, want) {
		t.Fatalf("describe() = %q, want prefix %q", got, want)
	}
}

func TestPhaseDeadlineIgnoresTimeEarlierPhasesUsed(t *testing.T) {
	first := startPhase("setup", time.Millisecond)
	<-first.ctx.Done()
	first.end()

	second := startPhase("CDC flush 0", time.Minute)
	defer second.end()
	if err := second.ctx.Err(); err != nil {
		t.Fatalf("a phase started after an expired one has ctx.Err() = %v, want nil", err)
	}
}
