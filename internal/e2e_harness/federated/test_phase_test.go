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
// budget or failed within it, and after how long. A nil err describes a phase
// whose operation returned no error but only after its deadline passed.
func (p *testPhase) describe(err error) string {
	elapsed := time.Since(p.start).Round(time.Millisecond)
	if err == nil {
		return fmt.Sprintf("phase %q ran out of its %v budget after %v, though its operation returned no error",
			p.name, p.budget, elapsed)
	}
	if p.expired() || errors.Is(err, context.DeadlineExceeded) {
		return fmt.Sprintf("phase %q ran out of its %v budget after %v: %v", p.name, p.budget, elapsed, err)
	}
	return fmt.Sprintf("phase %q failed after %v: %v", p.name, elapsed, err)
}

// finish ends the phase, failing tb with describe(err) when err is non-nil or
// the phase's deadline passed, and logging the phase's duration otherwise. The
// deadline is checked before the context is released, because an operation
// may return nil after its deadline: ClearAllData, for one, discards the
// errors of its statements, so a setup that ran out of time would otherwise
// pass as a success and let the test go on against partly cleared state.
func (p *testPhase) finish(tb testing.TB, err error) {
	tb.Helper()
	if err != nil || p.expired() {
		msg := p.describe(err)
		p.end()
		tb.Fatal(msg)
		return
	}
	tb.Logf("phase %q took %v of its %v budget", p.name, p.end().Round(time.Millisecond), p.budget)
}

// fatalRecorder is a testing.TB that records Fatal and Logf calls instead of
// stopping the test, so finish's outcome can be asserted.
type fatalRecorder struct {
	testing.TB
	fatal  []string
	logged []string
}

func (r *fatalRecorder) Helper() {}

func (r *fatalRecorder) Fatal(args ...any) { r.fatal = append(r.fatal, fmt.Sprint(args...)) }

func (r *fatalRecorder) Logf(format string, args ...any) {
	r.logged = append(r.logged, fmt.Sprintf(format, args...))
}

func TestPhaseFinishFailsAnExpiredPhaseWhoseOperationReturnedNil(t *testing.T) {
	p := startPhase("harness setup", time.Millisecond)
	<-p.ctx.Done()

	rec := &fatalRecorder{TB: t}
	p.finish(rec, nil)

	if len(rec.fatal) != 1 {
		t.Fatalf("finish(nil) after the deadline called Fatal %d times, want 1 (logged %q)", len(rec.fatal), rec.logged)
	}
	want := `phase "harness setup" ran out of its 1ms budget after `
	if !strings.HasPrefix(rec.fatal[0], want) {
		t.Fatalf("finish(nil) failed with %q, want prefix %q", rec.fatal[0], want)
	}
	if len(rec.logged) != 0 {
		t.Fatalf("finish(nil) after the deadline also logged %q, want no success log", rec.logged)
	}
}

func TestPhaseFinishLogsAPhaseThatEndedWithinItsBudget(t *testing.T) {
	p := startPhase("harness setup", time.Minute)

	rec := &fatalRecorder{TB: t}
	p.finish(rec, nil)

	if len(rec.fatal) != 0 {
		t.Fatalf("finish(nil) within the budget failed with %q", rec.fatal)
	}
	want := `phase "harness setup" took `
	if len(rec.logged) != 1 || !strings.HasPrefix(rec.logged[0], want) {
		t.Fatalf("finish(nil) within the budget logged %q, want one line with prefix %q", rec.logged, want)
	}
	if err := p.ctx.Err(); !errors.Is(err, context.Canceled) {
		t.Fatalf("finish left the phase context with Err() = %v, want context.Canceled", err)
	}
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
