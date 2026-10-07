package sizeguard

import (
	"errors"
	"fmt"
	"io/fs"
	"slices"
	"strings"
	"testing"
	"testing/fstest"
)

func linesOf(n int) []byte {
	return []byte(strings.Repeat("x\n", n))
}

// packageDir is a package directory holding every case a guard must tell
// apart. The fixtures live in memory: a real 501-line .go file would itself
// break the repository-wide cap that the guards exist to hold.
func packageDir() fstest.MapFS {
	return fstest.MapFS{
		"small.go":             {Data: linesOf(10)},
		"big.go":               {Data: linesOf(501)},
		"small_test.go":        {Data: linesOf(10)},
		"big_test.go":          {Data: linesOf(501)},
		"at_cap_test.go":       {Data: linesOf(500)},
		"unterminated_test.go": {Data: append(linesOf(500), 'x')},
		"notes.txt":            {Data: linesOf(900)},
		"sub/nested.go":        {Data: linesOf(900)},
	}
}

func overCap(name string) string {
	return fmt.Sprintf("%s has 501 lines, exceeds the 500-line source-file limit", name)
}

func TestCountLinesIncludesFinalUnterminatedLine(t *testing.T) {
	cases := []struct {
		name   string
		source []byte
		want   int
	}{
		{name: "empty", source: nil, want: 0},
		{name: "terminated", source: linesOf(500), want: 500},
		{name: "unterminated final line", source: append(linesOf(500), 'x'), want: 501},
	}
	for _, tc := range cases {
		if got := countLines(tc.source); got != tc.want {
			t.Errorf("%s: countLines = %d, want %d", tc.name, got, tc.want)
		}
	}
}

func TestListWatchesEveryGoFileInTheDirectory(t *testing.T) {
	cases := []struct {
		scope Scope
		want  []string
	}{
		{scope: IncludeTests, want: []string{
			"at_cap_test.go", "big.go", "big_test.go", "small.go", "small_test.go", "unterminated_test.go",
		}},
		{scope: ExcludeTests, want: []string{"big.go", "small.go"}},
	}
	for _, tc := range cases {
		got, err := List(packageDir(), tc.scope)
		if err != nil {
			t.Fatalf("scope %d: list: %v", tc.scope, err)
		}
		if !slices.Equal(got, tc.want) {
			t.Errorf("scope %d: List = %v, want %v", tc.scope, got, tc.want)
		}
	}
}

func TestListFailsWhenTheScopeWatchesNothing(t *testing.T) {
	if _, err := List(fstest.MapFS{}, IncludeTests); err == nil {
		t.Error("an empty directory listed without error")
	}
	onlyTests := fstest.MapFS{"only_test.go": {Data: linesOf(1)}}
	if _, err := List(onlyTests, ExcludeTests); err == nil {
		t.Error("a directory of test files listed without error under ExcludeTests")
	}
}

// TestAuditMeasuresEveryListedFile is the consumption half of the scope pin
// (#449). Pinning only what List returns left a gap: a _test.go filter in the
// size loop would have blinded the guard with every scope test still green.
// Here an over-cap test file must be reported, so a filter anywhere between the
// listing and the measurement fails this test.
func TestAuditMeasuresEveryListedFile(t *testing.T) {
	cases := []struct {
		scope Scope
		want  []string
	}{
		{scope: IncludeTests, want: []string{
			overCap("big.go"), overCap("big_test.go"), overCap("unterminated_test.go"),
		}},
		{scope: ExcludeTests, want: []string{overCap("big.go")}},
	}
	for _, tc := range cases {
		got, err := audit(packageDir(), tc.scope)
		if err != nil {
			t.Fatalf("scope %d: audit: %v", tc.scope, err)
		}
		if !slices.Equal(got, tc.want) {
			t.Errorf("scope %d: audit = %q, want %q", tc.scope, got, tc.want)
		}
	}
}

// TestScopeProblemsCatchesAListingThatDisagreesWithTheDirectory feeds the
// cross-check the listings a broken List could produce.
func TestScopeProblemsCatchesAListingThatDisagreesWithTheDirectory(t *testing.T) {
	cases := []struct {
		name    string
		scope   Scope
		listing []string
		want    []string
	}{
		{
			name:    "complete listing",
			scope:   IncludeTests,
			listing: []string{"at_cap_test.go", "big.go", "big_test.go", "small.go", "small_test.go", "unterminated_test.go"},
		},
		{
			name:    "a source dropped",
			scope:   ExcludeTests,
			listing: []string{"big.go"},
			want:    []string{"small.go is outside the file-size guard"},
		},
		{
			name:    "tests dropped from a guard that includes them",
			scope:   IncludeTests,
			listing: []string{"big.go", "small.go"},
			want: []string{
				"no _test.go file in the listing: the guard has stopped covering tests",
				"at_cap_test.go is outside the file-size guard",
				"big_test.go is outside the file-size guard",
				"small_test.go is outside the file-size guard",
				"unterminated_test.go is outside the file-size guard",
			},
		},
		{
			name:    "a test listed by a guard that excludes them",
			scope:   ExcludeTests,
			listing: []string{"big.go", "small.go", "small_test.go"},
			want:    []string{"test file small_test.go included in a guard that excludes tests"},
		},
	}
	for _, tc := range cases {
		got, err := scopeProblems(packageDir(), tc.scope, tc.listing)
		if err != nil {
			t.Fatalf("%s: scopeProblems: %v", tc.name, err)
		}
		if !slices.Equal(got, tc.want) {
			t.Errorf("%s: scopeProblems = %q, want %q", tc.name, got, tc.want)
		}
	}
}

func TestViolationsFailsOnAnUnreadableFile(t *testing.T) {
	_, err := violations(fstest.MapFS{}, []string{"missing.go"})
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("violations error = %v, want one wrapping fs.ErrNotExist", err)
	}
	if !strings.Contains(err.Error(), "missing.go") {
		t.Errorf("violations error %q does not name the file", err)
	}
}

// recordingTB stands in for the *testing.T a guard receives, so the step that
// turns problems into failures is pinned as well: a filter there would blind
// the guard as surely as one in violations.
type recordingTB struct {
	testing.TB
	errors []string
	fatal  string
}

func (r *recordingTB) Helper() {}

func (r *recordingTB) Error(args ...any) {
	r.errors = append(r.errors, fmt.Sprint(args...))
}

func (r *recordingTB) Fatalf(format string, args ...any) {
	r.fatal = fmt.Sprintf(format, args...)
}

func TestCheckFailsTOncePerProblem(t *testing.T) {
	rec := &recordingTB{}
	check(rec, packageDir(), IncludeTests)

	want := []string{overCap("big.go"), overCap("big_test.go"), overCap("unterminated_test.go")}
	if !slices.Equal(rec.errors, want) {
		t.Errorf("check reported %q, want %q", rec.errors, want)
	}
	if rec.fatal != "" {
		t.Errorf("check failed fatally: %s", rec.fatal)
	}
}

func TestCheckFailsFatallyWhenTheGuardCannotRun(t *testing.T) {
	rec := &recordingTB{}
	check(rec, fstest.MapFS{}, IncludeTests)

	if !strings.Contains(rec.fatal, "no guarded files matched *.go") {
		t.Errorf("check fatal = %q, want the listing failure", rec.fatal)
	}
	if len(rec.errors) != 0 {
		t.Errorf("check reported %q alongside a fatal listing failure", rec.errors)
	}
}

// TestAnUnknownScopeFailsTheGuard keeps the scope fail-closed. Scope is an int,
// so a conversion can build a value that is neither declared scope. Unchecked,
// such a value would watch like ExcludeTests: the guard would drop every test
// file and pass. It must stop the guard before anything is measured.
func TestAnUnknownScopeFailsTheGuard(t *testing.T) {
	for _, scope := range []Scope{IncludeTests - 1, ExcludeTests + 1} {
		if got, err := List(packageDir(), scope); err == nil {
			t.Errorf("scope %d: List = %v, want an unknown-scope error", scope, got)
		}

		rec := &recordingTB{}
		check(rec, packageDir(), scope)
		if want := fmt.Sprintf("unknown scope %d", scope); !strings.Contains(rec.fatal, want) {
			t.Errorf("scope %d: check fatal = %q, want it to contain %q", scope, rec.fatal, want)
		}
		if len(rec.errors) != 0 {
			t.Errorf("scope %d: check reported %q alongside the unknown-scope failure", scope, rec.errors)
		}
	}
}
