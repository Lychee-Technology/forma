package internal

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// Precondition operands on real PostgreSQL (#649 over #677). A precondition
// is the condition DSL rendered by the shared renderer, so an operand the
// renderer answers wrongly lets a false precondition authorize a write. The
// expected outcomes come from exact arithmetic over the stored values and
// from Go string matching, never from Query: Query and the guard share the
// renderer, so their agreement proves nothing about either.

// casRoute is one write entry point that renders a precondition.
type casRoute int

const (
	routeUpdate casRoute = iota
	routeDelete
	routeTxUpdate
	routeTxDelete
)

var casRoutes = []casRoute{routeUpdate, routeDelete, routeTxUpdate, routeTxDelete}

func (r casRoute) String() string {
	return [...]string{"Update", "Delete", "Tx.Update", "Tx.Delete"}[r]
}

// casRow is a row holding one attribute. A delete that lands replaces it
// with a recreated row, so id is the current row's.
type casRow struct {
	id    uuid.UUID
	attr  string
	value any
}

type casEnv struct {
	*condPG
	sentinel uuid.UUID
	writes   int
}

func newCasEnv(t *testing.T, numericEAV bool) *casEnv {
	t.Helper()
	e := newCondPG(t)
	if numericEAV {
		_, err := e.pool.Exec(e.ctx, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN value_numeric TYPE NUMERIC", sanitizeIdentifier(e.tables.EAVData)))
		require.NoError(t, err)
	}
	return &casEnv{condPG: e, sentinel: e.create(t, map[string]any{"name": "sentinel"})}
}

func (e *casEnv) seedRow(t *testing.T, attr string, value any) *casRow {
	t.Helper()
	return &casRow{id: e.create(t, map[string]any{attr: value}), attr: attr, value: value}
}

// check sends cond as the precondition of a write on row through route and
// asserts the outcome want names. A write that holds lands. One that does
// not is ErrPreconditionFailed and leaves entity_main, eav_data and
// change_log as they were; on a Tx route the Tx then writes the sentinel
// row and commits, which shows the refusal left it usable.
func (e *casEnv) check(t *testing.T, route casRoute, row *casRow, cond forma.Condition, want bool) {
	t.Helper()
	e.writes++
	label := fmt.Sprintf("%s on %s=%v with %s", route, row.attr, row.value, describeCondition(cond))
	written := time.Unix(int64(1_700_000_000+e.writes), 0).UTC()
	var before rowSnapshot
	if !want {
		before = e.snapshot(t, row.id)
	}
	err := e.write(route, row.id, cond, written)
	if !want {
		require.ErrorIs(t, err, forma.ErrPreconditionFailed, label)
		require.NotErrorIs(t, err, forma.ErrNotFound, label)
		require.Equal(t, before, e.snapshot(t, row.id), "%s: every table as it was", label)
		if route == routeTxUpdate || route == routeTxDelete {
			require.Equal(t, written, e.get(t, e.sentinel)["due_iso"], "%s: the Tx committed a later write", label)
		}
		return
	}
	require.NoError(t, err, label)
	if route == routeUpdate || route == routeTxUpdate {
		require.Equal(t, written, e.get(t, row.id)["due_iso"], "%s: the update landed", label)
		return
	}
	require.Empty(t, e.snapshot(t, row.id).main, "%s: the row is deleted", label)
	row.id = e.create(t, map[string]any{row.attr: row.value})
}

func (e *casEnv) write(route casRoute, rowID uuid.UUID, cond forma.Condition, written time.Time) error {
	switch route {
	case routeUpdate:
		_, err := e.em.Update(e.ctx, condUpdate(rowID, cond, map[string]any{"due_iso": written}))
		return err
	case routeDelete:
		return e.em.Delete(e.ctx, condDelete(rowID, cond))
	}
	var guarded error
	err := e.txr.InTx(e.ctx, func(ctx context.Context, tx forma.Tx) error {
		if route == routeTxUpdate {
			_, guarded = tx.Update(ctx, condUpdate(rowID, cond, map[string]any{"due_iso": written}))
		} else {
			guarded = tx.Delete(ctx, condDelete(rowID, cond))
		}
		if !errors.Is(guarded, forma.ErrPreconditionFailed) {
			return guarded
		}
		_, err := tx.Update(ctx, condUpdate(e.sentinel, nil, map[string]any{"due_iso": written}))
		return err
	})
	if err != nil {
		return err
	}
	return guarded
}

func describeCondition(cond forma.Condition) string {
	if leaf, ok := cond.(*forma.KvCondition); ok {
		return leaf.Attr + " " + leaf.Value
	}
	return fmt.Sprintf("%+v", cond)
}

// requireQueryRows asserts that a hot-tier Query with cond returns exactly
// want: the read side of the same arithmetic.
func (e *casEnv) requireQueryRows(t *testing.T, cond forma.Condition, want []*casRow) {
	t.Helper()
	expected := make(map[uuid.UUID]bool, len(want))
	for _, row := range want {
		expected[row.id] = true
	}
	require.Equal(t, expected, e.matching(t, cond), "Query %s", describeCondition(cond))
}

// TestPreconditionOperandsReproductions are the reassessment's
// reproductions, each sent through all four write routes with its outcome
// written out by hand. Before #677, rank=7 passed equals:7.9 (pgx truncated
// the operand to the int4 column's width), rank=1 passed gte:1.5 and failed
// lt:1.5, and lt:2147483648 failed to encode after the row lock was taken,
// an internal error that poisoned a Tx. in:[7.9] was already refused (a
// fractional element is dropped); it stays pinned beside equals.
func TestPreconditionOperandsReproductions(t *testing.T) {
	e := newCasEnv(t, true)
	cases := []struct {
		attr  string
		value any
		cond  string
		want  bool
	}{
		{"rank", int64(7), "equals:7.9", false},
		{"rank", int64(7), "in:[7.9]", false},
		{"rank", int64(7), "in:[7.9, 7]", true},
		{"rank", int64(7), "not_equals:7.9", true},
		{"rank", int64(7), "not_in:[7.9]", true},
		{"rank", int64(7), "gt:7.5", false},
		{"rank", int64(7), "lte:7.5", true},
		{"rank", int64(1), "lt:1.5", true},
		{"rank", int64(1), "gte:1.5", false},
		{"rank", int64(7), "lt:2147483648", true},
		{"rank", int64(7), "gt:2147483648", false},
		{"rank", int64(7), "gte:-2147483649", true},
		{"rank", int64(math.MaxInt32), "equals:2147483647", true},
		{"rank", int64(math.MaxInt32), "gt:2147483646.5", true},
		{"rank", int64(-1), "lt:-0.5", true},
		{"rank", int64(-1), "lte:-1.5", false},
		{"rank", int64(-1), "equals:-1.0", true},
		{"level", int64(math.MaxInt16), "lt:32768", true},
		{"level", int64(math.MaxInt16), "gt:32767.5", false},
		{"level", int64(math.MinInt16), "gte:-32768", true},
		{"level", int64(math.MinInt16), "lt:-32768", false},
		{"level", int64(math.MinInt16), "gt:-32769", true},
		{"version", int64(1<<53 + 1), "equals:9007199254740993", true},
		{"version", int64(1<<53 + 1), "equals:9007199254740992", false},
		{"version", int64(1<<53 + 1), "gt:9007199254740992", true},
		{"version", int64(math.MaxInt64), "equals:9223372036854775807", true},
		{"version", int64(math.MaxInt64), "lt:9223372036854775807", false},
		{"version", int64(math.MinInt64), "lte:-9223372036854775808", true},
		{"version", int64(math.MinInt64), "gt:-9223372036854775808", false},
		{"counter", int64(7), "equals:7.9", false},
		{"counter", int64(7), "in:[7.9]", false},
		{"counter", int64(7), "lt:7.5", true},
		{"big", int64(1 << 53), "equals:9007199254740992", true},
		{"big", int64(1 << 53), "lt:9007199254740992", false},
		{"score", 7.9, "gt:7.5", true},
		{"score", 7.9, "equals:7.9", true},
		{"ratio", 7.9, "lt:7.9", false},
	}
	for _, tc := range cases {
		for _, route := range casRoutes {
			row := e.seedRow(t, tc.attr, tc.value)
			e.check(t, route, row, kv(tc.attr, tc.cond), tc.want)
		}
	}
}

// numericCasAttrs are the numeric attributes under test with the values
// their rows hold: every whole-number column width, a double column, and
// eav_data. double marks storage where an operand compares as its float64
// image (#384): the double column, and eav_data's integer and numeric
// classes whatever value_numeric's type. A DOUBLE PRECISION value_numeric
// also holds bigint as its float64 image; production's is NUMERIC.
type numericCasAttr struct {
	attr   string
	eav    bool
	double bool
	values []any
}

func numericCasAttrs() []numericCasAttr {
	return []numericCasAttr{
		{"level", false, false, []any{int64(math.MinInt16), int64(-8), int64(-1), int64(0), int64(1), int64(7), int64(8), int64(math.MaxInt16)}},
		{"rank", false, false, []any{int64(math.MinInt32), int64(-8), int64(-1), int64(0), int64(1), int64(7), int64(8), int64(math.MaxInt32)}},
		{"version", false, false, []any{int64(math.MinInt64), int64(-(1 << 53) - 1), int64(-1), int64(0), int64(7),
			int64(1 << 53), int64(1<<53 + 1), int64(math.MaxInt64)}},
		{"ratio", false, true, []any{-7.5, -1.0, 0.0, 1.0, 7.0, 7.9, 8.0}},
		{"small", true, true, []any{int64(math.MinInt16), int64(-1), int64(0), int64(7), int64(math.MaxInt16)}},
		{"counter", true, true, []any{int64(math.MinInt32), int64(-1), int64(0), int64(1), int64(7), int64(math.MaxInt32)}},
		// EAV bigint is bounded by 2^53 at write (#590).
		{"big", true, false, []any{int64(-(1 << 53)), int64(-1), int64(0), int64(7), int64(1 << 53)}},
		{"score", true, true, []any{-7.5, -1.0, 0.0, 7.0, 7.9}},
	}
}

var casNumericOperands = []string{
	"7", "7.0", "7.5", "7.9", "-7.5", "1.5", "-0.5", "-1.5", "32767", "32768", "-32769",
	"2147483647", "2147483648", "-2147483649", "9007199254740992", "9007199254740993",
	"9223372036854775807", "-9223372036854775808",
}

// TestPreconditionNumericOperandsMatchArithmetic runs every numeric operator
// and operand against every attribute's rows. For each condition it checks
// that Query returns exactly the rows arithmetic selects, then sends it as a
// precondition on one row it selects and one it does not, rotating the
// rows and the four write routes so every route meets every attribute with
// both outcomes. eav_data runs with both value_numeric types.
func TestPreconditionNumericOperandsMatchArithmetic(t *testing.T) {
	for _, numericEAV := range []bool{false, true} {
		t.Run(fmt.Sprintf("numeric_eav=%v", numericEAV), func(t *testing.T) {
			e := newCasEnv(t, numericEAV)
			outcomes := map[string]int{}
			for _, a := range numericCasAttrs() {
				if numericEAV && !a.eav {
					continue // entity_main columns do not depend on value_numeric's type
				}
				if a.eav && !numericEAV {
					a.double = true // a DOUBLE PRECISION value_numeric holds every EAV value as its float64 image
				}
				rows := make([]*casRow, 0, len(a.values))
				for _, v := range a.values {
					rows = append(rows, e.seedRow(t, a.attr, v))
				}
				k := 0
				for _, literal := range casNumericOperands {
					for op, sqlOp := range conformanceOps {
						cond := kv(a.attr, operandLeaf(op, literal))
						operand := toBigFloat(conformanceOperand(literal, a.double))
						var held, failed []*casRow
						for _, row := range rows {
							if cmpHolds(sqlOp, toBigFloat(row.value).Cmp(operand)) {
								held = append(held, row)
							} else {
								failed = append(failed, row)
							}
						}
						e.requireQueryRows(t, cond, held)
						if len(held) > 0 {
							route := casRoutes[k%len(casRoutes)]
							e.check(t, route, held[k%len(held)], cond, true)
							outcomes[fmt.Sprintf("%s %s held", a.attr, route)]++
						}
						if len(failed) > 0 {
							route := casRoutes[(k+1)%len(casRoutes)]
							e.check(t, route, failed[k%len(failed)], cond, false)
							outcomes[fmt.Sprintf("%s %s failed", a.attr, route)]++
						}
						k++
					}
				}
			}
			for _, a := range numericCasAttrs() {
				if numericEAV && !a.eav {
					continue
				}
				for _, route := range casRoutes {
					require.Positive(t, outcomes[fmt.Sprintf("%s %s held", a.attr, route)], "%s %s held", a.attr, route)
					require.Positive(t, outcomes[fmt.Sprintf("%s %s failed", a.attr, route)], "%s %s failed", a.attr, route)
				}
			}
		})
	}
}

// TestPreconditionLikeOperandsAreLiteral sends starts_with and contains
// preconditions whose operands hold LIKE metacharacters. Before #677,
// starts_with:a_c held on a row holding abc, so the write landed.
func TestPreconditionLikeOperandsAreLiteral(t *testing.T) {
	e := newCasEnv(t, true)
	texts := []string{"abc", "a_c", "a%c", `a\c`}
	k := 0
	for _, attr := range []string{"label", "name"} {
		rows := make([]*casRow, 0, len(texts))
		for _, text := range texts {
			rows = append(rows, e.seedRow(t, attr, text))
		}
		for _, operand := range []string{"a_c", "_", "%", `a\`, `\c`, "a%", "a"} {
			for _, op := range []string{"starts_with", "contains"} {
				cond := kv(attr, op+":"+operand)
				var held, failed []*casRow
				for _, row := range rows {
					text := row.value.(string)
					if (op == "starts_with" && strings.HasPrefix(text, operand)) || (op == "contains" && strings.Contains(text, operand)) {
						held = append(held, row)
					} else {
						failed = append(failed, row)
					}
				}
				e.requireQueryRows(t, cond, held)
				for i, row := range failed {
					e.check(t, casRoutes[(k+i)%len(casRoutes)], row, cond, false)
				}
				for i, row := range held {
					e.check(t, casRoutes[(k+i+1)%len(casRoutes)], row, cond, true)
				}
				k++
			}
		}
	}
}

// TestNonFinitePreconditionIsRefusedBeforeAnyStatement: NaN and Inf
// operands, and a bigint operand beyond int64 (#502), are ErrInvalidInput
// naming the attribute on every route, before the write sends SQL. Before
// #677, lt:NaN held on every EAV row, and on an entity_main column it failed
// to encode after the row lock was taken, an internal error that poisoned a
// Tx.
func TestNonFinitePreconditionIsRefusedBeforeAnyStatement(t *testing.T) {
	e := newCasEnv(t, true)
	cases := []struct{ attr, value, want string }{
		{"rank", "lt:NaN", "invalid numeric value for 'rank': NaN"},
		{"level", "gt:-Inf", "invalid numeric value for 'level': -Inf"},
		{"ratio", "gt:Inf", "invalid numeric value for 'ratio': Inf"},
		{"counter", "lt:NaN", "invalid numeric value for 'counter': NaN"},
		{"score", "lte:-Inf", "invalid numeric value for 'score': -Inf"},
		{"small", `in:[1, "NaN"]`, "element at index 1 of the 'in' list for smallint attribute 'small'"},
		{"version", "equals:9223372036854775808", "out of range for bigint attribute 'version'"},
		{"big", "lt:NaN", "out of range for bigint attribute 'big'"},
	}
	row := e.create(t, map[string]any{"rank": int64(7), "level": int64(7), "ratio": 7.0, "counter": int64(7), "score": 7.0,
		"small": int64(7), "version": int64(7), "big": int64(7)})
	before := e.snapshot(t, row)
	requireRefused := func(err error, label, want string) {
		t.Helper()
		require.ErrorIs(t, err, forma.ErrInvalidInput, label)
		message, published := forma.ResolvePublicMessage(err)
		require.True(t, published, label)
		require.Contains(t, message, want, label)
	}
	for _, tc := range cases {
		label := tc.attr + " " + tc.value
		_, err := e.em.Query(e.ctx, &forma.QueryRequest{SchemaName: condSchemaName, Condition: kv(tc.attr, tc.value), Page: 1, ItemsPerPage: 10})
		requireRefused(err, "Query "+label, tc.want)
		_, err = e.em.Update(e.ctx, condUpdate(row, kv(tc.attr, tc.value), map[string]any{"name": "refused"}))
		requireRefused(err, "Update "+label, tc.want)
		requireRefused(e.em.Delete(e.ctx, condDelete(row, kv(tc.attr, tc.value))), "Delete "+label, tc.want)
	}
	require.Equal(t, before, e.snapshot(t, row), "every table as it was")

	err := e.txr.InTx(e.ctx, func(ctx context.Context, tx forma.Tx) error {
		for _, tc := range cases {
			_, err := tx.Update(ctx, condUpdate(row, kv(tc.attr, tc.value), map[string]any{"name": "refused"}))
			requireRefused(err, "Tx.Update "+tc.attr+" "+tc.value, tc.want)
			requireRefused(tx.Delete(ctx, condDelete(row, kv(tc.attr, tc.value))), "Tx.Delete "+tc.attr+" "+tc.value, tc.want)
		}
		_, err := tx.Update(ctx, condUpdate(row, kv("rank", "lt:2147483648"), map[string]any{"name": "after"}))
		return err
	})
	require.NoError(t, err, "the refusals did not poison the Tx")
	require.Equal(t, "after", e.get(t, row)["name"])
}
