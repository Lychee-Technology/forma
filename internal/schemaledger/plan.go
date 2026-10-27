package schemaledger

import (
	"fmt"
	"strings"
)

// Plan is apply-schema's judgement of one schema's declaration against its
// active generation.
type Plan struct {
	SchemaName string
	SchemaID   int16
	// From is the active generation the plan was judged against.
	From int64
	// Hash and Entries make up the generation the plan would record: the
	// declaration's entries plus each entry of the active generation the
	// declaration no longer holds, kept as retired with its recorded type.
	Hash    string
	Entries []Entry
	// Changes lists every attribute whose entry differs from the active
	// generation, additions and removals included, ordered by id.
	Changes []Change
}

// Unchanged reports a declaration identical to the active generation's
// files: there is nothing to record.
func (p Plan) Unchanged() bool { return len(p.Changes) == 0 && p.Hash == "" }

// Refusals answers every refused change, one line each.
func (p Plan) Refusals() []string { return p.reasons(Refused) }

// Warnings answers every allowed change that comes with a warning.
func (p Plan) Warnings() []string { return p.reasons(AllowedWithWarning) }

func (p Plan) reasons(v Verdict) []string {
	var out []string
	for _, c := range p.Changes {
		if c.Verdict == v {
			out = append(out, fmt.Sprintf("schema %s (id %d) attribute %s (id %d): %s to %s: %s",
				p.SchemaName, p.SchemaID, c.New.Name, c.New.AttrID, c.Old.describe(), c.New.describe(), c.Reason))
		}
	}
	return out
}

// PlanApply judges decl against the schema's active generation. A
// declaration whose content hash is the active generation's plans nothing.
// Otherwise every attribute both hold is judged by Judge; an attribute only
// the declaration holds is an addition, and one only the active generation
// holds is carried into the new generation as retired.
func PlanApply(active Generation, decl Declaration) Plan {
	plan := Plan{SchemaName: decl.SchemaName, SchemaID: decl.SchemaID, From: active.Number}
	if decl.Hash == active.Hash {
		return plan
	}
	plan.Hash = decl.Hash
	recorded := active.entryByID()
	declared := make(map[int16]bool, len(decl.Entries))
	for _, next := range decl.Entries {
		declared[next.AttrID] = true
		plan.Entries = append(plan.Entries, next)
		old, ok := recorded[next.AttrID]
		if !ok {
			plan.Changes = append(plan.Changes, Change{New: next, Verdict: Allowed, Reason: "added"})
			continue
		}
		c := Judge(old, next)
		if c.Verdict == Unchanged && old.Retired != next.Retired {
			c.Verdict = Allowed
		}
		if c.Verdict != Unchanged {
			plan.Changes = append(plan.Changes, c)
		}
	}
	for _, old := range active.Entries {
		if declared[old.AttrID] {
			continue
		}
		kept := old
		kept.Retired = true
		plan.Entries = append(plan.Entries, kept)
		if !old.Retired {
			plan.Changes = append(plan.Changes, Change{Old: old, New: kept, Verdict: Allowed, Reason: "removed; kept as retired"})
		}
	}
	sortEntries(plan.Entries)
	sortChanges(plan.Changes)
	return plan
}

func sortChanges(changes []Change) {
	for i := 1; i < len(changes); i++ {
		for j := i; j > 0 && changes[j].New.AttrID < changes[j-1].New.AttrID; j-- {
			changes[j], changes[j-1] = changes[j-1], changes[j]
		}
	}
}

// Describe renders the plan for apply-schema's report, one line per change.
func (p Plan) Describe() []string {
	lines := make([]string, 0, len(p.Changes))
	for _, c := range p.Changes {
		var what string
		switch {
		case c.Reason == "added":
			what = "added as " + c.New.describe()
		case strings.HasPrefix(c.Reason, "removed"):
			what = "removed; its entry is kept as retired " + c.Old.describe()
		case c.Old.sameType(c.New) && c.Old.sameBinding(c.New):
			what = fmt.Sprintf("retired flag %t to %t", c.Old.Retired, c.New.Retired)
		default:
			what = fmt.Sprintf("%s to %s", c.Old.describe(), c.New.describe())
		}
		lines = append(lines, fmt.Sprintf("attribute %s (id %d): %s [%s]", c.New.Name, c.New.AttrID, what, c.Verdict))
	}
	return lines
}

// String names the verdict in reports.
func (v Verdict) String() string {
	switch v {
	case Unchanged:
		return "unchanged"
	case Allowed:
		return "allowed"
	case AllowedWithWarning:
		return "allowed with warning"
	case Refused:
		return "refused"
	}
	return fmt.Sprintf("verdict(%d)", int(v))
}
