package production

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/google/uuid"
)

// AttrProfile produces the attribute payload for the i-th generated create
// (or a partial payload for updates when partial is true).
type AttrProfile func(r *rand.Rand, ordinal int, partial bool) map[string]any

// ScriptSpec describes a deterministic event script: Creates create events
// followed by an interleaving of Updates and Deletes targeting the created
// rows. The same Env seed always yields the same script.
type ScriptSpec struct {
	Schema  SchemaRef
	Creates int
	Updates int
	Deletes int
	Profile AttrProfile // defaults per schema name (full-type for e2e_wide)
}

// GenerateScript builds a seed-deterministic event script. Repeated calls
// continue the Env's single seeded stream with globally increasing ordinals,
// so values that encode the ordinal (titles, counts) stay unique across
// batches while the whole sequence remains reproducible from the seed.
// Update and delete events reference created rows via Event.Target;
// ApplyEvents resolves the row IDs the EntityManager assigns.
func (e *Env) GenerateScript(spec ScriptSpec) []*Event {
	if e.rng == nil {
		e.rng = rand.New(rand.NewSource(e.Seed))
	}
	events := generateScriptFrom(e.rng, spec, e.genOrdinal)
	e.genOrdinal += spec.Creates + spec.Updates
	return events
}

func generateScript(r *rand.Rand, spec ScriptSpec) []*Event {
	return generateScriptFrom(r, spec, 0)
}

func generateScriptFrom(r *rand.Rand, spec ScriptSpec, baseOrdinal int) []*Event {
	profile := spec.Profile
	if profile == nil {
		profile = defaultProfile(spec.Schema)
	}

	events := make([]*Event, 0, spec.Creates+spec.Updates+spec.Deletes)
	creates := make([]*Event, 0, spec.Creates)
	for i := 0; i < spec.Creates; i++ {
		ev := CreateEvent(spec.Schema, profile(r, baseOrdinal+i, false))
		creates = append(creates, ev)
		events = append(events, ev)
	}
	if len(creates) == 0 {
		return events
	}

	// Deletes target distinct created rows; updates may repeat but never
	// touch deleted rows so the expected visible set stays easy to reason
	// about in fixtures.
	deleted := make(map[int]bool, spec.Deletes)
	for i := 0; i < spec.Deletes && i < len(creates); i++ {
		idx := r.Intn(len(creates))
		for deleted[idx] {
			idx = (idx + 1) % len(creates)
		}
		deleted[idx] = true
	}

	for i := 0; i < spec.Updates; i++ {
		idx := r.Intn(len(creates))
		if deleted[idx] {
			continue
		}
		target := creates[idx]
		ev := UpdateEvent(spec.Schema, uuid.Nil, profile(r, baseOrdinal+spec.Creates+i, true))
		ev.Target = target
		events = append(events, ev)
	}
	for idx := range creates {
		if !deleted[idx] {
			continue
		}
		ev := DeleteEvent(spec.Schema, uuid.Nil)
		ev.Target = creates[idx]
		events = append(events, ev)
	}
	return events
}

func defaultProfile(schema SchemaRef) AttrProfile {
	switch schema.Name {
	case "e2e_wide":
		return FullTypeProfile()
	case "e2e_second":
		return SecondProfile()
	case "e2e_nested":
		return NestedProfile()
	default:
		return MinimalProfile()
	}
}

// FullTypeProfile covers one attribute per scalar forma.ValueType on the
// e2e_wide fixture, mixing main-column-bound and EAV-only storage (#174).
// Sortable attributes (count) get unique values so ordered assertions are
// deterministic.
func FullTypeProfile() AttrProfile {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	return func(r *rand.Rand, ordinal int, partial bool) map[string]any {
		attrs := map[string]any{
			"title": fmt.Sprintf("title-%04d-%04d", ordinal, r.Intn(10000)),
			"count": float64(ordinal*10 + r.Intn(10)), // unique per ordinal
			"score": float64(r.Intn(4000)) / 2,        // .0/.5 fractions stay float-exact
			"note":  fmt.Sprintf("note %d for row %d", r.Intn(1000), ordinal),
		}
		if partial {
			return attrs
		}
		attrs["rank"] = float64(r.Intn(100))
		attrs["amount"] = float64(r.Int63n(1_000_000_000))
		attrs["ref"] = deterministicUUID(r).String()
		attrs["active"] = r.Intn(2) == 0
		attrs["born"] = base.AddDate(0, 0, -r.Intn(20000)).Format("2006-01-02")
		attrs["seen"] = base.Add(time.Duration(r.Intn(1_000_000)) * time.Second).Format(time.RFC3339)
		attrs["joined"] = base.AddDate(0, 0, -r.Intn(20000)).Format("2006-01-02")
		attrs["touched"] = base.Add(time.Duration(r.Intn(1_000_000)) * time.Second).Format(time.RFC3339)
		attrs["level"] = float64(r.Intn(200) - 100)
		attrs["qty"] = float64(ordinal*7 + r.Intn(7))
		attrs["total"] = float64(r.Int63n(1_000_000_000))
		attrs["ratio"] = float64(r.Intn(4000)) / 4 // .25 steps stay float-exact
		attrs["token"] = deterministicUUID(r).String()
		return attrs
	}
}

// MinimalProfile fits the e2e_simple fixture.
func MinimalProfile() AttrProfile {
	return func(r *rand.Rand, ordinal int, partial bool) map[string]any {
		return map[string]any{
			"name":  fmt.Sprintf("row-%05d-%04d", ordinal, r.Intn(10000)),
			"value": float64(ordinal) + float64(r.Intn(2))/2,
		}
	}
}

// SecondProfile fits the e2e_second fixture (for #186).
func SecondProfile() AttrProfile {
	return func(r *rand.Rand, ordinal int, partial bool) map[string]any {
		return map[string]any{
			"label": fmt.Sprintf("label-%05d-%04d", ordinal, r.Intn(10000)),
			"code":  float64(ordinal*10 + r.Intn(10)),
		}
	}
}

// NestedProfile fits the e2e_nested fixture (for #260): attributes arrive as
// a nested object so the write path flattens them to dotted names
// ("contact.annualIncome"). annualIncome encodes the ordinal so ordered
// assertions are deterministic.
func NestedProfile() AttrProfile {
	return func(r *rand.Rand, ordinal int, partial bool) map[string]any {
		contact := map[string]any{
			"name":         fmt.Sprintf("contact-%05d-%04d", ordinal, r.Intn(10000)),
			"annualIncome": float64(ordinal*100 + r.Intn(100)),
		}
		if partial {
			return map[string]any{"contact": contact}
		}
		contact["note"] = fmt.Sprintf("note %d for row %d", r.Intn(1000), ordinal)
		return map[string]any{
			"contact": contact,
			"flag":    fmt.Sprintf("flag-%d", r.Intn(3)),
		}
	}
}

// deterministicUUID derives a valid v4-shaped UUID from the seeded stream.
func deterministicUUID(r *rand.Rand) uuid.UUID {
	var b [16]byte
	for i := range b {
		b[i] = byte(r.Intn(256))
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	id, _ := uuid.FromBytes(b[:])
	return id
}
