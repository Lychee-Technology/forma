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

// GenerateScript builds a seed-deterministic event script. Update and
// delete events reference created rows via Event.Target; ApplyEvents
// resolves the row IDs the EntityManager assigns.
func (e *Env) GenerateScript(spec ScriptSpec) []*Event {
	r := rand.New(rand.NewSource(e.Seed))
	return generateScript(r, spec)
}

func generateScript(r *rand.Rand, spec ScriptSpec) []*Event {
	profile := spec.Profile
	if profile == nil {
		profile = defaultProfile(spec.Schema)
	}

	events := make([]*Event, 0, spec.Creates+spec.Updates+spec.Deletes)
	creates := make([]*Event, 0, spec.Creates)
	for i := 0; i < spec.Creates; i++ {
		ev := CreateEvent(spec.Schema, profile(r, i, false))
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
		ev := UpdateEvent(spec.Schema, uuid.Nil, profile(r, spec.Creates+i, true))
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

// SecondProfile fits the e2e_second fixture (for #189).
func SecondProfile() AttrProfile {
	return func(r *rand.Rand, ordinal int, partial bool) map[string]any {
		return map[string]any{
			"label": fmt.Sprintf("label-%05d-%04d", ordinal, r.Intn(10000)),
			"code":  float64(ordinal*10 + r.Intn(10)),
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
