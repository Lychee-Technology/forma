package rfc3339

import "fmt"

// Defect says how a stored image departs from the canonical image of the
// instant it names (docs/rfc3339-storage-contract.md, "Rows outside the
// contract"). The PostgreSQL route compares the stored text and the DuckDB
// tiers compare the instant, so the two routes can answer differently on a
// row holding any image but the canonical one (#763, #842). The empty
// Defect means the image is canonical.
type Defect string

const (
	// Unreadable: Parse refuses the image, so every read of its row fails.
	Unreadable Defect = "unreadable"
	// Respelled: the image names a whole millisecond inside years 0000 to
	// 9999, under another spelling (an offset, a fraction of other than
	// three digits, the whole-second image of the iso8601 encoding). A
	// rewrite of that instant changes only the spelling.
	Respelled Defect = "spelling only"
	// SubMillisecond: the image names an instant finer than a millisecond.
	// A rewrite through the API floors it (#589), so the stored instant
	// moves by less than a millisecond.
	SubMillisecond Defect = "sub-millisecond"
	// NoCanonicalImage: an offset carries the instant the image names
	// outside years 0000 to 9999, which no canonical image spells and a
	// write refuses. The value has to change before a rewrite can store it.
	NoCanonicalImage Defect = "no canonical image"
)

// Judge answers the canonical image of the instant a stored image names,
// and how the stored image departs from it. It answers ("", Unreadable)
// when Parse refuses the image, ("", NoCanonicalImage) when the instant
// has no canonical image, and (image, "") when the image is canonical.
// The census of rfc3339 columns (internal/widthaudit) and the migrate-
// rfc3339 listing (internal/rfc3339migrate) both judge through it, so they
// cannot disagree on which images are canonical.
func Judge(image string) (canonical string, defect Defect) {
	parsed, err := Parse(image)
	if err != nil {
		return "", Unreadable
	}
	// UnixMilli floors: the nanoseconds within a second are never negative,
	// before the epoch too, so this is the millisecond normalization keeps.
	canonical, rule := Image(parsed.UnixMilli())
	switch {
	case rule != "":
		return "", NoCanonicalImage
	case canonical == image:
		return canonical, ""
	case parsed.Nanosecond()%1_000_000 != 0:
		return canonical, SubMillisecond
	}
	return canonical, Respelled
}

// Explain phrases the defect for a report on the row holding the image:
// what the image is, and what a rewrite of its instant through the API
// stores. canonical is Judge's first answer.
func (d Defect) Explain(canonical string) string {
	switch d {
	case "":
		return "the canonical rfc3339 image"
	case Unreadable:
		return "not an RFC 3339 image the read parses"
	case Respelled:
		return fmt.Sprintf("not the canonical rfc3339 image of its instant, %s; a rewrite through the API changes only the spelling", canonical)
	case SubMillisecond:
		return fmt.Sprintf("not the canonical rfc3339 image: it names an instant finer than a millisecond, which a rewrite through the API floors to %s", canonical)
	case NoCanonicalImage:
		return "not the canonical rfc3339 image: it names an instant outside years 0000 to 9999, which no image spells and a write refuses; the value has to change before a rewrite can store it"
	}
	return string(d)
}
