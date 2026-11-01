package federated

import (
	"fmt"
	"math"
	"math/big"
	"strconv"

	"github.com/lychee-technology/forma/internal/model"
)

// eavVerdict is the outcome of comparing two EAV values.
type eavVerdict int

const (
	// eavSame: both sides name the same value.
	eavSame eavVerdict = iota
	// eavDiffer: the sides name different values.
	eavDiffer
	// eavAmbiguous: one side knows its value exactly, the other carries only
	// a float64 image the exact value rounds to, which is not that value's
	// canonical image. The image cannot tell the exact value from its
	// neighbours (2^53+1 and 2^53 share one), so the comparison neither
	// asserts equality nor hides the difference: it reports the pair.
	eavAmbiguous
)

// eavNumber is an EAV attribute's numeric reading at the precision the read
// path delivered it: its float64 image, and its exact value whenever the
// record carries one. Forma reads the int64 sidecar ahead of the image (a
// bigint or unix_ms main column), and judges the NUMERIC token the read query
// emitted ahead of its rounded image (ValueNumericRaw, the date and integer
// contracts), so either is the value the comparison must hold exactly.
type eavNumber struct {
	image  *float64
	exact  *big.Rat
	source string // "int64" or "token" when exact is set
	text   string // exact's spelling as the record carries it
}

func newEAVNumber(attr model.EAVRecord) eavNumber {
	n := eavNumber{image: attr.ValueNumeric}
	switch {
	case attr.ValueInt64 != nil:
		n.exact = new(big.Rat).SetInt64(*attr.ValueInt64)
		n.source, n.text = "int64", strconv.FormatInt(*attr.ValueInt64, 10)
	case attr.ValueNumericRaw != "":
		// A non-finite spelling ("NaN", "Infinity") names no rational; its
		// image already carries it exactly.
		if exact, ok := new(big.Rat).SetString(attr.ValueNumericRaw); ok {
			n.exact = exact
			n.source, n.text = "token", attr.ValueNumericRaw
		}
	}
	return n
}

func (n eavNumber) present() bool { return n.image != nil || n.exact != nil }

func (n eavNumber) String() string {
	switch {
	case n.exact != nil:
		return fmt.Sprintf("%s (%s)", n.text, n.source)
	case n.image != nil:
		return fmt.Sprintf("%v (float64)", *n.image)
	}
	return "<nil>"
}

// compareNumbers compares two readings exactly when both are exact, by their
// float64 images when neither is (NaN equal to NaN: both sides know the same),
// and an exact reading against an image by exactAgainstImage.
func compareNumbers(a, b eavNumber) eavVerdict {
	switch {
	case !a.present() && !b.present():
		return eavSame
	case a.present() != b.present():
		return eavDiffer
	case a.exact != nil && b.exact != nil:
		return verdictOf(a.exact.Cmp(b.exact) == 0)
	case a.exact == nil && b.exact == nil:
		af, bf := *a.image, *b.image
		return verdictOf(af == bf || (math.IsNaN(af) && math.IsNaN(bf)))
	case a.exact != nil:
		return exactAgainstImage(a.exact, *b.image)
	}
	return exactAgainstImage(b.exact, *a.image)
}

// exactAgainstImage compares an exact value with a float64 image that is all
// the other side knows. The image names the exact value when it holds it
// exactly (9e15, 1.5) or when the value is the image's shortest round-trip
// decimal, the token a double column emits and Forma writes for it (0.1). An
// image the value merely rounds to (9007199254740992 for 9007199254740993,
// 1001 for 1001.0000000000000001, +Inf for 1e400) is ambiguous; any other
// image differs.
func exactAgainstImage(exact *big.Rat, image float64) eavVerdict {
	if !math.IsNaN(image) && !math.IsInf(image, 0) {
		if new(big.Rat).SetFloat64(image).Cmp(exact) == 0 {
			return eavSame
		}
		shortest, ok := new(big.Rat).SetString(strconv.FormatFloat(image, 'g', -1, 64))
		if ok && shortest.Cmp(exact) == 0 {
			return eavSame
		}
	}
	if rounded, _ := exact.Float64(); rounded == image {
		return eavAmbiguous
	}
	return eavDiffer
}

func verdictOf(same bool) eavVerdict {
	if same {
		return eavSame
	}
	return eavDiffer
}
