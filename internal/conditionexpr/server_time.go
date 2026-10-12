package conditionexpr

import (
	"strings"
	"time"

	"github.com/lychee-technology/forma"
)

// ServerTimeToken is the operand that names the PostgreSQL transaction
// timestamp (#646). It is an operand only on a date or datetime attribute;
// on any other value type it is an ordinary literal.
const ServerTimeToken = "$now"

// ServerTime is a parsed $now operand: the transaction timestamp shifted by
// OffsetMs milliseconds.
type ServerTime struct {
	OffsetMs int64
}

// IsServerTime reports whether value spells the $now operand. A malformed
// offset still answers true, so ParseServerTime can refuse it by name
// instead of letting it fall through as an unparseable date literal.
func IsServerTime(value string) bool {
	return strings.HasPrefix(value, ServerTimeToken)
}

// ParseServerTime parses "$now", "$now-30s" or "$now+5m". The offset is a
// time.ParseDuration string with an explicit sign, and must be a whole
// number of milliseconds: every date/datetime is stored at millisecond
// precision, so a finer offset has no faithful rendering.
func ParseServerTime(attr string, vt forma.ValueType, value string) (ServerTime, error) {
	offset := strings.TrimPrefix(value, ServerTimeToken)
	if offset == "" {
		return ServerTime{}, nil
	}
	if offset[0] != '+' && offset[0] != '-' {
		return ServerTime{}, serverTimeOffsetError(attr, vt, value)
	}
	d, err := time.ParseDuration(offset)
	if err != nil {
		return ServerTime{}, serverTimeOffsetError(attr, vt, value)
	}
	if d%time.Millisecond != 0 {
		return ServerTime{}, forma.InvalidInputf(
			"invalid $now offset in %q for %s attribute '%s': the offset must be a whole number of milliseconds, the precision %s values are stored at",
			value, vt, attr, vt)
	}
	return ServerTime{OffsetMs: d.Milliseconds()}, nil
}

func serverTimeOffsetError(attr string, vt forma.ValueType, value string) error {
	return forma.InvalidInputf(
		"invalid $now operand %q for %s attribute '%s': expected $now, or $now followed by a signed Go duration such as $now-30s or $now+5m",
		value, vt, attr)
}
